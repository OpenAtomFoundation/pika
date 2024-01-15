// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <stdio.h>
#include <fstream>

#include "rocksdb/env.h"
#include "pstd/include/pstd_defer.h"
#include "include/pika_server.h"
#include "include/rsync_client.h"

using namespace net;
using namespace pstd;
using namespace RsyncService;

extern PikaServer* g_pika_server;

const int kFlushIntervalUs = 10 * 1000 * 1000;
const int kBytesPerRequest = 4 << 20;
const int kThrottleCheckCycle = 10;

namespace rsync {
RsyncClient::RsyncClient(const std::string& dir, const std::string& db_name, const uint32_t slot_id)
    : snapshot_uuid_(""), dir_(dir), db_name_(db_name), slot_id_(slot_id),
      state_(IDLE), max_retries_(10), master_ip_(""), master_port_(0),
      parallel_num_(g_pika_conf->max_rsync_parallel_num()) {
  wo_mgr_.reset(new WaitObjectManager());
  client_thread_ = std::make_unique<RsyncClientThread>(3000, 60, wo_mgr_.get());
  work_threads_.resize(GetParallelNum());
  finished_work_cnt_.store(0);
}

void RsyncClient::Copy(const std::set<std::string>& file_set, int index) {
  Status s = Status::OK();
  for (const auto& file : file_set) {
    while (state_.load() == RUNNING) {
      LOG(INFO) << "copy remote file, filename: " << file;
      s = CopyRemoteFile(file, index);
      if (!s.ok()) {
        LOG(WARNING) << "copy remote file failed, msg: " << s.ToString();
        continue;
      }
      break;
    }
    if (state_.load() != RUNNING) {
      break;
    }
  }
  LOG(INFO) << "work_thread index: " << index << " copy remote files done";
  finished_work_cnt_.fetch_add(1);
  cond_.notify_all();
}

bool RsyncClient::Init() {
  if (state_ != IDLE) {
    LOG(WARNING) << "State should be IDLE when Init";
    return false;
  }
  master_ip_ = g_pika_server->master_ip();
  master_port_ = g_pika_server->master_port() + kPortShiftRsync2;
  file_set_.clear();
  client_thread_->StartThread();
  bool ret = Recover();
  if (!ret) {
    LOG(WARNING) << "RsyncClient recover failed";
    client_thread_->StopThread();
    return false;
  }
  finished_work_cnt_.store(0);
  LOG(INFO) << "RsyncClient recover success";
  return true;
}

void* RsyncClient::ThreadMain() {
  if (file_set_.empty()) {
    LOG(INFO) << "No remote files need copy, RsyncClient exit";
    state_.store(STOP);
    return nullptr;
  }

  Status s = Status::OK();
  LOG(INFO) << "RsyncClient begin to copy remote files";
  std::vector<std::set<std::string> > file_vec(GetParallelNum());
  int index = 0;
  for (const auto& file : file_set_) {
    file_vec[index++ % GetParallelNum()].insert(file);
  }

  for (int i = 0; i < GetParallelNum(); i++) {
    work_threads_[i] = std::move(std::thread(&RsyncClient::Copy, this, file_vec[i], i));
  }

  std::string meta_file_path = GetLocalMetaFilePath();
  std::ofstream outfile;
  outfile.open(meta_file_path, std::ios_base::app);
  if (!outfile.is_open()) {
    LOG(FATAL) << "unable to open meta file " << meta_file_path << ", error:"  << strerror(errno);
    return nullptr;
  }
  DEFER {
    outfile.close();
  };

  std::string meta_rep;
  uint64_t start_time = pstd::NowMicros();

  while (state_.load() == RUNNING) {
    uint64_t elapse = pstd::NowMicros() - start_time;
    if (elapse < kFlushIntervalUs) {
      int wait_for_us = kFlushIntervalUs - elapse;
      std::unique_lock<std::mutex> lock(mu_);
      cond_.wait_for(lock, std::chrono::microseconds(wait_for_us));
    }

    if (state_.load() != RUNNING) {
      break;
    }

    start_time = pstd::NowMicros();
    std::map<std::string, std::string> files_map;
    {
      std::lock_guard<std::mutex> guard(mu_);
      files_map.swap(meta_table_);
    }
    for (const auto& file : files_map) {
      meta_rep.append(file.first + ":" + file.second);
      meta_rep.append("\n");
    }
    outfile << meta_rep;
    outfile.flush();
    meta_rep.clear();

    if (finished_work_cnt_.load() == GetParallelNum()) {
      break;
    }
  }

  for (int i = 0; i < GetParallelNum(); i++) {
    work_threads_[i].join();
  }
  finished_work_cnt_.store(0);
  state_.store(STOP);
  LOG(INFO) << "RsyncClient copy remote files done";
  return nullptr;
}

Status RsyncClient::CopyRemoteFile(const std::string& filename, int index) {
    const std::string filepath = dir_ + "/" + filename;
    std::unique_ptr<RsyncWriter> writer(new RsyncWriter(filepath));
    Status s = Status::OK();
    size_t offset = 0;
    int retries = 0;

    DEFER {
      if (writer) {
        writer->Close();
        writer.reset();
      }
      if (!s.ok()) {
        DeleteFile(filepath);
      }
    };

    while (retries < max_retries_) {
      if (state_.load() != RUNNING) {
        break;
      }
      size_t copy_file_begin_time = pstd::NowMicros();
      size_t count = Throttle::GetInstance().ThrottledByThroughput(kBytesPerRequest);
      if (count == 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000 / kThrottleCheckCycle));
        continue;
      }
      RsyncRequest request;
      request.set_reader_index(index);
      request.set_type(kRsyncFile);
      request.set_db_name(db_name_);
      request.set_slot_id(slot_id_);
      FileRequest* file_req = request.mutable_file_req();
      file_req->set_filename(filename);
      file_req->set_offset(offset);
      file_req->set_count(count);

      std::string to_send;
      request.SerializeToString(&to_send);
      WaitObject* wo = wo_mgr_->UpdateWaitObject(index, filename, kRsyncFile, offset);
      s = client_thread_->Write(master_ip_, master_port_, to_send);
      if (!s.ok()) {
        LOG(WARNING) << "send rsync request failed";
        continue;
      }

      std::shared_ptr<RsyncResponse> resp = nullptr;
      s = wo->Wait(resp);
      if (s.IsTimeout() || resp == nullptr) {
        LOG(WARNING) << "rsync request timeout";
        retries++;
        continue;
      }

      size_t ret_count = resp->file_resp().count();
      size_t elaspe_time_us = pstd::NowMicros() - copy_file_begin_time;
      Throttle::GetInstance().ReturnUnusedThroughput(count, ret_count, elaspe_time_us);

      if (resp->code() != RsyncService::kOk) {
        //TODO: handle different error
        continue;
      }

      if (resp->snapshot_uuid() != snapshot_uuid_) {
        LOG(WARNING) << "receive newer dump, reset state to STOP, local_snapshot_uuid:"
                     << snapshot_uuid_ << "remote snapshot uuid: " << resp->snapshot_uuid();
        state_.store(STOP);
        return s;
      }

      s = writer->Write((uint64_t)offset, ret_count, resp->file_resp().data().c_str());
      if (!s.ok()) {
        LOG(WARNING) << "rsync client write file error";
        break;
      }

      offset += resp->file_resp().count();
      if (resp->file_resp().eof()) {
        s = writer->Fsync();
        if (!s.ok()) {
            return s;
        } 
        mu_.lock();
        meta_table_[filename] = "";
        mu_.unlock();
        break;
      }
      retries = 0;
    }

  return s;
}

Status RsyncClient::Start() {
  StartThread();
  return Status::OK();
}

Status RsyncClient::Stop() {
  if (state_ == IDLE) {
    return Status::OK();
  }
  LOG(WARNING) << "RsyncClient stop ...";
  state_ = STOP;
  cond_.notify_all();
  StopThread();
  client_thread_->StopThread();
  JoinThread();
  client_thread_->JoinThread();
  state_ = IDLE;
  return Status::OK();
}

bool RsyncClient::Recover() {
  std::string local_snapshot_uuid;
  std::string remote_snapshot_uuid;
  std::set<std::string> local_file_set;
  std::set<std::string> remote_file_set;
  std::map<std::string, std::string> local_file_map;

  Status s = CopyRemoteMeta(&remote_snapshot_uuid, &remote_file_set);
  if (!s.ok()) {
    LOG(WARNING) << "copy remote meta failed! error:" << s.ToString();
    return false;
  }

  s = LoadLocalMeta(&local_snapshot_uuid, &local_file_map);
  if (!s.ok()) {
    LOG(WARNING) << "load local meta failed";
    return false;
  }
  for (auto const& file : local_file_map) {
    local_file_set.insert(file.first);
  }

  std::set<std::string> expired_files;
  if (remote_snapshot_uuid != local_snapshot_uuid) {
    snapshot_uuid_ = remote_snapshot_uuid;
    file_set_ = remote_file_set;
    expired_files = local_file_set;
  } else {
    std::set<std::string> newly_files;
    set_difference(remote_file_set.begin(), remote_file_set.end(),
                   local_file_set.begin(), local_file_set.end(),
                   inserter(newly_files, newly_files.begin()));
    set_difference(local_file_set.begin(), local_file_set.end(),
                   remote_file_set.begin(), remote_file_set.end(),
                   inserter(expired_files, expired_files.begin()));
    file_set_.insert(newly_files.begin(), newly_files.end());
  }

  s = CleanUpExpiredFiles(local_snapshot_uuid != remote_snapshot_uuid, expired_files);
  if (!s.ok()) {
    LOG(WARNING) << "clean up expired files failed";
    return false;
  }
  s = UpdateLocalMeta(snapshot_uuid_, expired_files, &local_file_map);
  if (!s.ok()) {
    LOG(WARNING) << "update local meta failed";
    return false;
  }

  state_ = RUNNING;
  LOG(INFO) << "copy meta data done, slot_id: " << slot_id_
            << " snapshot_uuid: " << snapshot_uuid_
            << " file count: " << file_set_.size()
            << " expired file count: " << expired_files.size()
            << " local file count: " << local_file_set.size()
            << " remote file count: " << remote_file_set.size()
            << " remote snapshot_uuid: " << remote_snapshot_uuid
            << " local snapshot_uuid: " << local_snapshot_uuid
            << " file_set_: " << file_set_.size();
  for_each(file_set_.begin(), file_set_.end(),
           [](auto& file) {LOG(WARNING) << "file_set: " << file;});
  return true;
}

Status RsyncClient::CopyRemoteMeta(std::string* snapshot_uuid, std::set<std::string>* file_set) {
  Status s;
  int retries = 0;
  RsyncRequest request;
  request.set_reader_index(0);
  request.set_db_name(db_name_);
  request.set_slot_id(slot_id_);
  request.set_type(kRsyncMeta);
  std::string to_send;
  request.SerializeToString(&to_send);
  while (retries < max_retries_) {
    WaitObject* wo = wo_mgr_->UpdateWaitObject(0, "", kRsyncMeta, kInvalidOffset);
    s = client_thread_->Write(master_ip_, master_port_, to_send);
    if (!s.ok()) {
      retries++;
    }
    std::shared_ptr<RsyncResponse> resp;
    s = wo->Wait(resp);
    if (s.IsTimeout()) {
      LOG(WARNING) << "rsync CopyRemoteMeta request timeout, "
                   << "retry times: " << retries;
      retries++;
      continue;
    }

    if (resp.get() == nullptr || resp->code() != RsyncService::kOk) {
      s = Status::IOError("kRsyncMeta request failed! unknown reason");
      continue;
    }
    LOG(INFO) << "receive rsync meta infos, snapshot_uuid: " << resp->snapshot_uuid()
              << "files count: " << resp->meta_resp().filenames_size();
    for (std::string item : resp->meta_resp().filenames()) {
      file_set->insert(item);
    }

    *snapshot_uuid = resp->snapshot_uuid();
    s = Status::OK();
    break;
  }
  return s;
}

Status RsyncClient::LoadLocalMeta(std::string* snapshot_uuid, std::map<std::string, std::string>* file_map) {
  std::string meta_file_path = GetLocalMetaFilePath();
  if (!FileExists(meta_file_path)) {
    LOG(WARNING) << kDumpMetaFileName << " not exist";
    return Status::OK();
  }

  FILE* fp;
  char* line = nullptr;
  size_t len = 0;
  size_t read = 0;
  int32_t line_num = 0;

  std::atomic_int8_t retry_times = 5;

  while (retry_times > 0) {
    retry_times--;
    fp = fopen(meta_file_path.c_str(), "r");
    if (fp == nullptr) {
      LOG(WARNING) << "open meta file failed, meta_path: " << dir_;
    } else {
      break;
    }
  }

  // if the file cannot be read from disk, use the remote file directly
  if (fp == nullptr) {
    LOG(WARNING) << "open meta file failed, meta_path: " << meta_file_path << ", retry times: " << retry_times;
    return Status::IOError("open meta file failed, dir: ", meta_file_path);
  }

  while ((read = getline(&line, &len, fp)) != -1) {
    std::string str(line);
    std::string::size_type pos;
    while ((pos = str.find("\r")) != std::string::npos) {
      str.erase(pos, 1);
    }
    while ((pos = str.find("\n")) != std::string::npos) {
      str.erase(pos, 1);
    }

    if (str.empty()) {
      continue;
    }

    if (line_num == 0) {
      *snapshot_uuid = str.erase(0, kUuidPrefix.size());
    } else {
      if ((pos = str.find(":")) != std::string::npos) {
        std::string filename = str.substr(0, pos);
        std::string shecksum = str.substr(pos + 1, str.size());
        (*file_map)[filename] = shecksum;
      }
    }

    line_num++;
  }
  fclose(fp);
  return Status::OK();
}

Status RsyncClient::CleanUpExpiredFiles(bool need_reset_path, const std::set<std::string>& files) {
  if (need_reset_path) {
    std::string db_path = dir_ + (dir_.back() == '/' ? "" : "/");
    pstd::DeleteDirIfExist(db_path);
    pstd::CreatePath(db_path + "strings");
    pstd::CreatePath(db_path + "hashes");
    pstd::CreatePath(db_path + "lists");
    pstd::CreatePath(db_path + "sets");
    pstd::CreatePath(db_path + "zsets");
    return Status::OK();
  }

  std::string db_path = dir_ + (dir_.back() == '/' ? "" : "/");
  for (const auto& file : files) {
    bool b = pstd::DeleteDirIfExist(db_path + file);
    if (!b) {
      LOG(WARNING) << "delete file failed, file: " << file;
      return Status::IOError("delete file failed");
    }
  }
  return Status::OK();
}

Status RsyncClient::UpdateLocalMeta(const std::string& snapshot_uuid, const std::set<std::string>& expired_files,
                                    std::map<std::string, std::string>* localFileMap) {
  if (localFileMap->empty()) {
    return Status::OK();
  }
  
  for (const auto& item : expired_files) {
    localFileMap->erase(item);
  }

  std::string meta_file_path = GetLocalMetaFilePath();
  pstd::DeleteFile(meta_file_path);

  std::unique_ptr<WritableFile> file;
  pstd::Status s = pstd::NewWritableFile(meta_file_path, file);
  if (!s.ok()) {
    LOG(WARNING) << "create meta file failed, meta_file_path: " << meta_file_path;
    return s;
  }
  file->Append(kUuidPrefix + snapshot_uuid + "\n");

  for (const auto& item : *localFileMap) {
    std::string line = item.first + ":" + item.second + "\n";
    file->Append(line);
  }
  s = file->Close();
  if (!s.ok()) {
    LOG(WARNING) << "flush meta file failed, meta_file_path: " << meta_file_path;
    return s;
  }
  return Status::OK();
}

std::string RsyncClient::GetLocalMetaFilePath() {
  std::string db_path = dir_ + (dir_.back() == '/' ? "" : "/");
  return db_path + kDumpMetaFileName;
}

int RsyncClient::GetParallelNum() {
  return parallel_num_;
}

}  // end namespace rsync

