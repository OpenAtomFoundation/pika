#include "include/rsync_client.h"
#include <stdio.h>
#include "include/pika_server.h"
#include "pstd/include/pstd_defer.h"
#include "pstd/src/env.cc"
#include "rocksdb/env.h"

using namespace net;
using namespace pstd;
using namespace RsyncService;
using namespace pstd;

extern PikaServer* g_pika_server;

namespace rsync {
RsyncClient::RsyncClient(const std::string& dir, const std::string& db_name, const uint32_t slot_id)
    : dir_(dir), flush_period_(100), db_name_(db_name), slot_id_(slot_id), state_(IDLE), max_retries_(10) {
  client_thread_ = std::make_unique<RsyncClientThread>(10 * 1000, 60 * 1000, this);
}

bool RsyncClient::Init() {
  // todo client 的 StartThread 只能被调用一次，如果一个 slot 进行多次主从同步，这里会出问题吗？
  client_thread_->StartThread();
  bool ret = Recover();
  if (!ret) {
    LOG(WARNING) << "RsyncClient recover failed...";
    client_thread_->StopThread();
    return false;
  }
  LOG(INFO) << "RsyncClient recover success...";
  return true;
}

void* RsyncClient::ThreadMain() {
    int cnt = 0;
    int period = 0;
    Status s = Status::OK();
    std::string meta_file_path = dir_ + "/" + kDumpMetaFileName;
    int meta_fd = open(meta_file_path.c_str(), O_CREAT | O_RDWR, 0644);
    std::string meta_rep(kUuidPrefix);
    meta_rep.append(snapshot_uuid_);
    meta_rep.append("\n");
    while (state_.load(std::memory_order_relaxed) == RUNNING) {
        for (const auto& file : file_set_) {
            LOG(INFO) << "CopyRemoteFile: " << file;
            while (state_.load() == RUNNING) {
              s = CopyRemoteFile(file);
              if (!s.ok()) {
                  LOG(WARNING) << "rsync CopyRemoteFile failed, filename: " << file;
                  continue;
              }
              LOG(WARNING) << "CopyRemoteFile "<< file << "success...";
              break;
            }
            if (state_.load(std::memory_order_relaxed) != RUNNING) {
                break;
            }
            if (++period == flush_period_) {
                period = 0;
                meta_rep.append(file + ":" + meta_table_[file]);
                meta_rep.append("\n");
                write(meta_fd, meta_rep.data(), meta_rep.size());
                fsync(meta_fd);
                meta_rep.clear();
            }
        }
        if (meta_table_.size() == file_set_.size()) {
            LOG(INFO) << "rsync success...";
            state_.store(STOP, std::memory_order_relaxed);
            break;
        }
    }
    close(meta_fd);
    return nullptr;
}

void RsyncClient::OnReceive(RsyncResponse* resp) {
  std::unique_lock<std::mutex> lock(mu_);
  resp_list_.push_back(resp);
  cond_.notify_all();
}

Status RsyncClient::Wait(WaitObject* wo) {
  Status s = Status::Timeout("rsync timeout", "timeout");
  std::list<RsyncResponse*> resp_list;
  {
    std::unique_lock<std::mutex> lock(mu_);
    cond_.wait_for(lock, std::chrono::seconds(3), [this] { return !resp_list_.empty(); });
    resp_list.swap(resp_list_);
  }

  auto iter = resp_list.begin();
  while (iter != resp_list.end()) {
    RsyncResponse* resp = *iter;
    if (resp->type() != wo->type_) {
      LOG(WARNING) << "mismatch request/response type, skip";
      iter++;
      continue;
    }
    if (resp->type() == kRsyncFile &&
        (resp->file_resp().filename() != wo->filename_ || resp->file_resp().offset() != wo->offset_))  {
      LOG(WARNING) << "mismatch rsync response, skip";
      continue;
    }
    s = Status::OK();
    wo->resp_ = resp;
    resp_list.erase(iter);
    break;
  }

  iter = resp_list.begin();
  while (iter != resp_list.end()) {
    delete (*iter);
    iter++;
  }
  return s;
}

Status RsyncClient::CopyRemoteFile(const std::string& filename) {
    Status s;
    int retries = 0;
    size_t offset = 0;
    size_t copy_file_begin_time = pstd::NowMicros();
    size_t count = throttle_->ThrottledByThroughput(1024 * 1024);
    MD5 md5;
    std::unique_ptr<RsyncWriter> writer(new RsyncWriter(dir_ + "/" + filename));
    DEFER {
        if (writer) {
            writer->Close();
            writer.reset();
        }
        if (!s.ok()) {
            DeleteFile(filename);
        }
    };
    while (retries < max_retries_) {
        RsyncRequest request;
        request.set_type(kRsyncFile);
        request.set_db_name(db_name_);
        request.set_slot_id(slot_id_);
        FileRequest* file_req = request.mutable_file_req();
        file_req->set_filename(filename);
        file_req->set_offset(offset);
        file_req->set_count(count);
        std::string to_send;
        request.SerializeToString(&to_send);

    s = client_thread_->Write(g_pika_server->master_ip(), g_pika_server->master_port() + kPortShiftRsync2, to_send);
    if (!s.ok()) {
      LOG(WARNING) << "send rsync request failed";
      continue;
    }

    WaitObject wo(filename, kRsyncFile, offset);
    LOG(INFO) << "wait CopyRemoteFile response.....";
    s = Wait(&wo);
    if (s.IsTimeout() || wo.resp_ == nullptr) {
      LOG(WARNING) << "rsync request timeout";
      retries++;
      continue;
    }
    RsyncResponse* resp = wo.resp_;

    LOG(INFO) << "receive fileresponse, snapshot_uuid: " << resp->snapshot_uuid()
              << "filename: " << resp->file_resp().filename() << "offset: " << resp->file_resp().offset()
              << "count: " << resp->file_resp().count();

    if (resp->snapshot_uuid() != snapshot_uuid_) {
      LOG(WARNING) << "receive newer dump, reset state to STOP";
      state_.store(STOP);
      delete resp;
      return s;
    }

    size_t ret_count = resp->file_resp().count();
    resp->file_resp().data();
    s = writer->Write((uint64_t)offset, ret_count, resp->file_resp().data().c_str());
    if (!s.ok()) {
      LOG(WARNING) << "rsync client write file error";
      break;
    }

        md5.update(resp->file_resp().data().c_str(), ret_count);
        if (resp->file_resp().eof()) {
            if (md5.finalize().hexdigest() != resp->file_resp().checksum()) {
                LOG(WARNING) << "mismatch file checksum for file: " << filename;
                //TODO: 处理返回status
                s = Status::IOError("mismatch checksum", "mismatch checksum");
            }
            s = writer->Fsync();
            if (!s.ok()) {
                return s;
            }
            s = writer->Close();
            if (!s.ok()) {
                return s;
            }
            writer.reset();
            meta_table_[filename] = resp->file_resp().checksum();
            break;
        } else {
            offset += resp->file_resp().count();
        }
        size_t copy_file_end_time = pstd::NowMicros();
        size_t elaspe_time_us = copy_file_end_time - copy_file_begin_time;
        throttle_->ReturnUnusedThroughput(count, ret_count, elaspe_time_us);
        retries = 0;
    }

  return s;
}

Status RsyncClient::Start() {
  StartThread();
  return Status::OK();
}

Status RsyncClient::Stop() {
  state_ = STOP;
  StopThread();
  client_thread_->StopThread();
  JoinThread();
  client_thread_->JoinThread();
  state_ = IDLE;
  return Status::OK();
}

bool RsyncClient::Recover() {
  std::string remote_snapshot_uuid;
  std::set<std::string> remote_file_set;
  std::string local_snapshot_uuid;
  std::map<std::string, std::string> local_file_map;
  std::set<std::string> local_file_set;

  Status s = CopyRemoteMeta(&remote_snapshot_uuid, &remote_file_set);
  if (!s.ok()) {
    LOG(WARNING) << "copy remote meta failed";
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
    set_difference(remote_file_set.begin(), remote_file_set.end(), local_file_set.begin(), local_file_set.end(),
                   inserter(newly_files, newly_files.begin()));
    set_difference(local_file_set.begin(), local_file_set.end(), remote_file_set.begin(), remote_file_set.end(),
                   inserter(expired_files, expired_files.begin()));
    file_set_.insert(newly_files.begin(), newly_files.end());
  }

  s = CleanUpExpiredFiles(local_snapshot_uuid != remote_snapshot_uuid, expired_files);
  if (!s.ok()) {
    LOG(WARNING) << "clean up expired files failed";
    return false;
  }
  s = UpdateLocalMeta(snapshot_uuid_, expired_files, local_file_map);
  if (!s.ok()) {
    LOG(WARNING) << "update local meta failed";
    return false;
  }

  state_ = RUNNING;
  LOG(INFO) << "copy meta data done, slot_id: " << slot_id_ << "snapshot_uuid: " << snapshot_uuid_
            << "file count: " << file_set_.size() << "expired file count: " << expired_files.size()
            << ", local file count: " << local_file_set.size() << "remote file count: " << remote_file_set.size()
            << "remote snapshot_uuid: " << remote_snapshot_uuid << "local snapshot_uuid: " << local_snapshot_uuid;
  return true;
}

Status RsyncClient::CopyRemoteMeta(std::string* snapshot_uuid, std::set<std::string>* file_set) {
  Status s;
  int retries = 0;
  RsyncRequest request;
  request.set_db_name(db_name_);
  request.set_slot_id(slot_id_);
  request.set_type(kRsyncMeta);
  std::string to_send;
  request.SerializeToString(&to_send);
  while (retries < max_retries_) {
    s = client_thread_->Write(g_pika_server->master_ip(), g_pika_server->master_port() + kPortShiftRsync2, to_send);
    if (!s.ok()) {
      retries++;
    }
    WaitObject wo(kRsyncMeta);
    s = Wait(&wo);
    if (s.IsTimeout() || wo.resp_ == nullptr) {
      LOG(WARNING) << "rsync CopyRemoteMeta request timeout, retry times: " << retries;
      retries++;
      continue;
    }
    RsyncResponse* resp = wo.resp_;
    LOG(INFO) << "receive rsync meta infos, snapshot_uuid: " << resp->snapshot_uuid()
              << "files count: " << resp->meta_resp().filenames_size();

    for (std::string item : resp->meta_resp().filenames()) {
      file_set->insert(item);
    }
    *snapshot_uuid = resp->snapshot_uuid();
    for (int i = 0; i < resp->meta_resp().filenames_size(); i++) {
      file_set->insert(resp->meta_resp().filenames(i));
    }
    break;
  }
  return s;
}

Status RsyncClient::LoadLocalMeta(std::string* snapshot_uuid, std::map<std::string, std::string>* file_map) {
  std::string meta_file_path = GetLocalMetaFilePath();
  if (!FileExists(meta_file_path)) {
    return Status::OK();
  }

  FILE* fp;
  char* line = nullptr;
  size_t len = 0;
  size_t read = 0;
  int32_t line_num = 0;

  std::atomic_int8_t retry_times = 5;

  while (retry_times-- > 0) {
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
  return Status::OK();
}

Status RsyncClient::CleanUpExpiredFiles(bool need_reset_path, std::set<std::string> files) {
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

Status RsyncClient::UpdateLocalMeta(std::string& snapshot_uuid, std::set<std::string>& expired_files,
                                    std::map<std::string, std::string>& localFileMap) {
  localFileMap[kUuidPrefix] = snapshot_uuid;
  for (const auto& item : expired_files) {
    localFileMap.erase(item);
  }

  std::string meta_file_path = GetLocalMetaFilePath();
  pstd::DeleteFile(meta_file_path);

  std::unique_ptr<WritableFile> file;
  pstd::Status s = pstd::NewWritableFile(meta_file_path, file);
  if (!s.ok()) {
    LOG(WARNING) << "create meta file failed, meta_file_path: " << meta_file_path;
    return s;
  }

  for (const auto& item : localFileMap) {
    std::string line = item.first + item.second + "\n";
    file->Append(line);
  }
  s = file->Flush();
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

// TODO: shaoyi
Status RsyncClient::FlushMetaTable() {
  LOG(WARNING) << "FlushMetaTable called";
  return Status::OK();
}

}  // end namespace rsync