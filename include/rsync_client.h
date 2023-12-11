// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef RSYNC_CLIENT_H_
#define RSYNC_CLIENT_H_

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <list>
#include <atomic>
#include <memory>
#include <thread>
#include <condition_variable>

#include <glog/logging.h>

#include "net/include/bg_thread.h"
#include "net/include/net_cli.h"
#include "pstd/include/env.h"
#include "pstd/include/pstd_status.h"
#include "pstd/include/pstd_hash.h"
#include "pstd/include/pstd_string.h"
#include "pstd/include/pstd_status.h"
#include "include/pika_define.h"
#include "include/rsync_client_thread.h"
#include "include/throttle.h"
#include "rsync_service.pb.h"

extern std::unique_ptr<PikaConf> g_pika_conf;

const std::string kDumpMetaFileName = "DUMP_META_DATA";
const std::string kUuidPrefix = "snapshot-uuid:";

namespace rsync {

class RsyncWriter;
class Session;
class WaitObject;
class WaitObjectManager;

using pstd::Status;


class RsyncClient : public net::Thread {
 public:
  enum State {
      IDLE,
      RUNNING,
      STOP,
  };
  RsyncClient(const std::string& dir, const std::string& db_name);
  void* ThreadMain() override;
  void Copy(const std::set<std::string>& file_set, int index);
  bool Init();
  int GetParallelNum();
  Status Start();
  Status Stop();
  bool IsRunning() {
    return state_.load() == RUNNING;
  }
  bool IsStop() {
    return state_.load() == STOP;
  }
  bool IsIdle() { return state_.load() == IDLE;}
  void OnReceive(RsyncService::RsyncResponse* resp);

private:
  bool Recover();
  Status CopyRemoteFile(const std::string& filename, int index);
  Status CopyRemoteMeta(std::string* snapshot_uuid, std::set<std::string>* file_set);
  Status LoadLocalMeta(std::string* snapshot_uuid, std::map<std::string, std::string>* file_map);
  std::string GetLocalMetaFilePath();
  Status FlushMetaTable();
  Status CleanUpExpiredFiles(bool need_reset_path, const std::set<std::string>& files);
  Status UpdateLocalMeta(const std::string& snapshot_uuid, const std::set<std::string>& expired_files,
                         std::map<std::string, std::string>* localFileMap);
  void HandleRsyncMetaResponse(RsyncService::RsyncResponse* response);

private:
  typedef std::unique_ptr<RsyncClientThread> NetThreadUPtr;

  std::map<std::string, std::string> meta_table_;
  std::set<std::string> file_set_;
  std::string snapshot_uuid_;
  std::string dir_;
  std::string db_name_;
  uint32_t slot_id_ = 0;

  NetThreadUPtr client_thread_;
  std::vector<std::thread> work_threads_;
  std::atomic<int> finished_work_cnt_ = 0;

  std::atomic<State> state_;
  int max_retries_ = 10;
  std::unique_ptr<WaitObjectManager> wo_mgr_;
  std::condition_variable cond_;
  std::mutex mu_;

  std::string master_ip_;
  int master_port_;
  int parallel_num_;
};

class RsyncWriter {
public:
  RsyncWriter(const std::string& filepath) {
    filepath_ = filepath;
    fd_ = open(filepath.c_str(), O_RDWR | O_APPEND | O_CREAT, 0644);
  }
  ~RsyncWriter() {}
  Status Write(uint64_t offset, size_t n, const char* data) {
    const char* ptr = data;
    size_t left = n;
    Status s;
    while (left != 0) {
      ssize_t done = write(fd_, ptr, left);
      if (done < 0) {
        if (errno == EINTR) {
          continue;
        }
        LOG(WARNING) << "pwrite failed, filename: " << filepath_ << "errno: " << strerror(errno) << "n: " << n;
        return Status::IOError(filepath_, "pwrite failed");
      }
      left -= done;
      ptr += done;
      offset += done;
    }
    return Status::OK();
  }
  Status Close() {
    close(fd_);
    return Status::OK();
  }
  Status Fsync() {
    fsync(fd_);
    return Status::OK();
  }

private:
  std::string filepath_;
  int fd_ = -1;
};

class WaitObject {
public:
  WaitObject() : filename_(""), type_(RsyncService::kRsyncMeta), offset_(0), resp_(nullptr) {}
  ~WaitObject() {}

  void Reset(const std::string& filename, RsyncService::Type t, size_t offset) {
    std::lock_guard<std::mutex> guard(mu_);
    resp_ = nullptr;
    filename_ = filename;
    type_ = t;
    offset_ = offset;
  }

  pstd::Status Wait(RsyncService::RsyncResponse*& resp) {
    pstd::Status s = Status::Timeout("rsync timeout", "timeout");
    {
      std::unique_lock<std::mutex> lock(mu_);
      auto cv_s = cond_.wait_for(lock, std::chrono::seconds(1), [this] {
          return resp_ != nullptr;
      });
      if (!cv_s) {
        return s;
      }
      resp = resp_;
      s = Status::OK();
    }
    return s;
  }

  void WakeUp(RsyncService::RsyncResponse* resp) {
    std::unique_lock<std::mutex> lock(mu_);
    resp_ = resp;
    cond_.notify_all();
  }

  RsyncService::RsyncResponse* Response() {return resp_;}
  std::string Filename() {return filename_;}
  RsyncService::Type Type() {return type_;}
  size_t Offset() {return offset_;}
private:
  std::string filename_;
  RsyncService::Type type_;
  size_t offset_ = 0xFFFFFFFF;
  RsyncService::RsyncResponse* resp_ = nullptr;
  std::condition_variable cond_;
  std::mutex mu_;
};

class WaitObjectManager {
public:
  WaitObjectManager() {
    wo_vec_.resize(kMaxRsyncParallelNum);
    for (int i = 0; i < kMaxRsyncParallelNum; i++) {
      wo_vec_[i] = new WaitObject();
    }
  }
  ~WaitObjectManager() {
    for (int i = 0; i < wo_vec_.size(); i++) {
      delete wo_vec_[i];
      wo_vec_[i] = nullptr;
    }
  }

  WaitObject* UpdateWaitObject(int worker_index, const std::string& filename,
                               RsyncService::Type type, size_t offset) {
    std::lock_guard<std::mutex> guard(mu_);
    wo_vec_[worker_index]->Reset(filename, type, offset);
    return wo_vec_[worker_index];
  }

  void WakeUp(RsyncService::RsyncResponse* resp) {
    std::lock_guard<std::mutex> guard(mu_);
    int index = resp->reader_index();
    if (wo_vec_[index] == nullptr || resp->type() != wo_vec_[index]->Type()) {
      delete resp;
      return;
    }
    if (resp->type() == RsyncService::kRsyncFile &&
        (resp->file_resp().filename() != wo_vec_[index]->Filename())) {
      delete resp;
      return;
    }
    wo_vec_[index]->WakeUp(resp);
  }

private:
  std::vector<WaitObject*> wo_vec_;
  std::mutex mu_;
};

} // end namespace rsync
#endif

