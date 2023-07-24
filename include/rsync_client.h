// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef RSYNC_CLIENT_H_
#define RSYNC_CLIENT_H_
#include <glog/logging.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <list>
#include <atomic>
#include <memory>
#include <thread>
#include <condition_variable>

#include "include/rsync_client_thread.h"
#include "net/include/bg_thread.h"
#include "pstd/include/pstd_status.h"
#include "include/rsync_client_thread.h"
#include "net/include/net_cli.h"
#include "pstd/include/env.h"
#include "pstd/include/pstd_hash.h"
#include "pstd/include/pstd_string.h"
#include "pstd/include/pstd_status.h"
#include "rsync_service.pb.h"
#include "include/throttle.h"

using namespace pstd;
using namespace net;
using namespace RsyncService;

const std::string kDumpMetaFileName = "DUMP_META_DATA";
const std::string kUuidPrefix = "snapshot-uuid:";

namespace rsync {

class RsyncWriter;
class Session;
class WaitObject;

class RsyncClient : public net::Thread {
public:
  enum State {
      IDLE,
      RUNNING,
      STOP,
  };
  RsyncClient(const std::string& dir, const std::string& db_name, const uint32_t slot_id);
  void* ThreadMain() override;
  bool Init();
  Status Start();
  Status Stop();
  bool IsRunning() {
    return state_.load() == RUNNING;
  }
  bool IsIdle() { return state_.load() == IDLE;}
  void OnReceive(RsyncResponse* resp);

private:
  bool Recover();
  Status Wait(RsyncResponse*& resp);
  Status CopyRemoteFile(const std::string& filename);
  Status CopyRemoteMeta(std::string* snapshot_uuid, std::set<std::string>* file_set);
  Status LoadLocalMeta(std::string* snapshot_uuid, std::map<std::string, std::string>* file_map);
  std::string GetLocalMetaFilePath();
  Status FlushMetaTable();
  Status CleanUpExpiredFiles(bool need_reset_path, std::set<std::string> files);
  Status UpdateLocalMeta(std::string& snapshot_uuid, std::set<std::string>& expired_files, std::map<std::string, std::string>& localFileMap);
  void HandleRsyncMetaResponse(RsyncResponse* response);

private:
  std::map<std::string, std::string> meta_table_;
  int flush_period_;
  std::set<std::string> file_set_;
  std::string snapshot_uuid_;
  std::string dir_;
  std::string db_name_;
  uint32_t slot_id_;
  std::unique_ptr<RsyncClientThread> client_thread_;
  std::atomic<State> state_;
  int max_retries_;
  std::unique_ptr<WaitObject> wo_;
  std::condition_variable cond_;
  std::mutex mu_;
  std::unique_ptr<Throttle> throttle_;
  std::string master_ip_;
  int master_port_;
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
        if (errno == EINTR) continue;
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
  int fd_;
};

class WaitObject {
public:
  WaitObject() : filename_(""), type_(RsyncService::kRsyncMeta), offset_(0), resp_(nullptr) {}
  ~WaitObject() {}
  void Reset(const std::string& filename, RsyncService::Type t, size_t offset) {
    resp_ = nullptr;
    filename_ = filename;
    type_ = t;
    offset_ = offset;
  }
  void Reset(RsyncService::Type t) {
    resp_ = nullptr;
    filename_ = "";
    type_ = t;
    offset_ = 0xFFFFFFFF;
  }
  std::string filename_;
  RsyncService::Type type_;
  size_t offset_;
  RsyncResponse* resp_;
};

} // end namespace rsync

#endif
