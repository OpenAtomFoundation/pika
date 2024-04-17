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
const size_t kInvalidOffset = 0xFFFFFFFF;

namespace rsync {

class RsyncWriter;
class Session;
class WaitObject;
class WaitObjectManager;

using pstd::Status;

using ResponseSPtr = std::shared_ptr<RsyncService::RsyncResponse>;
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
  void ResetThrottleThroughputBytes(size_t new_throughput_bytes_per_s){
      std::lock_guard guard(config_mu_);
      if(last_rsync_config_updated_time_ms_ + 1000 < pstd::NowMilliSeconds()){
        //maximum update frequency of rsync config:1 time per sec
        Throttle::GetInstance().SetThrottleThroughputBytes(new_throughput_bytes_per_s);
        LOG(INFO) << "The conf item [throttle-bytes-per-second] is changed by Config Set command. "
                     "The rsync rate limit now is "
                  << new_throughput_bytes_per_s << "(Which Is Around " << (new_throughput_bytes_per_s >> 20) << " MB/s)";
        last_rsync_config_updated_time_ms_ = pstd::NowMilliSeconds();
      }
  };
  void ResetRsyncTimeout(int64_t  new_timeout_ms);
private:
  bool ComparisonUpdate();
  Status CopyRemoteFile(const std::string& filename, int index);
  Status PullRemoteMeta(std::string* snapshot_uuid, std::set<std::string>* file_set);
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

  NetThreadUPtr client_thread_;
  std::vector<std::thread> work_threads_;
  std::atomic<int> finished_work_cnt_ = 0;

  std::atomic<State> state_;
  int max_retries_ = 10;
  std::unique_ptr<WaitObjectManager> wo_mgr_;
  std::condition_variable cond_;
  std::mutex mu_;

  //1 when multi thread changing rsync rate and timeout at the same time,make them take effect sequentially
  //2 maximum update frequency of rsync config: 1 time per sec
  std::mutex config_mu_;
  uint64_t last_rsync_config_updated_time_ms_{0};

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
  WaitObject(int64_t wait_timeout_ms) : filename_(""), type_(RsyncService::kRsyncMeta), offset_(0), resp_(nullptr), wait_timout_ms_(wait_timeout_ms) {}
  WaitObject() = delete;
  ~WaitObject() {}

  void Reset(const std::string& filename, RsyncService::Type t, size_t offset) {
    std::lock_guard<std::mutex> guard(mu_);
    resp_.reset();
    filename_ = filename;
    type_ = t;
    offset_ = offset;
  }

  pstd::Status Wait(ResponseSPtr& resp) {
    std::unique_lock<std::mutex> lock(mu_);
    auto cv_s = cond_.wait_for(lock, std::chrono::milliseconds(wait_timout_ms_), [this] {
      return resp_.get() != nullptr;
    });
    if (!cv_s) {
      std::string timout_info("timeout during(in ms) is ");
      timout_info.append(std::to_string(wait_timout_ms_));
      return pstd::Status::Timeout("rsync timeout", timout_info);
    }
    resp = resp_;
    return pstd::Status::OK();
  }

  void WakeUp(RsyncService::RsyncResponse* resp) {
    std::unique_lock<std::mutex> lock(mu_);
    resp_.reset(resp);
    offset_ = kInvalidOffset;
    cond_.notify_all();
  }
  void ResetWaitTimeout(int64_t new_timeout_ms){
    std::lock_guard guard(mu_);
    wait_timout_ms_ = new_timeout_ms;
  }
  std::string Filename() {return filename_;}
  RsyncService::Type Type() {return type_;}
  size_t Offset() {return offset_;}
 private:
  int64_t wait_timout_ms_{1000};
  std::string filename_;
  RsyncService::Type type_;
  size_t offset_ = kInvalidOffset;
  ResponseSPtr resp_ = nullptr;
  std::condition_variable cond_;
  std::mutex mu_;
};

class WaitObjectManager {
 public:
  WaitObjectManager() {
    wo_vec_.resize(kMaxRsyncParallelNum);
    for (int i = 0; i < kMaxRsyncParallelNum; i++) {
      wo_vec_[i] = new WaitObject(g_pika_conf->rsync_timeout_ms());
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
    if (resp->code() != RsyncService::kOk) {
      LOG(WARNING) << "rsync response error";
      wo_vec_[index]->WakeUp(resp);
      return;
    }

    if (resp->type() == RsyncService::kRsyncFile &&
        ((resp->file_resp().filename() != wo_vec_[index]->Filename()) ||
	 (resp->file_resp().offset() != wo_vec_[index]->Offset()))) {
      delete resp;
      return;
    }
    wo_vec_[index]->WakeUp(resp);
  }
  void ResetWaitTimeOut(int64_t new_timeout_ms){
    std::lock_guard guard(mu_);
    for(auto wait_obj: wo_vec_){
      wait_obj->ResetWaitTimeout(new_timeout_ms);
    }
  }
 private:
  std::vector<WaitObject*> wo_vec_;
  std::mutex mu_;
};

} // end namespace rsync
#endif
