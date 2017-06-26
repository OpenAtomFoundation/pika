// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_DISPATCH_THREAD_H_
#define PIKA_DISPATCH_THREAD_H_

#include "slash/include/env.h"
#include "pink/include/server_thread.h"
#include "pika_client_conn.h"

class PikaWorkerSpecificData {
 public:
  PikaWorkerSpecificData()
      : thread_querynum_(0),
        last_thread_querynum_(0),
        last_time_us_(slash::NowMicros()),
        last_sec_thread_querynum_(0) {
    cmds_.reserve(300);
    InitCmdTable(&cmds_);
  }

  virtual ~PikaWorkerSpecificData() {
    DestoryCmdTable(cmds_);
  }

  uint64_t thread_querynum() {
    slash::ReadLock l(&rwlock_);
    return thread_querynum_;
  }

  void ResetThreadQuerynum() {
    slash::WriteLock l(&rwlock_);
    thread_querynum_ = 0;
    last_thread_querynum_ = 0;
  }

  uint64_t last_sec_thread_querynum() {
    slash::ReadLock l(&rwlock_);
    return last_sec_thread_querynum_;
  }

  void PlusThreadQuerynum() {
    slash::WriteLock l(&rwlock_);
    thread_querynum_++;
  }

  void ResetLastSecQuerynum() {
    uint64_t cur_time_us = slash::NowMicros();
    slash::WriteLock l(&rwlock_);
    last_sec_thread_querynum_ = ((thread_querynum_ - last_thread_querynum_) *
                                 1000000 / (cur_time_us - last_time_us_+1));
    last_thread_querynum_ = thread_querynum_;
    last_time_us_ = cur_time_us;
  }

  Cmd* GetCmd(const std::string& opt) {
    return GetCmdFromTable(opt, cmds_);
  }

 private:
  slash::RWMutex rwlock_;
  uint64_t thread_querynum_;
  uint64_t last_thread_querynum_;
  uint64_t last_time_us_;
  uint64_t last_sec_thread_querynum_;

  std::unordered_map<std::string, Cmd*> cmds_;
};

class PikaDispatchThread {
 public:
  PikaDispatchThread(std::set<std::string> &ips, int port, int work_num,
                     int cron_interval, int queue_limit);
  ~PikaDispatchThread();
  int StartThread();

  int64_t ThreadClientList(std::vector<ClientInfo> *clients);

  bool ClientKill(std::string ip_port) {
    return thread_rep_->KillConn(ip_port);
  }

  void ClientKillAll() {
    thread_rep_->KillAllConns();
  }

  uint64_t thread_querynum() {
    uint64_t query_num = 0;
    for (auto data : workers_data_) {
      query_num += data->thread_querynum();
    }
    return query_num;
  }

  void ResetThreadQuerynum() {
    for (auto data : workers_data_) {
      data->ResetThreadQuerynum();
    }
  }

  uint64_t last_sec_thread_querynum() {
    uint64_t lquery_num = 0;
    for (auto data : workers_data_) {
      lquery_num += data->last_sec_thread_querynum();
    }
    return lquery_num;
  }

 private:
  class ClientConnFactory : public pink::ConnFactory {
   public:
    virtual pink::PinkConn *NewPinkConn(int connfd,
                                        const std::string &ip_port,
                                        pink::ServerThread *server_thread,
                                        void* worker_specific_data) const {
      return new PikaClientConn(connfd, ip_port, server_thread, worker_specific_data);
    }
  };

  class PikaDispatchHandles : public pink::ServerHandle {
   public:
    explicit PikaDispatchHandles(PikaDispatchThread* pika_disptcher)
        : pika_disptcher_(pika_disptcher) {
    }
    bool AccessHandle(std::string& ip) const override;

    void CronHandle() const override {
      for (auto data : pika_disptcher_->workers_data_) {
        data->ResetLastSecQuerynum();
      }
    }

    int CreateWorkerSpecificData(void** data) const override {
      *data = pika_disptcher_->NewPikaWorkerSpecificData();
      return 0;
    }

    int DeleteWorkerSpecificData(void* data) const override {
      delete reinterpret_cast<PikaWorkerSpecificData*>(data);
      return 0;
    }

   private:
    PikaDispatchThread* pika_disptcher_;
  };

  ClientConnFactory conn_factory_;
  PikaDispatchHandles handles_;
  std::vector<PikaWorkerSpecificData*> workers_data_;
  pink::ServerThread* thread_rep_;

  PikaWorkerSpecificData* NewPikaWorkerSpecificData() {
    workers_data_.push_back(new PikaWorkerSpecificData());
    return workers_data_.back();
  }
};
#endif
