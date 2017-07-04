// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_DISPATCH_THREAD_H_
#define PIKA_DISPATCH_THREAD_H_

#include "slash/include/env.h"
#include "pink/include/server_thread.h"
#include "pika_client_conn.h"
#include "pika_conf.h"
#include "pika_server.h"

extern PikaConf *g_pika_conf;
extern PikaServer* g_pika_server;

class PikaWorkerSpecificData {
 public:
  PikaWorkerSpecificData() {
    cmds_.reserve(300);
    InitCmdTable(&cmds_);
  }

  virtual ~PikaWorkerSpecificData() {
    DestoryCmdTable(cmds_);
  }

  Cmd* GetCmd(const std::string& opt) {
    return GetCmdFromTable(opt, cmds_);
  }

 private:
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

  class Handles : public pink::ServerHandle {
   public:
    explicit Handles(PikaDispatchThread* pika_disptcher)
        : pika_disptcher_(pika_disptcher) {
    }
    bool AccessHandle(std::string& ip) const override;

    void CronHandle() const override {
      pika_disptcher_->thread_rep_->set_keepalive_timeout(g_pika_conf->timeout());
      g_pika_server->ResetLastSecQuerynum();
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
  Handles handles_;
  std::vector<PikaWorkerSpecificData*> workers_data_;
  pink::ServerThread* thread_rep_;

  PikaWorkerSpecificData* NewPikaWorkerSpecificData() {
    workers_data_.push_back(new PikaWorkerSpecificData());
    return workers_data_.back();
  }
};
#endif
