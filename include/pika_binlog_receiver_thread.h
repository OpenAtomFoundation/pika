// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_BINLOG_RECEIVER_THREAD_H_
#define PIKA_BINLOG_RECEIVER_THREAD_H_

#include <queue>
#include <set>

#include "pink/include/server_thread.h"
#include "slash/include/slash_mutex.h"
#include "slash/include/env.h"
#include "include/pika_define.h"
#include "include/pika_binlog_receiver_conn.h"
#include "include/pika_master_conn.h"
#include "include/pika_command.h"
#include "include/pika_conf.h"

extern PikaConf* g_pika_conf;

class PikaBinlogReceiverThread {
 public:
  PikaBinlogReceiverThread(const std::set<std::string> &ips, int port, int cron_interval = 0);
  ~PikaBinlogReceiverThread();

  int StartThread();

  void KillBinlogSender();

  uint64_t GetnPlusSerial() {
    return serial_++;
  }

  Cmd* GetCmd(const std::string& opt) {
    return GetCmdFromTable(opt, cmds_);
  }

 private:
  class MasterConnFactory : public pink::ConnFactory {
   public:
    explicit MasterConnFactory(PikaBinlogReceiverThread* binlog_receiver)
        : binlog_receiver_(binlog_receiver) {
    }

    virtual std::shared_ptr<pink::PinkConn> NewPinkConn(
        int connfd,
        const std::string &ip_port,
        pink::Thread *thread,
        void* worker_specific_data,
        pink::PinkEpoll* pink_epoll) const override {
        if (g_pika_conf->identify_binlog_type() == "old") {
          LOG(INFO) << "Master conn factory create pika master conn";
          return std::make_shared<PikaMasterConn>(connfd, ip_port, binlog_receiver_);
        } else {
          LOG(INFO) << "Master conn factory creat pika binlog conn ip_port" << ip_port;
          return std::make_shared<PikaBinlogReceiverConn>(connfd, ip_port, binlog_receiver_);
        }
    }

   private:
    PikaBinlogReceiverThread* binlog_receiver_;
  };

  class Handles : public pink::ServerHandle {
   public:
    explicit Handles(PikaBinlogReceiverThread* binlog_receiver)
        : binlog_receiver_(binlog_receiver) {
    }

    using pink::ServerHandle::AccessHandle;
    bool AccessHandle(std::string& ip) const override;

   private:
    PikaBinlogReceiverThread* binlog_receiver_;
  };

  MasterConnFactory conn_factory_;
  Handles handles_;
  pink::ServerThread* thread_rep_;

  CmdTable cmds_;

  uint64_t serial_;
};
#endif
