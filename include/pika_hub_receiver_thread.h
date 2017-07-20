// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_HUB_RECEIVER_THREAD_H_
#define PIKA_HUB_RECEIVER_THREAD_H_

#include <queue>
#include <set>

#include "pink/include/server_thread.h"
#include "slash/include/slash_mutex.h"
#include "slash/include/env.h"
#include "pika_define.h"
#include "pika_hub_conn.h"
#include "pika_command.h"

class PikaHubReceiverThread {
 public:
  PikaHubReceiverThread(const std::set<std::string> &ips, int port, int cron_interval = 0);
  ~PikaHubReceiverThread();

  int StartThread();

  // void KillBinlogSender();

  uint64_t GetnPlusSerial() {
    return serial_++;
  }

  Cmd* GetCmd(const std::string& opt) {
    return GetCmdFromTable(opt, cmds_);
  }

 private:
  class HubConnFactory : public pink::ConnFactory {
   public:
    explicit HubConnFactory(PikaHubReceiverThread* hub_receiver)
        : hub_receiver_(hub_receiver) {
    }

    virtual pink::PinkConn *NewPinkConn(
        int connfd,
        const std::string &ip_port,
        pink::ServerThread *thread,
        void* worker_specific_data) const override {
      return new PikaHubConn(connfd, ip_port, hub_receiver_);
    }

   private:
    PikaHubReceiverThread* hub_receiver_;
  };

  class Handles : public pink::ServerHandle {
   public:
    explicit Handles(PikaHubReceiverThread* hub_receiver)
        : hub_receiver_(hub_receiver) {
    }

    bool AccessHandle(std::string& ip) const override;
    void FdClosedHandle(int fd, const std::string& ip) const override;

   private:
    PikaHubReceiverThread* hub_receiver_;
  };

  HubConnFactory conn_factory_;
  Handles handles_;
  pink::ServerThread* thread_rep_;

  CmdTable cmds_;

  uint64_t serial_;
};

#endif
