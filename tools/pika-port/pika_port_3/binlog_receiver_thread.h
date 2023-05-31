// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef BINLOG_RECEIVER_THREAD_H_
#define BINLOG_RECEIVER_THREAD_H_

#include <queue>
#include <string>

#include "master_conn.h"
#include "net/include/server_thread.h"
#include "pika_define.h"
#include "pstd/include/pstd_mutex.h"

class BinlogReceiverThread {
 public:
  BinlogReceiverThread(const std::string& host, int port, int cron_interval = 0);
  virtual ~BinlogReceiverThread();
  int StartThread();

  void KillBinlogSender();

  // uint64_t GetnPlusSerial() {
  //   return serial_++;
  // }

  // Cmd* GetCmd(const std::string& opt) {
  //   return GetCmdFromDB(opt, cmds_);
  // }

 private:
  class MasterConnFactory : public net::ConnFactory {
   public:
    explicit MasterConnFactory(BinlogReceiverThread* binlog_receiver) : binlog_receiver_(binlog_receiver) {}

    virtual std::shared_ptr<net::NetConn> NewNetConn(int connfd, const std::string& ip_port, net::Thread* thread,
                                     void* worker_specific_data, 
                                     net::NetMultiplexer* net_mpx = nullptr) const override {
      return std::static_pointer_cast<net::NetConn>(std::make_shared<MasterConn>(connfd, ip_port, binlog_receiver_));
    }

   private:
    BinlogReceiverThread* binlog_receiver_;
  };

  class Handles : public net::ServerHandle {
   public:
    explicit Handles(BinlogReceiverThread* binlog_receiver) : binlog_receiver_(binlog_receiver) {}

    bool AccessHandle(std::string& ip) const override;

   private:
    BinlogReceiverThread* binlog_receiver_;
  };

  MasterConnFactory conn_factory_;
  Handles handles_;
  net::ServerThread* thread_rep_;

  // CmdDB cmds_;
  // uint64_t serial_;
};
#endif
