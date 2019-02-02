// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_CLIENT_H_
#define PIKA_REPL_CLIENT_H_

#include <string>
#include <memory>

#include "include/pika_binlog_reader.h"
#include "include/pika_repl_client_conn.h"

#include "pink/include/pink_conn.h"
#include "pink/include/client_thread.h"
#include "pink/include/thread_pool.h"
#include "slash/include/slash_status.h"

class ReplClientHandle : public pink::ClientHandle {
 public:
  //  void CronHandle() const override {
  //    std::cout << "HandleCronHandle" << std::endl;
  //  }
  //  void FdTimeoutHandle(int fd, const std::string& ip_port) const override {
  //    std::cout << "FdTimeoutHandle ip_port" << std::endl;
  //  }
  //  void FdClosedHandle(int fd, const std::string& ip_port) const override {
  //    std::cout << "FdClosedHandle " << fd << " ip_port " << ip_port << std::endl;
  //  }
  //  bool AccessHandle(std::string& ip) const override {
  //    // ban 127.0.0.1 if you want to test this routine
  //    if (ip.find("127.0.0.2") != std::string::npos) {
  //      std::cout << "AccessHandle " << ip << std::endl;
  //      return false;
  //    }
  //    return true;
  //  }
  //  int CreateWorkerSpecificData(void** data) const override {
  //    return 0;
  //  }
  //  int DeleteWorkerSpecificData(void* data) const override {
  //    return 0;
  //  }
};

#include "slash/include/slash_status.h"

#include "src/pika_inner_message.pb.h"

#define kBinlogSyncBatchNum 10

using slash::Status;

class ReplClientHandle : public pink::ClientHandle {
 public:
  //  void CronHandle() const override {
  //    std::cout << "HandleCronHandle" << std::endl;
  //  }
  //  void FdTimeoutHandle(int fd, const std::string& ip_port) const override {
  //    std::cout << "FdTimeoutHandle ip_port" << std::endl;
  //  }
  //  void FdClosedHandle(int fd, const std::string& ip_port) const override {
  //    std::cout << "FdClosedHandle " << fd << " ip_port " << ip_port << std::endl;
  //  }
  //  bool AccessHandle(std::string& ip) const override {
  //    // ban 127.0.0.1 if you want to test this routine
  //    if (ip.find("127.0.0.2") != std::string::npos) {
  //      std::cout << "AccessHandle " << ip << std::endl;
  //      return false;
  //    }
  //    return true;
  //  }
  //  int CreateWorkerSpecificData(void** data) const override {
  //    return 0;
  //  }
  //  int DeleteWorkerSpecificData(void* data) const override {
  //    return 0;
  //  }
};

struct RmNode {
  std::string table_;
  int partition_;
  std::string ip_;
  int port_;
  RmNode(const std::string& table, int partition, const std::string& ip, int port) : table_(table), partition_(partition), ip_(ip), port_(port) {
  }
};

class PikaReplClient {
 public:
  PikaReplClient(int cron_interval, int keepalive_timeout);
  ~PikaReplClient();
  slash::Status Write(const std::string& ip, const int port, const std::string& msg);
  //void ThreadPollSchedule(pink::TaskFunc func, void*arg);
  int Start();
  Status AddBinlogReader(const RmNode& slave, std::shared_ptr<Binlog> logger, uint32_t filenum, uint64_t offset);
  void RunStateMachine(const RmNode& slave);
  bool NeedToSendBinlog(const RmNode& slave);
 private:
  class ReplClientConnFactory : public pink::ConnFactory {
   public:
    virtual std::shared_ptr<pink::PinkConn> NewPinkConn(
        int connfd,
        const std::string &ip_port,
        pink::Thread *thread,
        void* worker_specific_data,
        pink::PinkEpoll* pink_epoll) const override {
      return std::make_shared<PikaReplClientConn>(connfd, ip_port, thread, worker_specific_data);
    }
  };

  PikaBinlogReader* NewPikaBinlogReader(std::shared_ptr<Binlog> logger, uint32_t filenum, uint64_t offset);

  Status TrySyncBinlog(const RmNode& slave, bool authed);
  void BuildAuthPb(const RmNode& slave, InnerMessage::InnerRequest& request);
  void BuildBinlogPb(const RmNode& slave, const std::string& msg, uint32_t filenum, uint64_t offset, InnerMessage::InnerRequest& request);

  Status BuildAuthMsg(std::string* scratch);
  Status BuildBinlogMsgFromFile(PikaBinlogReader* binlog_reader, std::string* scratch, uint32_t* filenum, uint64_t* offset);

  ReplClientConnFactory conn_factory_;
  int cron_interval_;
  int keepalive_timeout_;
  pink::ClientThread* client_thread_;
  pink::ClientHandle* handle_;
  // keys of this map: table_partition_slaveip:port
  std::map<std::string, PikaBinlogReader*> slave_binlog_readers_;
};

#endif
