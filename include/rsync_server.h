// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <stdio.h>
#include <unistd.h>

#include "net/include/net_conn.h"
#include "net/include/net_thread.h"
#include "net/include/pb_conn.h"
#include "net/include/server_thread.h"
#include "net/include/thread_pool.h"
#include "net/src/holy_thread.h"
#include "net/src/net_multiplexer.h"
#include "pstd/include/env.h"
#include "pstd_hash.h"
#include "rsync_service.pb.h"
namespace rsync {
struct RsyncServerTaskArg {
  std::shared_ptr<RsyncService::RsyncRequest> req;
  std::shared_ptr<net::PbConn> conn;
  RsyncServerTaskArg(std::shared_ptr<RsyncService::RsyncRequest> _req, std::shared_ptr<net::PbConn> _conn)
      : req(std::move(_req)), conn(std::move(_conn)) {}
};
class RsyncReader;
class RsyncServerThread;

class RsyncServer {
public:
  RsyncServer(const std::set<std::string>& ips, const int port);
  ~RsyncServer();
  void Schedule(net::TaskFunc func, void* arg);
  int Start();
  int Stop();
private:
  std::unique_ptr<net::ThreadPool> work_thread_ = nullptr;
  std::unique_ptr<RsyncServerThread> rsync_server_thread_ = nullptr;
};

class RsyncServerConn : public net::PbConn {
public:
  RsyncServerConn(int connfd, const std::string& ip_port,
                  net::Thread* thread, void* worker_specific_data,
                  net::NetMultiplexer* mpx);
  virtual ~RsyncServerConn() override;
  int DealMessage() override;
  static void HandleMetaRsyncRequest(void* arg);
  static void HandleFileRsyncRequest(void* arg);
private:
  void* data_ = nullptr;
};

class RsyncServerThread : public net::HolyThread {
public:
  RsyncServerThread(const std::set<std::string>& ips, int port, int cron_internal, RsyncServer* arg);
  ~RsyncServerThread();

private:
  class RsyncServerConnFactory : public net::ConnFactory {
  public:
      explicit RsyncServerConnFactory(RsyncServer* sched) : scheduler_(sched) {}

      std::shared_ptr<net::NetConn> NewNetConn(int connfd, const std::string& ip_port,
                                              net::Thread* thread, void* worker_specific_data,
                                              net::NetMultiplexer* net) const override {
        return std::static_pointer_cast<net::NetConn>(
        std::make_shared<RsyncServerConn>(connfd, ip_port, thread, scheduler_, net));
      }
  private:
    RsyncServer* scheduler_ = nullptr;
  };
  class RsyncServerHandle : public net::ServerHandle {
  public:
    void FdClosedHandle(int fd, const std::string& ip_port) const override;
    void FdTimeoutHandle(int fd, const std::string& ip_port) const override;
    bool AccessHandle(int fd, std::string& ip) const override;
    void CronHandle() const override;
  };
private:
  RsyncServerConnFactory conn_factory_;
  RsyncServerHandle handle_;
};

} //end namespace rsync
