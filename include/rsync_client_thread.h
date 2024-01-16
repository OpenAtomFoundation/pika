// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef RSYNC_CLIENT_THREAD_H_
#define RSYNC_CLIENT_THREAD_H_

#include "net/include/client_thread.h"
#include "net/include/net_conn.h"
#include "net/include/pb_conn.h"
#include "rsync_service.pb.h"

using namespace pstd;
using namespace net;

namespace rsync {

class RsyncClientConn : public PbConn {
 public:
  RsyncClientConn(int fd, const std::string& ip_port, net::Thread* thread, void* cb_handler, NetMultiplexer* mpx);
  ~RsyncClientConn() override;
  int DealMessage() override;

 private:
  void* cb_handler_ = nullptr;
};

class RsyncClientConnFactory : public ConnFactory {
 public:
  RsyncClientConnFactory(void* scheduler) : cb_handler_(scheduler) {}
  std::shared_ptr<net::NetConn> NewNetConn(int connfd, const std::string& ip_port, net::Thread* thread,
                                           void* cb_handler, net::NetMultiplexer* net) const override {
    return std::static_pointer_cast<net::NetConn>(
        std::make_shared<RsyncClientConn>(connfd, ip_port, thread, cb_handler_, net));
  }

 private:
  void* cb_handler_ = nullptr;
};

class RsyncClientThread : public ClientThread {
 public:
  RsyncClientThread(int cron_interval, int keepalive_timeout, void* scheduler);
  ~RsyncClientThread() override;

 private:
  RsyncClientConnFactory conn_factory_;
  ClientHandle handle_;
};

}  // end namespace rsync
#endif
