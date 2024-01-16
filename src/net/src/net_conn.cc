// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <unistd.h>
#include <cstdio>

#include <glog/logging.h>

#include <utility>

#include "net/include/net_conn.h"
#include "net/include/net_thread.h"
#include "net/src/net_util.h"
#include "pstd/include/xdebug.h"

namespace net {

NetConn::NetConn(const int fd, std::string ip_port, Thread* thread, NetMultiplexer* net_mpx)
    : fd_(fd),
      ip_port_(std::move(ip_port)),
#ifdef __ENABLE_SSL
      ssl_(nullptr),
#endif
      thread_(thread),
      net_multiplexer_(net_mpx) {
  gettimeofday(&last_interaction_, nullptr);
}

#ifdef __ENABLE_SSL
NetConn::~NetConn() {
  SSL_free(ssl_);
  ssl_ = nullptr;
}
#endif

void NetConn::SetClose(bool close) { close_ = close; }

bool NetConn::SetNonblock() {
  flags_ = Setnonblocking(fd());
  return flags_ != -1;
}

#ifdef __ENABLE_SSL
bool NetConn::CreateSSL(SSL_CTX* ssl_ctx) {
  ssl_ = SSL_new(ssl_ctx);
  if (!ssl_) {
    LOG(WARNING) << "SSL_new() failed";
    return false;
  }

  if (SSL_set_fd(ssl_, fd_) == 0) {
    LOG(WARNING) << "SSL_set_fd() failed";
    return false;
  }

  SSL_set_accept_state(ssl_);

  return true;
}
#endif

}  // namespace net
