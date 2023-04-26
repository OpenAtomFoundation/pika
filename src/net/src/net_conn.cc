// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <stdio.h>
#include <unistd.h>

#include "net/include/net_conn.h"
#include "net/include/net_thread.h"
#include "net/src/net_util.h"
#include "pstd/include/xdebug.h"

namespace net {

NetConn::NetConn(const NetID id, const int fd, const std::string& ip_port, Thread* thread, NetMultiplexer* net_mpx)
    : id_(id),
      fd_(fd),
      ip_port_(ip_port),
      is_reply_(false),
#ifdef __ENABLE_SSL
      ssl_(nullptr),
#endif
      thread_(thread),
      net_multiplexer_(net_mpx) {
  gettimeofday(&last_interaction_, nullptr);
}

NetConn::~NetConn() {
#ifdef __ENABLE_SSL
  SSL_free(ssl_);
  ssl_ = nullptr;
#endif
}

void NetConn::SetClose(bool close) { close_ = close; }

bool NetConn::SetNonblock() {
  flags_ = Setnonblocking(fd());
  if (flags_ == -1) {
    return false;
  }
  return true;
}

#ifdef __ENABLE_SSL
bool NetConn::CreateSSL(SSL_CTX* ssl_ctx) {
  ssl_ = SSL_new(ssl_ctx);
  if (!ssl_) {
    log_warn("SSL_new() failed");
    return false;
  }

  if (SSL_set_fd(ssl_, fd_) == 0) {
    log_warn("SSL_set_fd() failed");
    return false;
  }

  SSL_set_accept_state(ssl_);

  return true;
}
#endif

}  // namespace net
