// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <stdio.h>
#include <unistd.h>

#include "pstd/include/xdebug.h"
#include "pink/include/pink_conn.h"
#include "pink/include/pink_thread.h"
#include "pink/src/pink_util.h"

namespace pink {

PinkConn::PinkConn(const int fd,
                   const std::string &ip_port,
                   Thread *thread,
                   PinkEpoll* pink_epoll)
    : fd_(fd),
      ip_port_(ip_port),
      is_reply_(false),
#ifdef __ENABLE_SSL
      ssl_(nullptr),
#endif
      thread_(thread),
      pink_epoll_(pink_epoll) {
  gettimeofday(&last_interaction_, nullptr);
}

PinkConn::~PinkConn() {
#ifdef __ENABLE_SSL
  SSL_free(ssl_);
  ssl_ = nullptr;
#endif
}

bool PinkConn::SetNonblock() {
  flags_ = Setnonblocking(fd());
  if (flags_ == -1) {
    return false;
  }
  return true;
}

#ifdef __ENABLE_SSL
bool PinkConn::CreateSSL(SSL_CTX* ssl_ctx) {
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

}  // namespace pink
