// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PINK_INCLUDE_PINK_CONN_H_
#define PINK_INCLUDE_PINK_CONN_H_

#include <sys/time.h>
#include <string>

#ifdef __ENABLE_SSL
#include <openssl/err.h>
#include <openssl/ssl.h>
#endif

#include "pink/include/pink_define.h"
#include "pink/include/server_thread.h"
#include "pink/src/pink_epoll.h"

namespace pink {

class Thread;

class PinkConn : public std::enable_shared_from_this<PinkConn> {
 public:
  PinkConn(const int fd, const std::string &ip_port, Thread *thread, PinkEpoll* pink_epoll = nullptr);
  virtual ~PinkConn();

  /*
   * Set the fd to nonblock && set the flag_ the the fd flag
   */
  bool SetNonblock();

#ifdef __ENABLE_SSL
  bool CreateSSL(SSL_CTX* ssl_ctx);
#endif

  virtual ReadStatus GetRequest() = 0;
  virtual WriteStatus SendReply() = 0;
  virtual int WriteResp(const std::string& resp) {
    return 0;
  }

  virtual void TryResizeBuffer() {}

  int flags() const {
    return flags_;
  }

  void set_fd(const int fd) {
    fd_ = fd;
  }

  int fd() const {
    return fd_;
  }

  std::string ip_port() const {
    return ip_port_;
  }

  bool is_ready_to_reply() {
    return is_writable() && is_reply();
  }

  virtual void set_is_writable(const bool is_writable) {
    is_writable_ = is_writable;
  }

  virtual bool is_writable() {
    return is_writable_;
  }

  virtual void set_is_reply(const bool is_reply) {
    is_reply_ = is_reply;
  }

  virtual bool is_reply() {
    return is_reply_;
  }

  bool IsClose() { return close_; }
  void SetClose(bool close) { close_ = close; }

  void set_last_interaction(const struct timeval &now) {
    last_interaction_ = now;
  }

  struct timeval last_interaction() const {
    return last_interaction_;
  }

  Thread *thread() const {
    return thread_;
  }

  void set_pink_epoll(PinkEpoll* ep) {
    pink_epoll_ = ep;
  }

  PinkEpoll* pink_epoll() const {
    return pink_epoll_;
  }

#ifdef __ENABLE_SSL
  SSL* ssl() {
    return ssl_;
  }

  bool security() {
    return ssl_ != nullptr;
  }
#endif

 private:
  int fd_;
  std::string ip_port_;
  bool is_reply_;
  bool is_writable_;
  bool close_;
  struct timeval last_interaction_;
  int flags_;

#ifdef __ENABLE_SSL
  SSL* ssl_;
#endif

  // thread this conn belong to
  Thread *thread_;
  // the pink epoll this conn belong to
  PinkEpoll *pink_epoll_;

  /*
   * No allowed copy and copy assign operator
   */
  PinkConn(const PinkConn&);
  void operator=(const PinkConn&);
};


/*
 * for every conn, we need create a corresponding ConnFactory
 */
class ConnFactory {
 public:
  virtual ~ConnFactory() {}
  virtual std::shared_ptr<PinkConn> NewPinkConn(
    int connfd,
    const std::string &ip_port,
    Thread *thread,
    void* worker_private_data, /* Has set in ThreadEnvHandle */
    PinkEpoll* pink_epoll = nullptr) const = 0;
};

}  // namespace pink

#endif  // PINK_INCLUDE_PINK_CONN_H_
