// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_INCLUDE_NET_CONN_H_
#define NET_INCLUDE_NET_CONN_H_

#include <sys/time.h>
#include <string>

#ifdef __ENABLE_SSL
#  include <openssl/err.h>
#  include <openssl/ssl.h>
#endif

#include "net/include/net_define.h"
#include "net/include/server_thread.h"
#include "net/src/net_multiplexer.h"

namespace net {

class Thread;

class NetConn : public std::enable_shared_from_this<NetConn> {
 public:
  NetConn(const int fd, const std::string& ip_port, Thread* thread, NetMultiplexer* mpx = nullptr);
  virtual ~NetConn();

  /*
   * Set the fd to nonblock && set the flag_ the the fd flag
   */
  bool SetNonblock();

#ifdef __ENABLE_SSL
  bool CreateSSL(SSL_CTX* ssl_ctx);
#endif

  virtual ReadStatus GetRequest() = 0;
  virtual WriteStatus SendReply() = 0;
  virtual int WriteResp(const std::string& resp) { return 0; }

  virtual void TryResizeBuffer() {}

  int flags() const { return flags_; }

  void set_fd(const int fd) { fd_ = fd; }

  int fd() const { return fd_; }

  std::string ip_port() const { return ip_port_; }

  bool is_ready_to_reply() { return is_writable() && is_reply(); }

  virtual void set_is_writable(const bool is_writable) { is_writable_ = is_writable; }

  virtual bool is_writable() { return is_writable_; }

  virtual void set_is_reply(const bool is_reply) { is_reply_ = is_reply; }

  virtual bool is_reply() { return is_reply_; }

  std::string name() { return name_; }
  void set_name(std::string name) { name_ = std::move(name); }

  bool IsClose() { return close_; }
  void SetClose(bool close) { close_ = close; }

  void set_last_interaction(const struct timeval& now) { last_interaction_ = now; }

  struct timeval last_interaction() const { return last_interaction_; }

  Thread* thread() const { return thread_; }

  void set_net_multiplexer(NetMultiplexer* ep) { net_multiplexer_ = ep; }

  NetMultiplexer* net_multiplexer() const { return net_multiplexer_; }

#ifdef __ENABLE_SSL
  SSL* ssl() { return ssl_; }

  bool security() { return ssl_ != nullptr; }
#endif

 private:
  int fd_;
  std::string ip_port_;
  bool is_reply_;
  bool is_writable_;
  bool close_;
  struct timeval last_interaction_;
  int flags_;
  std::string name_;

#ifdef __ENABLE_SSL
  SSL* ssl_;
#endif

  // thread this conn belong to
  Thread* thread_;
  // the net epoll this conn belong to
  NetMultiplexer* net_multiplexer_;

  /*
   * No allowed copy and copy assign operator
   */
  NetConn(const NetConn&);
  void operator=(const NetConn&);
};

/*
 * for every conn, we need create a corresponding ConnFactory
 */
class ConnFactory {
 public:
  virtual ~ConnFactory() {}
  virtual std::shared_ptr<NetConn> NewNetConn(int connfd, const std::string& ip_port, Thread* thread,
                                              void* worker_private_data, /* Has set in ThreadEnvHandle */
                                              NetMultiplexer* net_mpx = nullptr) const = 0;
};

}  // namespace net

#endif  // NET_INCLUDE_NET_CONN_H_
