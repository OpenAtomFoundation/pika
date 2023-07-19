// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_SRC_SERVER_SOCKET_H_
#define NET_SRC_SERVER_SOCKET_H_

#include <netinet/in.h>
#include <sys/socket.h>

#include <iostream>
#include <string>

#include "pstd/include/noncopyable.h"

namespace net {

class ServerSocket : public pstd::noncopyable {
 public:
  explicit ServerSocket(int port, bool is_block = false);

  virtual ~ServerSocket();

  /*
   * Listen to a specific ip addr on a multi eth machine
   * Return 0 if Listen success, <0 other wise
   */
  int Listen(const std::string& bind_ip = std::string());

  void Close();

  /*
   * The get and set functions
   */
  void set_port(int port) { port_ = port; }

  int port() { return port_; }

  void set_keep_alive(bool keep_alive) { keep_alive_ = keep_alive; }
  bool keep_alive() const { return keep_alive_; }

  void set_send_timeout(int send_timeout) { send_timeout_ = send_timeout; }
  int send_timeout() const { return send_timeout_; }

  void set_recv_timeout(int recv_timeout) { recv_timeout_ = recv_timeout; }

  int recv_timeout() const { return recv_timeout_; }

  int sockfd() const { return sockfd_; }

  void set_sockfd(int sockfd) { sockfd_ = sockfd; }

 private:
  int SetNonBlock();
  /*
   * The tcp server port and address
   */
  int port_;
  int flags_;
  int send_timeout_{0};
  int recv_timeout_{0};
  int accept_timeout_{0};
  int accept_backlog_{1024};
  int tcp_send_buffer_{0};
  int tcp_recv_buffer_{0};
  bool keep_alive_{false};
  bool listening_{false};
  bool is_block_;

  struct sockaddr_in servaddr_;
  int sockfd_;

};

}  // namespace net

#endif  // NET_SRC_SERVER_SOCKET_H_
