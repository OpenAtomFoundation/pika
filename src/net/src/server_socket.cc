// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "net/include/net_define.h"
#include "net/src/net_util.h"
#include "net/src/server_socket.h"

namespace net {

ServerSocket::ServerSocket(int port, bool is_block)
    : port_(port),
      
      is_block_(is_block) {}

ServerSocket::~ServerSocket() { Close(); }

/*
 * Listen to a specific ip addr on a multi eth machine
 * Return 0 if Listen success, other wise
 */
int ServerSocket::Listen(const std::string& bind_ip) {
  int ret = 0;
  sockfd_ = socket(AF_INET, SOCK_STREAM, 0);
  memset(&servaddr_, 0, sizeof(servaddr_));

  int yes = 1;
  ret = setsockopt(sockfd_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
  if (ret < 0) {
    return kSetSockOptError;
  }

  servaddr_.sin_family = AF_INET;
  if (bind_ip.empty()) {
    servaddr_.sin_addr.s_addr = htonl(INADDR_ANY);
  } else {
    servaddr_.sin_addr.s_addr = inet_addr(bind_ip.c_str());
  }
  servaddr_.sin_port = htons(port_);

  fcntl(sockfd_, F_SETFD, fcntl(sockfd_, F_GETFD) | FD_CLOEXEC);

  ret = bind(sockfd_, reinterpret_cast<struct sockaddr*>(&servaddr_), sizeof(servaddr_));
  if (ret < 0) {
    return kBindError;
  }
  ret = listen(sockfd_, accept_backlog_);
  if (ret < 0) {
    return kListenError;
  }
  listening_ = true;

  if (!is_block_) {
    SetNonBlock();
  }
  return kSuccess;
}

int ServerSocket::SetNonBlock() {
  flags_ = Setnonblocking(sockfd());
  if (flags_ == -1) {
    return -1;
  }
  return 0;
}

void ServerSocket::Close() { close(sockfd_); }

}  // namespace net
