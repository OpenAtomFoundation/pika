// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <stdio.h>
#include <sys/epoll.h>
#include <stdlib.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <string.h>

#include "pink/src/server_socket.h"
#include "pink/src/pink_util.h"
#include "pink/include/pink_define.h"

namespace pink {

ServerSocket::ServerSocket(int port, bool is_block)
    : port_(port),
      send_timeout_(0),
      recv_timeout_(0),
      accept_timeout_(0),
      accept_backlog_(1024),
      tcp_send_buffer_(0),
      tcp_recv_buffer_(0),
      keep_alive_(false),
      listening_(false),
  is_block_(is_block) {
}

ServerSocket::~ServerSocket() {
  Close();
}

/*
 * Listen to a specific ip addr on a multi eth machine
 * Return 0 if Listen success, other wise
 */
int ServerSocket::Listen(const std::string &bind_ip) {
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

  ret = bind(sockfd_, (struct sockaddr *) &servaddr_, sizeof(servaddr_));
  if (ret < 0) {
    return kBindError;
  }
  ret = listen(sockfd_, accept_backlog_);
  if (ret < 0) {
    return kListenError;
  }
  listening_ = true;

  if (is_block_ == false) {
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

void ServerSocket::Close() {
  close(sockfd_);
}

}  // namespace pink
