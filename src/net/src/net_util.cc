// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#include "net/src/net_util.h"

#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include "net/include/net_define.h"

namespace net {

int Setnonblocking(int sockfd) {
  int flags;
  if ((flags = fcntl(sockfd, F_GETFL, 0)) < 0) {
    close(sockfd);
    return -1;
  }
  flags |= O_NONBLOCK;
  if (fcntl(sockfd, F_SETFL, flags) < 0) {
    close(sockfd);
    return -1;
  }
  return flags;
}

}  // namespace net
