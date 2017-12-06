// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <glog/logging.h>
#include "slash/include/slash_string.h"
#include "include/pika_heartbeat_conn.h"
#include "include/pika_server.h"

extern PikaServer *g_pika_server;

PikaHeartbeatConn::PikaHeartbeatConn(int fd, std::string ip_port)
      : RedisConn(fd, ip_port, NULL) {
}

int PikaHeartbeatConn::DealMessage(
    PikaCmdArgsType& argv, std::string* response) {
  if (argv[0] == "ping") {
    response->append("+PONG\r\n");
  } else if (argv[0] == "spci") {
    int64_t sid = -1;
    slash::string2l(argv[1].data(), argv[1].size(), &sid);
    g_pika_server->MayUpdateSlavesMap(sid, fd());
    response->append("+OK\r\n");
  } else {
    response->append("-ERR What the fuck are u sending\r\n");
  }
  return 0;
}
