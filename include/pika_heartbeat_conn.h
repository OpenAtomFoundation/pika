// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_HEARTBEAT_CONN_H_
#define PIKA_HEARTBEAT_CONN_H_

#include "pink/include/pb_conn.h"

#include "include/pika_command.h"

class PikaHeartbeatConn : public pink::PbConn {
 public:
  PikaHeartbeatConn(int fd, std::string ip_port,
          pink::Thread* thread, void* worker_specific_data,
          pink::PinkEpoll* epoll);
  virtual ~PikaHeartbeatConn();

  virtual int DealMessage();
};

#endif
