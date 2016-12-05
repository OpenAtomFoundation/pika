// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_HEARTBEAT_CONN_H_
#define PIKA_HEARTBEAT_CONN_H_

#include <glog/logging.h>

#include "redis_conn.h"
#include "pink_thread.h"


class PikaHeartbeatThread;

class PikaHeartbeatConn: public pink::RedisConn {
public:
  PikaHeartbeatConn(int fd, std::string ip_port, pink::Thread *thread);
  virtual ~PikaHeartbeatConn();
  virtual int DealMessage();
private:
  PikaHeartbeatThread* pika_thread_;
};

#endif
