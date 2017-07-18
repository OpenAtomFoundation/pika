// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_MASTER_CONN_H_
#define PIKA_MASTER_CONN_H_

#include "pink/include/redis_conn.h"
#include "pika_command.h"

class PikaBinlogReceiverThread;

class PikaMasterConn: public pink::RedisConn {
 public:
  PikaMasterConn(int fd, std::string ip_port, void* worker_specific_data);
  virtual int DealMessage();
 private:
  PikaBinlogReceiverThread* binlog_receiver_;
};

#endif
