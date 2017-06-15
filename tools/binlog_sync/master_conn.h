// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef MASTER_CONN_H_
#define MASTER_CONN_H_

#include "pink/include/redis_conn.h"
#include "pink/include/pink_thread.h"
#include "pika_command.h"

class BinlogReceiverThread;

class MasterConn: public pink::RedisConn {
public:
  MasterConn(int fd, std::string ip_port, pink::Thread *thread);
  virtual ~MasterConn();
  virtual int DealMessage();
private:
  BinlogReceiverThread* self_thread_;
  void RestoreArgs();
  std::string raw_args_;
};

#endif
