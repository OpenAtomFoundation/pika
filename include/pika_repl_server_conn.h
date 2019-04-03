// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_SERVER_CONN_H_
#define PIKA_REPL_SERVER_CONN_H_

#include <string>

#include "pink/include/pb_conn.h"
#include "pink/include/pink_thread.h"

#include "src/pika_inner_message.pb.h"

class PikaReplServerConn: public pink::PbConn {
 public:
  PikaReplServerConn(int fd, std::string ip_port, pink::Thread* thread, void* worker_specific_data, pink::PinkEpoll* epoll);
  virtual ~PikaReplServerConn();
  int DealMessage();
};

#endif  // INCLUDE_PIKA_REPL_SERVER_CONN_H_
