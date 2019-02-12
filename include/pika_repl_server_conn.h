// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_SERVER_CONN_H_
#define PIKA_REPL_SERVER_CONN_H_

#include "pink/include/pb_conn.h"

class PikaReplServerConn: public pink::PbConn {
 public:
  PikaReplServerConn(int fd, const std::string& ip_port, pink::Thread* thread, void* worker_specific_data);
  virtual ~PikaReplServerConn() = default;

  static void DoReplServerTask(void* arg);
  int DealMessage() override;
 private:
};

#endif
