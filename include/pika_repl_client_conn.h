// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_CLIENT_CONN_H_
#define PIKA_REPL_CLIENT_CONN_H_

#include <vector>
#include <string>

#include "pink/include/pb_conn.h"
#include "pink/include/pink_thread.h"

class PikaReplClientConn: public pink::PbConn {
 public:
  PikaReplClientConn(int fd, const std::string& ip_port, pink::Thread *thread, void* worker_specific_data);
  virtual ~PikaReplClientConn() = default;

  static void DoReplClientTask(void* arg);
  int DealMessage() override;
 private:
};

#endif
