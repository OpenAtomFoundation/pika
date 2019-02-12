// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_SERVER_H_
#define PIKA_REPL_SERVER_H_

#include "include/pika_repl_server_thread.h"

class PikaReplServer {
  public:
   PikaReplServer(const std::set<std::string>& ips, int port, int cron_interval);
   ~PikaReplServer();

   int Start();

  private:
   PikaReplServerThread* pika_repl_server_thread_;
};

#endif
