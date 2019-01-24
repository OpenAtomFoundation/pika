// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_SERVER_THREAD_H_
#define PIKA_REPL_SERVER_THREAD_H_

#include <map>
#include <string>
#include <set>

#include "pink/src/holy_thread.h"
#include "pink/include/pink_conn.h"

class PikaReplServerThread: public pink::HolyThread {
 public:
  PikaReplServerThread(const std::set<std::string> &ips, int port, pink::ConnFactory* conn_factory, int cron_interval, const pink::ServerHandle* handle);
  virtual ~PikaReplServerThread() = default;
  void Write(const std::string& msg, const std::string& ip_port, int fd);
 private:
  void NotifyWrite(const std::string& ip_port, int fd);
  virtual void ProcessNotifyEvents(const pink::PinkFiredEvent* pfe) override;
  slash::Mutex write_buf_mu_;
  std::map<int, std::string> write_buf_;
};

#endif
