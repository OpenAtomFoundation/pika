// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_repl_server_thread.h"

PikaReplServerThread::PikaReplServerThread(const std::set<std::string>& ips,
                                           int port,
                                           int cron_interval) :
  HolyThread(ips, port, &conn_factory_, cron_interval, &handles_, true),
  conn_factory_(this),
  serial_(0) {
}

void PikaReplServerThread::Handles::CronHandle() const {
}

bool PikaReplServerThread::Handles::AccessHandle(std::string& ip) const {
  return true;
}
