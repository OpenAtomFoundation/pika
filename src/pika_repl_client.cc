// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_repl_client.h"

#include <glog/logging.h>

PikaReplClient::PikaReplClient(int cron_interval, int keepalive_timeout)
    : cron_interval_(cron_interval),
      keepalive_timeout_(keepalive_timeout) {
  client_thread_ = new pink::ClientThread(&conn_factory_, cron_interval_, keepalive_timeout_);
  client_thread_->set_thread_name("PikaReplClient");
}

PikaReplClient::~PikaReplClient() {
  delete client_thread_;
}

int PikaReplClient::Start() {
  int res = client_thread_->StartThread();
  if (res != pink::kSuccess) {
    LOG(FATAL) << "Start ReplClient ClientThread Error: " << res << (res == pink::kCreateThreadError ? ": create thread error " : ": other error");
  }
  return res;
}

void PikaReplClient::Write(const std::string& ip, const int port, const std::string& msg) {
  client_thread_->Write(ip, port, msg);
}
