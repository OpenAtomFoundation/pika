// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_repl_client_thread.h"

#include "include/pika_server.h"

#include "slash/include/slash_string.h"

extern PikaServer* g_pika_server;

PikaReplClientThread::PikaReplClientThread(int cron_interval, int keepalive_timeout) :
  ClientThread(&conn_factory_, cron_interval, keepalive_timeout, &handle_, NULL) {
}

void PikaReplClientThread::ReplClientHandle::FdClosedHandle(int fd, const std::string& ip_port) const {
  std::string ip;
  int port = 0;
  if (!slash::ParseIpPortString(ip_port, ip, port)) {
    LOG(WARNING) << "Parse ip_port error " << ip_port;
  }
  g_pika_server->DeleteSlave(ip, port);
};

void PikaReplClientThread::ReplClientHandle::FdTimeoutHandle(int fd, const std::string& ip_port) const {
  LOG(INFO) << "slave " << ip_port << " " << fd << " timeout";
};
