// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_repl_server_thread.h"

#include "include/pika_rm.h"
#include "include/pika_server.h"

extern PikaServer* g_pika_server;
extern PikaReplicaManager* g_pika_rm;

PikaReplServerThread::PikaReplServerThread(const std::set<std::string>& ips,
                                           int port,
                                           int cron_interval) :
  HolyThread(ips, port, &conn_factory_, cron_interval, &handle_, true),
  conn_factory_(this),
  port_(port),
  serial_(0) {
  set_keepalive_timeout(180);
}

int PikaReplServerThread::ListenPort() {
  return port_;
}

void PikaReplServerThread::ReplServerHandle::FdClosedHandle(int fd, const std::string& ip_port) const {
  LOG(INFO) << "ServerThread Close Slave Conn, fd: " << fd << ", ip_port: " << ip_port;
  g_pika_server->DeleteSlave(fd);
  g_pika_rm->ReplServerRemoveClientConn(fd);
}
