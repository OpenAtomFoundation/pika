// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_repl_client_thread.h"

#include "include/pika_rm.h"
#include "include/pika_server.h"

#include "pstd/include/pstd_string.h"

extern PikaServer* g_pika_server;
extern std::unique_ptr<PikaReplicaManager> g_pika_rm;

PikaReplClientThread::PikaReplClientThread(int32_t cron_interval, int32_t keepalive_timeout)
    : ClientThread(&conn_factory_, cron_interval, keepalive_timeout, &handle_, nullptr) {}

void PikaReplClientThread::ReplClientHandle::FdClosedHandle(int32_t fd, const std::string& ip_port) const {
  LOG(INFO) << "ReplClient Close conn, fd=" << fd << ", ip_port=" << ip_port;
  std::string ip;
  int32_t port = 0;
  if (!pstd::ParseIpPortString(ip_port, ip, port)) {
    LOG(WARNING) << "Parse ip_port error " << ip_port;
    return;
  }
  if (ip == g_pika_server->master_ip() && port == g_pika_server->master_port() + kPortShiftReplServer &&
      PIKA_REPL_ERROR != g_pika_server->repl_state()) {  // if state machine in error state, no retry
    LOG(WARNING) << "Master conn disconnect : " << ip_port << " try reconnect";
    g_pika_server->ResetMetaSyncStatus();
  }
  g_pika_server->UpdateMetaSyncTimestamp();
};

void PikaReplClientThread::ReplClientHandle::FdTimeoutHandle(int32_t fd, const std::string& ip_port) const {
  LOG(INFO) << "ReplClient Timeout conn, fd=" << fd << ", ip_port=" << ip_port;
  std::string ip;
  int32_t port = 0;
  if (!pstd::ParseIpPortString(ip_port, ip, port)) {
    LOG(WARNING) << "Parse ip_port error " << ip_port;
    return;
  }
  if (ip == g_pika_server->master_ip() && port == g_pika_server->master_port() + kPortShiftReplServer &&
      PIKA_REPL_ERROR != g_pika_server->repl_state() &&
      PikaReplicaManager::CheckSlaveSlotState(ip, port)) {  // if state machine in error state, no retry
    LOG(WARNING) << "Master conn timeout : " << ip_port << " try reconnect";
    g_pika_server->ResetMetaSyncStatus();
  }
  g_pika_server->UpdateMetaSyncTimestamp();
};
