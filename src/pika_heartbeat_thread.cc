// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_heartbeat_thread.h"

#include <glog/logging.h>

#include "slash/include/slash_mutex.h"

#include "include/pika_server.h"

extern PikaServer* g_pika_server;

PikaHeartbeatThread::PikaHeartbeatThread(std::set<std::string>& ips, int port,
                                         int cron_interval)
      : handles_(this) {
  thread_rep_ = NewHolyThread(ips, port, &conn_factory_, cron_interval, &handles_);
  thread_rep_->set_keepalive_timeout(20);
}

PikaHeartbeatThread::~PikaHeartbeatThread() {
  thread_rep_->StopThread();
  LOG(INFO) << "PikaHeartbeat thread " << thread_rep_->thread_id() << " exit!!!";
  delete thread_rep_;
}

int PikaHeartbeatThread::StartThread() {
  return thread_rep_->StartThread();
}

void PikaHeartbeatThread::Handles::CronHandle() const {
	struct timeval now;
	gettimeofday(&now, NULL);

  /*
   * find out stay STAGE_ONE too long
   * erase it in slaves_;
   */
  slash::MutexLock l(&g_pika_server->slave_mutex_);
  for (auto& slave : g_pika_server->slaves_) {
    DLOG(INFO) << "sid: " << slave.sid << " ip_port: " << slave.ip_port <<
      " port " << slave.port << " sender_tid: " << slave.sender_tid <<
      " hb_fd: " << slave.hb_fd << " stage :" << slave.stage <<
      " sender: " << slave.sender << " create_time: " << slave.create_time.tv_sec;

    if (slave.stage == SLAVE_ITEM_STAGE_ONE &&
        now.tv_sec - slave.create_time.tv_sec > 30) {
      // Kill BinlogSender
      LOG(WARNING) << "Erase slave stay STAGE_ONE too long: " <<
        slave.ip_port << " from slaves map of heartbeat thread";
      //TODO maybe bug here
      g_pika_server->slave_mutex_.Unlock();
      g_pika_server->DeleteSlave(slave.hb_fd);
      g_pika_server->slave_mutex_.Lock();
    }
  }
}

void PikaHeartbeatThread::Handles::FdClosedHandle(
        int fd, const std::string& ip_port) const {
  LOG(INFO) << "Find closed Slave: " << ip_port;
  g_pika_server->DeleteSlave(fd);
}

void PikaHeartbeatThread::Handles::FdTimeoutHandle(
        int fd, const std::string& ip_port) const {
  LOG(INFO) << "Find Timeout Slave: " << ip_port;
  g_pika_server->DeleteSlave(fd);
}

bool PikaHeartbeatThread::Handles::AccessHandle(std::string& ip) const {
  if (ip == "127.0.0.1") {
    ip = g_pika_server->host();
  }
  slash::MutexLock l(&g_pika_server->slave_mutex_);
  for (auto& slave : g_pika_server->slaves_) {
    if (slave.ip_port.find(ip) != std::string::npos) {
      LOG(INFO) << "HeartbeatThread access connection " << ip;
      return true;
    }
  }
  LOG(WARNING) << "HeartbeatThread deny connection: " << ip;
  return false;
}
