// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_MONITOR_THREAD_H_
#define PIKA_MONITOR_THREAD_H_

#include <atomic>
#include <deque>
#include <list>
#include <queue>

#include "net/include/net_thread.h"
#include "pstd/include/pstd_mutex.h"

#include "include/pika_client_conn.h"
#include "include/pika_define.h"

class PikaMonitorThread : public net::Thread {
 public:
  PikaMonitorThread();
  virtual ~PikaMonitorThread();

  void AddMonitorClient(std::shared_ptr<PikaClientConn> client_ptr);
  void AddMonitorMessage(const std::string& monitor_message);
  int32_t ThreadClientList(std::vector<ClientInfo>* client = NULL);
  bool ThreadClientKill(const std::string& ip_port = "all");
  bool HasMonitorClients();

 private:
  void AddCronTask(MonitorCronTask task);
  bool FindClient(const std::string& ip_port);
  net::WriteStatus SendMessage(int32_t fd, std::string& message);
  void RemoveMonitorClient(const std::string& ip_port);

  std::atomic<bool> has_monitor_clients_;
  pstd::Mutex monitor_mutex_protector_;
  pstd::CondVar monitor_cond_;

  std::list<ClientInfo> monitor_clients_;
  std::deque<std::string> monitor_messages_;
  std::queue<MonitorCronTask> cron_tasks_;

  virtual void* ThreadMain();
  void RemoveMonitorClient(int32_t client_fd);
};
#endif
