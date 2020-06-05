// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_PROXY_H_
#define PIKA_PROXY_H_

#include "include/pika_proxy_conn.h"
#include "include/pika_proxy_cli.h"
#include "include/pika_client_conn.h"

class ProxyCliManager {
 public:
  ProxyCliManager(int conn_every_backend, int keepalive_time);
  ~ProxyCliManager();
  int Start();
  int Stop();
  // no need mutex lock here
  Status ChooseForwardToBackend(ProxyTask* task);

 private:
  Status ForwardNextAvailableConn(ProxyTask* task);
  std::vector<std::shared_ptr<ProxyCli>> clis_;
  std::atomic<uint64_t> rr_counter_;
  int conn_every_backend_;
};

class PikaProxy {
 public:
  PikaProxy();
  ~PikaProxy();
  int Start();
  int Stop();
  // write to client_thread and put it into task_queue
  static void ForwardToBackend(void* arg);
  // grep task_queue and
  static void WritebackToCliConn(void* arg);
  // bypass to g_pika_server
  void ScheduleForwardToBackend(
      const std::shared_ptr<PikaClientConn>& conn_ptr,
      const std::vector<pink::RedisCmdArgsType>& redis_cmds,
      const std::vector<Node>& dst);
  void MayScheduleWritebackToCliConn(std::shared_ptr<PikaProxyConn> conn_ptr,
      std::shared_ptr<ProxyCli> cli, const std::string res);
  std::shared_ptr<ProxyCliManager> cli_manager() {
    return cli_manager_ptr_;
  }

 private:
  std::shared_ptr<ProxyCliManager> cli_manager_ptr_;
};

#endif  // PIKA_PROXY_H_
