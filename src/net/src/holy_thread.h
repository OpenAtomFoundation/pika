// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_SRC_HOLY_THREAD_H_
#define NET_SRC_HOLY_THREAD_H_

#include <atomic>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "net/include/net_conn.h"
#include "net/include/server_thread.h"
#include "pstd/include/pstd_mutex.h"
#include "pstd/include/xdebug.h"

namespace net {
class NetConn;

class HolyThread : public ServerThread {
 public:
  // This type thread will listen and work self list redis thread
  HolyThread(int32_t port, ConnFactory* conn_factory, int32_t cron_interval = 0, const ServerHandle* handle = nullptr,
             bool async = true);
  HolyThread(const std::string& bind_ip, int32_t port, ConnFactory* conn_factory, int32_t cron_interval = 0,
             const ServerHandle* handle = nullptr, bool async = true);
  HolyThread(const std::set<std::string>& bind_ips, int32_t port, ConnFactory* conn_factory, int32_t cron_interval = 0,
             const ServerHandle* handle = nullptr, bool async = true);
  ~HolyThread() override;

  int32_t StartThread() override;

  int32_t StopThread() override;

  void set_keepalive_timeout(int32_t timeout) override { keepalive_timeout_ = timeout; }

  int32_t conn_num() const override;

  std::vector<ServerThread::ConnInfo> conns_info() const override;

  std::shared_ptr<NetConn> MoveConnOut(int32_t fd) override;

  void MoveConnIn(std::shared_ptr<NetConn> conn, const NotifyType& type) override {}

  void KillAllConns() override;

  bool KillConn(const std::string& ip_port) override;

  virtual std::shared_ptr<NetConn> get_conn(int32_t fd);

  void ProcessNotifyEvents(const net::NetFiredEvent* pfe) override;
  void Cleanup();

 private:
  mutable pstd::RWMutex rwlock_; /* For external statistics */
  std::map<int32_t, std::shared_ptr<NetConn>> conns_;

  ConnFactory* conn_factory_ = nullptr;
  void* private_data_ = nullptr;

  std::atomic<int32_t> keepalive_timeout_;  // keepalive second
  bool async_;

  void DoCronTask() override;

  pstd::Mutex killer_mutex_;
  std::set<std::string> deleting_conn_ipport_;

  void HandleNewConn(int32_t connfd, const std::string& ip_port) override;
  void HandleConnEvent(NetFiredEvent* pfe) override;

  void CloseFd(const std::shared_ptr<NetConn>& conn);
};  // class HolyThread

}  // namespace net
#endif  // NET_SRC_HOLY_THREAD_H_
