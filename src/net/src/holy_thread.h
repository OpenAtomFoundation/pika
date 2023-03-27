// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_SRC_HOLY_THREAD_H_
#define NET_SRC_HOLY_THREAD_H_

#include <map>
#include <set>
#include <string>
#include <atomic>
#include <vector>

#include "pstd/include/xdebug.h"
#include "pstd/include/pstd_mutex.h"
#include "net/include/server_thread.h"
#include "net/include/net_conn.h"

namespace net {
class NetConn;

class HolyThread: public ServerThread {
 public:
  // This type thread thread will listen and work self list redis thread
  HolyThread(int port, ConnFactory* conn_factory,
             int cron_interval = 0, const ServerHandle* handle = nullptr, bool async = true);
  HolyThread(const std::string& bind_ip, int port,
             ConnFactory* conn_factory,
             int cron_interval = 0, const ServerHandle* handle = nullptr, bool async = true);
  HolyThread(const std::set<std::string>& bind_ips, int port,
             ConnFactory* conn_factory,
             int cron_interval = 0, const ServerHandle* handle = nullptr, bool async = true);
  virtual ~HolyThread();

  virtual int StartThread() override;

  virtual int StopThread() override;

  virtual void set_keepalive_timeout(int timeout) override {
    keepalive_timeout_ = timeout;
  }

  virtual int conn_num() const override;

  virtual std::vector<ServerThread::ConnInfo> conns_info() const override;

  virtual std::shared_ptr<NetConn> MoveConnOut(int fd) override;

  virtual void MoveConnIn(std::shared_ptr<NetConn> conn, const NotifyType& type) override { }

  virtual void KillAllConns() override;

  virtual bool KillConn(const std::string& ip_port) override;

  virtual std::shared_ptr<NetConn> get_conn(int fd);

  void ProcessNotifyEvents(const net::NetFiredEvent* pfe) override;

 private:
  mutable pstd::RWMutex rwlock_; /* For external statistics */
  std::map<int, std::shared_ptr<NetConn>> conns_;

  ConnFactory *conn_factory_;
  void* private_data_;

  std::atomic<int> keepalive_timeout_;  // keepalive second
  bool async_;

  void DoCronTask() override;

  pstd::Mutex killer_mutex_;
  std::set<std::string> deleting_conn_ipport_;

  void HandleNewConn(int connfd, const std::string &ip_port) override;
  void HandleConnEvent(NetFiredEvent *pfe) override;

  void CloseFd(std::shared_ptr<NetConn> conn);
  void Cleanup();
};  // class HolyThread


}  // namespace net
#endif  // NET_SRC_HOLY_THREAD_H_
