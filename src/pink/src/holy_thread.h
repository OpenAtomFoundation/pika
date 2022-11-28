// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PINK_SRC_HOLY_THREAD_H_
#define PINK_SRC_HOLY_THREAD_H_

#include <map>
#include <set>
#include <string>
#include <atomic>
#include <vector>

#include "slash/include/xdebug.h"
#include "slash/include/slash_mutex.h"
#include "pink/include/server_thread.h"
#include "pink/include/pink_conn.h"

namespace pink {
class PinkConn;

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

  virtual std::shared_ptr<PinkConn> MoveConnOut(int fd) override;

  virtual void MoveConnIn(std::shared_ptr<PinkConn> conn, const NotifyType& type) override { }

  virtual void KillAllConns() override;

  virtual bool KillConn(const std::string& ip_port) override;

  virtual std::shared_ptr<PinkConn> get_conn(int fd);

  void ProcessNotifyEvents(const pink::PinkFiredEvent* pfe) override;

 private:
  mutable slash::RWMutex rwlock_; /* For external statistics */
  std::map<int, std::shared_ptr<PinkConn>> conns_;

  ConnFactory *conn_factory_;
  void* private_data_;

  std::atomic<int> keepalive_timeout_;  // keepalive second
  bool async_;

  void DoCronTask() override;

  slash::Mutex killer_mutex_;
  std::set<std::string> deleting_conn_ipport_;

  void HandleNewConn(int connfd, const std::string &ip_port) override;
  void HandleConnEvent(PinkFiredEvent *pfe) override;

  void CloseFd(std::shared_ptr<PinkConn> conn);
  void Cleanup();
};  // class HolyThread


}  // namespace pink
#endif  // PINK_SRC_HOLY_THREAD_H_
