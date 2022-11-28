// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PINK_SRC_DISPATCH_THREAD_H_
#define PINK_SRC_DISPATCH_THREAD_H_

#include <set>
#include <map>
#include <queue>
#include <string>
#include <vector>

#include "slash/include/xdebug.h"
#include "pink/include/server_thread.h"
#include "pink/include/pink_conn.h"

namespace pink {

class PinkItem;
class PinkFiredEvent;
class WorkerThread;

class DispatchThread : public ServerThread {
 public:
  DispatchThread(int port,
                 int work_num, ConnFactory* conn_factory,
                 int cron_interval,
                 int queue_limit,
                 const ServerHandle* handle);
  DispatchThread(const std::string &ip, int port,
                 int work_num, ConnFactory* conn_factory,
                 int cron_interval,
                 int queue_limit,
                 const ServerHandle* handle);
  DispatchThread(const std::set<std::string>& ips, int port,
                 int work_num, ConnFactory* conn_factory,
                 int cron_interval,
                 int queue_limit,
                 const ServerHandle* handle);

  virtual ~DispatchThread();

  virtual int StartThread() override;

  virtual int StopThread() override;

  virtual void set_keepalive_timeout(int timeout) override;

  virtual int conn_num() const override;

  virtual std::vector<ServerThread::ConnInfo> conns_info() const override;

  virtual std::shared_ptr<PinkConn> MoveConnOut(int fd) override;

  virtual void MoveConnIn(std::shared_ptr<PinkConn> conn, const NotifyType& type) override;

  virtual void KillAllConns() override;

  virtual bool KillConn(const std::string& ip_port) override;

  void HandleNewConn(const int connfd, const std::string& ip_port) override;

  void SetQueueLimit(int queue_limit) override;
 private:
  /*
   * Here we used auto poll to find the next work thread,
   * last_thread_ is the last work thread
   */
  int last_thread_;
  int work_num_;
  /*
   * This is the work threads
   */
  WorkerThread** worker_thread_;
  int queue_limit_;
  std::map<WorkerThread*, void*> localdata_;

  void HandleConnEvent(PinkFiredEvent *pfe) override {
    UNUSED(pfe);
  }

  // No copying allowed
  DispatchThread(const DispatchThread&);
  void operator=(const DispatchThread&);
};  // class DispatchThread

}  // namespace pink
#endif  // PINK_SRC_DISPATCH_THREAD_H_
