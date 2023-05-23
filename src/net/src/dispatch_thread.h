// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_SRC_DISPATCH_THREAD_H_
#define NET_SRC_DISPATCH_THREAD_H_

#include <map>
#include <queue>
#include <set>
#include <string>
#include <vector>

#include "net/include/net_conn.h"
#include "net/include/server_thread.h"
#include "pstd/include/xdebug.h"

namespace net {

class NetItem;
class NetFiredEvent;
class WorkerThread;

class DispatchThread : public ServerThread, public pstd::noncopyable {
 public:
  DispatchThread(int port, int work_num, ConnFactory* conn_factory, int cron_interval, int queue_limit,
                 const ServerHandle* handle);
  DispatchThread(const std::string& ip, int port, int work_num, ConnFactory* conn_factory, int cron_interval,
                 int queue_limit, const ServerHandle* handle);
  DispatchThread(const std::set<std::string>& ips, int port, int work_num, ConnFactory* conn_factory, int cron_interval,
                 int queue_limit, const ServerHandle* handle);

  ~DispatchThread() override;

  int StartThread() override;

  int StopThread() override;

  void set_keepalive_timeout(int timeout) override;

  int conn_num() const override;

  std::vector<ServerThread::ConnInfo> conns_info() const override;

  std::shared_ptr<NetConn> MoveConnOut(int fd) override;

  void MoveConnIn(std::shared_ptr<NetConn> conn, const NotifyType& type) override;

  void KillAllConns() override;

  bool KillConn(const std::string& ip_port) override;

  void HandleNewConn(int connfd, const std::string& ip_port) override;

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
  std::vector<std::unique_ptr<WorkerThread>> worker_thread_;
  int queue_limit_;
  std::map<WorkerThread*, void*> localdata_;

  void HandleConnEvent(NetFiredEvent* pfe) override { UNUSED(pfe); }

};  // class DispatchThread

}  // namespace net
#endif  // NET_SRC_DISPATCH_THREAD_H_
