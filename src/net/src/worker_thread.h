// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_SRC_WORKER_THREAD_H_
#define NET_SRC_WORKER_THREAD_H_

#include <sys/epoll.h>

#include <string>
#include <functional>
#include <map>
#include <atomic>
#include <vector>
#include <set>

#include "pstd/include/xdebug.h"
#include "pstd/include/pstd_mutex.h"

#include "net/include/server_thread.h"
#include "net/src/net_epoll.h"
#include "net/include/net_thread.h"
#include "net/include/net_define.h"

namespace net {

class NetItem;
class NetEpoll;
class NetFiredEvent;
class NetConn;
class ConnFactory;

class WorkerThread : public Thread {
 public:
  explicit WorkerThread(ConnFactory *conn_factory, ServerThread* server_thread,
                        int queue_limit_, int cron_interval = 0);

  virtual ~WorkerThread();

  void set_keepalive_timeout(int timeout) {
    keepalive_timeout_ = timeout;
  }

  int conn_num() const;

  std::vector<ServerThread::ConnInfo> conns_info() const;

  std::shared_ptr<NetConn> MoveConnOut(int fd);

  bool MoveConnIn(std::shared_ptr<NetConn> conn, const NotifyType& notify_type, bool force);

  bool MoveConnIn(const NetItem& it, bool force);

  NetEpoll* net_epoll() {
    return net_epoll_;
  }
  bool TryKillConn(const std::string& ip_port);

  mutable pstd::RWMutex rwlock_; /* For external statistics */
  std::map<int, std::shared_ptr<NetConn>> conns_;

  void* private_data_;

 private:
  ServerThread* server_thread_;
  ConnFactory *conn_factory_;
  int cron_interval_;


  /*
   * The epoll handler
   */
  NetEpoll *net_epoll_;

  std::atomic<int> keepalive_timeout_;  // keepalive second

  virtual void *ThreadMain() override;
  void DoCronTask();

  pstd::Mutex killer_mutex_;
  std::set<std::string> deleting_conn_ipport_;

  // clean conns
  void CloseFd(std::shared_ptr<NetConn> conn);
  void Cleanup();
};  // class WorkerThread

}  // namespace net
#endif  // NET_SRC_WORKER_THREAD_H_
