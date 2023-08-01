// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_INCLUDE_BACKEND_THREAD_H_
#define NET_INCLUDE_BACKEND_THREAD_H_

#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "net/include/net_thread.h"
#include "net/src/net_multiplexer.h"
#include "pstd/include/pstd_mutex.h"
#include "pstd/include/pstd_status.h"

// remove 'unused parameter' warning
#define UNUSED(expr) \
  do {               \
    (void)(expr);    \
  } while (0)

#define kConnWriteBuf (1024 * 1024 * 100)  // cache 100 MB data per connection

namespace net {

struct NetFiredEvent;
class ConnFactory;
class NetConn;

/*
 *  BackendHandle will be invoked at appropriate occasion
 *  in client thread's main loop.
 */
class BackendHandle {
 public:
  BackendHandle() = default;
  virtual ~BackendHandle() = default;

  /*
   *  CronHandle() will be invoked on every cron_interval elapsed.
   */
  virtual void CronHandle() const {}

  /*
   *  FdTimeoutHandle(...) will be invoked after connection timeout.
   */
  virtual void FdTimeoutHandle(int32_t fd, const std::string& ip_port) const {
    UNUSED(fd);
    UNUSED(ip_port);
  }

  /*
   *  FdClosedHandle(...) will be invoked before connection closed.
   */
  virtual void FdClosedHandle(int32_t fd, const std::string& ip_port) const {
    UNUSED(fd);
    UNUSED(ip_port);
  }

  /*
   *  AccessHandle(...) will be invoked after Write invoked
   *  but before handled.
   */
  virtual bool AccessHandle(std::string& ip) const {
    UNUSED(ip);
    return true;
  }

  /*
   *  CreateWorkerSpecificData(...) will be invoked in StartThread() routine.
   *  'data' pointer should be assigned.
   */
  virtual int32_t CreateWorkerSpecificData(void** data) const {
    UNUSED(data);
    return 0;
  }

  /*
   *  DeleteWorkerSpecificData(...) is related to CreateWorkerSpecificData(...),
   *  it will be invoked in StopThread(...) routine,
   *  resources assigned in CreateWorkerSpecificData(...) should be deleted in
   *  this handle
   */
  virtual int32_t DeleteWorkerSpecificData(void* data) const {
    UNUSED(data);
    return 0;
  }

  /*
   * DestConnectFailedHandle(...) will run the invoker's logic when socket connect failed
   */
  virtual void DestConnectFailedHandle(const std::string& ip_port, const std::string& reason) const {
    UNUSED(ip_port);
    UNUSED(reason);
  }
};

class BackendThread : public Thread {
 public:
  BackendThread(ConnFactory* conn_factory, int32_t cron_interval, int32_t keepalive_timeout, BackendHandle* handle,
                void* private_data);
  ~BackendThread() override;
  /*
   * StartThread will return the error code as pthread_create return
   *  Return 0 if success
   */
  int32_t StartThread() override;
  int32_t StopThread() override;
  pstd::Status Write(int32_t fd, const std::string& msg);
  pstd::Status Close(int32_t fd);
  // Try to connect fd noblock, if return EINPROGRESS or EAGAIN or EWOULDBLOCK
  // put this fd in epoll (SetWaitConnectOnEpoll), process in ProcessConnectStatus
  pstd::Status Connect(const std::string& dst_ip, int32_t dst_port, int32_t* fd);
  std::shared_ptr<NetConn> GetConn(int32_t fd);

 private:
  void* ThreadMain() override;

  void InternalDebugPrint();
  // Set connect fd into epoll
  // connect condition: no EPOLLERR EPOLLHUP events,  no error in socket opt
  pstd::Status ProcessConnectStatus(NetFiredEvent* pfe, int32_t* should_close);
  void SetWaitConnectOnEpoll(int32_t sockfd);

  void AddConnection(const std::string& peer_ip, int32_t peer_port, int32_t sockfd);
  void CloseFd(const std::shared_ptr<NetConn>& conn);
  void CloseFd(int32_t fd);
  void CleanUpConnRemaining(int32_t fd);
  void DoCronTask();
  void NotifyWrite(std::string& ip_port);
  void NotifyWrite(int32_t fd);
  void NotifyClose(int32_t fd);
  void ProcessNotifyEvents(const NetFiredEvent* pfe);

  int32_t keepalive_timeout_;
  int32_t cron_interval_;
  BackendHandle* handle_;
  bool own_handle_{false};
  void* private_data_;

  /*
   * The Epoll event handler
   */
  std::unique_ptr<NetMultiplexer> net_multiplexer_;

  ConnFactory* conn_factory_;

  pstd::Mutex mu_;
  std::map<int32_t, std::vector<std::string>> to_send_;  // ip+":"+port, to_send_msg

  std::map<int32_t, std::shared_ptr<NetConn>> conns_;
  std::set<int32_t> connecting_fds_;
};

}  // namespace net
#endif  // NET_INCLUDE_CLIENT_THREAD_H_
