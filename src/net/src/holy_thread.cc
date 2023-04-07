// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <vector>

#include "net/src/holy_thread.h"

#include "net/src/net_epoll.h"
#include "net/src/net_item.h"
#include "net/include/net_conn.h"
#include "pstd/include/xdebug.h"

namespace net {

HolyThread::HolyThread(int port,
                       ConnFactory* conn_factory,
                       int cron_interval, const ServerHandle* handle, bool async)
    : ServerThread::ServerThread(port, cron_interval, handle),
      conn_factory_(conn_factory),
      private_data_(nullptr),
      keepalive_timeout_(kDefaultKeepAliveTime),
      async_(async) {
}

HolyThread::HolyThread(const std::string& bind_ip, int port,
                       ConnFactory* conn_factory,
                       int cron_interval, const ServerHandle* handle, bool async)
    : ServerThread::ServerThread(bind_ip, port, cron_interval, handle),
      conn_factory_(conn_factory),
      async_(async) {
}

HolyThread::HolyThread(const std::set<std::string>& bind_ips, int port,
                       ConnFactory* conn_factory,
                       int cron_interval, const ServerHandle* handle, bool async)
    : ServerThread::ServerThread(bind_ips, port, cron_interval, handle),
      conn_factory_(conn_factory),
      async_(async) {
}

HolyThread::~HolyThread() {
  Cleanup();
}

int HolyThread::conn_num() const {
  pstd::ReadLock l(&rwlock_);
  return conns_.size();
}

std::vector<ServerThread::ConnInfo> HolyThread::conns_info() const {
  std::vector<ServerThread::ConnInfo> result;
  pstd::ReadLock l(&rwlock_);
  for (auto& conn : conns_) {
    result.push_back({
                      conn.first,
                      conn.second->ip_port(),
                      conn.second->last_interaction()
                     });
  }
  return result;
}

std::shared_ptr<NetConn> HolyThread::MoveConnOut(int fd) {
  pstd::WriteLock l(&rwlock_);
  std::shared_ptr<NetConn> conn = nullptr;
  auto iter = conns_.find(fd);
  if (iter != conns_.end()) {
    int fd = iter->first;
    conn = iter->second;
    net_epoll_->NetDelEvent(fd, 0);
    conns_.erase(iter);
  }
  return conn;
}

std::shared_ptr<NetConn> HolyThread::get_conn(int fd) {
  pstd::ReadLock l(&rwlock_);
  auto iter = conns_.find(fd);
  if (iter != conns_.end()) {
    return iter->second;
  } else {
    return nullptr;
  }
}

int HolyThread::StartThread() {
  int ret = handle_->CreateWorkerSpecificData(&private_data_);
  if (ret != 0) {
    return ret;
  }
  return ServerThread::StartThread();
}

int HolyThread::StopThread() {
  if (private_data_) {
    int ret = handle_->DeleteWorkerSpecificData(private_data_);
    if (ret != 0) {
      return ret;
    }
    private_data_ = nullptr;
  }
  return ServerThread::StopThread();
}

void HolyThread::HandleNewConn(const int connfd, const std::string &ip_port) {
  std::shared_ptr<NetConn> tc = conn_factory_->NewNetConn(
      connfd, ip_port, this, private_data_, net_epoll_);
  tc->SetNonblock();
  {
    pstd::WriteLock l(&rwlock_);
    conns_[connfd] = tc;
  }

  net_epoll_->NetAddEvent(connfd, EPOLLIN);
}

void HolyThread::HandleConnEvent(NetFiredEvent *pfe) {
  if (pfe == nullptr) {
    return;
  }
  std::shared_ptr<NetConn> in_conn = nullptr;
  int should_close = 0;
  std::map<int, std::shared_ptr<NetConn>>::iterator iter;
  {
    pstd::ReadLock l(&rwlock_);
    if ((iter = conns_.find(pfe->fd)) == conns_.end()) {
      net_epoll_->NetDelEvent(pfe->fd, 0);
      return;
    }
  }
  in_conn = iter->second;
  if (async_) {
    if (pfe->mask & EPOLLIN) {
      ReadStatus read_status = in_conn->GetRequest();
      struct timeval now;
      gettimeofday(&now, nullptr);
      in_conn->set_last_interaction(now);
      if (read_status == kReadAll) {
      // do nothing still watch EPOLLIN
      } else if (read_status == kReadHalf) {
        return;
      } else {
        // kReadError kReadClose kFullError kParseError kDealError
        should_close = 1;
      }
    }
    if ((pfe->mask & EPOLLOUT) && in_conn->is_reply()) {
      WriteStatus write_status = in_conn->SendReply();
      if (write_status == kWriteAll) {
        in_conn->set_is_reply(false);
        net_epoll_->NetModEvent(pfe->fd, 0, EPOLLIN);
      } else if (write_status == kWriteHalf) {
        return;
      } else if (write_status == kWriteError) {
        should_close = 1;
      }
    }
  } else {
    if (pfe->mask & EPOLLIN) {
      ReadStatus getRes = in_conn->GetRequest();
      struct timeval now;
      gettimeofday(&now, nullptr);
      in_conn->set_last_interaction(now);
      if (getRes != kReadAll && getRes != kReadHalf) {
        // kReadError kReadClose kFullError kParseError kDealError
        should_close = 1;
      } else if (in_conn->is_reply()) {
        net_epoll_->NetModEvent(pfe->fd, 0, EPOLLOUT);
      } else {
        return;
      }
    }
    if (pfe->mask & EPOLLOUT) {
      WriteStatus write_status = in_conn->SendReply();
      if (write_status == kWriteAll) {
        in_conn->set_is_reply(false);
        net_epoll_->NetModEvent(pfe->fd, 0, EPOLLIN);
      } else if (write_status == kWriteHalf) {
        return;
      } else if (write_status == kWriteError) {
        should_close = 1;
      }
    }
  }
  if ((pfe->mask & EPOLLERR) || (pfe->mask & EPOLLHUP) || should_close) {
    net_epoll_->NetDelEvent(pfe->fd, 0);
    CloseFd(in_conn);
    in_conn = nullptr;

    {
      pstd::WriteLock l(&rwlock_);
      conns_.erase(pfe->fd);
    }
  }
}

void HolyThread::DoCronTask() {
  struct timeval now;
  gettimeofday(&now, NULL);
  std::vector<std::shared_ptr<NetConn>> to_close;
  std::vector<std::shared_ptr<NetConn>> to_timeout;
  {
    pstd::WriteLock l(&rwlock_);

    // Check whether close all connection
    pstd::MutexLock kl(&killer_mutex_);
    if (deleting_conn_ipport_.count(kKillAllConnsTask)) {
      for (auto& conn : conns_) {
        to_close.push_back(conn.second);
      }
      conns_.clear();
      deleting_conn_ipport_.clear();
      return;
    }

    std::map<int, std::shared_ptr<NetConn>>::iterator iter = conns_.begin();
    while (iter != conns_.end()) {
      std::shared_ptr<NetConn> conn = iter->second;
      // Check connection should be closed
      if (deleting_conn_ipport_.count(conn->ip_port())) {
        to_close.push_back(conn);
        deleting_conn_ipport_.erase(conn->ip_port());
        iter = conns_.erase(iter);
        continue;
      }

      // Check keepalive timeout connection
      if (keepalive_timeout_ > 0 &&
          (now.tv_sec - conn->last_interaction().tv_sec >
           keepalive_timeout_)) {
        to_timeout.push_back(conn);
        iter = conns_.erase(iter);
        continue;
      }

      // Maybe resize connection buffer
      conn->TryResizeBuffer();

      ++iter;
    }
  }
  for (const auto conn : to_close) {
    CloseFd(conn);
  }
  for (const auto conn : to_timeout) {
    CloseFd(conn);
    handle_->FdTimeoutHandle(conn->fd(), conn->ip_port());
  }
}

void HolyThread::CloseFd(std::shared_ptr<NetConn> conn) {
  close(conn->fd());
  handle_->FdClosedHandle(conn->fd(), conn->ip_port());
}

// clean all conns
void HolyThread::Cleanup() {
  std::map<int, std::shared_ptr<NetConn>> to_close;
  {
    pstd::WriteLock l(&rwlock_);
    to_close = std::move(conns_);
    conns_.clear();
  }
  for (auto& iter : to_close) {
    CloseFd(iter.second);
  }
}

void HolyThread::KillAllConns() {
  KillConn(kKillAllConnsTask);
}

bool HolyThread::KillConn(const std::string& ip_port) {
  bool find = false;
  if (ip_port != kKillAllConnsTask) {
    pstd::ReadLock l(&rwlock_);
    for (auto& iter : conns_) {
      if (iter.second->ip_port() == ip_port) {
        find = true;
        break;
      }
    }
  }
  if (find || ip_port == kKillAllConnsTask) {
    pstd::MutexLock l(&killer_mutex_);
    deleting_conn_ipport_.insert(ip_port);
    return true;
  }
  return false;
}

void HolyThread::ProcessNotifyEvents(const net::NetFiredEvent* pfe) {
  if (pfe->mask & EPOLLIN) {
    char bb[2048];
    int32_t nread = read(net_epoll_->NotifyReceiveFd(), bb, 2048);
    //  log_info("notify_received bytes %d\n", nread);
    if (nread == 0) {
      return;
    } else {
      for (int32_t idx = 0; idx < nread; ++idx) {
        net::NetItem ti = net_epoll_->NotifyQueuePop();
        std::string ip_port = ti.ip_port();
        int fd = ti.fd();
        if (ti.notify_type() == net::kNotiWrite) {
          net_epoll_->NetModEvent(ti.fd(), 0, EPOLLOUT | EPOLLIN);
        } else if (ti.notify_type() == net::kNotiClose) {
          log_info("receive noti close\n");
          std::shared_ptr<net::NetConn> conn = get_conn(fd);
          if (conn == nullptr) {
            continue;
          }
          CloseFd(conn);
          conn = nullptr;
          {
            pstd::WriteLock l(&rwlock_);
            conns_.erase(fd);
          }
        }
      }
    }
  }
}

extern ServerThread *NewHolyThread(
    int port,
    ConnFactory *conn_factory,
    int cron_interval, const ServerHandle* handle) {
  return new HolyThread(port, conn_factory, cron_interval, handle);
}
extern ServerThread *NewHolyThread(
    const std::string &bind_ip, int port,
    ConnFactory *conn_factory,
    int cron_interval, const ServerHandle* handle) {
  return new HolyThread(bind_ip, port, conn_factory, cron_interval, handle);
}
extern ServerThread *NewHolyThread(
    const std::set<std::string>& bind_ips, int port,
    ConnFactory *conn_factory,
    int cron_interval, const ServerHandle* handle) {
  return new HolyThread(bind_ips, port, conn_factory, cron_interval, handle);
}
extern ServerThread *NewHolyThread(
    const std::set<std::string>& bind_ips, int port,
    ConnFactory *conn_factory, bool async,
    int cron_interval, const ServerHandle* handle) {
  return new HolyThread(bind_ips, port, conn_factory, cron_interval, handle, async);
}
};  // namespace net
