// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <vector>

#include "pink/src/worker_thread.h"

#include "pink/include/pink_conn.h"
#include "pink/src/pink_item.h"
#include "pink/src/pink_epoll.h"

namespace pink {


WorkerThread::WorkerThread(ConnFactory *conn_factory,
                           ServerThread* server_thread,
                           int queue_limit_,
                           int cron_interval)
      : private_data_(nullptr),
        server_thread_(server_thread),
        conn_factory_(conn_factory),
        cron_interval_(cron_interval),
        keepalive_timeout_(kDefaultKeepAliveTime) {
  /*
   * install the protobuf handler here
   */
  pink_epoll_ = new PinkEpoll(queue_limit_);
}

WorkerThread::~WorkerThread() {
  delete(pink_epoll_);
}

int WorkerThread::conn_num() const {
  slash::ReadLock l(&rwlock_);
  return conns_.size();
}

std::vector<ServerThread::ConnInfo> WorkerThread::conns_info() const {
  std::vector<ServerThread::ConnInfo> result;
  slash::ReadLock l(&rwlock_);
  for (auto& conn : conns_) {
    result.push_back({
                      conn.first,
                      conn.second->ip_port(),
                      conn.second->last_interaction()
                     });
  }
  return result;
}

std::shared_ptr<PinkConn> WorkerThread::MoveConnOut(int fd) {
  slash::WriteLock l(&rwlock_);
  std::shared_ptr<PinkConn> conn = nullptr;
  auto iter = conns_.find(fd);
  if (iter != conns_.end()) {
    int fd = iter->first;
    conn = iter->second;
    pink_epoll_->PinkDelEvent(fd);
    conns_.erase(iter);
  }
  return conn;
}

bool WorkerThread::MoveConnIn(std::shared_ptr<PinkConn> conn, const NotifyType& notify_type, bool force) {
  PinkItem it(conn->fd(), conn->ip_port(), notify_type);
  bool success = MoveConnIn(it, force);
  if (success) {
    slash::WriteLock l(&rwlock_);
    conns_[conn->fd()] = conn;
  }
  return success;
}

bool WorkerThread::MoveConnIn(const PinkItem& it, bool force) {
  return pink_epoll_->Register(it, force);
}

void *WorkerThread::ThreadMain() {
  int nfds;
  PinkFiredEvent *pfe = NULL;
  char bb[2048];
  PinkItem ti;
  std::shared_ptr<PinkConn> in_conn = nullptr;

  struct timeval when;
  gettimeofday(&when, NULL);
  struct timeval now = when;

  when.tv_sec += (cron_interval_ / 1000);
  when.tv_usec += ((cron_interval_ % 1000) * 1000);
  int timeout = cron_interval_;
  if (timeout <= 0) {
    timeout = PINK_CRON_INTERVAL;
  }

  while (!should_stop()) {
    if (cron_interval_ > 0) {
      gettimeofday(&now, NULL);
      if (when.tv_sec > now.tv_sec ||
          (when.tv_sec == now.tv_sec && when.tv_usec > now.tv_usec)) {
        timeout = (when.tv_sec - now.tv_sec) * 1000 +
          (when.tv_usec - now.tv_usec) / 1000;
      } else {
        DoCronTask();
        when.tv_sec = now.tv_sec + (cron_interval_ / 1000);
        when.tv_usec = now.tv_usec + ((cron_interval_ % 1000) * 1000);
        timeout = cron_interval_;
      }
    }

    nfds = pink_epoll_->PinkPoll(timeout);

    for (int i = 0; i < nfds; i++) {
      pfe = (pink_epoll_->firedevent()) + i;
      if (pfe->fd == pink_epoll_->notify_receive_fd()) {
        if (pfe->mask & EPOLLIN) {
          int32_t nread = read(pink_epoll_->notify_receive_fd(), bb, 2048);
          if (nread == 0) {
            continue;
          } else {
            for (int32_t idx = 0; idx < nread; ++idx) {
              PinkItem ti = pink_epoll_->notify_queue_pop();
              if (ti.notify_type() == kNotiConnect) {
                std::shared_ptr<PinkConn> tc = conn_factory_->NewPinkConn(
                    ti.fd(), ti.ip_port(),
                    server_thread_, private_data_, pink_epoll_);
                if (!tc || !tc->SetNonblock()) {
                  continue;
                }

#ifdef __ENABLE_SSL
                // Create SSL failed
                if (server_thread_->security() &&
                  !tc->CreateSSL(server_thread_->ssl_ctx())) {
                  CloseFd(tc);
                  continue;
                }
#endif

                {
                  slash::WriteLock l(&rwlock_);
                  conns_[ti.fd()] = tc;
                }
                pink_epoll_->PinkAddEvent(ti.fd(), EPOLLIN);
              } else if (ti.notify_type() == kNotiClose) {
                // should close?
              } else if (ti.notify_type() == kNotiEpollout) {
                pink_epoll_->PinkModEvent(ti.fd(), 0, EPOLLOUT);
              } else if (ti.notify_type() == kNotiEpollin) {
                pink_epoll_->PinkModEvent(ti.fd(), 0, EPOLLIN);
              } else if (ti.notify_type() == kNotiEpolloutAndEpollin) {
                pink_epoll_->PinkModEvent(ti.fd(), 0, EPOLLOUT | EPOLLIN);
              } else if (ti.notify_type() == kNotiWait) {
                // do not register events
                pink_epoll_->PinkAddEvent(ti.fd(), 0);
              }
            }
          }
        } else {
          continue;
        }
      } else {
        in_conn = NULL;
        int should_close = 0;
        if (pfe == NULL) {
          continue;
        }

        {
          slash::ReadLock l(&rwlock_);
          std::map<int, std::shared_ptr<PinkConn>>::iterator iter = conns_.find(pfe->fd);
          if (iter == conns_.end()) {
            pink_epoll_->PinkDelEvent(pfe->fd);
            continue;
          }
          in_conn = iter->second;
        }

        if ((pfe->mask & EPOLLOUT) && in_conn->is_reply()) {
          WriteStatus write_status = in_conn->SendReply();
          in_conn->set_last_interaction(now);
          if (write_status == kWriteAll) {
            pink_epoll_->PinkModEvent(pfe->fd, 0, EPOLLIN);
            in_conn->set_is_reply(false);
            if (in_conn->IsClose()) {
                 should_close = 1;
            }
          } else if (write_status == kWriteHalf) {
            continue;
          } else {
            should_close = 1;
          }
        }

        if (!should_close && (pfe->mask & EPOLLIN)) {
          ReadStatus read_status = in_conn->GetRequest();
          in_conn->set_last_interaction(now);
          if (read_status == kReadAll) {
            pink_epoll_->PinkModEvent(pfe->fd, 0, 0);
            // Wait for the conn complete asynchronous task and
            // Mod Event to EPOLLOUT
          } else if (read_status == kReadHalf) {
            continue;
          } else {
            should_close = 1;
          }
        }

        if ((pfe->mask & EPOLLERR) || (pfe->mask & EPOLLHUP) || should_close) {
          pink_epoll_->PinkDelEvent(pfe->fd);
          CloseFd(in_conn);
          in_conn = NULL;
          {
            slash::WriteLock l(&rwlock_);
            conns_.erase(pfe->fd);
          }
          should_close = 0;
        }
      }  // connection event
    }  // for (int i = 0; i < nfds; i++)
  }  // while (!should_stop())

  Cleanup();
  return NULL;
}

void WorkerThread::DoCronTask() {
  struct timeval now;
  gettimeofday(&now, NULL);
  std::vector<std::shared_ptr<PinkConn>> to_close;
  std::vector<std::shared_ptr<PinkConn>> to_timeout;
  {
    slash::WriteLock l(&rwlock_);

    // Check whether close all connection
    slash::MutexLock kl(&killer_mutex_);
    if (deleting_conn_ipport_.count(kKillAllConnsTask)) {
      for (auto& conn : conns_) {
        to_close.push_back(conn.second);
      }
      conns_.clear();
      deleting_conn_ipport_.clear();
      return;
    }

    std::map<int, std::shared_ptr<PinkConn>>::iterator iter = conns_.begin();
    while (iter != conns_.end()) {
      std::shared_ptr<PinkConn> conn = iter->second;
      // Check connection should be closed
      if (deleting_conn_ipport_.count(conn->ip_port())) {
        to_close.push_back(conn);
        deleting_conn_ipport_.erase(conn->ip_port());
        iter = conns_.erase(iter);
        continue;
      }

      // Check keepalive timeout connection
      if (keepalive_timeout_ > 0 &&
          (now.tv_sec - conn->last_interaction().tv_sec > keepalive_timeout_)) {
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
    server_thread_->handle_->FdTimeoutHandle(conn->fd(), conn->ip_port());
  }
}

bool WorkerThread::TryKillConn(const std::string& ip_port) {
  bool find = false;
  if (ip_port != kKillAllConnsTask) {
    slash::ReadLock l(&rwlock_);
    for (auto& iter : conns_) {
      if (iter.second->ip_port() == ip_port) {
        find = true;
        break;
      }
    }
  }
  if (find || ip_port == kKillAllConnsTask) {
    slash::MutexLock l(&killer_mutex_);
    deleting_conn_ipport_.insert(ip_port);
    return true;
  }
  return false;
}

void WorkerThread::CloseFd(std::shared_ptr<PinkConn> conn) {
  close(conn->fd());
  server_thread_->handle_->FdClosedHandle(conn->fd(), conn->ip_port());
}

void WorkerThread::Cleanup() {
  std::map<int, std::shared_ptr<PinkConn>> to_close;
  {
    slash::WriteLock l(&rwlock_);
    to_close = std::move(conns_);
    conns_.clear();
  }
  for (const auto& iter : to_close) {
    CloseFd(iter.second);
  }

}

};  // namespace pink
