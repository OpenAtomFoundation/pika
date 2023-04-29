// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <vector>

#include <glog/logging.h>

#include "pstd/include/testutil.h"
#include "net/src/worker_thread.h"

#include "net/include/net_conn.h"
#include "net/src/net_item.h"

namespace net {

WorkerThread::WorkerThread(ConnFactory* conn_factory, ServerThread* server_thread, int queue_limit, int cron_interval)
    : private_data_(nullptr),
      server_thread_(server_thread),
      conn_factory_(conn_factory),
      cron_interval_(cron_interval),
      keepalive_timeout_(kDefaultKeepAliveTime) {
  /*
   * install the protobuf handler here
   */
  net_multiplexer_.reset(CreateNetMultiplexer(queue_limit));
  net_multiplexer_->Initialize();
}

WorkerThread::~WorkerThread() {}

int WorkerThread::conn_num() const {
  pstd::ReadLock l(&rwlock_);
  return conns_.size();
}

std::vector<ServerThread::ConnInfo> WorkerThread::conns_info() const {
  std::vector<ServerThread::ConnInfo> result;
  pstd::ReadLock l(&rwlock_);
  for (auto& conn : conns_) {
    result.push_back({conn.first, conn.second->ip_port(), conn.second->last_interaction()});
  }
  return result;
}

std::shared_ptr<NetConn> WorkerThread::MoveConnOut(int fd) {
  pstd::WriteLock l(&rwlock_);
  std::shared_ptr<NetConn> conn = nullptr;
  auto iter = conns_.find(fd);
  if (iter != conns_.end()) {
    int fd = iter->first;
    conn = iter->second;
    net_multiplexer_->NetDelEvent(fd, 0);
    DLOG(INFO) << "move out connection " << conn->String();
    conns_.erase(iter);
  }
  return conn;
}

bool WorkerThread::MoveConnIn(std::shared_ptr<NetConn> conn, const NotifyType& notify_type, bool force) {
  NetItem it(conn->fd(), conn->ip_port(), notify_type);
  bool success = MoveConnIn(it, force);
  if (success) {
    pstd::WriteLock l(&rwlock_);
    conns_[conn->fd()] = conn;
  }
  return success;
}

bool WorkerThread::MoveConnIn(const NetItem& it, bool force) { return net_multiplexer_->Register(it, force); }

void* WorkerThread::ThreadMain() {
  int nfds;
  NetFiredEvent* pfe = nullptr;
  char bb[2048];
  NetItem ti;
  std::shared_ptr<NetConn> in_conn = nullptr;

  struct timeval when;
  gettimeofday(&when, nullptr);
  struct timeval now = when;

  when.tv_sec += (cron_interval_ / 1000);
  when.tv_usec += ((cron_interval_ % 1000) * 1000);
  int timeout = cron_interval_;
  if (timeout <= 0) {
    timeout = NET_CRON_INTERVAL;
  }

  while (!should_stop()) {
    if (cron_interval_ > 0) {
      gettimeofday(&now, nullptr);
      if (when.tv_sec > now.tv_sec || (when.tv_sec == now.tv_sec && when.tv_usec > now.tv_usec)) {
        timeout = (when.tv_sec - now.tv_sec) * 1000 + (when.tv_usec - now.tv_usec) / 1000;
      } else {
        DoCronTask();
        when.tv_sec = now.tv_sec + (cron_interval_ / 1000);
        when.tv_usec = now.tv_usec + ((cron_interval_ % 1000) * 1000);
        timeout = cron_interval_;
      }
    }

    nfds = net_multiplexer_->NetPoll(timeout);

    for (int i = 0; i < nfds; i++) {
      pfe = (net_multiplexer_->FiredEvents()) + i;
      if (pfe->fd == net_multiplexer_->NotifyReceiveFd()) {
        if (pfe->mask & kReadable) {
          int32_t nread = read(net_multiplexer_->NotifyReceiveFd(), bb, 2048);
          if (nread == 0) {
            continue;
          } else {
            for (int32_t idx = 0; idx < nread; ++idx) {
              NetItem ti = net_multiplexer_->NotifyQueuePop();
              if (ti.notify_type() == kNotiConnect) {
                std::shared_ptr<NetConn> tc = conn_factory_->NewNetConn(ti.fd(), ti.ip_port(), server_thread_,
                                                                        private_data_, net_multiplexer_.get());
                if (!tc || !tc->SetNonblock()) {
                  continue;
                }

#ifdef __ENABLE_SSL
                // Create SSL failed
                if (server_thread_->security() && !tc->CreateSSL(server_thread_->ssl_ctx())) {
                  CloseFd(tc);
                  continue;
                }
#endif

                {
                  pstd::WriteLock l(&rwlock_);
                  conns_[ti.fd()] = tc;
                }
                net_multiplexer_->NetAddEvent(ti.fd(), kReadable);
              } else if (ti.notify_type() == kNotiClose) {
                // should close?
              } else if (ti.notify_type() == kNotiEpollout) {
                net_multiplexer_->NetModEvent(ti.fd(), 0, kWritable);
              } else if (ti.notify_type() == kNotiEpollin) {
                net_multiplexer_->NetModEvent(ti.fd(), 0, kReadable);
              } else if (ti.notify_type() == kNotiEpolloutAndEpollin) {
                net_multiplexer_->NetModEvent(ti.fd(), 0, kReadable | kWritable);
              } else if (ti.notify_type() == kNotiWait) {
                // do not register events
                net_multiplexer_->NetAddEvent(ti.fd(), 0);
              }
            }
          }
        } else {
          continue;
        }
      } else {
        in_conn = nullptr;
        int should_close = 0;
        if (pfe == nullptr) {
          continue;
        }

        {
          pstd::ReadLock l(&rwlock_);
          std::map<int, std::shared_ptr<NetConn>>::iterator iter = conns_.find(pfe->fd);
          if (iter == conns_.end()) {
            net_multiplexer_->NetDelEvent(pfe->fd, 0);
            continue;
          }
          in_conn = iter->second;
        }

        if ((pfe->mask & kWritable) && in_conn->is_reply()) {
          WriteStatus write_status = in_conn->SendReply();
          in_conn->set_last_interaction(now);
          if (write_status == kWriteAll) {
            net_multiplexer_->NetModEvent(pfe->fd, 0, kReadable);
            in_conn->set_is_reply(false);
            if (in_conn->IsClose()) {
              should_close = 1;
              LOG(INFO) << "will close client connection " << in_conn->String();
            }
          } else if (write_status == kWriteHalf) {
            continue;
          } else {
            should_close = 1;
          }
        }

        if (!should_close && (pfe->mask & kReadable)) {
          ReadStatus read_status = in_conn->GetRequest();
          in_conn->set_last_interaction(now);
          if (read_status == kReadAll) {
            net_multiplexer_->NetModEvent(pfe->fd, 0, 0);
            // Wait for the conn complete asynchronous task and
            // Mod Event to kWritable
          } else if (read_status == kReadHalf) {
            continue;
          } else {
            should_close = 1;
          }
        }

        if ((pfe->mask & kErrorEvent) || should_close) {
          net_multiplexer_->NetDelEvent(pfe->fd, 0);
          CloseFd(in_conn);
          in_conn = nullptr;
          {
            pstd::WriteLock l(&rwlock_);
            conns_.erase(pfe->fd);
          }
          should_close = 0;
        }
      }  // connection event
    }    // for (int i = 0; i < nfds; i++)
  }      // while (!should_stop())

  Cleanup();
  return nullptr;
}

void WorkerThread::DoCronTask() {
  struct timeval now;
  gettimeofday(&now, nullptr);
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
        LOG(INFO) << "will close client connection " << conn->String();
        continue;
      }

      // Check keepalive timeout connection
      if (keepalive_timeout_ > 0 && (now.tv_sec - conn->last_interaction().tv_sec > keepalive_timeout_)) {
        to_timeout.push_back(conn);
        iter = conns_.erase(iter);
        LOG(INFO) << "connection " << conn->String() << " keepalive timeout, the keepalive_timeout_ is " << keepalive_timeout_.load();
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

void WorkerThread::CloseFd(std::shared_ptr<NetConn> conn) {
  close(conn->fd());
  server_thread_->handle_->FdClosedHandle(conn->fd(), conn->ip_port());
}

void WorkerThread::Cleanup() {
  std::map<int, std::shared_ptr<NetConn>> to_close;
  {
    pstd::WriteLock l(&rwlock_);
    to_close = std::move(conns_);
    conns_.clear();
  }
  for (const auto& iter : to_close) {
    CloseFd(iter.second);
  }
}

};  // namespace net
