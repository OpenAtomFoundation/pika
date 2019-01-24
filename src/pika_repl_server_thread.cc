// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_repl_server_thread.h"

PikaReplServerThread::PikaReplServerThread(const std::set<std::string> &ips, int port, pink::ConnFactory* conn_factory, int cron_interval, const pink::ServerHandle* handle)
  : HolyThread(ips, port, conn_factory, cron_interval, handle) {
}


void PikaReplServerThread::Write(const std::string& msg, const std::string& ip_port, int fd) {
  {
    slash::MutexLock l(&write_buf_mu_);
    write_buf_[fd] += msg;
  }
  NotifyWrite(ip_port, fd);
}

void PikaReplServerThread::NotifyWrite(const std::string& ip_port, int fd) {
  pink::PinkItem ti(fd, ip_port, pink::kNotiWrite);
  pink_epoll_->notify_queue_lock();
  std::queue<pink::PinkItem> *q = &(pink_epoll_->notify_queue_);
  q->push(ti);
  pink_epoll_->notify_queue_unlock();
  write(pink_epoll_->notify_send_fd(), "", 1);
}

void PikaReplServerThread::ProcessNotifyEvents(const pink::PinkFiredEvent* pfe) {
  if (pfe->mask & EPOLLIN) {
    char bb[2048];
    int32_t nread = read(pink_epoll_->notify_receive_fd(), bb, 2048);
    //  log_info("notify_received bytes %d\n", nread);
    if (nread == 0) {
      return;
    } else {
      for (int32_t idx = 0; idx < nread; ++idx) {
        pink::PinkItem ti;
        {
          pink_epoll_->notify_queue_lock();
          ti = pink_epoll_->notify_queue_.front();
          pink_epoll_->notify_queue_.pop();
          pink_epoll_->notify_queue_unlock();
        }
        std::string ip_port = ti.ip_port();
        int fd = ti.fd();
        if (ti.notify_type() == pink::kNotiWrite) {
          std::shared_ptr<pink::PinkConn> conn = get_conn(fd);
          {
          slash::MutexLock l(&write_buf_mu_);
          conn->WriteResp(write_buf_[fd]);
          write_buf_.erase(fd);
          }
        }
      }
    }
  }
}
