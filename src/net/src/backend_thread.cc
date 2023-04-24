// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "net/include/backend_thread.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>

#include "net/include/net_conn.h"
#include "net/src/server_socket.h"
#include "pstd/include/pstd_string.h"
#include "pstd/include/xdebug.h"

namespace net {

using pstd::Status;

BackendThread::BackendThread(ConnFactory* conn_factory, int cron_interval, int keepalive_timeout,
                             BackendHandle* handle, void* private_data)
    : keepalive_timeout_(keepalive_timeout),
      cron_interval_(cron_interval),
      handle_(handle),
      own_handle_(false),
      private_data_(private_data),
      conn_factory_(conn_factory) {
  net_multiplexer_.reset(CreateNetMultiplexer());
  net_multiplexer_->Initialize();
}

BackendThread::~BackendThread() {}

int BackendThread::StartThread() {
  if (!handle_) {
    handle_ = new BackendHandle();
    own_handle_ = true;
  }
  own_handle_ = false;
  int res = handle_->CreateWorkerSpecificData(&private_data_);
  if (res != 0) {
    return res;
  }
  return Thread::StartThread();
}

int BackendThread::StopThread() {
  if (private_data_) {
    int res = handle_->DeleteWorkerSpecificData(private_data_);
    if (res != 0) {
      return res;
    }
    private_data_ = nullptr;
  }
  if (own_handle_) {
    delete handle_;
  }
  return Thread::StopThread();
}

Status BackendThread::Write(const int fd, const std::string& msg) {
  {
    pstd::MutexLock l(&mu_);
    if (conns_.find(fd) == conns_.end()) {
      return Status::Corruption(std::to_string(fd) + " cannot find !");
    }
    auto addr = conns_.find(fd)->second->ip_port();
    if (!handle_->AccessHandle(addr)) {
      return Status::Corruption(addr + " is baned by user!");
    }
    size_t size = 0;
    for (auto& str : to_send_[fd]) {
      size += str.size();
    }
    if (size > kConnWriteBuf) {
      return Status::Corruption("Connection buffer over maximum size");
    }
    to_send_[fd].push_back(msg);
  }
  NotifyWrite(fd);
  return Status::OK();
}

Status BackendThread::Close(const int fd) {
  {
    pstd::MutexLock l(&mu_);
    if (conns_.find(fd) == conns_.end()) {
      return Status::OK();
    }
  }
  NotifyClose(fd);
  return Status::OK();
}

Status BackendThread::ProcessConnectStatus(NetFiredEvent* pfe, int* should_close) {
  if (pfe->mask & kErrorEvent) {
    *should_close = 1;
    return Status::Corruption("POLLERR or POLLHUP");
  }
  int val = 0;
  socklen_t lon = sizeof(int);

  if (getsockopt(pfe->item.fd(), SOL_SOCKET, SO_ERROR, &val, &lon) == -1) {
    *should_close = 1;
    return Status::Corruption("Get Socket opt failed");
  }
  if (val) {
    *should_close = 1;
    return Status::Corruption("Get socket error " + std::to_string(val));
  }
  return Status::OK();
}

void BackendThread::SetWaitConnectOnEpoll(int sockfd) {
  net_multiplexer_->NetAddEvent(sockfd, kReadable | kWritable);
  connecting_fds_.insert(sockfd);
}

void BackendThread::AddConnection(const std::string& peer_ip, int peer_port, int sockfd) {
  std::string ip_port = peer_ip + ":" + std::to_string(peer_port);
  std::shared_ptr<NetConn> tc = conn_factory_->NewNetConn(sockfd, ip_port, this, NULL, net_multiplexer_.get());
  tc->SetNonblock();
  // This flag specifies that the file descriptor should be closed when an exec function is invoked.
  fcntl(sockfd, F_SETFD, fcntl(sockfd, F_GETFD) | FD_CLOEXEC);

  {
    pstd::MutexLock l(&mu_);
    conns_.insert(std::make_pair(sockfd, tc));
  }
}

Status BackendThread::Connect(const std::string& dst_ip, const int dst_port, int* fd) {
  Status s;
  int sockfd = -1;
  int rv;
  char cport[6];
  struct addrinfo hints, *servinfo, *p;
  snprintf(cport, sizeof(cport), "%d", dst_port);
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;

  if (fd == nullptr) {
    return Status::InvalidArgument("fd argument is nullptr");
  }
  // We do not handle IPv6
  if ((rv = getaddrinfo(dst_ip.c_str(), cport, &hints, &servinfo)) != 0) {
    return Status::IOError("connect getaddrinfo error for ", dst_ip);
  }
  for (p = servinfo; p != NULL; p = p->ai_next) {
    if ((sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1) {
      continue;
    }
    int flags = fcntl(sockfd, F_GETFL, 0);
    fcntl(sockfd, F_SETFL, flags | O_NONBLOCK);

    if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
      if (errno == EHOSTUNREACH) {
        CloseFd(sockfd);
        continue;
      } else if (errno == EINPROGRESS || errno == EAGAIN || errno == EWOULDBLOCK) {
        AddConnection(dst_ip, dst_port, sockfd);
        SetWaitConnectOnEpoll(sockfd);
        freeaddrinfo(servinfo);
        *fd = sockfd;
        return Status::OK();
      } else {
        CloseFd(sockfd);
        freeaddrinfo(servinfo);
        return Status::IOError("EHOSTUNREACH", "The target host cannot be reached");
      }
    }

    AddConnection(dst_ip, dst_port, sockfd);
    net_multiplexer_->NetAddEvent(sockfd, kReadable | kWritable);
    struct sockaddr_in laddr;
    socklen_t llen = sizeof(laddr);
    getsockname(sockfd, (struct sockaddr*)&laddr, &llen);
    std::string lip(inet_ntoa(laddr.sin_addr));
    int lport = ntohs(laddr.sin_port);
    if (dst_ip == lip && dst_port == lport) {
      return Status::IOError("EHOSTUNREACH", "same ip port");
    }

    freeaddrinfo(servinfo);
    return s;
  }
  if (p == NULL) {
    s = Status::IOError(strerror(errno), "Can't create socket ");
    return s;
  }
  freeaddrinfo(servinfo);
  freeaddrinfo(p);
  int val = 1;
  setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val));
  *fd = sockfd;
  return s;
}

std::shared_ptr<NetConn> BackendThread::GetConn(int fd) {
  pstd::MutexLock l(&mu_);
  auto iter = conns_.find(fd);
  if (iter == conns_.end()) {
    return nullptr;
  }
  return iter->second;
}

void BackendThread::CloseFd(std::shared_ptr<NetConn> conn) {
  close(conn->fd());
  CleanUpConnRemaining(conn->fd());
  handle_->FdClosedHandle(conn->fd(), conn->ip_port());
}

void BackendThread::CloseFd(const int fd) {
  close(fd);
  CleanUpConnRemaining(fd);
  // user don't use ip_port
  handle_->FdClosedHandle(fd, "");
}

void BackendThread::CleanUpConnRemaining(const int fd) {
  pstd::MutexLock l(&mu_);
  to_send_.erase(fd);
}

void BackendThread::DoCronTask() {
  struct timeval now;
  gettimeofday(&now, NULL);
  pstd::MutexLock l(&mu_);
  std::map<int, std::shared_ptr<NetConn>>::iterator iter = conns_.begin();
  while (iter != conns_.end()) {
    std::shared_ptr<NetConn> conn = iter->second;

    // Check keepalive timeout connection
    if (keepalive_timeout_ > 0 && (now.tv_sec - conn->last_interaction().tv_sec > keepalive_timeout_)) {
      log_info("Do cron task del fd %d\n", conn->fd());
      net_multiplexer_->NetDelEvent(conn->fd(), 0);
      close(conn->fd());
      handle_->FdTimeoutHandle(conn->fd(), conn->ip_port());
      if (conns_.count(conn->fd())) {
        conns_.erase(conn->fd());
      }
      if (connecting_fds_.count(conn->fd())) {
        connecting_fds_.erase(conn->fd());
      }
      iter = conns_.erase(iter);
      continue;
    }

    // Maybe resize connection buffer
    conn->TryResizeBuffer();

    ++iter;
  }
}

void BackendThread::InternalDebugPrint() {
  log_info("___________________________________\n");
  {
    pstd::MutexLock l(&mu_);
    log_info("To send map: \n");
    for (const auto& to_send : to_send_) {
      UNUSED(to_send);
      const std::vector<std::string>& tmp = to_send.second;
      for (const auto& tmp_to_send : tmp) {
        UNUSED(tmp_to_send);
        log_info("%s %s\n", to_send.first.c_str(), tmp_to_send.c_str());
      }
    }
  }
  log_info("Connected fd map: \n");
  pstd::MutexLock l(&mu_);
  for (const auto& fd_conn : conns_) {
    UNUSED(fd_conn);
    log_info("fd %d", fd_conn.first);
  }
  log_info("Connecting fd map: \n");
  for (const auto& connecting_fd : connecting_fds_) {
    UNUSED(connecting_fd);
    log_info("fd: %d", connecting_fd);
  }
  log_info("___________________________________\n");
}

void BackendThread::NotifyWrite(const std::string ip_port) {
  // put fd = 0, cause this lib user doesnt need to know which fd to write to
  // we will check fd by checking ipport_conns_
  NetItem ti(0, ip_port, kNotiWrite);
  net_multiplexer_->Register(ti, true);
}

void BackendThread::NotifyWrite(const int fd) {
  NetItem ti(fd, "", kNotiWrite);
  net_multiplexer_->Register(ti, true);
}

void BackendThread::NotifyClose(const int fd) {
  NetItem ti(fd, "", kNotiClose);
  net_multiplexer_->Register(ti, true);
}

void BackendThread::ProcessNotifyEvents(const NetFiredEvent* pfe) {
  if (pfe->mask & kReadable) {
    char bb[2048];
    int32_t nread = read(net_multiplexer_->NotifyReceiveFd(), bb, 2048);
    if (nread == 0) {
      return;
    } else {
      for (int32_t idx = 0; idx < nread; ++idx) {
        NetItem ti = net_multiplexer_->NotifyQueuePop();
        int fd = ti.fd();
        std::string ip_port = ti.ip_port();
        pstd::MutexLock l(&mu_);
        if (ti.notify_type() == kNotiWrite) {
          if (conns_.find(fd) == conns_.end()) {
            // TODO: need clean and notify?
            continue;
          } else {
            // connection exist
            net_multiplexer_->NetModEvent(fd, 0, kReadable | kWritable);
          }
          {
            auto iter = to_send_.find(fd);
            if (iter == to_send_.end()) {
              continue;
            }
            // get msg from to_send_
            std::vector<std::string>& msgs = iter->second;
            for (auto& msg : msgs) {
              conns_[fd]->WriteResp(msg);
            }
            to_send_.erase(iter);
          }
        } else if (ti.notify_type() == kNotiClose) {
          log_info("received kNotiClose\n");
          net_multiplexer_->NetDelEvent(fd, 0);
          CloseFd(fd);
          conns_.erase(fd);
          connecting_fds_.erase(fd);
        }
      }
    }
  }
}

void* BackendThread::ThreadMain() {
  int nfds = 0;
  NetFiredEvent* pfe = NULL;

  struct timeval when;
  gettimeofday(&when, NULL);
  struct timeval now = when;

  when.tv_sec += (cron_interval_ / 1000);
  when.tv_usec += ((cron_interval_ % 1000) * 1000);
  int timeout = cron_interval_;
  if (timeout <= 0) {
    timeout = NET_CRON_INTERVAL;
  }

  std::string ip_port;

  while (!should_stop()) {
    if (cron_interval_ > 0) {
      gettimeofday(&now, nullptr);
      if (when.tv_sec > now.tv_sec || (when.tv_sec == now.tv_sec && when.tv_usec > now.tv_usec)) {
        timeout = (when.tv_sec - now.tv_sec) * 1000 + (when.tv_usec - now.tv_usec) / 1000;
      } else {
        // do user defined cron
        handle_->CronHandle();

        DoCronTask();
        when.tv_sec = now.tv_sec + (cron_interval_ / 1000);
        when.tv_usec = now.tv_usec + ((cron_interval_ % 1000) * 1000);
        timeout = cron_interval_;
      }
    }
    //{
    // InternalDebugPrint();
    //}
    nfds = net_multiplexer_->NetPoll(timeout);
    for (int i = 0; i < nfds; i++) {
      pfe = (net_multiplexer_->FiredEvents()) + i;
      if (pfe == NULL) {
        continue;
      }

      if (pfe->fd == net_multiplexer_->NotifyReceiveFd()) {
        ProcessNotifyEvents(pfe);
        continue;
      }

      int should_close = 0;
      mu_.Lock();
      std::map<int, std::shared_ptr<NetConn>>::iterator iter = conns_.find(pfe->fd);
      if (iter == conns_.end()) {
        mu_.Unlock();
        log_info("fd %d not found in fd_conns\n", pfe->fd);
        net_multiplexer_->NetDelEvent(pfe->fd, 0);
        continue;
      }
      mu_.Unlock();

      std::shared_ptr<NetConn> conn = iter->second;

      if (connecting_fds_.count(pfe->fd)) {
        Status s = ProcessConnectStatus(pfe, &should_close);
        if (!s.ok()) {
          handle_->DestConnectFailedHandle(conn->ip_port(), s.ToString());
        }
        connecting_fds_.erase(pfe->fd);
      }

      if (!should_close && (pfe->mask & kWritable) && conn->is_reply()) {
        WriteStatus write_status = conn->SendReply();
        conn->set_last_interaction(now);
        if (write_status == kWriteAll) {
          net_multiplexer_->NetModEvent(pfe->fd, 0, kReadable);
          conn->set_is_reply(false);
        } else if (write_status == kWriteHalf) {
          continue;
        } else {
          log_info("send reply error %d\n", write_status);
          should_close = 1;
        }
      }

      if (!should_close && (pfe->mask & kReadable)) {
        ReadStatus read_status = conn->GetRequest();
        conn->set_last_interaction(now);
        if (read_status == kReadAll) {
        } else if (read_status == kReadHalf) {
          continue;
        } else {
          log_info("Get request error %d\n", read_status);
          should_close = 1;
        }
      }

      if ((pfe->mask & kErrorEvent) || should_close) {
        {
          log_info("close connection %d reason %d %d\n", pfe->fd, pfe->mask, should_close);
          net_multiplexer_->NetDelEvent(pfe->fd, 0);
          CloseFd(conn);
          mu_.Lock();
          conns_.erase(pfe->fd);
          mu_.Unlock();
          if (connecting_fds_.count(conn->fd())) {
            connecting_fds_.erase(conn->fd());
          }
        }
      }
    }
  }
  return nullptr;
}

}  // namespace net
