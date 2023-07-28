// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "net/include/client_thread.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>

#include <glog/logging.h>

#include "net/include/net_conn.h"
#include "net/src/server_socket.h"
#include "pstd/include/pstd_string.h"
#include "pstd/include/xdebug.h"

namespace net {

using pstd::Status;

ClientThread::ClientThread(ConnFactory* conn_factory, int cron_interval, int keepalive_timeout, ClientHandle* handle,
                           void* private_data)
    : keepalive_timeout_(keepalive_timeout),
      cron_interval_(cron_interval),
      handle_(handle),
      private_data_(private_data),
      conn_factory_(conn_factory) {
  net_multiplexer_.reset(CreateNetMultiplexer());
  net_multiplexer_->Initialize();
}

ClientThread::~ClientThread() = default;

int ClientThread::StartThread() {
  if (!handle_) {
    handle_ = new ClientHandle();
    own_handle_ = true;
  }
  own_handle_ = false;
  int res = handle_->CreateWorkerSpecificData(&private_data_);
  if (res) {
    return res;
  }
  return Thread::StartThread();
}

int ClientThread::StopThread() {
  if (private_data_) {
    int res = handle_->DeleteWorkerSpecificData(private_data_);
    if (res) {
      return res;
    }
    private_data_ = nullptr;
  }
  if (own_handle_) {
    delete handle_;
  }
  return Thread::StopThread();
}

Status ClientThread::Write(const std::string& ip, const int port, const std::string& msg) {
  std::string ip_port = ip + ":" + std::to_string(port);
  if (!handle_->AccessHandle(ip_port)) {
    return Status::Corruption(ip_port + " is baned by user!");
  }
  {
    std::lock_guard l(mu_);
    size_t size = 0;
    for (auto& str : to_send_[ip_port]) {
      size += str.size();
    }
    if (size > kConnWriteBuf) {
      return Status::Corruption("Connection buffer over maximum size");
    }
    to_send_[ip_port].push_back(msg);
  }
  NotifyWrite(ip_port);
  return Status::OK();
}

Status ClientThread::Close(const std::string& ip, const int port) {
  {
    std::lock_guard l(to_del_mu_);
    to_del_.push_back(ip + ":" + std::to_string(port));
  }
  return Status::OK();
}

Status ClientThread::ProcessConnectStatus(NetFiredEvent* pfe, int* should_close) {
  if (pfe->mask & kErrorEvent) {
    *should_close = 1;
    return Status::Corruption("POLLERR or POLLHUP");
  }
  int val = 0;
  socklen_t lon = sizeof(int);

  if (getsockopt(pfe->fd, SOL_SOCKET, SO_ERROR, &val, &lon) == -1) {
    *should_close = 1;
    return Status::Corruption("Get Socket opt failed");
  }
  if (val) {
    *should_close = 1;
    return Status::Corruption("Get socket error " + std::to_string(val));
  }
  return Status::OK();
}

void ClientThread::SetWaitConnectOnEpoll(int sockfd) {
  net_multiplexer_->NetAddEvent(sockfd, kReadable | kWritable);
  connecting_fds_.insert(sockfd);
}

void ClientThread::NewConnection(const std::string& peer_ip, int peer_port, int sockfd) {
  std::string ip_port = peer_ip + ":" + std::to_string(peer_port);
  std::shared_ptr<NetConn> tc = conn_factory_->NewNetConn(sockfd, ip_port, this, nullptr, net_multiplexer_.get());
  tc->SetNonblock();
  // This flag specifies that the file descriptor should be closed when an exec function is invoked.
  fcntl(sockfd, F_SETFD, fcntl(sockfd, F_GETFD) | FD_CLOEXEC);

  fd_conns_.insert(std::make_pair(sockfd, tc));
  ipport_conns_.insert(std::make_pair(ip_port, tc));
}

Status ClientThread::ScheduleConnect(const std::string& dst_ip, int dst_port) {
  Status s;
  int sockfd = -1;
  int rv;
  char cport[6];
  struct addrinfo hints;
  struct addrinfo *servinfo;
  struct addrinfo *p;
  snprintf(cport, sizeof(cport), "%d", dst_port);
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;

  // We do not handle IPv6
  if (rv = getaddrinfo(dst_ip.c_str(), cport, &hints, &servinfo); rv) {
    return Status::IOError("connect getaddrinfo error for ", dst_ip);
  }
  for (p = servinfo; p != nullptr; p = p->ai_next) {
    if ((sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1) {
      continue;
    }
    int flags = fcntl(sockfd, F_GETFL, 0);
    fcntl(sockfd, F_SETFL, flags | O_NONBLOCK);

    if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
      if (errno == EHOSTUNREACH) {
        CloseFd(sockfd, dst_ip + ":" + std::to_string(dst_port));
        continue;
      } else if (errno == EINPROGRESS || errno == EAGAIN || errno == EWOULDBLOCK) {
        NewConnection(dst_ip, dst_port, sockfd);
        SetWaitConnectOnEpoll(sockfd);
        freeaddrinfo(servinfo);
        return Status::OK();
      } else {
        CloseFd(sockfd, dst_ip + ":" + std::to_string(dst_port));
        freeaddrinfo(servinfo);
        return Status::IOError("EHOSTUNREACH", "The target host cannot be reached");
      }
    }

    NewConnection(dst_ip, dst_port, sockfd);
    net_multiplexer_->NetAddEvent(sockfd, kReadable | kWritable);
    struct sockaddr_in laddr;
    socklen_t llen = sizeof(laddr);
    getsockname(sockfd, reinterpret_cast<struct sockaddr*>(&laddr), &llen);
    std::string lip(inet_ntoa(laddr.sin_addr));
    int lport = ntohs(laddr.sin_port);
    if (dst_ip == lip && dst_port == lport) {
      return Status::IOError("EHOSTUNREACH", "same ip port");
    }

    freeaddrinfo(servinfo);

    return s;
  }
  if (!p) {
    s = Status::IOError(strerror(errno), "Can't create socket ");
    return s;
  }
  freeaddrinfo(servinfo);
  freeaddrinfo(p);
  int val = 1;
  setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val));
  return s;
}

void ClientThread::CloseFd(const std::shared_ptr<NetConn>& conn) {
  close(conn->fd());
  CleanUpConnRemaining(conn->ip_port());
  handle_->FdClosedHandle(conn->fd(), conn->ip_port());
}

void ClientThread::CloseFd(int fd, const std::string& ip_port) {
  close(fd);
  CleanUpConnRemaining(ip_port);
  handle_->FdClosedHandle(fd, ip_port);
}

void ClientThread::CleanUpConnRemaining(const std::string& ip_port) {
  std::lock_guard l(mu_);
  to_send_.erase(ip_port);
}

void ClientThread::DoCronTask() {
  struct timeval now;
  gettimeofday(&now, nullptr);
  auto iter = fd_conns_.begin();
  while (iter != fd_conns_.end()) {
    std::shared_ptr<NetConn> conn = iter->second;

    // Check keepalive timeout connection
    if (keepalive_timeout_ > 0 && (now.tv_sec - conn->last_interaction().tv_sec > keepalive_timeout_)) {
      LOG(INFO) << "Do cron task del fd " << conn->fd();
      net_multiplexer_->NetDelEvent(conn->fd(), 0);
      // did not clean up content in to_send queue
      // will try to send remaining by reconnecting
      close(conn->fd());
      handle_->FdTimeoutHandle(conn->fd(), conn->ip_port());
      if (ipport_conns_.count(conn->ip_port())) {
        ipport_conns_.erase(conn->ip_port());
      }
      if (connecting_fds_.count(conn->fd())) {
        connecting_fds_.erase(conn->fd());
      }
      iter = fd_conns_.erase(iter);
      continue;
    }

    // Maybe resize connection buffer
    conn->TryResizeBuffer();

    ++iter;
  }

  std::vector<std::string> to_del;
  {
    std::lock_guard l(to_del_mu_);
    to_del = std::move(to_del_);
    to_del_.clear();
  }

  for (auto& conn_name : to_del) {
    auto iter = ipport_conns_.find(conn_name);
    if (iter == ipport_conns_.end()) {
      continue;
    }
    std::shared_ptr<NetConn> conn = iter->second;
    net_multiplexer_->NetDelEvent(conn->fd(), 0);
    CloseFd(conn);
    fd_conns_.erase(conn->fd());
    ipport_conns_.erase(conn->ip_port());
    connecting_fds_.erase(conn->fd());
  }
}

void ClientThread::InternalDebugPrint() {
  LOG(INFO) << "___________________________________";
  {
    std::lock_guard l(mu_);
    LOG(INFO) << "To send map: ";
    for (const auto& to_send : to_send_) {
      UNUSED(to_send);
      const std::vector<std::string>& tmp = to_send.second;
      for (const auto& tmp_to_send : tmp) {
        UNUSED(tmp_to_send);
        LOG(INFO) << to_send.first << " " << tmp_to_send;
      }
    }
  }
  LOG(INFO) << "Ipport conn map: ";
  for (const auto& ipport_conn : ipport_conns_) {
    UNUSED(ipport_conn);
    LOG(INFO) << "ipport " << ipport_conn.first;
  }
  LOG(INFO) << "Connected fd map: ";
  for (const auto& fd_conn : fd_conns_) {
    UNUSED(fd_conn);
    LOG(INFO) << "fd " << fd_conn.first;
  }
  LOG(INFO) << "Connecting fd map: ";
  for (const auto& connecting_fd : connecting_fds_) {
    UNUSED(connecting_fd);
    LOG(INFO) << "fd: " << connecting_fd;
  }
  LOG(INFO) << "___________________________________";
}

void ClientThread::NotifyWrite(const std::string& ip_port) {
  // put fd = 0, cause this lib user does not need to know which fd to write to
  // we will check fd by checking ipport_conns_
  NetItem ti(0, ip_port, kNotiWrite);
  net_multiplexer_->Register(ti, true);
}

void ClientThread::ProcessNotifyEvents(const NetFiredEvent* pfe) {
  if (pfe->mask & kReadable) {
    char bb[2048];
    int64_t nread = read(net_multiplexer_->NotifyReceiveFd(), bb, 2048);
    if (nread == 0) {
      return;
    } else {
      for (int32_t idx = 0; idx < nread; ++idx) {
        NetItem ti = net_multiplexer_->NotifyQueuePop();
        std::string ip_port = ti.ip_port();
        int fd = ti.fd();
        if (ti.notify_type() == kNotiWrite) {
          if (ipport_conns_.find(ip_port) == ipport_conns_.end()) {
            std::string ip;
            int port = 0;
            if (!pstd::ParseIpPortString(ip_port, ip, port)) {
              continue;
            }
            Status s = ScheduleConnect(ip, port);
            if (!s.ok()) {
              std::string ip_port = ip + ":" + std::to_string(port);
              handle_->DestConnectFailedHandle(ip_port, s.ToString());
              LOG(INFO) << "Ip " << ip << ", port " << port << " Connect err " << s.ToString();
              continue;
            }
          } else {
            // connection exist
            net_multiplexer_->NetModEvent(ipport_conns_[ip_port]->fd(), 0, kReadable | kWritable);
          }
          std::vector<std::string> msgs;
          {
            std::lock_guard l(mu_);
            auto iter = to_send_.find(ip_port);
            if (iter == to_send_.end()) {
              continue;
            }
            msgs.swap(iter->second);
          }
          // get msg from to_send_
          std::vector<std::string> send_failed_msgs;
          for (auto& msg : msgs) {
            if (ipport_conns_[ip_port]->WriteResp(msg)) {
              send_failed_msgs.push_back(msg);
            }
          }
          std::lock_guard l(mu_);
          if (!send_failed_msgs.empty()) {
            send_failed_msgs.insert(send_failed_msgs.end(), to_send_[ip_port].begin(),
                                    to_send_[ip_port].end());
            send_failed_msgs.swap(to_send_[ip_port]);
            NotifyWrite(ip_port);
          }
        } else if (ti.notify_type() == kNotiClose) {
          LOG(INFO) << "received kNotiClose";
          net_multiplexer_->NetDelEvent(fd, 0);
          CloseFd(fd, ip_port);
          fd_conns_.erase(fd);
          ipport_conns_.erase(ip_port);
          connecting_fds_.erase(fd);
        }
      }
    }
  }
}

void* ClientThread::ThreadMain() {
  int nfds = 0;
  NetFiredEvent* pfe = nullptr;

  struct timeval when;
  gettimeofday(&when, nullptr);
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
        timeout = static_cast<int32_t>((when.tv_sec - now.tv_sec) * 1000 + (when.tv_usec - now.tv_usec) / 1000);
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
      if (!pfe) {
        continue;
      }

      if (pfe->fd == net_multiplexer_->NotifyReceiveFd()) {
        ProcessNotifyEvents(pfe);
        continue;
      }

      int should_close = 0;
      auto iter = fd_conns_.find(pfe->fd);
      if (iter == fd_conns_.end()) {
        LOG(INFO) << "fd " << pfe->fd << "not found in fd_conns";
        net_multiplexer_->NetDelEvent(pfe->fd, 0);
        continue;
      }

      std::shared_ptr<NetConn> conn = iter->second;

      if (connecting_fds_.count(pfe->fd)) {
        Status s = ProcessConnectStatus(pfe, &should_close);
        if (!s.ok()) {
          handle_->DestConnectFailedHandle(conn->ip_port(), s.ToString());
        }
        connecting_fds_.erase(pfe->fd);
      }

      if ((should_close == 0) && (pfe->mask & kWritable) && conn->is_reply()) {
        WriteStatus write_status = conn->SendReply();
        conn->set_last_interaction(now);
        if (write_status == kWriteAll) {
          net_multiplexer_->NetModEvent(pfe->fd, 0, kReadable);
          conn->set_is_reply(false);
        } else if (write_status == kWriteHalf) {
          continue;
        } else {
          LOG(INFO) << "send reply error " << write_status;
          should_close = 1;
        }
      }

      if ((should_close == 0) && (pfe->mask & kReadable)) {
        ReadStatus read_status = conn->GetRequest();
        conn->set_last_interaction(now);
        if (read_status == kReadAll) {
          // net_multiplexer_->NetModEvent(pfe->fd, 0, EPOLLOUT);
        } else if (read_status == kReadHalf) {
          continue;
        } else {
          LOG(INFO) << "Get request error " << read_status;
          should_close = 1;
        }
      }

      if ((pfe->mask & kErrorEvent) || should_close) {
        {
          LOG(INFO) << "close connection " << pfe->fd << " reason " << pfe->mask << " " << should_close;
          net_multiplexer_->NetDelEvent(pfe->fd, 0);
          CloseFd(conn);
          fd_conns_.erase(pfe->fd);
          if (ipport_conns_.count(conn->ip_port())) {
            ipport_conns_.erase(conn->ip_port());
          }
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
