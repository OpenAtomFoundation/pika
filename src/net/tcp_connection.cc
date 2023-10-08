/*
* Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
* This source code is licensed under the BSD-style license found in the
* LICENSE file in the root directory of this source tree. An additional grant
* of patent rights can be found in the PATENTS file in the same directory.
*/

#include "tcp_connection.h"

#include <netinet/tcp.h>

#include <cassert>
#include <memory>

#include "event2/event.h"
#include "event2/util.h"
#include "event_loop.h"
#include "log.h"
#include "util.h"

namespace pikiwidb {
TcpConnection::TcpConnection(EventLoop* loop) : loop_(loop) {
  memset(&peer_addr_, 0, sizeof peer_addr_);
  last_active_ = std::chrono::steady_clock::now();
}

TcpConnection::~TcpConnection() {
  if (idle_timer_ != -1) {
    loop_->Cancel(idle_timer_);
  }

  if (bev_) {
    INFO("close tcp fd {}", Fd());
    bufferevent_disable(bev_, EV_READ | EV_WRITE);
    bufferevent_free(bev_);
  }
}

void TcpConnection::OnAccept(int fd, const std::string& peer_ip, int peer_port) {
  assert(loop_->InThisLoop());

  peer_ip_ = peer_ip;
  peer_port_ = peer_port;
  peer_addr_ = MakeSockaddr(peer_ip.c_str(), peer_port);

  evutil_make_socket_nonblocking(fd);
  evutil_make_socket_closeonexec(fd);

  auto base = reinterpret_cast<struct event_base*>(loop_->GetReactor()->Backend());
  bev_ = bufferevent_socket_new(base, fd, BEV_OPT_CLOSE_ON_FREE);
  assert(bev_);

  HandleConnect();
}

bool TcpConnection::Connect(const char* ip, int port) {
  assert(loop_->InThisLoop());

  if (state_ != State::kNone) {
    ERROR("repeat connect tcp socket to {}:{}", ip, port);
    return false;
  }

  // new bufferevent then connect
  auto base = reinterpret_cast<struct event_base*>(loop_->GetReactor()->Backend());
  auto bev = bufferevent_socket_new(base, -1, BEV_OPT_CLOSE_ON_FREE);
  if (!bev) {
    ERROR("can't new bufferevent");
    return false;
  }
  bufferevent_setcb(bev, nullptr, nullptr, &TcpConnection::OnEvent, this);

  sockaddr_in addr = MakeSockaddr(ip, port);
  int err = bufferevent_socket_connect(bev, (struct sockaddr*)&addr, int(sizeof addr));
  if (err != 0) {
    ERROR("bufferevent_socket_connect failed to {}:{}", ip, port);
    bufferevent_free(bev);
    return false;
  }

  // success, keep this
  if (!loop_->Register(shared_from_this(), 0)) {
    ERROR("add tcp obj to loop failed, fd {}", bufferevent_getfd(bev));
    bufferevent_free(bev);
    return false;
  }

  INFO("in loop {}, trying connect to {}:{}", loop_->GetName(), ip, port);
  // update state
  bev_ = bev;
  peer_ip_ = ip;
  peer_port_ = port;
  peer_addr_ = addr;
  state_ = State::kConnecting;

  return true;
}

int TcpConnection::Fd() const {
  if (bev_) {
    return bufferevent_getfd(bev_);
  }

  return -1;
}

bool TcpConnection::SendPacket(const void* data, size_t size) {
  if (state_ != State::kConnected) {
    ERROR("send tcp data in wrong state {}", static_cast<int>(state_));
    return false;
  }

  if (!data || size == 0) {
    return true;
  }

  if (loop_->InThisLoop()) {
    auto output = bufferevent_get_output(bev_);
    evbuffer_add(output, data, size);
  } else {
    auto w_obj(weak_from_this());
    loop_->Execute([w_obj, data, size]() {
      auto c = w_obj.lock();
      if (!c) {
        return;  // connection already lost
      }

      auto tcp_conn = std::static_pointer_cast<TcpConnection>(c);
      auto output = bufferevent_get_output(tcp_conn->bev_);
      evbuffer_add(output, data, size);
    });
  }
  return true;
}

bool TcpConnection::SendPacket(const evbuffer_iovec* iovecs, size_t nvecs) {
  if (state_ != State::kConnected) {
    ERROR("send tcp data in wrong state {}", static_cast<int>(state_));
    return false;
  }

  if (!iovecs || nvecs <= 0) {
    return true;
  }

  if (loop_->InThisLoop()) {
      auto output = bufferevent_get_output(bev_);
      evbuffer_add_iovec(output, const_cast<evbuffer_iovec*>(iovecs), nvecs);
  } else {
    std::vector<std::string> buffers;
    for (int i = 0; i < nvecs; ++i) {
      buffers.emplace_back(static_cast<char*>(iovecs[i].iov_base), iovecs[i].iov_len);
    }

    auto w_obj(weak_from_this());
    loop_->Execute([w_obj, buffers = std::move(buffers)]() {
      std::vector<evbuffer_iovec> buffersSlices;
      for (auto& buffer : buffers) {
        buffersSlices.emplace_back(evbuffer_iovec{const_cast<char*>(buffer.data()), buffer.size()});
      }

      auto c = w_obj.lock();
      if (!c) {
        return;  // connection already lost
      }

      auto tcp_conn = std::static_pointer_cast<TcpConnection>(c);
      auto output = bufferevent_get_output(tcp_conn->bev_);
      evbuffer_add_iovec(output, const_cast<evbuffer_iovec*>(buffersSlices.data()), buffersSlices.size());
    });
  }

  return true;
}

void TcpConnection::HandleConnect() {
  assert(loop_->InThisLoop());
  assert(state_ == State::kNone || state_ == State::kConnecting);
  INFO("HandleConnect success with {}:{}", peer_ip_, peer_port_);

  state_ = State::kConnected;
  bufferevent_setcb(bev_, &TcpConnection::OnRecvData, nullptr, &TcpConnection::OnEvent, this);
  bufferevent_enable(bev_, EV_READ);

  if (on_new_conn_) {
    on_new_conn_(this);
  }
}

void TcpConnection::HandleConnectFailed() {
  assert(loop_->InThisLoop());
  assert(state_ == State::kConnecting);
  ERROR("HandleConnectFailed to {}:{}", peer_ip_, peer_port_);

  state_ = State::kFailed;
  if (on_fail_) {
    on_fail_(loop_, peer_ip_.c_str(), peer_port_);
  }

  loop_->Unregister(shared_from_this());
}

void TcpConnection::HandleDisconnect() {
  assert(loop_->InThisLoop());
  assert(state_ == State::kConnected);

  state_ = State::kDisconnected;
  if (on_disconnect_) {
    on_disconnect_(this);
  }

  loop_->Unregister(shared_from_this());
}

void TcpConnection::SetIdleTimeout(int timeout_ms) {
  if (timeout_ms <= 0) {
    return;
  }

  idle_timeout_ms_ = timeout_ms;
  if (idle_timer_ != -1) {
    loop_->Cancel(idle_timer_);
  }

  auto w_obj(weak_from_this());
  // Actual precision is 0.1s.
  idle_timer_ = loop_->ScheduleRepeatedly(100, [w_obj]() {
    auto c = w_obj.lock();
    if (!c) {
      return;  // connection already lost
    }

    auto tcp_conn = std::static_pointer_cast<TcpConnection>(c);
    bool timeout = tcp_conn->CheckIdleTimeout();
    if (timeout) {
      tcp_conn->ActiveClose(false);
    }
  });
}

void TcpConnection::SetNodelay(bool enable) {
  if (bev_) {
    int fd = bufferevent_getfd(bev_);
    int nodelay = enable ? 1 : 0;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (const char*)&nodelay, sizeof(int));
  }
}

bool TcpConnection::CheckIdleTimeout() const {
  using namespace std::chrono;

  int elapsed = static_cast<int>(duration_cast<milliseconds>(steady_clock::now() - last_active_).count());
  if (elapsed > idle_timeout_ms_) {
    WARN("TcpConnection::Timeout: elapsed {}, idle timeout {}, peer {}:{}", elapsed, idle_timeout_ms_, peer_ip_,
         peer_port_);
    return true;
  }

  return false;
}

void TcpConnection::OnRecvData(struct bufferevent* bev, void* obj) {
  auto me = std::static_pointer_cast<TcpConnection>(reinterpret_cast<TcpConnection*>(obj)->shared_from_this());

  assert(me->loop_->InThisLoop());
  assert(me->bev_ == bev);

  if (me->idle_timer_ != -1) {
    me->last_active_ = std::chrono::steady_clock::now();
  }

  auto input = bufferevent_get_input(bev);
  evbuffer_pullup(input, -1);

  struct evbuffer_iovec data[1];
  int nvecs = evbuffer_peek(input, -1, nullptr, data, 1);
  if (nvecs != 1) {
    return;
  }

  const char* start = reinterpret_cast<const char*>(data[0].iov_base);
  const int len = static_cast<int>(data[0].iov_len);
  int total_consumed = 0;
  bool error = false;
  while (!error && total_consumed < len) {
    int consumed = me->on_message_(me.get(), start + total_consumed, len - total_consumed);
    if (consumed > 0) {
      total_consumed += consumed;
    } else {
      if (consumed < 0) {
        error = true;
      }

      break;
    }
  }

  if (total_consumed > 0) {
    evbuffer_drain(input, total_consumed);
  }

  if (error) {
    me->HandleDisconnect();
  }
}

void TcpConnection::OnEvent(struct bufferevent* bev, short events, void* obj) {
  auto me = std::static_pointer_cast<TcpConnection>(reinterpret_cast<TcpConnection*>(obj)->shared_from_this());

  assert(me->loop_->InThisLoop());

  INFO("TcpConnection::OnEvent {:x}, state {}, obj {}", events, static_cast<int>(me->state_), obj);

  switch (me->state_) {
    case State::kConnecting:
      if (events & BEV_EVENT_CONNECTED) {
        me->HandleConnect();
      } else {
        me->HandleConnectFailed();
      }
      return;

    case State::kConnected:
      if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR)) {
        me->HandleDisconnect();
      }
      return;

    default:
      ERROR("TcpConnection::OnEvent wrong state {}", static_cast<int>(me->state_));
      return;
  }
}

void TcpConnection::SetContext(std::shared_ptr<void> ctx) { context_ = std::move(ctx); }

void TcpConnection::ActiveClose(bool sync) {
  // weak: don't prolong life of this
  std::weak_ptr<TcpConnection> me = std::static_pointer_cast<TcpConnection>(shared_from_this());
  auto destroy = [me]() {
    auto conn = me.lock();
    if (conn && conn->state_ == State::kConnected) {
      conn->HandleDisconnect();
    }
  };

  if (loop_->InThisLoop()) {
    destroy();
  } else {
    auto fut = loop_->Execute([=]() { destroy(); });
    if (sync) {
      fut.get();
    }
  }
}

bool TcpConnection::Connected() const { return state_ == State::kConnected; }

}  // namespace pikiwidb
