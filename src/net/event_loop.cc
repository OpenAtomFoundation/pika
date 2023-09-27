/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "event_loop.h"

#if defined(__gnu_linux__)
#  include <sys/prctl.h>
#endif
#include <unistd.h>

#include "libevent_reactor.h"
#include "log.h"
#include "util.h"

namespace pikiwidb {

static thread_local EventLoop* g_this_loop = nullptr;

std::atomic<int> EventLoop::obj_id_generator_{0};
std::atomic<TimerId> EventLoop::timerid_generator_{0};

void EventLoop::Init() {
  if (g_this_loop) {
    printf("There must be only one EventLoop per thread\n");
    abort();
  }
  g_this_loop = this;

  reactor_.reset(new internal::LibeventReactor());
  notifier_ = std::make_shared<internal::PipeObject>();
}

void EventLoop::Run() {
#if defined(__gnu_linux__)
  if (!name_.empty()) {
    prctl(PR_SET_NAME, ToValue<unsigned long>(name_.c_str()));
  }
#endif

  Register(notifier_, kEventRead);
  while (running_) {
    if (task_mutex_.try_lock()) {
      decltype(tasks_) funcs;
      funcs.swap(tasks_);
      task_mutex_.unlock();

      for (const auto& f : funcs) {
        f();
      }
    }

    if (!reactor_->Poll()) {
      ERROR("Reactor poll failed");
    }
  }

  for (auto& pair : objects_) {
    reactor_->Unregister(pair.second.get());
  }

  objects_.clear();
  reactor_.reset();
}

void EventLoop::Stop() {
  running_ = false;
  notifier_->Notify();
}

std::future<bool> EventLoop::Cancel(TimerId id) {
  if (InThisLoop()) {
    bool ok = reactor_ ? reactor_->Cancel(id) : false;

    std::promise<bool> prom;
    auto fut = prom.get_future();
    prom.set_value(ok);
    return fut;
  } else {
    auto fut = Execute([id, this]() -> bool {
      if (!reactor_) {
        return false;
      }
      bool ok = reactor_->Cancel(id);
      INFO("cancell timer {} {}", id, ok ? "succ" : "fail");
      return ok;
    });
    return fut;
  }
}

bool EventLoop::InThisLoop() const {
//  printf("EventLoop::InThisLoop this %p, g_this_loop %p\n", this, g_this_loop);
  return this == g_this_loop;
}

EventLoop* EventLoop::Self() { return g_this_loop; }

bool EventLoop::Register(std::shared_ptr<EventObject> obj, int events) {
  if (!obj) return false;

  assert(InThisLoop());
  assert(obj->GetUniqueId() == -1);

  if (!reactor_) {
    return false;
  }

  // alloc unique id
  int id = -1;
  do {
    id = obj_id_generator_.fetch_add(1) + 1;
    if (id < 0) {
      obj_id_generator_.store(0);
    }
  } while (id < 0 || objects_.count(id) != 0);

  obj->SetUniqueId(id);
  if (reactor_->Register(obj.get(), events)) {
    objects_.insert({obj->GetUniqueId(), obj});
    return true;
  }

  return false;
}

bool EventLoop::Modify(std::shared_ptr<EventObject> obj, int events) {
  if (!obj) return false;

  assert(InThisLoop());
  assert(obj->GetUniqueId() >= 0);
  assert(objects_.count(obj->GetUniqueId()) == 1);

  if (!reactor_) {
    return false;
  }
  return reactor_->Modify(obj.get(), events);
}

void EventLoop::Unregister(std::shared_ptr<EventObject> obj) {
  if (!obj) return;

  int id = obj->GetUniqueId();
  assert(InThisLoop());
  assert(id >= 0);
  assert(objects_.count(id) == 1);

  if (!reactor_) {
    return;
  }
  reactor_->Unregister(obj.get());
  objects_.erase(id);
}

bool EventLoop::Listen(const char* ip, int port, NewTcpConnectionCallback ccb, EventLoopSelector selector) {
  auto s = std::make_shared<TcpListener>(this);
  s->SetNewConnCallback(ccb);
  s->SetEventLoopSelector(selector);

  return s->Bind(ip, port);
}

std::shared_ptr<TcpConnection> EventLoop::Connect(const char* ip, int port, NewTcpConnectionCallback ccb, TcpConnectionFailCallback fcb) {
  auto c = std::make_shared<TcpConnection>(this);
  c->SetNewConnCallback(ccb);
  c->SetFailCallback(fcb);

  if (!c->Connect(ip, port)) {
    c.reset();
  }

  return c;
}

std::shared_ptr<HttpServer> EventLoop::ListenHTTP(const char* ip, int port, EventLoopSelector selector, HttpServer::OnNewClient cb) {
  auto server = std::make_shared<HttpServer>();
  server->SetOnNewHttpContext(std::move(cb));

  // capture server to make it long live with TcpListener
  auto ncb = [server](TcpConnection* conn) {
    server->OnNewConnection(conn);
  };
  Listen(ip, port, ncb, selector);

  return server;
}

std::shared_ptr<HttpClient> EventLoop::ConnectHTTP(const char* ip, int port) {
  auto client = std::make_shared<HttpClient>();

  // capture client to make it long live with TcpConnection
  auto ncb = [client](TcpConnection* conn) { client->OnConnect(conn); };
  auto fcb = [client](EventLoop*, const char* ip, int port) { client->OnConnectFail(ip, port); };

  client->SetLoop(this);
  Connect(ip, port, std::move(ncb), std::move(fcb));

  return client;
}

void EventLoop::Reset() {
  for (auto& kv : objects_) {
    Unregister(kv.second);
  }
  objects_.clear();

  {
    std::unique_lock<std::mutex> guard(task_mutex_);
    tasks_.clear();
  }

  reactor_.reset(new internal::LibeventReactor());
  notifier_ = std::make_shared<internal::PipeObject>();
}

}  // namespace pikiwidb
