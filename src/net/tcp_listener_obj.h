#pragma once

#include "event2/listener.h"
#include "event_obj.h"
#include "tcp_obj.h"

namespace pikiwidb {
class EventLoop;

class TcpListenerObj : public EventObject {
 public:
  explicit TcpListenerObj(EventLoop* loop);
  ~TcpListenerObj();

  bool Bind(const char* ip, int port);
  int Fd() const override;

  void SetNewConnCallback(NewTcpConnCallback cb) { on_new_conn_ = std::move(cb); }
  EventLoop* SelectEventLoop();

 private:
  static void OnNewConnection(struct evconnlistener*, evutil_socket_t, struct sockaddr*, int, void*);
  static void OnError(struct evconnlistener*, void*);

  EventLoop* const loop_;
  struct evconnlistener* listener_{nullptr};

  NewTcpConnCallback on_new_conn_;
};

}  // namespace pikiwidb
