#pragma once

#include "event2/listener.h"
#include "event_obj.h"
#include "tcp_connection.h"

namespace pikiwidb {
class EventLoop;

class TcpListener : public EventObject {
 public:
  explicit TcpListener(EventLoop* loop);
  ~TcpListener();

  bool Bind(const char* ip, int port);
  int Fd() const override;

  void SetNewConnCallback(NewTcpConnectionCallback cb) { on_new_conn_ = std::move(cb); }
  EventLoop* SelectEventLoop();

 private:
  static void OnNewConnection(struct evconnlistener*, evutil_socket_t, struct sockaddr*, int, void*);
  static void OnError(struct evconnlistener*, void*);

  EventLoop* const loop_;
  struct evconnlistener* listener_{nullptr};

  NewTcpConnectionCallback on_new_conn_;
};

}  // namespace pikiwidb
