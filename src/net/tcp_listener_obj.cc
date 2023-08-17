#include "tcp_listener_obj.h"

#include <errno.h>
#include <unistd.h>

#include <string>

//#include "application.h"
#include "event_loop.h"
#include "log.h"
#include "util.h"

namespace pikiwidb {
TcpListenerObj::TcpListenerObj(EventLoop* loop) : loop_(loop) {}

TcpListenerObj::~TcpListenerObj() {
  if (listener_) {
    INFO("close tcp listener fd {}", Fd());
    evconnlistener_free(listener_);
  }
}

bool TcpListenerObj::Bind(const char* ip, int port) {
  if (listener_) {
    ERROR("repeat bind tcp socket to port {}", port);
    return false;
  }

  sockaddr_in addr = MakeSockaddr(ip, port);
  auto base = reinterpret_cast<struct event_base*>(loop_->GetReactor()->Backend());
  auto listener =
      evconnlistener_new_bind(base, &TcpListenerObj::OnNewConnection, this,
                              LEV_OPT_CLOSE_ON_EXEC | LEV_OPT_CLOSE_ON_FREE | LEV_OPT_REUSEABLE | LEV_OPT_DISABLED, -1,
                              (const struct sockaddr*)&addr, int(sizeof(addr)));
  if (!listener) {
    ERROR("failed listen tcp port {}", port);
    return false;
  }

  evconnlistener_set_error_cb(listener, &TcpListenerObj::OnError);
  if (!loop_->Register(shared_from_this(), 0)) {
    ERROR("add tcp listener to loop failed, socket {}", Fd());
    evconnlistener_free(listener);
    return false;
  }

  INFO("tcp listen on port {}", port);
  listener_ = listener;
  evconnlistener_enable(listener_);
  return true;
}

int TcpListenerObj::Fd() const {
  if (listener_) {
    return static_cast<int>(evconnlistener_get_fd(listener_));
  }

  return -1;
}

EventLoop* TcpListenerObj::SelectEventloop() {
  if (loop_selector_) {
    return loop_selector_();
  }

  return loop_;
}

void TcpListenerObj::OnNewConnection(struct evconnlistener*, evutil_socket_t fd, struct sockaddr* peer, int,
                                     void* obj) {
  auto acceptor = reinterpret_cast<TcpListenerObj*>(obj);
  if (acceptor->on_new_conn_) {
    // convert address
    std::string ipstr = GetSockaddrIp(peer);
    int port = GetSockaddrPort(peer);
    if (ipstr.empty() || port == -1) {
      ERROR("invalid peer address for tcp fd {}", fd);
      close(fd);
      return;
    }

    INFO("new conn fd {} from {}", fd, ipstr);

    // make new conn
    auto loop = acceptor->SelectEventloop();
    // Application::Instance().Next();
    auto on_create = acceptor->on_new_conn_;  // cpp11 doesn't support lambda
                                              // capture initializers
    auto create_conn = [loop, on_create, fd, ipstr, port]() {
      auto conn(std::make_shared<TcpObject>(loop));
      conn->SetNewConnCallback(on_create);
      conn->OnAccept(fd, ipstr, port);
      if (!loop->Register(conn, 0)) {
        ERROR("Failed to register socket {}", fd);
      }
    };
    loop->Execute(std::move(create_conn));
  } else {
    WARN("close new conn fd {}", fd);
    close(fd);
  }
}

void TcpListenerObj::OnError(struct evconnlistener* listener, void* obj) {
  auto acceptor = reinterpret_cast<TcpListenerObj*>(obj);
  INFO("listener fd {} with errno {}", acceptor->Fd(), errno);

  // man 2 accept. TODO alert
  switch (errno) {
    case EAGAIN:
    case EINTR:
    case ECONNABORTED:
    case EPROTO:
      return;

    case EMFILE:
    case ENFILE:
      ERROR("not enough file descriptor, error is {}", errno);
      return;

    case ENOBUFS:
    case ENOMEM:
      ERROR("not enough memory, socket buffer limits");
      return;

    default:
      ERROR("BUG: accept with errno = {}", errno);
      assert(false);
      break;
  }
}
}  // namespace pikiwidb
