#pragma once

#include <chrono>
#include <functional>
#include <memory>
#include <string>

#include "event2/buffer.h"
#include "event2/bufferevent.h"
#include "event_obj.h"
#include "reactor.h"
#include "unbounded_buffer.h"

namespace pikiwidb {
class EventLoop;
class TcpConnection;

// init a new tcp connection which from ::accept or ::connect
using NewTcpConnectionCallback = std::function<void(TcpConnection*)>;
// called when got incoming data, return bytes of consumed, -1 means fatal
using TcpMessageCallback = std::function<int(TcpConnection*, const char* data, int len)>;
// called when connect failed, usually retry or report error
using TcpConnectionFailCallback = std::function<void(EventLoop*, const char* peer_ip, int port)>;
// called when a connection being reset
using TcpDisconnectCallback = std::function<void(TcpConnection*)>;

// After client connects the server or the server accepts a new client,
// the pikiwidb will create a TcpConnection to handle the connection.
class TcpConnection : public EventObject {
 public:
  explicit TcpConnection(EventLoop* loop);
  ~TcpConnection();

  // init tcp object by result of ::accept
  void OnAccept(int fd, const std::string& peer_ip, int peer_port);
  // init tcp object by trying ::connect
  bool Connect(const char* ip, int port);

  int Fd() const override;

  bool SendPacket(const std::string& buf) { return SendPacket(buf.data(), buf.size()); }
  bool SendPacket(const void*, size_t);
  bool SendPacket(UnboundedBuffer& data) { return SendPacket(data.ReadAddr(), data.ReadableSize()); }
  bool SendPacket(const evbuffer_iovec* iovecs, size_t nvecs);

  void SetNewConnCallback(NewTcpConnectionCallback cb) { on_new_conn_ = std::move(cb); }
  void SetOnDisconnect(TcpDisconnectCallback cb) { on_disconnect_ = std::move(cb); }
  void SetMessageCallback(TcpMessageCallback cb) { on_message_ = std::move(cb); }
  void SetFailCallback(TcpConnectionFailCallback cb) { on_fail_ = std::move(cb); }

  // connection context
  template <typename T>
  std::shared_ptr<T> GetContext() const;
  void SetContext(std::shared_ptr<void> ctx);

  EventLoop* GetEventLoop() const { return loop_; }
  const std::string& GetPeerIp() const { return peer_ip_; }
  int GetPeerPort() const { return peer_port_; }
  const sockaddr_in& PeerAddr() const { return peer_addr_; }

  // if sync == true, wait connection closed,
  // otherwise when return, connection maybe alive for a while
  void ActiveClose(bool sync = false);
  bool Connected() const;

  // set idle timeout for this client
  void SetIdleTimeout(int timeout_s);

  // Nagle algorithm
  void SetNodelay(bool enable);

 private:
  // check if idle timeout
  bool CheckIdleTimeout() const;

  static void OnRecvData(struct bufferevent* bev, void* ctx);
  static void OnEvent(struct bufferevent* bev, short what, void* ctx);

  void HandleConnect();
  void HandleConnectFailed();
  void HandleDisconnect();

  enum class State {
    kNone,
    kConnecting,
    kConnected,
    kDisconnected,  // unrecoverable but once connected before
    kFailed,        // unrecoverable and never connected
  };

  State state_ = State::kNone;

  EventLoop* const loop_;
  struct bufferevent* bev_ = nullptr;

  std::string peer_ip_;
  int peer_port_ = -1;
  struct sockaddr_in peer_addr_;

  TcpMessageCallback on_message_;
  TcpDisconnectCallback on_disconnect_;
  TcpConnectionFailCallback on_fail_;
  NewTcpConnectionCallback on_new_conn_;

  TimerId idle_timer_ = -1;
  int idle_timeout_ms_ = 0;
  std::chrono::steady_clock::time_point last_active_;

  std::shared_ptr<void> context_;
};

template <typename T>
inline std::shared_ptr<T> TcpConnection::GetContext() const {
  return std::static_pointer_cast<T>(context_);
}

}  // namespace pikiwidb

