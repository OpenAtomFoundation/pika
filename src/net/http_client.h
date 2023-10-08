#pragma once

#include <memory>

#include "http_parser.h"

namespace pikiwidb {
class TcpConnection;
class EventLoop;

class HttpClient : public std::enable_shared_from_this<HttpClient> {
 public:
  enum class ErrorCode {
    kNone,
    kConnectFail,      // connect failed, maybe dest port not exist
    kConnectTimeout,   // connect failed after timeout
    kConnectionReset,  // disconnect for some reason
    kTimeout,          // get response timeout
    kNoPipeline,       // not support http pipeline
  };
  using ErrorHandler = std::function<void(const HttpRequest&, ErrorCode)>;

  HttpClient();

  HttpClient(const HttpClient&) = delete;
  void operator=(const HttpClient&) = delete;

  // callback by framework
  void OnConnect(TcpConnection* conn);
  // callback by framework
  void OnConnectFail(const char* peer_ip, int port);
  // set timeout for this client
  void SetTimeout(int timeout_ms) { timeout_ms_ = timeout_ms; }
  // set idle timeout for this client
  void SetIdleTimeout(int timeout_ms) { idle_timeout_ms_ = timeout_ms; }
  // SendRequest is thread safe
  bool SendRequest(const HttpRequest& req, HttpResponseHandler handle, ErrorHandler err_handle = ErrorHandler());
  // set event loop
  void SetLoop(EventLoop* loop);

 private:
  struct RequestContext {
    HttpRequest request;
    HttpResponseHandler handle;
    ErrorHandler error_handle;
  };

  int Parse(TcpConnection*, const char* data, int len);
  void HandleResponse(const HttpResponse& rsp);
  bool DirectSendRequest(const HttpRequest& req, HttpResponseHandler handle, ErrorHandler err_handle);
  void OnDisconnect(TcpConnection*);
  void FlushBufferedRequest();
  void MaySetTimeout(const std::shared_ptr<RequestContext>&);

  bool never_connected_ = true;  // if true, buffer requests

  // use shared_ptr because I owned it and can be weak shared with timers.
  // hold request before connected
  std::shared_ptr<RequestContext> buffered_request_;
  std::shared_ptr<RequestContext> pending_request_;

  int timeout_ms_ = -1;
  int idle_timeout_ms_ = -1;

  HttpParser parser_;
  std::weak_ptr<TcpConnection> conn_;

  EventLoop* loop_ = nullptr;
};

}  // namespace pikiwidb