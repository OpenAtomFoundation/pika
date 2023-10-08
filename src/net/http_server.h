#pragma once

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

#include "http_parser.h"

namespace pikiwidb {
class TcpConnection;
class HttpContext;

class HttpServer {
 public:
  using Handler = std::function<std::unique_ptr<HttpResponse>(const HttpRequest&, std::shared_ptr<HttpContext>)>;
  using HandlerMap = std::unordered_map<std::string, Handler>;
  using OnNewClient = std::function<void(HttpContext*)>;

  const Handler& GetHandler(const std::string& url) const;
  void HandleFunc(const std::string& url, Handler handle);
  void SetOnNewHttpContext(OnNewClient on_new_http);

  void OnNewConnection(TcpConnection* conn);

  // set idle timeout for this client
  void SetIdleTimeout(int timeout_ms) { idle_timeout_ms_ = timeout_ms; }

 private:
  void OnDisconnect(TcpConnection* conn);

  std::unordered_map<int, std::shared_ptr<HttpContext>> contexts_;
  HandlerMap handlers_;
  OnNewClient on_new_http_ctx_;
  int idle_timeout_ms_ = -1;

  static Handler default_;
};

class HttpContext : public std::enable_shared_from_this<HttpContext> {
 public:
  HttpContext(llhttp_type type, std::shared_ptr<TcpConnection> c, HttpServer* server);

  int Parse(TcpConnection*, const char* data, int len);

  void SendResponse(const HttpResponse& rsp);

 private:
  // entry for handle http request
  void HandleRequest(const HttpRequest& req);

  HttpParser parser_;
  std::weak_ptr<TcpConnection> conn_;  // users may prolong life of ctx
  HttpServer* server_ = nullptr;
};

}  // namespace pikiwidb