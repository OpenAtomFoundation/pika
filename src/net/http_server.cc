#include "http_server.h"

#include "event_loop.h"
#include "log.h"
#include "tcp_connection.h"

namespace pikiwidb {

static std::unique_ptr<HttpResponse> Handle404(const HttpRequest&, std::shared_ptr<HttpContext>) {
  std::unique_ptr<HttpResponse> rsp(new HttpResponse);
  rsp->SetCode(404);
  rsp->SetStatus("NOT FOUND");
  rsp->SetHeader("Content-Length", "0");
  return rsp;
}

HttpServer::Handler HttpServer::default_ = Handle404;

const HttpServer::Handler& HttpServer::GetHandler(const std::string& url) const {
  auto it = handlers_.find(url);
  if (it != handlers_.end()) {
    return it->second;
  }
  return HttpServer::default_;
}

void HttpServer::HandleFunc(const std::string& url, Handler handle) { handlers_[url] = handle; }

void HttpServer::SetOnNewHttpContext(OnNewClient on_new_http) { on_new_http_ctx_ = std::move(on_new_http); }

void HttpServer::OnNewConnection(TcpConnection* conn) {
  auto _ = std::static_pointer_cast<TcpConnection>(conn->shared_from_this());
  auto ctx = std::make_shared<HttpContext>(HTTP_REQUEST, _, this);

  conn->SetContext(ctx);
  conn->SetMessageCallback(
      std::bind(&HttpContext::Parse, ctx.get(), std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
  conn->SetOnDisconnect(std::bind(&HttpServer::OnDisconnect, this, std::placeholders::_1));

  if (idle_timeout_ms_ > 0) {
    conn->SetIdleTimeout(idle_timeout_ms_);
  }

  if (on_new_http_ctx_) {
    on_new_http_ctx_(ctx.get());
  }
}

void HttpServer::OnDisconnect(TcpConnection* conn) {
  INFO("disconnect http fd : {}", conn->Fd());
  auto it = contexts_.find(conn->GetUniqueId());
  if (it != contexts_.end()) {
    contexts_.erase(it);
  }
}

HttpContext::HttpContext(llhttp_type type, std::shared_ptr<TcpConnection> c, HttpServer* server)
    : parser_(type), conn_(c), server_(server) {
  parser_.SetRequestHandler(std::bind(&HttpContext::HandleRequest, this, std::placeholders::_1));
}

int HttpContext::Parse(TcpConnection*, const char* data, int len) {
  bool ok = parser_.Execute(data, len);
  if (!ok) {
    ERROR("failed parse http req: {}, reason {}", data, parser_.ErrorReason());
    return -1;
  }

  return len;
}

void HttpContext::HandleRequest(const HttpRequest& req) {
  const auto& url = req.Url();
  auto path_query = ParseUrl(url);
  const auto& handle = server_->GetHandler(path_query.path);

  INFO("http handler for {}, query {}", path_query.path, path_query.query);
  auto rsp = handle(req, shared_from_this());
  if (rsp) {
    SendResponse(*rsp);
  }
}

void HttpContext::SendResponse(const HttpResponse& rsp) {
  auto conn = conn_.lock();
  if (!conn) {
    ERROR("cant send response because lose connection");
    return;
  }

  std::string rsp_data = rsp.Encode();
  conn->SendPacket(rsp_data.data(), rsp_data.size());
}

}  // namespace pikiwidb
