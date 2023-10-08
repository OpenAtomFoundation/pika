#include "http_client.h"

#include "event_loop.h"
#include "log.h"
#include "tcp_connection.h"

namespace pikiwidb {
HttpClient::HttpClient() : parser_(HTTP_RESPONSE) {}

void HttpClient::OnConnect(TcpConnection* conn) {
  assert(loop_ == conn->GetEventLoop());

  INFO("HttpClient::OnConnect to {}:{} in loop {}", conn->GetPeerIp(), conn->GetPeerPort(), loop_->GetName());
  never_connected_ = false;

  conn_ = std::static_pointer_cast<TcpConnection>(conn->shared_from_this());
  parser_.SetResponseHandler(std::bind(&HttpClient::HandleResponse, this, std::placeholders::_1));

  conn->SetContext(shared_from_this());
  conn->SetMessageCallback(
      std::bind(&HttpClient::Parse, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
  conn->SetOnDisconnect(std::bind(&HttpClient::OnDisconnect, this, std::placeholders::_1));
  conn->SetIdleTimeout(idle_timeout_ms_);

  FlushBufferedRequest();
}

void HttpClient::OnConnectFail(const char* peer_ip, int port) {
  assert(never_connected_);

  WARN("HttpClient::OnConnectFail to {}:{} in loop {}", peer_ip, port, loop_->GetName());
  if (buffered_request_) {
    const auto& req = *buffered_request_;
    if (req.error_handle) {
      req.error_handle(req.request, ErrorCode::kConnectFail);
    }

    WARN("HttpClient::OnConnectFail with request {}", req.request.Encode());
    buffered_request_.reset();
  }
}

bool HttpClient::SendRequest(const HttpRequest& req, HttpResponseHandler handle, ErrorHandler err_handle) {
  assert(loop_);

  if (loop_->InThisLoop()) {
    return DirectSendRequest(req, handle, err_handle);
  }

  loop_->Execute([this, req, handle, err_handle]() { this->DirectSendRequest(req, handle, err_handle); });
  return true;
}

void HttpClient::SetLoop(EventLoop* loop) { loop_ = loop; }

int HttpClient::Parse(TcpConnection*, const char* data, int len) {
  bool ok = parser_.Execute(data, len);
  if (!ok) {
    ERROR("failed parse http rsp: {}", data);
    return -1;
  }

  return len;
}

void HttpClient::HandleResponse(const HttpResponse& rsp) {
  if (!pending_request_) {
    WARN("http resp {} after timeout: {}", rsp.Encode(), timeout_ms_);
    return;
  }

  auto ctx = std::move(pending_request_);
  ctx->handle(rsp);
}

bool HttpClient::DirectSendRequest(const HttpRequest& req, HttpResponseHandler handle, ErrorHandler err_handle) {
  assert(loop_->InThisLoop());

  auto build_ctx = [&]() -> std::shared_ptr<RequestContext> {
    auto ctx = std::make_shared<RequestContext>();
    ctx->request = req;
    ctx->handle = std::move(handle);
    ctx->error_handle = std::move(err_handle);

    MaySetTimeout(ctx);
    return ctx;
  };

  auto conn = conn_.lock();
  if (!conn) {
    if (!never_connected_) {
      WARN("HttpClient conn once connected, but now reset");
      if (err_handle) {
        err_handle(req, ErrorCode::kConnectionReset);
      }
      return false;
    }

    // never connected
    if (buffered_request_) {
      if (err_handle) {
        err_handle(req, ErrorCode::kNoPipeline);
      }
      return false;  // not support pipeline
    }

    buffered_request_ = build_ctx();
    return true;
  }

  if (pending_request_) {
    if (err_handle) {
      err_handle(req, ErrorCode::kNoPipeline);
    }
    return false;  // not support pipeline
  }

  auto req_bytes = req.Encode();
  if (!conn->SendPacket(req_bytes.data(), req_bytes.size())) {
    if (err_handle) {
      err_handle(req, ErrorCode::kConnectionReset);
    }
    return false;
  }

  pending_request_ = build_ctx();
  return true;
}

void HttpClient::MaySetTimeout(const std::shared_ptr<RequestContext>& ctx) {
  if (!ctx->error_handle || timeout_ms_ <= 0) {
    return;
  }

  // set timeout handler
  auto w_cli(weak_from_this());
  std::weak_ptr<RequestContext> w_ctx(ctx);
  loop_->ScheduleLater(timeout_ms_, [w_cli, w_ctx]() {
    auto cli = w_cli.lock();
    if (!cli) {
      return;  // connection already lost
    }

    auto ctx = w_ctx.lock();
    if (!ctx) {
      return;  // already has response
    }

    if (cli->never_connected_) {
      ctx->error_handle(ctx->request, ErrorCode::kConnectTimeout);
    } else {
      ctx->error_handle(ctx->request, ErrorCode::kTimeout);
    }

    // clear context
    cli->buffered_request_.reset();
    cli->pending_request_.reset();
  });
}

void HttpClient::OnDisconnect(TcpConnection*) {
  if (pending_request_) {
    auto req = std::move(pending_request_);
    if (req->error_handle) {
      req->error_handle(req->request, ErrorCode::kConnectionReset);
    }

    WARN("HttpClient::OnDisconnect pending request {}", req->request.Encode());
  }
}

void HttpClient::FlushBufferedRequest() {
  assert(!never_connected_);

  if (buffered_request_) {
    auto req = std::move(buffered_request_);
    SendRequest(req->request, req->handle, req->error_handle);
    INFO("HttpClient send buffered request {}", req->request.Encode());
  }
}

}  // namespace pikiwidb
