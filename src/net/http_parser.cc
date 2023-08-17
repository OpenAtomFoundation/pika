#include "http_parser.h"

#include <algorithm>

#include "util.h"

namespace pikiwidb {

HttpParser::HttpParser(llhttp_type type) : type_(type) {
  llhttp_settings_init(&settings_);

  // request
  settings_.on_url = &HttpParser::OnUrl;
  settings_.on_url_complete = &HttpParser::OnUrlComplete;

  // response
  settings_.on_status = &HttpParser::OnStatus;
  settings_.on_status_complete = &HttpParser::OnStatusComplete;

  // both
  settings_.on_message_begin = &HttpParser::OnMessageBegin;
  settings_.on_message_complete = &HttpParser::OnMessageComplete;
  settings_.on_header_field = &HttpParser::OnHeaderField;
  settings_.on_header_value = &HttpParser::OnHeaderValue;
  settings_.on_headers_complete = &HttpParser::OnHeadersComplete;
  settings_.on_header_field_complete = &HttpParser::OnHeaderFieldComplete;
  settings_.on_header_value_complete = &HttpParser::OnHeaderValueComplete;
  settings_.on_body = &HttpParser::OnBody;

  llhttp_init(&parser_, type, &settings_);
  parser_.data = this;
}

bool HttpParser::Execute(const std::string& data) { return Execute(data.c_str(), data.size()); }

bool HttpParser::Execute(const char* data, size_t len) {
  if (!error_reason_.empty()) {
    return false;
  }

  auto err = llhttp_execute(&parser_, data, len);
  if (err != HPE_OK) {
    error_reason_ = llhttp_get_error_reason(&parser_);
    return false;
  }

  return true;
}

void HttpParser::Reset() {
  complete_ = false;
  llhttp_reset(&parser_);

  request_.Reset();
  response_.Reset();

  key_.clear();
  value_.clear();
  error_reason_.clear();
}

int HttpParser::OnMessageBegin(llhttp_t* h) {
  HttpParser* parser = (HttpParser*)h->data;
  parser->Reset();

  return 0;
};

int HttpParser::OnMessageComplete(llhttp_t* h) {
  HttpParser* parser = (HttpParser*)h->data;
  parser->complete_ = true;
  if (parser->IsRequest()) {
    parser->request_.SetMethod(llhttp_method_t(parser->parser_.method));
    if (parser->req_handler_) {
      parser->req_handler_(parser->request_);
    }
  } else {
    parser->response_.SetCode(parser->parser_.status_code);
    if (parser->rsp_handler_) {
      parser->rsp_handler_(parser->response_);
    }
  }

  return 0;
}

int HttpParser::OnUrl(llhttp_t* h, const char* data, size_t len) {
  HttpParser* parser = (HttpParser*)h->data;
  if (!parser->IsRequest()) {
    return -1;
  }
  parser->request_.AppendUrl(data, len);

  return 0;
}

int HttpParser::OnHeaderField(llhttp_t* h, const char* data, size_t len) {
  if (len > 0) {
    HttpParser* parser = (HttpParser*)h->data;
    parser->key_.append(data, len);
  }

  return 0;
}

int HttpParser::OnHeaderValue(llhttp_t* h, const char* data, size_t len) {
  if (len > 0) {
    HttpParser* parser = (HttpParser*)h->data;
    parser->value_.append(data, len);
  }

  return 0;
}

int HttpParser::OnStatus(llhttp_t* h, const char* data, size_t len) {
  if (len == 0) {
    return 0;
  }

  HttpParser* parser = (HttpParser*)h->data;
  if (parser->IsRequest()) {
    return -1;
  }
  parser->response_.AppendStatus(data, len);

  return 0;
}

int HttpParser::OnHeadersComplete(llhttp_t* h) { return 0; }

int HttpParser::OnBody(llhttp_t* h, const char* data, size_t len) {
  if (len == 0) {
    return 0;
  }

  HttpParser* parser = (HttpParser*)h->data;
  if (parser->IsRequest()) {
    parser->request_.AppendBody(data, len);
  } else {
    parser->response_.AppendBody(data, len);
  }

  return 0;
}

int HttpParser::OnUrlComplete(llhttp_t* h) { return 0; }

int HttpParser::OnStatusComplete(llhttp_t* h) { return 0; }

int HttpParser::OnHeaderFieldComplete(llhttp_t* h) { return 0; }

int HttpParser::OnHeaderValueComplete(llhttp_t* h) {
  HttpParser* parser = (HttpParser*)h->data;
  Trim(parser->key_);
  if (parser->key_.empty()) {
    return HPE_INVALID_HEADER_TOKEN;
  }

  if (parser->IsRequest()) {
    parser->request_.SetHeader(parser->key_, parser->value_);
  } else {
    parser->response_.SetHeader(parser->key_, parser->value_);
  }

  parser->key_.clear();
  parser->value_.clear();
  return 0;
}

void HttpParser::Reinit() {
  if (error_reason_.empty()) {
    return;
  }

  error_reason_.clear();
  llhttp_init(&parser_, type_, &settings_);
  parser_.data = this;
  Reset();
}

#define CRLF "\r\n"

// HTTP request
void HttpRequest::Reset() {
  method_ = llhttp_method_t(-1);
  url_.clear();
  headers_.clear();
  body_.clear();
}

void HttpRequest::Swap(HttpRequest& other) {
  std::swap(method_, other.method_);
  url_.swap(other.url_);
  headers_.swap(other.headers_);
  body_.swap(other.body_);
}

std::string HttpRequest::Encode() const {
  std::string buf;
  buf.reserve(512);

  buf.append(MethodString());
  buf.append(" ");
  buf.append(url_);
  // XX : no query
  buf.append(" HTTP/1.1" CRLF);

  for (const auto& kv : headers_) {
    buf.append(kv.first);
    buf.append(":");
    buf.append(kv.second);
    buf.append(CRLF);
  }

  buf.append(CRLF);  // empty line before body
  if (!body_.empty()) buf.append(body_);

  return buf;
}

const std::string& HttpRequest::ContentType() const {
  auto it = headers_.find("Content-Type");
  if (it != headers_.end()) {
    return it->second;
  }

  return kEmpty;
}

int HttpRequest::ContentLength() const {
  auto it = headers_.find("Content-Length");
  if (it != headers_.end()) {
    return std::stoi(it->second);
  }

  return 0;
}

// HTTP response
void HttpResponse::Reset() {
  code_ = 200;
  status_.clear();
  headers_.clear();
  body_.clear();
}

void HttpResponse::Swap(HttpResponse& other) {
  std::swap(code_, other.code_);
  status_.swap(other.status_);
  headers_.swap(other.headers_);
  body_.swap(other.body_);
}

std::string HttpResponse::Encode() const {
  std::string buf;
  buf.reserve(512);

  buf.append("HTTP/1.1 ");
  buf.append(std::to_string(code_));
  buf.append(" ");
  buf.append(status_);
  buf.append(CRLF);

  for (const auto& kv : headers_) {
    buf.append(kv.first);
    buf.append(":");
    buf.append(kv.second);
    buf.append(CRLF);
  }

  buf.append(CRLF);  // empty line before body
  if (!body_.empty()) buf.append(body_);

  return buf;
}

const std::string& HttpResponse::ContentType() const {
  auto it = headers_.find("Content-Type");
  if (it != headers_.end()) {
    return it->second;
  }

  return kEmpty;
}

int HttpResponse::ContentLength() const {
  auto it = headers_.find("Content-Length");
  if (it != headers_.end()) {
    return std::stoi(it->second);
  }

  return 0;
}

const std::string HttpRequest::kEmpty;
const std::string HttpResponse::kEmpty;

struct Url ParseUrl(const std::string& url) {
  struct Url result;
  if (url.empty()) {
    return result;
  }

  const char* start = &url[0];
  const char* end = start + url.size();
  const char* query = std::find(start, end, '?');
  if (query != end) {
    result.query = std::string(query + 1, end);
  }

  result.path = std::string(start, query);
  return result;
}

std::string ParseQuery(const std::string& url, const std::string& key) {
  auto result = ParseUrl(url);
  const auto& query = result.query;
  size_t start = query.find(key + "=");
  if (start == std::string::npos) {
    return "";
  }

  start += key.size() + 1;  // skip '='
  if (start >= query.size()) {
    return "";
  }

  size_t end = query.find("&", start);
  if (end != std::string::npos) {
    return query.substr(start, end - start);
  } else {
    return query.substr(start);
  }
}

}  // namespace pikiwidb
