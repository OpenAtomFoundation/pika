#pragma once

#include <functional>
#include <string>
#include <unordered_map>

#include "llhttp.h"

namespace pikiwidb {

//----- HTTP request -----------
class HttpRequest {
 public:
  HttpRequest() = default;

  void Reset();
  void Swap(HttpRequest& other);

  // Encode to string, deep copy
  std::string Encode() const;

  // method
  void SetMethod(llhttp_method method) { method_ = method; }
  llhttp_method Method() const { return method_; }
  const char* MethodString() const { return llhttp_method_name(method_); }

  // url
  void SetUrl(const std::string& url) { url_ = url; }
  const std::string& Url() const { return url_; }
  void AppendUrl(const std::string& url) { url_ += url; }
  void AppendUrl(const char* url, size_t len) { url_.append(url, len); }

  // header
  void SetHeader(const std::string& field, const std::string& value) { headers_.insert({field, value}); }
  const std::string& GetHeader(const std::string& field) const {
    auto it = headers_.find(field);
    return it == headers_.end() ? kEmpty : it->second;
  }
  const std::unordered_map<std::string, std::string>& Headers() const { return headers_; }

  // body
  void SetBody(const std::string& body) {
    body_ = body;
    SetHeader("Content-Length", std::to_string(body.size()));
  }
  void SetBody(const char* body, size_t len) { body_.assign(body, len); }
  void AppendBody(const std::string& body) { body_ += body; }
  void AppendBody(const char* body, size_t len) { body_.append(body, len); }
  const std::string& Body() const { return body_; }

  // utils
  const std::string& ContentType() const;
  int ContentLength() const;

 private:
  llhttp_method method_ = HTTP_GET;
  std::string url_;
  std::unordered_map<std::string, std::string> headers_;
  std::string body_;

  static const std::string kEmpty;
};

//----- HTTP response ----------
class HttpResponse {
 public:
  HttpResponse() = default;

  void Reset();
  void Swap(HttpResponse& other);

  // Encode to string, deep copy
  std::string Encode() const;

  // status code
  void SetCode(int code) { code_ = code; }
  int Code() const { return code_; }

  // status description
  void SetStatus(const std::string& status) { status_ = status; }
  void AppendStatus(const std::string& status) { status_ += status; }
  void AppendStatus(const char* status, size_t len) { status_.append(status, len); }
  const std::string& Status() const { return status_; }

  // headers
  void SetHeader(const std::string& field, const std::string& value) { headers_.insert({field, value}); }
  const std::string& GetHeader(const std::string& field) const {
    auto it = headers_.find(field);
    return it == headers_.end() ? kEmpty : it->second;
  }
  const std::unordered_map<std::string, std::string>& Headers() const { return headers_; }

  // body
  void SetBody(const std::string& body) {
    body_ = body;
    SetHeader("Content-Length", std::to_string(body.size()));
  }
  void AppendBody(const std::string& body) { body_ += body; }
  void AppendBody(const char* body, size_t len) { body_.append(body, len); }
  const std::string& Body() const { return body_; }

  // utils
  const std::string& ContentType() const;
  int ContentLength() const;

 private:
  int code_ = 200;
  std::string status_;
  std::unordered_map<std::string, std::string> headers_;
  std::string body_;

  static const std::string kEmpty;
};

using HttpRequestHandler = std::function<void(const HttpRequest&)>;
using HttpResponseHandler = std::function<void(const HttpResponse&)>;

//----- HTTP parser ---------
class HttpParser {
 public:
  explicit HttpParser(llhttp_type type);

  HttpParser(const HttpParser&) = delete;
  void operator=(const HttpParser&) = delete;

  bool Execute(const std::string& data);
  bool Execute(const char* data, size_t len);

  void SetRequestHandler(HttpRequestHandler h) { req_handler_ = std::move(h); }
  void SetResponseHandler(HttpResponseHandler h) { rsp_handler_ = std::move(h); }

  bool IsRequest() const { return type_ == HTTP_REQUEST; }
  bool IsComplete() const { return complete_; }

  // assert IsRequest() && IsComplete()
  const HttpRequest& Request() const& { return request_; }

  // assert !IsRequest() && IsComplete()
  const HttpResponse& Response() const& { return response_; }

  const std::string& ErrorReason() const { return error_reason_; }

  // for unit test
  void Reinit();

 private:
  void Reset();

  static int OnMessageBegin(llhttp_t* h);
  static int OnMessageComplete(llhttp_t* h);

  static int OnUrl(llhttp_t* h, const char* data, size_t len);
  static int OnHeaderField(llhttp_t* h, const char* data, size_t len);
  static int OnHeaderValue(llhttp_t* h, const char* data, size_t len);
  static int OnHeadersComplete(llhttp_t* h);
  static int OnStatus(llhttp_t* h, const char* data, size_t len);
  static int OnBody(llhttp_t* h, const char* data, size_t len);

  static int OnUrlComplete(llhttp_t* h);
  static int OnStatusComplete(llhttp_t* h);
  static int OnHeaderFieldComplete(llhttp_t* h);
  static int OnHeaderValueComplete(llhttp_t* h);

  llhttp_t parser_;
  llhttp_settings_t settings_;
  bool complete_ = false;

  const llhttp_type type_;  // request or response
  HttpRequest request_;
  HttpResponse response_;

  std::string key_, value_;  // temp vars for parse header
  std::string error_reason_;

  HttpRequestHandler req_handler_;
  HttpResponseHandler rsp_handler_;
};

// helper for parse url query
struct Url {
  std::string path;
  std::string query;
};
struct Url ParseUrl(const std::string& url);
std::string ParseQuery(const std::string& url, const std::string& key);

}  // namespace pikiwidb
