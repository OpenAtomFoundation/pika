// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#include "net/include/http_conn.h"
#include <stdlib.h>
#include <limits.h>
#include <stdio.h>

#include <string>
#include <algorithm>

#include "pstd/include/xdebug.h"
#include "pstd/include/pstd_string.h"
#include "net/include/net_define.h"

namespace net {

static const uint32_t kHTTPMaxMessage = 1024 * 1024 * 8;
static const uint32_t kHTTPMaxHeader = 1024 * 1024;

static const std::map<int, std::string> http_status_map = {
  {100, "Continue"},
  {101, "Switching Protocols"},
  {102, "Processing"},

  {200, "OK"},
  {201, "Created"},
  {202, "Accepted"},
  {203, "Non-Authoritative Information"},
  {204, "No Content"},
  {205, "Reset Content"},
  {206, "Partial Content"},
  {207, "Multi-Status"},

  {400, "Bad Request"},
  {401, "Unauthorized"},
  {402, ""},  // reserve
  {403, "Forbidden"},
  {404, "Not Found"},
  {405, "Method Not Allowed"},
  {406, "Not Acceptable"},
  {407, "Proxy Authentication Required"},
  {408, "Request Timeout"},
  {409, "Conflict"},
  {416, "Requested Range not satisfiable"},

  {500, "Internal Server Error"},
  {501, "Not Implemented"},
  {502, "Bad Gateway"},
  {503, "Service Unavailable"},
  {504, "Gateway Timeout"},
  {505, "HTTP Version Not Supported"},
  {506, "Variant Also Negotiates"},
  {507, "Insufficient Storage"},
  {508, "Bandwidth Limit Exceeded"},
  {509, "Not Extended"},
};

inline int find_lf(const char* data, int size) {
  const char* c = data;
  int count = 0;
  while (count < size) {
    if (*c == '\n') {
      break;
    }
    c++;
    count++;
  }
  return count;
}

bool HTTPRequest::ParseHeadLine(
    const char* data, int line_start, int line_end) {
  std::string param_key;
  std::string param_value;
  for (int i = line_start; i <= line_end; i++) {
    switch (parse_status_) {
      case kHeaderMethod:
        if (data[i] != ' ') {
          method_.push_back(data[i]);
        } else {
          parse_status_ = kHeaderPath;
        }
        break;
      case kHeaderPath:
        if (data[i] != ' ') {
          url_.push_back(data[i]);
        } else {
          parse_status_ = kHeaderVersion;
        }
        break;
      case kHeaderVersion:
        if (data[i] != '\r' && data[i] != '\n') {
          version_.push_back(data[i]);
        } else if (data[i] == '\n') {
          parse_status_ = kHeaderParamKey;
        }
        break;
      case kHeaderParamKey:
        if (data[i] != ':' && data[i] != ' ') {
          param_key.push_back(data[i]);
        } else if (data[i] == ' ') {
          parse_status_ = kHeaderParamValue;
        }
        break;
      case kHeaderParamValue:
        if (data[i] != '\r' && data[i] != '\n') {
          param_value.push_back(data[i]);
        } else if (data[i] == '\r') {
          headers_[pstd::StringToLower(param_key)] = param_value;
          parse_status_ = kHeaderParamKey;
        }
        break;

      default:
        return false;
    }
  }
  return true;
}

bool HTTPRequest::ParseGetUrl() {
  path_ = url_;
  // Format path
  if (headers_.count("host") &&
      path_.find(headers_["host"]) != std::string::npos &&
      path_.size() > (7 + headers_["host"].size())) {
    // http://www.xxx.xxx/path_/to
    path_.assign(path_.substr(7 + headers_["host"].size()));
  }
  size_t n = path_.find('?');
  if (n == std::string::npos) {
    return true;  // no parameter
  }
  if (!ParseParameters(path_, n + 1)) {
    return false;
  }
  path_.resize(n);
  return true;
}

// Parse query parameter from GET url or POST application/x-www-form-urlencoded
// format: key1=value1&key2=value2&key3=value3
bool HTTPRequest::ParseParameters(const std::string data, size_t line_start) {
  size_t pre = line_start, mid, end;
  while (pre < data.size()) {
    mid = data.find('=', pre);
    if (mid == std::string::npos) {
      mid = data.size();
    }
    end = data.find('&', pre);
    if (end == std::string::npos) {
      end = data.size();
    }
    if (end <= mid) {
      // empty value
      query_params_[data.substr(pre, end - pre)] = std::string();
      pre = end + 1;
    } else {
      query_params_[data.substr(pre, mid - pre)] =
        data.substr(mid + 1, end - mid - 1);
      pre = end + 1;
    }
  }
  return true;
}

int HTTPRequest::ParseHeader() {
  rbuf_[rbuf_pos_] = '\0';  // Avoid strstr() parsing expire char
  char *sep_pos = strstr(rbuf_, "\r\n\r\n");
  if (!sep_pos) {
    // Haven't find header
    return 0;
  }
  int header_len = sep_pos - rbuf_ + 4;
  int remain_size = header_len;
  if (remain_size <= 5) {
    // Header error
    return -1;
  }

  // Parse header line
  int line_start = 0;
  int line_end = 0;
  while (remain_size > 4) {
    line_end += find_lf(rbuf_ + line_start, remain_size);
    if (line_end < line_start) {
      return -1;
    }
    if (!ParseHeadLine(rbuf_, line_start, line_end)) {
      return -1;
    }
    remain_size -= (line_end - line_start + 1);
    line_start = ++line_end;
  }

  // Parse query parameter from url
  if (!ParseGetUrl()) {
    return -1;
  }

  remain_recv_len_ = headers_.count("content-length") ?
    std::stoul(headers_.at("content-length")) : 0;

  if (headers_.count("content-type")) {
    content_type_.assign(headers_.at("content-type"));
  }

  if (headers_.count("expect") &&
      (headers_.at("expect") == "100-Continue" ||
      headers_.at("expect") == "100-continue")) {
    reply_100continue_ = true;
  }

  return header_len;
}

void HTTPRequest::Dump() const {
  std::cout << "Method:  " << method_ << std::endl;
  std::cout << "Url:     " << url_ << std::endl;
  std::cout << "Path:    " << path_ << std::endl;
  std::cout << "Version: " << version_ << std::endl;
  std::cout << "Headers: " << std::endl;
  for (auto& header : headers_) {
    std::cout << "  ----- "
      << header.first << ": " << header.second << std::endl;
  }
  std::cout << "Query params: " << std::endl;
  for (auto& item : query_params_) {
    std::cout << "  ----- " << item.first << ": " << item.second << std::endl;
  }
}

// Return bytes actual be writen, should be less than size
bool HTTPResponse::SerializeHeader() {
  int serial_size = 0;
  int ret;

  std::string reason_phrase = http_status_map.at(status_code_);

  // Serialize statues line
  ret = snprintf(wbuf_, kHTTPMaxHeader, "HTTP/1.1 %d %s\r\n",
                 status_code_, reason_phrase.c_str());
  serial_size += ret;
  if (ret < 0 || ret == static_cast<int>(kHTTPMaxHeader)) {
    return false;
  }

  for (auto &line : headers_) {
    ret = snprintf(wbuf_ + serial_size, kHTTPMaxHeader - serial_size,
                   "%s: %s\r\n",
                   line.first.c_str(), line.second.c_str());
    serial_size += ret;
    if (ret < 0 || serial_size == static_cast<int>(kHTTPMaxHeader)) {
      return false;
    }
  }

  ret = snprintf(wbuf_ + serial_size, kHTTPMaxHeader - serial_size, "\r\n");
  serial_size += ret;
  if (ret < 0 || serial_size == static_cast<int>(kHTTPMaxHeader)) {
    return false;
  }

  buf_len_ = serial_size;
  return true;
}

HTTPConn::HTTPConn(const int fd, const std::string &ip_port,
                   Thread *thread, std::shared_ptr<HTTPHandles> handles,
                   void* worker_specific_data)
      : NetConn(fd, ip_port, thread),
#ifdef __ENABLE_SSL
        security_(thread->security()),
#endif
        handles_(handles) {
  handles_->worker_specific_data_ = worker_specific_data;
  // this pointer is safe here
  request_ = new HTTPRequest(this);
  response_ = new HTTPResponse(this);
}

HTTPConn::~HTTPConn() {
  delete request_;
  delete response_;
}

HTTPRequest::HTTPRequest(HTTPConn* conn)
    : conn_(conn),
      reply_100continue_(false),
      req_status_(kNewRequest),
      parse_status_(kHeaderMethod),
      rbuf_pos_(0),
      remain_recv_len_(0) {
  rbuf_ = new char[kHTTPMaxMessage];
}

HTTPRequest::~HTTPRequest() {
  delete[] rbuf_;
}

const std::string HTTPRequest::url() const {
  return url_;
}

const std::string HTTPRequest::path() const {
  return path_;
}

const std::string HTTPRequest::query_value(const std::string& field) const {
  if (query_params_.count(field)) {
    return query_params_.at(field);
  }
  return "";
}

const std::string HTTPRequest::postform_value(const std::string& field) const {
  if (postform_params_.count(field)) {
    return postform_params_.at(field);
  }
  return "";
}

const std::string HTTPRequest::method() const {
  return method_;
}

const std::string HTTPRequest::content_type() const {
  return content_type_;
}

const std::map<std::string, std::string> HTTPRequest::query_params() const {
  return query_params_;
}

const std::map<std::string, std::string> HTTPRequest::postform_params() const {
  return postform_params_;
}

const std::map<std::string, std::string> HTTPRequest::headers() const {
  return headers_;
}

const std::string HTTPRequest::client_ip_port() const {
  return client_ip_port_;
}

void HTTPRequest::Reset() {
  rbuf_pos_ = 0;
  method_.clear();
  path_.clear();
  version_.clear();
  url_.clear();
  content_type_.clear();
  remain_recv_len_ = 0;
  reply_100continue_ = false;
  postform_params_.clear();
  query_params_.clear();
  headers_.clear();
  parse_status_ = kHeaderMethod;
  client_ip_port_ = conn_->ip_port();
}

ReadStatus HTTPRequest::DoRead() {
  ssize_t nread;
#ifdef __ENABLE_SSL
  if (conn_->security_) {
    nread = SSL_read(conn_->ssl(), rbuf_ + rbuf_pos_,
                     static_cast<int>(kHTTPMaxMessage));
    if (nread <= 0) {
      int sslerr = SSL_get_error(conn_->ssl(), static_cast<int>(nread));
      switch (sslerr) {
        case SSL_ERROR_WANT_READ:
        case SSL_ERROR_WANT_WRITE:
          return kReadHalf;
        case SSL_ERROR_SYSCALL:
          break;
        case SSL_ERROR_SSL:
        default:
          return kReadClose;
      }
    }
  }
  else
#endif
  {
    nread = read(conn_->fd(), rbuf_ + rbuf_pos_,
                 kHTTPMaxMessage - rbuf_pos_);
  }
  if (nread > 0) {
    rbuf_pos_ += nread;
    if (req_status_ == kBodyReceiving) {
      remain_recv_len_ -= nread;
    }
  } else if (nread == -1 && errno == EAGAIN) {
    return kReadHalf;
  } else if (nread <= 0) {
    return kReadClose;
  }

  return kOk;
}

ReadStatus HTTPRequest::ReadData() {
  if (req_status_ == kNewRequest) {
    Reset();
    if (conn_->response_->Finished()) {
      conn_->response_->Reset();
    } else {
      return kReadHalf;
    }
    req_status_ = kHeaderReceiving;
  }

  ReadStatus s;
  while (true) {
    int header_len = 0;
    switch (req_status_) {
      case kHeaderReceiving:
        if ((s = DoRead()) != kOk) {
          conn_->handles_->HandleConnClosed();
          return s;
        }
        header_len = ParseHeader();
        if (header_len < 0 ||
            rbuf_pos_ > kHTTPMaxHeader) {
          // Parse header error
          conn_->handles_->HandleConnClosed();
          return kReadError;
        } else if (header_len > 0) {
          // Parse header success
          req_status_ = kBodyReceiving;
          bool need_reply = conn_->handles_->HandleRequest(this);
          if (need_reply) {
            req_status_ = kBodyReceived;
            break;
          }

          // Move remain body part to begin
          memmove(rbuf_, rbuf_ + header_len, rbuf_pos_ - header_len);
          remain_recv_len_ -= rbuf_pos_ - header_len;
          rbuf_pos_ -= header_len;

          if (reply_100continue_ && remain_recv_len_ != 0) {
            conn_->response_->SetStatusCode(100);
            reply_100continue_ = false;
            return kReadAll;
          }

          if (remain_recv_len_ == 0) {
            conn_->handles_->HandleBodyData(rbuf_, rbuf_pos_);
            req_status_ = kBodyReceived;
          }
        } else {
          // Haven't find header
        }
        break;
      case kBodyReceiving:
        if ((s = DoRead()) != kOk) {
          conn_->handles_->HandleConnClosed();
          return s;
        }
        if (rbuf_pos_ == kHTTPMaxMessage ||
            remain_recv_len_ == 0) {
          conn_->handles_->HandleBodyData(rbuf_, rbuf_pos_);
          rbuf_pos_ = 0;
        }
        if (remain_recv_len_ == 0) {
          req_status_ = kBodyReceived;
        }
        break;
      case kBodyReceived:
        req_status_ = kNewRequest;
        conn_->handles_->PrepareResponse(conn_->response_);
        return kReadAll;
      default:
        break;
    }
  }

  assert(true);
}

ReadStatus HTTPConn::GetRequest() {
  ReadStatus status = request_->ReadData();
  if (status == kReadAll) {
    set_is_reply(true);
  }
  return status;
}

HTTPResponse::HTTPResponse(HTTPConn* conn)
    : conn_(conn),
      resp_status_(kPrepareHeader),
      buf_len_(0),
      wbuf_pos_(0),
      remain_send_len_(0),
      finished_(true),
      status_code_(200) {
  wbuf_ = new char[kHTTPMaxMessage];
}

HTTPResponse::~HTTPResponse() {
  delete[] wbuf_;
}

void HTTPResponse::Reset() {
  headers_.clear();
  status_code_ = 200;
  finished_ = false;
  remain_send_len_ = 0;
  wbuf_pos_ = 0;
  buf_len_ = 0;
  resp_status_ = kPrepareHeader;
}

bool HTTPResponse::Finished() {
  return finished_;
}

void HTTPResponse::SetStatusCode(int code) {
  assert((code >= 100 && code <= 102) ||
         (code >= 200 && code <= 207) ||
         (code >= 400 && code <= 409) ||
         (code == 416) ||
         (code >= 500 && code <= 509));
  status_code_ = code;
}

void HTTPResponse::SetHeaders(
    const std::string& key, const std::string& value) {
  headers_[key] = value;
}

void HTTPResponse::SetHeaders(const std::string& key, const size_t value) {
  headers_[key] = std::to_string(value);
}

void HTTPResponse::SetContentLength(uint64_t size) {
  remain_send_len_ = size;
  if (headers_.count("Content-Length") ||
      headers_.count("content-length")) {
    return;
  }
  SetHeaders("Content-Length", size);
}

bool HTTPResponse::Flush() {
  if (resp_status_ == kPrepareHeader) {
    if (!SerializeHeader() ||
        buf_len_ > kHTTPMaxHeader) {
      return false;
    }
    resp_status_ = kSendingHeader;
  }
  if (resp_status_ == kSendingHeader) {
    ssize_t nwritten;
#ifdef __ENABLE_SSL
    if (conn_->security_) {
      nwritten = SSL_write(conn_->ssl(), wbuf_ + wbuf_pos_,
                           static_cast<int>(buf_len_));
      if (nwritten <= 0) {
        // FIXME (gaodq)
        int sslerr = SSL_get_error(conn_->ssl(), static_cast<int>(nwritten));
        switch (sslerr) {
          case SSL_ERROR_WANT_READ:
          case SSL_ERROR_WANT_WRITE:
            return true;
          case SSL_ERROR_SYSCALL:
            break;
          case SSL_ERROR_SSL:
          default:
            return false;
        }
      }
    }
    else
#endif
    {
      nwritten = write(conn_->fd(), wbuf_ + wbuf_pos_, buf_len_);
    }
    if (nwritten == -1 && errno == EAGAIN) {
      return true;
    } else if (nwritten <= 0) {
      // Connection close
      return false;
    } else if (nwritten == static_cast<ssize_t>(buf_len_)) {
      // Complete sending header
      wbuf_pos_ = 0;
      buf_len_ = 0;
      if (status_code_ == 100) {
        // Sending 100-continue, no body
        resp_status_ = kPrepareHeader;
        finished_ = true;
        return true;
      }
      resp_status_ = kSendingBody;
    } else {
      wbuf_pos_ += nwritten;
      buf_len_ -= nwritten;
    }
  }
  if (resp_status_ == kSendingBody) {
    if (remain_send_len_ == 0) {
      // Complete response
      finished_ = true;
      resp_status_ = kPrepareHeader;
      return true;
    }
    if (buf_len_ == 0) {
      size_t remain_buf = static_cast<uint64_t>(kHTTPMaxMessage) - wbuf_pos_;
      size_t needed_size = std::min<uint64_t>(remain_buf, remain_send_len_);
      buf_len_ = conn_->handles_->WriteResponseBody(
          wbuf_ + wbuf_pos_, needed_size);
    }

    if (buf_len_ == -1) {
      return false;
    }

    ssize_t nwritten;
#ifdef __ENABLE_SSL
    if (conn_->security_) {
      nwritten = SSL_write(conn_->ssl(), wbuf_ + wbuf_pos_,
                           static_cast<int>(buf_len_));
      if (nwritten <= 0) {
        // FIXME (gaodq)
        int sslerr = SSL_get_error(conn_->ssl(), static_cast<int>(nwritten));
        switch (sslerr) {
          case SSL_ERROR_WANT_READ:
          case SSL_ERROR_WANT_WRITE:
            return true;
          case SSL_ERROR_SYSCALL:
            break;
          case SSL_ERROR_SSL:
          default:
            return false;
        }
      }
    }
    else
#endif
    {
      nwritten = write(conn_->fd(), wbuf_ + wbuf_pos_, buf_len_);
    }
    if (nwritten == -1 && errno == EAGAIN) {
      return true;
    } else if (nwritten <= 0) {
      // Connection close
      return false;
    } else {
      wbuf_pos_ += nwritten;
      if (wbuf_pos_ == kHTTPMaxMessage) {
        wbuf_pos_ = 0;
      }
      buf_len_ -= nwritten;
      remain_send_len_ -= nwritten;
    }
  }

  // Continue
  return true;
}

WriteStatus HTTPConn::SendReply() {
  if (!response_->Flush()) {
    return kWriteError;
  }
  if (response_->Finished()) {
    return kWriteAll;
  }
  return kWriteHalf;
}

}  // namespace net
