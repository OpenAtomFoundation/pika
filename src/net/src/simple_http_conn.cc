// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "net/include/simple_http_conn.h"

#include <algorithm>
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <string>

#include "net/include/net_define.h"
#include "pstd/include/pstd_string.h"
#include "pstd/include/xdebug.h"

namespace net {

static const uint32_t kHTTPMaxMessage = 1024 * 1024 * 8;
static const uint32_t kHTTPMaxHeader = 1024 * 64;

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

Request::Request() : method("GET"), path("/index") {}

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

bool Request::ParseHeadLine(const char* data, int line_start, int line_end,
                            ParseStatus* parseStatus) {
  std::string param_key;
  std::string param_value;
  for (int i = line_start; i <= line_end; i++) {
    switch (*parseStatus) {
      case kHeaderMethod:
        if (data[i] != ' ') {
          method.push_back(data[i]);
        } else {
          *parseStatus = kHeaderPath;
        }
        break;
      case kHeaderPath:
        if (data[i] != ' ') {
          path.push_back(data[i]);
        } else {
          *parseStatus = kHeaderVersion;
        }
        break;
      case kHeaderVersion:
        if (data[i] != '\r' && data[i] != '\n') {
          version.push_back(data[i]);
        } else if (data[i] == '\n') {
          *parseStatus = kHeaderParamKey;
        }
        break;
      case kHeaderParamKey:
        if (data[i] != ':' && data[i] != ' ') {
          param_key.push_back(data[i]);
        } else if (data[i] == ' ') {
          *parseStatus = kHeaderParamValue;
        }
        break;
      case kHeaderParamValue:
        if (data[i] != '\r' && data[i] != '\n') {
          param_value.push_back(data[i]);
        } else if (data[i] == '\r') {
          headers[pstd::StringToLower(param_key)] = param_value;
          *parseStatus = kHeaderParamKey;
        }
        break;

      default:
        return false;
    }
  }
  return true;
}

bool Request::ParseGetUrl() {
  // Format path
  if (path.find(headers["host"]) != std::string::npos &&
      path.size() > (7 + headers["host"].size())) {
    // http://www.xxx.xxx/path/to
    path.assign(path.substr(7 + headers["host"].size()));
  }
  size_t n = path.find('?');
  if (n == std::string::npos) {
    return true;  // no parameter
  }
  if (!ParseParameters(path, n + 1)) {
    return false;
  }
  path.resize(n);
  return true;
}

// Parse query parameter from GET url or POST application/x-www-form-urlencoded
// format: key1=value1&key2=value2&key3=value3
bool Request::ParseParameters(const std::string& data, size_t line_start,
                              bool from_url) {
  size_t pre = line_start;
  size_t mid;
  size_t end;
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
      if (from_url) {
        query_params[data.substr(pre, end - pre)] = std::string();
      } else {
        post_params[data.substr(pre, end - pre)] = std::string();
      }
      pre = end + 1;
    } else {
      if (from_url) {
        query_params[data.substr(pre, mid - pre)] =
            data.substr(mid + 1, end - mid - 1);
      } else {
        post_params[data.substr(pre, mid - pre)] =
            data.substr(mid + 1, end - mid - 1);
      }
      pre = end + 1;
    }
  }
  return true;
}

bool Request::ParseHeadFromArray(const char* data, const int size) {
  int remain_size = size;
  if (remain_size <= 5) {
    return false;
  }

  // Parse header line
  int line_start = 0;
  int line_end = 0;
  ParseStatus parseStatus = kHeaderMethod;
  while (remain_size > 4) {
    line_end += find_lf(data + line_start, remain_size);
    if (line_end < line_start) {
      return false;
    }
    if (!ParseHeadLine(data, line_start, line_end, &parseStatus)) {
      return false;
    }
    remain_size -= (line_end - line_start + 1);
    line_start = ++line_end;
  }

  // Parse query parameter from url
  return ParseGetUrl();
}

bool Request::ParseBodyFromArray(const char* data, const int size) {
  content.append(data, size);
  if (method == "POST" &&
      headers["content-type"] == "application/x-www-form-urlencoded") {
    return ParseParameters(content, 0, false);
  }
  return true;
}

void Request::Clear() {
  version.clear();
  path.clear();
  method.clear();
  query_params.clear();
  post_params.clear();
  headers.clear();
  content.clear();
}

void Response::Clear() {
  status_code_ = 0;
  reason_phrase_.clear();
  headers_.clear();
  body_.clear();
}

// Return bytes actual be writen, should be less than size
int Response::SerializeHeaderToArray(char* data, size_t size) {
  int serial_size = 0;
  int ret;

  // Serialize statues line
  ret = snprintf(data, size, "HTTP/1.1 %d %s\r\n", status_code_,
                 reason_phrase_.c_str());
  if (ret < 0 || ret == static_cast<int>(size)) {
    return ret;
  }
  serial_size += ret;

  // Serialize header
  if (headers_.find("Content-Length") == headers_.end()) {
    SetHeaders("Content-Length", body_.size());
  }
  for (auto& line : headers_) {
    ret = snprintf(data + serial_size, size - serial_size, "%s: %s\r\n",
                   line.first.c_str(), line.second.c_str());
    if (ret < 0) {
      return ret;
    }
    serial_size += ret;
    if (serial_size == static_cast<int>(size)) {
      return serial_size;
    }
  }

  ret = snprintf(data + serial_size, size - serial_size, "\r\n");
  serial_size += ret;
  return serial_size;
}

// Serialize body begin from 'pos', return the new pos
int Response::SerializeBodyToArray(char* data, size_t size, int* pos) {
  // Serialize body
  int actual = size;
  if (body_.size() - *pos < size) {
    actual = body_.size() - *pos;
  }
  memcpy(data, body_.data() + *pos, actual);
  *pos += actual;
  return actual;
}

void Response::SetStatusCode(int code) {
  assert((code >= 100 && code <= 102) || (code >= 200 && code <= 207) ||
         (code >= 400 && code <= 409) || (code == 416) ||
         (code >= 500 && code <= 509));
  status_code_ = code;
  reason_phrase_.assign(http_status_map.at(code));
}

SimpleHTTPConn::SimpleHTTPConn(const int fd, const std::string& ip_port,
                               Thread* thread)
    : NetConn(fd, ip_port, thread),
      conn_status_(kHeader),
      rbuf_pos_(0),
      wbuf_len_(0),
      wbuf_pos_(0),
      header_len_(0),
      remain_packet_len_(0),
      response_pos_(-1) {
  rbuf_ = reinterpret_cast<char*>(malloc(sizeof(char) * kHTTPMaxMessage));
  wbuf_ = reinterpret_cast<char*>(malloc(sizeof(char) * kHTTPMaxMessage));
  request_ = new Request();
  response_ = new Response();
}

SimpleHTTPConn::~SimpleHTTPConn() {
  free(rbuf_);
  free(wbuf_);
  delete request_;
  delete response_;
}

/*
 * Build request_
 */
bool SimpleHTTPConn::BuildRequestHeader() {
  request_->Clear();
  if (!request_->ParseHeadFromArray(rbuf_, header_len_)) {
    return false;
  }
  auto iter = request_->headers.find("content-length");
  if (iter == request_->headers.end()) {
    remain_packet_len_ = 0;
  } else {
    long tmp = 0;
    if (pstd::string2int(iter->second.data(), iter->second.size(), &tmp) != 0) {
      remain_packet_len_ = tmp;
    } else {
      remain_packet_len_ = 0;
    }
  }

  if (rbuf_pos_ > header_len_) {
    remain_packet_len_ -= rbuf_pos_ - header_len_;
  }
  return true;
}

bool SimpleHTTPConn::AppendRequestBody() {
  return request_->ParseBodyFromArray(rbuf_ + header_len_,
                                      rbuf_pos_ - header_len_);
}

void SimpleHTTPConn::HandleMessage() {
  response_->Clear();
  DealMessage(request_, response_);
  set_is_reply(true);
}

ReadStatus SimpleHTTPConn::GetRequest() {
  ssize_t nread = 0;
  while (true) {
    switch (conn_status_) {
      case kHeader: {
        nread = read(fd(), rbuf_ + rbuf_pos_, kHTTPMaxHeader - rbuf_pos_);
        if (nread == -1 && errno == EAGAIN) {
          return kReadHalf;
        } else if (nread <= 0) {
          return kReadClose;
        } else {
          rbuf_pos_ += nread;
          // So that strstr will not parse the expire char
          rbuf_[rbuf_pos_] = '\0';
          char* sep_pos = strstr(rbuf_, "\r\n\r\n");
          if (!sep_pos) {
            break;
          }
          header_len_ = sep_pos - rbuf_ + 4;
          if (!BuildRequestHeader()) {
            return kReadError;
          }

          std::string sign = request_->headers.count("expect") != 0U
                                 ? request_->headers.at("expect")
                                 : "";
          if (sign == "100-continue" || sign == "100-Continue") {
            // Reply 100 Continue, then receive body
            response_->Clear();
            response_->SetStatusCode(100);
            set_is_reply(true);
            conn_status_ = kPacket;
            if (remain_packet_len_ > 0) {
              return kReadHalf;
            }
          }
          conn_status_ = kPacket;
        }
        break;
      }
      case kPacket: {
        if (remain_packet_len_ > 0) {
          nread = read(fd(), rbuf_ + rbuf_pos_,
                       (kHTTPMaxMessage - rbuf_pos_ > remain_packet_len_)
                           ? remain_packet_len_
                           : kHTTPMaxMessage - rbuf_pos_);
          if (nread == -1 && errno == EAGAIN) {
            return kReadHalf;
          } else if (nread <= 0) {
            return kReadClose;
          } else {
            rbuf_pos_ += nread;
            remain_packet_len_ -= nread;
          }
        }
        if (remain_packet_len_ == 0 ||       // no more content
            rbuf_pos_ == kHTTPMaxMessage) {  // buffer full
          AppendRequestBody();
          if (remain_packet_len_ == 0) {
            conn_status_ = kComplete;
          } else {
            rbuf_pos_ = header_len_ = 0;  // read more packet content from begin
          }
        }
        break;
      }
      case kComplete: {
        HandleMessage();
        conn_status_ = kHeader;
        rbuf_pos_ = 0;
        return kReadAll;
      }
      default: {
        return kReadError;
      }
    }
    // else continue
  }
}

bool SimpleHTTPConn::FillResponseBuf() {
  if (response_pos_ < 0) {
    // Not ever serialize response header
    int actual = response_->SerializeHeaderToArray(wbuf_ + wbuf_len_,
                                                   kHTTPMaxMessage - wbuf_len_);
    if (actual < 0) {
      return false;
    }
    wbuf_len_ += actual;
    response_pos_ = 0;  // Serialize body next time
  }
  while (response_->HasMoreBody(response_pos_) && wbuf_len_ < kHTTPMaxMessage) {
    // Has more body and more space in wbuf_
    wbuf_len_ += response_->SerializeBodyToArray(
        wbuf_ + wbuf_len_, kHTTPMaxMessage - wbuf_len_, &response_pos_);
  }
  return true;
}

WriteStatus SimpleHTTPConn::SendReply() {
  // Fill as more as content into the buf
  if (!FillResponseBuf()) {
    return kWriteError;
  }

  ssize_t nwritten = 0;
  while (wbuf_len_ > 0) {
    nwritten = write(fd(), wbuf_ + wbuf_pos_, wbuf_len_ - wbuf_pos_);
    if (nwritten == -1 && errno == EAGAIN) {
      return kWriteHalf;
    } else if (nwritten <= 0) {
      return kWriteError;
    }
    wbuf_pos_ += nwritten;
    if (wbuf_pos_ == wbuf_len_) {
      // Send all in wbuf_ and Try to fill more
      wbuf_len_ = 0;
      wbuf_pos_ = 0;
      if (!FillResponseBuf()) {
        return kWriteError;
      }
    }
  }
  response_pos_ = -1;  // fill header first next time

  return kWriteAll;
}

}  // namespace net
