// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_INCLUDE_SIMPLE_HTTP_CONN_H_
#define NET_INCLUDE_SIMPLE_HTTP_CONN_H_

#include <map>
#include <vector>
#include <string>

#include "pstd/include/pstd_status.h"
#include "pstd/include/xdebug.h"

#include "net/include/net_conn.h"
#include "net/include/net_define.h"
#include "net/src/net_util.h"

namespace net {

class Request {
 public:
  // attach in header
  std::string method;
  std::string path;
  std::string version;
  std::map<std::string, std::string> headers;

  // in header for Get, in content for Post Put Delete
  std::map<std::string, std::string> query_params;

  // POST: content-type: application/x-www-form-urlencoded
  std::map<std::string, std::string> post_params;

  // attach in content
  std::string content;

  Request();
  void Clear();
  bool ParseHeadFromArray(const char* data, const int size);
  bool ParseBodyFromArray(const char* data, const int size);

 private:
  enum ParseStatus {
    kHeaderMethod,
    kHeaderPath,
    kHeaderVersion,
    kHeaderParamKey,
    kHeaderParamValue,
    kBody
  };

  bool ParseGetUrl();
  bool ParseHeadLine(
      const char* data, int line_start,
      int line_end, ParseStatus* parseStatus);
  bool ParseParameters(
      const std::string data,
      size_t line_start = 0,
      bool from_url = true);
};

class Response {
 public:
  Response():
    status_code_(0) {
  }
  void Clear();
  int SerializeHeaderToArray(char* data, size_t size);
  int SerializeBodyToArray(char* data, size_t size, int *pos);
  bool HasMoreBody(size_t pos) {
    return pos < body_.size();
  }

  void SetStatusCode(int code);

  void SetHeaders(const std::string &key, const std::string &value) {
    headers_[key] = value;
  }

  void SetHeaders(const std::string &key, const int value) {
    headers_[key] = std::to_string(value);
  }

  void SetBody(const std::string &body) {
    body_.assign(body);
  }

 private:
  int status_code_;
  std::string reason_phrase_;
  std::map<std::string, std::string> headers_;
  std::string body_;
};

class SimpleHTTPConn: public NetConn {
 public:
  SimpleHTTPConn(
      const int fd,
      const std::string &ip_port,
      Thread *thread);
  virtual ~SimpleHTTPConn();

  virtual ReadStatus GetRequest() override;
  virtual WriteStatus SendReply() override;

 private:
  virtual void DealMessage(const Request* req, Response* res) = 0;

  bool BuildRequestHeader();
  bool AppendRequestBody();
  bool FillResponseBuf();
  void HandleMessage();

  ConnStatus conn_status_;
  char* rbuf_;
  uint32_t rbuf_pos_;
  char* wbuf_;
  uint32_t wbuf_len_;  // length we wanna write out
  uint32_t wbuf_pos_;
  uint32_t header_len_;
  uint64_t remain_packet_len_;

  Request* request_;
  int response_pos_;
  Response* response_;
};

}  // namespace net
#endif  // NET_INCLUDE_SIMPLE_HTTP_CONN_H_
