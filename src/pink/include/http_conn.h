// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PINK_INCLUDE_HTTP_CONN_H_
#define PINK_INCLUDE_HTTP_CONN_H_
#include <map>
#include <vector>
#include <string>
#include <memory>

#include "pstd/include/pstd_status.h"
#include "pstd/include/xdebug.h"

#include "pink/include/pink_conn.h"
#include "pink/include/pink_define.h"
#include "pink/src/pink_util.h"

namespace pink {

class HTTPConn;

class HTTPRequest {
 public:
  const std::string url() const;
  const std::string path() const;
  const std::string query_value(const std::string& field) const;
  const std::map<std::string, std::string> query_params() const;
  const std::map<std::string, std::string> postform_params() const;
  const std::map<std::string, std::string> headers() const;
  const std::string postform_value(const std::string& field) const;
  const std::string method() const;
  const std::string content_type() const;

  const std::string client_ip_port() const;

  void Reset();
  void Dump() const;

 private:
  friend class HTTPConn;
  explicit HTTPRequest(HTTPConn* conn);
  ~HTTPRequest();

  HTTPConn* conn_;

  std::string method_;
  std::string url_;
  std::string path_;
  std::string version_;
  std::string content_type_;
  bool reply_100continue_;
  std::map<std::string, std::string> postform_params_;
  std::map<std::string, std::string> query_params_;
  std::map<std::string, std::string> headers_;

  std::string client_ip_port_;

  enum RequestParserStatus {
    kHeaderMethod,
    kHeaderPath,
    kHeaderVersion,
    kHeaderParamKey,
    kHeaderParamValue,
  };

  enum RequestStatus {
    kNewRequest,
    kHeaderReceiving,
    kBodyReceiving,
    kBodyReceived,
  };

  RequestStatus req_status_;
  RequestParserStatus parse_status_;

  char* rbuf_;
  uint64_t rbuf_pos_;
  uint64_t remain_recv_len_;

  ReadStatus ReadData();
  int ParseHeader();

  ReadStatus DoRead();
  bool ParseHeadFromArray(const char* data, const int size);
  bool ParseGetUrl();
  bool ParseHeadLine(const char* data, int line_start, int line_end);
  bool ParseParameters(const std::string data, size_t line_start = 0);
};

class HTTPResponse {
 public:
  void SetStatusCode(int code);
  void SetHeaders(const std::string& key, const std::string& value);
  void SetHeaders(const std::string& key, const size_t value);
  void SetContentLength(uint64_t size);

  void Reset();
  bool Finished();

 private:
  friend class HTTPConn;
  HTTPConn* conn_;

  explicit HTTPResponse(HTTPConn* conn);
  ~HTTPResponse();

  enum ResponseStatus {
    kPrepareHeader,
    kSendingHeader,
    kSendingBody,
  };

  ResponseStatus resp_status_;

  char* wbuf_;
  int64_t buf_len_;
  int64_t wbuf_pos_;

  uint64_t remain_send_len_;
  bool finished_;

  int status_code_;
  std::map<std::string, std::string> headers_;

  bool Flush();
  bool SerializeHeader();
};

class HTTPHandles {
 public:
  // You need implement these handles.
  /*
   * We have parsed HTTP request for now,
   * then HandleRequest(req, resp) will be called.
   * Return true if reply needed, and then handle response header and body
   * by functions below, otherwise false.
   */
  virtual bool HandleRequest(const HTTPRequest* req) = 0;
  /*
   * ReadBodyData(...) will be called if there are data follow up,
   * We deliver data just once.
   */
  virtual void HandleBodyData(const char* data, size_t data_size) = 0;

  /*
   * Fill response headers in this handle when body received.
   * You MUST set Content-Length by means of calling resp->SetContentLength(num).
   * Besides, resp->SetStatusCode(code) should be called either.
   */
  virtual void PrepareResponse(HTTPResponse* resp) = 0;
  /*
   * Fill write buffer 'buf' in this handle, and should not exceed 'max_size'.
   * Return actual size filled.
   * Return -2 if has written all
   * Return Other as Error and close connection
   */
  virtual int WriteResponseBody(char* buf, size_t max_size) = 0;

  // Close handle
  virtual void HandleConnClosed() {
  }

  HTTPHandles() {
  }
  virtual ~HTTPHandles() {
  }

 protected:
  /*
   * Assigned in ServerHandle's CreateWorkerSpecificData
   * Used for handles above
   */
  void* worker_specific_data_;

 private:
  friend class HTTPConn;

  /*
   * No allowed copy and copy assign
   */
  HTTPHandles(const HTTPHandles&);
  void operator=(const HTTPHandles&);
};

class HTTPConn: public PinkConn {
 public:
  HTTPConn(const int fd, const std::string &ip_port,
           Thread *sthread, std::shared_ptr<HTTPHandles> handles_,
           void* worker_specific_data);
  ~HTTPConn();

  virtual ReadStatus GetRequest() override;
  virtual WriteStatus SendReply() override;

 private:
  friend class HTTPRequest;
  friend class HTTPResponse;

  HTTPRequest* request_;
  HTTPResponse* response_;

#ifdef __ENABLE_SSL
  bool security_;
#endif

  std::shared_ptr<HTTPHandles> handles_;
};

}  // namespace pink

#endif  // PINK_INCLUDE_HTTP_CONN_H_
