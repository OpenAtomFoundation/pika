// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_INCLUDE_PB_CONN_H_
#define NET_INCLUDE_PB_CONN_H_

#include <map>
#include <queue>
#include <string>

#include "google/protobuf/message.h"
#include "net/include/net_conn.h"
#include "net/include/net_define.h"
#include "pstd/include/pstd_status.h"

namespace net {

using pstd::Status;

class PbConn : public NetConn {
 public:
  struct WriteBuf {
    WriteBuf(const size_t item_pos = 0) : item_pos_(item_pos) {}
    std::queue<std::string> queue_;
    size_t item_pos_;
  };
  PbConn(int32_t fd, const std::string& ip_port, Thread* thread, NetMultiplexer* net_mpx = nullptr);
  ~PbConn() override;

  ReadStatus GetRequest() override;
  WriteStatus SendReply() override;
  void TryResizeBuffer() override;
  int32_t WriteResp(const std::string& resp) override;
  void NotifyWrite();
  void NotifyClose();
  void set_is_reply(bool reply) override;
  bool is_reply() override;
  /*
   * The Variable need by read the buf,
   * We allocate the memory when we start the server
   */
  uint32_t header_len_{static_cast<uint32_t>(-1)};
  char* rbuf_;
  uint32_t cur_pos_{0};
  uint32_t rbuf_len_{0};
  int32_t remain_packet_len_{0};

  ConnStatus connStatus_{kHeader};

 protected:
  // NOTE: if this function return non 0, the the server will close this connection
  //
  // In the implementation of DealMessage, we should distinguish two types of error
  //
  // 1. protocol parsing error
  // 2. service logic error
  //
  // protocol parsing error means that we receive a message that is not
  // a protobuf message that we know,
  // in this situation we should close this connection.
  // why we should close connection?
  // beacause if we parse protocol error, it means that the content in this
  // connection can't not be parse, we can't recognize the next message.
  // The only thing we can do is close this connection.
  // in this condition the DealMessage should return -1;
  //
  //
  // the logic error means that we have receive the message, and the
  // message is protobuf message that we define in proto file.
  // After receiving this message, we start execute our service logic.
  // the service logic error we should put it in res_, and return 0
  // since this is the service logic error, not the network error.
  // this connection we can use again.

  // If you want to send response back, build your pb version response yourself,
  // serializeToString and invoke WriteResp and NotifyWrite if necessary.
  virtual int32_t DealMessage() = 0;

 private:
  pstd::Mutex resp_mu_;
  WriteBuf write_buf_;
  pstd::Mutex is_reply_mu_;
  int64_t is_reply_{0};
  virtual void BuildInternalTag(const std::string& resp, std::string* tag);
};

}  // namespace net
#endif  // NET_INCLUDE_PB_CONN_H_
