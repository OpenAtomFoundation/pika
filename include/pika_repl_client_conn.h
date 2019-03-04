// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_CLIENT_CONN_H_
#define PIKA_REPL_CLIENT_CONN_H_

#include "pink/include/pb_conn.h"

#include <memory>

#include "include/pika_conf.h"
#include "src/pika_inner_message.pb.h"

class PikaReplClientConn: public pink::PbConn {
 public:
  PikaReplClientConn(int fd, const std::string& ip_port, pink::Thread *thread, void* worker_specific_data, pink::PinkEpoll* epoll);
  virtual ~PikaReplClientConn() = default;
  static void HandleBinlogSyncResponse(void* arg);

  int DealMessage() override;
 private:
  bool IsTableStructConsistent(const std::vector<TableStruct>& current_tables,
                               const std::vector<TableStruct>& expect_tables);
  int HandleMetaSyncResponse(const InnerMessage::InnerResponse& response);
  int HandleTrySyncResponse(const InnerMessage::InnerResponse& response);
  int HandleMetaSyncResponse(const std::shared_ptr<InnerMessage::InnerResponse> response);

  struct ReplRespArg {
    std::shared_ptr<InnerMessage::InnerResponse> resp;
    std::shared_ptr<pink::PbConn> conn;
    ReplRespArg(std::shared_ptr<InnerMessage::InnerResponse> _resp, std::shared_ptr<pink::PbConn> _conn) : resp(_resp), conn(_conn) {
    }
  };
};

#endif
