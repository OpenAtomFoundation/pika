// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_SERVER_CONN_H_
#define PIKA_REPL_SERVER_CONN_H_

#include <string>

#include "net/include/pb_conn.h"
#include "net/include/net_thread.h"

#include "include/pika_define.h"
#include "pika_inner_message.pb.h"

class SyncMasterPartition;

class PikaReplServerConn: public net::PbConn {
 public:
  PikaReplServerConn(int fd, std::string ip_port, net::Thread* thread, void* worker_specific_data, net::NetMultiplexer* mpx);
  virtual ~PikaReplServerConn();

  static void HandleMetaSyncRequest(void* arg);
  static void HandleTrySyncRequest(void* arg);

  static bool TrySyncOffsetCheck(
    const std::shared_ptr<SyncMasterPartition>& partition,
    const InnerMessage::InnerRequest::TrySync& try_sync_request,
    InnerMessage::InnerResponse::TrySync* try_sync_response);
  static bool TrySyncConsensusOffsetCheck(
    const std::shared_ptr<SyncMasterPartition>& partition,
    const InnerMessage::ConsensusMeta& meta,
    InnerMessage::InnerResponse* response,
    InnerMessage::InnerResponse::TrySync* try_sync_response);
  static bool TrySyncUpdateSlaveNode(
    const std::shared_ptr<SyncMasterPartition>& partition,
    const InnerMessage::InnerRequest::TrySync& try_sync_request,
    const std::shared_ptr<net::PbConn>& conn,
    InnerMessage::InnerResponse::TrySync* try_sync_response);
  static void BuildConsensusMeta(
    const bool& reject,
    const std::vector<LogOffset>& hints,
    const uint32_t& term,
    InnerMessage::InnerResponse* response);

  static void HandleDBSyncRequest(void* arg);
  static void HandleBinlogSyncRequest(void* arg);
  static void HandleRemoveSlaveNodeRequest(void* arg);

  int DealMessage();
};

#endif  // INCLUDE_PIKA_REPL_SERVER_CONN_H_
