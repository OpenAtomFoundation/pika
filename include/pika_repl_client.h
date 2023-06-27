// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_CLIENT_H_
#define PIKA_REPL_CLIENT_H_

#include <memory>
#include <string>
#include <utility>

#include "net/include/client_thread.h"
#include "net/include/net_conn.h"
#include "net/include/thread_pool.h"
#include "pstd/include/pstd_status.h"

#include "include/pika_binlog_reader.h"
#include "include/pika_define.h"
#include "include/pika_repl_bgworker.h"
#include "include/pika_repl_client_thread.h"
#include "include/pika_slot.h"

#include "net/include/thread_pool.h"
#include "pika_inner_message.pb.h"


struct ReplClientTaskArg {
  std::shared_ptr<InnerMessage::InnerResponse> res;
  std::shared_ptr<net::PbConn> conn;
  ReplClientTaskArg(std::shared_ptr<InnerMessage::InnerResponse> _res, std::shared_ptr<net::PbConn> _conn)
      : res(std::move(_res)), conn(std::move(_conn)) {}
};

struct ReplClientWriteBinlogTaskArg {
  std::shared_ptr<InnerMessage::InnerResponse> res;
  std::shared_ptr<net::PbConn> conn;
  void* res_private_data;
  PikaReplBgWorker* worker;
  ReplClientWriteBinlogTaskArg(std::shared_ptr<InnerMessage::InnerResponse>  _res,
                               std::shared_ptr<net::PbConn> _conn,
                               void* _res_private_data, PikaReplBgWorker* _worker)
      : res(std::move(_res)), conn(std::move(_conn)), res_private_data(_res_private_data), worker(_worker) {}
};

struct ReplClientWriteDBTaskArg {
  const std::shared_ptr<Cmd> cmd_ptr;
  LogOffset offset;
  std::string db_name;
  uint32_t slot_id;
  ReplClientWriteDBTaskArg(std::shared_ptr<Cmd> _cmd_ptr, const LogOffset& _offset, std::string _db_name,
                           uint32_t _slot_id)
      : cmd_ptr(std::move(_cmd_ptr)),
        offset(_offset),
        db_name(std::move(_db_name)),
        slot_id(_slot_id) {}
  ~ReplClientWriteDBTaskArg() = default;
};

class PikaReplClient {
 public:
  PikaReplClient(int cron_interval, int keepalive_timeout);
  ~PikaReplClient();

  int Start();
  int Stop();

  pstd::Status Write(const std::string& ip, int port, const std::string& msg);
  pstd::Status Close(const std::string& ip, int port);

  void Schedule(net::TaskFunc func, void* arg);
  void ScheduleWriteBinlogTask(const std::string& db_slot, const std::shared_ptr<InnerMessage::InnerResponse>& res,
                               std::shared_ptr<net::PbConn> conn, void* res_private_data);
  void ScheduleWriteDBTask(const std::shared_ptr<Cmd>& cmd_ptr, const LogOffset& offset, const std::string& db_name,
                           uint32_t slot_id);

  pstd::Status SendMetaSync();
  pstd::Status SendSlotDBSync(const std::string& ip, uint32_t port, const std::string& db_name, uint32_t slot_id,
                             const BinlogOffset& boffset, const std::string& local_ip);
  pstd::Status SendSlotTrySync(const std::string& ip, uint32_t port, const std::string& db_name,
                              uint32_t slot_id, const BinlogOffset& boffset, const std::string& local_ip);
  pstd::Status SendSlotBinlogSync(const std::string& ip, uint32_t port, const std::string& db_name,
                                 uint32_t slot_id, const LogOffset& ack_start, const LogOffset& ack_end,
                                 const std::string& local_ip, bool is_first_send);
  pstd::Status SendRemoveSlaveNode(const std::string& ip, uint32_t port, const std::string& db_name, uint32_t slot_id,
                             const std::string& local_ip);

 private:
  size_t GetHashIndex(const std::string& key, bool upper_half);
  void UpdateNextAvail() { next_avail_ = (next_avail_ + 1) % static_cast<int32_t>(bg_workers_.size()); }

  std::unique_ptr<PikaReplClientThread> client_thread_;
  int next_avail_ = 0;
  std::hash<std::string> str_hash;
  std::vector<std::unique_ptr<PikaReplBgWorker>> bg_workers_;
};

#endif
