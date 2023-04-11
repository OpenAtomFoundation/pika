// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_CLIENT_H_
#define PIKA_REPL_CLIENT_H_

#include <memory>
#include <string>

#include "net/include/client_thread.h"
#include "net/include/net_conn.h"
#include "net/include/thread_pool.h"
#include "pstd/include/pstd_status.h"

#include "include/pika_binlog_reader.h"
#include "include/pika_define.h"
#include "include/pika_partition.h"
#include "include/pika_repl_bgworker.h"
#include "include/pika_repl_client_thread.h"

#include "net/include/thread_pool.h"
#include "pika_inner_message.pb.h"

using pstd::Status;

struct ReplClientTaskArg {
  std::shared_ptr<InnerMessage::InnerResponse> res;
  std::shared_ptr<net::PbConn> conn;
  ReplClientTaskArg(std::shared_ptr<InnerMessage::InnerResponse> _res, std::shared_ptr<net::PbConn> _conn)
      : res(_res), conn(_conn) {}
};

struct ReplClientWriteBinlogTaskArg {
  std::shared_ptr<InnerMessage::InnerResponse> res;
  std::shared_ptr<net::PbConn> conn;
  void* res_private_data;
  PikaReplBgWorker* worker;
  ReplClientWriteBinlogTaskArg(const std::shared_ptr<InnerMessage::InnerResponse> _res,
                               std::shared_ptr<net::PbConn> _conn, void* _res_private_data, PikaReplBgWorker* _worker)
      : res(_res), conn(_conn), res_private_data(_res_private_data), worker(_worker) {}
};

struct ReplClientWriteDBTaskArg {
  const std::shared_ptr<Cmd> cmd_ptr;
  LogOffset offset;
  std::string table_name;
  uint32_t partition_id;
  ReplClientWriteDBTaskArg(const std::shared_ptr<Cmd> _cmd_ptr, const LogOffset _offset, const std::string _table_name,
                           uint32_t _partition_id)
      : cmd_ptr(_cmd_ptr), offset(_offset), table_name(_table_name), partition_id(_partition_id) {}
  ~ReplClientWriteDBTaskArg() {}
};

class PikaReplClient {
 public:
  PikaReplClient(int cron_interval, int keepalive_timeout);
  ~PikaReplClient();

  int Start();
  int Stop();

  pstd::Status Write(const std::string& ip, const int port, const std::string& msg);
  pstd::Status Close(const std::string& ip, const int port);

  void Schedule(net::TaskFunc func, void* arg);
  void ScheduleWriteBinlogTask(std::string table_partition, const std::shared_ptr<InnerMessage::InnerResponse> res,
                               std::shared_ptr<net::PbConn> conn, void* req_private_data);
  void ScheduleWriteDBTask(const std::shared_ptr<Cmd> cmd_ptr, const LogOffset& offset, const std::string& table_name,
                           uint32_t partition_id);

  Status SendMetaSync();
  Status SendPartitionDBSync(const std::string& ip, uint32_t port, const std::string& table_name, uint32_t partition_id,
                             const BinlogOffset& boffset, const std::string& local_ip);
  Status SendPartitionTrySync(const std::string& ip, uint32_t port, const std::string& table_name,
                              uint32_t partition_id, const BinlogOffset& boffset, const std::string& local_ip);
  Status SendPartitionBinlogSync(const std::string& ip, uint32_t port, const std::string& table_name,
                                 uint32_t partition_id, const LogOffset& ack_start, const LogOffset& ack_end,
                                 const std::string& local_ip, bool is_frist_send);
  Status SendRemoveSlaveNode(const std::string& ip, uint32_t port, const std::string& table_name, uint32_t partition_id,
                             const std::string& local_ip);

 private:
  size_t GetHashIndex(std::string key, bool upper_half);
  void UpdateNextAvail() { next_avail_ = (next_avail_ + 1) % bg_workers_.size(); }

  PikaReplClientThread* client_thread_;
  int next_avail_;
  std::hash<std::string> str_hash;
  std::vector<PikaReplBgWorker*> bg_workers_;
};

#endif
