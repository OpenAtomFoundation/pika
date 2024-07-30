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
#include "include/pika_define.h"

#include "include/pika_binlog_reader.h"
#include "include/pika_repl_bgworker.h"
#include "include/pika_repl_client_thread.h"

#include "net/include/thread_pool.h"
#include "pika_inner_message.pb.h"


struct ReplClientTaskArg {
  std::shared_ptr<InnerMessage::InnerResponse> res;
  std::shared_ptr<net::PbConn> conn;
  ReplClientTaskArg(const std::shared_ptr<InnerMessage::InnerResponse>& _res, const std::shared_ptr<net::PbConn>& _conn)
      : res(_res), conn(_conn) {}
};

struct ReplClientWriteBinlogTaskArg {
  std::shared_ptr<InnerMessage::InnerResponse> res;
  std::shared_ptr<net::PbConn> conn;
  void* res_private_data;
  PikaReplBgWorker* worker;
  ReplClientWriteBinlogTaskArg(const std::shared_ptr<InnerMessage::InnerResponse>& _res,
                               const std::shared_ptr<net::PbConn>& _conn,
                               void* _res_private_data, PikaReplBgWorker* _worker)
      : res(_res), conn(_conn), res_private_data(_res_private_data), worker(_worker) {}
};

struct ReplClientWriteDBTaskArg {
  const std::shared_ptr<Cmd> cmd_ptr;
  explicit ReplClientWriteDBTaskArg(std::shared_ptr<Cmd> _cmd_ptr)
      : cmd_ptr(std::move(_cmd_ptr)) {}
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
  void ScheduleByDBName(net::TaskFunc func, void* arg, const std::string& db_name);
  void ScheduleWriteBinlogTask(const std::string& db_name, const std::shared_ptr<InnerMessage::InnerResponse>& res,
                               const std::shared_ptr<net::PbConn>& conn, void* res_private_data);
  void ScheduleWriteDBTask(std::shared_ptr<Cmd> cmd_ptr, const std::string& db_name);

  pstd::Status SendMetaSync();
  pstd::Status SendDBSync(const std::string& ip, uint32_t port, const std::string& db_name,
                             const BinlogOffset& boffset, const std::string& local_ip);
  pstd::Status SendTrySync(const std::string& ip, uint32_t port, const std::string& db_name,
                               const BinlogOffset& boffset, const std::string& local_ip);
  pstd::Status SendBinlogSync(const std::string& ip, uint32_t port, const std::string& db_name,
                                  const LogOffset& ack_start, const LogOffset& ack_end,
                                 const std::string& local_ip, bool is_first_send);
  pstd::Status SendRemoveSlaveNode(const std::string& ip, uint32_t port, const std::string& db_name, const std::string& local_ip);

  void IncrAsyncWriteDBTaskCount(const std::string& db_name, int32_t incr_step) {
    int32_t db_index = db_name.back() - '0';
    assert(db_index >= 0 && db_index <= 7);
    async_write_db_task_counts_[db_index].fetch_add(incr_step, std::memory_order::memory_order_seq_cst);
    LOG(INFO) << db_name << " incr 1, curr:" <<  async_write_db_task_counts_[db_index].load(std::memory_order_seq_cst);
  }

  void DecrAsyncWriteDBTaskCount(const std::string& db_name, int32_t incr_step) {
    int32_t db_index = db_name.back() - '0';
    assert(db_index >= 0 && db_index <= 7);
    async_write_db_task_counts_[db_index].fetch_sub(incr_step, std::memory_order::memory_order_seq_cst);
    LOG(INFO) << db_name << " decr 1, curr:" <<  async_write_db_task_counts_[db_index].load(std::memory_order_seq_cst);
  }

  int32_t GetUnfinishedAsyncDBTaskCount(const std::string& db_name) {
    int32_t db_index = db_name.back() - '0';
    assert(db_index >= 0 && db_index <= 7);
    return async_write_db_task_counts_[db_index].load(std::memory_order_seq_cst);
  }

 private:
  size_t GetBinlogWorkerIndexByDBName(const std::string &db_name);
  size_t GetHashIndexByKey(const std::string& key);
  void UpdateNextAvail() { next_avail_ = (next_avail_ + 1) % static_cast<int32_t>(write_binlog_workers_.size()); }

  std::unique_ptr<PikaReplClientThread> client_thread_;
  int next_avail_ = 0;
  std::hash<std::string> str_hash;

  // this is used when consuming binlog, which indicates the nums of async write-DB tasks that are
  // queued or being executing by WriteDBWorkers. If a flushdb-binlog need to apply DB, it must wait
  // util this count drop to zero. you can also check pika discussion #2807 to know more
  // it is only used in slaveNode when consuming binlog
  std::atomic<int32_t> async_write_db_task_counts_[MAX_DB_NUM];
  // [NOTICE] write_db_workers_ must be declared after async_write_db_task_counts_ to ensure write_db_workers_ will be destroyed before async_write_db_task_counts_
  // when PikaReplClient is de-constructing, because some of the async task that exec by write_db_workers_ will manipulate async_write_db_task_counts_
  std::vector<std::unique_ptr<PikaReplBgWorker>> write_binlog_workers_;
  std::vector<std::unique_ptr<PikaReplBgWorker>> write_db_workers_;
};

#endif
