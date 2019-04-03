// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_CLIENT_H_
#define PIKA_REPL_CLIENT_H_

#include <string>
#include <memory>

#include "pink/include/pink_conn.h"
#include "pink/include/client_thread.h"
#include "pink/include/thread_pool.h"
#include "slash/include/slash_status.h"

#include "include/pika_partition.h"
#include "include/pika_binlog_reader.h"
#include "include/pika_repl_bgworker.h"
#include "include/pika_repl_client_thread.h"

#include "pink/include/thread_pool.h"
#include "src/pika_inner_message.pb.h"

#define kBinlogSendPacketNum 30
#define kBinlogSendBatchNum 100
#define kBinlogReadWinSize 3000

using slash::Status;

struct RmNode {
  std::string table_;
  uint32_t partition_;
  std::string ip_;
  int port_;

  RmNode(const std::string& table, int partition,
         const std::string& ip, int port)
      : table_(table), partition_(partition), ip_(ip), port_(port) {}

  RmNode(const RmNode& node) {
    table_ = node.table_;
    partition_ = node.partition_;
    ip_ = node.ip_;
    port_ = node.port_;
  }

  bool operator==(const RmNode& other) const {
    if (table_ == other.table_
      && partition_ == other.partition_
      && ip_ == other.ip_ && port_ == other.port_) {
      return true;
    }
    return false;
  }

  std::string ToString() const {
     return table_ + "_" + std::to_string(partition_) + "_" + ip_ + ":" + std::to_string(port_);
  }
};

struct hash_name {
  size_t operator()(const RmNode& n) const{
    return std::hash<std::string>()(n.table_) ^ std::hash<uint32_t>()(n.partition_) ^ std::hash<std::string>()(n.ip_) ^ std::hash<int>()(n.port_);
  }
};

struct BinlogChip {
  uint32_t file_num_;
  uint64_t offset_;
  std::string binlog_;
  BinlogChip(uint32_t file_num, uint64_t offset, std::string binlog) :file_num_(file_num), offset_(offset), binlog_(binlog) {
  }
  BinlogChip(const BinlogChip& binlog_chip) {
    file_num_ = binlog_chip.file_num_;
    offset_ = binlog_chip.offset_;
    binlog_ = binlog_chip.binlog_;
  }
};

struct WriteTask {
  struct RmNode rm_node_;
  struct BinlogChip binlog_chip_;
  WriteTask(RmNode rm_node, BinlogChip binlog_chip) : rm_node_(rm_node), binlog_chip_(binlog_chip) {
  }
};

struct ReplClientTaskArg {
  std::shared_ptr<InnerMessage::InnerResponse> res;
  std::shared_ptr<pink::PbConn> conn;
  ReplClientTaskArg(std::shared_ptr<InnerMessage::InnerResponse> _res,
                    std::shared_ptr<pink::PbConn> _conn)
      : res(_res), conn(_conn) {}
};

struct ReplClientWriteBinlogTaskArg {
  std::shared_ptr<InnerMessage::InnerResponse> res;
  std::shared_ptr<pink::PbConn> conn;
  void* res_private_data;
  PikaReplBgWorker* worker;
  ReplClientWriteBinlogTaskArg(
          const std::shared_ptr<InnerMessage::InnerResponse> _res,
          std::shared_ptr<pink::PbConn> _conn,
          void* _res_private_data,
          PikaReplBgWorker* _worker) :
      res(_res), conn(_conn),
      res_private_data(_res_private_data), worker(_worker) {}
};

struct ReplClientWriteDBTaskArg {
  PikaCmdArgsType* argv;
  BinlogItem* binlog_item;
  std::string table_name;
  uint32_t partition_id;
  ReplClientWriteDBTaskArg(PikaCmdArgsType* _argv,
                           BinlogItem* _binlog_item,
                           const std::string _table_name,
                           uint32_t _partition_id)
      : argv(_argv), binlog_item(_binlog_item),
        table_name(_table_name), partition_id(_partition_id) {}
  ~ReplClientWriteDBTaskArg() {
    delete argv;
    delete binlog_item;
  }
};


class PikaReplClient {
 public:
  PikaReplClient(int cron_interval, int keepalive_timeout);
  ~PikaReplClient();
  slash::Status Write(const std::string& ip, const int port, const std::string& msg);

  int Start();
  void Schedule(pink::TaskFunc func, void* arg);
  void ScheduleWriteBinlogTask(std::string table_partition,
                              const std::shared_ptr<InnerMessage::InnerResponse> res,
                              std::shared_ptr<pink::PbConn> conn,
                              void* req_private_data);
  void ScheduleWriteDBTask(const std::string& dispatch_key,
                           PikaCmdArgsType* argv, BinlogItem* binlog_item,
                           const std::string& table_name, uint32_t partition_id);

  Status AddBinlogSyncCtl(const RmNode& slave, std::shared_ptr<Binlog> logger, uint32_t filenum, uint64_t offset);
  Status RemoveSlave(const SlaveItem& slave);
  Status RemoveBinlogSyncCtl(const RmNode& slave);
  Status GetBinlogSyncCtlStatus(const RmNode& slave, BinlogOffset* const sent_boffset, BinlogOffset* const acked_boffset);

  Status SendMetaSync();
  Status SendPartitionDBSync(const std::string& table_name,
                             uint32_t partition_id,
                             const BinlogOffset& boffset);
  Status SendPartitionTrySync(const std::string& table_name,
                              uint32_t partition_id,
                              const BinlogOffset& boffset);
  Status SendPartitionBinlogSyncAck(const std::string& table_name,
                                    uint32_t partition_id,
                                    const BinlogOffset& ack_start,
                                    const BinlogOffset& ack_end);
  Status SendBinlogSync(const RmNode& slave);

  Status TriggerSendBinlogSync();

  int ConsumeWriteQueue();
  void DropItemInWriteQueue(const std::string& ip, int port);

  bool SetAckInfo(const RmNode& slave, uint32_t ack_filenum_start, uint64_t ack_offset_start, uint32_t ack_filenum_end, uint64_t ack_offset_end, uint64_t active_time);
  bool GetAckInfo(const RmNode& slave, uint32_t* act_file_num, uint64_t* ack_offset, uint64_t* active_time);

 private:
  size_t GetHashIndex(std::string key, bool upper_half);
  void UpdateNextAvail() {
    next_avail_ = (next_avail_ + 1) % bg_workers_.size();
  }

  PikaBinlogReader* NewPikaBinlogReader(std::shared_ptr<Binlog> logger, uint32_t filenum, uint64_t offset);

  void ProduceWriteQueue(WriteTask& task);

  void BuildBinlogPb(const RmNode& slave, const std::string& msg, uint32_t filenum, uint64_t offset, InnerMessage::InnerRequest* request);

  PikaReplClientThread* client_thread_;

  struct BinlogWinItem {
    uint32_t filenum_;
    uint64_t offset_;
    bool acked_;
    bool operator==(const BinlogWinItem& other) const {
      if (filenum_ == other.filenum_ && offset_ == other.offset_) {
        return true;
      }
      return false;
    }
    BinlogWinItem(uint32_t filenum, uint64_t offset) : filenum_(filenum), offset_(offset), acked_(false) {
    }
  };

  struct BinlogSyncCtl {
    slash::Mutex ctl_mu_;
    PikaBinlogReader* reader_;
    uint32_t ack_file_num_;
    uint64_t ack_offset_;
    uint64_t active_time_;
    // TODO implement ring buffer
    std::vector<BinlogWinItem> binlog_win_;

    BinlogSyncCtl(PikaBinlogReader* reader, uint32_t ack_file_num, uint64_t ack_offset, uint64_t active_time)
      : reader_(reader), ack_file_num_(ack_file_num), ack_offset_(ack_offset), active_time_(active_time) {
    }
    ~BinlogSyncCtl() {
      if (reader_) {
        delete reader_;
      }
    }
  };

  pthread_rwlock_t binlog_ctl_rw_;
  std::unordered_map<RmNode, BinlogSyncCtl*, hash_name> binlog_ctl_;

  slash::Mutex  write_queue_mu_;
  // every host owns a queue
  std::unordered_map<std::string, std::queue<WriteTask>> write_queues_;  // ip+port, queue<WriteTask>

  int next_avail_;
  std::hash<std::string> str_hash;
  std::vector<PikaReplBgWorker*> bg_workers_;
};

#endif
