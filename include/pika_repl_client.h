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
#include "include/pika_repl_client_thread.h"

#include "pink/include/thread_pool.h"
#include "src/pika_inner_message.pb.h"

#define kBinlogSyncBatchNum 10

using slash::Status;

struct RmNode {
  std::string table_;
  uint32_t partition_;
  std::string ip_;
  int port_;
  RmNode(const std::string& table, int partition, const std::string& ip, int port) : table_(table), partition_(partition), ip_(ip), port_(port) {
  }
  RmNode(const RmNode& node) {
    table_ = node.table_;
    partition_ = node.partition_;
    ip_ = node.ip_;
    port_ = node.port_;
  }
  bool operator <(const RmNode& other) const {
    if (table_ < other.table_) {
      return true;
    } else if (partition_ < other.partition_) {
      return true;
    } else if (ip_ < other.ip_) {
      return true;
    } else if (port_ < other.port_) {
      return true;
    }
    return false;
  }
  std::string ToString() const {
     return table_ + "_" + std::to_string(partition_) + "_" + ip_ + ":" + std::to_string(port_);
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

class PikaReplClient {
 public:
  PikaReplClient(int cron_interval, int keepalive_timeout);
  ~PikaReplClient();
  slash::Status Write(const std::string& ip, const int port, const std::string& msg);
  //void ThreadPollSchedule(pink::TaskFunc func, void*arg);
  int Start();
  Status AddBinlogReader(const RmNode& slave, std::shared_ptr<Binlog> logger, uint32_t filenum, uint64_t offset);
  Status RemoveBinlogReader(const RmNode& slave);

  bool NeedToSendBinlog(const RmNode& slave);

  Status SendMetaSync();
  Status SendPartitionTrySync(const std::string& table_name,
                              uint32_t partition_id,
                              const BinlogOffset& boffset);
  Status SendBinlogSync(const RmNode& slave);

  Status TriggerSendBinlogSync();

  int ConsumeWriteQueue();

  void Schedule(pink::TaskFunc func, void* arg){
    client_tp_->Schedule(func, arg);
  }

  bool SetAckInfo(const RmNode& slave, uint32_t ack_file_num, uint64_t ack_offset, uint64_t active_time);
  bool GetAckInfo(const RmNode& slave, uint32_t* act_file_num, uint64_t* ack_offset, uint64_t* active_time);

 private:
  PikaBinlogReader* NewPikaBinlogReader(std::shared_ptr<Binlog> logger, uint32_t filenum, uint64_t offset);

  void ProduceWriteQueue(WriteTask& task);

  void BuildBinlogPb(const RmNode& slave, const std::string& msg, uint32_t filenum, uint64_t offset, InnerMessage::InnerRequest& request);

  PikaReplClientThread* client_thread_;


  struct BinlogSyncCtl {
    slash::Mutex ctl_mu_;
    PikaBinlogReader* reader_;
    uint32_t ack_file_num_;
    uint64_t ack_offset_;
    uint64_t active_time_;

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
  std::map<RmNode, BinlogSyncCtl*> binlog_ctl_;

  slash::Mutex  write_queue_mu_;
  // every host owns a queue
  std::unordered_map<std::string, std::queue<WriteTask>> write_queues_;  // ip+port, queue<WriteTask>

  pink::ThreadPool* client_tp_;
};

#endif
