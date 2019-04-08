// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_RM_H_
#define PIKA_RM_H_

#include <string>
#include <memory>
#include <unordered_map>
#include <queue>
#include <vector>

#include "slash/include/slash_status.h"

#include "include/pika_partition.h"
#include "include/pika_binlog_reader.h"
// #include "include/pika_repl_client.h"
// #include "include/pika_repl_server.h"

#define kBinlogSendPacketNum 30
#define kBinlogSendBatchNum 100
#define kBinlogReadWinSize 3000

using slash::Status;

enum Role {
  kPartitionSingle = 0,
  kPartitionMaster = 1,
  kPartitionSlave  = 2,
};

enum SlaveState {
  kSlaveNotSync    = 0,
  kSlaveDbSync     = 1,
  kSlaveBinlogSync = 2,
};

enum BinlogSyncState {
  kNotSync         = 0,
  kReadFromCache   = 1,
  kReadFromFile    = 2,
};

class Node {
 public:
  Node(const std::string& ip, int port) : ip_(ip), port_(port) {
  }
  Node() : port_(0) {
  }
  const std::string& Ip() const {
    return ip_;
  }
  int Port() const {
    return port_;
  }
  std::string ToString() const {
    return ip_ + ":" + std::to_string(port_);
  }
 private:
  std::string ip_;
  int port_;
};

struct PartitionInfo {
  PartitionInfo(const std::string& table_name, uint32_t partition_id)
    : table_name_(table_name), partition_id_(partition_id) {
  }
  PartitionInfo(const PartitionInfo& other) {
    table_name_ = other.table_name_;
    partition_id_ = other.partition_id_;
  }
  PartitionInfo() : partition_id_(0) {
  }
  bool operator==(const PartitionInfo& other) const {
    if (table_name_ == other.table_name_
        && partition_id_ == other.partition_id_) {
      return true;
    }
    return false;
  }
  std::string table_name_;
  uint32_t partition_id_;
};

struct hash_partition_info {
  size_t operator()(const PartitionInfo& n) const {
    return std::hash<std::string>()(n.table_name_) ^ std::hash<uint32_t>()(n.partition_id_);
  }
};

class RmNode : public Node {
 public:
  RmNode(const std::string& ip, int port,
      const PartitionInfo& partition_info)
    : Node(ip, port), partition_info_(partition_info), session_id_(0) {
  }
  RmNode(const std::string& ip, int port, const std::string& table_name, uint32_t partition_id) : Node(ip, port), partition_info_(table_name, partition_id), session_id_(0) {
  }
  RmNode() : Node(), partition_info_(), session_id_(0) {
  }

  bool operator==(const RmNode& other) const {
    if (partition_info_.table_name_ == other.TableName()
      && partition_info_.partition_id_ == other.PartitionId()
      && Ip() == other.Ip() && Port() == other.Port()) {
      return true;
    }
    return false;
  }

  const std::string& TableName() const {
    return partition_info_.table_name_;
  }
  uint32_t PartitionId() const {
    return partition_info_.partition_id_;
  }
  const PartitionInfo& NodePartitionInfo() const {
    return partition_info_;
  }
  void SetSessionId(uint32_t session_id) {
    session_id_ = session_id;
  }
  uint32_t SessionId() const {
    return session_id_;
  }
  std::string ToString() const {
    return TableName() + "_" + std::to_string(PartitionId()) + "_" + Ip() + std::to_string(Port());
  }

 private:
  PartitionInfo partition_info_;
  uint32_t session_id_;
};

struct hash_rm_node {
  size_t operator()(const RmNode& n) const {
    return std::hash<std::string>()(n.TableName()) ^ std::hash<uint32_t>()(n.PartitionId()) ^ std::hash<std::string>()(n.Ip()) ^ std::hash<int>()(n.Port());
  }
};

struct SyncWinItem {
  BinlogOffset offset_;
  bool acked_;
  bool operator==(const SyncWinItem& other) const {
    if (offset_.filenum == other.offset_.filenum && offset_.offset == other.offset_.offset) {
      return true;
    }
    return false;
  }
  explicit SyncWinItem(const BinlogOffset& offset) : offset_(offset), acked_(false) {
  }
  SyncWinItem(uint32_t filenum, uint64_t offset) : offset_(filenum, offset), acked_(false) {
  }
};

class SyncWindow {
 public:
  SyncWindow() {
  }
  void Push(const SyncWinItem& item);
  bool Update(const SyncWinItem& start_item, const SyncWinItem& end_item, BinlogOffset* acked_offset);
  int Remainings();
 private:
  // TODO(whoiami) ring buffer maybe
  std::vector<SyncWinItem> win_;
};

// role slave use
class MasterNode : public RmNode {
 public:
  MasterNode(const std::string& ip, int port, const std::string& table_name, uint32_t partition_id);
  MasterNode();
};

// role master use
class SlaveNode : public RmNode {
 public:
  SlaveNode(const std::string& ip, int port, const std::string& table_name, uint32_t partition_id);
  ~SlaveNode();
  void Lock() {
    slave_mu_.Lock();
  }
  void Unlock() {
    slave_mu_.Unlock();
  }
  SlaveState slave_state;

  BinlogSyncState b_state;
  SyncWindow sync_win;
  BinlogOffset sent_offset;
  BinlogOffset acked_offset;

  std::shared_ptr<PikaBinlogReader> binlog_reader;
  Status InitBinlogFileReader(const std::shared_ptr<Binlog>& binlog, const BinlogOffset& offset);
  void ReleaseBinlogFileReader();

 private:
  slash::Mutex slave_mu_;
};

class SyncPartition {
 public:
  SyncPartition(const std::string& table_name, uint32_t partition_id);
  void BecomeMaster();
  void BecomeSlave(const std::string& master_ip, int master_port);
  void BecomeSingle();
  Status AddSlaveNode(const std::string& ip, int port);
  Status RemoveSlaveNode(const std::string& ip, int port);

  Status ActivateSlaveBinlogSync(const std::string& ip, int port, const std::shared_ptr<Binlog> binlog, const BinlogOffset& offset);
  Status ActivateSlaveDbSync(const std::string& ip, int port);

  Status SyncBinlogToWq(const std::string& ip, int port);
  Status UpdateSlaveBinlogAckInfo(const std::string& ip, int port, const BinlogOffset& start, const BinlogOffset& end);
  Status GetSlaveSyncBinlogInfo(const std::string& ip, int port, BinlogOffset* sent_offset, BinlogOffset* acked_offset);

  Status WakeUpSlaveBinlogSync();

 private:
  bool CheckReadBinlogFromCache();
  // inovker need to hold partition_mu_
  void CleanMasterNode();
  void CleanSlaveNode();
  // invoker need to hold slave_mu_
  Status ReadCachedBinlogToWq(const std::shared_ptr<SlaveNode>& slave_ptr);
  Status ReadBinlogFileToWq(const std::shared_ptr<SlaveNode>& slave_ptr);
  // inovker need to hold partition_mu_
  Status GetSlaveNode(const std::string& ip, int port, std::shared_ptr<SlaveNode>* slave_node);

  slash::Mutex partition_mu_;
  PartitionInfo partition_info_;
  Role role_;
  // role slave use
  MasterNode m_info_;
  ReplState repl_state_;
  // role master use
  std::vector<std::shared_ptr<SlaveNode>> slaves_;
  // BinlogCacheWindow win_;
  // std::shared_ptr<Binlog> binlog_;
};

struct BinlogChip {
  BinlogOffset offset_;
  std::string binlog_;
  BinlogChip(BinlogOffset offset, std::string binlog) : offset_(offset), binlog_(binlog) {
  }
  BinlogChip(const BinlogChip& binlog_chip) {
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

class BinlogReaderManager {
 public:
  ~BinlogReaderManager();
  Status FetchBinlogReader(const RmNode& rm_node, std::shared_ptr<PikaBinlogReader>* reader);
  Status ReleaseBinlogReader(const RmNode& rm_node);
 private:
  slash::Mutex reader_mu_;
  std::unordered_map<RmNode, std::shared_ptr<PikaBinlogReader>, hash_rm_node> occupied_;
  std::vector<std::shared_ptr<PikaBinlogReader>> vacant_;
};

class PikaReplicaManager {
 public:
  PikaReplicaManager();
  ~PikaReplicaManager();

  Status AddSyncPartition(const std::string& table_name, uint32_t partition_id);
  Status RemoveSyncPartition(const std::string& table_name, uint32_t partition_id);

  Status AddPartitionSlave(const RmNode& slave);
  Status RemovePartitionSlave(const RmNode& slave);

  Status LostConnection(const std::string& ip, int port);

  Status ActivateBinlogSync(const RmNode& slave, const std::shared_ptr<Binlog> binlog, const BinlogOffset& offset);
  Status ActivateDbSync(const RmNode& slave);

  // Update binlog win and try to send next binlog
  Status UpdateSyncBinlogStatus(const RmNode& slave, const BinlogOffset& offset_start, const BinlogOffset& offset_end);
  Status GetSyncBinlogStatus(const RmNode& slave, BinlogOffset* sent_boffset, BinlogOffset* acked_boffset);

  Status WakeUpBinlogSync();

  // write_queue related
  void ProduceWriteQueue(const std::string& ip, int port, const std::vector<WriteTask>& tasks);
  int ConsumeWriteQueue();
  void DropItemInWriteQueue(const std::string& ip, int port);

  // bool GetConn(const RmNode& slave, std::shared_ptr<pink::PbConn>& conn);

  BinlogReaderManager binlog_reader_mgr;

 private:
  Status AddSlave(const RmNode& slave);
  Status RecordNodePartition(const RmNode& slave);
  Status RemoveSlave(const RmNode& slave);
  Status EraseNodePartition(const RmNode& slave);

  slash::Mutex node_partitions_mu_;
  // used to manage peer slave node to partition map
  std::unordered_map<std::string, std::vector<RmNode>> node_partitions_;
  pthread_rwlock_t partitions_rw_;
  std::unordered_map<PartitionInfo, std::shared_ptr<SyncPartition>, hash_partition_info> sync_partitions_;

  slash::Mutex  write_queue_mu_;
  // every host owns a queue
  std::unordered_map<std::string, std::queue<WriteTask>> write_queues_;  // ip+port, queue<WriteTask>
};

#endif  //  PIKA_RM_H
