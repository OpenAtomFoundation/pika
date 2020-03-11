// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_DEFINE_H_
#define PIKA_DEFINE_H_

#include <set>
#include <glog/logging.h>

#include "pink/include/redis_cli.h"

#define PIKA_SYNC_BUFFER_SIZE           1000
#define PIKA_MAX_WORKER_THREAD_NUM      24
#define PIKA_REPL_SERVER_TP_SIZE        3
#define PIKA_META_SYNC_MAX_WAIT_TIME    10
#define PIKA_SCAN_STEP_LENGTH           1000
#define PIKA_MAX_CONN_RBUF              (1 << 28) // 256MB
#define PIKA_MAX_CONN_RBUF_LB           (1 << 26) // 64MB
#define PIKA_MAX_CONN_RBUF_HB           (1 << 29) // 512MB
#define PIKA_SERVER_ID_MAX              65535

class PikaServer;

/* Port shift */
const int kPortShiftRSync      = 1000;
const int kPortShiftReplServer = 2000;

const std::string kPikaPidFile = "pika.pid";
const std::string kPikaSecretFile = "rsync.secret";
const std::string kDefaultRsyncAuth = "default";

struct TableStruct {
  TableStruct(const std::string& tn,
              const uint32_t pn,
              const std::set<uint32_t>& pi)
      : table_name(tn), partition_num(pn), partition_ids(pi) {}

  bool operator == (const TableStruct& table_struct) const {
    return table_name == table_struct.table_name
        && partition_num == table_struct.partition_num
        && partition_ids == table_struct.partition_ids;
  }
  std::string table_name;
  uint32_t partition_num;
  std::set<uint32_t> partition_ids;
};

struct WorkerCronTask {
  int task;
  std::string ip_port;
};
typedef WorkerCronTask MonitorCronTask;
//task define
#define TASK_KILL 0
#define TASK_KILLALL 1

//slave item
struct SlaveItem {
  std::string ip_port;
  std::string ip;
  int port;
  int conn_fd;
  int stage;
  std::vector<TableStruct> table_structs;
  struct timeval create_time;
};

enum ReplState {
  kNoConnect   = 0,
  kTryConnect  = 1,
  kTryDBSync   = 2,
  kWaitDBSync  = 3,
  kWaitReply   = 4,
  kConnected   = 5,
  kError       = 6,
// set to kDBNoConnect if execute cmd 'dbslaveof db no one'
  kDBNoConnect = 7
};

// debug only
const std::string ReplStateMsg[] = {
  "kNoConnect",
  "kTryConnect",
  "kTryDBSync",
  "kWaitDBSync",
  "kWaitReply",
  "kConnected",
  "kError",
  "kDBNoConnect"
};

enum SlotState {
  INFREE = 0,
  INBUSY = 1,
};

struct LogicOffset {
  uint32_t term;
  uint64_t index;
  LogicOffset()
    : term(0), index(0) {}
  LogicOffset(uint32_t _term, uint64_t _index)
    : term(_term), index(_index) {}
  LogicOffset(const LogicOffset& other) {
    term = other.term;
    index = other.index;
  }
  bool operator==(const LogicOffset& other) const {
    return term == other.term && index == other.index;
  }
  bool operator!=(const LogicOffset& other) const {
    return term != other.term || index != other.index;
  }


  std::string ToString() const {
    return "term: " + std::to_string(term) + " index: " + std::to_string(index);
  }
};

struct BinlogOffset {
  uint32_t filenum;
  uint64_t offset;
  BinlogOffset()
      : filenum(0), offset(0) {}
  BinlogOffset(uint32_t num, uint64_t off)
      : filenum(num), offset(off) {}
  BinlogOffset(const BinlogOffset& other) {
    filenum = other.filenum;
    offset = other.offset;
  }
  std::string ToString() const {
    return "filenum: " + std::to_string(filenum) + " offset: " + std::to_string(offset);
  }
  bool operator==(const BinlogOffset& other) const {
    if (filenum == other.filenum && offset == other.offset) {
      return true;
    }
    return false;
  }
  bool operator!=(const BinlogOffset& other) const {
    if (filenum != other.filenum || offset != other.offset) {
      return true;
    }
    return false;
  }

  bool operator>(const BinlogOffset& other) const {
    if (filenum > other.filenum
        || (filenum == other.filenum && offset > other.offset)) {
      return true;
    }
    return false;
  }
  bool operator<(const BinlogOffset& other) const {
    if (filenum < other.filenum
        || (filenum == other.filenum && offset < other.offset)) {
      return true;
    }
    return false;
  }
  bool operator<=(const BinlogOffset& other) const {
    if (filenum < other.filenum
        || (filenum == other.filenum && offset <= other.offset)) {
      return true;
    }
    return false;
  }
  bool operator>=(const BinlogOffset& other) const {
    if (filenum > other.filenum
        || (filenum == other.filenum && offset >= other.offset)) {
      return true;
    }
    return false;
  }
};

struct LogOffset {
  LogOffset(const LogOffset& _log_offset) {
    b_offset = _log_offset.b_offset;
    l_offset = _log_offset.l_offset;
  }
  LogOffset() : b_offset(), l_offset() {
  }
  LogOffset(BinlogOffset _b_offset, LogicOffset _l_offset)
    : b_offset(_b_offset), l_offset(_l_offset) {
  }
  bool operator<(const LogOffset& other) const {
    return b_offset < other.b_offset;
  }
  bool operator==(const LogOffset& other) const {
    return b_offset == other.b_offset;
  }
  bool operator<=(const LogOffset& other) const {
    return b_offset <= other.b_offset;
  }
  bool operator>=(const LogOffset& other) const {
    return b_offset >= other.b_offset;
  }
  bool operator>(const LogOffset& other) const {
    return b_offset > other.b_offset;
  }
  std::string ToString() const  {
    return b_offset.ToString() + " " + l_offset.ToString();
  }
  BinlogOffset b_offset;
  LogicOffset  l_offset;
};

//dbsync arg
struct DBSyncArg {
  PikaServer* p;
  std::string ip;
  int port;
  std::string table_name;
  uint32_t partition_id;
  DBSyncArg(PikaServer* const _p,
            const std::string& _ip,
            int _port,
            const std::string& _table_name,
            uint32_t _partition_id)
      : p(_p), ip(_ip), port(_port),
        table_name(_table_name), partition_id(_partition_id) {}
};

// rm define
enum SlaveState {
  kSlaveNotSync    = 0,
  kSlaveDbSync     = 1,
  kSlaveBinlogSync = 2,
};

// debug only
const std::string SlaveStateMsg[] = {
  "SlaveNotSync",
  "SlaveDbSync",
  "SlaveBinlogSync"
};

enum BinlogSyncState {
  kNotSync         = 0,
  kReadFromCache   = 1,
  kReadFromFile    = 2,
};

// debug only
const std::string BinlogSyncStateMsg[] = {
  "NotSync",
  "ReadFromCache",
  "ReadFromFile"
};

struct BinlogChip {
  LogOffset offset_;
  std::string binlog_;
  BinlogChip(LogOffset offset, std::string binlog) : offset_(offset), binlog_(binlog) {
  }
  BinlogChip(const BinlogChip& binlog_chip) {
    offset_ = binlog_chip.offset_;
    binlog_ = binlog_chip.binlog_;
  }
};

struct PartitionInfo {
  PartitionInfo(const std::string& table_name, uint32_t partition_id)
    : table_name_(table_name), partition_id_(partition_id) {
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
  int operator<(const PartitionInfo& other) const {
    int ret = strcmp(table_name_.data(), other.table_name_.data());
    if (!ret) {
      if (partition_id_ < other.partition_id_) {
        ret = -1;
      } else if (partition_id_ > other.partition_id_) {
        ret = 1;
      } else {
        ret = 0;
      }
    }
    return ret;
  }
  std::string ToString() const {
    return "(" + table_name_ + ":" + std::to_string(partition_id_) + ")";
  }
  std::string table_name_;
  uint32_t partition_id_;
};

struct hash_partition_info {
  size_t operator()(const PartitionInfo& n) const {
    return std::hash<std::string>()(n.table_name_) ^ std::hash<uint32_t>()(n.partition_id_);
  }
};

class Node {
 public:
  Node(const std::string& ip, int port) : ip_(ip), port_(port) {
  }
  virtual ~Node() = default;
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

class RmNode : public Node {
 public:
  RmNode(const std::string& ip, int port,
         const PartitionInfo& partition_info)
    : Node(ip, port),
      partition_info_(partition_info),
      session_id_(0),
      last_send_time_(0),
      last_recv_time_(0) {}

  RmNode(const std::string& ip,
         int port,
         const std::string& table_name,
         uint32_t partition_id)
      : Node(ip, port),
        partition_info_(table_name, partition_id),
        session_id_(0),
        last_send_time_(0),
        last_recv_time_(0) {}

  RmNode(const std::string& ip,
         int port,
         const std::string& table_name,
         uint32_t partition_id,
         int32_t session_id)
      : Node(ip, port),
        partition_info_(table_name, partition_id),
        session_id_(session_id),
        last_send_time_(0),
        last_recv_time_(0) {}

  RmNode(const std::string& table_name,
         uint32_t partition_id)
      : Node(),
        partition_info_(table_name, partition_id),
        session_id_(0),
        last_send_time_(0),
        last_recv_time_(0) {}
  RmNode()
      : Node(),
      partition_info_(),
      session_id_(0),
      last_send_time_(0),
      last_recv_time_(0) {}

  virtual ~RmNode() = default;
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
  int32_t SessionId() const {
    return session_id_;
  }
  std::string ToString() const {
    return "partition=" + TableName() + "_" + std::to_string(PartitionId()) + ",ip_port="
        + Ip() + ":" + std::to_string(Port()) + ",session id=" + std::to_string(SessionId());
  }
  void SetLastSendTime(uint64_t last_send_time) {
    last_send_time_ = last_send_time;
  }
  uint64_t LastSendTime() const {
    return last_send_time_;
  }
  void SetLastRecvTime(uint64_t last_recv_time) {
    last_recv_time_ = last_recv_time;
  }
  uint64_t LastRecvTime() const {
    return last_recv_time_;
  }
 private:
  PartitionInfo partition_info_;
  int32_t session_id_;
  uint64_t last_send_time_;
  uint64_t last_recv_time_;
};

struct hash_rm_node {
  size_t operator()(const RmNode& n) const {
    return std::hash<std::string>()(n.TableName()) ^ std::hash<uint32_t>()(n.PartitionId()) ^ std::hash<std::string>()(n.Ip()) ^ std::hash<int>()(n.Port());
  }
};

struct WriteTask {
  struct RmNode rm_node_;
  struct BinlogChip binlog_chip_;
  LogOffset prev_offset_;
  WriteTask(RmNode rm_node, BinlogChip binlog_chip, LogOffset prev_offset) :
    rm_node_(rm_node), binlog_chip_(binlog_chip), prev_offset_(prev_offset) {
  }
};

//slowlog define
#define SLOWLOG_ENTRY_MAX_ARGC 32
#define SLOWLOG_ENTRY_MAX_STRING 128

//slowlog entry
struct SlowlogEntry {
  int64_t id;
  int64_t start_time;
  int64_t duration;
  pink::RedisCmdArgsType argv;
};

#define PIKA_MIN_RESERVED_FDS 5000

const int SLAVE_ITEM_STAGE_ONE    = 1;
const int SLAVE_ITEM_STAGE_TWO    = 2;

//repl_state_
const int PIKA_REPL_NO_CONNECT                  = 0;
const int PIKA_REPL_SHOULD_META_SYNC            = 1;
const int PIKA_REPL_META_SYNC_DONE              = 2;
const int PIKA_REPL_ERROR                       = 3;

//role
const int PIKA_ROLE_SINGLE        = 0;
const int PIKA_ROLE_SLAVE         = 1;
const int PIKA_ROLE_MASTER        = 2;

/*
 * The size of Binlogfile
 */
//static uint64_t kBinlogSize = 128; 
//static const uint64_t kBinlogSize = 1024 * 1024 * 100;

enum RecordType {
  kZeroType = 0,
  kFullType = 1,
  kFirstType = 2,
  kMiddleType = 3,
  kLastType = 4,
  kEof = 5,
  kBadRecord = 6,
  kOldRecord = 7
};

/*
 * the block size that we read and write from write2file
 * the default size is 64KB
 */
static const size_t kBlockSize = 64 * 1024;

/*
 * Header is Type(1 byte), length (3 bytes), time (4 bytes)
 */
static const size_t kHeaderSize = 1 + 3 + 4;

/*
 * the size of memory when we use memory mode
 * the default memory size is 2GB
 */
const int64_t kPoolSize = 1073741824;

const std::string kBinlogPrefix = "write2file";
const size_t kBinlogPrefixLen = 10;

const std::string kPikaMeta = "meta";
const std::string kManifest = "manifest";
const std::string kContext  = "context";

/*
 * define common character
 *
 */
#define COMMA ','

/*
 * define reply between master and slave
 *
 */
const std::string kInnerReplOk = "ok";
const std::string kInnerReplWait = "wait";

const unsigned int kMaxBitOpInputKey = 12800;
const int kMaxBitOpInputBit = 21;
/*
 * db sync
 */
const uint32_t kDBSyncMaxGap = 50;
const std::string kDBSyncModule = "document";

const std::string kBgsaveInfoFile = "info";
#endif
