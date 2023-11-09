// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_DEFINE_H_
#define PIKA_DEFINE_H_

#include <glog/logging.h>
#include <set>
#include <utility>

#include "net/include/redis_cli.h"

/*
 * TTL type
 */
#define PIKA_TTL_ZERO 0
#define PIKA_TTL_NONE (-1)
#define PIKA_TTL_STALE (-2)

#define PIKA_SYNC_BUFFER_SIZE 1000
#define PIKA_MAX_WORKER_THREAD_NUM 24
#define PIKA_REPL_SERVER_TP_SIZE 3
#define PIKA_META_SYNC_MAX_WAIT_TIME 10
#define PIKA_SCAN_STEP_LENGTH 1000
#define PIKA_MAX_CONN_RBUF (1 << 28)     // 256MB
#define PIKA_MAX_CONN_RBUF_LB (1 << 26)  // 64MB
#define PIKA_MAX_CONN_RBUF_HB (1 << 29)  // 512MB
#define PIKA_SERVER_ID_MAX 65535

class PikaServer;

/* Port shift */
const int kPortShiftRSync = 1000;
const int kPortShiftReplServer = 2000;
//TODO: Temporarily used for rsync server port shift. will be deleted.
const int kPortShiftRsync2 = 10001;
const std::string kPikaPidFile = "pika.pid";
const std::string kPikaSecretFile = "rsync.secret";
const std::string kDefaultRsyncAuth = "default";

/* Rsync */
const int kMaxRsyncParallelNum = 4;

struct DBStruct {
  DBStruct(std::string tn, const uint32_t pn, std::set<uint32_t> pi)
      : db_name(std::move(tn)), slot_num(pn), slot_ids(std::move(pi)) {}

  bool operator==(const DBStruct& db_struct) const {
    return db_name == db_struct.db_name && slot_num == db_struct.slot_num &&
           slot_ids == db_struct.slot_ids;
  }
  std::string db_name;
  uint32_t slot_num = 0;
  std::set<uint32_t> slot_ids;
};

struct WorkerCronTask {
  int task;
  std::string ip_port;
};
using MonitorCronTask = WorkerCronTask;
// task define
#define TASK_KILL 0
#define TASK_KILLALL 1

// slave item
struct SlaveItem {
  std::string ip_port;
  std::string ip;
  int port;
  int conn_fd;
  int stage;
  std::vector<DBStruct> db_structs;
  struct timeval create_time;
};

enum ReplState {
  kNoConnect = 0,
  kTryConnect = 1,
  kTryDBSync = 2,
  kWaitDBSync = 3,
  kWaitReply = 4,
  kConnected = 5,
  kError = 6,
  // set to kDBNoConnect if execute cmd 'dbslaveof db no one'
  kDBNoConnect = 7
};

// debug only
const std::string ReplStateMsg[] = {"kNoConnect", "kTryConnect", "kTryDBSync", "kWaitDBSync",
                                    "kWaitReply", "kConnected",  "kError",     "kDBNoConnect"};

enum SlotState {
  INFREE = 0,
  INBUSY = 1,
};

struct LogicOffset {
  uint32_t term{0};
  uint64_t index{0};
  LogicOffset() = default;
  LogicOffset(uint32_t _term, uint64_t _index) : term(_term), index(_index) {}
  LogicOffset(const LogicOffset& other) {
    term = other.term;
    index = other.index;
  }
  bool operator==(const LogicOffset& other) const { return term == other.term && index == other.index; }
  bool operator!=(const LogicOffset& other) const { return term != other.term || index != other.index; }

  std::string ToString() const { return "term: " + std::to_string(term) + " index: " + std::to_string(index); }
};

struct BinlogOffset {
  uint32_t filenum{0};
  uint64_t offset{0};
  BinlogOffset() = default;
  BinlogOffset(uint32_t num, uint64_t off) : filenum(num), offset(off) {}
  BinlogOffset(const BinlogOffset& other) {
    filenum = other.filenum;
    offset = other.offset;
  }
  std::string ToString() const { return "filenum: " + std::to_string(filenum) + " offset: " + std::to_string(offset); }
  bool operator==(const BinlogOffset& other) const {
    return filenum == other.filenum && offset == other.offset;
  }
  bool operator!=(const BinlogOffset& other) const {
    return filenum != other.filenum || offset != other.offset;
  }

  bool operator>(const BinlogOffset& other) const {
    return filenum > other.filenum || (filenum == other.filenum && offset > other.offset);
  }
  bool operator<(const BinlogOffset& other) const {
    return filenum < other.filenum || (filenum == other.filenum && offset < other.offset);
  }
  bool operator<=(const BinlogOffset& other) const {
    return filenum < other.filenum || (filenum == other.filenum && offset <= other.offset);
  }
  bool operator>=(const BinlogOffset& other) const {
    return filenum > other.filenum || (filenum == other.filenum && offset >= other.offset);
  }
};

struct LogOffset {
  LogOffset(const LogOffset& _log_offset) {
    b_offset = _log_offset.b_offset;
    l_offset = _log_offset.l_offset;
  }
  LogOffset() = default;
  LogOffset(const BinlogOffset& _b_offset, const LogicOffset& _l_offset) : b_offset(_b_offset), l_offset(_l_offset) {}
  bool operator<(const LogOffset& other) const { return b_offset < other.b_offset; }
  bool operator==(const LogOffset& other) const { return b_offset == other.b_offset; }
  bool operator<=(const LogOffset& other) const { return b_offset <= other.b_offset; }
  bool operator>=(const LogOffset& other) const { return b_offset >= other.b_offset; }
  bool operator>(const LogOffset& other) const { return b_offset > other.b_offset; }
  std::string ToString() const { return b_offset.ToString() + " " + l_offset.ToString(); }
  BinlogOffset b_offset;
  LogicOffset l_offset;
};

// dbsync arg
struct DBSyncArg {
  PikaServer* p;
  std::string ip;
  int port;
  std::string db_name;
  uint32_t slot_id;
  DBSyncArg(PikaServer* const _p, std::string _ip, int _port, std::string _db_name,
            uint32_t _slot_id)
      : p(_p), ip(std::move(_ip)), port(_port), db_name(std::move(_db_name)), slot_id(_slot_id) {}
};

// rm define
enum SlaveState {
  kSlaveNotSync = 0,
  kSlaveDbSync = 1,
  kSlaveBinlogSync = 2,
};

// debug only
const std::string SlaveStateMsg[] = {"SlaveNotSync", "SlaveDbSync", "SlaveBinlogSync"};

enum BinlogSyncState {
  kNotSync = 0,
  kReadFromCache = 1,
  kReadFromFile = 2,
};

// debug only
const std::string BinlogSyncStateMsg[] = {"NotSync", "ReadFromCache", "ReadFromFile"};

struct BinlogChip {
  LogOffset offset_;
  std::string binlog_;
  BinlogChip(const LogOffset& offset, std::string binlog) : offset_(offset), binlog_(std::move(binlog)) {}
  BinlogChip(const BinlogChip& binlog_chip) {
    offset_ = binlog_chip.offset_;
    binlog_ = binlog_chip.binlog_;
  }
};

struct SlotInfo {
  SlotInfo(std::string db_name, uint32_t slot_id)
      : db_name_(std::move(db_name)), slot_id_(slot_id) {}

  SlotInfo() = default;

  bool operator==(const SlotInfo& other) const {
    return db_name_ == other.db_name_ && slot_id_ == other.slot_id_;
  }

  bool operator<(const SlotInfo& other) const {
    return db_name_ < other.db_name_ || (db_name_ == other.db_name_ && slot_id_ < other.slot_id_);
  }

  std::string ToString() const { return "(" + db_name_ + ":" + std::to_string(slot_id_) + ")"; }
  std::string db_name_;
  uint32_t slot_id_{0};
};

struct hash_slot_info {
  size_t operator()(const SlotInfo& n) const {
    return std::hash<std::string>()(n.db_name_) ^ std::hash<uint32_t>()(n.slot_id_);
  }
};

class Node {
 public:
  Node(std::string  ip, int port) : ip_(std::move(ip)), port_(port) {}
  virtual ~Node() = default;
  Node() = default;
  const std::string& Ip() const { return ip_; }
  int Port() const { return port_; }
  std::string ToString() const { return ip_ + ":" + std::to_string(port_); }

 private:
  std::string ip_;
  int port_ = 0;
};

class RmNode : public Node {
 public:
  RmNode(const std::string& ip, int port, SlotInfo  slot_info)
      : Node(ip, port), slot_info_(std::move(slot_info)) {}

  RmNode(const std::string& ip, int port, const std::string& db_name, uint32_t slot_id)
      : Node(ip, port),
        slot_info_(db_name, slot_id)
        {}

  RmNode(const std::string& ip, int port, const std::string& db_name, uint32_t slot_id, int32_t session_id)
      : Node(ip, port),
        slot_info_(db_name, slot_id),
        session_id_(session_id)
        {}

  RmNode(const std::string& db_name, uint32_t slot_id)
      :  slot_info_(db_name, slot_id) {}
  RmNode() = default;

  ~RmNode() override = default;
  bool operator==(const RmNode& other) const {
    return slot_info_.db_name_ == other.DBName() && slot_info_.slot_id_ == other.SlotId() &&
        Ip() == other.Ip() && Port() == other.Port();
  }

  const std::string& DBName() const { return slot_info_.db_name_; }
  uint32_t SlotId() const { return slot_info_.slot_id_; }
  const SlotInfo& NodeSlotInfo() const { return slot_info_; }
  void SetSessionId(int32_t session_id) { session_id_ = session_id; }
  int32_t SessionId() const { return session_id_; }
  std::string ToString() const {
    return "slot=" + DBName() + "_" + std::to_string(SlotId()) + ",ip_port=" + Ip() + ":" +
           std::to_string(Port()) + ",session id=" + std::to_string(SessionId());
  }
  void SetLastSendTime(uint64_t last_send_time) { last_send_time_ = last_send_time; }
  uint64_t LastSendTime() const { return last_send_time_; }
  void SetLastRecvTime(uint64_t last_recv_time) { last_recv_time_ = last_recv_time; }
  uint64_t LastRecvTime() const { return last_recv_time_; }

 private:
  SlotInfo slot_info_;
  int32_t session_id_ = 0;
  uint64_t last_send_time_ = 0;
  uint64_t last_recv_time_ = 0;
};

struct hash_rm_node {
  size_t operator()(const RmNode& n) const {
    return std::hash<std::string>()(n.DBName()) ^ std::hash<uint32_t>()(n.SlotId()) ^
           std::hash<std::string>()(n.Ip()) ^ std::hash<int>()(n.Port());
  }
};

struct WriteTask {
  struct RmNode rm_node_;
  struct BinlogChip binlog_chip_;
  LogOffset prev_offset_;
  WriteTask(const RmNode& rm_node, const BinlogChip& binlog_chip, const LogOffset& prev_offset)
      : rm_node_(rm_node), binlog_chip_(binlog_chip), prev_offset_(prev_offset) {}
};

// slowlog define
#define SLOWLOG_ENTRY_MAX_ARGC 32
#define SLOWLOG_ENTRY_MAX_STRING 128

// slowlog entry
struct SlowlogEntry {
  int64_t id;
  int64_t start_time;
  int64_t duration;
  net::RedisCmdArgsType argv;
};

#define PIKA_MIN_RESERVED_FDS 5000

const int SLAVE_ITEM_STAGE_ONE = 1;
const int SLAVE_ITEM_STAGE_TWO = 2;

// repl_state_
const int PIKA_REPL_NO_CONNECT = 0;
const int PIKA_REPL_SHOULD_META_SYNC = 1;
const int PIKA_REPL_META_SYNC_DONE = 2;
const int PIKA_REPL_ERROR = 3;

// role
const int PIKA_ROLE_SINGLE = 0;
const int PIKA_ROLE_SLAVE = 1;
const int PIKA_ROLE_MASTER = 2;

/*
 * cache model
 */
constexpr int PIKA_CACHE_NONE = 0;
constexpr int PIKA_CACHE_READ = 1;

/*
 * cache size
 */
#define PIKA_CACHE_SIZE_MIN       536870912    // 512M
#define PIKA_CACHE_SIZE_DEFAULT   10737418240  // 10G

/*
 * The size of Binlogfile
 */
// static uint64_t kBinlogSize = 128;
// static const uint64_t kBinlogSize = 1024 * 1024 * 100;

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
const std::string kContext = "context";

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

const std::string PCacheKeyPrefixK = "K";
const std::string PCacheKeyPrefixH = "H";
const std::string PCacheKeyPrefixS = "S";
const std::string PCacheKeyPrefixZ = "Z";
const std::string PCacheKeyPrefixL = "L";
const std::string PCacheKeyPrefixB = "B";


/*
 * cache status
 */
const int PIKA_CACHE_STATUS_NONE = 0;
const int PIKA_CACHE_STATUS_INIT = 1;
const int PIKA_CACHE_STATUS_OK = 2;
const int PIKA_CACHE_STATUS_RESET = 3;
const int PIKA_CACHE_STATUS_DESTROY = 4;
const int PIKA_CACHE_STATUS_CLEAR = 5;
const int CACHE_START_FROM_BEGIN = 0;
const int CACHE_START_FROM_END = -1;
/*
 * key type
 */
const char PIKA_KEY_TYPE_KV = 'k';
const char PIKA_KEY_TYPE_HASH = 'h';
const char PIKA_KEY_TYPE_LIST = 'l';
const char PIKA_KEY_TYPE_SET = 's';
const char PIKA_KEY_TYPE_ZSET = 'z';


/*
 * cache task type
 */
enum CacheBgTask {
  CACHE_BGTASK_CLEAR = 0,
  CACHE_BGTASK_RESET_NUM = 1,
  CACHE_BGTASK_RESET_CFG = 2
};

#endif
