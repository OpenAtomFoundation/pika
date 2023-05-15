// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_COMMAND_H_
#define PIKA_COMMAND_H_

#include <unordered_map>

#include "net/include/net_conn.h"
#include "net/include/redis_conn.h"
#include "pstd/include/pstd_string.h"

#include "include/pika_partition.h"

class SyncMasterPartition;
class SyncSlavePartition;

// Constant for command name
// Admin
const std::string kCmdNameSlaveof = "slaveof";
const std::string kCmdNameDbSlaveof = "dbslaveof";
const std::string kCmdNameAuth = "auth";
const std::string kCmdNameBgsave = "bgsave";
const std::string kCmdNameCompact = "compact";
const std::string kCmdNamePurgelogsto = "purgelogsto";
const std::string kCmdNamePing = "ping";
const std::string kCmdNameSelect = "select";
const std::string kCmdNameFlushall = "flushall";
const std::string kCmdNameFlushdb = "flushdb";
const std::string kCmdNameClient = "client";
const std::string kCmdNameShutdown = "shutdown";
const std::string kCmdNameInfo = "info";
const std::string kCmdNameConfig = "config";
const std::string kCmdNameMonitor = "monitor";
const std::string kCmdNameDbsize = "dbsize";
const std::string kCmdNameTime = "time";
const std::string kCmdNameDelbackup = "delbackup";
const std::string kCmdNameEcho = "echo";
const std::string kCmdNameScandb = "scandb";
const std::string kCmdNameSlowlog = "slowlog";
const std::string kCmdNamePadding = "padding";
const std::string kCmdNamePKPatternMatchDel = "pkpatternmatchdel";
const std::string kCmdDummy = "dummy";
const std::string kCmdNameQuit = "quit";
const std::string kCmdNameHello = "hello";

// Kv
const std::string kCmdNameSet = "set";
const std::string kCmdNameGet = "get";
const std::string kCmdNameDel = "del";
const std::string kCmdNameUnlink = "unlink";
const std::string kCmdNameIncr = "incr";
const std::string kCmdNameIncrby = "incrby";
const std::string kCmdNameIncrbyfloat = "incrbyfloat";
const std::string kCmdNameDecr = "decr";
const std::string kCmdNameDecrby = "decrby";
const std::string kCmdNameGetset = "getset";
const std::string kCmdNameAppend = "append";
const std::string kCmdNameMget = "mget";
const std::string kCmdNameKeys = "keys";
const std::string kCmdNameSetnx = "setnx";
const std::string kCmdNameSetex = "setex";
const std::string kCmdNamePsetex = "psetex";
const std::string kCmdNameDelvx = "delvx";
const std::string kCmdNameMset = "mset";
const std::string kCmdNameMsetnx = "msetnx";
const std::string kCmdNameGetrange = "getrange";
const std::string kCmdNameSetrange = "setrange";
const std::string kCmdNameStrlen = "strlen";
const std::string kCmdNameExists = "exists";
const std::string kCmdNameExpire = "expire";
const std::string kCmdNamePexpire = "pexpire";
const std::string kCmdNameExpireat = "expireat";
const std::string kCmdNamePexpireat = "pexpireat";
const std::string kCmdNameTtl = "ttl";
const std::string kCmdNamePttl = "pttl";
const std::string kCmdNamePersist = "persist";
const std::string kCmdNameType = "type";
const std::string kCmdNameScan = "scan";
const std::string kCmdNameScanx = "scanx";
const std::string kCmdNamePKSetexAt = "pksetexat";
const std::string kCmdNamePKScanRange = "pkscanrange";
const std::string kCmdNamePKRScanRange = "pkrscanrange";

// Hash
const std::string kCmdNameHDel = "hdel";
const std::string kCmdNameHSet = "hset";
const std::string kCmdNameHGet = "hget";
const std::string kCmdNameHGetall = "hgetall";
const std::string kCmdNameHExists = "hexists";
const std::string kCmdNameHIncrby = "hincrby";
const std::string kCmdNameHIncrbyfloat = "hincrbyfloat";
const std::string kCmdNameHKeys = "hkeys";
const std::string kCmdNameHLen = "hlen";
const std::string kCmdNameHMget = "hmget";
const std::string kCmdNameHMset = "hmset";
const std::string kCmdNameHSetnx = "hsetnx";
const std::string kCmdNameHStrlen = "hstrlen";
const std::string kCmdNameHVals = "hvals";
const std::string kCmdNameHScan = "hscan";
const std::string kCmdNameHScanx = "hscanx";
const std::string kCmdNamePKHScanRange = "pkhscanrange";
const std::string kCmdNamePKHRScanRange = "pkhrscanrange";

// List
const std::string kCmdNameLIndex = "lindex";
const std::string kCmdNameLInsert = "linsert";
const std::string kCmdNameLLen = "llen";
const std::string kCmdNameBLPop = "blpop";
const std::string kCmdNameLPop = "lpop";
const std::string kCmdNameLPush = "lpush";
const std::string kCmdNameLPushx = "lpushx";
const std::string kCmdNameLRange = "lrange";
const std::string kCmdNameLRem = "lrem";
const std::string kCmdNameLSet = "lset";
const std::string kCmdNameLTrim = "ltrim";
const std::string kCmdNameRPop = "rpop";
const std::string kCmdNameRPopLPush = "rpoplpush";
const std::string kCmdNameRPush = "rpush";
const std::string kCmdNameRPushx = "rpushx";

// BitMap
const std::string kCmdNameBitSet = "setbit";
const std::string kCmdNameBitGet = "getbit";
const std::string kCmdNameBitPos = "bitpos";
const std::string kCmdNameBitOp = "bitop";
const std::string kCmdNameBitCount = "bitcount";

// Zset
const std::string kCmdNameZAdd = "zadd";
const std::string kCmdNameZCard = "zcard";
const std::string kCmdNameZScan = "zscan";
const std::string kCmdNameZIncrby = "zincrby";
const std::string kCmdNameZRange = "zrange";
const std::string kCmdNameZRangebyscore = "zrangebyscore";
const std::string kCmdNameZCount = "zcount";
const std::string kCmdNameZRem = "zrem";
const std::string kCmdNameZUnionstore = "zunionstore";
const std::string kCmdNameZInterstore = "zinterstore";
const std::string kCmdNameZRank = "zrank";
const std::string kCmdNameZRevrank = "zrevrank";
const std::string kCmdNameZScore = "zscore";
const std::string kCmdNameZRevrange = "zrevrange";
const std::string kCmdNameZRevrangebyscore = "zrevrangebyscore";
const std::string kCmdNameZRangebylex = "zrangebylex";
const std::string kCmdNameZRevrangebylex = "zrevrangebylex";
const std::string kCmdNameZLexcount = "zlexcount";
const std::string kCmdNameZRemrangebyrank = "zremrangebyrank";
const std::string kCmdNameZRemrangebylex = "zremrangebylex";
const std::string kCmdNameZRemrangebyscore = "zremrangebyscore";
const std::string kCmdNameZPopmax = "zpopmax";
const std::string kCmdNameZPopmin = "zpopmin";

// Set
const std::string kCmdNameSAdd = "sadd";
const std::string kCmdNameSPop = "spop";
const std::string kCmdNameSCard = "scard";
const std::string kCmdNameSMembers = "smembers";
const std::string kCmdNameSScan = "sscan";
const std::string kCmdNameSRem = "srem";
const std::string kCmdNameSUnion = "sunion";
const std::string kCmdNameSUnionstore = "sunionstore";
const std::string kCmdNameSInter = "sinter";
const std::string kCmdNameSInterstore = "sinterstore";
const std::string kCmdNameSIsmember = "sismember";
const std::string kCmdNameSDiff = "sdiff";
const std::string kCmdNameSDiffstore = "sdiffstore";
const std::string kCmdNameSMove = "smove";
const std::string kCmdNameSRandmember = "srandmember";

// HyperLogLog
const std::string kCmdNamePfAdd = "pfadd";
const std::string kCmdNamePfCount = "pfcount";
const std::string kCmdNamePfMerge = "pfmerge";

// GEO
const std::string kCmdNameGeoAdd = "geoadd";
const std::string kCmdNameGeoPos = "geopos";
const std::string kCmdNameGeoDist = "geodist";
const std::string kCmdNameGeoHash = "geohash";
const std::string kCmdNameGeoRadius = "georadius";
const std::string kCmdNameGeoRadiusByMember = "georadiusbymember";

// Pub/Sub
const std::string kCmdNamePublish = "publish";
const std::string kCmdNameSubscribe = "subscribe";
const std::string kCmdNameUnSubscribe = "unsubscribe";
const std::string kCmdNamePubSub = "pubsub";
const std::string kCmdNamePSubscribe = "psubscribe";
const std::string kCmdNamePUnSubscribe = "punsubscribe";

const std::string kClusterPrefix = "pkcluster";
typedef net::RedisCmdArgsType PikaCmdArgsType;
static const int RAW_ARGS_LEN = 1024 * 1024;

enum CmdFlagsMask {
  kCmdFlagsMaskRW = 1,
  kCmdFlagsMaskType = 30,
  kCmdFlagsMaskLocal = 32,
  kCmdFlagsMaskSuspend = 64,
  kCmdFlagsMaskPrior = 128,
  kCmdFlagsMaskAdminRequire = 256,
  kCmdFlagsMaskPartition = 1536
};

enum CmdFlags {
  kCmdFlagsRead = 0,  // default rw
  kCmdFlagsWrite = 1,
  kCmdFlagsAdmin = 0,  // default type
  kCmdFlagsKv = 2,
  kCmdFlagsHash = 4,
  kCmdFlagsList = 6,
  kCmdFlagsSet = 8,
  kCmdFlagsZset = 10,
  kCmdFlagsBit = 12,
  kCmdFlagsHyperLogLog = 14,
  kCmdFlagsGeo = 16,
  kCmdFlagsPubSub = 18,
  kCmdFlagsNoLocal = 0,  // default nolocal
  kCmdFlagsLocal = 32,
  kCmdFlagsNoSuspend = 0,  // default nosuspend
  kCmdFlagsSuspend = 64,
  kCmdFlagsNoPrior = 0,  // default noprior
  kCmdFlagsPrior = 128,
  kCmdFlagsNoAdminRequire = 0,  // default no need admin
  kCmdFlagsAdminRequire = 256,
  kCmdFlagsDoNotSpecifyPartition = 0,  // default do not specify partition
  kCmdFlagsSinglePartition = 512,
  kCmdFlagsMultiPartition = 1024
};

void inline RedisAppendContent(std::string& str, const std::string& value);
void inline RedisAppendLen(std::string& str, int64_t ori, const std::string& prefix);

const std::string kNewLine = "\r\n";

class CmdRes {
 public:
  enum CmdRet {
    kNone = 0,
    kOk,
    kPong,
    kSyntaxErr,
    kInvalidInt,
    kInvalidBitInt,
    kInvalidBitOffsetInt,
    kInvalidBitPosArgument,
    kWrongBitOpNotNum,
    kInvalidFloat,
    kOverFlow,
    kNotFound,
    kOutOfRange,
    kInvalidPwd,
    kNoneBgsave,
    kPurgeExist,
    kInvalidParameter,
    kWrongNum,
    kInvalidIndex,
    kInvalidDbType,
    kInvalidTable,
    kInconsistentHashTag,
    kErrOther
  };

  CmdRes() : ret_(kNone) {}

  bool none() const { return ret_ == kNone && message_.empty(); }
  bool ok() const { return ret_ == kOk || ret_ == kNone; }
  void clear() {
    message_.clear();
    ret_ = kNone;
  }
  std::string raw_message() const { return message_; }
  std::string message() const {
    std::string result;
    switch (ret_) {
      case kNone:
        return message_;
      case kOk:
        return "+OK\r\n";
      case kPong:
        return "+PONG\r\n";
      case kSyntaxErr:
        return "-ERR syntax error\r\n";
      case kInvalidInt:
        return "-ERR value is not an integer or out of range\r\n";
      case kInvalidBitInt:
        return "-ERR bit is not an integer or out of range\r\n";
      case kInvalidBitOffsetInt:
        return "-ERR bit offset is not an integer or out of range\r\n";
      case kWrongBitOpNotNum:
        return "-ERR BITOP NOT must be called with a single source key.\r\n";

      case kInvalidBitPosArgument:
        return "-ERR The bit argument must be 1 or 0.\r\n";
      case kInvalidFloat:
        return "-ERR value is not a valid float\r\n";
      case kOverFlow:
        return "-ERR increment or decrement would overflow\r\n";
      case kNotFound:
        return "-ERR no such key\r\n";
      case kOutOfRange:
        return "-ERR index out of range\r\n";
      case kInvalidPwd:
        return "-ERR invalid password\r\n";
      case kNoneBgsave:
        return "-ERR No BGSave Works now\r\n";
      case kPurgeExist:
        return "-ERR binlog already in purging...\r\n";
      case kInvalidParameter:
        return "-ERR Invalid Argument\r\n";
      case kWrongNum:
        result = "-ERR wrong number of arguments for '";
        result.append(message_);
        result.append("' command\r\n");
        break;
      case kInvalidIndex:
        result = "-ERR invalid DB index for '";
        result.append(message_);
        result.append("'\r\n");
        break;
      case kInvalidDbType:
        result = "-ERR invalid DB for '";
        result.append(message_);
        result.append("'\r\n");
        break;
      case kInconsistentHashTag:
        return "-ERR parameters hashtag is inconsistent\r\n";
      case kInvalidTable:
        result = "-ERR invalid Table for '";
        result.append(message_);
        result.append("'\r\n");
        break;
      case kErrOther:
        result = "-ERR ";
        result.append(message_);
        result.append(kNewLine);
        break;
      default:
        break;
    }
    return result;
  }

  // Inline functions for Create Redis protocol
  void AppendStringLen(int64_t ori) { RedisAppendLen(message_, ori, "$"); }
  void AppendArrayLen(int64_t ori) { RedisAppendLen(message_, ori, "*"); }
  void AppendInteger(int64_t ori) { RedisAppendLen(message_, ori, ":"); }
  void AppendContent(const std::string& value) { RedisAppendContent(message_, value); }
  void AppendString(const std::string& value) {
    AppendStringLen(value.size());
    AppendContent(value);
  }
  void AppendStringRaw(const std::string& value) { message_.append(value); }
  void SetRes(CmdRet _ret, const std::string content = "") {
    ret_ = _ret;
    if (!content.empty()) {
      message_ = content;
    }
  }

 private:
  std::string message_;
  CmdRet ret_ = kNone;
};

class Cmd : public std::enable_shared_from_this<Cmd> {
 public:
  enum CmdStage { kNone, kBinlogStage, kExecuteStage };
  struct HintKeys {
    HintKeys() {}
    void Push(const std::string& key, int hint) {
      keys.push_back(key);
      hints.push_back(hint);
    }
    bool empty() const { return keys.empty() && hints.empty(); }
    std::vector<std::string> keys;
    std::vector<int> hints;
  };
  struct ProcessArg {
    ProcessArg() {}
    ProcessArg(std::shared_ptr<Partition> _partition, std::shared_ptr<SyncMasterPartition> _sync_partition,
               HintKeys _hint_keys)
        : partition(_partition), sync_partition(_sync_partition), hint_keys(_hint_keys) {}
    std::shared_ptr<Partition> partition;
    std::shared_ptr<SyncMasterPartition> sync_partition;
    HintKeys hint_keys;
  };
  Cmd(const std::string& name, int arity, uint16_t flag)
      : name_(name), arity_(arity), flag_(flag), stage_(kNone), do_duration_(0) {}
  virtual ~Cmd() {}

  virtual std::vector<std::string> current_key() const;
  virtual void Execute();
  virtual void ProcessFlushDBCmd();
  virtual void ProcessFlushAllCmd();
  virtual void ProcessSinglePartitionCmd();
  virtual void ProcessMultiPartitionCmd();
  virtual void ProcessDoNotSpecifyPartitionCmd();
  virtual void Do(std::shared_ptr<Partition> partition = nullptr) = 0;
  virtual Cmd* Clone() = 0;
  // used for execute multikey command into different slots
  virtual void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) = 0;
  virtual void Merge() = 0;

  void Initial(const PikaCmdArgsType& argv, const std::string& table_name);

  bool is_write() const;
  bool is_local() const;
  bool is_suspend() const;
  bool is_admin_require() const;
  bool is_single_partition() const;
  bool is_multi_partition() const;
  bool HashtagIsConsistent(const std::string& lhs, const std::string& rhs) const;
  uint64_t GetDoDuration() const { return do_duration_; };

  std::string name() const;
  CmdRes& res();
  std::string table_name() const;
  BinlogOffset binlog_offset() const;
  const PikaCmdArgsType& argv() const;
  virtual std::string ToBinlog(uint32_t exec_time, uint32_t term_id, uint64_t logic_id, uint32_t filenum,
                               uint64_t offset);

  void SetConn(const std::shared_ptr<net::NetConn> conn);
  std::shared_ptr<net::NetConn> GetConn();

  void SetResp(const std::shared_ptr<std::string> resp);
  std::shared_ptr<std::string> GetResp();

  void SetStage(CmdStage stage);

 protected:
  // enable copy, used default copy
  // Cmd(const Cmd&);
  void ProcessCommand(std::shared_ptr<Partition> partition, std::shared_ptr<SyncMasterPartition> sync_partition,
                      const HintKeys& hint_key = HintKeys());
  void InternalProcessCommand(std::shared_ptr<Partition> partition, std::shared_ptr<SyncMasterPartition> sync_partition,
                              const HintKeys& hint_key);
  void DoCommand(std::shared_ptr<Partition> partition, const HintKeys& hint_key);
  void DoBinlog(std::shared_ptr<SyncMasterPartition> partition);
  bool CheckArg(int num) const;
  void LogCommand() const;

  std::string name_;
  int arity_ = -2;
  uint16_t flag_ = 0;

  CmdRes res_;
  PikaCmdArgsType argv_;
  std::string table_name_;

  std::weak_ptr<net::NetConn> conn_;
  std::weak_ptr<std::string> resp_;
  CmdStage stage_ = kNone;
  uint64_t do_duration_ = 0;

 private:
  virtual void DoInitial() = 0;
  virtual void Clear(){};

  Cmd& operator=(const Cmd&);
};

typedef std::unordered_map<std::string, Cmd*> CmdTable;

// Method for Cmd Table
void InitCmdTable(CmdTable* cmd_table);
Cmd* GetCmdFromTable(const std::string& opt, const CmdTable& cmd_table);
void DestoryCmdTable(CmdTable* cmd_table);

void RedisAppendContent(std::string& str, const std::string& value) {
  str.append(value.data(), value.size());
  str.append(kNewLine);
}

void RedisAppendLen(std::string& str, int64_t ori, const std::string& prefix) {
  char buf[32];
  pstd::ll2string(buf, 32, static_cast<long long>(ori));
  str.append(prefix);
  str.append(buf);
  str.append(kNewLine);
}

void TryAliasChange(std::vector<std::string>* argv);

#endif
