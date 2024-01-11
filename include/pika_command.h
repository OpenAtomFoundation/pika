// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_COMMAND_H_
#define PIKA_COMMAND_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "net/include/net_conn.h"
#include "net/include/redis_conn.h"
#include "pstd/include/pstd_string.h"

#include "include/pika_slot.h"
#include "net/src/dispatch_thread.h"

class SyncMasterSlot;
class SyncSlaveSlot;

// Constant for command name
// Admin
const std::string kCmdNameSlaveof = "slaveof";
const std::string kCmdNameDbSlaveof = "dbslaveof";
const std::string kCmdNameAuth = "auth";
const std::string kCmdNameBgsave = "bgsave";
const std::string kCmdNameCompact = "compact";
const std::string kCmdNameCompactRange = "compactrange";
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
const std::string kCmdNameCommand = "command";
const std::string kCmdNameDiskRecovery = "diskrecovery";
const std::string kCmdNameClearReplicationID = "clearreplicationid";
const std::string kCmdNameDisableWal = "disablewal";
const std::string kCmdNameLastSave = "lastsave";
const std::string kCmdNameCache = "cache";
const std::string kCmdNameClearCache = "clearcache";

// Migrate slot
const std::string kCmdNameSlotsMgrtSlot = "slotsmgrtslot";
const std::string kCmdNameSlotsMgrtTagSlot = "slotsmgrttagslot";
const std::string kCmdNameSlotsMgrtOne = "slotsmgrtone";
const std::string kCmdNameSlotsMgrtTagOne = "slotsmgrttagone";
const std::string kCmdNameSlotsInfo = "slotsinfo";
const std::string kCmdNameSlotsHashKey = "slotshashkey";
const std::string kCmdNameSlotsReload = "slotsreload";
const std::string kCmdNameSlotsReloadOff = "slotsreloadoff";
const std::string kCmdNameSlotsDel = "slotsdel";
const std::string kCmdNameSlotsScan = "slotsscan";
const std::string kCmdNameSlotsCleanup = "slotscleanup";
const std::string kCmdNameSlotsCleanupOff = "slotscleanupoff";
const std::string kCmdNameSlotsMgrtTagSlotAsync = "slotsmgrttagslot-async";
const std::string kCmdNameSlotsMgrtSlotAsync = "slotsmgrtslot-async";
const std::string kCmdNameSlotsMgrtExecWrapper = "slotsmgrt-exec-wrapper";
const std::string kCmdNameSlotsMgrtAsyncStatus = "slotsmgrt-async-status";
const std::string kCmdNameSlotsMgrtAsyncCancel = "slotsmgrt-async-cancel";
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
const std::string kCmdNamePType = "ptype";
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
const std::string kCmdNameBRpop = "brpop";
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

// transation
const std::string kCmdNameMulti = "multi";
const std::string kCmdNameExec = "exec";
const std::string kCmdNameDiscard = "discard";
const std::string kCmdNameWatch = "watch";
const std::string kCmdNameUnWatch = "unwatch";

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

// ACL
const std::string KCmdNameAcl = "acl";

// Stream
const std::string kCmdNameXAdd = "xadd";
const std::string kCmdNameXDel = "xdel";
const std::string kCmdNameXRead = "xread";
const std::string kCmdNameXLen = "xlen";
const std::string kCmdNameXRange = "xrange";
const std::string kCmdNameXRevrange = "xrevrange";
const std::string kCmdNameXTrim = "xtrim";
const std::string kCmdNameXInfo = "xinfo";

const std::string kClusterPrefix = "pkcluster";
using PikaCmdArgsType = net::RedisCmdArgsType;
static const int RAW_ARGS_LEN = 1024 * 1024;

enum CmdFlags {
  kCmdFlagsRead = 1,  // default rw
  kCmdFlagsWrite = (1 << 1),
  kCmdFlagsAdmin = (1 << 2),  // default type
  kCmdFlagsKv = (1 << 3),
  kCmdFlagsHash = (1 << 4),
  kCmdFlagsList = (1 << 5),
  kCmdFlagsSet = (1 << 6),
  kCmdFlagsZset = (1 << 7),
  kCmdFlagsBit = (1 << 8),
  kCmdFlagsHyperLogLog = (1 << 9),
  kCmdFlagsGeo = (1 << 10),
  kCmdFlagsPubSub = (1 << 11),
  kCmdFlagsLocal = (1 << 12),
  kCmdFlagsSuspend = (1 << 13),
  kCmdFlagsAdminRequire = (1 << 14),
  kCmdFlagsSingleSlot = (1 << 15),
  kCmdFlagsMultiSlot = (1 << 16),
  kCmdFlagsNoAuth = (1 << 17),  // command no auth can also be executed
  kCmdFlagsReadCache = (1 << 18),
  kCmdFlagsUpdateCache = (1 << 19),
  kCmdFlagsDoThroughDB = (1 << 20),
  kCmdFlagsOperateKey = (1 << 21),  // redis keySpace
  kCmdFlagsPreDo = (1 << 22),
  kCmdFlagsStream = (1 << 23),
  kCmdFlagsFast = (1 << 24),
  kCmdFlagsSlow = (1 << 25),
};

void inline RedisAppendContent(std::string& str, const std::string& value);
void inline RedisAppendLen(std::string& str, int64_t ori, const std::string& prefix);
void inline RedisAppendLenUint64(std::string& str, uint64_t ori, const std::string& prefix) {
  RedisAppendLen(str, static_cast<int64_t>(ori), prefix);
}

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
    kInvalidDB,
    kInconsistentHashTag,
    kErrOther,
    kCacheMiss,
    KIncrByOverFlow,
    kInvalidTransaction,
    kTxnQueued,
    kTxnAbort,
  };

  CmdRes() = default;

  bool none() const { return ret_ == kNone && message_.empty(); }
  bool ok() const { return ret_ == kOk || ret_ == kNone; }
  CmdRet ret() const { return ret_; }
  void clear() {
    message_.clear();
    ret_ = kNone;
  }
  bool CacheMiss() const { return ret_ == kCacheMiss; }
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
      case kInvalidDB:
        result = "-ERR invalid DB for '";
        result.append(message_);
        result.append("'\r\n");
        break;
      case kInvalidTransaction:
        return "-ERR WATCH inside MULTI is not allowed\r\n";
      case kTxnQueued:
        result = "+QUEUED";
        result.append("\r\n");
        break;
      case kTxnAbort:
        result = "-EXECABORT ";
        result.append(message_);
        result.append(kNewLine);
        break;
      case kErrOther:
        result = "-ERR ";
        result.append(message_);
        result.append(kNewLine);
        break;
      case KIncrByOverFlow:
        result = "-ERR increment would produce NaN or Infinity";
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
  void AppendStringLenUint64(uint64_t ori) { RedisAppendLenUint64(message_, ori, "$"); }
  void AppendArrayLen(int64_t ori) { RedisAppendLen(message_, ori, "*"); }
  void AppendArrayLenUint64(uint64_t ori) { RedisAppendLenUint64(message_, ori, "*"); }
  void AppendInteger(int64_t ori) { RedisAppendLen(message_, ori, ":"); }
  void AppendContent(const std::string& value) { RedisAppendContent(message_, value); }
  void AppendString(const std::string& value) {
    AppendStringLenUint64(value.size());
    AppendContent(value);
  }
  void AppendStringRaw(const std::string& value) { message_.append(value); }

  void AppendStringVector(const std::vector<std::string>& strArray) {
    if (strArray.empty()) {
      AppendArrayLen(-1);
      return;
    }
    AppendArrayLen(strArray.size());
    for (const auto& item : strArray) {
      AppendString(item);
    }
  }

  void SetRes(CmdRet _ret, const std::string& content = "") {
    ret_ = _ret;
    if (!content.empty()) {
      message_ = content;
    }
  }
  CmdRet GetCmdRet() const { return ret_; }

 private:
  std::string message_;
  CmdRet ret_ = kNone;
};

/**
 * Current used by:
 * blpop,brpop
 */
struct UnblockTaskArgs {
  std::string key;
  std::shared_ptr<Slot> slot;
  net::DispatchThread* dispatchThread{nullptr};
  UnblockTaskArgs(std::string key_, std::shared_ptr<Slot> slot_, net::DispatchThread* dispatchThread_)
      : key(std::move(key_)), slot(std::move(slot_)), dispatchThread(dispatchThread_) {}
};

class Cmd : public std::enable_shared_from_this<Cmd> {
 public:
  enum CmdStage { kNone, kBinlogStage, kExecuteStage };
  struct HintKeys {
    HintKeys() = default;
    void Push(const std::string& key, int hint) {
      keys.push_back(key);
      hints.push_back(hint);
    }
    bool empty() const { return keys.empty() && hints.empty(); }
    std::vector<std::string> keys;
    std::vector<int> hints;
  };
  struct ProcessArg {
    ProcessArg() = default;
    ProcessArg(std::shared_ptr<Slot> _slot, std::shared_ptr<SyncMasterSlot> _sync_slot, HintKeys _hint_keys)
        : slot(std::move(_slot)), sync_slot(std::move(_sync_slot)), hint_keys(std::move(_hint_keys)) {}
    std::shared_ptr<Slot> slot;
    std::shared_ptr<SyncMasterSlot> sync_slot;
    HintKeys hint_keys;
  };
  struct CommandStatistics {
    CommandStatistics() = default;
    CommandStatistics(const CommandStatistics& other) {
      cmd_time_consuming.store(other.cmd_time_consuming.load());
      cmd_count.store(other.cmd_count.load());
    }
    std::atomic<int32_t> cmd_count = {0};
    std::atomic<int32_t> cmd_time_consuming = {0};
  };
  CommandStatistics state;
  Cmd(std::string name, int arity, uint32_t flag, uint32_t aclCategory = 0);
  virtual ~Cmd() = default;

  virtual std::vector<std::string> current_key() const;
  virtual void Execute();
  virtual void ProcessSingleSlotCmd();
  virtual void ProcessMultiSlotCmd();
  virtual void Do(std::shared_ptr<Slot> slot = nullptr) = 0;
  virtual void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) {}
  virtual void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) {}
  virtual void ReadCache(std::shared_ptr<Slot> slot = nullptr) {}
  rocksdb::Status CmdStatus() { return s_; };
  virtual Cmd* Clone() = 0;
  // used for execute multikey command into different slots
  virtual void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) = 0;
  virtual void Merge() = 0;

  int8_t SubCmdIndex(const std::string& cmdName);  // if the command no subCommand，return -1；

  void Initial(const PikaCmdArgsType& argv, const std::string& db_name);

  uint32_t flag() const;
  bool hasFlag(uint32_t flag) const;
  bool is_read() const;
  bool is_write() const;

  bool IsLocal() const;
  bool IsSuspend() const;
  bool IsAdminRequire() const;
  bool is_single_slot() const;
  bool is_multi_slot() const;
  bool HasSubCommand() const;                   // The command is there a sub command
  std::vector<std::string> SubCommand() const;  // Get command is there a sub command
  bool IsNeedUpdateCache() const;
  bool is_only_from_cache() const;
  bool IsNeedReadCache() const;
  bool IsNeedCacheDo() const;
  bool HashtagIsConsistent(const std::string& lhs, const std::string& rhs) const;
  uint64_t GetDoDuration() const { return do_duration_; };
  uint32_t AclCategory() const;
  void AddAclCategory(uint32_t aclCategory);
  void SetDbName(const std::string& db_name) { db_name_ = db_name; }
  std::string GetDBName() { return db_name_; }

  std::string name() const;
  CmdRes& res();
  std::string db_name() const;
  BinlogOffset binlog_offset() const;
  PikaCmdArgsType& argv();
  virtual std::string ToRedisProtocol();

  void SetConn(const std::shared_ptr<net::NetConn>& conn);
  std::shared_ptr<net::NetConn> GetConn();

  void SetResp(const std::shared_ptr<std::string>& resp);
  std::shared_ptr<std::string> GetResp();

  void SetStage(CmdStage stage);

  virtual void DoBinlog(const std::shared_ptr<SyncMasterSlot>& slot);

  uint32_t GetCmdId() const { return cmdId_; };
  bool CheckArg(uint64_t num) const;

 protected:
  // enable copy, used default copy
  // Cmd(const Cmd&);
  void ProcessCommand(const std::shared_ptr<Slot>& slot, const std::shared_ptr<SyncMasterSlot>& sync_slot,
                      const HintKeys& hint_key = HintKeys());
  void InternalProcessCommand(const std::shared_ptr<Slot>& slot, const std::shared_ptr<SyncMasterSlot>& sync_slot,
                              const HintKeys& hint_key);
  void DoCommand(const std::shared_ptr<Slot>& slot, const HintKeys& hint_key);
  void LogCommand() const;

  std::string name_;
  int arity_ = -2;
  uint32_t flag_ = 0;

  std::vector<std::string> subCmdName_;  // sub command name, may be empty

 protected:
  CmdRes res_;
  PikaCmdArgsType argv_;
  std::string db_name_;
  rocksdb::Status s_;

  std::weak_ptr<net::NetConn> conn_;
  std::weak_ptr<std::string> resp_;
  CmdStage stage_ = kNone;
  uint64_t do_duration_ = 0;
  uint32_t cmdId_ = 0;
  uint32_t aclCategory_ = 0;

 private:
  virtual void DoInitial() = 0;
  virtual void Clear(){};

  Cmd& operator=(const Cmd&);
};

using CmdTable = std::unordered_map<std::string, std::unique_ptr<Cmd>>;

// Method for Cmd Table
void InitCmdTable(CmdTable* cmd_table);
Cmd* GetCmdFromDB(const std::string& opt, const CmdTable& cmd_table);

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
