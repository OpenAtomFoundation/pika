// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_list.h"
#include <utility>
#include "include/pika_cache.h"
#include "include/pika_data_distribution.h"
#include "include/pika_rm.h"
#include "include/pika_server.h"
#include "include/pika_slot_command.h"
#include "pstd/include/pstd_string.h"
#include "scope_record_lock.h"

extern PikaServer* g_pika_server;
extern std::unique_ptr<PikaReplicaManager> g_pika_rm;

void LIndexCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLIndex);
    return;
  }
  key_ = argv_[1];
  std::string index = argv_[2];
  if (pstd::string2int(index.data(), index.size(), &index_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
  }
}

void LIndexCmd::Do(std::shared_ptr<DB> db) {
  std::string value;
  s_ = db->storage()->LIndex(key_, index_, &value);
  if (s_.ok()) {
    res_.AppendString(value);
  } else if (s_.IsNotFound()) {
    res_.AppendStringLen(-1);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void LIndexCmd::ReadCache(std::shared_ptr<DB> db) {
  std::string CachePrefixKeyL = PCacheKeyPrefixL + key_;
  std::string value;
  auto s = db->cache()->LIndex(CachePrefixKeyL, index_, &value);
  if (s.ok()) {
    res_.AppendString(value);
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void LIndexCmd::DoThroughDB(std::shared_ptr<DB> db) {
  res_.clear();
   Do(db);
}

void LIndexCmd::DoUpdateCache(std::shared_ptr<DB> db) {
  if (s_.ok()) {
    db->cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_LIST, key_, db);
  }
}

void LInsertCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLInsert);
    return;
  }
  key_ = argv_[1];
  std::string dir = argv_[2];
  if (strcasecmp(dir.data(), "before") == 0) {
    dir_ = storage::Before;
  } else if (strcasecmp(dir.data(), "after") == 0) {
    dir_ = storage::After;
  } else {
    res_.SetRes(CmdRes::kSyntaxErr);
    return;
  }
  pivot_ = argv_[3];
  value_ = argv_[4];
}

void LInsertCmd::Do(std::shared_ptr<DB> db) {
  int64_t llen = 0;
  s_ = db->storage()->LInsert(key_, dir_, pivot_, value_, &llen);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendInteger(llen);
    AddSlotKey("l", key_, db);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void LInsertCmd::DoThroughDB(std::shared_ptr<DB> db) {
   Do(db);
}

void LInsertCmd::DoUpdateCache(std::shared_ptr<DB> db) {
  if (s_.ok()) {
    std::string CachePrefixKeyL = PCacheKeyPrefixL + key_;
    db->cache()->LInsert(CachePrefixKeyL, dir_, pivot_, value_);
  }
}

void LLenCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLLen);
    return;
  }
  key_ = argv_[1];
}

void LLenCmd::Do(std::shared_ptr<DB> db) {
  uint64_t llen = 0;
  s_ = db->storage()->LLen(key_, &llen);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendInteger(static_cast<int64_t>(llen));
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void LLenCmd::ReadCache(std::shared_ptr<DB> db) {
  std::string CachePrefixKeyL = PCacheKeyPrefixL + key_;
  uint64_t llen = 0;
  auto s = db->cache()->LLen(CachePrefixKeyL, &llen);
  if (s.ok()){
    res_.AppendInteger(llen);
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void LLenCmd::DoThroughDB(std::shared_ptr<DB> db) {
  res_.clear();
   Do(db);
}

void LLenCmd::DoUpdateCache(std::shared_ptr<DB> db) {
  if (s_.ok()) {
    db->cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_LIST, key_, db);
  }
}

void BlockingBaseCmd::TryToServeBLrPopWithThisKey(const std::string& key, std::shared_ptr<DB> db) {
  std::shared_ptr<net::RedisConn> curr_conn = std::dynamic_pointer_cast<net::RedisConn>(GetConn());
  if (!curr_conn) {
    // current node is a slave and is applying a binlog of lpush/rpush/rpoplpush, just return
    return;
  }
  auto dispatchThread = dynamic_cast<net::DispatchThread*>(curr_conn->thread());

  {
    std::shared_lock read_latch(dispatchThread->GetBlockMtx());
    auto& key_to_conns = dispatchThread->GetMapFromKeyToConns();
    net::BlockKey blrPop_key{curr_conn->GetCurrentTable(), key};

    if (auto it = key_to_conns.find(blrPop_key); it == key_to_conns.end()) {
      // no client is waitting for this key
      return;
    }
  }

  auto* args = new UnblockTaskArgs(key, std::move(db), dispatchThread);
  g_pika_server->ScheduleClientPool(&ServeAndUnblockConns, args);
}

void BlockingBaseCmd::ServeAndUnblockConns(void* args) {
  auto bg_args = std::unique_ptr<UnblockTaskArgs>(static_cast<UnblockTaskArgs*>(args));
  net::DispatchThread* dispatchThread = bg_args->dispatchThread;
  std::shared_ptr<DB> db = bg_args->db;
  std::string key = std::move(bg_args->key);
  auto& key_to_conns_ = dispatchThread->GetMapFromKeyToConns();
  net::BlockKey blrPop_key{db->GetDBName(), key};

  pstd::lock::ScopeRecordLock record_lock(db->LockMgr(), key);//It's a RAII Lock
  std::unique_lock map_lock(dispatchThread->GetBlockMtx());// do not change the sequence of these two lock, or deadlock will happen
  auto it = key_to_conns_.find(blrPop_key);
  if (it == key_to_conns_.end()) {
    return;
  }
  CmdRes res;
  std::vector<WriteBinlogOfPopArgs> pop_binlog_args;
  auto& waitting_list = it->second;
  std::vector<std::string> values;
  rocksdb::Status s;
  // traverse this list from head to tail(in the order of adding sequence) ,means "first blocked, first get served“
  for (auto conn_blocked = waitting_list->begin(); conn_blocked != waitting_list->end();) {
    if (conn_blocked->GetBlockType() == BlockKeyType::Blpop) {
      s = db->storage()->LPop(key, 1, &values);
    } else {  // BlockKeyType is Brpop
      s = db->storage()->RPop(key, 1, &values);
    }
    if (s.ok()) {
      res.AppendArrayLen(2);
      res.AppendString(key);
      res.AppendString(values[0]);
    } else if (s.IsNotFound()) {
      // this key has no more elements to serve more blocked conn.
      break;
    } else {
      res.SetRes(CmdRes::kErrOther, s.ToString());
    }
    auto conn_ptr = conn_blocked->GetConnBlocked();
    // send response to this client
    conn_ptr->WriteResp(res.message());
    res.clear();
    conn_ptr->NotifyEpoll(true);
    pop_binlog_args.emplace_back(conn_blocked->GetBlockType(), key, db, conn_ptr);
    conn_blocked = waitting_list->erase(conn_blocked);  // remove this conn from current waiting list
    // erase all waiting info of this conn
    dispatchThread->CleanWaitNodeOfUnBlockedBlrConn(conn_ptr);
  }
  dispatchThread->CleanKeysAfterWaitNodeCleaned();
  map_lock.unlock();
  WriteBinlogOfPop(pop_binlog_args);
}

void BlockingBaseCmd::WriteBinlogOfPop(std::vector<WriteBinlogOfPopArgs>& pop_args) {
  // write binlog of l/rpop
  for (auto& pop_arg : pop_args) {
    std::shared_ptr<Cmd> pop_cmd;
    std::string pop_type;
    if (pop_arg.block_type == BlockKeyType::Blpop) {
      pop_type = kCmdNameLPop;
      pop_cmd = std::make_shared<LPopCmd>(kCmdNameLPop, 2, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsList);
    } else if (pop_arg.block_type == BlockKeyType::Brpop) {
      pop_type = kCmdNameRPop;
      pop_cmd = std::make_shared<RPopCmd>(kCmdNameRPop, 2, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsList);
    }

    PikaCmdArgsType args;
    args.push_back(std::move(pop_type));
    args.push_back(pop_arg.key);
    pop_cmd->Initial(args, pop_arg.db->GetDBName());
    std::shared_ptr<SyncMasterDB> sync_db =
        g_pika_rm->GetSyncMasterDBByName(DBInfo(pop_arg.db->GetDBName()));
    if (!sync_db) {
      LOG(WARNING) << "Writing binlog of blpop/brpop failed: SyncMasterSlot not found";
    } else {
      pop_cmd->SetConn(pop_arg.conn);
      auto resp_ptr = std::make_shared<std::string>("this resp won't be used for current code(consensus-level always be 0)");
      pop_cmd->SetResp(resp_ptr);
      pop_cmd->DoBinlog(sync_db);
    }
  }
}

void LPushCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLPush);
    return;
  }
  key_ = argv_[1];
  size_t pos = 2;
  while (pos < argv_.size()) {
    values_.push_back(argv_[pos++]);
  }
}

void LPushCmd::Do(std::shared_ptr<DB> db) {
  uint64_t llen = 0;
  s_ = db->storage()->LPush(key_, values_, &llen);
  if (s_.ok()) {
    res_.AppendInteger(static_cast<int64_t>(llen));
    AddSlotKey("l", key_, db);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
  if (auto client_conn = std::dynamic_pointer_cast<PikaClientConn>(GetConn()); client_conn != nullptr) {
    if (client_conn->IsInTxn()) {
      return;
    }
  }
  TryToServeBLrPopWithThisKey(key_, db);
}

void LPushCmd::DoThroughDB(std::shared_ptr<DB> db) {
   Do(db);
}

void LPushCmd::DoUpdateCache(std::shared_ptr<DB> db) {
  if (s_.ok()) {
    std::string CachePrefixKeyL = PCacheKeyPrefixL + key_;
    db->cache()->LPushx(CachePrefixKeyL, values_);
  }
}

void BlockingBaseCmd::BlockThisClientToWaitLRPush(BlockKeyType block_pop_type, std::vector<std::string>& keys,
                                                  int64_t expire_time) {
  std::shared_ptr<net::RedisConn> conn_to_block = std::dynamic_pointer_cast<net::RedisConn>(GetConn());

  auto dispatchThread = dynamic_cast<net::DispatchThread*>(conn_to_block->thread());
  std::lock_guard latch(dispatchThread->GetBlockMtx());
  auto& key_to_conns = dispatchThread->GetMapFromKeyToConns();
  auto& conn_to_keys_ = dispatchThread->GetMapFromConnToKeys();

  std::vector<net::BlockKey> blrpop_keys;
  for (auto& key : keys) {
    net::BlockKey blrpop_key{conn_to_block->GetCurrentTable(), key};
    blrpop_keys.push_back(blrpop_key);
    auto it = key_to_conns.find(blrpop_key);
    if (it == key_to_conns.end()) {
      // no waiting info found, means no other clients are waiting for the list related with this key right now
      key_to_conns.emplace(blrpop_key, std::make_unique<std::list<net::BlockedConnNode>>());
      it = key_to_conns.find(blrpop_key);
    }
    auto& wait_list_of_this_key = it->second;
    // add current client-connection to the tail of waiting list of this key
    wait_list_of_this_key->emplace_back(expire_time, conn_to_block, block_pop_type);
  }

  // construct a list of keys and insert into this map as value(while key of the map is conn_fd)
  conn_to_keys_.emplace(conn_to_block->fd(),
                        std::make_unique<std::list<net::BlockKey>>(blrpop_keys.begin(), blrpop_keys.end()));
}

void BlockingBaseCmd::removeDuplicates(std::vector<std::string>& keys_) {
  std::unordered_set<std::string> seen;
  auto it = std::remove_if(keys_.begin(), keys_.end(), [&seen](const auto& key) { return !seen.insert(key).second; });
  keys_.erase(it, keys_.end());
}

void BLPopCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameBLPop);
    return;
  }

  // fetching all keys(*argv_.begin is the command itself and *argv_.end() is the timeout value)
  keys_.assign(++argv_.begin(), --argv_.end());
  removeDuplicates(keys_);
  int64_t timeout = 0;
  if (!pstd::string2int(argv_.back().data(), argv_.back().size(), &timeout)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  constexpr int64_t seconds_of_ten_years = 10 * 365 * 24 * 3600;
  if (timeout < 0 || timeout > seconds_of_ten_years) {
    res_.SetRes(CmdRes::kErrOther,
                "timeout can't be a negative value and can't exceed the number of seconds in 10 years");
    return;
  }

  if (timeout > 0) {
    auto now = std::chrono::system_clock::now();
    expire_time_ =
        std::chrono::time_point_cast<std::chrono::milliseconds>(now).time_since_epoch().count() + timeout * 1000;
  }  // else(timeout is 0): expire_time_ default value is 0, means never expire;
}

void BLPopCmd::Do(std::shared_ptr<DB> db) {
  for (auto& this_key : keys_) {
    std::vector<std::string> values;
    s_ = db->storage()->LPop(this_key, 1, &values);
    if (s_.ok()) {
      res_.AppendArrayLen(2);
      res_.AppendString(this_key);
      res_.AppendString(values[0]);
      // write an binlog of lpop
      binlog_args_.block_type = BlockKeyType::Blpop;
      binlog_args_.key = this_key;
      binlog_args_.db = db;
      binlog_args_.conn = GetConn();
      is_binlog_deferred_ = false;
      return;
    } else if (s_.IsNotFound()) {
      continue;
    } else {
      res_.SetRes(CmdRes::kErrOther, s_.ToString());
      return;
    }
  }
  is_binlog_deferred_ = true;
  if (auto client_conn = std::dynamic_pointer_cast<PikaClientConn>(GetConn()); client_conn != nullptr) {
    if (client_conn->IsInTxn()) {
      res_.AppendArrayLen(-1);
      return ;
    }
  }
  BlockThisClientToWaitLRPush(BlockKeyType::Blpop, keys_, expire_time_);
}

void BLPopCmd::DoBinlog(const std::shared_ptr<SyncMasterDB>& db) {
  if (is_binlog_deferred_) {
    return;
  }
  std::vector<WriteBinlogOfPopArgs> args;
  args.push_back(std::move(binlog_args_));
  WriteBinlogOfPop(args);
}

void LPopCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLPop);
    return;
  }
  key_ = argv_[1];
  size_t argc = argv_.size();
  size_t index = 2;
  if (index < argc) {
    if (pstd::string2int(argv_[index].data(), argv_[index].size(), &count_) == 0) {
      res_.SetRes(CmdRes::kWrongNum, kCmdNameLPop);
      return;
    }
    if (count_ < 0) {
      res_.SetRes(CmdRes::kSyntaxErr);
      return;
    }
  }
}

void LPopCmd::Do(std::shared_ptr<DB> db) {
  std::vector<std::string> elements;
  s_ = db->storage()->LPop(key_, count_, &elements);

  if (s_.ok()) {
    if (elements.size() > 1) {
      res_.AppendArrayLenUint64(elements.size());
    }
    for (const auto& element : elements) {
      res_.AppendString(element);
    }
  } else if (s_.IsNotFound()) {
    res_.AppendStringLen(-1);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void LPopCmd::DoThroughDB(std::shared_ptr<DB> db) {
   Do(db);
}

void LPopCmd::DoUpdateCache(std::shared_ptr<DB> db) {
  if (s_.ok()) {
    std::string CachePrefixKeyL = PCacheKeyPrefixL + key_;
    std::string value;
    db->cache()->LPop(CachePrefixKeyL, &value);
  }
}

void LPushxCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLPushx);
    return;
  }
  key_ = argv_[1];
  size_t pos = 2;
  while (pos < argv_.size()) {
    values_.push_back(argv_[pos++]);
  }
}

void LPushxCmd::Do(std::shared_ptr<DB> db) {
  uint64_t llen = 0;
  s_ = db->storage()->LPushx(key_, values_, &llen);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendInteger(static_cast<int64_t>(llen));
    AddSlotKey("l", key_, db);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void LPushxCmd::DoThroughDB(std::shared_ptr<DB> db) {
   Do(db);
}

void LPushxCmd::DoUpdateCache(std::shared_ptr<DB> db) {
  if (s_.ok()) {
    std::string CachePrefixKeyL = PCacheKeyPrefixL + key_;
    std::vector<std::string> values;
    values.push_back(value_);
    db->cache()->LPushx(CachePrefixKeyL, values);
  }
}

void LRangeCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLRange);
    return;
  }
  key_ = argv_[1];
  std::string left = argv_[2];
  if (pstd::string2int(left.data(), left.size(), &left_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  std::string right = argv_[3];
  if (pstd::string2int(right.data(), right.size(), &right_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
  }
}

void LRangeCmd::Do(std::shared_ptr<DB> db) {
  std::vector<std::string> values;
  s_ = db->storage()->LRange(key_, left_, right_, &values);
  if (s_.ok()) {
    res_.AppendArrayLenUint64(values.size());
    for (const auto& value : values) {
      res_.AppendString(value);
    }
  } else if (s_.IsNotFound()) {
    res_.AppendArrayLen(0);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void LRangeCmd::ReadCache(std::shared_ptr<DB> db) {
  std::vector<std::string> values;
  std::string CachePrefixKeyL = PCacheKeyPrefixL + key_;
  auto s = db->cache()->LRange(CachePrefixKeyL, left_, right_, &values);
  if (s.ok()) {
    res_.AppendArrayLen(values.size());
    for (const auto& value : values) {
      res_.AppendString(value);
    }
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void LRangeCmd::DoThroughDB(std::shared_ptr<DB> db) {
  res_.clear();
   Do(db);
}

void LRangeCmd::DoUpdateCache(std::shared_ptr<DB> db) {
  if (s_.ok()) {
    db->cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_LIST, key_, db);
  }
}

void LRemCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLRem);
    return;
  }
  key_ = argv_[1];
  std::string count = argv_[2];
  if (pstd::string2int(count.data(), count.size(), &count_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  value_ = argv_[3];
}

void LRemCmd::Do(std::shared_ptr<DB> db) {
  uint64_t res = 0;
  s_ = db->storage()->LRem(key_, count_, value_, &res);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendInteger(static_cast<int64_t>(res));
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void LRemCmd::DoThroughDB(std::shared_ptr<DB> db) {
   Do(db);
}

void LRemCmd::DoUpdateCache(std::shared_ptr<DB> db) {
  if (s_.ok()) {
    std::string CachePrefixKeyL = PCacheKeyPrefixL + key_;
    db->cache()->LRem(CachePrefixKeyL, count_, value_);
  }
}

void LSetCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLSet);
    return;
  }
  key_ = argv_[1];
  std::string index = argv_[2];
  if (pstd::string2int(index.data(), index.size(), &index_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  value_ = argv_[3];
}

void LSetCmd::Do(std::shared_ptr<DB> db) {
  s_ = db->storage()->LSet(key_, index_, value_);
  if (s_.ok()) {
    res_.SetRes(CmdRes::kOk);
    AddSlotKey("l", key_, db);
  } else if (s_.IsNotFound()) {
    res_.SetRes(CmdRes::kNotFound);
  } else if (s_.IsCorruption() && s_.ToString() == "Corruption: index out of range") {
    // TODO(): refine return value
    res_.SetRes(CmdRes::kOutOfRange);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void LSetCmd::DoThroughDB(std::shared_ptr<DB> db) {
   Do(db);
}

void LSetCmd::DoUpdateCache(std::shared_ptr<DB> db) {
  if (s_.ok()) {
    std::string CachePrefixKeyL = PCacheKeyPrefixL + key_;
    db->cache()->LSet(CachePrefixKeyL, index_, value_);
  }
}

void LTrimCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLSet);
    return;
  }
  key_ = argv_[1];
  std::string start = argv_[2];
  if (pstd::string2int(start.data(), start.size(), &start_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  std::string stop = argv_[3];
  if (pstd::string2int(stop.data(), stop.size(), &stop_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
  }
}

void LTrimCmd::Do(std::shared_ptr<DB> db) {
  s_ = db->storage()->LTrim(key_, start_, stop_);
  if (s_.ok() || s_.IsNotFound()) {
    res_.SetRes(CmdRes::kOk);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void LTrimCmd::DoThroughDB(std::shared_ptr<DB> db) {
   Do(db);
}

void LTrimCmd::DoUpdateCache(std::shared_ptr<DB> db) {
  if (s_.ok()) {
    std::string CachePrefixKeyL = PCacheKeyPrefixL + key_;
    db->cache()->LTrim(CachePrefixKeyL, start_, stop_);
  }
}

void BRPopCmd::Do(std::shared_ptr<DB> db) {
  for (auto& this_key : keys_) {
    std::vector<std::string> values;
    s_ = db->storage()->RPop(this_key, 1, &values);
    if (s_.ok()) {
      res_.AppendArrayLen(2);
      res_.AppendString(this_key);
      res_.AppendString(values[0]);
      // write an binlog of rpop
      binlog_args_.block_type = BlockKeyType::Brpop;
      binlog_args_.key = this_key;
      binlog_args_.db = db;
      binlog_args_.conn = GetConn();
      is_binlog_deferred_ = false;
      return;
    } else if (s_.IsNotFound()) {
      continue;
    } else {
      res_.SetRes(CmdRes::kErrOther, s_.ToString());
      return;
    }
  }
  is_binlog_deferred_ = true;
  if (auto client_conn = std::dynamic_pointer_cast<PikaClientConn>(GetConn()); client_conn != nullptr) {
    if (client_conn->IsInTxn()) {
      res_.AppendArrayLen(-1);
      return ;
    }
  }
  BlockThisClientToWaitLRPush(BlockKeyType::Brpop, keys_, expire_time_);
}

void BRPopCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameBLPop);
    return;
  }

  // fetching all keys(*argv_.begin is the command itself and *argv_.end() is the timeout value)
  keys_.assign(++argv_.begin(), --argv_.end());
  removeDuplicates(keys_);
  int64_t timeout = 0;
  if (!pstd::string2int(argv_.back().data(), argv_.back().size(), &timeout)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  constexpr int64_t seconds_of_ten_years = 10 * 365 * 24 * 3600;
  if (timeout < 0 || timeout > seconds_of_ten_years) {
    res_.SetRes(CmdRes::kErrOther,
                "timeout can't be a negative value and can't exceed the number of seconds in 10 years");
    return;
  }

  if (timeout > 0) {
    auto now = std::chrono::system_clock::now();
    expire_time_ =
        std::chrono::time_point_cast<std::chrono::milliseconds>(now).time_since_epoch().count() + timeout * 1000;
  }  // else(timeout is 0): expire_time_ default value is 0, means never expire;
}

void BRPopCmd::DoBinlog(const std::shared_ptr<SyncMasterDB>& db) {
  if (is_binlog_deferred_) {
    return;
  }
  std::vector<WriteBinlogOfPopArgs> args;
  args.push_back(std::move(binlog_args_));
  WriteBinlogOfPop(args);
}



void RPopCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameRPop);
    return;
  }
  key_ = argv_[1];
  size_t index = 2;
  if (index < argv_.size()) {
    if (pstd::string2int(argv_[index].data(), argv_[index].size(), &count_) == 0) {
      res_.SetRes(CmdRes::kWrongNum, kCmdNameRPop);
      return;
    }
    if (count_ < 0) {
      res_.SetRes(CmdRes::kSyntaxErr);
      return;
    }
  }
}

void RPopCmd::Do(std::shared_ptr<DB> db) {
  std::vector <std::string> elements;
  s_ = db->storage()->RPop(key_, count_, &elements);
  if (s_.ok()) {
    if (elements.size() > 1) {
      res_.AppendArrayLenUint64(elements.size());
    }
    for (const auto &element: elements) {
      res_.AppendString(element);
    }
  } else if (s_.IsNotFound()) {
    res_.AppendStringLen(-1);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void RPopCmd::DoThroughDB(std::shared_ptr<DB> db) {
   Do(db);
}

void RPopCmd::DoUpdateCache(std::shared_ptr<DB> db) {
  if (s_.ok()) {
    std::string CachePrefixKeyL = PCacheKeyPrefixL + key_;
    std::string value;
    db->cache()->RPop(CachePrefixKeyL, &value);
  }
}

void RPopLPushCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameRPopLPush);
    return;
  }
  source_ = argv_[1];
  receiver_ = argv_[2];
  if (!HashtagIsConsistent(source_, receiver_)) {
    res_.SetRes(CmdRes::kInconsistentHashTag);
  }
}

void RPopLPushCmd::Do(std::shared_ptr<DB> db) {
  std::string value;
  s_ = db->storage()->RPoplpush(source_, receiver_, &value);
  if (s_.ok()) {
    AddSlotKey("k", receiver_, db);
    res_.AppendString(value);
    value_poped_from_source_ = value;
    is_write_binlog_ = true;
  } else if (s_.IsNotFound()) {
    // no actual write operation happened, will not write binlog
    res_.AppendStringLen(-1);
    is_write_binlog_ = false;
    return;
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
    return;
  }
  TryToServeBLrPopWithThisKey(receiver_, db);
}

void RPopLPushCmd::ReadCache(std::shared_ptr<DB> db) {
  res_.SetRes(CmdRes::kErrOther, "the command is not support in cache mode");
}

void RPopLPushCmd::DoBinlog(const std::shared_ptr<SyncMasterDB>& db) {
  if (!is_write_binlog_) {
    return;
  }
  PikaCmdArgsType rpop_args;
  rpop_args.push_back("RPOP");
  rpop_args.push_back(source_);
  rpop_cmd_->Initial(rpop_args, db_name_);

  PikaCmdArgsType lpush_args;
  lpush_args.push_back("LPUSH");
  lpush_args.push_back(receiver_);
  lpush_args.push_back(value_poped_from_source_);
  lpush_cmd_->Initial(lpush_args, db_name_);

  rpop_cmd_->SetConn(GetConn());
  rpop_cmd_->SetResp(resp_.lock());
  lpush_cmd_->SetConn(GetConn());
  lpush_cmd_->SetResp(resp_.lock());

  rpop_cmd_->DoBinlog(db);
  lpush_cmd_->DoBinlog(db);
}

void RPushCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameRPush);
    return;
  }
  key_ = argv_[1];
  size_t pos = 2;
  while (pos < argv_.size()) {
    values_.push_back(argv_[pos++]);
  }
}

void RPushCmd::Do(std::shared_ptr<DB> db) {
  uint64_t llen = 0;
  s_ = db->storage()->RPush(key_, values_, &llen);
  if (s_.ok()) {
    res_.AppendInteger(static_cast<int64_t>(llen));
    AddSlotKey("l", key_, db);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
  if (auto client_conn = std::dynamic_pointer_cast<PikaClientConn>(GetConn()); client_conn != nullptr) {
    if (client_conn->IsInTxn()) {
      return;
    }
  }
  TryToServeBLrPopWithThisKey(key_, db);
}

void RPushCmd::DoThroughDB(std::shared_ptr<DB> db) {
   Do(db);
}

void RPushCmd::DoUpdateCache(std::shared_ptr<DB> db) {
  if (s_.ok()) {
    std::string CachePrefixKeyL = PCacheKeyPrefixL + key_;
    db->cache()->RPushx(CachePrefixKeyL, values_);
  }
}

void RPushxCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameRPushx);
    return;
  }
  key_ = argv_[1];
  size_t pos = 2;
  while (pos < argv_.size()) {
    values_.push_back(argv_[pos++]);
  }
}

void RPushxCmd::Do(std::shared_ptr<DB> db) {
  uint64_t llen = 0;
  s_ = db->storage()->RPushx(key_, values_, &llen);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendInteger(static_cast<int64_t>(llen));
    AddSlotKey("l", key_, db);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void RPushxCmd::DoThroughDB(std::shared_ptr<DB> db) {
   Do(db);
}

void RPushxCmd::DoUpdateCache(std::shared_ptr<DB> db) {
  if (s_.ok()) {
    std::string CachePrefixKeyL = PCacheKeyPrefixL + key_;
    std::vector<std::string> values;
    values.push_back(value_);
    db->cache()->RPushx(CachePrefixKeyL, values);
  }
}