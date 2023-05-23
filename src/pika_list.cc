// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <utility>
#include "include/pika_data_distribution.h"
#include "include/pika_list.h"
#include "include/pika_server.h"
#include "pstd/include/pstd_string.h"
extern PikaServer* g_pika_server;
void LIndexCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLIndex);
    return;
  }
  key_ = argv_[1];
  std::string index = argv_[2];
  if (!pstd::string2int(index.data(), index.size(), &index_)) {
    res_.SetRes(CmdRes::kInvalidInt);
  }
  return;
}
void LIndexCmd::Do(std::shared_ptr<Partition> partition) {
  std::string value;
  rocksdb::Status s = partition->db()->LIndex(key_, index_, &value);
  if (s.ok()) {
    res_.AppendString(value);
  } else if (s.IsNotFound()) {
    res_.AppendStringLen(-1);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void LInsertCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLInsert);
    return;
  }
  key_ = argv_[1];
  std::string dir = argv_[2];
  if (!strcasecmp(dir.data(), "before")) {
    dir_ = storage::Before;
  } else if (!strcasecmp(dir.data(), "after")) {
    dir_ = storage::After;
  } else {
    res_.SetRes(CmdRes::kSyntaxErr);
    return;
  }
  pivot_ = argv_[3];
  value_ = argv_[4];
}
void LInsertCmd::Do(std::shared_ptr<Partition> partition) {
  int64_t llen = 0;
  rocksdb::Status s = partition->db()->LInsert(key_, dir_, pivot_, value_, &llen);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(llen);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void LLenCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLLen);
    return;
  }
  key_ = argv_[1];
}
void LLenCmd::Do(std::shared_ptr<Partition> partition) {
  uint64_t llen = 0;
  rocksdb::Status s = partition->db()->LLen(key_, &llen);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(llen);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

typedef struct {
  std::string key;
  std::string curr_db;
  std::shared_ptr<Partition> partition;
  net::DispatchThread* dispatchThread;
} BlrPopUnblockTaskArgs;

void BPopServeCmd::TryToServeBLrPopWithThisKey(const std::string& key, std::shared_ptr<Partition> partition) {
  std::shared_ptr<net::RedisConn> curr_conn = std::dynamic_pointer_cast<net::RedisConn>(GetConn());
  auto dispatchThread = dynamic_cast<net::DispatchThread*>(curr_conn->thread());

  {
    std::shared_lock read_latch(dispatchThread->GetBLRPopBlockingMapLatch());
    auto& map_from_keys_to_conns_for_blrpop = dispatchThread->GetMapFromKeysToConnsForBlrpop();
    net::BlrPopKey blrPop_key{curr_conn->GetCurrentTable(), key};
    auto it = map_from_keys_to_conns_for_blrpop.find(blrPop_key);
    if (it == map_from_keys_to_conns_for_blrpop.end()) {
      // no client is waitting for this key
      return;
    }
  }

  auto* args = new BlrPopUnblockTaskArgs();
  args->key = key;
  args->dispatchThread = dispatchThread;
  args->partition = std::move(partition);
  args->curr_db = curr_conn->GetCurrentTable();
  g_pika_server->ScheduleClientPool(&ServeAndUnblockConns, args);
}
void BPopServeCmd::ServeAndUnblockConns(void* args) {
  auto bg_args = std::unique_ptr<BlrPopUnblockTaskArgs>(static_cast<BlrPopUnblockTaskArgs*>(args));
  net::DispatchThread* dispatchThread = bg_args->dispatchThread;
  std::shared_ptr<Partition> partition = std::move(bg_args->partition);
  std::string key = std::move(bg_args->key);
  auto& map_from_keys_to_conns_for_blrpop = dispatchThread->GetMapFromKeysToConnsForBlrpop();
  net::BlrPopKey blrPop_key{std::move(bg_args->curr_db), key};

  std::lock_guard latch(dispatchThread->GetBLRPopBlockingMapLatch());
  auto it = map_from_keys_to_conns_for_blrpop.find(blrPop_key);
  if (it == map_from_keys_to_conns_for_blrpop.end()) {
    return;
  }
  auto& waitting_list_of_this_key = it->second;
  std::string value;
  rocksdb::Status s;
  // traverse this list from head to tail(in the order of adding sequence) ,means "first blocked, first get served“
  CmdRes res;
  for (auto conn_blocked = waitting_list_of_this_key->begin(); conn_blocked != waitting_list_of_this_key->end();) {
    if (conn_blocked->GetBlockType() == net::BlockPopType::Blpop) {
      s = partition->db()->LPop(key, &value);
    } else {  // BlockPopType is Brpop
      s = partition->db()->RPop(key, &value);
    }

    if (s.ok()) {
      res.AppendArrayLen(2);
      res.AppendString(key);
      res.AppendString(value);
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
    conn_blocked = waitting_list_of_this_key->erase(conn_blocked);  // remove this conn from current waiting list
    // erase all waiting info of this conn
    dispatchThread->CleanWaitNodeOfUnBlockedBlrConn(conn_ptr);
  }
  dispatchThread->CleanKeysAfterWaitNodeCleaned();
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
void LPushCmd::Do(std::shared_ptr<Partition> partition) {
  uint64_t llen = 0;
  rocksdb::Status s = partition->db()->LPush(key_, values_, &llen);
  if (s.ok()) {
    res_.AppendInteger(llen);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  TryToServeBLrPopWithThisKey(key_, partition);
}

void BLRPopBaseCmd::BlockThisClientToWaitLRPush(net::BlockPopType block_pop_type) {
  std::shared_ptr<net::RedisConn> conn_to_block = std::dynamic_pointer_cast<net::RedisConn>(GetConn());

  auto dispatchThread = dynamic_cast<net::DispatchThread*>(conn_to_block->thread());
  std::lock_guard latch(dispatchThread->GetBLRPopBlockingMapLatch());
  auto& map_from_keys_to_conns_for_blrpop = dispatchThread->GetMapFromKeysToConnsForBlrpop();
  auto& map_from_conns_to_keys_for_blrpop = dispatchThread->GetMapFromConnsToKeysForBlrpop();

  std::vector<net::BlrPopKey> blrpop_keys;
  for (auto& key : keys_) {
    net::BlrPopKey blrpop_key{conn_to_block->GetCurrentTable(), key};
    blrpop_keys.push_back(blrpop_key);
    auto it = map_from_keys_to_conns_for_blrpop.find(blrpop_key);
    if (it == map_from_keys_to_conns_for_blrpop.end()) {
      // no waiting info found, means no other clients are waiting for the list related with this key right now
      map_from_keys_to_conns_for_blrpop.emplace(blrpop_key, std::make_unique<std::list<net::BlockedPopConnNode>>());
      it = map_from_keys_to_conns_for_blrpop.find(blrpop_key);
    }
    auto& wait_list_of_this_key = it->second;
    // add current client-connection to the tail of waiting list of this key
    wait_list_of_this_key->emplace_back(expire_time_, conn_to_block, block_pop_type);
  }

  // construct a list of keys and insert into this map as value(while key of the map is conn_fd)
  map_from_conns_to_keys_for_blrpop.emplace(
      conn_to_block->fd(), std::make_unique<std::list<net::BlrPopKey>>(blrpop_keys.begin(), blrpop_keys.end()));

  std::cout << "-------------db name:" << conn_to_block->GetCurrentTable() << "-------------" << std::endl;
  std::cout << "from key to conn:" << std::endl;
  for (auto& pair : map_from_keys_to_conns_for_blrpop) {
    std::cout << "key:<" << pair.first.db_name << "," << pair.first.key << ">  list of it:" << std::endl;
    for (auto& it : *pair.second) {
      it.SelfPrint();
    }
  }

  std::cout << "\n\nfrom conn to key:" << std::endl;
  for (auto& pair : map_from_conns_to_keys_for_blrpop) {
    std::cout << "fd:" << pair.first << "  related keys:" << std::endl;
    for (auto& it : *pair.second) {
      std::cout << " <" << it.db_name << "," << it.key << "> " << std::endl;
    }
  }
  std::cout << "-----------end------------------" << std::endl;
}

void BLPopCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameBLPop);
    return;
  }

  // fetching all keys(*argv_.begin is the command itself and *argv_.end() is the timeout value)
  keys_.assign(++argv_.begin(), --argv_.end());
  int64_t timeout;
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

void BLPopCmd::Do(std::shared_ptr<Partition> partition) {
  for (auto& this_key : keys_) {
    std::string value;
    rocksdb::Status s = partition->db()->LPop(this_key, &value);
    if (s.ok()) {
      res_.AppendArrayLen(2);
      res_.AppendString(this_key);
      res_.AppendString(value);
      return;
    } else if (s.IsNotFound()) {
      continue;
    } else {
      res_.SetRes(CmdRes::kErrOther, s.ToString());
      return;
    }
  }
  /*
   * need To Do: If blpop is exec within a transaction and no elements can pop from keys_,
   * jsut return nil(-1), do not block the conn, maybe need to add a if here
   */
  BlockThisClientToWaitLRPush(net::BlockPopType::Blpop);
}

void LPopCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLPop);
    return;
  }
  key_ = argv_[1];
}
void LPopCmd::Do(std::shared_ptr<Partition> partition) {
  std::string value;
  rocksdb::Status s = partition->db()->LPop(key_, &value);
  if (s.ok()) {
    res_.AppendString(value);
  } else if (s.IsNotFound()) {
    res_.AppendStringLen(-1);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void LPushxCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLPushx);
    return;
  }
  key_ = argv_[1];
  value_ = argv_[2];
}
void LPushxCmd::Do(std::shared_ptr<Partition> partition) {
  uint64_t llen = 0;
  rocksdb::Status s = partition->db()->LPushx(key_, value_, &llen);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(llen);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void LRangeCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLRange);
    return;
  }
  key_ = argv_[1];
  std::string left = argv_[2];
  if (!pstd::string2int(left.data(), left.size(), &left_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  std::string right = argv_[3];
  if (!pstd::string2int(right.data(), right.size(), &right_)) {
    res_.SetRes(CmdRes::kInvalidInt);
  }
  return;
}
void LRangeCmd::Do(std::shared_ptr<Partition> partition) {
  std::vector<std::string> values;
  rocksdb::Status s = partition->db()->LRange(key_, left_, right_, &values);
  if (s.ok()) {
    res_.AppendArrayLen(values.size());
    for (const auto& value : values) {
      res_.AppendString(value);
    }
  } else if (s.IsNotFound()) {
    res_.AppendArrayLen(0);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void LRemCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLRem);
    return;
  }
  key_ = argv_[1];
  std::string count = argv_[2];
  if (!pstd::string2int(count.data(), count.size(), &count_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  value_ = argv_[3];
}
void LRemCmd::Do(std::shared_ptr<Partition> partition) {
  uint64_t res = 0;
  rocksdb::Status s = partition->db()->LRem(key_, count_, value_, &res);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(res);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void LSetCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLSet);
    return;
  }
  key_ = argv_[1];
  std::string index = argv_[2];
  if (!pstd::string2int(index.data(), index.size(), &index_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  value_ = argv_[3];
}
void LSetCmd::Do(std::shared_ptr<Partition> partition) {
  rocksdb::Status s = partition->db()->LSet(key_, index_, value_);
  if (s.ok()) {
    res_.SetRes(CmdRes::kOk);
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kNotFound);
  } else if (s.IsCorruption() && s.ToString() == "Corruption: index out of range") {
    // TODO refine return value
    res_.SetRes(CmdRes::kOutOfRange);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void LTrimCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLSet);
    return;
  }
  key_ = argv_[1];
  std::string start = argv_[2];
  if (!pstd::string2int(start.data(), start.size(), &start_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  std::string stop = argv_[3];
  if (!pstd::string2int(stop.data(), stop.size(), &stop_)) {
    res_.SetRes(CmdRes::kInvalidInt);
  }
  return;
}
void LTrimCmd::Do(std::shared_ptr<Partition> partition) {
  rocksdb::Status s = partition->db()->LTrim(key_, start_, stop_);
  if (s.ok() || s.IsNotFound()) {
    res_.SetRes(CmdRes::kOk);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void BRPopCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameBLPop);
    return;
  }

  // fetching all keys(*argv_.begin is the command itself and *argv_.end() is the timeout value)
  keys_.assign(++argv_.begin(), --argv_.end());
  int64_t timeout;
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

void BRPopCmd::Do(std::shared_ptr<Partition> partition) {
  for (auto& this_key : keys_) {
    std::string value;
    rocksdb::Status s = partition->db()->RPop(this_key, &value);
    if (s.ok()) {
      res_.AppendArrayLen(2);
      res_.AppendString(this_key);
      res_.AppendString(value);
      return;
    } else if (s.IsNotFound()) {
      continue;
    } else {
      res_.SetRes(CmdRes::kErrOther, s.ToString());
      return;
    }
  }
  /*
   * nedd To Do: If blpop is exec within a transaction and no elements can pop from keys_,
   * jsut return nil(-1), do not block the conn, maybe need to add a if here
   */
  BlockThisClientToWaitLRPush(net::BlockPopType::Brpop);
}

void RPopCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameRPop);
    return;
  }
  key_ = argv_[1];
}
void RPopCmd::Do(std::shared_ptr<Partition> partition) {
  std::string value;
  rocksdb::Status s = partition->db()->RPop(key_, &value);
  if (s.ok()) {
    res_.AppendString(value);
  } else if (s.IsNotFound()) {
    res_.AppendStringLen(-1);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
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
void RPopLPushCmd::Do(std::shared_ptr<Partition> partition) {
  std::string value;
  rocksdb::Status s = partition->db()->RPoplpush(source_, receiver_, &value);
  if (s.ok()) {
    res_.AppendString(value);
  } else if (s.IsNotFound()) {
    res_.AppendStringLen(-1);
    return;
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }
  TryToServeBLrPopWithThisKey(receiver_, partition);
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
void RPushCmd::Do(std::shared_ptr<Partition> partition) {
  uint64_t llen = 0;
  rocksdb::Status s = partition->db()->RPush(key_, values_, &llen);
  if (s.ok()) {
    res_.AppendInteger(llen);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  TryToServeBLrPopWithThisKey(key_, partition);
}

void RPushxCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameRPushx);
    return;
  }
  key_ = argv_[1];
  value_ = argv_[2];
}
void RPushxCmd::Do(std::shared_ptr<Partition> partition) {
  uint64_t llen = 0;
  rocksdb::Status s = partition->db()->RPushx(key_, value_, &llen);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(llen);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}
