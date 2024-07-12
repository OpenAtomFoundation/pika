// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_kv.h"
#include <memory>

#include "include/pika_command.h"
#include "include/pika_slot_command.h"
#include "include/pika_cache.h"
#include "include/pika_conf.h"
#include "pstd/include/pstd_string.h"

extern std::unique_ptr<PikaConf> g_pika_conf;
/* SET key value [NX] [XX] [EX <seconds>] [PX <milliseconds>] */
void SetCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSet);
    return;
  }
  key_ = argv_[1];
  value_ = argv_[2];
  condition_ = SetCmd::kNONE;
  sec_ = 0;
  size_t index = 3;
  while (index != argv_.size()) {
    std::string opt = argv_[index];
    if (strcasecmp(opt.data(), "xx") == 0) {
      condition_ = SetCmd::kXX;
    } else if (strcasecmp(opt.data(), "nx") == 0) {
      condition_ = SetCmd::kNX;
    } else if (strcasecmp(opt.data(), "vx") == 0) {
      condition_ = SetCmd::kVX;
      index++;
      if (index == argv_.size()) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      } else {
        target_ = argv_[index];
      }
    } else if ((strcasecmp(opt.data(), "ex") == 0) || (strcasecmp(opt.data(), "px") == 0)) {
      condition_ = (condition_ == SetCmd::kNONE) ? SetCmd::kEXORPX : condition_;
      index++;
      if (index == argv_.size()) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      if (pstd::string2int(argv_[index].data(), argv_[index].size(), &sec_) == 0) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
      }

      if (strcasecmp(opt.data(), "px") == 0) {
        sec_ /= 1000;
      }
      has_ttl_ = true;
    } else {
      res_.SetRes(CmdRes::kSyntaxErr);
      return;
    }
    index++;
  }
}

void SetCmd::Do() {
  int32_t res = 1;
  switch (condition_) {
    case SetCmd::kXX:
      s_ = db_->storage()->Setxx(key_, value_, &res, sec_);
      break;
    case SetCmd::kNX:
      s_ = db_->storage()->Setnx(key_, value_, &res, sec_);
      break;
    case SetCmd::kVX:
      s_ = db_->storage()->Setvx(key_, target_, value_, &success_, sec_);
      break;
    case SetCmd::kEXORPX:
      s_ = db_->storage()->Setex(key_, value_, sec_);
      break;
    default:
      s_ = db_->storage()->Set(key_, value_);
      break;
  }

  if (s_.ok() || s_.IsNotFound()) {
    if (condition_ == SetCmd::kVX) {
      res_.AppendInteger(success_);
    } else {
      if (res == 1) {
        res_.SetRes(CmdRes::kOk);
        AddSlotKey("k", key_, db_);
      } else {
        res_.AppendStringLen(-1);
      }
    }
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void SetCmd::DoThroughDB() {
  Do();
}

void SetCmd::DoUpdateCache() {
  if (SetCmd::kNX == condition_) {
    return;
  }
  if (s_.ok()) {
    if (has_ttl_) {
      db_->cache()->Setxx(key_, value_, sec_);
    } else {
      db_->cache()->SetxxWithoutTTL(key_, value_);
    }
  }
}

std::string SetCmd::ToRedisProtocol() {
  if (condition_ == SetCmd::kEXORPX) {
    std::string content;
    content.reserve(RAW_ARGS_LEN);
    RedisAppendLen(content, 4, "*");

    // to pksetexat cmd
    std::string pksetexat_cmd("pksetexat");
    RedisAppendLenUint64(content, pksetexat_cmd.size(), "$");
    RedisAppendContent(content, pksetexat_cmd);
    // key
    RedisAppendLenUint64(content, key_.size(), "$");
    RedisAppendContent(content, key_);
    // time_stamp
    char buf[100];
    auto time_stamp = time(nullptr) + sec_;
    pstd::ll2string(buf, 100, time_stamp);
    std::string at(buf);
    RedisAppendLenUint64(content, at.size(), "$");
    RedisAppendContent(content, at);
    // value
    RedisAppendLenUint64(content, value_.size(), "$");
    RedisAppendContent(content, value_);
    return content;
  } else {
    return Cmd::ToRedisProtocol();
  }
}

void GetCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameGet);
    return;
  }
  key_ = argv_[1];
}

void GetCmd::Do() {
  s_ = db_->storage()->GetWithTTL(key_, &value_, &sec_);
  if (s_.ok()) {
    res_.AppendStringLenUint64(value_.size());
    res_.AppendContent(value_);
  } else if (s_.IsNotFound()) {
    res_.AppendStringLen(-1);
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void GetCmd::ReadCache() {
  auto s = db_->cache()->Get(key_, &value_);
  if (s.ok()) {
    res_.AppendStringLen(value_.size());
    res_.AppendContent(value_);
  } else {
    res_.SetRes(CmdRes::kCacheMiss);
  }
}

void GetCmd::DoThroughDB() {
  res_.clear();
  Do();
}

void GetCmd::DoUpdateCache() {
  if (s_.ok()) {
    db_->cache()->WriteKVToCache(key_, value_, sec_);
  }
}

void DelCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, name());
    return;
  }
  auto iter = argv_.begin();
  keys_.assign(++iter, argv_.end());
}

void DelCmd::Do() {
  int64_t count = db_->storage()->Del(keys_);
  if (count >= 0) {
    res_.AppendInteger(count);
    s_ = rocksdb::Status::OK();
    std::vector<std::string>::const_iterator it;
    for (it = keys_.begin(); it != keys_.end(); it++) {
      RemSlotKey(*it, db_);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, "delete error");
    s_ = rocksdb::Status::Corruption("delete error");
  }
}

void DelCmd::DoThroughDB() {
  Do();
}

void DelCmd::DoUpdateCache() {
  if (s_.ok()) {
    db_->cache()->Del(keys_);
  }
}

void DelCmd::Split(const HintKeys& hint_keys) {
  std::map<storage::DataType, storage::Status> type_status;
  int64_t count = db_->storage()->Del(hint_keys.keys);
  if (count >= 0) {
    split_res_ += count;
  } else {
    res_.SetRes(CmdRes::kErrOther, "delete error");
  }
}

void DelCmd::Merge() { res_.AppendInteger(split_res_); }

void DelCmd::DoBinlog() {
  std::string opt = argv_.at(0);
  for(auto& key: keys_) {
    argv_.clear();
    argv_.emplace_back(opt);
    argv_.emplace_back(key);
    Cmd::DoBinlog();
  }
}

void IncrCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameIncr);
    return;
  }
  key_ = argv_[1];
}

void IncrCmd::Do() {
  s_ = db_->storage()->Incrby(key_, 1, &new_value_);
  if (s_.ok()) {
    res_.AppendContent(":" + std::to_string(new_value_));
    AddSlotKey("k", key_, db_);
  } else if (s_.IsCorruption() && s_.ToString() == "Corruption: Value is not a integer") {
    res_.SetRes(CmdRes::kInvalidInt);
  } else if (s_.IsInvalidArgument() && s_.ToString().substr(0, std::char_traits<char>::length(ErrTypeMessage)) == ErrTypeMessage) {
    res_.SetRes(CmdRes::kMultiKey);
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kOverFlow);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void IncrCmd::DoThroughDB() {
  Do();
}

void IncrCmd::DoUpdateCache() {
  if (s_.ok()) {
    db_->cache()->Incrxx(key_);
  }
}

void IncrbyCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameIncrby);
    return;
  }
  key_ = argv_[1];
  if (pstd::string2int(argv_[2].data(), argv_[2].size(), &by_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt, kCmdNameIncrby);
    return;
  }
}

void IncrbyCmd::Do() {
  s_ = db_->storage()->Incrby(key_, by_, &new_value_);
  if (s_.ok()) {
    res_.AppendContent(":" + std::to_string(new_value_));
    AddSlotKey("k", key_, db_);
  } else if (s_.IsCorruption() && s_.ToString() == "Corruption: Value is not a integer") {
    res_.SetRes(CmdRes::kInvalidInt);
  } else if (s_.IsInvalidArgument() && s_.ToString().substr(0, std::char_traits<char>::length(ErrTypeMessage)) == ErrTypeMessage) {
    res_.SetRes(CmdRes::kMultiKey);
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kOverFlow);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void IncrbyCmd::DoThroughDB() {
  Do();
}

void IncrbyCmd::DoUpdateCache() {
  if (s_.ok()) {
    db_->cache()->IncrByxx(key_, by_);
  }
}

void IncrbyfloatCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameIncrbyfloat);
    return;
  }
  key_ = argv_[1];
  value_ = argv_[2];
  if (pstd::string2d(argv_[2].data(), argv_[2].size(), &by_) == 0) {
    res_.SetRes(CmdRes::kInvalidFloat);
    return;
  }
}

void IncrbyfloatCmd::Do() {
  s_ = db_->storage()->Incrbyfloat(key_, value_, &new_value_);
  if (s_.ok()) {
    res_.AppendStringLenUint64(new_value_.size());
    res_.AppendContent(new_value_);
    AddSlotKey("k", key_, db_);
  } else if (s_.IsCorruption() && s_.ToString() == "Corruption: Value is not a vaild float") {
    res_.SetRes(CmdRes::kInvalidFloat);
  } else if (s_.IsInvalidArgument() && s_.ToString().substr(0, std::char_traits<char>::length(ErrTypeMessage)) == ErrTypeMessage) {
    res_.SetRes(CmdRes::kMultiKey);
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::KIncrByOverFlow);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void IncrbyfloatCmd::DoThroughDB() {
  Do();
}

void IncrbyfloatCmd::DoUpdateCache() {
  if (s_.ok()) {
    long double long_double_by;
    if (storage::StrToLongDouble(value_.data(), value_.size(), &long_double_by) != -1) {
      db_->cache()->Incrbyfloatxx(key_, long_double_by);
    }
  }
}

void DecrCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameDecr);
    return;
  }
  key_ = argv_[1];
}

void DecrCmd::Do() {
  s_= db_->storage()->Decrby(key_, 1, &new_value_);
  if (s_.ok()) {
    res_.AppendContent(":" + std::to_string(new_value_));
  } else if (s_.IsCorruption() && s_.ToString() == "Corruption: Value is not a integer") {
    res_.SetRes(CmdRes::kInvalidInt);
  } else if (s_.IsInvalidArgument() && s_.ToString().substr(0, std::char_traits<char>::length(ErrTypeMessage)) == ErrTypeMessage) {
    res_.SetRes(CmdRes::kMultiKey);
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kOverFlow);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void DecrCmd::DoThroughDB() {
  Do();
}

void DecrCmd::DoUpdateCache() {
  if (s_.ok()) {
    db_->cache()->Decrxx(key_);
  }
}

void DecrbyCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameDecrby);
    return;
  }
  key_ = argv_[1];
  if (pstd::string2int(argv_[2].data(), argv_[2].size(), &by_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
}

void DecrbyCmd::Do() {
  s_ = db_->storage()->Decrby(key_, by_, &new_value_);
  if (s_.ok()) {
    AddSlotKey("k", key_, db_);
    res_.AppendContent(":" + std::to_string(new_value_));
  } else if (s_.IsCorruption() && s_.ToString() == "Corruption: Value is not a integer") {
    res_.SetRes(CmdRes::kInvalidInt);
  } else if (s_.IsInvalidArgument() && s_.ToString().substr(0, std::char_traits<char>::length(ErrTypeMessage)) == ErrTypeMessage) {
    res_.SetRes(CmdRes::kMultiKey);
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kOverFlow);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void DecrbyCmd::DoThroughDB() {
  Do();
}

void DecrbyCmd::DoUpdateCache() {
  if (s_.ok()) {
    db_->cache()->DecrByxx(key_, by_);
  }
}

void GetsetCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameGetset);
    return;
  }
  key_ = argv_[1];
  new_value_ = argv_[2];
}

void GetsetCmd::Do() {
  std::string old_value;
  s_ = db_->storage()->GetSet(key_, new_value_, &old_value);
  if (s_.ok()) {
    if (old_value.empty()) {
      res_.AppendContent("$-1");
    } else {
      res_.AppendStringLenUint64(old_value.size());
      res_.AppendContent(old_value);
    }
    AddSlotKey("k", key_, db_);
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void GetsetCmd::DoThroughDB() {
  Do();
}

void GetsetCmd::DoUpdateCache() {
  if (s_.ok()) {
    db_->cache()->SetxxWithoutTTL(key_, new_value_);
  }
}

void AppendCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameAppend);
    return;
  }
  key_ = argv_[1];
  value_ = argv_[2];
}

void AppendCmd::Do() {
  int32_t new_len = 0;
  s_ = db_->storage()->Append(key_, value_, &new_len);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendInteger(new_len);
    AddSlotKey("k", key_, db_);
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void AppendCmd::DoThroughDB(){
  Do();
}

void AppendCmd::DoUpdateCache() {
  if (s_.ok()) {
    db_->cache()->Appendxx(key_, value_);
  }
}

void MgetCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameMget);
    return;
  }
  keys_ = argv_;
  keys_.erase(keys_.begin());
  split_res_.resize(keys_.size());
  cache_miss_keys_.clear();
}

void MgetCmd::AssembleResponseFromCache() {
  res_.AppendArrayLenUint64(keys_.size());
  for (const auto& key : keys_) {
    auto it = cache_hit_values_.find(key);
    if (it != cache_hit_values_.end()) {
      res_.AppendStringLen(it->second.size());
      res_.AppendContent(it->second);
    } else {
      res_.SetRes(CmdRes::kErrOther, "Internal error during cache assembly");
      return;
    }
  }
}

void MgetCmd::Do() {
  // Without using the cache and querying only the DB, we need to use keys_.
  // This line will only be assigned when querying the DB directly.
  if(cache_miss_keys_.size() == 0) {
    cache_miss_keys_ = keys_;
  }
  db_value_status_array_.clear();
  s_ = db_->storage()->MGetWithTTL(cache_miss_keys_, &db_value_status_array_);
  if (!s_.ok()) {
    if (s_.IsInvalidArgument()) {
      res_.SetRes(CmdRes::kMultiKey);
    } else {
      res_.SetRes(CmdRes::kErrOther, s_.ToString());
    }
    return;
  }

  MergeCachedAndDbResults();
}

void MgetCmd::Split(const HintKeys& hint_keys) {
  std::vector<storage::ValueStatus> vss;
  const std::vector<std::string>& keys = hint_keys.keys;
  rocksdb::Status s = db_->storage()->MGet(keys, &vss);
  if (s.ok()) {
    if (hint_keys.hints.size() != vss.size()) {
      res_.SetRes(CmdRes::kErrOther, "internal Mget return size invalid");
    }
    const std::vector<int>& hints = hint_keys.hints;
    for (size_t i = 0; i < vss.size(); ++i) {
      split_res_[hints[i]] = vss[i];
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void MgetCmd::Merge() {
  res_.AppendArrayLenUint64(split_res_.size());
  for (const auto& vs : split_res_) {
    if (vs.status.ok()) {
      res_.AppendStringLenUint64(vs.value.size());
      res_.AppendContent(vs.value);
    } else {
      res_.AppendContent("$-1");
    }
  }
}

void MgetCmd::DoThroughDB() {
  res_.clear();
  Do();
}

void MgetCmd::ReadCache() {
  for (const auto key : keys_) {
    std::string value;
    auto s = db_->cache()->Get(const_cast<std::string&>(key), &value);
    if (s.ok()) {
      cache_hit_values_[key] = value;
    } else {
      cache_miss_keys_.push_back(key);
    }
  }
  if (cache_miss_keys_.empty()) {
    AssembleResponseFromCache();
  } else {
    res_.SetRes(CmdRes::kCacheMiss);
  }
}

void MgetCmd::DoUpdateCache() {
  size_t db_index = 0;
  for (const auto key : cache_miss_keys_) {
    if (db_index < db_value_status_array_.size() && db_value_status_array_[db_index].status.ok()) {
      db_->cache()->WriteKVToCache(const_cast<std::string&>(key), db_value_status_array_[db_index].value, db_value_status_array_[db_index].ttl);
    }
    db_index++;
  }
}

void MgetCmd::MergeCachedAndDbResults() {
  res_.AppendArrayLenUint64(keys_.size());

  std::unordered_map<std::string, std::string> db_results_map;
  for (size_t i = 0; i < cache_miss_keys_.size(); ++i) {
    if (db_value_status_array_[i].status.ok()) {
      db_results_map[cache_miss_keys_[i]] = db_value_status_array_[i].value;
    }
  }

  for (const auto& key : keys_) {
    auto cache_it = cache_hit_values_.find(key);
    if (cache_it != cache_hit_values_.end()) {
      res_.AppendStringLen(cache_it->second.size());
      res_.AppendContent(cache_it->second);
    } else {
      auto db_it = db_results_map.find(key);
      if (db_it != db_results_map.end()) {
        res_.AppendStringLen(db_it->second.size());
        res_.AppendContent(db_it->second);
      } else {
        res_.AppendContent("$-1");
      }
    }
  }
}


void KeysCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameKeys);
    return;
  }
  pattern_ = argv_[1];
  if (argv_.size() == 3) {
    std::string opt = argv_[2];
    if (strcasecmp(opt.data(), "string") == 0) {
      type_ = storage::DataType::kStrings;
    } else if (strcasecmp(opt.data(), "zset") == 0) {
      type_ = storage::DataType::kZSets;
    } else if (strcasecmp(opt.data(), "set") == 0) {
      type_ = storage::DataType::kSets;
    } else if (strcasecmp(opt.data(), "list") == 0) {
      type_ = storage::DataType::kLists;
    } else if (strcasecmp(opt.data(), "hash") == 0) {
      type_ = storage::DataType::kHashes;
    } else if (strcasecmp(opt.data(), "stream") == 0) {
      type_ = storage::DataType::kStreams;
    } else {
      res_.SetRes(CmdRes::kSyntaxErr);
    }
  } else if (argv_.size() > 3) {
    res_.SetRes(CmdRes::kSyntaxErr);
  }
}

void KeysCmd::Do() {
  int64_t total_key = 0;
  int64_t cursor = 0;
  size_t raw_limit = g_pika_conf->max_client_response_size();
  std::string raw;
  std::vector<std::string> keys;
  do {
    keys.clear();
    cursor = db_->storage()->Scan(type_, cursor, pattern_, PIKA_SCAN_STEP_LENGTH, &keys);
    for (const auto& key : keys) {
      RedisAppendLenUint64(raw, key.size(), "$");
      RedisAppendContent(raw, key);
    }
    if (raw.size() >= raw_limit) {
      res_.SetRes(CmdRes::kErrOther, "Response exceeds the max-client-response-size limit");
      return;
    }
    total_key += static_cast<int64_t>(keys.size());
  } while (cursor != 0);

  res_.AppendArrayLen(total_key);
  res_.AppendStringRaw(raw);
}

void SetnxCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSetnx);
    return;
  }
  key_ = argv_[1];
  value_ = argv_[2];
}

void SetnxCmd::Do() {
  success_ = 0;
  s_ = db_->storage()->Setnx(key_, value_, &success_);
  if (s_.ok()) {
    res_.AppendInteger(success_);
    AddSlotKey("k", key_, db_);
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

std::string SetnxCmd::ToRedisProtocol() {
  std::string content;
  content.reserve(RAW_ARGS_LEN);
  RedisAppendLen(content, 3, "*");

  // don't check variable 'success_', because if 'success_' was false, an empty binlog will be saved into file.
  // to setnx cmd
  std::string set_cmd("setnx");
  RedisAppendLenUint64(content, set_cmd.size(), "$");
  RedisAppendContent(content, set_cmd);
  // key
  RedisAppendLenUint64(content, key_.size(), "$");
  RedisAppendContent(content, key_);
  // value
  RedisAppendLenUint64(content, value_.size(), "$");
  RedisAppendContent(content, value_);
  return content;
}

void SetexCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSetex);
    return;
  }
  key_ = argv_[1];
  if (pstd::string2int(argv_[2].data(), argv_[2].size(), &sec_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  value_ = argv_[3];
}

void SetexCmd::Do() {
  s_ = db_->storage()->Setex(key_, value_, sec_);
  if (s_.ok()) {
    res_.SetRes(CmdRes::kOk);
    AddSlotKey("k", key_, db_);
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void SetexCmd::DoThroughDB() {
  Do();
}

void SetexCmd::DoUpdateCache() {
  if (s_.ok()) {
    db_->cache()->Setxx(key_, value_, sec_);
  }
}

std::string SetexCmd::ToRedisProtocol() {
  std::string content;
  content.reserve(RAW_ARGS_LEN);
  RedisAppendLen(content, 4, "*");

  // to pksetexat cmd
  std::string pksetexat_cmd("pksetexat");
  RedisAppendLenUint64(content, pksetexat_cmd.size(), "$");
  RedisAppendContent(content, pksetexat_cmd);
  // key
  RedisAppendLenUint64(content, key_.size(), "$");
  RedisAppendContent(content, key_);
  // time_stamp
  char buf[100];
  auto time_stamp = time(nullptr) + sec_;
  pstd::ll2string(buf, 100, time_stamp);
  std::string at(buf);
  RedisAppendLenUint64(content, at.size(), "$");
  RedisAppendContent(content, at);
  // value
  RedisAppendLenUint64(content, value_.size(), "$");
  RedisAppendContent(content, value_);
  return content;
}

void PsetexCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePsetex);
    return;
  }
  key_ = argv_[1];
  if (pstd::string2int(argv_[2].data(), argv_[2].size(), &usec_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  value_ = argv_[3];
}

void PsetexCmd::Do() {
  s_ = db_->storage()->Setex(key_, value_, usec_ / 1000);
  if (s_.ok()) {
    res_.SetRes(CmdRes::kOk);
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void PsetexCmd::DoThroughDB() {
  Do();
}

void PsetexCmd::DoUpdateCache() {
  if (s_.ok()) {
    db_->cache()->Setxx(key_, value_,  usec_ / 1000);
  }
}

std::string PsetexCmd::ToRedisProtocol() {
  std::string content;
  content.reserve(RAW_ARGS_LEN);
  RedisAppendLen(content, 4, "*");

  // to pksetexat cmd
  std::string pksetexat_cmd("pksetexat");
  RedisAppendLenUint64(content, pksetexat_cmd.size(), "$");
  RedisAppendContent(content, pksetexat_cmd);
  // key
  RedisAppendLenUint64(content, key_.size(), "$");
  RedisAppendContent(content, key_);
  // time_stamp
  char buf[100];
  auto time_stamp = time(nullptr) + usec_ / 1000;
  pstd::ll2string(buf, 100, time_stamp);
  std::string at(buf);
  RedisAppendLenUint64(content, at.size(), "$");
  RedisAppendContent(content, at);
  // value
  RedisAppendLenUint64(content, value_.size(), "$");
  RedisAppendContent(content, value_);
  return content;
}

void DelvxCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameDelvx);
    return;
  }
  key_ = argv_[1];
  value_ = argv_[2];
}

void DelvxCmd::Do() {
  rocksdb::Status s = db_->storage()->Delvx(key_, value_, &success_);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(success_);
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void MsetCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameMset);
    return;
  }
  size_t argc = argv_.size();
  if (argc % 2 == 0) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameMset);
    return;
  }
  kvs_.clear();
  for (size_t index = 1; index != argc; index += 2) {
    kvs_.push_back({argv_[index], argv_[index + 1]});
  }
}

void MsetCmd::Do() {
  s_ = db_->storage()->MSet(kvs_);
  if (s_.ok()) {
    res_.SetRes(CmdRes::kOk);
    std::vector<storage::KeyValue>::const_iterator it;
    for (it = kvs_.begin(); it != kvs_.end(); it++) {
      AddSlotKey("k", it->key, db_);
    }
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void MsetCmd::DoThroughDB() {
  Do();
}

void MsetCmd::DoUpdateCache() {
  if (s_.ok()) {
    for (auto key : kvs_) {
      db_->cache()->SetxxWithoutTTL(key.key, key.value);
    }
  }
}

void MsetCmd::Split(const HintKeys& hint_keys) {
  std::vector<storage::KeyValue> kvs;
  const std::vector<std::string>& keys = hint_keys.keys;
  const std::vector<int>& hints = hint_keys.hints;
  if (keys.size() != hints.size()) {
    res_.SetRes(CmdRes::kErrOther, "SplitError hint_keys size not match");
  }
  for (size_t i = 0; i < keys.size(); i++) {
    if (kvs_[hints[i]].key == keys[i]) {
      kvs.push_back(kvs_[hints[i]]);
    } else {
      res_.SetRes(CmdRes::kErrOther, "SplitError hint key: " + keys[i]);
      return;
    }
  }
  storage::Status s = db_->storage()->MSet(kvs);
  if (s.ok()) {
    res_.SetRes(CmdRes::kOk);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }
}

void MsetCmd::Merge() {}

void MsetCmd::DoBinlog() {
  PikaCmdArgsType set_argv;
  set_argv.resize(3);
  //used "set" instead of "SET" to distinguish the binlog of Set
  set_argv[0] = "set";
  set_cmd_->SetConn(GetConn());
  set_cmd_->SetResp(resp_.lock());
  for(auto& kv: kvs_){
    set_argv[1] = kv.key;
    set_argv[2] = kv.value;
    set_cmd_->Initial(set_argv, db_name_);
    set_cmd_->DoBinlog();
  }
}

void MsetnxCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameMsetnx);
    return;
  }
  size_t argc = argv_.size();
  if (argc % 2 == 0) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameMsetnx);
    return;
  }
  kvs_.clear();
  for (size_t index = 1; index != argc; index += 2) {
    kvs_.push_back({argv_[index], argv_[index + 1]});
  }
}

void MsetnxCmd::Do() {
  success_ = 0;
  rocksdb::Status s = db_->storage()->MSetnx(kvs_, &success_);
  if (s.ok()) {
    res_.AppendInteger(success_);
    std::vector<storage::KeyValue>::const_iterator it;
    for (it = kvs_.begin(); it != kvs_.end(); it++) {
      AddSlotKey("k", it->key, db_);
    }
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void MsetnxCmd::DoBinlog() {
  if (!success_) {
    //some keys already exist, set operations aborted, no need of binlog
    return;
  }
  PikaCmdArgsType set_argv;
  set_argv.resize(3);
  //used "set" instead of "SET" to distinguish the binlog of SetCmd
  set_argv[0] = "set";
  set_cmd_->SetConn(GetConn());
  set_cmd_->SetResp(resp_.lock());
  for (auto& kv: kvs_) {
    set_argv[1] = kv.key;
    set_argv[2] = kv.value;
    set_cmd_->Initial(set_argv, db_name_);
    set_cmd_->DoBinlog();
  }
}

void GetrangeCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameGetrange);
    return;
  }
  key_ = argv_[1];
  if (pstd::string2int(argv_[2].data(), argv_[2].size(), &start_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  if (pstd::string2int(argv_[3].data(), argv_[3].size(), &end_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
}

void GetrangeCmd::Do() {
  std::string substr;
  s_= db_->storage()->Getrange(key_, start_, end_, &substr);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendStringLenUint64(substr.size());
    res_.AppendContent(substr);
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void GetrangeCmd::ReadCache() {
  std::string substr;
  auto s = db_->cache()->GetRange(key_, start_, end_, &substr);
  if (s.ok()) {
    res_.AppendStringLen(substr.size());
    res_.AppendContent(substr);
  } else {
    res_.SetRes(CmdRes::kCacheMiss);
  }
}

void GetrangeCmd::DoThroughDB() {
  res_.clear();
  std::string substr;
  s_ = db_->storage()->GetrangeWithValue(key_, start_, end_, &substr, &value_, &sec_);
  if (s_.ok()) {
    res_.AppendStringLen(substr.size());
    res_.AppendContent(substr);
  } else if (s_.IsNotFound()) {
    res_.AppendStringLen(substr.size());
    res_.AppendContent(substr);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void GetrangeCmd::DoUpdateCache() {
  if (s_.ok()) {
    db_->cache()->WriteKVToCache(key_, value_, sec_);
  }
}

void SetrangeCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSetrange);
    return;
  }
  key_ = argv_[1];
  if (pstd::string2int(argv_[2].data(), argv_[2].size(), &offset_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  value_ = argv_[3];
}

void SetrangeCmd::Do() {
  int32_t new_len = 0;
  s_ = db_->storage()->Setrange(key_, offset_, value_, &new_len);
  if (s_.ok()) {
    res_.AppendInteger(new_len);
    AddSlotKey("k", key_, db_);
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void SetrangeCmd::DoThroughDB() {
  Do();
}

void SetrangeCmd::DoUpdateCache() {
  if (s_.ok()) {
    db_->cache()->SetRangexx(key_, offset_, value_);
  }
}

void StrlenCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameStrlen);
    return;
  }
  key_ = argv_[1];
}

void StrlenCmd::Do() {
  int32_t len = 0;
  s_ = db_->storage()->Strlen(key_, &len);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendInteger(len);
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void StrlenCmd::ReadCache() {
  int32_t len = 0;
  auto s= db_->cache()->Strlen(key_, &len);
  if (s.ok()) {
    res_.AppendInteger(len);
  } else {
    res_.SetRes(CmdRes::kCacheMiss);
  }
}

void StrlenCmd::DoThroughDB() {
  res_.clear();
  s_ = db_->storage()->GetWithTTL(key_, &value_, &sec_);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendInteger(value_.size());
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void StrlenCmd::DoUpdateCache() {
  if (s_.ok()) {
    db_->cache()->WriteKVToCache(key_, value_, sec_);
  }
}

void ExistsCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameExists);
    return;
  }
  keys_ = argv_;
  keys_.erase(keys_.begin());
  cache_miss_keys_.clear();
  cache_hit_keys_ = 0;
}

void ExistsCmd::Do() {
  if(cache_miss_keys_.size() == 0) {
    cache_miss_keys_ = keys_;
  }
  int64_t res = db_->storage()->Exists(cache_miss_keys_);
  if (res != -1) {
    res_.AppendInteger(res + cache_hit_keys_);
  } else {
    res_.SetRes(CmdRes::kErrOther, "exists internal error");
  }
}

void ExistsCmd::Split(const HintKeys& hint_keys) {
  int64_t res = db_->storage()->Exists(hint_keys.keys);
  if (res != -1) {
    split_res_ += res;
  } else {
    res_.SetRes(CmdRes::kErrOther, "exists internal error");
  }
}

void ExistsCmd::Merge() { res_.AppendInteger(split_res_); }

void ExistsCmd::ReadCache() {
  for ( auto key : keys_) {
    auto exist = db_->cache()->Exists(key);
    if (exist) {
      cache_hit_keys_++;
    } else {
      cache_miss_keys_.push_back(key);
    }
  }
  if (cache_miss_keys_.empty()) {
    res_.AppendInteger(cache_hit_keys_);
  } else {
    res_.SetRes(CmdRes::kCacheMiss);
  }
}

void ExistsCmd::DoThroughDB() {
  res_.clear();
  Do();
}

void ExistsCmd::DoUpdateCache() {
  for (auto key : cache_miss_keys_) {
    enum storage::DataType type = storage::DataType::kNones;
    rocksdb::Status s = db_->storage()->GetType(key, type);
    if (type >= storage::DataType::kStrings || type < storage::DataType::kStreams) {
      db_->cache()->PushKeyToAsyncLoadQueue(
        storage::DataTypeTag[static_cast<int>(type)], key, db_);
    }
  }
}

void ExpireCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameExpire);
    return;
  }
  key_ = argv_[1];
  if (pstd::string2int(argv_[2].data(), argv_[2].size(), &sec_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
}

void ExpireCmd::Do() {
  int32_t res = db_->storage()->Expire(key_, sec_);
  if (res != -1) {
    res_.AppendInteger(res);
    s_ = rocksdb::Status::OK();
  } else {
    res_.SetRes(CmdRes::kErrOther, "expire internal error");
    s_ = rocksdb::Status::Corruption("expire internal error");
  }
}

std::string ExpireCmd::ToRedisProtocol() {
  std::string content;
  content.reserve(RAW_ARGS_LEN);
  RedisAppendLen(content, 3, "*");

  // to expireat cmd
  std::string expireat_cmd("expireat");
  RedisAppendLenUint64(content, expireat_cmd.size(), "$");
  RedisAppendContent(content, expireat_cmd);
  // key
  RedisAppendLenUint64(content, key_.size(), "$");
  RedisAppendContent(content, key_);
  // sec
  char buf[100];
  int64_t expireat = time(nullptr) + sec_;
  pstd::ll2string(buf, 100, expireat);
  std::string at(buf);
  RedisAppendLenUint64(content, at.size(), "$");
  RedisAppendContent(content, at);
  return content;
}

void ExpireCmd::DoThroughDB() {
  Do();
}

void ExpireCmd::DoUpdateCache() {
  if (s_.ok()) {
    db_->cache()->Expire(key_, sec_);
  }
}

void PexpireCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePexpire);
    return;
  }
  key_ = argv_[1];
  if (pstd::string2int(argv_[2].data(), argv_[2].size(), &msec_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
}

void PexpireCmd::Do() {
  int64_t res = db_->storage()->Expire(key_, msec_ / 1000);
  if (res != -1) {
    res_.AppendInteger(res);
    s_ = rocksdb::Status::OK();
  } else {
    res_.SetRes(CmdRes::kErrOther, "expire internal error");
    s_ = rocksdb::Status::Corruption("expire internal error");
  }
}

std::string PexpireCmd::ToRedisProtocol() {
  std::string content;
  content.reserve(RAW_ARGS_LEN);
  RedisAppendLenUint64(content, argv_.size(), "*");

  // to expireat cmd
  std::string expireat_cmd("expireat");
  RedisAppendLenUint64(content, expireat_cmd.size(), "$");
  RedisAppendContent(content, expireat_cmd);
  // key
  RedisAppendLenUint64(content, key_.size(), "$");
  RedisAppendContent(content, key_);
  // sec
  char buf[100];
  int64_t expireat = time(nullptr) + msec_ / 1000;
  pstd::ll2string(buf, 100, expireat);
  std::string at(buf);
  RedisAppendLenUint64(content, at.size(), "$");
  RedisAppendContent(content, at);
  return content;
}

void PexpireCmd::DoThroughDB(){
  Do();
}

void PexpireCmd::DoUpdateCache() {
  if (s_.ok()) {
    db_->cache()->Expire(key_, msec_ / 1000);
  }
}

void ExpireatCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameExpireat);
    return;
  }
  key_ = argv_[1];
  if (pstd::string2int(argv_[2].data(), argv_[2].size(), &time_stamp_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
}

void ExpireatCmd::Do() {
  int32_t res = db_->storage()->Expireat(key_, time_stamp_);
  if (res != -1) {
    res_.AppendInteger(res);
    s_ = rocksdb::Status::OK();
  } else {
    res_.SetRes(CmdRes::kErrOther, "expireat internal error");
    s_ = rocksdb::Status::Corruption("expireat internal error");
  }
}

void ExpireatCmd::DoThroughDB() {
  Do();
}

void ExpireatCmd::DoUpdateCache() {
  if (s_.ok()) {
    db_->cache()->Expireat(key_, time_stamp_);
  }
}

void PexpireatCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePexpireat);
    return;
  }
  key_ = argv_[1];
  if (pstd::string2int(argv_[2].data(), argv_[2].size(), &time_stamp_ms_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
}

std::string PexpireatCmd::ToRedisProtocol() {
  std::string content;
  content.reserve(RAW_ARGS_LEN);
  RedisAppendLenUint64(content, argv_.size(), "*");

  // to expireat cmd
  std::string expireat_cmd("expireat");
  RedisAppendLenUint64(content, expireat_cmd.size(), "$");
  RedisAppendContent(content, expireat_cmd);
  // key
  RedisAppendLenUint64(content, key_.size(), "$");
  RedisAppendContent(content, key_);
  // sec
  char buf[100];
  int64_t expireat = time_stamp_ms_ / 1000;
  pstd::ll2string(buf, 100, expireat);
  std::string at(buf);
  RedisAppendLenUint64(content, at.size(), "$");
  RedisAppendContent(content, at);
  return content;
}

void PexpireatCmd::Do() {
  int32_t res = db_->storage()->Expireat(key_, static_cast<int32_t>(time_stamp_ms_ / 1000));
  if (res != -1) {
    res_.AppendInteger(res);
    s_ = rocksdb::Status::OK();
  } else {
    res_.SetRes(CmdRes::kErrOther, "pexpireat internal error");
    s_ = rocksdb::Status::Corruption("pexpireat internal error");
  }
}

void PexpireatCmd::DoThroughDB() {
  Do();
}

void PexpireatCmd::DoUpdateCache() {
  if (s_.ok()) {
    db_->cache()->Expireat(key_, time_stamp_ms_ / 1000);
  }
}

void TtlCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameTtl);
    return;
  }
  key_ = argv_[1];
}

void TtlCmd::Do() {
  int64_t timestamp = db_->storage()->TTL(key_);
  if (timestamp == -3) {
    res_.SetRes(CmdRes::kErrOther, "ttl internal error");
  } else {
    res_.AppendInteger(timestamp);
  }
}

void TtlCmd::ReadCache() {
  int64_t timestamp = db_->cache()->TTL(key_);
  if (timestamp == -3) {
    res_.SetRes(CmdRes::kErrOther, "ttl internal error");
  } else if (timestamp != -2) {
    res_.AppendInteger(timestamp);
  } else {
    res_.SetRes(CmdRes::kCacheMiss);
  }
}

void TtlCmd::DoThroughDB() {
  res_.clear();
  Do();
}

void PttlCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePttl);
    return;
  }
  key_ = argv_[1];
}

void PttlCmd::Do() {
  int64_t timestamp = db_->storage()->TTL(key_);
  if (timestamp == -3) {
    res_.SetRes(CmdRes::kErrOther, "ttl internal error");
  } else {
    res_.AppendInteger(timestamp);
  }
}

void PttlCmd::ReadCache() {
  int64_t timestamp = db_->cache()->TTL(key_);
  if (timestamp == -3) {
    res_.SetRes(CmdRes::kErrOther, "ttl internal error");
  } else if (timestamp != -2) {
    if (timestamp == -1) {
      res_.AppendInteger(-1);
    } else {
      res_.AppendInteger(timestamp * 1000);
    }
  } else {
    // mean this key not exist
    res_.SetRes(CmdRes::kCacheMiss);
  }
}

void PttlCmd::DoThroughDB() {
  res_.clear();
  Do();
}

void PersistCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePersist);
    return;
  }
  key_ = argv_[1];
}

void PersistCmd::Do() {
  int32_t res = db_->storage()->Persist(key_);
  if (res != -1) {
    res_.AppendInteger(res);
    s_ = rocksdb::Status::OK();
  } else {
    res_.SetRes(CmdRes::kErrOther, "persist internal error");
    s_ = rocksdb::Status::Corruption("persist internal error");
  }
}

void PersistCmd::DoThroughDB() {
  Do();
}

void PersistCmd::DoUpdateCache() {
  if (s_.ok()) {
    db_->cache()->Persist(key_);
  }
}

void TypeCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameType);
    return;
  }
  key_ = argv_[1];
}

void TypeCmd::Do() {
  enum storage::DataType type = storage::DataType::kNones;
  std::string key_type;
  rocksdb::Status s = db_->storage()->GetType(key_, type);
  if (s.ok()) {
    res_.AppendContent("+" + std::string(DataTypeToString(type)));
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void TypeCmd::ReadCache() {
  std::string key_type;
  rocksdb::Status s = db_->cache()->Type(key_, &key_type);
  if (s.ok()) {
    res_.AppendContent("+" + key_type);
  } else {
    res_.SetRes(CmdRes::kCacheMiss);
  }
}

void TypeCmd::DoThroughDB() {
  res_.clear();
  Do();
}

void ScanCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameScan);
    return;
  }
  if (pstd::string2int(argv_[1].data(), argv_[1].size(), &cursor_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  size_t index = 2;
  size_t argc = argv_.size();

  while (index < argc) {
    std::string opt = argv_[index];
    if ((strcasecmp(opt.data(), "match") == 0) || (strcasecmp(opt.data(), "count") == 0) ||
        (strcasecmp(opt.data(), "type") == 0)) {
      index++;
      if (index >= argc) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      if (strcasecmp(opt.data(), "match") == 0) {
        pattern_ = argv_[index];
      } else if (strcasecmp(opt.data(), "type") == 0) {
        std::string str_type = argv_[index];
        if (strcasecmp(str_type.data(), "string") == 0) {
          type_ = storage::DataType::kStrings;
        } else if (strcasecmp(str_type.data(), "zset") == 0) {
          type_ = storage::DataType::kZSets;
        } else if (strcasecmp(str_type.data(), "set") == 0) {
          type_ = storage::DataType::kSets;
        } else if (strcasecmp(str_type.data(), "list") == 0) {
          type_ = storage::DataType::kLists;
        } else if (strcasecmp(str_type.data(), "hash") == 0) {
          type_ = storage::DataType::kHashes;
        } else {
          res_.SetRes(CmdRes::kSyntaxErr);
        }
      } else if ((pstd::string2int(argv_[index].data(), argv_[index].size(), &count_) == 0) || count_ <= 0) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
      }
    } else {
      res_.SetRes(CmdRes::kSyntaxErr);
      return;
    }
    index++;
  }
}

void ScanCmd::Do() {
  int64_t total_key = 0;
  int64_t batch_count = 0;
  int64_t left = count_;
  int64_t cursor_ret = cursor_;
  size_t raw_limit = g_pika_conf->max_client_response_size();
  std::string raw;
  std::vector<std::string> keys;
  // To avoid memory overflow, we call the Scan method in batches
  do {
    keys.clear();
    batch_count = left < PIKA_SCAN_STEP_LENGTH ? left : PIKA_SCAN_STEP_LENGTH;
    left = left > PIKA_SCAN_STEP_LENGTH ? left - PIKA_SCAN_STEP_LENGTH : 0;
    cursor_ret = db_->storage()->Scan(type_, cursor_ret, pattern_, batch_count, &keys);
    for (const auto& key : keys) {
      RedisAppendLenUint64(raw, key.size(), "$");
      RedisAppendContent(raw, key);
    }
    if (raw.size() >= raw_limit) {
      res_.SetRes(CmdRes::kErrOther, "Response exceeds the max-client-response-size limit");
      return;
    }
    total_key += static_cast<int64_t>(keys.size());
  } while (cursor_ret != 0 && (left != 0));

  res_.AppendArrayLen(2);

  char buf[32];
  int len = pstd::ll2string(buf, sizeof(buf), cursor_ret);
  res_.AppendStringLen(len);
  res_.AppendContent(buf);

  res_.AppendArrayLen(total_key);
  res_.AppendStringRaw(raw);
}

void ScanxCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameScanx);
    return;
  }
  if (strcasecmp(argv_[1].data(), "string") == 0) {
    type_ = storage::DataType::kStrings;
  } else if (strcasecmp(argv_[1].data(), "hash") == 0) {
    type_ = storage::DataType::kHashes;
  } else if (strcasecmp(argv_[1].data(), "set") == 0) {
    type_ = storage::DataType::kSets;
  } else if (strcasecmp(argv_[1].data(), "zset") == 0) {
    type_ = storage::DataType::kZSets;
  } else if (strcasecmp(argv_[1].data(), "list") == 0) {
    type_ = storage::DataType::kLists;
  } else {
    res_.SetRes(CmdRes::kInvalidDbType);
    return;
  }

  start_key_ = argv_[2];
  size_t index = 3;
  size_t argc = argv_.size();
  while (index < argc) {
    std::string opt = argv_[index];
    if ((strcasecmp(opt.data(), "match") == 0) || (strcasecmp(opt.data(), "count") == 0)) {
      index++;
      if (index >= argc) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      if (strcasecmp(opt.data(), "match") == 0) {
        pattern_ = argv_[index];
      } else if ((pstd::string2int(argv_[index].data(), argv_[index].size(), &count_) == 0) || count_ <= 0) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
      }
    } else {
      res_.SetRes(CmdRes::kSyntaxErr);
      return;
    }
    index++;
  }
}

void ScanxCmd::Do() {
  std::string next_key;
  std::vector<std::string> keys;
  rocksdb::Status s = db_->storage()->Scanx(type_, start_key_, pattern_, count_, &keys, &next_key);

  if (s.ok()) {
    res_.AppendArrayLen(2);
    res_.AppendStringLenUint64(next_key.size());
    res_.AppendContent(next_key);

    res_.AppendArrayLenUint64(keys.size());
    std::vector<std::string>::iterator iter;
    for (const auto& key : keys) {
      res_.AppendString(key);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void PKSetexAtCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePKSetexAt);
    return;
  }
  key_ = argv_[1];
  value_ = argv_[3];
  if ((pstd::string2int(argv_[2].data(), argv_[2].size(), &time_stamp_) == 0) || time_stamp_ >= INT32_MAX) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
}

void PKSetexAtCmd::Do() {
  s_ = db_->storage()->PKSetexAt(key_, value_, static_cast<int32_t>(time_stamp_));
  if (s_.ok()) {
    res_.SetRes(CmdRes::kOk);
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void PKSetexAtCmd::DoThroughDB() {
  Do();
}

void PKSetexAtCmd::DoUpdateCache() {
  if (s_.ok()) {
    auto expire = time_stamp_ - static_cast<int64_t>(std::time(nullptr));
    if (expire <= 0) [[unlikely]] {
      db_->cache()->Del({key_});
      return;
    }
    db_->cache()->Setxx(key_, value_, expire);
  }
}

void PKScanRangeCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePKScanRange);
    return;
  }
  if (strcasecmp(argv_[1].data(), "string_with_value") == 0) {
    type_ = storage::DataType::kStrings;
    string_with_value = true;
  } else if (strcasecmp(argv_[1].data(), "string") == 0) {
    type_ = storage::DataType::kStrings;
  } else if (strcasecmp(argv_[1].data(), "hash") == 0) {
    type_ = storage::DataType::kHashes;
  } else if (strcasecmp(argv_[1].data(), "set") == 0) {
    type_ = storage::DataType::kSets;
  } else if (strcasecmp(argv_[1].data(), "zset") == 0) {
    type_ = storage::DataType::kZSets;
  } else if (strcasecmp(argv_[1].data(), "list") == 0) {
    type_ = storage::DataType::kLists;
  } else {
    res_.SetRes(CmdRes::kInvalidDbType);
    return;
  }

  key_start_ = argv_[2];
  key_end_ = argv_[3];
  // start key and end key hash tag have to be same in non classic mode
  if (!HashtagIsConsistent(key_start_, key_start_)) {
    res_.SetRes(CmdRes::kInconsistentHashTag);
    return;
  }
  size_t index = 4;
  size_t argc = argv_.size();
  while (index < argc) {
    std::string opt = argv_[index];
    if ((strcasecmp(opt.data(), "match") == 0) || (strcasecmp(opt.data(), "limit") == 0)) {
      index++;
      if (index >= argc) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      if (strcasecmp(opt.data(), "match") == 0) {
        pattern_ = argv_[index];
      } else if ((pstd::string2int(argv_[index].data(), argv_[index].size(), &limit_) == 0) || limit_ <= 0) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
      }
    } else {
      res_.SetRes(CmdRes::kSyntaxErr);
      return;
    }
    index++;
  }
}

void PKScanRangeCmd::Do() {
  std::string next_key;
  std::vector<std::string> keys;
  std::vector<storage::KeyValue> kvs;
  s_ = db_->storage()->PKScanRange(type_, key_start_, key_end_, pattern_, static_cast<int32_t>(limit_), &keys, &kvs, &next_key);

  if (s_.ok()) {
    res_.AppendArrayLen(2);
    res_.AppendStringLenUint64(next_key.size());
    res_.AppendContent(next_key);
    if (type_ == storage::DataType::kStrings) {
      res_.AppendArrayLenUint64(string_with_value ? 2 * kvs.size() : kvs.size());
      for (const auto& kv : kvs) {
        res_.AppendString(kv.key);
        if (string_with_value) {
          res_.AppendString(kv.value);
        }
      }
    } else {
      res_.AppendArrayLenUint64(keys.size());
      for (const auto& key : keys) {
        res_.AppendString(key);
      }
    }
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void PKRScanRangeCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePKRScanRange);
    return;
  }
  if (strcasecmp(argv_[1].data(), "string_with_value") == 0) {
    type_ = storage::DataType::kStrings;
    string_with_value = true;
  } else if (strcasecmp(argv_[1].data(), "string") == 0) {
    type_ = storage::DataType::kStrings;
  } else if (strcasecmp(argv_[1].data(), "hash") == 0) {
    type_ = storage::DataType::kHashes;
  } else if (strcasecmp(argv_[1].data(), "set") == 0) {
    type_ = storage::DataType::kSets;
  } else if (strcasecmp(argv_[1].data(), "zset") == 0) {
    type_ = storage::DataType::kZSets;
  } else if (strcasecmp(argv_[1].data(), "list") == 0) {
    type_ = storage::DataType::kLists;
  } else {
    res_.SetRes(CmdRes::kInvalidDbType);
    return;
  }

  key_start_ = argv_[2];
  key_end_ = argv_[3];
  // start key and end key hash tag have to be same in non classic mode
  if (!HashtagIsConsistent(key_start_, key_start_)) {
    res_.SetRes(CmdRes::kInconsistentHashTag);
    return;
  }
  size_t index = 4;
  size_t argc = argv_.size();
  while (index < argc) {
    std::string opt = argv_[index];
    if ((strcasecmp(opt.data(), "match") == 0) || (strcasecmp(opt.data(), "limit") == 0)) {
      index++;
      if (index >= argc) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      if (strcasecmp(opt.data(), "match") == 0) {
        pattern_ = argv_[index];
      } else if ((pstd::string2int(argv_[index].data(), argv_[index].size(), &limit_) == 0) || limit_ <= 0) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
      }
    } else {
      res_.SetRes(CmdRes::kSyntaxErr);
      return;
    }
    index++;
  }
}

void PKRScanRangeCmd::Do() {
  std::string next_key;
  std::vector<std::string> keys;
  std::vector<storage::KeyValue> kvs;
  s_ = db_->storage()->PKRScanRange(type_, key_start_, key_end_, pattern_, static_cast<int32_t>(limit_),
                                &keys, &kvs, &next_key);

  if (s_.ok()) {
    res_.AppendArrayLen(2);
    res_.AppendStringLenUint64(next_key.size());
    res_.AppendContent(next_key);

    if (type_ == storage::DataType::kStrings) {
      res_.AppendArrayLenUint64(string_with_value ? 2 * kvs.size() : kvs.size());
      for (const auto& kv : kvs) {
        res_.AppendString(kv.key);
        if (string_with_value) {
          res_.AppendString(kv.value);
        }
      }
    } else {
      res_.AppendArrayLenUint64(keys.size());
      for (const auto& key : keys) {
        res_.AppendString(key);
      }
    }
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}
