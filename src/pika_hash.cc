// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_hash.h"

#include "pstd/include/pstd_string.h"

#include "include/pika_conf.h"
#include "include/pika_slot_command.h"
#include "include/pika_cache.h"

extern std::unique_ptr<PikaConf> g_pika_conf;

void HDelCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHDel);
    return;
  }
  key_ = argv_[1];
  auto iter = argv_.begin();
  iter++;
  iter++;
  fields_.assign(iter, argv_.end());
}

void HDelCmd::Do() {
  s_ = db_->storage()->HDel(key_, fields_, &deleted_);

  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendInteger(deleted_);
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void HDelCmd::DoThroughDB() {
  Do();
}

void HDelCmd::DoUpdateCache() {
  if (s_.ok() && deleted_ > 0) {
    db_->cache()->HDel(key_, fields_);
  }
}

void HSetCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHSet);
    return;
  }
  key_ = argv_[1];
  field_ = argv_[2];
  value_ = argv_[3];
}

void HSetCmd::Do() {
  int32_t ret = 0;
  s_ = db_->storage()->HSet(key_, field_, value_, &ret);
  if (s_.ok()) {
    res_.AppendContent(":" + std::to_string(ret));
    AddSlotKey("h", key_, db_);
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void HSetCmd::DoThroughDB() {
  Do();
}

void HSetCmd::DoUpdateCache() {
  if (s_.ok()) {
    db_->cache()->HSetIfKeyExist(key_, field_, value_);
  }
}

void HGetCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHGet);
    return;
  }
  key_ = argv_[1];
  field_ = argv_[2];
}

void HGetCmd::Do() {
  std::string value;
  s_ = db_->storage()->HGet(key_, field_, &value);
  if (s_.ok()) {
    res_.AppendStringLenUint64(value.size());
    res_.AppendContent(value);
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else if (s_.IsNotFound()) {
    res_.AppendContent("$-1");
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void HGetCmd::ReadCache() {
  std::string value;
  auto s = db_->cache()->HGet(key_, field_, &value);
  if (s.ok()) {
    res_.AppendStringLen(value.size());
    res_.AppendContent(value);
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void HGetCmd::DoThroughDB() {
  res_.clear();
  Do();
}

void HGetCmd::DoUpdateCache() {
  if (s_.ok()) {
    db_->cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_HASH, key_, db_);
  }
}

void HGetallCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHGetall);
    return;
  }
  key_ = argv_[1];
}

void HGetallCmd::Do() {
  int64_t total_fv = 0;
  int64_t cursor = 0;
  int64_t next_cursor = 0;
  size_t raw_limit = g_pika_conf->max_client_response_size();
  std::string raw;
  std::vector<storage::FieldValue> fvs;

  do {
    fvs.clear();
    s_ = db_->storage()->HScan(key_, cursor, "*", PIKA_SCAN_STEP_LENGTH, &fvs, &next_cursor);
    if (!s_.ok()) {
      raw.clear();
      total_fv = 0;
      break;
    } else {
      for (const auto& fv : fvs) {
        RedisAppendLenUint64(raw, fv.field.size(), "$");
        RedisAppendContent(raw, fv.field);
        RedisAppendLenUint64(raw, fv.value.size(), "$");
        RedisAppendContent(raw, fv.value);
      }
      if (raw.size() >= raw_limit) {
        res_.SetRes(CmdRes::kErrOther, "Response exceeds the max-client-response-size limit");
        return;
      }
      total_fv += static_cast<int64_t>(fvs.size());
      cursor = next_cursor;
    }
  } while (cursor != 0);

  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendArrayLen(total_fv * 2);
    res_.AppendStringRaw(raw);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void HGetallCmd::ReadCache() {
  std::vector<storage::FieldValue> fvs;
  auto s = db_->cache()->HGetall(key_, &fvs);
  if (s.ok()) {
    res_.AppendArrayLen(fvs.size() * 2);
    for (const auto& fv : fvs) {
      res_.AppendStringLen(fv.field.size());
      res_.AppendContent(fv.field);
      res_.AppendStringLen(fv.value.size());
      res_.AppendContent(fv.value);
    }
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void HGetallCmd::DoThroughDB() {
  res_.clear();
  Do();
}

void HGetallCmd::DoUpdateCache() {
  if (s_.ok()) {
    db_->cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_HASH, key_, db_);
  }
}

void HExistsCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHExists);
    return;
  }
  key_ = argv_[1];
  field_ = argv_[2];
}

void HExistsCmd::Do() {
  s_ = db_->storage()->HExists(key_, field_);
  if (s_.ok()) {
    res_.AppendContent(":1");
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else if (s_.IsNotFound()) {
    res_.AppendContent(":0");
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void HExistsCmd::ReadCache() {
  auto s = db_->cache()->HExists(key_, field_);
  if (s.ok()) {
    res_.AppendContent(":1");
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void HExistsCmd::DoThroughDB() {
  res_.clear();
  Do();
}

void HExistsCmd::DoUpdateCache() {
  if (s_.ok()) {
    db_->cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_HASH, key_, db_);
  }
}

void HIncrbyCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHIncrby);
    return;
  }
  key_ = argv_[1];
  field_ = argv_[2];
  if (argv_[3].find(' ') != std::string::npos || (pstd::string2int(argv_[3].data(), argv_[3].size(), &by_) == 0)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
}

void HIncrbyCmd::Do() {
  s_ = db_->storage()->HIncrby(key_, field_, by_, &new_value_, &ttl_);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendContent(":" + std::to_string(new_value_));
    AddSlotKey("h", key_, db_);
  } else if (s_.IsInvalidArgument() && s_.ToString().substr(0, std::char_traits<char>::length(ErrTypeMessage)) == ErrTypeMessage) {
    res_.SetRes(CmdRes::kMultiKey);
  } else if (s_.IsCorruption() && s_.ToString() == "Corruption: hash value is not an integer") {
    res_.SetRes(CmdRes::kInvalidInt);
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kOverFlow);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void HIncrbyCmd::DoThroughDB() {
  Do();
}

void HIncrbyCmd::DoUpdateCache() {
  if (s_.ok()) {
    db_->cache()->HIncrbyxx(key_, field_, by_);
  }
}

std::string HIncrbyCmd::ToRedisProtocol() {
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
  auto time_stamp = time(nullptr) + ttl_;
  pstd::ll2string(buf, 100, time_stamp);
  std::string at(buf);
  RedisAppendLenUint64(content, at.size(), "$");
  RedisAppendContent(content, at);
  // value
  std::string new_value_str = std::to_string(new_value_);
  RedisAppendLenUint64(content, new_value_str.size(), "$");
  RedisAppendContent(content, new_value_str);
  return content;
}

void HIncrbyfloatCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHIncrbyfloat);
    return;
  }
  key_ = argv_[1];
  field_ = argv_[2];
  by_ = argv_[3];
}

void HIncrbyfloatCmd::Do() {
  std::string new_value;
  s_ = db_->storage()->HIncrbyfloat(key_, field_, by_, &new_value);
  if (s_.ok()) {
    res_.AppendStringLenUint64(new_value.size());
    res_.AppendContent(new_value);
    AddSlotKey("h", key_, db_);
  } else if (s_.IsInvalidArgument() && s_.ToString().substr(0, std::char_traits<char>::length(ErrTypeMessage)) == ErrTypeMessage) {
    res_.SetRes(CmdRes::kMultiKey);
  } else if (s_.IsCorruption() && s_.ToString() == "Corruption: value is not a vaild float") {
    res_.SetRes(CmdRes::kInvalidFloat);
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kOverFlow);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void HIncrbyfloatCmd::DoThroughDB() {
  Do();
}

void HIncrbyfloatCmd::DoUpdateCache() {
  if (s_.ok()) {
    long double long_double_by;
    if (storage::StrToLongDouble(by_.data(), by_.size(), &long_double_by) != -1) {
      db_->cache()->HIncrbyfloatxx(key_, field_, long_double_by);
    }
  }
}

void HKeysCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHKeys);
    return;
  }
  key_ = argv_[1];
}

void HKeysCmd::Do() {
  std::vector<std::string> fields;
  s_ = db_->storage()->HKeys(key_, &fields);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendArrayLenUint64(fields.size());
    for (const auto& field : fields) {
      res_.AppendString(field);
    }
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void HKeysCmd::ReadCache() {
  std::vector<std::string> fields;
  auto s = db_->cache()->HKeys(key_, &fields);
  if (s.ok()) {
    res_.AppendArrayLen(fields.size());
    for (const auto& field : fields) {
      res_.AppendString(field);
    }
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void HKeysCmd::DoThroughDB() {
  res_.clear();
  Do();
}

void HKeysCmd::DoUpdateCache() {
  if (s_.ok()) {
    db_->cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_HASH, key_, db_);
  }
}

void HLenCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHLen);
    return;
  }
  key_ = argv_[1];
}

void HLenCmd::Do() {
  int32_t len = 0;
  s_ = db_->storage()->HLen(key_, &len);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendInteger(len);
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, "something wrong in hlen");
  }
}

void HLenCmd::ReadCache() {
  uint64_t len = 0;
  auto s = db_->cache()->HLen(key_, &len);
  if (s.ok()) {
    res_.AppendInteger(len);
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, "something wrong in hlen");
  }
}

void HLenCmd::DoThroughDB() {
  res_.clear();
  Do();
}

void HLenCmd::DoUpdateCache() {
  if (s_.ok()) {
    db_->cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_HASH, key_, db_);
  }
}

void HMgetCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHMget);
    return;
  }
  key_ = argv_[1];
  auto iter = argv_.begin();
  iter++;
  iter++;
  fields_.assign(iter, argv_.end());
}

void HMgetCmd::Do() {
  std::vector<storage::ValueStatus> vss;
  s_ = db_->storage()->HMGet(key_, fields_, &vss);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendArrayLenUint64(vss.size());
    for (const auto& vs : vss) {
      if (vs.status.ok()) {
        res_.AppendStringLenUint64(vs.value.size());
        res_.AppendContent(vs.value);
      } else {
        res_.AppendContent("$-1");
      }
    }
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void HMgetCmd::ReadCache() {
  std::vector<storage::ValueStatus> vss;
  auto s = db_->cache()->HMGet(key_, fields_, &vss);
  if (s.ok()) {
    res_.AppendArrayLen(vss.size());
    for (const auto& vs : vss) {
      if (vs.status.ok()) {
        res_.AppendStringLen(vs.value.size());
        res_.AppendContent(vs.value);
      } else {
        res_.AppendContent("$-1");
      }
    }
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void HMgetCmd::DoThroughDB() {
  res_.clear();
  Do();
}

void HMgetCmd::DoUpdateCache() {
  if (s_.ok()) {
    db_->cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_HASH, key_, db_);
  }
}

void HMsetCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHMset);
    return;
  }
  key_ = argv_[1];
  size_t argc = argv_.size();
  if (argc % 2 != 0) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHMset);
    return;
  }
  size_t index = 2;
  fvs_.clear();
  for (; index < argc; index += 2) {
    fvs_.push_back({argv_[index], argv_[index + 1]});
  }
}

void HMsetCmd::Do() {
  s_ = db_->storage()->HMSet(key_, fvs_);
  if (s_.ok()) {
    res_.SetRes(CmdRes::kOk);
    AddSlotKey("h", key_, db_);
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void HMsetCmd::DoThroughDB() {
  Do();
}

void HMsetCmd::DoUpdateCache() {
  if (s_.ok()) {
    db_->cache()->HMSetxx(key_, fvs_);
  }
}

void HSetnxCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHSetnx);
    return;
  }
  key_ = argv_[1];
  field_ = argv_[2];
  value_ = argv_[3];
}

void HSetnxCmd::Do() {
  int32_t ret = 0;
  s_ = db_->storage()->HSetnx(key_, field_, value_, &ret);
  if (s_.ok()) {
    res_.AppendContent(":" + std::to_string(ret));
    AddSlotKey("h", key_, db_);
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void HSetnxCmd::DoThroughDB() {
  Do();
}

void HSetnxCmd::DoUpdateCache() {
  if (s_.ok()) {
    db_->cache()->HSetIfKeyExistAndFieldNotExist(key_, field_, value_);
  }
}

void HStrlenCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHStrlen);
    return;
  }
  key_ = argv_[1];
  field_ = argv_[2];
}

void HStrlenCmd::Do() {
  int32_t len = 0;
  s_ = db_->storage()->HStrlen(key_, field_, &len);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendInteger(len);
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, "something wrong in hstrlen");
  }
}

void HStrlenCmd::ReadCache() {
  uint64_t len = 0;
  auto s = db_->cache()->HStrlen(key_, field_, &len);
  if (s.ok()) {
    res_.AppendInteger(len);
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, "something wrong in hstrlen");
  }
  return;
}

void HStrlenCmd::DoThroughDB() {
  res_.clear();
  Do();
}

void HStrlenCmd::DoUpdateCache() {
  if (s_.ok()) {
    db_->cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_HASH, key_, db_);
  }
}

void HValsCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHVals);
    return;
  }
  key_ = argv_[1];
}

void HValsCmd::Do() {
  std::vector<std::string> values;
  s_ = db_->storage()->HVals(key_, &values);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendArrayLenUint64(values.size());
    for (const auto& value : values) {
      res_.AppendStringLenUint64(value.size());
      res_.AppendContent(value);
    }
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void HValsCmd::ReadCache() {
  std::vector<std::string> values;
  auto s = db_->cache()->HVals(key_, &values);
  if (s.ok()) {
    res_.AppendArrayLen(values.size());
    for (const auto& value : values) {
      res_.AppendStringLen(value.size());
      res_.AppendContent(value);
    }
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void HValsCmd::DoThroughDB() {
  res_.clear();
  Do();
}

void HValsCmd::DoUpdateCache() {
  if (s_.ok()) {
    db_->cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_HASH, key_, db_);
  }
}

void HScanCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHScan);
    return;
  }
  key_ = argv_[1];
  if (pstd::string2int(argv_[2].data(), argv_[2].size(), &cursor_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
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
      } else if (pstd::string2int(argv_[index].data(), argv_[index].size(), &count_) == 0) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
      }
    } else {
      res_.SetRes(CmdRes::kSyntaxErr);
      return;
    }
    index++;
  }
  if (count_ < 0) {
    res_.SetRes(CmdRes::kSyntaxErr);
    return;
  }
}

void HScanCmd::Do() {
  int64_t next_cursor = 0;
  std::vector<storage::FieldValue> field_values;
  auto s = db_->storage()->HScan(key_, cursor_, pattern_, count_, &field_values, &next_cursor);

  if (s.ok() || s.IsNotFound()) {
    res_.AppendContent("*2");
    char buf[32];
    int32_t len = pstd::ll2string(buf, sizeof(buf), next_cursor);
    res_.AppendStringLen(len);
    res_.AppendContent(buf);

    res_.AppendArrayLenUint64(field_values.size() * 2);
    for (const auto& field_value : field_values) {
      res_.AppendString(field_value.field);
      res_.AppendString(field_value.value);
    }
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void HScanxCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHScan);
    return;
  }
  key_ = argv_[1];
  start_field_ = argv_[2];

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
      } else if (pstd::string2int(argv_[index].data(), argv_[index].size(), &count_) == 0) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
      }
    } else {
      res_.SetRes(CmdRes::kSyntaxErr);
      return;
    }
    index++;
  }
  if (count_ < 0) {
    res_.SetRes(CmdRes::kSyntaxErr);
    return;
  }
}

void HScanxCmd::Do() {
  std::string next_field;
  std::vector<storage::FieldValue> field_values;
  rocksdb::Status s = db_->storage()->HScanx(key_, start_field_, pattern_, count_, &field_values, &next_field);

  if (s.ok() || s.IsNotFound()) {
    res_.AppendArrayLen(2);
    res_.AppendStringLenUint64(next_field.size());
    res_.AppendContent(next_field);

    res_.AppendArrayLenUint64(2 * field_values.size());
    for (const auto& field_value : field_values) {
      res_.AppendString(field_value.field);
      res_.AppendString(field_value.value);
    }
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void PKHScanRangeCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePKHScanRange);
    return;
  }
  key_ = argv_[1];
  field_start_ = argv_[2];
  field_end_ = argv_[3];

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

void PKHScanRangeCmd::Do() {
  std::string next_field;
  std::vector<storage::FieldValue> field_values;
  rocksdb::Status s =
      db_->storage()->PKHScanRange(key_, field_start_, field_end_, pattern_, static_cast<int32_t>(limit_), &field_values, &next_field);

  if (s.ok() || s.IsNotFound()) {
    res_.AppendArrayLen(2);
    res_.AppendString(next_field);

    res_.AppendArrayLenUint64(2 * field_values.size());
    for (const auto& field_value : field_values) {
      res_.AppendString(field_value.field);
      res_.AppendString(field_value.value);
    }
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void PKHRScanRangeCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePKHRScanRange);
    return;
  }
  key_ = argv_[1];
  field_start_ = argv_[2];
  field_end_ = argv_[3];

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

void PKHRScanRangeCmd::Do() {
  std::string next_field;
  std::vector<storage::FieldValue> field_values;
  rocksdb::Status s =
      db_->storage()->PKHRScanRange(key_, field_start_, field_end_, pattern_, static_cast<int32_t>(limit_), &field_values, &next_field);

  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendArrayLen(2);
    res_.AppendString(next_field);

    res_.AppendArrayLenUint64(2 * field_values.size());
    for (const auto& field_value : field_values) {
      res_.AppendString(field_value.field);
      res_.AppendString(field_value.value);
    }
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  }  else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}
