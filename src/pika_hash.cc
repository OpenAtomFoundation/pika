// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_hash.h"

#include "pstd/include/pstd_string.h"

#include "include/pika_conf.h"
#include "include/pika_slot_command.h"
#include "include/pika_stream_base.h"
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

void HDelCmd::Do(std::shared_ptr<Slot> slot) {
  s_ = slot->db()->HDel(key_, fields_, &deleted_);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendInteger(deleted_);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void HDelCmd::DoThroughDB(std::shared_ptr<Slot> slot) {
  Do(slot);
}

void HDelCmd::DoUpdateCache(std::shared_ptr<Slot> slot) {
  if (s_.ok() && deleted_ > 0) {
    std::string CachePrefixKeyH = PCacheKeyPrefixH + key_;
    slot->cache()->HDel(CachePrefixKeyH, fields_);
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

  // check the conflict of stream used prefix, see details in defination of STREAM_TREE_PREFIX
  if (key_.compare(0, STERAM_TREE_PREFIX.size(), STERAM_TREE_PREFIX) == 0 ||
      key_.compare(0, STREAM_DATA_HASH_PREFIX.size(), STREAM_DATA_HASH_PREFIX) == 0) {
    res_.SetRes(CmdRes::kErrOther, "hash key can't start with " + STERAM_TREE_PREFIX + " or " + STREAM_META_HASH_KEY);
    return;
  }
}

void HSetCmd::Do(std::shared_ptr<Slot> slot) {
  int32_t ret = 0;
  s_ = slot->db()->HSet(key_, field_, value_, &ret);
  if (s_.ok()) {
    res_.AppendContent(":" + std::to_string(ret));
    AddSlotKey("h", key_, slot);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void HSetCmd::DoThroughDB(std::shared_ptr<Slot> slot) {
  Do(slot);
}

void HSetCmd::DoUpdateCache(std::shared_ptr<Slot> slot) {
  if (s_.ok()) {
    std::string CachePrefixKeyH = PCacheKeyPrefixH + key_;
    slot->cache()->HSetIfKeyExist(CachePrefixKeyH, field_, value_);
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

void HGetCmd::Do(std::shared_ptr<Slot> slot) {
  std::string value;
  s_ = slot->db()->HGet(key_, field_, &value);
  if (s_.ok()) {
    res_.AppendStringLenUint64(value.size());
    res_.AppendContent(value);
  } else if (s_.IsNotFound()) {
    res_.AppendContent("$-1");
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void HGetCmd::ReadCache(std::shared_ptr<Slot> slot) {
  std::string value;
  std::string CachePrefixKeyH = PCacheKeyPrefixH + key_;
  auto s = slot->cache()->HGet(CachePrefixKeyH, field_, &value);
  if (s.ok()) {
    res_.AppendStringLen(value.size());
    res_.AppendContent(value);
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void HGetCmd::DoThroughDB(std::shared_ptr<Slot> slot) {
  res_.clear();
  Do(slot);
}

void HGetCmd::DoUpdateCache(std::shared_ptr<Slot> slot) {
  if (s_.ok()) {
    slot->cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_HASH, key_, slot);
  }
}

void HGetallCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHGetall);
    return;
  }
  key_ = argv_[1];
}

void HGetallCmd::Do(std::shared_ptr<Slot> slot) {
  int64_t total_fv = 0;
  int64_t cursor = 0;
  int64_t next_cursor = 0;
  size_t raw_limit = g_pika_conf->max_client_response_size();
  std::string raw;
  std::vector<storage::FieldValue> fvs;

  do {
    fvs.clear();
    s_ = slot->db()->HScan(key_, cursor, "*", PIKA_SCAN_STEP_LENGTH, &fvs, &next_cursor);
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

void HGetallCmd::ReadCache(std::shared_ptr<Slot> slot) {
  std::vector<storage::FieldValue> fvs;
  std::string CachePrefixKeyH = PCacheKeyPrefixH + key_;
  auto s = slot->cache()->HGetall(CachePrefixKeyH, &fvs);
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

void HGetallCmd::DoThroughDB(std::shared_ptr<Slot> slot) {
  res_.clear();
  Do(slot);
}

void HGetallCmd::DoUpdateCache(std::shared_ptr<Slot> slot) {
  if (s_.ok()) {
    slot->cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_HASH, key_, slot);
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

void HExistsCmd::Do(std::shared_ptr<Slot> slot) {
  s_ = slot->db()->HExists(key_, field_);
  if (s_.ok()) {
    res_.AppendContent(":1");
  } else if (s_.IsNotFound()) {
    res_.AppendContent(":0");
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void HExistsCmd::ReadCache(std::shared_ptr<Slot> slot) {
  std::string CachePrefixKeyH = PCacheKeyPrefixH + key_;
  auto s = slot->cache()->HExists(CachePrefixKeyH, field_);
  if (s.ok()) {
    res_.AppendContent(":1");
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void HExistsCmd::DoThroughDB(std::shared_ptr<Slot> slot) {
  res_.clear();
  Do(slot);
}

void HExistsCmd::DoUpdateCache(std::shared_ptr<Slot> slot) {
  if (s_.ok()) {
    slot->cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_HASH, key_, slot);
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

void HIncrbyCmd::Do(std::shared_ptr<Slot> slot) {
  int64_t new_value = 0;
  s_ = slot->db()->HIncrby(key_, field_, by_, &new_value);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendContent(":" + std::to_string(new_value));
    AddSlotKey("h", key_, slot);
  } else if (s_.IsCorruption() && s_.ToString() == "Corruption: hash value is not an integer") {
    res_.SetRes(CmdRes::kInvalidInt);
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kOverFlow);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void HIncrbyCmd::DoThroughDB(std::shared_ptr<Slot> slot) {
  Do(slot);
}

void HIncrbyCmd::DoUpdateCache(std::shared_ptr<Slot> slot) {
  if (s_.ok()) {
    std::string CachePrefixKeyH = PCacheKeyPrefixH + key_;
    slot->cache()->HIncrbyxx(CachePrefixKeyH, field_, by_);
  }
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

void HIncrbyfloatCmd::Do(std::shared_ptr<Slot> slot) {
  std::string new_value;
  s_ = slot->db()->HIncrbyfloat(key_, field_, by_, &new_value);
  if (s_.ok()) {
    res_.AppendStringLenUint64(new_value.size());
    res_.AppendContent(new_value);
    AddSlotKey("h", key_, slot);
  } else if (s_.IsCorruption() && s_.ToString() == "Corruption: value is not a vaild float") {
    res_.SetRes(CmdRes::kInvalidFloat);
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kOverFlow);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void HIncrbyfloatCmd::DoThroughDB(std::shared_ptr<Slot> slot) {
  Do(slot);
}

void HIncrbyfloatCmd::DoUpdateCache(std::shared_ptr<Slot> slot) {
  if (s_.ok()) {
    long double long_double_by;
    if (storage::StrToLongDouble(by_.data(), by_.size(), &long_double_by) != -1) {
      std::string CachePrefixKeyH = PCacheKeyPrefixH + key_;
      slot->cache()->HIncrbyfloatxx(CachePrefixKeyH, field_, long_double_by);
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

void HKeysCmd::Do(std::shared_ptr<Slot> slot) {
  std::vector<std::string> fields;
  s_ = slot->db()->HKeys(key_, &fields);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendArrayLenUint64(fields.size());
    for (const auto& field : fields) {
      res_.AppendString(field);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void HKeysCmd::ReadCache(std::shared_ptr<Slot> slot) {
  std::vector<std::string> fields;
  std::string CachePrefixKeyH = PCacheKeyPrefixH + key_;
  auto s = slot->cache()->HKeys(CachePrefixKeyH, &fields);
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

void HKeysCmd::DoThroughDB(std::shared_ptr<Slot> slot) {
  res_.clear();
  Do(slot);
}

void HKeysCmd::DoUpdateCache(std::shared_ptr<Slot> slot) {
  if (s_.ok()) {
    slot->cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_HASH, key_, slot);
  }
}

void HLenCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHLen);
    return;
  }
  key_ = argv_[1];
}

void HLenCmd::Do(std::shared_ptr<Slot> slot) {
  int32_t len = 0;
  s_ = slot->db()->HLen(key_, &len);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendInteger(len);
  } else {
    res_.SetRes(CmdRes::kErrOther, "something wrong in hlen");
  }
}

void HLenCmd::ReadCache(std::shared_ptr<Slot> slot) {
  uint64_t len = 0;
  std::string CachePrefixKeyH = PCacheKeyPrefixH + key_;
  auto s = slot->cache()->HLen(CachePrefixKeyH, &len);
  if (s.ok()) {
    res_.AppendInteger(len);
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, "something wrong in hlen");
  }
}

void HLenCmd::DoThroughDB(std::shared_ptr<Slot> slot) {
  res_.clear();
  Do(slot);
}

void HLenCmd::DoUpdateCache(std::shared_ptr<Slot> slot) {
  if (s_.ok()) {
    slot->cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_HASH, key_, slot);
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

void HMgetCmd::Do(std::shared_ptr<Slot> slot) {
  std::vector<storage::ValueStatus> vss;
  s_ = slot->db()->HMGet(key_, fields_, &vss);
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
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void HMgetCmd::ReadCache(std::shared_ptr<Slot> slot) {
  std::vector<storage::ValueStatus> vss;
  std::string CachePrefixKeyH = PCacheKeyPrefixH + key_;
  auto s = slot->cache()->HMGet(CachePrefixKeyH, fields_, &vss);
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

void HMgetCmd::DoThroughDB(std::shared_ptr<Slot> slot) {
  res_.clear();
  Do(slot);
}

void HMgetCmd::DoUpdateCache(std::shared_ptr<Slot> slot) {
  if (s_.ok()) {
    slot->cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_HASH, key_, slot);
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

void HMsetCmd::Do(std::shared_ptr<Slot> slot) {
  s_ = slot->db()->HMSet(key_, fvs_);
  if (s_.ok()) {
    res_.SetRes(CmdRes::kOk);
    AddSlotKey("h", key_, slot);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void HMsetCmd::DoThroughDB(std::shared_ptr<Slot> slot) {
  Do(slot);
}

void HMsetCmd::DoUpdateCache(std::shared_ptr<Slot> slot) {
  if (s_.ok()) {
    std::string CachePrefixKeyH = PCacheKeyPrefixH + key_;
    slot->cache()->HMSetxx(CachePrefixKeyH, fvs_);
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

void HSetnxCmd::Do(std::shared_ptr<Slot> slot) {
  int32_t ret = 0;
  s_ = slot->db()->HSetnx(key_, field_, value_, &ret);
  if (s_.ok()) {
    res_.AppendContent(":" + std::to_string(ret));
    AddSlotKey("h", key_, slot);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void HSetnxCmd::DoThroughDB(std::shared_ptr<Slot> slot) {
  Do(slot);
}

void HSetnxCmd::DoUpdateCache(std::shared_ptr<Slot> slot) {
  if (s_.ok()) {
    std::string CachePrefixKeyH = PCacheKeyPrefixH + key_;
    slot->cache()->HSetIfKeyExistAndFieldNotExist(CachePrefixKeyH, field_, value_);
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

void HStrlenCmd::Do(std::shared_ptr<Slot> slot) {
  int32_t len = 0;
  s_ = slot->db()->HStrlen(key_, field_, &len);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendInteger(len);
  } else {
    res_.SetRes(CmdRes::kErrOther, "something wrong in hstrlen");
  }
}

void HStrlenCmd::ReadCache(std::shared_ptr<Slot> slot) {
  uint64_t len = 0;
  std::string CachePrefixKeyH = PCacheKeyPrefixH + key_;
  auto s = slot->cache()->HStrlen(CachePrefixKeyH, field_, &len);
  if (s.ok()) {
    res_.AppendInteger(len);
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, "something wrong in hstrlen");
  }
  return;
}

void HStrlenCmd::DoThroughDB(std::shared_ptr<Slot> slot) {
  res_.clear();
  Do(slot);
}

void HStrlenCmd::DoUpdateCache(std::shared_ptr<Slot> slot) {
  if (s_.ok()) {
    slot->cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_HASH, key_, slot);
  }
}

void HValsCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHVals);
    return;
  }
  key_ = argv_[1];
}

void HValsCmd::Do(std::shared_ptr<Slot> slot) {
  std::vector<std::string> values;
  s_ = slot->db()->HVals(key_, &values);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendArrayLenUint64(values.size());
    for (const auto& value : values) {
      res_.AppendStringLenUint64(value.size());
      res_.AppendContent(value);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void HValsCmd::ReadCache(std::shared_ptr<Slot> slot) {
  std::vector<std::string> values;
  std::string CachePrefixKeyH = PCacheKeyPrefixH + key_;
  auto s = slot->cache()->HVals(CachePrefixKeyH, &values);
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

void HValsCmd::DoThroughDB(std::shared_ptr<Slot> slot) {
  res_.clear();
  Do(slot);
}

void HValsCmd::DoUpdateCache(std::shared_ptr<Slot> slot) {
  if (s_.ok()) {
    slot->cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_HASH, key_, slot);
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

void HScanCmd::Do(std::shared_ptr<Slot> slot) {
  int64_t next_cursor = 0;
  std::vector<storage::FieldValue> field_values;
  auto s = slot->db()->HScan(key_, cursor_, pattern_, count_, &field_values, &next_cursor);

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

void HScanxCmd::Do(std::shared_ptr<Slot> slot) {
  std::string next_field;
  std::vector<storage::FieldValue> field_values;
  rocksdb::Status s = slot->db()->HScanx(key_, start_field_, pattern_, count_, &field_values, &next_field);

  if (s.ok() || s.IsNotFound()) {
    res_.AppendArrayLen(2);
    res_.AppendStringLenUint64(next_field.size());
    res_.AppendContent(next_field);

    res_.AppendArrayLenUint64(2 * field_values.size());
    for (const auto& field_value : field_values) {
      res_.AppendString(field_value.field);
      res_.AppendString(field_value.value);
    }
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

void PKHScanRangeCmd::Do(std::shared_ptr<Slot> slot) {
  std::string next_field;
  std::vector<storage::FieldValue> field_values;
  rocksdb::Status s =
      slot->db()->PKHScanRange(key_, field_start_, field_end_, pattern_, static_cast<int32_t>(limit_), &field_values, &next_field);

  if (s.ok() || s.IsNotFound()) {
    res_.AppendArrayLen(2);
    res_.AppendString(next_field);

    res_.AppendArrayLenUint64(2 * field_values.size());
    for (const auto& field_value : field_values) {
      res_.AppendString(field_value.field);
      res_.AppendString(field_value.value);
    }
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

void PKHRScanRangeCmd::Do(std::shared_ptr<Slot> slot) {
  std::string next_field;
  std::vector<storage::FieldValue> field_values;
  rocksdb::Status s =
      slot->db()->PKHRScanRange(key_, field_start_, field_end_, pattern_, static_cast<int32_t>(limit_), &field_values, &next_field);

  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendArrayLen(2);
    res_.AppendString(next_field);

    res_.AppendArrayLenUint64(2 * field_values.size());
    for (const auto& field_value : field_values) {
      res_.AppendString(field_value.field);
      res_.AppendString(field_value.value);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}
