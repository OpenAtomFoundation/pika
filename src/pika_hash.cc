// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_hash.h"

#include "pstd/include/pstd_string.h"

#include "include/pika_conf.h"
#include "include/pika_slot_command.h"

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
  int32_t num = 0;
  rocksdb::Status s = slot->db()->HDel(key_, fields_, &num);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(num);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
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

void HSetCmd::Do(std::shared_ptr<Slot> slot) {
  int32_t ret = 0;
  rocksdb::Status s = slot->db()->HSet(key_, field_, value_, &ret);
  if (s.ok()) {
    res_.AppendContent(":" + std::to_string(ret));
    AddSlotKey("h", key_, slot);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
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
  rocksdb::Status s = slot->db()->HGet(key_, field_, &value);
  if (s.ok()) {
    res_.AppendStringLen(value.size());
    res_.AppendContent(value);
  } else if (s.IsNotFound()) {
    res_.AppendContent("$-1");
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
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
  rocksdb::Status s;
  std::vector<storage::FieldValue> fvs;

  do {
    fvs.clear();
    s = slot->db()->HScan(key_, cursor, "*", PIKA_SCAN_STEP_LENGTH, &fvs, &next_cursor);
    if (!s.ok()) {
      raw.clear();
      total_fv = 0;
      break;
    } else {
      for (const auto& fv : fvs) {
        RedisAppendLen(raw, fv.field.size(), "$");
        RedisAppendContent(raw, fv.field);
        RedisAppendLen(raw, fv.value.size(), "$");
        RedisAppendContent(raw, fv.value);
      }
      if (raw.size() >= raw_limit) {
        res_.SetRes(CmdRes::kErrOther, "Response exceeds the max-client-response-size limit");
        return;
      }
      total_fv += fvs.size();
      cursor = next_cursor;
    }
  } while (cursor != 0);

  if (s.ok() || s.IsNotFound()) {
    res_.AppendArrayLen(total_fv * 2);
    res_.AppendStringRaw(raw);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
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
  rocksdb::Status s = slot->db()->HExists(key_, field_);
  if (s.ok()) {
    res_.AppendContent(":1");
  } else if (s.IsNotFound()) {
    res_.AppendContent(":0");
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
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
  int64_t new_value;
  rocksdb::Status s = slot->db()->HIncrby(key_, field_, by_, &new_value);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendContent(":" + std::to_string(new_value));
    AddSlotKey("h", key_, slot);
  } else if (s.IsCorruption() && s.ToString() == "Corruption: hash value is not an integer") {
    res_.SetRes(CmdRes::kInvalidInt);
  } else if (s.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kOverFlow);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
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
  rocksdb::Status s = slot->db()->HIncrbyfloat(key_, field_, by_, &new_value);
  if (s.ok()) {
    res_.AppendStringLen(new_value.size());
    res_.AppendContent(new_value);
    AddSlotKey("h", key_, slot);
  } else if (s.IsCorruption() && s.ToString() == "Corruption: value is not a vaild float") {
    res_.SetRes(CmdRes::kInvalidFloat);
  } else if (s.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kOverFlow);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
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
  rocksdb::Status s = slot->db()->HKeys(key_, &fields);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendArrayLen(fields.size());
    for (const auto& field : fields) {
      res_.AppendString(field);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
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
  rocksdb::Status s = slot->db()->HLen(key_, &len);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(len);
  } else {
    res_.SetRes(CmdRes::kErrOther, "something wrong in hlen");
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
  rocksdb::Status s = slot->db()->HMGet(key_, fields_, &vss);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendArrayLen(vss.size());
    for (const auto& vs : vss) {
      if (vs.status.ok()) {
        res_.AppendStringLen(vs.value.size());
        res_.AppendContent(vs.value);
      } else {
        res_.AppendContent("$-1");
      }
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
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
  rocksdb::Status s = slot->db()->HMSet(key_, fvs_);
  if (s.ok()) {
    res_.SetRes(CmdRes::kOk);
    AddSlotKey("h", key_, slot);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
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
  rocksdb::Status s = slot->db()->HSetnx(key_, field_, value_, &ret);
  if (s.ok()) {
    res_.AppendContent(":" + std::to_string(ret));
    AddSlotKey("h", key_, slot);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
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
  rocksdb::Status s = slot->db()->HStrlen(key_, field_, &len);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(len);
  } else {
    res_.SetRes(CmdRes::kErrOther, "something wrong in hstrlen");
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
  rocksdb::Status s = slot->db()->HVals(key_, &values);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendArrayLen(values.size());
    for (const auto& value : values) {
      res_.AppendStringLen(value.size());
      res_.AppendContent(value);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
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
  rocksdb::Status s = slot->db()->HScan(key_, cursor_, pattern_, count_, &field_values, &next_cursor);

  if (s.ok() || s.IsNotFound()) {
    res_.AppendContent("*2");
    char buf[32];
    int32_t len = pstd::ll2string(buf, sizeof(buf), next_cursor);
    res_.AppendStringLen(len);
    res_.AppendContent(buf);

    res_.AppendArrayLen(field_values.size() * 2);
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
    res_.AppendStringLen(next_field.size());
    res_.AppendContent(next_field);

    res_.AppendArrayLen(2 * field_values.size());
    for (const auto& field_value : field_values) {
      res_.AppendString(field_value.field);
      res_.AppendString(field_value.value);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
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
      slot->db()->PKHScanRange(key_, field_start_, field_end_, pattern_, limit_, &field_values, &next_field);

  if (s.ok() || s.IsNotFound()) {
    res_.AppendArrayLen(2);
    res_.AppendString(next_field);

    res_.AppendArrayLen(2 * field_values.size());
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
      slot->db()->PKHRScanRange(key_, field_start_, field_end_, pattern_, limit_, &field_values, &next_field);

  if (s.ok() || s.IsNotFound()) {
    res_.AppendArrayLen(2);
    res_.AppendString(next_field);

    res_.AppendArrayLen(2 * field_values.size());
    for (const auto& field_value : field_values) {
      res_.AppendString(field_value.field);
      res_.AppendString(field_value.value);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}
