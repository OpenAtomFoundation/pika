// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "slash/include/slash_string.h"
#include "include/pika_hash.h"
#include "include/pika_server.h"

extern PikaServer *g_pika_server;

void HDelCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHDel);
    return;
  }
  key_ = argv[1];
  PikaCmdArgsType::iterator iter = argv.begin();
  iter++; 
  iter++;
  fields_.assign(iter, argv.end());
  return;
}

void HDelCmd::Do() {
  int32_t num = 0;
  rocksdb::Status s = g_pika_server->db()->HDel(key_, fields_, &num);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(num);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void HSetCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHSet);
    return;
  }
  key_ = argv[1];
  field_ = argv[2];
  value_ = argv[3];
  return;
}

void HSetCmd::Do() {
  int32_t ret = 0;
  rocksdb::Status s = g_pika_server->db()->HSet(key_, field_, value_, &ret);
  if (s.ok()) {
    res_.AppendContent(":" + std::to_string(ret));
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void HGetCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHGet);
    return;
  }
  key_ = argv[1];
  field_ = argv[2];
  return;
}

void HGetCmd::Do() {
  std::string value;
  rocksdb::Status s = g_pika_server->db()->HGet(key_, field_, &value);
  if (s.ok()) {
    res_.AppendStringLen(value.size());
    res_.AppendContent(value);
  } else if (s.IsNotFound()) {
    res_.AppendContent("$-1");
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void HGetallCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHGetall);
    return;
  }
  key_ = argv[1];
  return;
}

void HGetallCmd::Do() {
  std::vector<blackwidow::FieldValue> fvs;
  rocksdb::Status s = g_pika_server->db()->HGetall(key_, &fvs);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendArrayLen(fvs.size() * 2);
    for (const auto& fv : fvs) {
      res_.AppendStringLen(fv.field.size());
      res_.AppendContent(fv.field);
      res_.AppendStringLen(fv.value.size());
      res_.AppendContent(fv.value);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}


void HExistsCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHExists);
    return;
  }
  key_ = argv[1];
  field_ = argv[2];
  return;
}

void HExistsCmd::Do() {
  rocksdb::Status s = g_pika_server->db()->HExists(key_, field_);
  if (s.ok()) {
    res_.AppendContent(":1");
  } else if (s.IsNotFound()) {
    res_.AppendContent(":0");
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void HIncrbyCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHIncrby);
    return;
  }
  key_ = argv[1];
  field_ = argv[2];
  if (argv[3].find(" ") != std::string::npos || !slash::string2l(argv[3].data(), argv[3].size(), &by_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  return;
}

void HIncrbyCmd::Do() {
  int64_t new_value;
  rocksdb::Status s = g_pika_server->db()->HIncrby(key_, field_, by_, &new_value);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendContent(":" + std::to_string(new_value));
  } else if (s.IsCorruption() && s.ToString() == "Corruption: hash value is not an integer") {
    res_.SetRes(CmdRes::kInvalidInt);
  } else if (s.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kOverFlow);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void HIncrbyfloatCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHIncrbyfloat);
    return;
  }
  key_ = argv[1];
  field_ = argv[2];
  by_ = argv[3];
  return;
}

void HIncrbyfloatCmd::Do() {
  std::string new_value;
  rocksdb::Status s = g_pika_server->db()->HIncrbyfloat(key_, field_, by_, &new_value);
  if (s.ok()) {
    res_.AppendStringLen(new_value.size());
    res_.AppendContent(new_value);
  } else if (s.IsCorruption() && s.ToString() == "Corruption: value is not a vaild float") {
    res_.SetRes(CmdRes::kInvalidFloat);
  } else if (s.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kOverFlow);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void HKeysCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHKeys);
    return;
  }
  key_ = argv[1];
  return;
}

void HKeysCmd::Do() {
  std::vector<std::string> fields;
  rocksdb::Status s = g_pika_server->db()->HKeys(key_, &fields);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendArrayLen(fields.size());
    for (const auto& field : fields) {
      res_.AppendString(field);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void HLenCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHLen);
    return;
  }
  key_ = argv[1];
  return;
}

void HLenCmd::Do() {
  int32_t len = 0;
  rocksdb::Status s = g_pika_server->db()->HLen(key_, &len);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(len);
  } else {
    res_.SetRes(CmdRes::kErrOther, "something wrong in hlen");
  }
  return;
}

void HMgetCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHMget);
    return;
  }
  key_ = argv[1];
  PikaCmdArgsType::iterator iter = argv.begin();
  iter++;
  iter++;
  fields_.assign(iter, argv.end()); 
  return;
}

void HMgetCmd::Do() {
  std::vector<std::string> values;
  rocksdb::Status s = g_pika_server->db()->HMGet(key_, fields_, &values);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendArrayLen(values.size());
    for (const auto& value : values) {
      if (!value.empty()) {
        res_.AppendStringLen(value.size());
        res_.AppendContent(value);
      } else {
        res_.AppendContent("$-1");
      }
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void HMsetCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHMset);
    return;
  }
  key_ = argv[1];
  size_t argc = argv.size();
  if (argc % 2 != 0) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHMset);
    return;
  }
  size_t index = 2;
  fvs_.clear();
  for (; index < argc; index += 2) {
    fvs_.push_back({argv[index], argv[index + 1]});
  }
  return;
}

void HMsetCmd::Do() {
  rocksdb::Status s = g_pika_server->db()->HMSet(key_, fvs_);
  if (s.ok()) {
    res_.SetRes(CmdRes::kOk);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void HSetnxCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHSetnx);
    return;
  }
  key_ = argv[1];
  field_ = argv[2];
  value_ = argv[3];
  return;
}

void HSetnxCmd::Do() {
  int32_t ret = 0;
  rocksdb::Status s = g_pika_server->db()->HSetnx(key_, field_, value_, &ret);
  if (s.ok()) {
    res_.AppendContent(":" + std::to_string(ret));
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void HStrlenCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHStrlen);
    return;
  }
  key_ = argv[1];
  field_ = argv[2];
  return;
}

void HStrlenCmd::Do() {
  int32_t len = 0;
  rocksdb::Status s = g_pika_server->db()->HStrlen(key_, field_, &len);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(len);
  } else {
    res_.SetRes(CmdRes::kErrOther, "something wrong in hstrlen");
  }
  return;
}

void HValsCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHVals);
    return;
  }
  key_ = argv[1];
  return;
}

void HValsCmd::Do() {
  std::vector<std::string> values;
  rocksdb::Status s = g_pika_server->db()->HVals(key_, &values);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendArrayLen(values.size());
    for (const auto& value : values) {
      res_.AppendStringLen(value.size());
      res_.AppendContent(value);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void HScanCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHScan);
    return;
  }
  key_ = argv[1];
  if (!slash::string2l(argv[2].data(), argv[2].size(), &cursor_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  size_t index = 3, argc = argv.size();

  while (index < argc) {
    std::string opt = slash::StringToLower(argv[index]); 
    if (opt == "match" || opt == "count") {
      index++;
      if (index >= argc) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      if (opt == "match") {
        pattern_ = argv[index];
      } else if (!slash::string2l(argv[index].data(), argv[index].size(), &count_)) {
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
  return;
}

void HScanCmd::Do() {
  int64_t next_cursor = 0;
  std::vector<blackwidow::FieldValue> field_values;
  rocksdb::Status s = g_pika_server->db()->HScan(key_, cursor_, pattern_, count_, &field_values, &next_cursor);

  if (s.ok() || s.IsNotFound()) {
    res_.AppendContent("*2");
    char buf[32];
    int32_t len = slash::ll2string(buf, sizeof(buf), next_cursor);
    res_.AppendStringLen(len);
    res_.AppendContent(buf);

    res_.AppendArrayLen(field_values.size()*2);
    for (const auto& field_value : field_values) {
      res_.AppendString(field_value.field);
      res_.AppendString(field_value.value);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void HScanxCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHScan);
    return;
  }
  key_ = argv[1];
  start_field_ = argv[2];

  size_t index = 3, argc = argv.size();
  while (index < argc) {
    std::string opt = slash::StringToLower(argv[index]);
    if (opt == "match" || opt == "count") {
      index++;
      if (index >= argc) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      if (opt == "match") {
        pattern_ = argv[index];
      } else if (!slash::string2l(argv[index].data(), argv[index].size(), &count_)) {
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
  return;
}

void HScanxCmd::Do() {
  std::string next_field;
  std::vector<blackwidow::FieldValue> field_values;
  rocksdb::Status s = g_pika_server->db()->HScanx(key_, start_field_, pattern_, count_, &field_values, &next_field);

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
  return;
}
