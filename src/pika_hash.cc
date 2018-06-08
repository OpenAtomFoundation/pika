// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "slash/include/slash_string.h"
#include "nemo.h"
#include "include/pika_hash.h"
#include "include/pika_server.h"
#include "include/pika_slot.h"

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
  rocksdb::Status s = g_pika_server->bdb()->HDel(key_, fields_, &num);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(num);
    KeyNotExistsRem("h", key_);
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
  rocksdb::Status s = g_pika_server->bdb()->HSet(key_, field_, value_, &ret);
  if (s.ok()) {
    res_.AppendContent(":" + std::to_string(ret));
    SlotKeyAdd("h", key_);
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
  rocksdb::Status s = g_pika_server->bdb()->HGet(key_, field_, &value);
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
  rocksdb::Status s = g_pika_server->bdb()->HGetall(key_, &fvs);
  if (s.ok()) {
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
  rocksdb::Status s = g_pika_server->bdb()->HExists(key_, field_);
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
  rocksdb::Status s = g_pika_server->bdb()->HIncrby(key_, field_, by_, &new_value);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendContent(":" + std::to_string(new_value));
    SlotKeyAdd("h", key_);
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
  rocksdb::Status s = g_pika_server->bdb()->HIncrbyfloat(key_, field_, by_, &new_value);
  if (s.ok()) {
    res_.AppendStringLen(new_value.size());
    res_.AppendContent(new_value);
    SlotKeyAdd("h", key_);
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
  nemo::Status s = g_pika_server->db()->HKeys(key_, fields);
  if (s.ok()) {
    res_.AppendArrayLen(fields.size());
    std::vector<std::string>::const_iterator iter;
    for (iter = fields.begin(); iter != fields.end(); iter++) {
      res_.AppendStringLen(iter->size());
      res_.AppendContent(*iter);
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
  rocksdb::Status s = g_pika_server->bdb()->HLen(key_, &len);
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
  rocksdb::Status s = g_pika_server->bdb()->HMGet(key_, fields_, &values);
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
  rocksdb::Status s = g_pika_server->bdb()->HMSet(key_, fvs_);
  if (s.ok()) {
    res_.SetRes(CmdRes::kOk);
    SlotKeyAdd("h", key_);
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
  rocksdb::Status s = g_pika_server->bdb()->HSetnx(key_, field_, value_, &ret);
  if (s.ok()) {
    res_.AppendContent(":" + std::to_string(ret));
    SlotKeyAdd("h", key_);
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
  rocksdb::Status s = g_pika_server->bdb()->HStrlen(key_, field_, &len);
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
  rocksdb::Status s = g_pika_server->bdb()->HVals(key_, &values);
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
  int64_t hlen = g_pika_server->db()->HLen(key_);
  if (hlen >= 0 && cursor_ >= hlen) {
    cursor_ = 0;
  }
  if (hlen <= 512) {
    count_ = 512;
  }
  nemo::HIterator *iter = g_pika_server->db()->HScan(key_, "", "", -1);
  iter->Skip(cursor_);
  if (!iter->Valid()) {
    delete iter;
    iter = g_pika_server->db()->HScan(key_, "", "", -1);
    cursor_ = 0;
  }
  std::vector<nemo::FV> fv_v;
  bool use_pattern = false;
  if (pattern_ != "*") {
    use_pattern = true;
  }
  for (; iter->Valid() && count_; iter->Next()) {
    count_--;
    cursor_++;
    if (use_pattern && !slash::stringmatchlen(pattern_.data(), pattern_.size(), iter->field().data(), iter->field().size(), 0)) {
      continue;
    } 
    fv_v.push_back({iter->field(), iter->value()});
  }
  if (!(iter->Valid())) {
    cursor_ = 0;
  }
  delete iter;
  
  res_.AppendContent("*2");

  char buf[32];
  int32_t len = slash::ll2string(buf, sizeof(buf), cursor_);
  res_.AppendStringLen(len);
  res_.AppendContent(buf);

  res_.AppendArrayLen(fv_v.size()*2);
  std::vector<nemo::FV>::const_iterator iter_fv = fv_v.begin();
  for (; iter_fv != fv_v.end(); iter_fv++) {
    res_.AppendStringLen(iter_fv->field.size());
    res_.AppendContent(iter_fv->field);
    res_.AppendStringLen(iter_fv->val.size());
    res_.AppendContent(iter_fv->val);
  }
  return;
}
