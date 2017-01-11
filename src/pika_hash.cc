// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "slash_string.h"
#include "nemo.h"
#include "pika_hash.h"
#include "pika_server.h"
#include "pika_slot.h"

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
  int64_t num = 0;
  nemo::Status s;
  size_t index = 0, fields_num = fields_.size();
  for (; index != fields_num; index++) {
    s = g_pika_server->db()->HDel(key_, fields_[index]);
    if (s.ok()) {
      num++;
    }
  }
  res_.AppendInteger(num);
  KeyNotExistsRem("h", key_);
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
  nemo::Status s = g_pika_server->db()->HGet(key_, field_, &value);
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
  std::vector<nemo::FV> fvs;
  nemo::Status s = g_pika_server->db()->HGetall(key_, fvs);
  if (s.ok()) {
    res_.AppendArrayLen(fvs.size()*2);
    std::vector<nemo::FV>::const_iterator iter;
    for (iter = fvs.begin(); iter != fvs.end(); iter++) {
      res_.AppendStringLen(iter->field.size());
      res_.AppendContent(iter->field);
      res_.AppendStringLen(iter->val.size());
      res_.AppendContent(iter->val);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

static void DoHSet(const std::string &key, const std::string &field, const std::string &value, CmdRes& res, const std::string &exp_res) {
  nemo::Status s = g_pika_server->db()->HSet(key, field, value);
  if (s.ok()) {
    res.AppendContent(exp_res);
  } else {
    res.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void HSetCmd::Do() {
  std::string tmp;
  nemo::Status s = g_pika_server->db()->HGet(key_, field_, &tmp);
  if (s.ok()) {
    if (tmp != value_) {
      DoHSet(key_, field_, value_, res_, ":0");
      SlotKeyAdd("h", key_);
    } else {
      res_.AppendContent(":0");
    }
  } else if (s.IsNotFound()) {
    DoHSet(key_, field_, value_, res_, ":1");
    SlotKeyAdd("h", key_);
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
  std::string value;
  bool exist = g_pika_server->db()->HExists(key_, field_);
  if (exist) {
    res_.AppendContent(":1");
  } else {
    res_.AppendContent(":0");
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
  std::string new_value;
  nemo::Status s = g_pika_server->db()->HIncrby(key_, field_, by_, new_value);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendContent(":" + new_value);
    SlotKeyAdd("h", key_);
  } else if (s.IsCorruption() && s.ToString() == "Corruption: value is not integer") {
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
  if (argv[3].find(" ") != std::string::npos || !slash::string2d(argv[3].data(), argv[3].size(), &by_)) {
    res_.SetRes(CmdRes::kInvalidFloat);
    return;
  }
  return;
}

void HIncrbyfloatCmd::Do() {
  std::string new_value;
  nemo::Status s = g_pika_server->db()->HIncrbyfloat(key_, field_, by_, new_value);
  if (s.ok()) {
    res_.AppendStringLen(new_value.size());
    res_.AppendContent(new_value);
    SlotKeyAdd("h", key_);
  } else if (s.IsCorruption() && s.ToString() == "Corruption: value is not float") {
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
  int64_t len = 0;
  len = g_pika_server->db()->HLen(key_);
  if (len >= 0) {
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
  std::vector<nemo::FVS> fvs_v;
  nemo::Status s = g_pika_server->db()->HMGet(key_, fields_, fvs_v);
  if (s.ok()) {
    res_.AppendArrayLen(fvs_v.size());
    std::vector<nemo::FVS>::const_iterator iter = fvs_v.begin();
    for (; iter != fvs_v.end(); iter++) {
      if ((iter->status).ok()) {
        res_.AppendStringLen(iter->val.size());
        res_.AppendContent(iter->val);
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
  fv_v_.clear();
  for (; index < argc; index += 2) {
    fv_v_.push_back({argv[index], argv[index+1]});  
  }
  return;
}

void HMsetCmd::Do() {
  nemo::Status s = g_pika_server->db()->HMSet(key_, fv_v_);
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
  nemo::Status s = g_pika_server->db()->HSetnx(key_, field_, value_);
  if (s.ok()) {
    res_.AppendContent(":1");
    SlotKeyAdd("h", key_);
  } else if (s.IsCorruption() && s.ToString() == "Corruption: Already Exist") {
    res_.AppendContent(":0");
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
  int64_t len = g_pika_server->db()->HStrlen(key_, field_);
  if (len >= 0) {
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
  std::vector<std::string> vals;
  nemo::Status s = g_pika_server->db()->HVals(key_, vals);
  if (s.ok()) {
    res_.AppendArrayLen(vals.size());
    std::vector<std::string>::const_iterator iter = vals.begin();
    for (; iter != vals.end(); iter++) {
      res_.AppendStringLen(iter->size());
      res_.AppendContent(*iter);
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
    if (use_pattern && !slash::stringmatchlen(pattern_.data(), pattern_.size(), iter->field().data(), iter->field().size(), 0)) {
      continue;
    } 
    count_--;
    cursor_++;
    fv_v.push_back({iter->field(), iter->value()});
  }
  if (fv_v.size() <= 0 || !(iter->Valid())) {
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




