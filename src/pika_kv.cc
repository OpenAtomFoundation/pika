// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "slash/include/slash_string.h"
#include "nemo.h"
#include "include/pika_kv.h"
#include "include/pika_server.h"
#include "include/pika_slot.h"

extern PikaServer *g_pika_server;

void SetCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSet);
    return;
  }
  key_ = argv[1];
  value_ = argv[2];
  condition_ = SetCmd::kANY;
  sec_ = 0;
  size_t index = 3;
  while (index != argv.size()) {
    std::string opt = slash::StringToLower(argv[index]);
    if (opt == "xx") {
      condition_ = SetCmd::kXX;
    } else if (opt == "nx") {
      condition_ = SetCmd::kNX;
    } else if (opt == "ex" || opt == "px") {
      index++;
      if (index == argv.size()) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      if (!slash::string2l(argv[index].data(), argv[index].size(), &sec_)) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
      }
      if (opt == "px") {
        sec_ /= 1000;
      }
    } else {
      res_.SetRes(CmdRes::kSyntaxErr);
      return;
    }
    index++;
  }
  return;
}

void SetCmd::Do() {
  nemo::Status s;
  int32_t res = 1;
  switch (condition_) {
    case SetCmd::kXX:
      s = g_pika_server->bdb()->Setxx(key_, value_, &res);
      break;
    case SetCmd::kNX:
      s = g_pika_server->bdb()->Setnx(key_, value_, &res);
      break;
    default:
      s = g_pika_server->bdb()->Set(key_, value_);
      break;
  }

  if (s.ok() || s.IsNotFound()) {
    if (res == 1) {
      res_.SetRes(CmdRes::kOk);
    } else {
      res_.AppendArrayLen(-1);;
    }
    SlotKeyAdd("k", key_);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void GetCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameGet);
    return;
  }
  key_ = argv[1];
  return;
}

void GetCmd::Do() {
  std::string value;
  rocksdb::Status s = g_pika_server->bdb()->Get(key_, &value);
  if (s.ok()) {
    res_.AppendStringLen(value.size());
    res_.AppendContent(value);
  } else if (s.IsNotFound()) {
    res_.AppendStringLen(-1);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void DelCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameDel);
    return;
  }
  std::vector<std::string>::iterator iter = argv.begin();
  keys_.assign(++iter, argv.end());
  return;
}

void DelCmd::Do() {
  std::vector<std::string>::const_iterator it;
  for (it = keys_.begin(); it != keys_.end(); it++) {
    SlotKeyRem(*it);
  }

  std::map<blackwidow::BlackWidow::DataType, blackwidow::Status> type_status;
  int64_t count = g_pika_server->bdb()->Del(keys_, &type_status);
  if (count >= 0) {
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, "delete error");
  }
  return;
}

void IncrCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameIncr);
    return;
  }
  key_ = argv[1];
  return;
}

void IncrCmd::Do() {
  rocksdb::Status s = g_pika_server->bdb()->Incrby(key_, 1, &new_value_);
  if (s.ok()) {
   res_.AppendContent(":" + std::to_string(new_value_));
   SlotKeyAdd("k", key_);
  } else if (s.IsCorruption() && s.ToString() == "Corruption: Value is not a integer") {
    res_.SetRes(CmdRes::kInvalidInt);
  } else if (s.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kOverFlow);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

std::string IncrCmd::ToBinlog(
    const PikaCmdArgsType& argv,
    const std::string& server_id,
    const std::string& binlog_info,
    bool need_send_to_hub) {
  std::string res;
  res.reserve(RAW_ARGS_LEN);
  RedisAppendLen(res, 3 + 4, "*");

  // to set cmd
  std::string set_cmd("set");
  RedisAppendLen(res, set_cmd.size(), "$");
  RedisAppendContent(res, set_cmd);
  // key
  RedisAppendLen(res, key_.size(), "$");
  RedisAppendContent(res, key_);
  // value
  std::string value = std::to_string(new_value_);
  RedisAppendLen(res, value.size(), "$");
  RedisAppendContent(res, value);

  AppendAffiliatedInfo(res, server_id, binlog_info, need_send_to_hub);
  return res;
}

void IncrbyCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameIncrby);
    return;
  }
  key_ = argv[1];
  if (!slash::string2l(argv[2].data(), argv[2].size(), &by_)) {
    res_.SetRes(CmdRes::kInvalidInt, kCmdNameIncrby);
    return;
  }
  return;
}

void IncrbyCmd::Do() {
  rocksdb::Status s = g_pika_server->bdb()->Incrby(key_, by_, &new_value_);
  if (s.ok()) {
    res_.AppendContent(":" + std::to_string(new_value_));
    SlotKeyAdd("k", key_);
  } else if (s.IsCorruption() && s.ToString() == "Corruption: Value is not a integer") {
    res_.SetRes(CmdRes::kInvalidInt);
  } else if (s.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kOverFlow);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

std::string IncrbyCmd::ToBinlog(
    const PikaCmdArgsType& argv,
    const std::string& server_id,
    const std::string& binlog_info,
    bool need_send_to_hub) {
  std::string res;
  res.reserve(RAW_ARGS_LEN);
  RedisAppendLen(res, 3 + 4, "*");

  // to set cmd
  std::string set_cmd("set");
  RedisAppendLen(res, set_cmd.size(), "$");
  RedisAppendContent(res, set_cmd);
  // key
  RedisAppendLen(res, key_.size(), "$");
  RedisAppendContent(res, key_);
  // value
  std::string value = std::to_string(new_value_);
  RedisAppendLen(res, value.size(), "$");
  RedisAppendContent(res, value);

  AppendAffiliatedInfo(res, server_id, binlog_info, need_send_to_hub);
  return res;
}

void IncrbyfloatCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameIncrbyfloat);
    return;
  }
  key_ = argv[1];
  value_ = argv[2];
  if (!slash::string2d(argv[2].data(), argv[2].size(), &by_)) {
    res_.SetRes(CmdRes::kInvalidFloat);
    return;
  }
  return;
}

void IncrbyfloatCmd::Do() {
  rocksdb::Status s = g_pika_server->bdb()->Incrbyfloat(key_, value_, &new_value_);
  if (s.ok()) {
    res_.AppendStringLen(new_value_.size());
    res_.AppendContent(new_value_);
    SlotKeyAdd("k", key_);
  } else if (s.IsCorruption() && s.ToString() == "Corruption: Value is not a vaild float"){
    res_.SetRes(CmdRes::kInvalidFloat);
  } else if (s.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kOverFlow);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

std::string IncrbyfloatCmd::ToBinlog(
    const PikaCmdArgsType& argv,
    const std::string& server_id,
    const std::string& binlog_info,
    bool need_send_to_hub) {
  std::string res;
  res.reserve(RAW_ARGS_LEN);
  RedisAppendLen(res, 3 + 4, "*");

  // to set cmd
  std::string set_cmd("set");
  RedisAppendLen(res, set_cmd.size(), "$");
  RedisAppendContent(res, set_cmd);
  // key
  RedisAppendLen(res, key_.size(), "$");
  RedisAppendContent(res, key_);
  // value
  RedisAppendLen(res, new_value_.size(), "$");
  RedisAppendContent(res, new_value_);

  AppendAffiliatedInfo(res, server_id, binlog_info, need_send_to_hub);
  return res;
}

void DecrCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameDecr);
    return;
  }
  key_ = argv[1];
  return;
}

void DecrCmd::Do() {
  rocksdb::Status s = g_pika_server->bdb()->Decrby(key_, 1, &new_value_);
  if (s.ok()) {
   res_.AppendContent(":" + std::to_string(new_value_));
  } else if (s.IsCorruption() && s.ToString() == "Corruption: Value is not a integer") {
    res_.SetRes(CmdRes::kInvalidInt);
  } else if (s.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kOverFlow);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

std::string DecrCmd::ToBinlog(
    const PikaCmdArgsType& argv,
    const std::string& server_id,
    const std::string& binlog_info,
    bool need_send_to_hub) {
  std::string res;
  res.reserve(RAW_ARGS_LEN);
  RedisAppendLen(res, 3 + 4, "*");

  // to set cmd
  std::string set_cmd("set");
  RedisAppendLen(res, set_cmd.size(), "$");
  RedisAppendContent(res, set_cmd);
  // key
  RedisAppendLen(res, key_.size(), "$");
  RedisAppendContent(res, key_);
  // value
  std::string value = std::to_string(new_value_);
  RedisAppendLen(res, value.size(), "$");
  RedisAppendContent(res, value);

  AppendAffiliatedInfo(res, server_id, binlog_info, need_send_to_hub);
  return res;
}

void DecrbyCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameDecrby);
    return;
  }
  key_ = argv[1];
  if (!slash::string2l(argv[2].data(), argv[2].size(), &by_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  return;
}

void DecrbyCmd::Do() {
  rocksdb::Status s = g_pika_server->bdb()->Decrby(key_, by_, &new_value_);
  if (s.ok()) {
    res_.AppendContent(":" + std::to_string(new_value_));
  } else if (s.IsCorruption() && s.ToString() == "Corruption: Value is not a integer") {
    res_.SetRes(CmdRes::kInvalidInt);
  } else if (s.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kOverFlow);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

std::string DecrbyCmd::ToBinlog(
    const PikaCmdArgsType& argv,
    const std::string& server_id,
    const std::string& binlog_info,
    bool need_send_to_hub) {
  std::string res;
  res.reserve(RAW_ARGS_LEN);
  RedisAppendLen(res, 3 + 4, "*");

  // to set cmd
  std::string set_cmd("set");
  RedisAppendLen(res, set_cmd.size(), "$");
  RedisAppendContent(res, set_cmd);
  // key
  RedisAppendLen(res, key_.size(), "$");
  RedisAppendContent(res, key_);
  // value
  std::string value = std::to_string(new_value_);
  RedisAppendLen(res, value.size(), "$");
  RedisAppendContent(res, value);

  AppendAffiliatedInfo(res, server_id, binlog_info, need_send_to_hub);
  return res;
}

void GetsetCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameGetset);
    return;
  }
  key_ = argv[1];
  new_value_ = argv[2];
  return;
}

void GetsetCmd::Do() {
  std::string old_value;
  rocksdb::Status s = g_pika_server->bdb()->GetSet(key_, new_value_, &old_value);
  if (s.ok()) {
    if (old_value.empty()) {
      res_.AppendContent("$-1");
    } else {
      res_.AppendStringLen(old_value.size());
      res_.AppendContent(old_value);
    }
    SlotKeyAdd("k", key_);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void AppendCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameAppend);
    return;
  }
  key_ = argv[1];
  value_ = argv[2];
  return;
}

void AppendCmd::Do() {
  int32_t new_len = 0;
  rocksdb::Status s = g_pika_server->bdb()->Append(key_, value_, &new_len);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(new_len);
    SlotKeyAdd("k", key_);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void MgetCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameMget);
    return;
  }
  keys_ = argv;
  keys_.erase(keys_.begin());
  return;
}

void MgetCmd::Do() {
  std::vector<std::string> values;
  rocksdb::Status s = g_pika_server->bdb()->MGet(keys_, &values);
  res_.AppendArrayLen(values.size());
  for (const auto& value : values) {
    if (!value.empty()) {
      res_.AppendStringLen(value.size());
      res_.AppendContent(value);
    } else {
      res_.AppendContent("$-1");
    }
  }
  return;
}

void KeysCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameKeys);
    return;
  }
  pattern_ = argv[1];
  if (argv.size() == 3) {
    std::string opt = slash::StringToLower(argv[2]);
    if (opt == "string" || opt == "zset" || opt == "set" || opt == "list" || opt == "hash") {
        type_ = opt;
    } else {
      res_.SetRes(CmdRes::kSyntaxErr);
    }
  } else if (argv.size() > 3) {
    res_.SetRes(CmdRes::kSyntaxErr);
  }
  return;
}

void KeysCmd::Do() {
  std::vector<std::string> keys;
  nemo::Status s = g_pika_server->db()->Keys(pattern_, keys, type_);
  res_.AppendArrayLen(keys.size());
  for (std::vector<std::string>::iterator iter = keys.begin(); iter != keys.end(); iter++) {
    res_.AppendStringLen(iter->size());
    res_.AppendContent(*iter);
  }
  return;
}

void SetnxCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSetnx);
    return;
  }
  key_ = argv[1];
  value_ = argv[2];
  return;
}

void SetnxCmd::Do() {
  success_ = 0;
  rocksdb::Status s = g_pika_server->bdb()->Setnx(key_, value_, &success_);
  if (s.ok()) {
    res_.AppendInteger(success_);
    SlotKeyAdd("k", key_);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

std::string SetnxCmd::ToBinlog(
    const PikaCmdArgsType& argv,
    const std::string& server_id,
    const std::string& binlog_info,
    bool need_send_to_hub) {
  std::string res;
  if (success_) {
    res.reserve(RAW_ARGS_LEN);
    RedisAppendLen(res, 3 + 4, "*");

    // to set cmd
    std::string set_cmd("set");
    RedisAppendLen(res, set_cmd.size(), "$");
    RedisAppendContent(res, set_cmd);
    // key
    RedisAppendLen(res, key_.size(), "$");
    RedisAppendContent(res, key_);
    // value
    RedisAppendLen(res, value_.size(), "$");
    RedisAppendContent(res, value_);

    AppendAffiliatedInfo(res, server_id, binlog_info, need_send_to_hub);
  }
  return res;
}

void SetexCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSetex);
    return;
  }
  key_ = argv[1];
  if (!slash::string2l(argv[2].data(), argv[2].size(), &sec_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  value_ = argv[3];
  return;
}

void SetexCmd::Do() {
  rocksdb::Status s = g_pika_server->bdb()->Setex(key_, value_, sec_);
  if (s.ok()) {
    res_.SetRes(CmdRes::kOk);
    SlotKeyAdd("k", key_);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void MsetCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameMset);
    return;
  }
  size_t argc = argv.size();
  if (argc % 2 == 0) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameMset);
    return;
  }
  kvs_.clear();
  for (size_t index = 1; index != argc; index += 2) {
    kvs_.push_back({argv[index], argv[index + 1]});
  }
  return;
}

void MsetCmd::Do() {
  blackwidow::Status s = g_pika_server->bdb()->MSet(kvs_);
  if (s.ok()) {
    res_.SetRes(CmdRes::kOk);
    std::vector<blackwidow::BlackWidow::KeyValue>::const_iterator it;
    for (it = kvs_.begin(); it != kvs_.end(); it++) {
      SlotKeyAdd("k", it->key);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

std::string MsetCmd::ToBinlog(
    const PikaCmdArgsType& argv,
    const std::string& server_id,
    const std::string& binlog_info,
    bool need_send_to_hub) {
  std::string res;
  res.reserve(RAW_ARGS_LEN);

  std::vector<blackwidow::BlackWidow::KeyValue>::const_iterator it;
  for (it = kvs_.begin(); it != kvs_.end(); it++) {
    RedisAppendLen(res, 3 + 4, "*");

    // to set cmd
    std::string set_cmd("set");
    RedisAppendLen(res, set_cmd.size(), "$");
    RedisAppendContent(res, set_cmd);
    // key
    RedisAppendLen(res, it->key.size(), "$");
    RedisAppendContent(res, it->key);
    // value
    RedisAppendLen(res, it->value.size(), "$");
    RedisAppendContent(res, it->value);

    AppendAffiliatedInfo(res, server_id, binlog_info, need_send_to_hub);
  }
  return res;
}

void MsetnxCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameMsetnx);
    return;
  }
  size_t argc = argv.size();
  if (argc % 2 == 0) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameMsetnx);
    return;
  }
  kvs_.clear();
  for (size_t index = 1; index != argc; index += 2) {
    kvs_.push_back({argv[index], argv[index + 1]});
  }
  return;
}

void MsetnxCmd::Do() {
  success_ = 0;
  rocksdb::Status s = g_pika_server->bdb()->MSetnx(kvs_, &success_);
  if (s.ok()) {
    res_.AppendInteger(success_);
    std::vector<blackwidow::BlackWidow::KeyValue>::const_iterator it;
    for (it = kvs_.begin(); it != kvs_.end(); it++) {
      SlotKeyAdd("k", it->key);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

std::string MsetnxCmd::ToBinlog(
    const PikaCmdArgsType& argv,
    const std::string& server_id,
    const std::string& binlog_info,
    bool need_send_to_hub) {
  std::string res;
  if (success_) {
    res.reserve(RAW_ARGS_LEN);

    std::vector<blackwidow::BlackWidow::KeyValue>::const_iterator it;
    for (it = kvs_.begin(); it != kvs_.end(); it++) {
      RedisAppendLen(res, 3 + 4, "*");

      // to set cmd
      std::string set_cmd("set");
      RedisAppendLen(res, set_cmd.size(), "$");
      RedisAppendContent(res, set_cmd);
      // key
      RedisAppendLen(res, it->key.size(), "$");
      RedisAppendContent(res, it->key);
      // value
      RedisAppendLen(res, it->value.size(), "$");
      RedisAppendContent(res, it->value);

      AppendAffiliatedInfo(res, server_id, binlog_info, need_send_to_hub);
    }
  }
  return res;
}

void GetrangeCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameGetrange);
    return;
  }
  key_ = argv[1];
  if (!slash::string2l(argv[2].data(), argv[2].size(), &start_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  if (!slash::string2l(argv[3].data(), argv[3].size(), &end_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  return;
}

void GetrangeCmd::Do() {
  std::string substr;
  rocksdb::Status s = g_pika_server->bdb()->Getrange(key_, start_, end_, &substr);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendStringLen(substr.size());
    res_.AppendContent(substr);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void SetrangeCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSetrange);
    return;
  }
  key_ = argv[1];
  if (!slash::string2l(argv[2].data(), argv[2].size(), &offset_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  value_ = argv[3];
  return;
}

void SetrangeCmd::Do() {
  int32_t new_len;
  rocksdb::Status s = g_pika_server->bdb()->Setrange(key_, offset_, value_, &new_len);
  if (s.ok()) {
    res_.AppendInteger(new_len);
    SlotKeyAdd("k", key_);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void StrlenCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameStrlen);
    return;
  }
  key_ = argv[1];
  return;
}

void StrlenCmd::Do() {
  int32_t len = 0;
  rocksdb::Status s = g_pika_server->bdb()->Strlen(key_, &len);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(len);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void ExistsCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameExists);
    return;
  }
  keys_ = argv;
  keys_.erase(keys_.begin());
  return;
}

void ExistsCmd::Do() {
  int64_t res;

  nemo::Status s = g_pika_server->db()->Exists(keys_, &res);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(res);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void ExpireCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameExpire);
    return;
  }
  key_ = argv[1];
  if (!slash::string2l(argv[2].data(), argv[2].size(), &sec_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  return;
}

void ExpireCmd::Do() {
  int64_t res = 0;
  nemo::Status s = g_pika_server->db()->Expire(key_, sec_, &res);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(res);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

std::string ExpireCmd::ToBinlog(
    const PikaCmdArgsType& argv,
    const std::string& server_id,
    const std::string& binlog_info,
    bool need_send_to_hub) {
  std::string res;
  res.reserve(RAW_ARGS_LEN);
  RedisAppendLen(res, argv.size() + 4, "*");

  // to expireat cmd
  std::string expireat_cmd("expireat");
  RedisAppendLen(res, expireat_cmd.size(), "$");
  RedisAppendContent(res, expireat_cmd);
  // key
  RedisAppendLen(res, key_.size(), "$");
  RedisAppendContent(res, key_);
  // sec
  char buf[100];
  int64_t expireat = time(nullptr) + sec_;
  slash::ll2string(buf, 100, expireat);
  std::string at(buf);
  RedisAppendLen(res, at.size(), "$");
  RedisAppendContent(res, at);

  AppendAffiliatedInfo(res, server_id, binlog_info, need_send_to_hub);
  return res;
}

void PexpireCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePexpire);
    return;
  }
  key_ = argv[1];
  if (!slash::string2l(argv[2].data(), argv[2].size(), &msec_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  return;
}

void PexpireCmd::Do() {
  int64_t res = 0;
  nemo::Status s = g_pika_server->db()->Expire(key_, msec_/1000, &res);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(res);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

std::string PexpireCmd::ToBinlog(
    const PikaCmdArgsType& argv,
    const std::string& server_id,
    const std::string& binlog_info,
    bool need_send_to_hub) {
  std::string res;
  res.reserve(RAW_ARGS_LEN);
  RedisAppendLen(res, argv.size() + 4, "*");

  // to expireat cmd
  std::string expireat_cmd("expireat");
  RedisAppendLen(res, expireat_cmd.size(), "$");
  RedisAppendContent(res, expireat_cmd);
  // key
  RedisAppendLen(res, key_.size(), "$");
  RedisAppendContent(res, key_);
  // sec
  char buf[100];
  int64_t expireat = time(nullptr) + msec_ / 1000;
  slash::ll2string(buf, 100, expireat);
  std::string at(buf);
  RedisAppendLen(res, at.size(), "$");
  RedisAppendContent(res, at);

  AppendAffiliatedInfo(res, server_id, binlog_info, need_send_to_hub);
  return res;
}

void ExpireatCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameExpireat);
    return;
  }
  key_ = argv[1];
  if (!slash::string2l(argv[2].data(), argv[2].size(), &time_stamp_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  return;
}

void ExpireatCmd::Do() {
  int64_t res = 0;
  nemo::Status s = g_pika_server->db()->Expireat(key_, time_stamp_, &res);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(res);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void PexpireatCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePexpireat);
    return;
  }
  key_ = argv[1];
  if (!slash::string2l(argv[2].data(), argv[2].size(), &time_stamp_ms_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  return;
}

std::string PexpireatCmd::ToBinlog(
    const PikaCmdArgsType& argv,
    const std::string& server_id,
    const std::string& binlog_info,
    bool need_send_to_hub) {
  std::string res;
  res.reserve(RAW_ARGS_LEN);
  RedisAppendLen(res, argv.size() + 4, "*");

  // to expireat cmd
  std::string expireat_cmd("expireat");
  RedisAppendLen(res, expireat_cmd.size(), "$");
  RedisAppendContent(res, expireat_cmd);
  // key
  RedisAppendLen(res, key_.size(), "$");
  RedisAppendContent(res, key_);
  // sec
  char buf[100];
  int64_t expireat = time_stamp_ms_ / 1000;
  slash::ll2string(buf, 100, expireat);
  std::string at(buf);
  RedisAppendLen(res, at.size(), "$");
  RedisAppendContent(res, at);

  AppendAffiliatedInfo(res, server_id, binlog_info, need_send_to_hub);
  return res;
}

void PexpireatCmd::Do() {
  int64_t res = 0;
  nemo::Status s = g_pika_server->db()->Expireat(key_, time_stamp_ms_/1000, &res);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(res);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void TtlCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameTtl);
    return;
  }
  key_ = argv[1];
  return;
}

void TtlCmd::Do() {
  int64_t ttl = 0;
  nemo::Status s = g_pika_server->db()->TTL(key_, &ttl);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(ttl);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void PttlCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePttl);
    return;
  }
  key_ = argv[1];
  return;
}

void PttlCmd::Do() {
  int64_t ttl = 0;
  nemo::Status s = g_pika_server->db()->TTL(key_, &ttl);
  if (ttl > 0) {
    ttl *= 1000;
  }
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(ttl);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void PersistCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePersist);
    return;
  }
  key_ = argv[1];
  return;
}

void PersistCmd::Do() {
  int64_t res = 0;
  nemo::Status s = g_pika_server->db()->Persist(key_, &res);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(res);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void TypeCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameType);
    return;
  }
  key_ = argv[1];
  return;
}

void TypeCmd::Do() {
  std::string res;
  nemo::Status s = g_pika_server->db()->Type(key_, &res);
  if (s.ok()) {
    res_.AppendContent("+" + res);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void ScanCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameScan);
    return;
  }
  if (!slash::string2l(argv[1].data(), argv[1].size(), &cursor_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  size_t index = 2, argc = argv.size();

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
      } else if (!slash::string2l(argv[index].data(), argv[index].size(), &count_) || count_ <= 0) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
      }
    } else {
      res_.SetRes(CmdRes::kSyntaxErr);
      return;
    }
    index++;
  }
  return;
}

void ScanCmd::Do() {
  std::vector<std::string> keys;
  int64_t cursor_ret = 0;
  nemo::Status s = g_pika_server->db()->Scan(cursor_, pattern_, count_, keys, &cursor_ret);
  
  res_.AppendArrayLen(2);

  char buf[32];
  int len = slash::ll2string(buf, sizeof(buf), cursor_ret);
  res_.AppendStringLen(len);
  res_.AppendContent(buf);

  res_.AppendArrayLen(keys.size());
  std::vector<std::string>::const_iterator iter;
  for (iter = keys.begin(); iter != keys.end(); iter++) {
    res_.AppendStringLen(iter->size());
    res_.AppendContent(*iter);
  }
  return;
}
