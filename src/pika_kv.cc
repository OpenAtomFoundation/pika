// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "slash/include/slash_string.h"
#include "include/pika_kv.h"
#include "include/pika_server.h"

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
  rocksdb::Status s;
  int32_t res = 1;
  switch (condition_) {
    case SetCmd::kXX:
      s = g_pika_server->bdb()->Setxx(key_, value_, &res, sec_);
      break;
    case SetCmd::kNX:
      s = g_pika_server->bdb()->Setnx(key_, value_, &res, sec_);
      break;
    default:
      s = g_pika_server->bdb()->Set(key_, value_, sec_);
      break;
  }

  if (s.ok() || s.IsNotFound()) {
    if (res == 1) {
      res_.SetRes(CmdRes::kOk);
    } else {
      res_.AppendArrayLen(-1);;
    }
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
  }

  std::map<blackwidow::DataType, blackwidow::Status> type_status;
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
  rocksdb::Status s = g_pika_server->bdb()->Keys(type_, pattern_, &keys);
  res_.AppendArrayLen(keys.size());
  for (const auto& key : keys) {
    res_.AppendString(key);
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

  std::vector<blackwidow::KeyValue>::const_iterator it;
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

    std::vector<blackwidow::KeyValue>::const_iterator it;
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
  std::map<blackwidow::DataType, rocksdb::Status> type_status;
  int64_t res = g_pika_server->bdb()->Exists(keys_, &type_status);
  if (res != -1) {
    res_.AppendInteger(res);
  } else {
    res_.SetRes(CmdRes::kErrOther, "exists internal error");
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
  std::map<blackwidow::DataType, rocksdb::Status> type_status;
  int64_t res = g_pika_server->bdb()->Expire(key_, sec_, &type_status);
  if (res != -1) {
    res_.AppendInteger(res);
  } else {
    res_.SetRes(CmdRes::kErrOther, "expire internal error");
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
  std::map<blackwidow::DataType, rocksdb::Status> type_status;
  int64_t res = g_pika_server->bdb()->Expire(key_, msec_/1000, &type_status);
  if (res != -1) {
    res_.AppendInteger(res);
  } else {
    res_.SetRes(CmdRes::kErrOther, "expire internal error");
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
  std::map<blackwidow::DataType, rocksdb::Status> type_status;
  int32_t res = g_pika_server->bdb()->Expireat(key_, time_stamp_, &type_status);
  if (res != -1) {
    res_.AppendInteger(res);
  } else {
    res_.SetRes(CmdRes::kErrOther, "expireat internal error");
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
  std::map<blackwidow::DataType, rocksdb::Status> type_status;
  int32_t res = g_pika_server->bdb()->Expireat(key_, time_stamp_ms_/1000, &type_status);
  if (res != -1) {
    res_.AppendInteger(res);
  } else {
    res_.SetRes(CmdRes::kErrOther, "pexpireat internal error");
  }
  return;
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
  std::map<blackwidow::DataType, int64_t> type_timestamp;
  std::map<blackwidow::DataType, rocksdb::Status> type_status;
  type_timestamp = g_pika_server->bdb()->TTL(key_, &type_status);
  for (const auto& item : type_timestamp) {
     // mean operation exception errors happen in database
     if (item.second == -3) {
       res_.SetRes(CmdRes::kErrOther, "ttl internal error");
       return;
     }
  }
  if (type_timestamp[blackwidow::kStrings] != -2) {
    res_.AppendInteger(type_timestamp[blackwidow::kStrings]);
  } else if (type_timestamp[blackwidow::kHashes] != -2) {
    res_.AppendInteger(type_timestamp[blackwidow::kHashes]);
  } else if (type_timestamp[blackwidow::kLists] != -2) {
    res_.AppendInteger(type_timestamp[blackwidow::kLists]);
  } else if (type_timestamp[blackwidow::kSets] != -2) {
    res_.AppendInteger(type_timestamp[blackwidow::kSets]);
  } else if (type_timestamp[blackwidow::kZSets] != -2) {
    res_.AppendInteger(type_timestamp[blackwidow::kZSets]);
  } else {
    // mean this key not exist
    res_.AppendInteger(-2);
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
  std::map<blackwidow::DataType, int64_t> type_timestamp;
  std::map<blackwidow::DataType, rocksdb::Status> type_status;
  type_timestamp = g_pika_server->bdb()->TTL(key_, &type_status);
  for (const auto& item : type_timestamp) {
     // mean operation exception errors happen in database
     if (item.second == -3) {
       res_.SetRes(CmdRes::kErrOther, "ttl internal error");
       return;
     }
  }
  if (type_timestamp[blackwidow::kStrings] != -2) {
    if (type_timestamp[blackwidow::kStrings] == -1) {
      res_.AppendInteger(-1);
    } else {
      res_.AppendInteger(type_timestamp[blackwidow::kStrings] * 1000);
    }
  } else if (type_timestamp[blackwidow::kHashes] != -2) {
    if (type_timestamp[blackwidow::kHashes] == -1) {
      res_.AppendInteger(-1);
    } else {
      res_.AppendInteger(type_timestamp[blackwidow::kHashes] * 1000);
    }
  } else if (type_timestamp[blackwidow::kLists] != -2) {
    if (type_timestamp[blackwidow::kLists] == -1) {
      res_.AppendInteger(-1);
    } else {
      res_.AppendInteger(type_timestamp[blackwidow::kLists] * 1000);
    }
  } else if (type_timestamp[blackwidow::kSets] != -2) {
    if (type_timestamp[blackwidow::kSets] == -1) {
      res_.AppendInteger(-1);
    } else {
      res_.AppendInteger(type_timestamp[blackwidow::kSets] * 1000);
    }
  } else if (type_timestamp[blackwidow::kZSets] != -2) {
    if (type_timestamp[blackwidow::kZSets] == -1) {
      res_.AppendInteger(-1);
    } else {
      res_.AppendInteger(type_timestamp[blackwidow::kZSets] * 1000);
    }
  } else {
    // mean this key not exist
    res_.AppendInteger(-2);
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
  std::map<blackwidow::DataType, rocksdb::Status> type_status;
  int32_t res = g_pika_server->bdb()->Persist(key_, &type_status);
  if (res != -1) {
    res_.AppendInteger(res);
  } else {
    res_.SetRes(CmdRes::kErrOther, "persist internal error");
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
  rocksdb::Status s = g_pika_server->bdb()->Type(key_, &res);
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
  int64_t cursor_ret = g_pika_server->bdb()->Scan(cursor_, pattern_, count_, &keys);
  
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
