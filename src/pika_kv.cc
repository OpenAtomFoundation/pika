// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "slash/include/slash_string.h"
#include "include/pika_kv.h"
#include "include/pika_server.h"
#include "include/pika_slot.h"

extern PikaServer *g_pika_server;

/* SET key value [NX] [XX] [EX <seconds>] [PX <milliseconds>] */
void SetCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSet);
    return;
  }
  key_ = argv[1];
  value_ = argv[2];
  condition_ = SetCmd::kNONE;
  sec_ = 0;
  size_t index = 3;
  while (index != argv.size()) {
    std::string opt = argv[index];
    if (!strcasecmp(opt.data(), "xx")) {
      condition_ = SetCmd::kXX;
    } else if (!strcasecmp(opt.data(), "nx")) {
      condition_ = SetCmd::kNX;
    } else if (!strcasecmp(opt.data(), "vx")) {
      condition_ = SetCmd::kVX;
      index++;
      if (index == argv.size()) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      } else {
        target_ = argv[index];
      }
    } else if (!strcasecmp(opt.data(), "ex") || !strcasecmp(opt.data(), "px")) {
      condition_ = (condition_ == SetCmd::kNONE) ? SetCmd::kEXORPX : condition_;
      index++;
      if (index == argv.size()) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      if (!slash::string2l(argv[index].data(), argv[index].size(), &sec_)) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
      } else if (sec_ <= 0) {
        res_.SetRes(CmdRes::kErrOther, "invalid expire time in set");
        return;
      }

      if (!strcasecmp(opt.data(), "px")) {
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
      s = g_pika_server->db()->Setxx(key_, value_, &res, sec_);
      break;
    case SetCmd::kNX:
      s = g_pika_server->db()->Setnx(key_, value_, &res, sec_);
      break;
    case SetCmd::kVX:
      s = g_pika_server->db()->Setvx(key_, target_, value_, &success_, sec_);
      break;
    case SetCmd::kEXORPX:
      s = g_pika_server->db()->Setex(key_, value_, sec_);
      break;
    default:
      s = g_pika_server->db()->Set(key_, value_);
      break;
  }

  if (s.ok() || s.IsNotFound()) {
    if (condition_ == SetCmd::kVX) {
      res_.AppendInteger(success_);
    } else {
      if (res == 1) {
        res_.SetRes(CmdRes::kOk);
      } else {
        res_.AppendArrayLen(-1);;
      }
    }
    SlotKeyAdd("k", key_);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

std::string SetCmd::ToBinlog(
      const PikaCmdArgsType& argv,
      uint32_t exec_time,
      const std::string& server_id,
      uint64_t logic_id,
      uint32_t filenum,
      uint64_t offset) {
  if (condition_ == SetCmd::kEXORPX) {
    std::string content;
    content.reserve(RAW_ARGS_LEN);
    RedisAppendLen(content, 4, "*");

    // to pksetexat cmd
    std::string pksetexat_cmd("pksetexat");
    RedisAppendLen(content, pksetexat_cmd.size(), "$");
    RedisAppendContent(content, pksetexat_cmd);
    // key
    RedisAppendLen(content, key_.size(), "$");
    RedisAppendContent(content, key_);
    // time_stamp
    char buf[100];
    int32_t time_stamp = time(nullptr) + sec_;
    slash::ll2string(buf, 100, time_stamp);
    std::string at(buf);
    RedisAppendLen(content, at.size(), "$");
    RedisAppendContent(content, at);
    // value
    RedisAppendLen(content, value_.size(), "$");
    RedisAppendContent(content, value_);
    return PikaBinlogTransverter::BinlogEncode(BinlogType::TypeFirst,
                                               exec_time,
                                               std::stoi(server_id),
                                               logic_id,
                                               filenum,
                                               offset,
                                               content,
                                               {});
  } else {
    return Cmd::ToBinlog(argv, exec_time, server_id, logic_id, filenum, offset);
  }
}

void GetCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameGet);
    return;
  }
  key_ = argv[1];
  return;
}

void GetCmd::Do() {
  std::string value;
  rocksdb::Status s = g_pika_server->db()->Get(key_, &value);
  if (s.ok()) {
    res_.AppendStringLen(value.size());
    res_.AppendContent(value);
  } else if (s.IsNotFound()) {
    res_.AppendStringLen(-1);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void DelCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameDel);
    return;
  }
  std::vector<std::string>::const_iterator iter = argv.begin();
  keys_.assign(++iter, argv.end());
  return;
}

void DelCmd::Do() {
  std::vector<std::string>::const_iterator it;
  for (it = keys_.begin(); it != keys_.end(); it++) {
    SlotKeyRem(*it);
  }

  std::map<blackwidow::DataType, blackwidow::Status> type_status;
  int64_t count = g_pika_server->db()->Del(keys_, &type_status);
  if (count >= 0) {
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, "delete error");
  }
  return;
}

void IncrCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameIncr);
    return;
  }
  key_ = argv[1];
  return;
}

void IncrCmd::Do() {
  rocksdb::Status s = g_pika_server->db()->Incrby(key_, 1, &new_value_);
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
      uint32_t exec_time,
      const std::string& server_id,
      uint64_t logic_id,
      uint32_t filenum,
      uint64_t offset) {
  std::string content;
  content.reserve(RAW_ARGS_LEN);
  RedisAppendLen(content, 3, "*");

  // to set cmd
  std::string set_cmd("set");
  RedisAppendLen(content, set_cmd.size(), "$");
  RedisAppendContent(content, set_cmd);
  // key
  RedisAppendLen(content, key_.size(), "$");
  RedisAppendContent(content, key_);
  // value
  std::string value = std::to_string(new_value_);
  RedisAppendLen(content, value.size(), "$");
  RedisAppendContent(content, value);

  return PikaBinlogTransverter::BinlogEncode(BinlogType::TypeFirst,
                                             exec_time,
                                             std::stoi(server_id),
                                             logic_id,
                                             filenum,
                                             offset,
                                             content,
                                             {});
}

void IncrbyCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
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
  rocksdb::Status s = g_pika_server->db()->Incrby(key_, by_, &new_value_);
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
      uint32_t exec_time,
      const std::string& server_id,
      uint64_t logic_id,
      uint32_t filenum,
      uint64_t offset) {
  std::string content;
  content.reserve(RAW_ARGS_LEN);
  RedisAppendLen(content, 3, "*");

  // to set cmd
  std::string set_cmd("set");
  RedisAppendLen(content, set_cmd.size(), "$");
  RedisAppendContent(content, set_cmd);
  // key
  RedisAppendLen(content, key_.size(), "$");
  RedisAppendContent(content, key_);
  // value
  std::string value = std::to_string(new_value_);
  RedisAppendLen(content, value.size(), "$");
  RedisAppendContent(content, value);

  return PikaBinlogTransverter::BinlogEncode(BinlogType::TypeFirst,
                                             exec_time,
                                             std::stoi(server_id),
                                             logic_id,
                                             filenum,
                                             offset,
                                             content,
                                             {});
}

void IncrbyfloatCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
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
  rocksdb::Status s = g_pika_server->db()->Incrbyfloat(key_, value_, &new_value_);
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
      uint32_t exec_time,
      const std::string& server_id,
      uint64_t logic_id,
      uint32_t filenum,
      uint64_t offset) {
  std::string content;
  content.reserve(RAW_ARGS_LEN);
  RedisAppendLen(content, 3, "*");

  // to set cmd
  std::string set_cmd("set");
  RedisAppendLen(content, set_cmd.size(), "$");
  RedisAppendContent(content, set_cmd);
  // key
  RedisAppendLen(content, key_.size(), "$");
  RedisAppendContent(content, key_);
  // value
  RedisAppendLen(content, new_value_.size(), "$");
  RedisAppendContent(content, new_value_);

  return PikaBinlogTransverter::BinlogEncode(BinlogType::TypeFirst,
                                             exec_time,
                                             std::stoi(server_id),
                                             logic_id,
                                             filenum,
                                             offset,
                                             content,
                                             {});
}

void DecrCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameDecr);
    return;
  }
  key_ = argv[1];
  return;
}

void DecrCmd::Do() {
  rocksdb::Status s = g_pika_server->db()->Decrby(key_, 1, &new_value_);
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
      uint32_t exec_time,
      const std::string& server_id,
      uint64_t logic_id,
      uint32_t filenum,
      uint64_t offset) {
  std::string content;
  content.reserve(RAW_ARGS_LEN);
  RedisAppendLen(content, 3, "*");

  // to set cmd
  std::string set_cmd("set");
  RedisAppendLen(content, set_cmd.size(), "$");
  RedisAppendContent(content, set_cmd);
  // key
  RedisAppendLen(content, key_.size(), "$");
  RedisAppendContent(content, key_);
  // value
  std::string value = std::to_string(new_value_);
  RedisAppendLen(content, value.size(), "$");
  RedisAppendContent(content, value);

  return PikaBinlogTransverter::BinlogEncode(BinlogType::TypeFirst,
                                             exec_time,
                                             std::stoi(server_id),
                                             logic_id,
                                             filenum,
                                             offset,
                                             content,
                                             {});
}

void DecrbyCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
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
  rocksdb::Status s = g_pika_server->db()->Decrby(key_, by_, &new_value_);
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
      uint32_t exec_time,
      const std::string& server_id,
      uint64_t logic_id,
      uint32_t filenum,
      uint64_t offset) {
  std::string content;
  content.reserve(RAW_ARGS_LEN);
  RedisAppendLen(content, 3, "*");

  // to set cmd
  std::string set_cmd("set");
  RedisAppendLen(content, set_cmd.size(), "$");
  RedisAppendContent(content, set_cmd);
  // key
  RedisAppendLen(content, key_.size(), "$");
  RedisAppendContent(content, key_);
  // value
  std::string value = std::to_string(new_value_);
  RedisAppendLen(content, value.size(), "$");
  RedisAppendContent(content, value);

  return PikaBinlogTransverter::BinlogEncode(BinlogType::TypeFirst,
                                             exec_time,
                                             std::stoi(server_id),
                                             logic_id,
                                             filenum,
                                             offset,
                                             content,
                                             {});
}

void GetsetCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
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
  rocksdb::Status s = g_pika_server->db()->GetSet(key_, new_value_, &old_value);
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

void AppendCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
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
  rocksdb::Status s = g_pika_server->db()->Append(key_, value_, &new_len);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(new_len);
    SlotKeyAdd("k", key_);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void MgetCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameMget);
    return;
  }
  keys_ = argv;
  keys_.erase(keys_.begin());
  return;
}

void MgetCmd::Do() {
  std::vector<blackwidow::ValueStatus> vss;
  rocksdb::Status s = g_pika_server->db()->MGet(keys_, &vss);
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
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void KeysCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameKeys);
    return;
  }
  pattern_ = argv[1];
  if (argv.size() == 3) {
    std::string opt = argv[2];
    if (!strcasecmp(opt.data(), "string")
      || !strcasecmp(opt.data(), "zset")
      || !strcasecmp(opt.data(), "set")
      || !strcasecmp(opt.data(), "list")
      || !strcasecmp(opt.data(),"hash")) {
      type_ = slash::StringToLower(opt);
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
  rocksdb::Status s = g_pika_server->db()->Keys(type_, pattern_, &keys);
  res_.AppendArrayLen(keys.size());
  for (const auto& key : keys) {
    res_.AppendString(key);
  }
  return;
}

void SetnxCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
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
  rocksdb::Status s = g_pika_server->db()->Setnx(key_, value_, &success_);
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
      uint32_t exec_time,
      const std::string& server_id,
      uint64_t logic_id,
      uint32_t filenum,
      uint64_t offset) {
  std::string content;
  if (success_) {
    content.reserve(RAW_ARGS_LEN);
    RedisAppendLen(content, 3, "*");

    // to set cmd
    std::string set_cmd("set");
    RedisAppendLen(content, set_cmd.size(), "$");
    RedisAppendContent(content, set_cmd);
    // key
    RedisAppendLen(content, key_.size(), "$");
    RedisAppendContent(content, key_);
    // value
    RedisAppendLen(content, value_.size(), "$");
    RedisAppendContent(content, value_);

    return PikaBinlogTransverter::BinlogEncode(BinlogType::TypeFirst,
                                               exec_time,
                                               std::stoi(server_id),
                                               logic_id,
                                               filenum,
                                               offset,
                                               content,
                                               {});
  }
  return content;
}

void SetexCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
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
  rocksdb::Status s = g_pika_server->db()->Setex(key_, value_, sec_);
  if (s.ok()) {
    res_.SetRes(CmdRes::kOk);
    SlotKeyAdd("k", key_);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

std::string SetexCmd::ToBinlog(
      const PikaCmdArgsType& argv,
      uint32_t exec_time,
      const std::string& server_id,
      uint64_t logic_id,
      uint32_t filenum,
      uint64_t offset) {

  std::string content;
  content.reserve(RAW_ARGS_LEN);
  RedisAppendLen(content, 4, "*");

  // to pksetexat cmd
  std::string pksetexat_cmd("pksetexat");
  RedisAppendLen(content, pksetexat_cmd.size(), "$");
  RedisAppendContent(content, pksetexat_cmd);
  // key
  RedisAppendLen(content, key_.size(), "$");
  RedisAppendContent(content, key_);
  // time_stamp
  char buf[100];
  int32_t time_stamp = time(nullptr) + sec_;
  slash::ll2string(buf, 100, time_stamp);
  std::string at(buf);
  RedisAppendLen(content, at.size(), "$");
  RedisAppendContent(content, at);
  // value
  RedisAppendLen(content, value_.size(), "$");
  RedisAppendContent(content, value_);
  return PikaBinlogTransverter::BinlogEncode(BinlogType::TypeFirst,
                                             exec_time,
                                             std::stoi(server_id),
                                             logic_id,
                                             filenum,
                                             offset,
                                             content,
                                             {});
}

void PsetexCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePsetex);
    return;
  }
  key_ = argv[1];
  if (!slash::string2l(argv[2].data(), argv[2].size(), &usec_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  value_ = argv[3];
  return;
}

void PsetexCmd::Do() {
  rocksdb::Status s = g_pika_server->db()->Setex(key_, value_, usec_ / 1000);
  if (s.ok()) {
    res_.SetRes(CmdRes::kOk);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

std::string PsetexCmd::ToBinlog(
      const PikaCmdArgsType& argv,
      uint32_t exec_time,
      const std::string& server_id,
      uint64_t logic_id,
      uint32_t filenum,
      uint64_t offset) {

  std::string content;
  content.reserve(RAW_ARGS_LEN);
  RedisAppendLen(content, 4, "*");

  // to pksetexat cmd
  std::string pksetexat_cmd("pksetexat");
  RedisAppendLen(content, pksetexat_cmd.size(), "$");
  RedisAppendContent(content, pksetexat_cmd);
  // key
  RedisAppendLen(content, key_.size(), "$");
  RedisAppendContent(content, key_);
  // time_stamp
  char buf[100];
  int32_t time_stamp = time(nullptr) + usec_ / 1000;
  slash::ll2string(buf, 100, time_stamp);
  std::string at(buf);
  RedisAppendLen(content, at.size(), "$");
  RedisAppendContent(content, at);
  // value
  RedisAppendLen(content, value_.size(), "$");
  RedisAppendContent(content, value_);
  return PikaBinlogTransverter::BinlogEncode(BinlogType::TypeFirst,
                                             exec_time,
                                             std::stoi(server_id),
                                             logic_id,
                                             filenum,
                                             offset,
                                             content,
                                             {});
}

void DelvxCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameDelvx);
    return;
  }
  key_ = argv[1];
  value_ = argv[2];
  return;
}

void DelvxCmd::Do() {
  rocksdb::Status s = g_pika_server->db()->Delvx(key_, value_, &success_);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(success_);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void MsetCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
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
  blackwidow::Status s = g_pika_server->db()->MSet(kvs_);
  if (s.ok()) {
    res_.SetRes(CmdRes::kOk);
    std::vector<blackwidow::KeyValue>::const_iterator it;
    for (it = kvs_.begin(); it != kvs_.end(); it++) {
      SlotKeyAdd("k", it->key);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void MsetnxCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
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
  rocksdb::Status s = g_pika_server->db()->MSetnx(kvs_, &success_);
  if (s.ok()) {
    res_.AppendInteger(success_);
    std::vector<blackwidow::KeyValue>::const_iterator it;
    for (it = kvs_.begin(); it != kvs_.end(); it++) {
      SlotKeyAdd("k", it->key);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void GetrangeCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
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
  rocksdb::Status s = g_pika_server->db()->Getrange(key_, start_, end_, &substr);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendStringLen(substr.size());
    res_.AppendContent(substr);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void SetrangeCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
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
  rocksdb::Status s = g_pika_server->db()->Setrange(key_, offset_, value_, &new_len);
  if (s.ok()) {
    res_.AppendInteger(new_len);
    SlotKeyAdd("k", key_);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void StrlenCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameStrlen);
    return;
  }
  key_ = argv[1];
  return;
}

void StrlenCmd::Do() {
  int32_t len = 0;
  rocksdb::Status s = g_pika_server->db()->Strlen(key_, &len);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(len);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void ExistsCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
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
  int64_t res = g_pika_server->db()->Exists(keys_, &type_status);
  if (res != -1) {
    res_.AppendInteger(res);
  } else {
    res_.SetRes(CmdRes::kErrOther, "exists internal error");
  }
  return;
}

void ExpireCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
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
  int64_t res = g_pika_server->db()->Expire(key_, sec_, &type_status);
  if (res != -1) {
    res_.AppendInteger(res);
  } else {
    res_.SetRes(CmdRes::kErrOther, "expire internal error");
  }
  return;
}

std::string ExpireCmd::ToBinlog(
      const PikaCmdArgsType& argv,
      uint32_t exec_time,
      const std::string& server_id,
      uint64_t logic_id,
      uint32_t filenum,
      uint64_t offset) {
  std::string content;
  content.reserve(RAW_ARGS_LEN);
  RedisAppendLen(content, 3, "*");

  // to expireat cmd
  std::string expireat_cmd("expireat");
  RedisAppendLen(content, expireat_cmd.size(), "$");
  RedisAppendContent(content, expireat_cmd);
  // key
  RedisAppendLen(content, key_.size(), "$");
  RedisAppendContent(content, key_);
  // sec
  char buf[100];
  int64_t expireat = time(nullptr) + sec_;
  slash::ll2string(buf, 100, expireat);
  std::string at(buf);
  RedisAppendLen(content, at.size(), "$");
  RedisAppendContent(content, at);

  return PikaBinlogTransverter::BinlogEncode(BinlogType::TypeFirst,
                                             exec_time,
                                             std::stoi(server_id),
                                             logic_id,
                                             filenum,
                                             offset,
                                             content,
                                             {});
}

void PexpireCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
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
  int64_t res = g_pika_server->db()->Expire(key_, msec_/1000, &type_status);
  if (res != -1) {
    res_.AppendInteger(res);
  } else {
    res_.SetRes(CmdRes::kErrOther, "expire internal error");
  }
  return;
}

std::string PexpireCmd::ToBinlog(
      const PikaCmdArgsType& argv,
      uint32_t exec_time,
      const std::string& server_id,
      uint64_t logic_id,
      uint32_t filenum,
      uint64_t offset) {
  std::string content;
  content.reserve(RAW_ARGS_LEN);
  RedisAppendLen(content, argv.size(), "*");

  // to expireat cmd
  std::string expireat_cmd("expireat");
  RedisAppendLen(content, expireat_cmd.size(), "$");
  RedisAppendContent(content, expireat_cmd);
  // key
  RedisAppendLen(content, key_.size(), "$");
  RedisAppendContent(content, key_);
  // sec
  char buf[100];
  int64_t expireat = time(nullptr) + msec_ / 1000;
  slash::ll2string(buf, 100, expireat);
  std::string at(buf);
  RedisAppendLen(content, at.size(), "$");
  RedisAppendContent(content, at);

  return PikaBinlogTransverter::BinlogEncode(BinlogType::TypeFirst,
                                             exec_time,
                                             std::stoi(server_id),
                                             logic_id,
                                             filenum,
                                             offset,
                                             content,
                                             {});
}

void ExpireatCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
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
  int32_t res = g_pika_server->db()->Expireat(key_, time_stamp_, &type_status);
  if (res != -1) {
    res_.AppendInteger(res);
  } else {
    res_.SetRes(CmdRes::kErrOther, "expireat internal error");
  }
}

void PexpireatCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
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
      uint32_t exec_time,
      const std::string& server_id,
      uint64_t logic_id,
      uint32_t filenum,
      uint64_t offset) {
  std::string content;
  content.reserve(RAW_ARGS_LEN);
  RedisAppendLen(content, argv.size(), "*");

  // to expireat cmd
  std::string expireat_cmd("expireat");
  RedisAppendLen(content, expireat_cmd.size(), "$");
  RedisAppendContent(content, expireat_cmd);
  // key
  RedisAppendLen(content, key_.size(), "$");
  RedisAppendContent(content, key_);
  // sec
  char buf[100];
  int64_t expireat = time_stamp_ms_ / 1000;
  slash::ll2string(buf, 100, expireat);
  std::string at(buf);
  RedisAppendLen(content, at.size(), "$");
  RedisAppendContent(content, at);

  return PikaBinlogTransverter::BinlogEncode(BinlogType::TypeFirst,
                                             exec_time,
                                             std::stoi(server_id),
                                             logic_id,
                                             filenum,
                                             offset,
                                             content,
                                             {});
}

void PexpireatCmd::Do() {
  std::map<blackwidow::DataType, rocksdb::Status> type_status;
  int32_t res = g_pika_server->db()->Expireat(key_, time_stamp_ms_/1000, &type_status);
  if (res != -1) {
    res_.AppendInteger(res);
  } else {
    res_.SetRes(CmdRes::kErrOther, "pexpireat internal error");
  }
  return;
}

void TtlCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
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
  type_timestamp = g_pika_server->db()->TTL(key_, &type_status);
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
  } else if (type_timestamp[blackwidow::kZSets] != -2) {
    res_.AppendInteger(type_timestamp[blackwidow::kZSets]);
  } else if (type_timestamp[blackwidow::kSets] != -2) {
    res_.AppendInteger(type_timestamp[blackwidow::kSets]);
  } else {
    // mean this key not exist
    res_.AppendInteger(-2);
  }
  return;
}

void PttlCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
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
  type_timestamp = g_pika_server->db()->TTL(key_, &type_status);
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

void PersistCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePersist);
    return;
  }
  key_ = argv[1];
  return;
}

void PersistCmd::Do() {
  std::map<blackwidow::DataType, rocksdb::Status> type_status;
  int32_t res = g_pika_server->db()->Persist(key_, &type_status);
  if (res != -1) {
    res_.AppendInteger(res);
  } else {
    res_.SetRes(CmdRes::kErrOther, "persist internal error");
  }
  return;
}

void TypeCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameType);
    return;
  }
  key_ = argv[1];
  return;
}

void TypeCmd::Do() {
  std::string res;
  rocksdb::Status s = g_pika_server->db()->Type(key_, &res);
  if (s.ok()) {
    res_.AppendContent("+" + res);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void ScanCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
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
    std::string opt = argv[index];
    if (!strcasecmp(opt.data(), "match")
      || !strcasecmp(opt.data(), "count")) {
      index++;
      if (index >= argc) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      if (!strcasecmp(opt.data(), "match")) {
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
  int64_t cursor_ret = g_pika_server->db()->Scan(cursor_, pattern_, count_, &keys);
  
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

void ScanxCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameScanx);
    return;
  }
  if (!strcasecmp(argv[1].data(), "string")) {
    type_ = blackwidow::kStrings;
  } else if (!strcasecmp(argv[1].data(), "hash")) {
    type_ = blackwidow::kHashes;
  } else if (!strcasecmp(argv[1].data(), "set")) {
    type_ = blackwidow::kSets;
  } else if (!strcasecmp(argv[1].data(), "zset")) {
    type_ = blackwidow::kZSets;
  } else if (!strcasecmp(argv[1].data(), "list")) {
    type_ = blackwidow::kLists;
  } else {
    res_.SetRes(CmdRes::kInvalidDbType);
    return;
  }

  start_key_ = argv[2];
  size_t index = 3, argc = argv.size();
  while (index < argc) {
    std::string opt = argv[index];
    if (!strcasecmp(opt.data(), "match")
      || !strcasecmp(opt.data(), "count")) {
      index++;
      if (index >= argc) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      if (!strcasecmp(opt.data(), "match")) {
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

void ScanxCmd::Do() {
  std::string next_key;
  std::vector<std::string> keys;
  rocksdb::Status s = g_pika_server->db()->Scanx(type_, start_key_, pattern_, count_, &keys, &next_key);

  if (s.ok()) {
    res_.AppendArrayLen(2);
    res_.AppendStringLen(next_key.size());
    res_.AppendContent(next_key);

    res_.AppendArrayLen(keys.size());
    std::vector<std::string>::const_iterator iter;
    for (const auto& key : keys){
      res_.AppendString(key);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void PKSetexAtCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePKSetexAt);
    return;
  }
  key_ = argv[1];
  value_ = argv[3];
  if (!slash::string2l(argv[2].data(), argv[2].size(), &time_stamp_)
    || time_stamp_ >= INT32_MAX) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  return;
}

void PKSetexAtCmd::Do() {
  rocksdb::Status s = g_pika_server->db()->PKSetexAt(key_, value_, time_stamp_);
  if (s.ok()) {
    res_.SetRes(CmdRes::kOk);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void PKScanRangeCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePKScanRange);
    return;
  }
  if (!strcasecmp(argv[1].data(), "string_with_value")) {
    type_ = blackwidow::kStrings;
    string_with_value = true;
  } else if (!strcasecmp(argv[1].data(), "string")) {
    type_ = blackwidow::kStrings;
  } else if (!strcasecmp(argv[1].data(), "hash")) {
    type_ = blackwidow::kHashes;
  } else if (!strcasecmp(argv[1].data(), "set")) {
    type_ = blackwidow::kSets;
  } else if (!strcasecmp(argv[1].data(), "zset")) {
    type_ = blackwidow::kZSets;
  } else if (!strcasecmp(argv[1].data(), "list")) {
    type_ = blackwidow::kLists;
  } else {
    res_.SetRes(CmdRes::kInvalidDbType);
    return;
  }

  key_start_ = argv[2];
  key_end_ = argv[3];
  size_t index = 4, argc = argv.size();
  while (index < argc) {
    std::string opt = argv[index];
    if (!strcasecmp(opt.data(), "match")
      || !strcasecmp(opt.data(), "limit")) {
      index++;
      if (index >= argc) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      if (!strcasecmp(opt.data(), "match")) {
        pattern_ = argv[index];
      } else if (!slash::string2l(argv[index].data(), argv[index].size(), &limit_) || limit_ <= 0) {
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

void PKScanRangeCmd::Do() {
  std::string next_key;
  std::vector<std::string> keys;
  std::vector<blackwidow::KeyValue> kvs;
  rocksdb::Status s = g_pika_server->db()->PKScanRange(type_, key_start_, key_end_, pattern_, limit_, &keys, &kvs, &next_key);

  if (s.ok()) {
    res_.AppendArrayLen(2);
    res_.AppendStringLen(next_key.size());
    res_.AppendContent(next_key);

    if (type_ == blackwidow::kStrings) {
      res_.AppendArrayLen(string_with_value ? 2 * kvs.size() : kvs.size());
      for (const auto& kv : kvs) {
        res_.AppendString(kv.key);
        if (string_with_value) {
          res_.AppendString(kv.value);
        }
      }
    } else {
      res_.AppendArrayLen(keys.size());
      for (const auto& key : keys){
        res_.AppendString(key);
      }
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void PKRScanRangeCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePKRScanRange);
    return;
  }
  if (!strcasecmp(argv[1].data(), "string_with_value")) {
    type_ = blackwidow::kStrings;
    string_with_value = true;
  } else if (!strcasecmp(argv[1].data(), "string")) {
    type_ = blackwidow::kStrings;
  } else if (!strcasecmp(argv[1].data(), "hash")) {
    type_ = blackwidow::kHashes;
  } else if (!strcasecmp(argv[1].data(), "set")) {
    type_ = blackwidow::kSets;
  } else if (!strcasecmp(argv[1].data(), "zset")) {
    type_ = blackwidow::kZSets;
  } else if (!strcasecmp(argv[1].data(), "list")) {
    type_ = blackwidow::kLists;
  } else {
    res_.SetRes(CmdRes::kInvalidDbType);
    return;
  }

  key_start_ = argv[2];
  key_end_ = argv[3];
  size_t index = 4, argc = argv.size();
  while (index < argc) {
    std::string opt = argv[index];
    if (!strcasecmp(opt.data(), "match")
      || !strcasecmp(opt.data(), "limit")) {
      index++;
      if (index >= argc) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      if (!strcasecmp(opt.data(), "match")) {
        pattern_ = argv[index];
      } else if (!slash::string2l(argv[index].data(), argv[index].size(), &limit_) || limit_ <= 0) {
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

void PKRScanRangeCmd::Do() {
  std::string next_key;
  std::vector<std::string> keys;
  std::vector<blackwidow::KeyValue> kvs;
  rocksdb::Status s = g_pika_server->db()->PKRScanRange(type_, key_start_, key_end_, pattern_, limit_, &keys, &kvs, &next_key);

  if (s.ok()) {
    res_.AppendArrayLen(2);
    res_.AppendStringLen(next_key.size());
    res_.AppendContent(next_key);

    if (type_ == blackwidow::kStrings) {
      res_.AppendArrayLen(string_with_value ? 2 * kvs.size() : kvs.size());
      for (const auto& kv : kvs) {
        res_.AppendString(kv.key);
        if (string_with_value) {
          res_.AppendString(kv.value);
        }
      }
    } else {
      res_.AppendArrayLen(keys.size());
      for (const auto& key : keys){
        res_.AppendString(key);
      }
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}
