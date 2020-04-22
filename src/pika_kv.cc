// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_kv.h"

#include "slash/include/slash_string.h"

#include "include/pika_conf.h"
#include "include/pika_binlog_transverter.h"

extern PikaConf *g_pika_conf;

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
    if (!strcasecmp(opt.data(), "xx")) {
      condition_ = SetCmd::kXX;
    } else if (!strcasecmp(opt.data(), "nx")) {
      condition_ = SetCmd::kNX;
    } else if (!strcasecmp(opt.data(), "vx")) {
      condition_ = SetCmd::kVX;
      index++;
      if (index == argv_.size()) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      } else {
        target_ = argv_[index];
      }
    } else if (!strcasecmp(opt.data(), "ex") || !strcasecmp(opt.data(), "px")) {
      condition_ = (condition_ == SetCmd::kNONE) ? SetCmd::kEXORPX : condition_;
      index++;
      if (index == argv_.size()) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      if (!slash::string2l(argv_[index].data(), argv_[index].size(), &sec_)) {
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

void SetCmd::Do(std::shared_ptr<Partition> partition) {
  rocksdb::Status s;
  int32_t res = 1;
  switch (condition_) {
    case SetCmd::kXX:
      s = partition->db()->Setxx(key_, value_, &res, sec_);
      break;
    case SetCmd::kNX:
      s = partition->db()->Setnx(key_, value_, &res, sec_);
      break;
    case SetCmd::kVX:
      s = partition->db()->Setvx(key_, target_, value_, &success_, sec_);
      break;
    case SetCmd::kEXORPX:
      s = partition->db()->Setex(key_, value_, sec_);
      break;
    default:
      s = partition->db()->Set(key_, value_);
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
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

std::string SetCmd::ToBinlog(
      uint32_t exec_time,
      uint32_t term_id,
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
                                               term_id,
                                               logic_id,
                                               filenum,
                                               offset,
                                               content,
                                               {});
  } else {
    return Cmd::ToBinlog(exec_time, term_id, logic_id, filenum, offset);
  }
}

void GetCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameGet);
    return;
  }
  key_ = argv_[1];
  return;
}

void GetCmd::Do(std::shared_ptr<Partition> partition) {
  std::string value;
  rocksdb::Status s = partition->db()->Get(key_, &value);
  if (s.ok()) {
    res_.AppendStringLen(value.size());
    res_.AppendContent(value);
  } else if (s.IsNotFound()) {
    res_.AppendStringLen(-1);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void DelCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, name());
    return;
  }
  std::vector<std::string>::iterator iter = argv_.begin();
  keys_.assign(++iter, argv_.end());
  return;
}

void DelCmd::Do(std::shared_ptr<Partition> partition) {
  std::map<blackwidow::DataType, blackwidow::Status> type_status;
  int64_t count = partition->db()->Del(keys_, &type_status);
  if (count >= 0) {
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, name() + " error");
  }
  return;
}

void IncrCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameIncr);
    return;
  }
  key_ = argv_[1];
  return;
}

void IncrCmd::Do(std::shared_ptr<Partition> partition) {
  rocksdb::Status s = partition->db()->Incrby(key_, 1, &new_value_);
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

void IncrbyCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameIncrby);
    return;
  }
  key_ = argv_[1];
  if (!slash::string2l(argv_[2].data(), argv_[2].size(), &by_)) {
    res_.SetRes(CmdRes::kInvalidInt, kCmdNameIncrby);
    return;
  }
  return;
}

void IncrbyCmd::Do(std::shared_ptr<Partition> partition) {
  rocksdb::Status s = partition->db()->Incrby(key_, by_, &new_value_);
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

void IncrbyfloatCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameIncrbyfloat);
    return;
  }
  key_ = argv_[1];
  value_ = argv_[2];
  if (!slash::string2d(argv_[2].data(), argv_[2].size(), &by_)) {
    res_.SetRes(CmdRes::kInvalidFloat);
    return;
  }
  return;
}

void IncrbyfloatCmd::Do(std::shared_ptr<Partition> partition) {
  rocksdb::Status s = partition->db()->Incrbyfloat(key_, value_, &new_value_);
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

void DecrCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameDecr);
    return;
  }
  key_ = argv_[1];
  return;
}

void DecrCmd::Do(std::shared_ptr<Partition> partition) {
  rocksdb::Status s = partition->db()->Decrby(key_, 1, &new_value_);
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

void DecrbyCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameDecrby);
    return;
  }
  key_ = argv_[1];
  if (!slash::string2l(argv_[2].data(), argv_[2].size(), &by_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  return;
}

void DecrbyCmd::Do(std::shared_ptr<Partition> partition) {
  rocksdb::Status s = partition->db()->Decrby(key_, by_, &new_value_);
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

void GetsetCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameGetset);
    return;
  }
  key_ = argv_[1];
  new_value_ = argv_[2];
  return;
}

void GetsetCmd::Do(std::shared_ptr<Partition> partition) {
  std::string old_value;
  rocksdb::Status s = partition->db()->GetSet(key_, new_value_, &old_value);
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

void AppendCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameAppend);
    return;
  }
  key_ = argv_[1];
  value_ = argv_[2];
  return;
}

void AppendCmd::Do(std::shared_ptr<Partition> partition) {
  int32_t new_len = 0;
  rocksdb::Status s = partition->db()->Append(key_, value_, &new_len);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(new_len);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void MgetCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameMget);
    return;
  }
  keys_ = argv_;
  keys_.erase(keys_.begin());
  return;
}

void MgetCmd::Do(std::shared_ptr<Partition> partition) {
  std::vector<blackwidow::ValueStatus> vss;
  rocksdb::Status s = partition->db()->MGet(keys_, &vss);
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

void KeysCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameKeys);
    return;
  }
  pattern_ = argv_[1];
  if (argv_.size() == 3) {
    std::string opt = argv_[2];
    if (!strcasecmp(opt.data(), "string")) {
      type_ = blackwidow::DataType::kStrings;
    } else if (!strcasecmp(opt.data(), "zset")) {
      type_ = blackwidow::DataType::kZSets;
    } else if (!strcasecmp(opt.data(), "set")) {
      type_ = blackwidow::DataType::kSets;
    } else if (!strcasecmp(opt.data(), "list")) {
      type_ = blackwidow::DataType::kLists;
    } else if (!strcasecmp(opt.data(), "hash")) {
      type_ = blackwidow::DataType::kHashes;
    } else {
      res_.SetRes(CmdRes::kSyntaxErr);
    }
  } else if (argv_.size() > 3) {
    res_.SetRes(CmdRes::kSyntaxErr);
  }
  return;
}

void KeysCmd::Do(std::shared_ptr<Partition> partition) {
  int64_t total_key = 0;
  int64_t cursor = 0;
  size_t raw_limit = g_pika_conf->max_client_response_size();
  std::string raw;
  std::vector<std::string> keys;
  do {
    keys.clear();
    cursor = partition->db()->Scan(type_, cursor, pattern_, PIKA_SCAN_STEP_LENGTH, &keys);
    for (const auto& key : keys) {
      RedisAppendLen(raw, key.size(), "$");
      RedisAppendContent(raw, key);
    }
    if (raw.size() >= raw_limit) {
      res_.SetRes(CmdRes::kErrOther, "Response exceeds the max-client-response-size limit");
      return;
    }
    total_key += keys.size();
  } while (cursor != 0);

  res_.AppendArrayLen(total_key);
  res_.AppendStringRaw(raw);
  return;
}

void SetnxCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSetnx);
    return;
  }
  key_ = argv_[1];
  value_ = argv_[2];
  return;
}

void SetnxCmd::Do(std::shared_ptr<Partition> partition) {
  success_ = 0;
  rocksdb::Status s = partition->db()->Setnx(key_, value_, &success_);
  if (s.ok()) {
    res_.AppendInteger(success_);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

std::string SetnxCmd::ToBinlog(
      uint32_t exec_time,
      uint32_t term_id,
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
                                               term_id,
                                               logic_id,
                                               filenum,
                                               offset,
                                               content,
                                               {});
  }
  return content;
}

void SetexCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSetex);
    return;
  }
  key_ = argv_[1];
  if (!slash::string2l(argv_[2].data(), argv_[2].size(), &sec_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  value_ = argv_[3];
  return;
}

void SetexCmd::Do(std::shared_ptr<Partition> partition) {
  rocksdb::Status s = partition->db()->Setex(key_, value_, sec_);
  if (s.ok()) {
    res_.SetRes(CmdRes::kOk);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

std::string SetexCmd::ToBinlog(
      uint32_t exec_time,
      uint32_t term_id,
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
                                             term_id,
                                             logic_id,
                                             filenum,
                                             offset,
                                             content,
                                             {});
}

void PsetexCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePsetex);
    return;
  }
  key_ = argv_[1];
  if (!slash::string2l(argv_[2].data(), argv_[2].size(), &usec_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  value_ = argv_[3];
  return;
}

void PsetexCmd::Do(std::shared_ptr<Partition> partition) {
  rocksdb::Status s = partition->db()->Setex(key_, value_, usec_ / 1000);
  if (s.ok()) {
    res_.SetRes(CmdRes::kOk);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

std::string PsetexCmd::ToBinlog(
      uint32_t exec_time,
      uint32_t term_id,
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
                                             term_id,
                                             logic_id,
                                             filenum,
                                             offset,
                                             content,
                                             {});
}

void DelvxCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameDelvx);
    return;
  }
  key_ = argv_[1];
  value_ = argv_[2];
  return;
}

void DelvxCmd::Do(std::shared_ptr<Partition> partition) {
  rocksdb::Status s = partition->db()->Delvx(key_, value_, &success_);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(success_);
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
  return;
}

void MsetCmd::Do(std::shared_ptr<Partition> partition) {
  blackwidow::Status s = partition->db()->MSet(kvs_);
  if (s.ok()) {
    res_.SetRes(CmdRes::kOk);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
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
  return;
}

void MsetnxCmd::Do(std::shared_ptr<Partition> partition) {
  success_ = 0;
  rocksdb::Status s = partition->db()->MSetnx(kvs_, &success_);
  if (s.ok()) {
    res_.AppendInteger(success_);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void GetrangeCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameGetrange);
    return;
  }
  key_ = argv_[1];
  if (!slash::string2l(argv_[2].data(), argv_[2].size(), &start_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  if (!slash::string2l(argv_[3].data(), argv_[3].size(), &end_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  return;
}

void GetrangeCmd::Do(std::shared_ptr<Partition> partition) {
  std::string substr;
  rocksdb::Status s = partition->db()->Getrange(key_, start_, end_, &substr);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendStringLen(substr.size());
    res_.AppendContent(substr);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void SetrangeCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSetrange);
    return;
  }
  key_ = argv_[1];
  if (!slash::string2l(argv_[2].data(), argv_[2].size(), &offset_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  value_ = argv_[3];
  return;
}

void SetrangeCmd::Do(std::shared_ptr<Partition> partition) {
  int32_t new_len;
  rocksdb::Status s = partition->db()->Setrange(key_, offset_, value_, &new_len);
  if (s.ok()) {
    res_.AppendInteger(new_len);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void StrlenCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameStrlen);
    return;
  }
  key_ = argv_[1];
  return;
}

void StrlenCmd::Do(std::shared_ptr<Partition> partition) {
  int32_t len = 0;
  rocksdb::Status s = partition->db()->Strlen(key_, &len);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(len);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void ExistsCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameExists);
    return;
  }
  keys_ = argv_;
  keys_.erase(keys_.begin());
  return;
}

void ExistsCmd::Do(std::shared_ptr<Partition> partition) {
  std::map<blackwidow::DataType, rocksdb::Status> type_status;
  int64_t res = partition->db()->Exists(keys_, &type_status);
  if (res != -1) {
    res_.AppendInteger(res);
  } else {
    res_.SetRes(CmdRes::kErrOther, "exists internal error");
  }
  return;
}

void ExpireCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameExpire);
    return;
  }
  key_ = argv_[1];
  if (!slash::string2l(argv_[2].data(), argv_[2].size(), &sec_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  return;
}

void ExpireCmd::Do(std::shared_ptr<Partition> partition) {
  std::map<blackwidow::DataType, rocksdb::Status> type_status;
  int64_t res = partition->db()->Expire(key_, sec_, &type_status);
  if (res != -1) {
    res_.AppendInteger(res);
  } else {
    res_.SetRes(CmdRes::kErrOther, "expire internal error");
  }
  return;
}

std::string ExpireCmd::ToBinlog(
      uint32_t exec_time,
      uint32_t term_id,
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
                                             term_id,
                                             logic_id,
                                             filenum,
                                             offset,
                                             content,
                                             {});
}

void PexpireCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePexpire);
    return;
  }
  key_ = argv_[1];
  if (!slash::string2l(argv_[2].data(), argv_[2].size(), &msec_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  return;
}

void PexpireCmd::Do(std::shared_ptr<Partition> partition) {
  std::map<blackwidow::DataType, rocksdb::Status> type_status;
  int64_t res = partition->db()->Expire(key_, msec_/1000, &type_status);
  if (res != -1) {
    res_.AppendInteger(res);
  } else {
    res_.SetRes(CmdRes::kErrOther, "expire internal error");
  }
  return;
}

std::string PexpireCmd::ToBinlog(
      uint32_t exec_time,
      uint32_t term_id,
      uint64_t logic_id,
      uint32_t filenum,
      uint64_t offset) {
  std::string content;
  content.reserve(RAW_ARGS_LEN);
  RedisAppendLen(content, argv_.size(), "*");

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
                                             term_id,
                                             logic_id,
                                             filenum,
                                             offset,
                                             content,
                                             {});
}

void ExpireatCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameExpireat);
    return;
  }
  key_ = argv_[1];
  if (!slash::string2l(argv_[2].data(), argv_[2].size(), &time_stamp_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  return;
}

void ExpireatCmd::Do(std::shared_ptr<Partition> partition) {
  std::map<blackwidow::DataType, rocksdb::Status> type_status;
  int32_t res = partition->db()->Expireat(key_, time_stamp_, &type_status);
  if (res != -1) {
    res_.AppendInteger(res);
  } else {
    res_.SetRes(CmdRes::kErrOther, "expireat internal error");
  }
}

void PexpireatCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePexpireat);
    return;
  }
  key_ = argv_[1];
  if (!slash::string2l(argv_[2].data(), argv_[2].size(), &time_stamp_ms_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  return;
}

std::string PexpireatCmd::ToBinlog(
      uint32_t exec_time,
      uint32_t term_id,
      uint64_t logic_id,
      uint32_t filenum,
      uint64_t offset) {
  std::string content;
  content.reserve(RAW_ARGS_LEN);
  RedisAppendLen(content, argv_.size(), "*");

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
                                             term_id,
                                             logic_id,
                                             filenum,
                                             offset,
                                             content,
                                             {});
}

void PexpireatCmd::Do(std::shared_ptr<Partition> partition) {
  std::map<blackwidow::DataType, rocksdb::Status> type_status;
  int32_t res = partition->db()->Expireat(key_, time_stamp_ms_/1000, &type_status);
  if (res != -1) {
    res_.AppendInteger(res);
  } else {
    res_.SetRes(CmdRes::kErrOther, "pexpireat internal error");
  }
  return;
}

void TtlCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameTtl);
    return;
  }
  key_ = argv_[1];
  return;
}

void TtlCmd::Do(std::shared_ptr<Partition> partition) {
  std::map<blackwidow::DataType, int64_t> type_timestamp;
  std::map<blackwidow::DataType, rocksdb::Status> type_status;
  type_timestamp = partition->db()->TTL(key_, &type_status);
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

void PttlCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePttl);
    return;
  }
  key_ = argv_[1];
  return;
}

void PttlCmd::Do(std::shared_ptr<Partition> partition) {
  std::map<blackwidow::DataType, int64_t> type_timestamp;
  std::map<blackwidow::DataType, rocksdb::Status> type_status;
  type_timestamp = partition->db()->TTL(key_, &type_status);
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

void PersistCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePersist);
    return;
  }
  key_ = argv_[1];
  return;
}

void PersistCmd::Do(std::shared_ptr<Partition> partition) {
  std::map<blackwidow::DataType, rocksdb::Status> type_status;
  int32_t res = partition->db()->Persist(key_, &type_status);
  if (res != -1) {
    res_.AppendInteger(res);
  } else {
    res_.SetRes(CmdRes::kErrOther, "persist internal error");
  }
  return;
}

void TypeCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameType);
    return;
  }
  key_ = argv_[1];
  return;
}

void TypeCmd::Do(std::shared_ptr<Partition> partition) {
  std::string res;
  rocksdb::Status s = partition->db()->Type(key_, &res);
  if (s.ok()) {
    res_.AppendContent("+" + res);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void ScanCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameScan);
    return;
  }
  if (!slash::string2l(argv_[1].data(), argv_[1].size(), &cursor_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  size_t index = 2, argc = argv_.size();

  while (index < argc) {
    std::string opt = argv_[index];
    if (!strcasecmp(opt.data(), "match")
      || !strcasecmp(opt.data(), "count")) {
      index++;
      if (index >= argc) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      if (!strcasecmp(opt.data(), "match")) {
        pattern_ = argv_[index];
      } else if (!slash::string2l(argv_[index].data(), argv_[index].size(), &count_) || count_ <= 0) {
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

void ScanCmd::Do(std::shared_ptr<Partition> partition) {
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
    cursor_ret = partition->db()->Scan(blackwidow::DataType::kAll, cursor_ret,
            pattern_, batch_count, &keys);
    for (const auto& key : keys) {
      RedisAppendLen(raw, key.size(), "$");
      RedisAppendContent(raw, key);
    }
    if (raw.size() >= raw_limit) {
      res_.SetRes(CmdRes::kErrOther, "Response exceeds the max-client-response-size limit");
      return;
    }
    total_key += keys.size();
  } while (cursor_ret != 0 && left);

  res_.AppendArrayLen(2);

  char buf[32];
  int len = slash::ll2string(buf, sizeof(buf), cursor_ret);
  res_.AppendStringLen(len);
  res_.AppendContent(buf);

  res_.AppendArrayLen(total_key);
  res_.AppendStringRaw(raw);
  return;
}

void ScanxCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameScanx);
    return;
  }
  if (!strcasecmp(argv_[1].data(), "string")) {
    type_ = blackwidow::kStrings;
  } else if (!strcasecmp(argv_[1].data(), "hash")) {
    type_ = blackwidow::kHashes;
  } else if (!strcasecmp(argv_[1].data(), "set")) {
    type_ = blackwidow::kSets;
  } else if (!strcasecmp(argv_[1].data(), "zset")) {
    type_ = blackwidow::kZSets;
  } else if (!strcasecmp(argv_[1].data(), "list")) {
    type_ = blackwidow::kLists;
  } else {
    res_.SetRes(CmdRes::kInvalidDbType);
    return;
  }

  start_key_ = argv_[2];
  size_t index = 3, argc = argv_.size();
  while (index < argc) {
    std::string opt = argv_[index];
    if (!strcasecmp(opt.data(), "match")
      || !strcasecmp(opt.data(), "count")) {
      index++;
      if (index >= argc) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      if (!strcasecmp(opt.data(), "match")) {
        pattern_ = argv_[index];
      } else if (!slash::string2l(argv_[index].data(), argv_[index].size(), &count_) || count_ <= 0) {
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

void ScanxCmd::Do(std::shared_ptr<Partition> partition) {
  std::string next_key;
  std::vector<std::string> keys;
  rocksdb::Status s = partition->db()->Scanx(type_, start_key_, pattern_, count_, &keys, &next_key);

  if (s.ok()) {
    res_.AppendArrayLen(2);
    res_.AppendStringLen(next_key.size());
    res_.AppendContent(next_key);

    res_.AppendArrayLen(keys.size());
    std::vector<std::string>::iterator iter;
    for (const auto& key : keys){
      res_.AppendString(key);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void PKSetexAtCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePKSetexAt);
    return;
  }
  key_ = argv_[1];
  value_ = argv_[3];
  if (!slash::string2l(argv_[2].data(), argv_[2].size(), &time_stamp_)
    || time_stamp_ >= INT32_MAX) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  return;
}

void PKSetexAtCmd::Do(std::shared_ptr<Partition> partition) {
  rocksdb::Status s = partition->db()->PKSetexAt(key_, value_, time_stamp_);
  if (s.ok()) {
    res_.SetRes(CmdRes::kOk);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void PKScanRangeCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePKScanRange);
    return;
  }
  if (!strcasecmp(argv_[1].data(), "string_with_value")) {
    type_ = blackwidow::kStrings;
    string_with_value = true;
  } else if (!strcasecmp(argv_[1].data(), "string")) {
    type_ = blackwidow::kStrings;
  } else if (!strcasecmp(argv_[1].data(), "hash")) {
    type_ = blackwidow::kHashes;
  } else if (!strcasecmp(argv_[1].data(), "set")) {
    type_ = blackwidow::kSets;
  } else if (!strcasecmp(argv_[1].data(), "zset")) {
    type_ = blackwidow::kZSets;
  } else if (!strcasecmp(argv_[1].data(), "list")) {
    type_ = blackwidow::kLists;
  } else {
    res_.SetRes(CmdRes::kInvalidDbType);
    return;
  }

  key_start_ = argv_[2];
  key_end_ = argv_[3];
  size_t index = 4, argc = argv_.size();
  while (index < argc) {
    std::string opt = argv_[index];
    if (!strcasecmp(opt.data(), "match")
      || !strcasecmp(opt.data(), "limit")) {
      index++;
      if (index >= argc) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      if (!strcasecmp(opt.data(), "match")) {
        pattern_ = argv_[index];
      } else if (!slash::string2l(argv_[index].data(), argv_[index].size(), &limit_) || limit_ <= 0) {
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

void PKScanRangeCmd::Do(std::shared_ptr<Partition> partition) {
  std::string next_key;
  std::vector<std::string> keys;
  std::vector<blackwidow::KeyValue> kvs;
  rocksdb::Status s = partition->db()->PKScanRange(type_, key_start_, key_end_, pattern_, limit_, &keys, &kvs, &next_key);

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

void PKRScanRangeCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePKRScanRange);
    return;
  }
  if (!strcasecmp(argv_[1].data(), "string_with_value")) {
    type_ = blackwidow::kStrings;
    string_with_value = true;
  } else if (!strcasecmp(argv_[1].data(), "string")) {
    type_ = blackwidow::kStrings;
  } else if (!strcasecmp(argv_[1].data(), "hash")) {
    type_ = blackwidow::kHashes;
  } else if (!strcasecmp(argv_[1].data(), "set")) {
    type_ = blackwidow::kSets;
  } else if (!strcasecmp(argv_[1].data(), "zset")) {
    type_ = blackwidow::kZSets;
  } else if (!strcasecmp(argv_[1].data(), "list")) {
    type_ = blackwidow::kLists;
  } else {
    res_.SetRes(CmdRes::kInvalidDbType);
    return;
  }

  key_start_ = argv_[2];
  key_end_ = argv_[3];
  size_t index = 4, argc = argv_.size();
  while (index < argc) {
    std::string opt = argv_[index];
    if (!strcasecmp(opt.data(), "match")
      || !strcasecmp(opt.data(), "limit")) {
      index++;
      if (index >= argc) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      if (!strcasecmp(opt.data(), "match")) {
        pattern_ = argv_[index];
      } else if (!slash::string2l(argv_[index].data(), argv_[index].size(), &limit_) || limit_ <= 0) {
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

void PKRScanRangeCmd::Do(std::shared_ptr<Partition> partition) {
  std::string next_key;
  std::vector<std::string> keys;
  std::vector<blackwidow::KeyValue> kvs;
  rocksdb::Status s = partition->db()->PKRScanRange(type_, key_start_, key_end_, pattern_, limit_, &keys, &kvs, &next_key);

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
