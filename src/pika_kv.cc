// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_kv.h"

#include "pstd/include/pstd_string.h"

#include "include/pika_binlog_transverter.h"
#include "include/pika_conf.h"
#include "include/pika_data_distribution.h"

extern std::unique_ptr<PikaConf> g_pika_conf;

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
    if (strcasecmp(opt.data(), "xx") == 0) {
      condition_ = SetCmd::kXX;
    } else if (strcasecmp(opt.data(), "nx") == 0) {
      condition_ = SetCmd::kNX;
    } else if (strcasecmp(opt.data(), "vx") == 0) {
      condition_ = SetCmd::kVX;
      index++;
      if (index == argv_.size()) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      } else {
        target_ = argv_[index];
      }
    } else if ((strcasecmp(opt.data(), "ex") == 0) || (strcasecmp(opt.data(), "px") == 0)) {
      condition_ = (condition_ == SetCmd::kNONE) ? SetCmd::kEXORPX : condition_;
      index++;
      if (index == argv_.size()) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      if (pstd::string2int(argv_[index].data(), argv_[index].size(), &sec_) == 0) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
      }

      if (strcasecmp(opt.data(), "px") == 0) {
        sec_ /= 1000;
      }
    } else {
      res_.SetRes(CmdRes::kSyntaxErr);
      return;
    }
    index++;
  }
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
        res_.AppendStringLen(-1);
      }
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

std::string SetCmd::ToBinlog(uint32_t exec_time, uint32_t term_id, uint64_t logic_id, uint32_t filenum,
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
    pstd::ll2string(buf, 100, time_stamp);
    std::string at(buf);
    RedisAppendLen(content, at.size(), "$");
    RedisAppendContent(content, at);
    // value
    RedisAppendLen(content, value_.size(), "$");
    RedisAppendContent(content, value_);
    return PikaBinlogTransverter::BinlogEncode(BinlogType::TypeFirst, exec_time, term_id, logic_id, filenum, offset,
                                               content, {});
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
  auto iter = argv_.begin();
  keys_.assign(++iter, argv_.end());
}

void DelCmd::Do(std::shared_ptr<Partition> partition) {
  std::map<storage::DataType, storage::Status> type_status;
  int64_t count = partition->db()->Del(keys_, &type_status);
  if (count >= 0) {
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, "delete error");
  }
}

void DelCmd::Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) {
  std::map<storage::DataType, storage::Status> type_status;
  int64_t count = partition->db()->Del(hint_keys.keys, &type_status);
  if (count >= 0) {
    split_res_ += count;
  } else {
    res_.SetRes(CmdRes::kErrOther, "delete error");
  }
}

void DelCmd::Merge() {
  res_.AppendInteger(split_res_);
}

void IncrCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameIncr);
    return;
  }
  key_ = argv_[1];
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
}

void IncrbyCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameIncrby);
    return;
  }
  key_ = argv_[1];
  if (pstd::string2int(argv_[2].data(), argv_[2].size(), &by_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt, kCmdNameIncrby);
    return;
  }
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
}

void IncrbyfloatCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameIncrbyfloat);
    return;
  }
  key_ = argv_[1];
  value_ = argv_[2];
  if (pstd::string2d(argv_[2].data(), argv_[2].size(), &by_) == 0) {
    res_.SetRes(CmdRes::kInvalidFloat);
    return;
  }
}

void IncrbyfloatCmd::Do(std::shared_ptr<Partition> partition) {
  rocksdb::Status s = partition->db()->Incrbyfloat(key_, value_, &new_value_);
  if (s.ok()) {
    res_.AppendStringLen(new_value_.size());
    res_.AppendContent(new_value_);
  } else if (s.IsCorruption() && s.ToString() == "Corruption: Value is not a vaild float") {
    res_.SetRes(CmdRes::kInvalidFloat);
  } else if (s.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kOverFlow);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void DecrCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameDecr);
    return;
  }
  key_ = argv_[1];
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
}

void DecrbyCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameDecrby);
    return;
  }
  key_ = argv_[1];
  if (pstd::string2int(argv_[2].data(), argv_[2].size(), &by_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
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
}

void GetsetCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameGetset);
    return;
  }
  key_ = argv_[1];
  new_value_ = argv_[2];
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
}

void AppendCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameAppend);
    return;
  }
  key_ = argv_[1];
  value_ = argv_[2];
}

void AppendCmd::Do(std::shared_ptr<Partition> partition) {
  int32_t new_len = 0;
  rocksdb::Status s = partition->db()->Append(key_, value_, &new_len);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(new_len);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void MgetCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameMget);
    return;
  }
  keys_ = argv_;
  keys_.erase(keys_.begin());
  split_res_.resize(keys_.size());
}

void MgetCmd::Do(std::shared_ptr<Partition> partition) {
  std::vector<storage::ValueStatus> vss;
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
}

void MgetCmd::Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) {
  std::vector<storage::ValueStatus> vss;
  const std::vector<std::string>& keys = hint_keys.keys;
  rocksdb::Status s = partition->db()->MGet(keys, &vss);
  if (s.ok()) {
    if (hint_keys.hints.size() != vss.size()) {
      res_.SetRes(CmdRes::kErrOther, "internal Mget return size invalid");
    }
    const std::vector<int>& hints = hint_keys.hints;
    for (size_t i = 0; i < vss.size(); ++i) {
      split_res_[hints[i]] = vss[i];
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void MgetCmd::Merge() {
  res_.AppendArrayLen(split_res_.size());
  for (const auto& vs : split_res_) {
    if (vs.status.ok()) {
      res_.AppendStringLen(vs.value.size());
      res_.AppendContent(vs.value);
    } else {
      res_.AppendContent("$-1");
    }
  }
}

void KeysCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameKeys);
    return;
  }
  pattern_ = argv_[1];
  if (argv_.size() == 3) {
    std::string opt = argv_[2];
    if (strcasecmp(opt.data(), "string") == 0) {
      type_ = storage::DataType::kStrings;
    } else if (strcasecmp(opt.data(), "zset") == 0) {
      type_ = storage::DataType::kZSets;
    } else if (strcasecmp(opt.data(), "set") == 0) {
      type_ = storage::DataType::kSets;
    } else if (strcasecmp(opt.data(), "list") == 0) {
      type_ = storage::DataType::kLists;
    } else if (strcasecmp(opt.data(), "hash") == 0) {
      type_ = storage::DataType::kHashes;
    } else {
      res_.SetRes(CmdRes::kSyntaxErr);
    }
  } else if (argv_.size() > 3) {
    res_.SetRes(CmdRes::kSyntaxErr);
  }
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
}

void SetnxCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSetnx);
    return;
  }
  key_ = argv_[1];
  value_ = argv_[2];
}

void SetnxCmd::Do(std::shared_ptr<Partition> partition) {
  success_ = 0;
  rocksdb::Status s = partition->db()->Setnx(key_, value_, &success_);
  if (s.ok()) {
    res_.AppendInteger(success_);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

std::string SetnxCmd::ToBinlog(uint32_t exec_time, uint32_t term_id, uint64_t logic_id, uint32_t filenum,
                               uint64_t offset) {
  std::string content;
  content.reserve(RAW_ARGS_LEN);
  RedisAppendLen(content, 3, "*");

  // don't check variable 'success_', because if 'success_' was false, an empty binlog will be saved into file.
  // to setnx cmd
  std::string set_cmd("setnx");
  RedisAppendLen(content, set_cmd.size(), "$");
  RedisAppendContent(content, set_cmd);
  // key
  RedisAppendLen(content, key_.size(), "$");
  RedisAppendContent(content, key_);
  // value
  RedisAppendLen(content, value_.size(), "$");
  RedisAppendContent(content, value_);

  return PikaBinlogTransverter::BinlogEncode(BinlogType::TypeFirst, exec_time, term_id, logic_id, filenum, offset,
                                             content, {});
}

void SetexCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSetex);
    return;
  }
  key_ = argv_[1];
  if (pstd::string2int(argv_[2].data(), argv_[2].size(), &sec_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  value_ = argv_[3];
}

void SetexCmd::Do(std::shared_ptr<Partition> partition) {
  rocksdb::Status s = partition->db()->Setex(key_, value_, sec_);
  if (s.ok()) {
    res_.SetRes(CmdRes::kOk);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

std::string SetexCmd::ToBinlog(uint32_t exec_time, uint32_t term_id, uint64_t logic_id, uint32_t filenum,
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
  pstd::ll2string(buf, 100, time_stamp);
  std::string at(buf);
  RedisAppendLen(content, at.size(), "$");
  RedisAppendContent(content, at);
  // value
  RedisAppendLen(content, value_.size(), "$");
  RedisAppendContent(content, value_);
  return PikaBinlogTransverter::BinlogEncode(BinlogType::TypeFirst, exec_time, term_id, logic_id, filenum, offset,
                                             content, {});
}

void PsetexCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePsetex);
    return;
  }
  key_ = argv_[1];
  if (pstd::string2int(argv_[2].data(), argv_[2].size(), &usec_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  value_ = argv_[3];
}

void PsetexCmd::Do(std::shared_ptr<Partition> partition) {
  rocksdb::Status s = partition->db()->Setex(key_, value_, usec_ / 1000);
  if (s.ok()) {
    res_.SetRes(CmdRes::kOk);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

std::string PsetexCmd::ToBinlog(uint32_t exec_time, uint32_t term_id, uint64_t logic_id, uint32_t filenum,
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
  pstd::ll2string(buf, 100, time_stamp);
  std::string at(buf);
  RedisAppendLen(content, at.size(), "$");
  RedisAppendContent(content, at);
  // value
  RedisAppendLen(content, value_.size(), "$");
  RedisAppendContent(content, value_);
  return PikaBinlogTransverter::BinlogEncode(BinlogType::TypeFirst, exec_time, term_id, logic_id, filenum, offset,
                                             content, {});
}

void DelvxCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameDelvx);
    return;
  }
  key_ = argv_[1];
  value_ = argv_[2];
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
 }

void MsetCmd::Do(std::shared_ptr<Partition> partition) {
  storage::Status s = partition->db()->MSet(kvs_);
  if (s.ok()) {
    res_.SetRes(CmdRes::kOk);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void MsetCmd::Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) {
  std::vector<storage::KeyValue> kvs;
  const std::vector<std::string>& keys = hint_keys.keys;
  const std::vector<int>& hints = hint_keys.hints;
  if (keys.size() != hints.size()) {
    res_.SetRes(CmdRes::kErrOther, "SplitError hint_keys size not match");
  }
  for (size_t i = 0; i < keys.size(); i++) {
    if (kvs_[hints[i]].key == keys[i]) {
      kvs.push_back(kvs_[hints[i]]);
    } else {
      res_.SetRes(CmdRes::kErrOther, "SplitError hint key: " + keys[i]);
      return;
    }
  }
  storage::Status s = partition->db()->MSet(kvs);
  if (s.ok()) {
    res_.SetRes(CmdRes::kOk);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }
}

void MsetCmd::Merge() {}

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
  if (pstd::string2int(argv_[2].data(), argv_[2].size(), &start_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  if (pstd::string2int(argv_[3].data(), argv_[3].size(), &end_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
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
  if (pstd::string2int(argv_[2].data(), argv_[2].size(), &offset_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  value_ = argv_[3];
}

void SetrangeCmd::Do(std::shared_ptr<Partition> partition) {
  int32_t new_len;
  rocksdb::Status s = partition->db()->Setrange(key_, offset_, value_, &new_len);
  if (s.ok()) {
    res_.AppendInteger(new_len);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void StrlenCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameStrlen);
    return;
  }
  key_ = argv_[1];
}

void StrlenCmd::Do(std::shared_ptr<Partition> partition) {
  int32_t len = 0;
  rocksdb::Status s = partition->db()->Strlen(key_, &len);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(len);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void ExistsCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameExists);
    return;
  }
  keys_ = argv_;
  keys_.erase(keys_.begin());
}

void ExistsCmd::Do(std::shared_ptr<Partition> partition) {
  std::map<storage::DataType, rocksdb::Status> type_status;
  int64_t res = partition->db()->Exists(keys_, &type_status);
  if (res != -1) {
    res_.AppendInteger(res);
  } else {
    res_.SetRes(CmdRes::kErrOther, "exists internal error");
  }
}

void ExistsCmd::Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) {
  std::map<storage::DataType, rocksdb::Status> type_status;
  int64_t res = partition->db()->Exists(hint_keys.keys, &type_status);
  if (res != -1) {
    split_res_ += res;
  } else {
    res_.SetRes(CmdRes::kErrOther, "exists internal error");
  }
}

void ExistsCmd::Merge() {
  res_.AppendInteger(split_res_);
}

void ExpireCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameExpire);
    return;
  }
  key_ = argv_[1];
  if (pstd::string2int(argv_[2].data(), argv_[2].size(), &sec_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
}

void ExpireCmd::Do(std::shared_ptr<Partition> partition) {
  std::map<storage::DataType, rocksdb::Status> type_status;
  int64_t res = partition->db()->Expire(key_, sec_, &type_status);
  if (res != -1) {
    res_.AppendInteger(res);
  } else {
    res_.SetRes(CmdRes::kErrOther, "expire internal error");
  }
}

std::string ExpireCmd::ToBinlog(uint32_t exec_time, uint32_t term_id, uint64_t logic_id, uint32_t filenum,
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
  pstd::ll2string(buf, 100, expireat);
  std::string at(buf);
  RedisAppendLen(content, at.size(), "$");
  RedisAppendContent(content, at);

  return PikaBinlogTransverter::BinlogEncode(BinlogType::TypeFirst, exec_time, term_id, logic_id, filenum, offset,
                                             content, {});
}

void PexpireCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePexpire);
    return;
  }
  key_ = argv_[1];
  if (pstd::string2int(argv_[2].data(), argv_[2].size(), &msec_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
}

void PexpireCmd::Do(std::shared_ptr<Partition> partition) {
  std::map<storage::DataType, rocksdb::Status> type_status;
  int64_t res = partition->db()->Expire(key_, msec_ / 1000, &type_status);
  if (res != -1) {
    res_.AppendInteger(res);
  } else {
    res_.SetRes(CmdRes::kErrOther, "expire internal error");
  }
}

std::string PexpireCmd::ToBinlog(uint32_t exec_time, uint32_t term_id, uint64_t logic_id, uint32_t filenum,
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
  pstd::ll2string(buf, 100, expireat);
  std::string at(buf);
  RedisAppendLen(content, at.size(), "$");
  RedisAppendContent(content, at);

  return PikaBinlogTransverter::BinlogEncode(BinlogType::TypeFirst, exec_time, term_id, logic_id, filenum, offset,
                                             content, {});
}

void ExpireatCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameExpireat);
    return;
  }
  key_ = argv_[1];
  if (pstd::string2int(argv_[2].data(), argv_[2].size(), &time_stamp_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
}

void ExpireatCmd::Do(std::shared_ptr<Partition> partition) {
  std::map<storage::DataType, rocksdb::Status> type_status;
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
  if (pstd::string2int(argv_[2].data(), argv_[2].size(), &time_stamp_ms_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
}

std::string PexpireatCmd::ToBinlog(uint32_t exec_time, uint32_t term_id, uint64_t logic_id, uint32_t filenum,
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
  pstd::ll2string(buf, 100, expireat);
  std::string at(buf);
  RedisAppendLen(content, at.size(), "$");
  RedisAppendContent(content, at);

  return PikaBinlogTransverter::BinlogEncode(BinlogType::TypeFirst, exec_time, term_id, logic_id, filenum, offset,
                                             content, {});
}

void PexpireatCmd::Do(std::shared_ptr<Partition> partition) {
  std::map<storage::DataType, rocksdb::Status> type_status;
  int32_t res = partition->db()->Expireat(key_, time_stamp_ms_ / 1000, &type_status);
  if (res != -1) {
    res_.AppendInteger(res);
  } else {
    res_.SetRes(CmdRes::kErrOther, "pexpireat internal error");
  }
}

void TtlCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameTtl);
    return;
  }
  key_ = argv_[1];
}

void TtlCmd::Do(std::shared_ptr<Partition> partition) {
  std::map<storage::DataType, int64_t> type_timestamp;
  std::map<storage::DataType, rocksdb::Status> type_status;
  type_timestamp = partition->db()->TTL(key_, &type_status);
  for (const auto& item : type_timestamp) {
    // mean operation exception errors happen in database
    if (item.second == -3) {
      res_.SetRes(CmdRes::kErrOther, "ttl internal error");
      return;
    }
  }
  if (type_timestamp[storage::kStrings] != -2) {
    res_.AppendInteger(type_timestamp[storage::kStrings]);
  } else if (type_timestamp[storage::kHashes] != -2) {
    res_.AppendInteger(type_timestamp[storage::kHashes]);
  } else if (type_timestamp[storage::kLists] != -2) {
    res_.AppendInteger(type_timestamp[storage::kLists]);
  } else if (type_timestamp[storage::kZSets] != -2) {
    res_.AppendInteger(type_timestamp[storage::kZSets]);
  } else if (type_timestamp[storage::kSets] != -2) {
    res_.AppendInteger(type_timestamp[storage::kSets]);
  } else {
    // mean this key not exist
    res_.AppendInteger(-2);
  }
}

void PttlCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePttl);
    return;
  }
  key_ = argv_[1];
}

void PttlCmd::Do(std::shared_ptr<Partition> partition) {
  std::map<storage::DataType, int64_t> type_timestamp;
  std::map<storage::DataType, rocksdb::Status> type_status;
  type_timestamp = partition->db()->TTL(key_, &type_status);
  for (const auto& item : type_timestamp) {
    // mean operation exception errors happen in database
    if (item.second == -3) {
      res_.SetRes(CmdRes::kErrOther, "ttl internal error");
      return;
    }
  }
  if (type_timestamp[storage::kStrings] != -2) {
    if (type_timestamp[storage::kStrings] == -1) {
      res_.AppendInteger(-1);
    } else {
      res_.AppendInteger(type_timestamp[storage::kStrings] * 1000);
    }
  } else if (type_timestamp[storage::kHashes] != -2) {
    if (type_timestamp[storage::kHashes] == -1) {
      res_.AppendInteger(-1);
    } else {
      res_.AppendInteger(type_timestamp[storage::kHashes] * 1000);
    }
  } else if (type_timestamp[storage::kLists] != -2) {
    if (type_timestamp[storage::kLists] == -1) {
      res_.AppendInteger(-1);
    } else {
      res_.AppendInteger(type_timestamp[storage::kLists] * 1000);
    }
  } else if (type_timestamp[storage::kSets] != -2) {
    if (type_timestamp[storage::kSets] == -1) {
      res_.AppendInteger(-1);
    } else {
      res_.AppendInteger(type_timestamp[storage::kSets] * 1000);
    }
  } else if (type_timestamp[storage::kZSets] != -2) {
    if (type_timestamp[storage::kZSets] == -1) {
      res_.AppendInteger(-1);
    } else {
      res_.AppendInteger(type_timestamp[storage::kZSets] * 1000);
    }
  } else {
    // mean this key not exist
    res_.AppendInteger(-2);
  }
}

void PersistCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePersist);
    return;
  }
  key_ = argv_[1];
}

void PersistCmd::Do(std::shared_ptr<Partition> partition) {
  std::map<storage::DataType, rocksdb::Status> type_status;
  int32_t res = partition->db()->Persist(key_, &type_status);
  if (res != -1) {
    res_.AppendInteger(res);
  } else {
    res_.SetRes(CmdRes::kErrOther, "persist internal error");
  }
}

void TypeCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameType);
    return;
  }
  key_ = argv_[1];
}

void TypeCmd::Do(std::shared_ptr<Partition> partition) {
  std::string res;
  rocksdb::Status s = partition->db()->Type(key_, &res);
  if (s.ok()) {
    res_.AppendContent("+" + res);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void ScanCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameScan);
    return;
  }
  if (pstd::string2int(argv_[1].data(), argv_[1].size(), &cursor_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  size_t index = 2;
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
      } else if ((pstd::string2int(argv_[index].data(), argv_[index].size(), &count_) == 0) || count_ <= 0) {
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
    cursor_ret = partition->db()->Scan(storage::DataType::kAll, cursor_ret, pattern_, batch_count, &keys);
    for (const auto& key : keys) {
      RedisAppendLen(raw, key.size(), "$");
      RedisAppendContent(raw, key);
    }
    if (raw.size() >= raw_limit) {
      res_.SetRes(CmdRes::kErrOther, "Response exceeds the max-client-response-size limit");
      return;
    }
    total_key += keys.size();
  } while (cursor_ret != 0 && (left != 0));

  res_.AppendArrayLen(2);

  char buf[32];
  int len = pstd::ll2string(buf, sizeof(buf), cursor_ret);
  res_.AppendStringLen(len);
  res_.AppendContent(buf);

  res_.AppendArrayLen(total_key);
  res_.AppendStringRaw(raw);
}

void ScanxCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameScanx);
    return;
  }
  if (strcasecmp(argv_[1].data(), "string") == 0) {
    type_ = storage::kStrings;
  } else if (strcasecmp(argv_[1].data(), "hash") == 0) {
    type_ = storage::kHashes;
  } else if (strcasecmp(argv_[1].data(), "set") == 0) {
    type_ = storage::kSets;
  } else if (strcasecmp(argv_[1].data(), "zset") == 0) {
    type_ = storage::kZSets;
  } else if (strcasecmp(argv_[1].data(), "list") == 0) {
    type_ = storage::kLists;
  } else {
    res_.SetRes(CmdRes::kInvalidDbType);
    return;
  }

  start_key_ = argv_[2];
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
      } else if ((pstd::string2int(argv_[index].data(), argv_[index].size(), &count_) == 0) || count_ <= 0) {
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
    for (const auto& key : keys) {
      res_.AppendString(key);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  }

void PKSetexAtCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePKSetexAt);
    return;
  }
  key_ = argv_[1];
  value_ = argv_[3];
  if ((pstd::string2int(argv_[2].data(), argv_[2].size(), &time_stamp_) == 0) || time_stamp_ >= INT32_MAX) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
}

void PKSetexAtCmd::Do(std::shared_ptr<Partition> partition) {
  rocksdb::Status s = partition->db()->PKSetexAt(key_, value_, time_stamp_);
  if (s.ok()) {
    res_.SetRes(CmdRes::kOk);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void PKScanRangeCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePKScanRange);
    return;
  }
  if (strcasecmp(argv_[1].data(), "string_with_value") == 0) {
    type_ = storage::kStrings;
    string_with_value = true;
  } else if (strcasecmp(argv_[1].data(), "string") == 0) {
    type_ = storage::kStrings;
  } else if (strcasecmp(argv_[1].data(), "hash") == 0) {
    type_ = storage::kHashes;
  } else if (strcasecmp(argv_[1].data(), "set") == 0) {
    type_ = storage::kSets;
  } else if (strcasecmp(argv_[1].data(), "zset") == 0) {
    type_ = storage::kZSets;
  } else if (strcasecmp(argv_[1].data(), "list") == 0) {
    type_ = storage::kLists;
  } else {
    res_.SetRes(CmdRes::kInvalidDbType);
    return;
  }

  key_start_ = argv_[2];
  key_end_ = argv_[3];
  // start key and end key hash tag have to be same in non classic mode
  if (!HashtagIsConsistent(key_start_, key_start_)) {
    res_.SetRes(CmdRes::kInconsistentHashTag);
    return;
  }
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

void PKScanRangeCmd::Do(std::shared_ptr<Partition> partition) {
  std::string next_key;
  std::vector<std::string> keys;
  std::vector<storage::KeyValue> kvs;
  rocksdb::Status s =
      partition->db()->PKScanRange(type_, key_start_, key_end_, pattern_, limit_, &keys, &kvs, &next_key);

  if (s.ok()) {
    res_.AppendArrayLen(2);
    res_.AppendStringLen(next_key.size());
    res_.AppendContent(next_key);

    if (type_ == storage::kStrings) {
      res_.AppendArrayLen(string_with_value ? 2 * kvs.size() : kvs.size());
      for (const auto& kv : kvs) {
        res_.AppendString(kv.key);
        if (string_with_value) {
          res_.AppendString(kv.value);
        }
      }
    } else {
      res_.AppendArrayLen(keys.size());
      for (const auto& key : keys) {
        res_.AppendString(key);
      }
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void PKRScanRangeCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePKRScanRange);
    return;
  }
  if (strcasecmp(argv_[1].data(), "string_with_value") == 0) {
    type_ = storage::kStrings;
    string_with_value = true;
  } else if (strcasecmp(argv_[1].data(), "string") == 0) {
    type_ = storage::kStrings;
  } else if (strcasecmp(argv_[1].data(), "hash") == 0) {
    type_ = storage::kHashes;
  } else if (strcasecmp(argv_[1].data(), "set") == 0) {
    type_ = storage::kSets;
  } else if (strcasecmp(argv_[1].data(), "zset") == 0) {
    type_ = storage::kZSets;
  } else if (strcasecmp(argv_[1].data(), "list") == 0) {
    type_ = storage::kLists;
  } else {
    res_.SetRes(CmdRes::kInvalidDbType);
    return;
  }

  key_start_ = argv_[2];
  key_end_ = argv_[3];
  // start key and end key hash tag have to be same in non classic mode
  if (!HashtagIsConsistent(key_start_, key_start_)) {
    res_.SetRes(CmdRes::kInconsistentHashTag);
    return;
  }
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

void PKRScanRangeCmd::Do(std::shared_ptr<Partition> partition) {
  std::string next_key;
  std::vector<std::string> keys;
  std::vector<storage::KeyValue> kvs;
  rocksdb::Status s =
      partition->db()->PKRScanRange(type_, key_start_, key_end_, pattern_, limit_, &keys, &kvs, &next_key);

  if (s.ok()) {
    res_.AppendArrayLen(2);
    res_.AppendStringLen(next_key.size());
    res_.AppendContent(next_key);

    if (type_ == storage::kStrings) {
      res_.AppendArrayLen(string_with_value ? 2 * kvs.size() : kvs.size());
      for (const auto& kv : kvs) {
        res_.AppendString(kv.key);
        if (string_with_value) {
          res_.AppendString(kv.value);
        }
      }
    } else {
      res_.AppendArrayLen(keys.size());
      for (const auto& key : keys) {
        res_.AppendString(key);
      }
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}
