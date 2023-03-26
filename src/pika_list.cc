// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_list.h"

#include "pstd/include/pstd_string.h"
#include "include/pika_data_distribution.h"

void LIndexCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLIndex);
    return;
  }
  key_ = argv_[1];
  std::string index = argv_[2];
  if (!pstd::string2l(index.data(), index.size(), &index_)) {
    res_.SetRes(CmdRes::kInvalidInt);
  }
  return;
}
void LIndexCmd::Do(std::shared_ptr<Partition> partition) {
  std::string value;
  rocksdb::Status s = partition->db()->LIndex(key_, index_, &value);
  if (s.ok()) {
    res_.AppendString(value);
  } else if (s.IsNotFound()) {
    res_.AppendStringLen(-1);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void LInsertCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLInsert);
    return;
  }
  key_ = argv_[1];
  std::string dir = argv_[2];
  if (!strcasecmp(dir.data(), "before")) {
    dir_ = blackwidow::Before;
  } else if (!strcasecmp(dir.data(), "after")) {
    dir_ = blackwidow::After;
  } else {
    res_.SetRes(CmdRes::kSyntaxErr);
    return;
  }
  pivot_ = argv_[3];
  value_ = argv_[4];
}
void LInsertCmd::Do(std::shared_ptr<Partition> partition) {
  int64_t llen = 0;
  rocksdb::Status s = partition->db()->LInsert(key_, dir_, pivot_, value_, &llen);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(llen);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void LLenCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLLen);
    return;
  }
  key_ = argv_[1];
}
void LLenCmd::Do(std::shared_ptr<Partition> partition) {
  uint64_t llen = 0;
  rocksdb::Status s = partition->db()->LLen(key_, &llen);
  if (s.ok() || s.IsNotFound()){
    res_.AppendInteger(llen);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void LPushCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLPush);
    return;
  }
  key_ = argv_[1];
  size_t pos = 2;
  while (pos < argv_.size()) {
    values_.push_back(argv_[pos++]);
  }
}
void LPushCmd::Do(std::shared_ptr<Partition> partition) {
  uint64_t llen = 0;
  rocksdb::Status s = partition->db()->LPush(key_, values_, &llen);
  if (s.ok()) {
    res_.AppendInteger(llen);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void LPopCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLPop);
    return;
  }
  key_ = argv_[1];
}
void LPopCmd::Do(std::shared_ptr<Partition> partition) {
  std::string value;
  rocksdb::Status s = partition->db()->LPop(key_, &value);
  if (s.ok()) {
    res_.AppendString(value);
  } else if (s.IsNotFound()) {
    res_.AppendStringLen(-1);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void LPushxCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLPushx);
    return;
  }
  key_ = argv_[1];
  value_ = argv_[2];
}
void LPushxCmd::Do(std::shared_ptr<Partition> partition) {
  uint64_t llen = 0;
  rocksdb::Status s = partition->db()->LPushx(key_, value_, &llen);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(llen);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void LRangeCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLRange);
    return;
  }
  key_ = argv_[1];
  std::string left = argv_[2];
  if (!pstd::string2l(left.data(), left.size(), &left_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  std::string right = argv_[3];
  if (!pstd::string2l(right.data(), right.size(), &right_)) {
    res_.SetRes(CmdRes::kInvalidInt);
  }
  return;
}
void LRangeCmd::Do(std::shared_ptr<Partition> partition) {
  std::vector<std::string> values;
  rocksdb::Status s = partition->db()->LRange(key_, left_, right_, &values);
  if (s.ok()) {
    res_.AppendArrayLen(values.size());
    for (const auto& value : values) {
      res_.AppendString(value);
    }
  } else if (s.IsNotFound()) {
    res_.AppendArrayLen(0);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void LRemCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLRem);
    return;
  }
  key_ = argv_[1];
  std::string count = argv_[2];
  if (!pstd::string2l(count.data(), count.size(), &count_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  value_ = argv_[3];
}
void LRemCmd::Do(std::shared_ptr<Partition> partition) {
  uint64_t res = 0;
  rocksdb::Status s = partition->db()->LRem(key_, count_, value_, &res);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(res);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void LSetCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLSet);
    return;
  }
  key_ = argv_[1];
  std::string index = argv_[2];
  if (!pstd::string2l(index.data(), index.size(), &index_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  value_ = argv_[3];
}
void LSetCmd::Do(std::shared_ptr<Partition> partition) {
    rocksdb::Status s = partition->db()->LSet(key_, index_, value_);
    if (s.ok()) {
      res_.SetRes(CmdRes::kOk);
    } else if (s.IsNotFound()) {
      res_.SetRes(CmdRes::kNotFound);
    } else if (s.IsCorruption() && s.ToString() == "Corruption: index out of range") {
      //TODO refine return value
      res_.SetRes(CmdRes::kOutOfRange);
    } else {
      res_.SetRes(CmdRes::kErrOther, s.ToString());
    }
}

void LTrimCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLSet);
    return;
  }
  key_ = argv_[1];
  std::string start = argv_[2];
  if (!pstd::string2l(start.data(), start.size(), &start_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  std::string stop = argv_[3];
  if (!pstd::string2l(stop.data(), stop.size(), &stop_)) {
    res_.SetRes(CmdRes::kInvalidInt);
  }
  return;
}
void LTrimCmd::Do(std::shared_ptr<Partition> partition) {
  rocksdb::Status s = partition->db()->LTrim(key_, start_, stop_);
  if (s.ok() || s.IsNotFound()) {
    res_.SetRes(CmdRes::kOk);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void RPopCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameRPop);
    return;
  }
  key_ = argv_[1];
}
void RPopCmd::Do(std::shared_ptr<Partition> partition) {
  std::string value;
  rocksdb::Status s = partition->db()->RPop(key_, &value);
  if (s.ok()) {
    res_.AppendString(value);
  } else if (s.IsNotFound()) {
    res_.AppendStringLen(-1);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void RPopLPushCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameRPopLPush);
    return;
  }
  source_ = argv_[1];
  receiver_ = argv_[2];
  if (!HashtagIsConsistent(source_, receiver_)) {
    res_.SetRes(CmdRes::kInconsistentHashTag);
  }
}
void RPopLPushCmd::Do(std::shared_ptr<Partition> partition) {
  std::string value;
  rocksdb::Status s = partition->db()->RPoplpush(source_, receiver_, &value);
  if (s.ok()) {
    res_.AppendString(value);
  } else if (s.IsNotFound()) {
    res_.AppendStringLen(-1);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void RPushCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameRPush);
    return;
  }
  key_ = argv_[1];
  size_t pos = 2;
  while (pos < argv_.size()) {
    values_.push_back(argv_[pos++]);
  }
}
void RPushCmd::Do(std::shared_ptr<Partition> partition) {
  uint64_t llen = 0;
  rocksdb::Status s = partition->db()->RPush(key_, values_, &llen);
  if (s.ok()) {
    res_.AppendInteger(llen);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void RPushxCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameRPushx);
    return;
  }
  key_ = argv_[1];
  value_ = argv_[2];
}
void RPushxCmd::Do(std::shared_ptr<Partition> partition) {
  uint64_t llen = 0;
  rocksdb::Status s = partition->db()->RPushx(key_, value_, &llen);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(llen);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}
