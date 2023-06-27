// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_list.h"
#include "include/pika_data_distribution.h"
#include "pstd/include/pstd_string.h"

void LIndexCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLIndex);
    return;
  }
  key_ = argv_[1];
  std::string index = argv_[2];
  if (pstd::string2int(index.data(), index.size(), &index_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
  }
}
void LIndexCmd::Do(std::shared_ptr<Slot> slot) {
  std::string value;
  rocksdb::Status s = slot->db()->LIndex(key_, index_, &value);
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
  if (strcasecmp(dir.data(), "before") == 0) {
    dir_ = storage::Before;
  } else if (strcasecmp(dir.data(), "after") == 0) {
    dir_ = storage::After;
  } else {
    res_.SetRes(CmdRes::kSyntaxErr);
    return;
  }
  pivot_ = argv_[3];
  value_ = argv_[4];
}
void LInsertCmd::Do(std::shared_ptr<Slot> slot) {
  int64_t llen = 0;
  rocksdb::Status s = slot->db()->LInsert(key_, dir_, pivot_, value_, &llen);
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
void LLenCmd::Do(std::shared_ptr<Slot> slot) {
  uint64_t llen = 0;
  rocksdb::Status s = slot->db()->LLen(key_, &llen);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(static_cast<int64_t>(llen));
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
void LPushCmd::Do(std::shared_ptr<Slot> slot) {
  uint64_t llen = 0;
  rocksdb::Status s = slot->db()->LPush(key_, values_, &llen);
  if (s.ok()) {
    res_.AppendInteger(static_cast<int64_t>(llen));
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
void LPopCmd::Do(std::shared_ptr<Slot> slot) {
  std::string value;
  rocksdb::Status s = slot->db()->LPop(key_, &value);
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
  size_t pos = 2;
  while (pos < argv_.size()) {
    values_.push_back(argv_[pos++]);
  }
}
void LPushxCmd::Do(std::shared_ptr<Slot> slot) {
  uint64_t llen = 0;
  rocksdb::Status s = slot->db()->LPushx(key_, values_, &llen);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(static_cast<int64_t>(llen));
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
  if (pstd::string2int(left.data(), left.size(), &left_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  std::string right = argv_[3];
  if (pstd::string2int(right.data(), right.size(), &right_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
  }
}
void LRangeCmd::Do(std::shared_ptr<Slot> slot) {
  std::vector<std::string> values;
  rocksdb::Status s = slot->db()->LRange(key_, left_, right_, &values);
  if (s.ok()) {
    res_.AppendArrayLenUint64(values.size());
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
  if (pstd::string2int(count.data(), count.size(), &count_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  value_ = argv_[3];
}
void LRemCmd::Do(std::shared_ptr<Slot> slot) {
  uint64_t res = 0;
  rocksdb::Status s = slot->db()->LRem(key_, count_, value_, &res);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(static_cast<int64_t>(res));
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
  if (pstd::string2int(index.data(), index.size(), &index_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  value_ = argv_[3];
}
void LSetCmd::Do(std::shared_ptr<Slot> slot) {
  rocksdb::Status s = slot->db()->LSet(key_, index_, value_);
  if (s.ok()) {
    res_.SetRes(CmdRes::kOk);
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kNotFound);
  } else if (s.IsCorruption() && s.ToString() == "Corruption: index out of range") {
    // TODO(): refine return value
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
  if (pstd::string2int(start.data(), start.size(), &start_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  std::string stop = argv_[3];
  if (pstd::string2int(stop.data(), stop.size(), &stop_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
  }
}
void LTrimCmd::Do(std::shared_ptr<Slot> slot) {
  rocksdb::Status s = slot->db()->LTrim(key_, start_, stop_);
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
void RPopCmd::Do(std::shared_ptr<Slot> slot) {
  std::string value;
  rocksdb::Status s = slot->db()->RPop(key_, &value);
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
void RPopLPushCmd::Do(std::shared_ptr<Slot> slot) {
  std::string value;
  rocksdb::Status s = slot->db()->RPoplpush(source_, receiver_, &value);
  if (s.ok()) {
    res_.AppendString(value);
    value_poped_from_source_ = value;
    is_write_binlog_ = true;
  } else if (s.IsNotFound()) {
    // no actual write operation happened, will not write binlog
    res_.AppendStringLen(-1);
    is_write_binlog_ = false;
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void RPopLPushCmd::DoBinlog(const std::shared_ptr<SyncMasterSlot>& slot) {
  if(!is_write_binlog_){
    return;
  }
  PikaCmdArgsType rpop_args;
  rpop_args.push_back("RPOP");
  rpop_args.push_back(source_);
  rpop_cmd_->Initial(rpop_args, db_name_);

  PikaCmdArgsType lpush_args;
  lpush_args.push_back("LPUSH");
  lpush_args.push_back(receiver_);
  lpush_args.push_back(value_poped_from_source_);
  lpush_cmd_->Initial(lpush_args, db_name_);

  rpop_cmd_->SetConn(GetConn());
  rpop_cmd_->SetResp(resp_.lock());
  lpush_cmd_->SetConn(GetConn());
  lpush_cmd_->SetResp(resp_.lock());

  rpop_cmd_->DoBinlog(slot);
  lpush_cmd_->DoBinlog(slot);
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
void RPushCmd::Do(std::shared_ptr<Slot> slot) {
  uint64_t llen = 0;
  rocksdb::Status s = slot->db()->RPush(key_, values_, &llen);
  if (s.ok()) {
    res_.AppendInteger(static_cast<int64_t>(llen));
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
  size_t pos = 2;
  while (pos < argv_.size()) {
    values_.push_back(argv_[pos++]);
  }
}
void RPushxCmd::Do(std::shared_ptr<Slot> slot) {
  uint64_t llen = 0;
  rocksdb::Status s = slot->db()->RPushx(key_, values_, &llen);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(static_cast<int64_t>(llen));
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}
