// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <vector>

#include "slash/include/slash_string.h"
#include "include/pika_list.h"
#include "include/pika_server.h"

extern PikaServer *g_pika_server;

void LIndexCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLIndex);
    return;
  }
  key_ = argv[1];
  std::string index = argv[2];
  if (!slash::string2l(index.data(), index.size(), &index_)) {
    res_.SetRes(CmdRes::kInvalidInt);
  }
  return;
}
void LIndexCmd::Do() {
  std::string value;
  rocksdb::Status s = g_pika_server->db()->LIndex(key_, index_, &value);
  if (s.ok()) {
    res_.AppendString(value);
  } else if (s.IsNotFound()) {
    res_.AppendStringLen(-1);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void LInsertCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLInsert);
    return;
  }
  key_ = argv[1];
  std::string dir = argv[2];
  if (!strcasecmp(dir.data(), "before")) {
    dir_ = blackwidow::Before;
  } else if (!strcasecmp(dir.data(), "after")) {
    dir_ = blackwidow::After;
  } else {
    res_.SetRes(CmdRes::kSyntaxErr);
    return;
  }
  pivot_ = argv[3];
  value_ = argv[4];
}
void LInsertCmd::Do() {
  int64_t llen = 0;
  rocksdb::Status s = g_pika_server->db()->LInsert(key_, dir_, pivot_, value_, &llen);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(llen);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void LLenCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLLen);
    return;
  }
  key_ = argv[1];
}
void LLenCmd::Do() {
  uint64_t llen = 0;
  rocksdb::Status s = g_pika_server->db()->LLen(key_, &llen);
  if (s.ok() || s.IsNotFound()){
    res_.AppendInteger(llen);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void LPushCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLPush);
    return;
  }
  key_ = argv[1];
  size_t pos = 2;
  while (pos < argv.size()) {
    values_.push_back(argv[pos++]);
  }
}
void LPushCmd::Do() {
  uint64_t llen = 0;
  rocksdb::Status s = g_pika_server->db()->LPush(key_, values_, &llen);
  if (s.ok()) {
    res_.AppendInteger(llen);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void LPopCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLPop);
    return;
  }
  key_ = argv[1];
}
void LPopCmd::Do() {
  std::string value;
  rocksdb::Status s = g_pika_server->db()->LPop(key_, &value);
  if (s.ok()) {
    res_.AppendString(value);
  } else if (s.IsNotFound()) {
    res_.AppendStringLen(-1);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void LPushxCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLPushx);
    return;
  }
  key_ = argv[1];
  value_ = argv[2];
}
void LPushxCmd::Do() {
  uint64_t llen = 0;
  rocksdb::Status s = g_pika_server->db()->LPushx(key_, value_, &llen);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(llen);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void LRangeCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLRange);
    return;
  }
  key_ = argv[1];
  std::string left = argv[2];
  if (!slash::string2l(left.data(), left.size(), &left_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  std::string right = argv[3];
  if (!slash::string2l(right.data(), right.size(), &right_)) {
    res_.SetRes(CmdRes::kInvalidInt);
  }
  return;
}
void LRangeCmd::Do() {
  std::vector<std::string> values;
  rocksdb::Status s = g_pika_server->db()->LRange(key_, left_, right_, &values);
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

void LRemCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLRem);
    return;
  }
  key_ = argv[1];
  std::string count = argv[2];
  if (!slash::string2l(count.data(), count.size(), &count_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  value_ = argv[3];
}
void LRemCmd::Do() {
  uint64_t res = 0;
  rocksdb::Status s = g_pika_server->db()->LRem(key_, count_, value_, &res);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(res);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void LSetCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLSet);
    return;
  }
  key_ = argv[1];
  std::string index = argv[2];
  if (!slash::string2l(index.data(), index.size(), &index_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  value_ = argv[3];
}
void LSetCmd::Do() {
    rocksdb::Status s = g_pika_server->db()->LSet(key_, index_, value_);
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

void LTrimCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLSet);
    return;
  }
  key_ = argv[1];
  std::string start = argv[2];
  if (!slash::string2l(start.data(), start.size(), &start_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  std::string stop = argv[3];
  if (!slash::string2l(stop.data(), stop.size(), &stop_)) {
    res_.SetRes(CmdRes::kInvalidInt);
  }
  return;
}
void LTrimCmd::Do() {
  rocksdb::Status s = g_pika_server->db()->LTrim(key_, start_, stop_);
  if (s.ok() || s.IsNotFound()) {
    res_.SetRes(CmdRes::kOk);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void RPopCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameRPop);
    return;
  }
  key_ = argv[1];
}
void RPopCmd::Do() {
  std::string value;
  rocksdb::Status s = g_pika_server->db()->RPop(key_, &value);
  if (s.ok()) {
    res_.AppendString(value);
  } else if (s.IsNotFound()) {
    res_.AppendStringLen(-1);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void RPopLPushCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameRPopLPush);
    return;
  }
  source_ = argv[1];
  receiver_ = argv[2];
}
void RPopLPushCmd::Do() {
  std::string value;
  rocksdb::Status s = g_pika_server->db()->RPoplpush(source_, receiver_, &value);
  if (s.ok()) {
    res_.AppendString(value);
  } else if (s.IsNotFound()) {
    res_.AppendStringLen(-1);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void RPushCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameRPush);
    return;
  }
  key_ = argv[1];
  size_t pos = 2;
  while (pos < argv.size()) {
    values_.push_back(argv[pos++]);
  }
}
void RPushCmd::Do() {
  uint64_t llen = 0;
  rocksdb::Status s = g_pika_server->db()->RPush(key_, values_, &llen);
  if (s.ok()) {
    res_.AppendInteger(llen);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void RPushxCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameRPushx);
    return;
  }
  key_ = argv[1];
  value_ = argv[2];
}
void RPushxCmd::Do() {
  uint64_t llen = 0;
  rocksdb::Status s = g_pika_server->db()->RPushx(key_, value_, &llen);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(llen);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}
