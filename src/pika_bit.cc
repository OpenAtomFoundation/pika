// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <limits>
#include "slash/include/slash_string.h"
#include "include/pika_bit.h"
#include "include/pika_server.h"

extern PikaServer *g_pika_server;

void BitSetCmd::DoInitial(const PikaCmdArgsType& argv) {
  if (!CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameBitSet);
    return;
  }
  key_ = argv[1];
  if (!slash::string2l(argv[2].data(), argv[2].size(), &bit_offset_)) {
    res_.SetRes(CmdRes::kInvalidBitOffsetInt);
    return;
  }
  if (!slash::string2l(argv[3].data(), argv[3].size(), &on_)) {
    res_.SetRes(CmdRes::kInvalidBitInt);
    return;
  }
  if (bit_offset_ < 0) {
    res_.SetRes(CmdRes::kInvalidBitOffsetInt);
    return;
  }
  // value no bigger than 2^18
  if ( (bit_offset_ >> kMaxBitOpInputBit) > 0) {
    res_.SetRes(CmdRes::kInvalidBitOffsetInt);
    return;
  }
  if (on_ & ~1) {
    res_.SetRes(CmdRes::kInvalidBitInt);
    return;
  }
  return;
}

void BitSetCmd::Do() {
  std::string value;
  int32_t bit_val = 0;
  rocksdb::Status s = g_pika_server->db()->SetBit(key_, bit_offset_, on_, &bit_val);
  if (s.ok()){
    res_.AppendInteger((int)bit_val);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void BitGetCmd::DoInitial(const PikaCmdArgsType& argv) {
  if (!CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameBitGet);
    return;
  }
  key_ = argv[1];
  if (!slash::string2l(argv[2].data(), argv[2].size(), &bit_offset_)) {
    res_.SetRes(CmdRes::kInvalidBitOffsetInt);
    return;
  }
  if (bit_offset_ < 0) {
    res_.SetRes(CmdRes::kInvalidBitOffsetInt);
    return;
  }
  return;
}

void BitGetCmd::Do() {
  int32_t bit_val = 0;
  rocksdb::Status s = g_pika_server->db()->GetBit(key_, bit_offset_, &bit_val);
  if (s.ok()) {
    res_.AppendInteger((int)bit_val);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void BitCountCmd::DoInitial(const PikaCmdArgsType& argv) {
  if (!CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameBitCount);
    return;
  }
  key_ = argv[1];
  if (argv.size() == 4) {
    count_all_ = false;
    if (!slash::string2l(argv[2].data(), argv[2].size(), &start_offset_)) {
      res_.SetRes(CmdRes::kInvalidInt);
      return;
    }
    if (!slash::string2l(argv[3].data(), argv[3].size(), &end_offset_)) {
      res_.SetRes(CmdRes::kInvalidInt);
      return;
    }
  } else if (argv.size() == 2) {
    count_all_ = true;
  } else {
    res_.SetRes(CmdRes::kSyntaxErr, kCmdNameBitCount);
  }
  return;
}

void BitCountCmd::Do() {
  int32_t count = 0;
  rocksdb::Status s;
  if (count_all_) {
    s = g_pika_server->db()->BitCount(key_, start_offset_, end_offset_, &count, false);
  } else {
    s = g_pika_server->db()->BitCount(key_, start_offset_, end_offset_, &count, true);
  }

  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void BitPosCmd::DoInitial(const PikaCmdArgsType& argv) {
  if (!CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameBitPos);
    return;
  }
  key_ = argv[1];
  if (!slash::string2l(argv[2].data(), argv[2].size(), &bit_val_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  if (bit_val_ & ~1) {
    res_.SetRes(CmdRes::kInvalidBitPosArgument);
    return;
  }
  if (argv.size() == 3) {
    pos_all_ = true;
    endoffset_set_ = false;
  } else if (argv.size() == 4) {
    pos_all_ = false;
    endoffset_set_ = false;
    if (!slash::string2l(argv[3].data(), argv[3].size(), &start_offset_)) {
      res_.SetRes(CmdRes::kInvalidInt);
      return;
    } 
  } else if (argv.size() == 5) {
    pos_all_ = false;
    endoffset_set_ = true;
    if (!slash::string2l(argv[3].data(), argv[3].size(), &start_offset_)) {
      res_.SetRes(CmdRes::kInvalidInt);
      return;
    } 
    if (!slash::string2l(argv[4].data(), argv[4].size(), &end_offset_)) {
      res_.SetRes(CmdRes::kInvalidInt);
      return;
    }
  } else
    res_.SetRes(CmdRes::kSyntaxErr, kCmdNameBitPos);
  return;
}

void BitPosCmd::Do() {
  int64_t pos = 0;
  rocksdb::Status s;
  if (pos_all_) {
    s = g_pika_server->db()->BitPos(key_, bit_val_, &pos);
  } else if (!pos_all_ && !endoffset_set_) {
    s = g_pika_server->db()->BitPos(key_, bit_val_, start_offset_, &pos);
  } else if (!pos_all_ && endoffset_set_) {
    s = g_pika_server->db()->BitPos(key_, bit_val_, start_offset_, end_offset_, &pos);
  }
  if (s.ok()) {
    res_.AppendInteger((int)pos);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void BitOpCmd::DoInitial(const PikaCmdArgsType& argv) {
  if (!CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameBitOp);
    return;
  }
  std::string op_str = argv[1];
  if (!strcasecmp(op_str.data(), "not")) {
    op_ = blackwidow::kBitOpNot;
  } else if (!strcasecmp(op_str.data(), "and")) {
    op_ = blackwidow::kBitOpAnd;
  } else if (!strcasecmp(op_str.data(), "or")) {
    op_ = blackwidow::kBitOpOr;
  } else if (!strcasecmp(op_str.data(), "xor")) {
    op_ = blackwidow::kBitOpXor;
  } else {
    res_.SetRes(CmdRes::kSyntaxErr, kCmdNameBitOp);
    return;
  }
  if (op_ == blackwidow::kBitOpNot && argv.size() != 4) {
      res_.SetRes(CmdRes::kWrongBitOpNotNum, kCmdNameBitOp);
      return;
  } else if (op_ != blackwidow::kBitOpNot && argv.size() < 4) {
      res_.SetRes(CmdRes::kWrongNum, kCmdNameBitOp);
      return;
  } else if (argv.size() >= kMaxBitOpInputKey) {
      res_.SetRes(CmdRes::kWrongNum, kCmdNameBitOp);
      return;
  }

  dest_key_ = argv[2].data();
  for(unsigned int i = 3; i <= argv.size() - 1; i++) {
      src_keys_.push_back(argv[i].data());
  }
  return;
}

void BitOpCmd::Do() {
  int64_t result_length;
  rocksdb::Status s = g_pika_server->db()->BitOp(op_, dest_key_, src_keys_, &result_length);
  if (s.ok()) {
    res_.AppendInteger(result_length);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}
