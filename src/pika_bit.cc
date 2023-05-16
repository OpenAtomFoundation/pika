// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_bit.h"

#include "pstd/include/pstd_string.h"

#include "include/pika_define.h"

void BitSetCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameBitSet);
    return;
  }
  key_ = argv_[1];
  if (pstd::string2int(argv_[2].data(), argv_[2].size(), &bit_offset_) == 0) {
    res_.SetRes(CmdRes::kInvalidBitOffsetInt);
    return;
  }
  if (pstd::string2int(argv_[3].data(), argv_[3].size(), &on_) == 0) {
    res_.SetRes(CmdRes::kInvalidBitInt);
    return;
  }
  if (bit_offset_ < 0) {
    res_.SetRes(CmdRes::kInvalidBitOffsetInt);
    return;
  }
  // value no bigger than 2^18
  if ((bit_offset_ >> kMaxBitOpInputBit) > 0) {
    res_.SetRes(CmdRes::kInvalidBitOffsetInt);
    return;
  }
  if ((on_ & ~1) != 0) {
    res_.SetRes(CmdRes::kInvalidBitInt);
    return;
  }
  }

void BitSetCmd::Do(std::shared_ptr<Partition> partition) {
  std::string value;
  int32_t bit_val = 0;
  rocksdb::Status s = partition->db()->SetBit(key_, bit_offset_, on_, &bit_val);
  if (s.ok()) {
    res_.AppendInteger(static_cast<int>(bit_val));
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void BitGetCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameBitGet);
    return;
  }
  key_ = argv_[1];
  if (pstd::string2int(argv_[2].data(), argv_[2].size(), &bit_offset_) == 0) {
    res_.SetRes(CmdRes::kInvalidBitOffsetInt);
    return;
  }
  if (bit_offset_ < 0) {
    res_.SetRes(CmdRes::kInvalidBitOffsetInt);
    return;
  }
  }

void BitGetCmd::Do(std::shared_ptr<Partition> partition) {
  int32_t bit_val = 0;
  rocksdb::Status s = partition->db()->GetBit(key_, bit_offset_, &bit_val);
  if (s.ok()) {
    res_.AppendInteger(static_cast<int>(bit_val));
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void BitCountCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameBitCount);
    return;
  }
  key_ = argv_[1];
  if (argv_.size() == 4) {
    count_all_ = false;
    if (pstd::string2int(argv_[2].data(), argv_[2].size(), &start_offset_) == 0) {
      res_.SetRes(CmdRes::kInvalidInt);
      return;
    }
    if (pstd::string2int(argv_[3].data(), argv_[3].size(), &end_offset_) == 0) {
      res_.SetRes(CmdRes::kInvalidInt);
      return;
    }
  } else if (argv_.size() == 2) {
    count_all_ = true;
  } else {
    res_.SetRes(CmdRes::kSyntaxErr, kCmdNameBitCount);
  }
  }

void BitCountCmd::Do(std::shared_ptr<Partition> partition) {
  int32_t count = 0;
  rocksdb::Status s;
  if (count_all_) {
    s = partition->db()->BitCount(key_, start_offset_, end_offset_, &count, false);
  } else {
    s = partition->db()->BitCount(key_, start_offset_, end_offset_, &count, true);
  }

  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void BitPosCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameBitPos);
    return;
  }
  key_ = argv_[1];
  if (pstd::string2int(argv_[2].data(), argv_[2].size(), &bit_val_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  if ((bit_val_ & ~1) != 0) {
    res_.SetRes(CmdRes::kInvalidBitPosArgument);
    return;
  }
  if (argv_.size() == 3) {
    pos_all_ = true;
    endoffset_set_ = false;
  } else if (argv_.size() == 4) {
    pos_all_ = false;
    endoffset_set_ = false;
    if (pstd::string2int(argv_[3].data(), argv_[3].size(), &start_offset_) == 0) {
      res_.SetRes(CmdRes::kInvalidInt);
      return;
    }
  } else if (argv_.size() == 5) {
    pos_all_ = false;
    endoffset_set_ = true;
    if (pstd::string2int(argv_[3].data(), argv_[3].size(), &start_offset_) == 0) {
      res_.SetRes(CmdRes::kInvalidInt);
      return;
    }
    if (pstd::string2int(argv_[4].data(), argv_[4].size(), &end_offset_) == 0) {
      res_.SetRes(CmdRes::kInvalidInt);
      return;
    }
  } else {
    res_.SetRes(CmdRes::kSyntaxErr, kCmdNameBitPos);
}
}

void BitPosCmd::Do(std::shared_ptr<Partition> partition) {
  int64_t pos = 0;
  rocksdb::Status s;
  if (pos_all_) {
    s = partition->db()->BitPos(key_, bit_val_, &pos);
  } else if (!pos_all_ && !endoffset_set_) {
    s = partition->db()->BitPos(key_, bit_val_, start_offset_, &pos);
  } else if (!pos_all_ && endoffset_set_) {
    s = partition->db()->BitPos(key_, bit_val_, start_offset_, end_offset_, &pos);
  }
  if (s.ok()) {
    res_.AppendInteger(static_cast<int>(pos));
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void BitOpCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameBitOp);
    return;
  }
  std::string op_str = argv_[1];
  if (strcasecmp(op_str.data(), "not") == 0) {
    op_ = storage::kBitOpNot;
  } else if (strcasecmp(op_str.data(), "and") == 0) {
    op_ = storage::kBitOpAnd;
  } else if (strcasecmp(op_str.data(), "or") == 0) {
    op_ = storage::kBitOpOr;
  } else if (strcasecmp(op_str.data(), "xor") == 0) {
    op_ = storage::kBitOpXor;
  } else {
    res_.SetRes(CmdRes::kSyntaxErr, kCmdNameBitOp);
    return;
  }
  if (op_ == storage::kBitOpNot && argv_.size() != 4) {
    res_.SetRes(CmdRes::kWrongBitOpNotNum, kCmdNameBitOp);
    return;
  } else if (op_ != storage::kBitOpNot && argv_.size() < 4) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameBitOp);
    return;
  } else if (argv_.size() >= kMaxBitOpInputKey) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameBitOp);
    return;
  }

  dest_key_ = argv_[2];
  for (unsigned int i = 3; i <= argv_.size() - 1; i++) {
    src_keys_.emplace_back(argv_[i].data());
  }
  }

void BitOpCmd::Do(std::shared_ptr<Partition> partition) {
  int64_t result_length;
  rocksdb::Status s = partition->db()->BitOp(op_, dest_key_, src_keys_, &result_length);
  if (s.ok()) {
    res_.AppendInteger(result_length);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}
