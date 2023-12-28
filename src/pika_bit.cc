// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_bit.h"

#include "pstd/include/pstd_string.h"

#include "include/pika_define.h"
#include "include/pika_slot_command.h"
#include "include/pika_cache.h"

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

void BitSetCmd::Do(std::shared_ptr<Slot> slot) {
  std::string value;
  int32_t bit_val = 0;
  s_ = slot->db()->SetBit(key_, bit_offset_, static_cast<int32_t>(on_), &bit_val);
  if (s_.ok()) {
    res_.AppendInteger(static_cast<int>(bit_val));
    AddSlotKey("k", key_, slot);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void BitSetCmd::DoThroughDB(std::shared_ptr<Slot> slot) {
  Do(slot);
}

void BitSetCmd::DoUpdateCache(std::shared_ptr<Slot> slot) {
  if (s_.ok()) {
    std::string CachePrefixKeyK = PCacheKeyPrefixK + key_;
    slot->cache()->SetBitIfKeyExist(CachePrefixKeyK, bit_offset_, on_);
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

void BitGetCmd::Do(std::shared_ptr<Slot> slot) {
  int32_t bit_val = 0;
  s_ = slot->db()->GetBit(key_, bit_offset_, &bit_val);
  if (s_.ok()) {
    res_.AppendInteger(static_cast<int>(bit_val));
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void BitGetCmd::ReadCache(std::shared_ptr<Slot> slot) {
  int64_t bit_val = 0;
  std::string CachePrefixKeyK = PCacheKeyPrefixK + key_;
  auto s = slot->cache()->GetBit(CachePrefixKeyK, bit_offset_, &bit_val);
  if (s.ok()) {
    res_.AppendInteger(bit_val);
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void BitGetCmd::DoThroughDB(std::shared_ptr<Slot> slot) {
  res_.clear();
  Do(slot);
}

void BitGetCmd::DoUpdateCache(std::shared_ptr<Slot> slot){
  if (s_.ok()) {
    slot->cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_KV, key_, slot);
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

void BitCountCmd::Do(std::shared_ptr<Slot> slot) {
  int32_t count = 0;
  if (count_all_) {
    s_ = slot->db()->BitCount(key_, start_offset_, end_offset_, &count, false);
  } else {
    s_ = slot->db()->BitCount(key_, start_offset_, end_offset_, &count, true);
  }

  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void BitCountCmd::ReadCache(std::shared_ptr<Slot> slot) {
  int64_t count = 0;
  int64_t start = static_cast<long>(start_offset_);
  int64_t end = static_cast<long>(end_offset_);
  rocksdb::Status s;
  std::string CachePrefixKeyK = PCacheKeyPrefixK + key_;
  if (count_all_) {
    s = slot->cache()->BitCount(CachePrefixKeyK, start, end, &count, 0);
  } else {
    s = slot->cache()->BitCount(CachePrefixKeyK, start, end, &count, 1);
  }

  if (s.ok()) {
    res_.AppendInteger(count);
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void BitCountCmd::DoThroughDB(std::shared_ptr<Slot> slot) {
  res_.clear();
  Do(slot);
}

void BitCountCmd::DoUpdateCache(std::shared_ptr<Slot> slot) {
  if (s_.ok()) {
    slot->cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_KV, key_, slot);
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

void BitPosCmd::Do(std::shared_ptr<Slot> slot) {
  int64_t pos = 0;
  rocksdb::Status s;
  if (pos_all_) {
    s_ = slot->db()->BitPos(key_, static_cast<int32_t>(bit_val_), &pos);
  } else if (!pos_all_ && !endoffset_set_) {
    s_ = slot->db()->BitPos(key_, static_cast<int32_t>(bit_val_), start_offset_, &pos);
  } else if (!pos_all_ && endoffset_set_) {
    s_ = slot->db()->BitPos(key_, static_cast<int32_t>(bit_val_), start_offset_, end_offset_, &pos);
  }
  if (s_.ok()) {
    res_.AppendInteger(static_cast<int>(pos));
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void BitPosCmd::ReadCache(std::shared_ptr<Slot> slot) {
  int64_t pos = 0;
  rocksdb::Status s;
  int64_t bit = static_cast<long>(bit_val_);
  int64_t start = static_cast<long>(start_offset_);
  int64_t end = static_cast<long>(end_offset_);\
  std::string CachePrefixKeyK = PCacheKeyPrefixK + key_;
  if (pos_all_) {
    s = slot->cache()->BitPos(CachePrefixKeyK, bit, &pos);
  } else if (!pos_all_ && !endoffset_set_) {
    s = slot->cache()->BitPos(CachePrefixKeyK, bit, start, &pos);
  } else if (!pos_all_ && endoffset_set_) {
    s = slot->cache()->BitPos(CachePrefixKeyK, bit, start, end, &pos);
  }
  if (s.ok()) {
    res_.AppendInteger(pos);
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void BitPosCmd::DoThroughDB(std::shared_ptr<Slot> slot) {
  res_.clear();
  Do(slot);
}

void BitPosCmd::DoUpdateCache(std::shared_ptr<Slot> slot) {
  if (s_.ok()) {
    slot->cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_KV, key_, slot);
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
  for (size_t i = 3; i <= argv_.size() - 1; i++) {
    src_keys_.emplace_back(argv_[i].data());
  }
}

void BitOpCmd::Do(std::shared_ptr<Slot> slot) {
  int64_t result_length = 0;
  s_ = slot->db()->BitOp(op_, dest_key_, src_keys_, value_to_dest_, &result_length);
  if (s_.ok()) {
    res_.AppendInteger(result_length);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void BitOpCmd::DoThroughDB(std::shared_ptr<Slot> slot) {
  Do(slot);
}

void BitOpCmd::DoUpdateCache(std::shared_ptr<Slot> slot) {
  if (s_.ok()) {
    std::vector<std::string> v;
    v.emplace_back(PCacheKeyPrefixK + dest_key_);
    slot->cache()->Del(v);
  }
}

void BitOpCmd::DoBinlog(const std::shared_ptr<SyncMasterSlot>& slot) {
  PikaCmdArgsType set_args;
  //used "set" instead of "SET" to distinguish the binlog of SetCmd
  set_args.emplace_back("set");
  set_args.emplace_back(dest_key_);
  set_args.emplace_back(value_to_dest_);
  set_cmd_->Initial(set_args, db_name_);
  set_cmd_->SetConn(GetConn());
  set_cmd_->SetResp(resp_.lock());
  //value of this binlog might be strange if you print it out(eg. set bitkey_out1 «ѦFO<t·), but it's ok.
  set_cmd_->DoBinlog(slot);
}
