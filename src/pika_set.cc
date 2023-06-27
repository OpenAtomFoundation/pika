// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_set.h"

#include "include/pika_slot_command.h"
#include "pstd/include/pstd_string.h"

void SAddCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSAdd);
    return;
  }
  key_ = argv_[1];
  auto iter = argv_.begin();
  iter++;
  iter++;
  members_.assign(iter, argv_.end());
}

void SAddCmd::Do(std::shared_ptr<Slot> slot) {
  int32_t count = 0;
  rocksdb::Status s = slot->db()->SAdd(key_, members_, &count);
  if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    AddSlotKey("s", key_, slot);
    return;
  }
  res_.AppendInteger(count);
}

void SPopCmd::DoInitial() {
  size_t argc = argv_.size();
  size_t index = 2;
  if (!CheckArg(argc)) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSPop);
    return;
  }

  key_ = argv_[1];
  count_ = 1;

  if (index < argc) {
    if (pstd::string2int(argv_[index].data(), argv_[index].size(), &count_) == 0) {
      res_.SetRes(CmdRes::kErrOther, kCmdNameSPop);
      return;
    }
    if (count_ <= 0) {
      res_.SetRes(CmdRes::kErrOther, kCmdNameSPop);
      return;
    }
  }
}

void SPopCmd::Do(std::shared_ptr<Slot> slot) {
  std::vector<std::string> members;
  rocksdb::Status s = slot->db()->SPop(key_, &members, count_);
  if (s.ok()) {
    res_.AppendArrayLenUint64(members.size());
    for (const auto& member : members) {
      res_.AppendStringLenUint64(member.size());
      res_.AppendContent(member);
      AddSlotKey("s", key_, slot);
    }
  } else if (s.IsNotFound()) {
    res_.AppendContent("$-1");
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void SCardCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSCard);
    return;
  }
  key_ = argv_[1];
}

void SCardCmd::Do(std::shared_ptr<Slot> slot) {
  int32_t card = 0;
  rocksdb::Status s = slot->db()->SCard(key_, &card);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(card);
  } else {
    res_.SetRes(CmdRes::kErrOther, "scard error");
  }
}

void SMembersCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSMembers);
    return;
  }
  key_ = argv_[1];
}

void SMembersCmd::Do(std::shared_ptr<Slot> slot) {
  std::vector<std::string> members;
  rocksdb::Status s = slot->db()->SMembers(key_, &members);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendArrayLenUint64(members.size());
    for (const auto& member : members) {
      res_.AppendStringLenUint64(member.size());
      res_.AppendContent(member);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void SScanCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSScan);
    return;
  }
  key_ = argv_[1];
  if (pstd::string2int(argv_[2].data(), argv_[2].size(), &cursor_) == 0) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSScan);
    return;
  }
  size_t argc = argv_.size();
  size_t index = 3;
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
      } else if (pstd::string2int(argv_[index].data(), argv_[index].size(), &count_) == 0) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
      }
    } else {
      res_.SetRes(CmdRes::kSyntaxErr);
      return;
    }
    index++;
  }
  if (count_ < 0) {
    res_.SetRes(CmdRes::kSyntaxErr);
    return;
  }
}

void SScanCmd::Do(std::shared_ptr<Slot> slot) {
  int64_t next_cursor = 0;
  std::vector<std::string> members;
  rocksdb::Status s = slot->db()->SScan(key_, cursor_, pattern_, count_, &members, &next_cursor);

  if (s.ok() || s.IsNotFound()) {
    res_.AppendContent("*2");
    char buf[32];
    int64_t len = pstd::ll2string(buf, sizeof(buf), next_cursor);
    res_.AppendStringLen(len);
    res_.AppendContent(buf);

    res_.AppendArrayLenUint64(members.size());
    for (const auto& member : members) {
      res_.AppendString(member);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void SRemCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSMembers);
    return;
  }
  key_ = argv_[1];
  auto iter = argv_.begin();
  iter++;
  members_.assign(++iter, argv_.end());
}

void SRemCmd::Do(std::shared_ptr<Slot> slot) {
  int32_t count = 0;
  rocksdb::Status s = slot->db()->SRem(key_, members_, &count);
  res_.AppendInteger(count);
  AddSlotKey("s", key_, slot);
}

void SUnionCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSUnion);
    return;
  }
  auto iter = argv_.begin();
  keys_.assign(++iter, argv_.end());
}

void SUnionCmd::Do(std::shared_ptr<Slot> slot) {
  std::vector<std::string> members;
  slot->db()->SUnion(keys_, &members);
  res_.AppendArrayLenUint64(members.size());
  for (const auto& member : members) {
    res_.AppendStringLenUint64(member.size());
    res_.AppendContent(member);
  }
}

void SUnionstoreCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSUnionstore);
    return;
  }
  dest_key_ = argv_[1];
  auto iter = argv_.begin();
  iter++;
  keys_.assign(++iter, argv_.end());
}

void SUnionstoreCmd::Do(std::shared_ptr<Slot> slot) {
  int32_t count = 0;
  rocksdb::Status s = slot->db()->SUnionstore(dest_key_, keys_, &count);
  if (s.ok()) {
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void SInterCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSInter);
    return;
  }
  auto iter = argv_.begin();
  keys_.assign(++iter, argv_.end());
}

void SInterCmd::Do(std::shared_ptr<Slot> slot) {
  std::vector<std::string> members;
  slot->db()->SInter(keys_, &members);
  res_.AppendArrayLenUint64(members.size());
  for (const auto& member : members) {
    res_.AppendStringLenUint64(member.size());
    res_.AppendContent(member);
  }
}

void SInterstoreCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSInterstore);
    return;
  }
  dest_key_ = argv_[1];
  auto iter = argv_.begin();
  iter++;
  keys_.assign(++iter, argv_.end());
}

void SInterstoreCmd::Do(std::shared_ptr<Slot> slot) {
  int32_t count = 0;
  rocksdb::Status s = slot->db()->SInterstore(dest_key_, keys_, &count);
  if (s.ok()) {
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void SIsmemberCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSIsmember);
    return;
  }
  key_ = argv_[1];
  member_ = argv_[2];
}

void SIsmemberCmd::Do(std::shared_ptr<Slot> slot) {
  int32_t is_member = 0;
  slot->db()->SIsmember(key_, member_, &is_member);
  if (is_member != 0) {
    res_.AppendContent(":1");
  } else {
    res_.AppendContent(":0");
  }
}

void SDiffCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSDiff);
    return;
  }
  auto iter = argv_.begin();
  keys_.assign(++iter, argv_.end());
}

void SDiffCmd::Do(std::shared_ptr<Slot> slot) {
  std::vector<std::string> members;
  slot->db()->SDiff(keys_, &members);
  res_.AppendArrayLenUint64(members.size());
  for (const auto& member : members) {
    res_.AppendStringLenUint64(member.size());
    res_.AppendContent(member);
  }
}

void SDiffstoreCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSDiffstore);
    return;
  }
  dest_key_ = argv_[1];
  auto iter = argv_.begin();
  iter++;
  keys_.assign(++iter, argv_.end());
}

void SDiffstoreCmd::Do(std::shared_ptr<Slot> slot) {
  int32_t count = 0;
  rocksdb::Status s = slot->db()->SDiffstore(dest_key_, keys_, &count);
  if (s.ok()) {
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void SMoveCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSMove);
    return;
  }
  src_key_ = argv_[1];
  dest_key_ = argv_[2];
  member_ = argv_[3];
}

void SMoveCmd::Do(std::shared_ptr<Slot> slot) {
  int32_t res = 0;
  rocksdb::Status s = slot->db()->SMove(src_key_, dest_key_, member_, &res);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(res);
    AddSlotKey("s", src_key_, slot);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void SRandmemberCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSRandmember);
    return;
  }
  key_ = argv_[1];
  if (argv_.size() > 3) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSRandmember);
    return;
  } else if (argv_.size() == 3) {
    if (pstd::string2int(argv_[2].data(), argv_[2].size(), &count_) == 0) {
      res_.SetRes(CmdRes::kInvalidInt);
    } else {
      reply_arr = true;
      ;
    }
  }
}

void SRandmemberCmd::Do(std::shared_ptr<Slot> slot) {
  std::vector<std::string> members;
  rocksdb::Status s = slot->db()->SRandmember(key_, static_cast<int32_t>(count_), &members);
  if (s.ok() || s.IsNotFound()) {
    if (!reply_arr && (static_cast<unsigned int>(!members.empty()) != 0U)) {
      res_.AppendStringLenUint64(members[0].size());
      res_.AppendContent(members[0]);
    } else {
      res_.AppendArrayLenUint64(members.size());
      for (const auto& member : members) {
        res_.AppendStringLenUint64(member.size());
        res_.AppendContent(member);
      }
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}
