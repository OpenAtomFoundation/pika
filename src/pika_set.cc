// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "slash/include/slash_string.h"
#include "include/pika_set.h"

void SAddCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSAdd);
    return;
  }
  key_ = argv_[1];
  PikaCmdArgsType::iterator iter = argv_.begin();
  iter++; 
  iter++;
  members_.assign(iter, argv_.end());
  return;
}

void SAddCmd::Do(std::shared_ptr<Partition> partition) {
  int32_t count = 0;
  rocksdb::Status s = partition->db()->SAdd(key_, members_, &count);
  if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }
  res_.AppendInteger(count);
  return;
}

void SPopCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSPop);
    return;
  }
  key_ = argv_[1];
  return;
}

void SPopCmd::Do(std::shared_ptr<Partition> partition) {
  std::string member;
  rocksdb::Status s = partition->db()->SPop(key_, &member);
  if (s.ok()) {
    res_.AppendStringLen(member.size());
    res_.AppendContent(member);
  } else if (s.IsNotFound()) {
    res_.AppendContent("$-1");
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void SCardCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSCard);
    return;
  }
  key_ = argv_[1];
  return;
}

void SCardCmd::Do(std::shared_ptr<Partition> partition) {
  int32_t card = 0;
  rocksdb::Status s = partition->db()->SCard(key_, &card);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(card);
  } else {
    res_.SetRes(CmdRes::kErrOther, "scard error");
  }
  return;
}

void SMembersCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSMembers);
    return;
  }
  key_ = argv_[1];
  return;
}

void SMembersCmd::Do(std::shared_ptr<Partition> partition) {
  std::vector<std::string> members;
  rocksdb::Status s = partition->db()->SMembers(key_, &members);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendArrayLen(members.size());
    for (const auto& member : members) {
      res_.AppendStringLen(member.size());
      res_.AppendContent(member);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void SScanCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSScan);
    return;
  }
  key_ = argv_[1];
  if (!slash::string2l(argv_[2].data(), argv_[2].size(), &cursor_)) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSScan);
    return;
  }
  size_t argc = argv_.size(), index = 3;
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
      } else if (!slash::string2l(argv_[index].data(), argv_[index].size(), &count_)) {
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
  return;
}

void SScanCmd::Do(std::shared_ptr<Partition> partition) {
  int64_t next_cursor = 0;
  std::vector<std::string> members;
  rocksdb::Status s = partition->db()->SScan(key_, cursor_, pattern_, count_, &members, &next_cursor);

  if (s.ok() || s.IsNotFound()) {
    res_.AppendContent("*2");
    char buf[32];
    int64_t len = slash::ll2string(buf, sizeof(buf), next_cursor);
    res_.AppendStringLen(len);
    res_.AppendContent(buf);

    res_.AppendArrayLen(members.size());
    for (const auto& member : members) {
      res_.AppendString(member);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void SRemCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSMembers);
    return;
  }
  key_ = argv_[1];
  PikaCmdArgsType::iterator iter = argv_.begin();
  iter++;
  members_.assign(++iter, argv_.end());
  return;
}

void SRemCmd::Do(std::shared_ptr<Partition> partition) {
  int32_t count = 0;
  rocksdb::Status s = partition->db()->SRem(key_, members_, &count);
  res_.AppendInteger(count);
  return;
}

void SUnionCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSUnion);
    return;
  }
  PikaCmdArgsType::iterator iter = argv_.begin();
  keys_.assign(++iter, argv_.end());
  return;
}

void SUnionCmd::Do(std::shared_ptr<Partition> partition) {
  std::vector<std::string> members;
  partition->db()->SUnion(keys_, &members);
  res_.AppendArrayLen(members.size());
  for (const auto& member : members) {
    res_.AppendStringLen(member.size());
    res_.AppendContent(member);
  }
  return;
}

void SUnionstoreCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSUnionstore);
    return;
  }
  dest_key_ = argv_[1];
  PikaCmdArgsType::iterator iter = argv_.begin();
  iter++;
  keys_.assign(++iter, argv_.end());
  return;
}

void SUnionstoreCmd::Do(std::shared_ptr<Partition> partition) {
  int32_t count = 0;
  rocksdb::Status s = partition->db()->SUnionstore(dest_key_, keys_, &count);
  if (s.ok()) {
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void SInterCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSInter);
    return;
  }
  PikaCmdArgsType::iterator iter = argv_.begin();
  keys_.assign(++iter, argv_.end());
  return;
}

void SInterCmd::Do(std::shared_ptr<Partition> partition) {
  std::vector<std::string> members;
  partition->db()->SInter(keys_, &members);
  res_.AppendArrayLen(members.size());
  for (const auto& member : members) {
    res_.AppendStringLen(member.size());
    res_.AppendContent(member);
  }
  return;
}

void SInterstoreCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSInterstore);
    return;
  }
  dest_key_ = argv_[1];
  PikaCmdArgsType::iterator iter = argv_.begin();
  iter++;
  keys_.assign(++iter, argv_.end());
  return;
}

void SInterstoreCmd::Do(std::shared_ptr<Partition> partition) {
  int32_t count = 0;
  rocksdb::Status s = partition->db()->SInterstore(dest_key_, keys_, &count);
  if (s.ok()) {
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void SIsmemberCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSIsmember);
    return;
  }
  key_ = argv_[1];
  member_ = argv_[2];
  return;
}

void SIsmemberCmd::Do(std::shared_ptr<Partition> partition) {
  int32_t is_member = 0;
  partition->db()->SIsmember(key_, member_, &is_member);
  if (is_member) {
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
  PikaCmdArgsType::iterator iter = argv_.begin();
  keys_.assign(++iter, argv_.end());
  return;
}

void SDiffCmd::Do(std::shared_ptr<Partition> partition) {
  std::vector<std::string> members;
  partition->db()->SDiff(keys_, &members);
  res_.AppendArrayLen(members.size());
  for (const auto& member : members) {
    res_.AppendStringLen(member.size());
    res_.AppendContent(member);
  }
  return;
}

void SDiffstoreCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSDiffstore);
    return;
  }
  dest_key_ = argv_[1];
  PikaCmdArgsType::iterator iter = argv_.begin();
  iter++;
  keys_.assign(++iter, argv_.end());
  return;
}

void SDiffstoreCmd::Do(std::shared_ptr<Partition> partition) {
  int32_t count = 0;
  rocksdb::Status s = partition->db()->SDiffstore(dest_key_, keys_, &count);
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
  return;
}

void SMoveCmd::Do(std::shared_ptr<Partition> partition) {
  int32_t res = 0;
  rocksdb::Status s = partition->db()->SMove(src_key_, dest_key_, member_, &res);
  if (s.ok() || s.IsNotFound()) {
    if (s.IsNotFound()){
      res_.AppendInteger(res);
    } else {
      res_.AppendInteger(res);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
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
    if (!slash::string2l(argv_[2].data(), argv_[2].size(), &count_)) {
      res_.SetRes(CmdRes::kInvalidInt);
    } else {
      reply_arr = true;;
    }
  }
  return;
}

void SRandmemberCmd::Do(std::shared_ptr<Partition> partition) {
  std::vector<std::string> members;
  rocksdb::Status s = partition->db()->SRandmember(key_, count_, &members);
  if (s.ok() || s.IsNotFound()) {
    if (!reply_arr && members.size()) {
      res_.AppendStringLen(members[0].size());
      res_.AppendContent(members[0]);
    } else {
      res_.AppendArrayLen(members.size());
      for (const auto& member : members) {
        res_.AppendStringLen(member.size());
        res_.AppendContent(member);
      }
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}
