// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_set.h"
#include "include/pika_cache.h"
#include "include/pika_conf.h"
#include "pstd/include/pstd_string.h"
#include "include/pika_slot_command.h"

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

void SAddCmd::Do() {
  int32_t count = 0;
  s_ = db_->storage()->SAdd(key_, members_, &count);
  if (s_.ToString() == ErrTypeMessage) {
    res_.SetRes(CmdRes::kMultiKey);
    return;
  }
  if (!s_.ok()) {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
    return;
  }
  AddSlotKey("s", key_, db_);
  res_.AppendInteger(count);
}

void SAddCmd::DoThroughDB() {
  Do();
}

void SAddCmd::DoUpdateCache() {
  if (s_.ok()) {
    std::string CachePrefixKeyS = PCacheKeyPrefixS + key_;
    db_->cache()->SAddIfKeyExist(CachePrefixKeyS, members_);
  }
}

void SPopCmd::DoInitial() {
  size_t argc = argv_.size();
  if (!CheckArg(argc)) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSPop);
    return;
  }
  count_ = 1;
  key_ = argv_[1];
  if (argc > 3) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSPop);
  } else if (argc == 3) {
    if (pstd::string2int(argv_[2].data(), argv_[2].size(), &count_) == 0) {
      res_.SetRes(CmdRes::kErrOther, kCmdNameSPop);
      return;
    }
    if (count_ <= 0) {
      res_.SetRes(CmdRes::kErrOther, kCmdNameSPop);
      return;
    }
  }
}

void SPopCmd::Do() {
   s_ = db_->storage()->SPop(key_, &members_, count_);
  if (s_.ok()) {
    res_.AppendArrayLenUint64(members_.size());
    for (const auto& member : members_) {
      res_.AppendStringLenUint64(member.size());
      res_.AppendContent(member);
    }
  } else if (s_.IsNotFound()) {
    res_.AppendContent("$-1");
  } else if (s_.ToString() == ErrTypeMessage) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void SPopCmd::DoThroughDB() {
  Do();
}

void SPopCmd::DoUpdateCache() {
  if (s_.ok()) {
    std::string CachePrefixKeyS = PCacheKeyPrefixS + key_;
    db_->cache()->SRem(CachePrefixKeyS, members_);
  }
}

void SPopCmd::DoBinlog() {
  if (!s_.ok()) {
    return;
  }

  PikaCmdArgsType srem_args;
  srem_args.emplace_back("srem");
  srem_args.emplace_back(key_);
  for (auto m = members_.begin(); m != members_.end(); ++m) {
    srem_args.emplace_back(*m);
  }
  
  srem_cmd_->Initial(srem_args, db_name_);
  srem_cmd_->SetConn(GetConn());
  srem_cmd_->SetResp(resp_.lock());
  srem_cmd_->DoBinlog();
}

void SCardCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSCard);
    return;
  }
  key_ = argv_[1];
}

void SCardCmd::Do() {
  int32_t card = 0;
  s_ = db_->storage()->SCard(key_, &card);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendInteger(card);
  } else if (s_.ToString() == ErrTypeMessage) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, "scard error");
  }
}

void SCardCmd::ReadCache() {
  uint64_t card = 0;
  std::string CachePrefixKeyS = PCacheKeyPrefixS + key_;
  auto s = db_->cache()->SCard(CachePrefixKeyS, &card);
  if (s.ok()) {
    res_.AppendInteger(card);
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, "scard error");
  }
}

void SCardCmd::DoThroughDB() {
  res_.clear();
  Do();
}

void SCardCmd::DoUpdateCache() {
  if (s_.ok()) {
    db_->cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_SET, key_, db_);
  }
}

void SMembersCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSMembers);
    return;
  }
  key_ = argv_[1];
}

void SMembersCmd::Do() {
  std::vector<std::string> members;
  s_ = db_->storage()->SMembers(key_, &members);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendArrayLenUint64(members.size());
    for (const auto& member : members) {
      res_.AppendStringLenUint64(member.size());
      res_.AppendContent(member);
    }
  } else if (s_.ToString() == ErrTypeMessage) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void SMembersCmd::ReadCache() {
  std::vector<std::string> members;
  std::string CachePrefixKeyS = PCacheKeyPrefixS + key_;
  auto s = db_->cache()->SMembers(CachePrefixKeyS, &members);
  if (s.ok()) {
    res_.AppendArrayLen(members.size());
    for (const auto& member : members) {
      res_.AppendStringLen(member.size());
      res_.AppendContent(member);
    }
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void SMembersCmd::DoThroughDB() {
  res_.clear();
  Do();
}

void SMembersCmd::DoUpdateCache() {
  if (s_.ok()) {
    db_->cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_SET, key_, db_);
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

void SScanCmd::Do() {
  int64_t next_cursor = 0;
  std::vector<std::string> members;
  rocksdb::Status s = db_->storage()->SScan(key_, cursor_, pattern_, count_, &members, &next_cursor);

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
  } else if (s_.ToString() == ErrTypeMessage) {
    res_.SetRes(CmdRes::kMultiKey);
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

void SRemCmd::Do() {
  s_ = db_->storage()->SRem(key_, members_, &deleted_);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendInteger(deleted_);
  } else if (s_.ToString() == ErrTypeMessage) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void SRemCmd::DoThroughDB() {
  Do();
}

void SRemCmd::DoUpdateCache() {
  if (s_.ok() && deleted_ > 0) {
    std::string CachePrefixKeyS = PCacheKeyPrefixS + key_;
    db_->cache()->SRem(CachePrefixKeyS, members_);
  }
}

void SUnionCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSUnion);
    return;
  }
  auto iter = argv_.begin();
  keys_.assign(++iter, argv_.end());
}

void SUnionCmd::Do() {
  std::vector<std::string> members;
  s_ = db_->storage()->SUnion(keys_, &members);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendArrayLenUint64(members.size());
    for (const auto& member : members) {
      res_.AppendStringLenUint64(member.size());
      res_.AppendContent(member);
    }
  } else if (s_.ToString() == ErrTypeMessage) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
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

void SUnionstoreCmd::Do() {
  int32_t count = 0;
  s_ = db_->storage()->SUnionstore(dest_key_, keys_, value_to_dest_, &count);
  if (s_.ok()) {
    res_.AppendInteger(count);
  } else if (s_.ToString() == ErrTypeMessage) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void SUnionstoreCmd::DoThroughDB() {
  Do();
}

void SUnionstoreCmd::DoUpdateCache() {
  if (s_.ok()) {
    std::vector<std::string> v;
    v.emplace_back(PCacheKeyPrefixS + dest_key_);
    db_->cache()->Del(v);
  }
}

void SetOperationCmd::DoBinlog() {
  PikaCmdArgsType del_args;
  del_args.emplace_back("del");
  del_args.emplace_back(dest_key_);
  del_cmd_->Initial(del_args, db_name_);
  del_cmd_->SetConn(GetConn());
  del_cmd_->SetResp(resp_.lock());
  del_cmd_->DoBinlog();

  if (value_to_dest_.size() == 0) {
    //The union/diff/inter operation got an empty set, just exec del to simulate overwrite an empty set to dest_key
    return;
  }

  PikaCmdArgsType initial_args;
  initial_args.emplace_back("sadd");//use "sadd" to distinguish the binlog of SaddCmd which use "SADD" for binlog
  initial_args.emplace_back(dest_key_);
  initial_args.emplace_back(value_to_dest_[0]);
  sadd_cmd_->Initial(initial_args, db_name_);
  sadd_cmd_->SetConn(GetConn());
  sadd_cmd_->SetResp(resp_.lock());

  auto& sadd_argv = sadd_cmd_->argv();
  size_t data_size = value_to_dest_[0].size();

  for (size_t i = 1; i < value_to_dest_.size(); i++) {
    if (data_size >= 131072) {
      // If the binlog has reached the size of 128KB. (131,072 bytes = 128KB)
      sadd_cmd_->DoBinlog();
      sadd_argv.clear();
      sadd_argv.emplace_back("sadd");
      sadd_argv.emplace_back(dest_key_);
      data_size = 0;
    }
    sadd_argv.emplace_back(value_to_dest_[i]);
    data_size += value_to_dest_[i].size();
  }
  sadd_cmd_->DoBinlog();
}

void SInterCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSInter);
    return;
  }
  auto iter = argv_.begin();
  keys_.assign(++iter, argv_.end());
}

void SInterCmd::Do() {
  std::vector<std::string> members;
  s_ = db_->storage()->SInter(keys_, &members);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendArrayLenUint64(members.size());
    for (const auto& member : members) {
      res_.AppendStringLenUint64(member.size());
      res_.AppendContent(member);
    }
  } else if (s_.ToString() == ErrTypeMessage) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
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

void SInterstoreCmd::Do() {
  int32_t count = 0;
  s_ = db_->storage()->SInterstore(dest_key_, keys_, value_to_dest_, &count);
  if (s_.ok()) {
    res_.AppendInteger(count);
  } else if (s_.ToString() == ErrTypeMessage) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void SInterstoreCmd::DoThroughDB() {
  Do();
}

void SInterstoreCmd::DoUpdateCache() {
  if (s_.ok()) {
    std::vector<std::string> v;
    v.emplace_back(PCacheKeyPrefixS + dest_key_);
    db_->cache()->Del(v);
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

void SIsmemberCmd::Do() {
  int32_t is_member = 0;
  s_ = db_->storage()->SIsmember(key_, member_, &is_member);
  if (is_member != 0) {
    res_.AppendContent(":1");
  } else if (s_.ToString() == ErrTypeMessage) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.AppendContent(":0");
  }
}

void SIsmemberCmd::ReadCache() {
  std::string CachePrefixKeyS = PCacheKeyPrefixS + key_;
  auto s = db_->cache()->SIsmember(CachePrefixKeyS, member_);
  if (s.ok()) {
    res_.AppendContent(":1");
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}


void SIsmemberCmd::DoThroughDB() {
  res_.clear();
  Do();
}

void SIsmemberCmd::DoUpdateCache() {
  if (s_.ok()) {
    db_->cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_SET, key_, db_);
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

void SDiffCmd::Do() {
  std::vector<std::string> members;
  s_ = db_->storage()->SDiff(keys_, &members);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendArrayLenUint64(members.size());
    for (const auto& member : members) {
      res_.AppendStringLenUint64(member.size());
      res_.AppendContent(member);
    }
  } else if (s_.ToString() == ErrTypeMessage) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther,s_.ToString());
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

void SDiffstoreCmd::Do() {
  int32_t count = 0;
  s_ = db_->storage()->SDiffstore(dest_key_, keys_, value_to_dest_, &count);
  if (s_.ok()) {
    res_.AppendInteger(count);
  } else if (s_.ToString() == ErrTypeMessage) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void SDiffstoreCmd::DoThroughDB() {
  Do();
}

void SDiffstoreCmd::DoUpdateCache() {
  if (s_.ok()) {
    std::vector<std::string> v;
    v.emplace_back(PCacheKeyPrefixS + dest_key_);
    db_->cache()->Del(v);
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

void SMoveCmd::Do() {
  int32_t res = 0;
  s_ = db_->storage()->SMove(src_key_, dest_key_, member_, &res);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendInteger(res);
    move_success_ = res;
  } else if (s_.ToString() == ErrTypeMessage) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void SMoveCmd::DoThroughDB() {
  Do();
}

void SMoveCmd::DoUpdateCache() {
  if (s_.ok()) {
    std::vector<std::string> members;
    members.emplace_back(member_);
    std::string CachePrefixKeyS = PCacheKeyPrefixS + src_key_;
    std::string CachePrefixKeyD = PCacheKeyPrefixS + dest_key_;
    db_->cache()->SRem(CachePrefixKeyS, members);
    db_->cache()->SAddIfKeyExist(CachePrefixKeyD, members);
  }
}

void SMoveCmd::DoBinlog() {
  if (!move_success_) {
    //the member is not in the source set, nothing changed
    return;
  }
  PikaCmdArgsType srem_args;
  //SremCmd use "SREM", SMove use "srem"
  srem_args.emplace_back("srem");
  srem_args.emplace_back(src_key_);
  srem_args.emplace_back(member_);
  srem_cmd_->Initial(srem_args, db_name_);

  PikaCmdArgsType sadd_args;
  //Saddcmd use "SADD", Smovecmd use "sadd"
  sadd_args.emplace_back("sadd");
  sadd_args.emplace_back(dest_key_);
  sadd_args.emplace_back(member_);
  sadd_cmd_->Initial(sadd_args, db_name_);

  srem_cmd_->SetConn(GetConn());
  srem_cmd_->SetResp(resp_.lock());
  sadd_cmd_->SetConn(GetConn());
  sadd_cmd_->SetResp(resp_.lock());

  srem_cmd_->DoBinlog();
  sadd_cmd_->DoBinlog();
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
    }
  }
}

void SRandmemberCmd::Do() {
  std::vector<std::string> members;
  s_ = db_->storage()->SRandmember(key_, static_cast<int32_t>(count_), &members);
  if (s_.ok() || s_.IsNotFound()) {
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
  } else if (s_.ToString() == ErrTypeMessage) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void SRandmemberCmd::ReadCache() {
  std::vector<std::string> members;
  std::string CachePrefixKeyS = PCacheKeyPrefixS + key_;
  auto s = db_->cache()->SRandmember(CachePrefixKeyS, count_, &members);
  if (s.ok()) {
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
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void SRandmemberCmd::DoThroughDB() {
  res_.clear();
  Do();
}

void SRandmemberCmd::DoUpdateCache() {
  if (s_.ok()) {
    db_->cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_SET, key_, db_);
  }
}

