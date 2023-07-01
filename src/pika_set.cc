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
    res_.AppendArrayLen(members.size());
    for (const auto& member : members) {
      res_.AppendStringLen(member.size());
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
    res_.AppendArrayLen(members.size());
    for (const auto& member : members) {
      res_.AppendStringLen(member.size());
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

    res_.AppendArrayLen(members.size());
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
  res_.AppendArrayLen(members.size());
  for (const auto& member : members) {
    res_.AppendStringLen(member.size());
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
  rocksdb::Status s = slot->db()->SUnionstore(dest_key_, keys_, value_to_dest_, &count);
  if (s.ok()) {
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void SetOperationCmd::DoBinlog(const std::shared_ptr<SyncMasterSlot>& slot) {
  PikaCmdArgsType del_args;
  del_args.emplace_back("del");
  del_args.emplace_back(dest_key_);
  del_cmd_->Initial(std::move(del_args), db_name_);
  del_cmd_->SetConn(GetConn());
  del_cmd_->SetResp(resp_.lock());
  del_cmd_->DoBinlog(slot);

  if(value_to_dest_.size() == 0){
    //The union/diff/inter operation got an empty set, just exec del to simulate overwrite an empty set to dest_key
    return;
  }

  PikaCmdArgsType initial_args;
  initial_args.emplace_back("sadd");//use "sadd" to distinguish the binlog of SaddCmd which use "SADD" for binlog
  initial_args.emplace_back(dest_key_);
  initial_args.emplace_back(value_to_dest_[0]);
  sadd_cmd_->Initial(std::move(initial_args), db_name_);
  sadd_cmd_->SetConn(GetConn());
  sadd_cmd_->SetResp(resp_.lock());

  auto& sadd_argv = sadd_cmd_->argv();
  size_t data_size = value_to_dest_[0].size();

  for(int i = 1; i < value_to_dest_.size(); i++){
    if(data_size >= 131072){
      // If the binlog has reached the size of 128KB. (131,072 bytes = 128KB)
      sadd_cmd_->DoBinlog(slot);
      sadd_argv.clear();
      sadd_argv.emplace_back("sadd");
      sadd_argv.emplace_back(dest_key_);
      data_size = 0;
    }
    sadd_argv.emplace_back(value_to_dest_[i]);
    data_size += value_to_dest_[i].size();
  }
  sadd_cmd_->DoBinlog(slot);
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
  res_.AppendArrayLen(members.size());
  for (const auto& member : members) {
    res_.AppendStringLen(member.size());
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
  rocksdb::Status s = slot->db()->SInterstore(dest_key_, keys_, value_to_dest_, &count);
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
  res_.AppendArrayLen(members.size());
  for (const auto& member : members) {
    res_.AppendStringLen(member.size());
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
  rocksdb::Status s = slot->db()->SDiffstore(dest_key_, keys_, value_to_dest_, &count);
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
    move_success_ = res;
    AddSlotKey("s", src_key_, slot);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}
void SMoveCmd::DoBinlog(const std::shared_ptr<SyncMasterSlot>& slot) {
  if(!move_success_){
    //the member is not in the source set, nothing changed
    return;
  }
  PikaCmdArgsType srem_args;
  //SremCmd use "SREM", SMove use "srem"
  srem_args.push_back("srem");
  srem_args.push_back(src_key_);
  srem_args.push_back(member_);
  srem_cmd_->Initial(std::move(srem_args), db_name_);

  PikaCmdArgsType sadd_args;
  //Saddcmd use "SADD", Smovecmd use "sadd"
  sadd_args.push_back("sadd");
  sadd_args.push_back(dest_key_);
  sadd_args.push_back(member_);
  sadd_cmd_->Initial(std::move(sadd_args), db_name_);

  srem_cmd_->SetConn(GetConn());
  srem_cmd_->SetResp(resp_.lock());
  sadd_cmd_->SetConn(GetConn());
  sadd_cmd_->SetResp(resp_.lock());

  srem_cmd_->DoBinlog(slot);
  sadd_cmd_->DoBinlog(slot);
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
  rocksdb::Status s = slot->db()->SRandmember(key_, count_, &members);
  if (s.ok() || s.IsNotFound()) {
    if (!reply_arr && (static_cast<unsigned int>(!members.empty()) != 0U)) {
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
}

