#include "slash_string.h"
#include "nemo.h"
#include "pika_set.h"
#include "pika_server.h"

extern PikaServer *g_pika_server;

void SAddCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSAdd);
    return;
  }
  key_ = argv[1];
  PikaCmdArgsType::iterator iter = argv.begin();
  iter++; 
  iter++;
  members_.assign(iter, argv.end());
  return;
}

void SAddCmd::Do() {
  int64_t res = 0, count = 0;
  nemo::Status s;
  std::vector<std::string>::const_iterator iter = members_.begin();
  for (; iter != members_.end(); iter++) {
    s = g_pika_server->db()->SAdd(key_, *iter, &res);
    if (s.ok()) {
      count += res;
    } else {
      res_.SetRes(CmdRes::kErrOther, s.ToString());
      return;
    }
  }
  res_.AppendInteger(count);
  return;
}

void SPopCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSPop);
    return;
  }
  key_ = argv[1];
  return;
}

void SPopCmd::Do() {
  std::string member;
  nemo::Status s = g_pika_server->db()->SPop(key_, member);
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

void SCardCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSCard);
    return;
  }
  key_ = argv[1];
  return;
}

void SCardCmd::Do() {
  int64_t card = 0;
  card = g_pika_server->db()->SCard(key_);
  if (card >= 0) {
    res_.AppendInteger(card);
  } else {
    res_.SetRes(CmdRes::kErrOther, "scard error");
  }
  return;
}

void SMembersCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSMembers);
    return;
  }
  key_ = argv[1];
  return;
}

void SMembersCmd::Do() {
  std::vector<std::string> members;
  nemo::Status s = g_pika_server->db()->SMembers(key_, members);
  if (s.ok()) {
    res_.AppendArrayLen(members.size());
    std::vector<std::string>::const_iterator iter = members.begin();
    for (; iter != members.end(); iter++) {
      res_.AppendStringLen(iter->size());
      res_.AppendContent(*iter);
    } 
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void SScanCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSScan);
    return;
  }
  key_ = argv[1];
  if (!slash::string2l(argv[2].data(), argv[2].size(), &cursor_)) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSScan);
    return;
  }
  size_t argc = argv.size(), index = 3;
  while (index < argc) {
    std::string opt = slash::StringToLower(argv[index]); 
    if (opt == "match" || opt == "count") {
      index++;
      if (index >= argc) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      if (opt == "match") {
        pattern_ = argv[index];
      } else if (!slash::string2l(argv[index].data(), argv[index].size(), &count_)) {
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

void SScanCmd::Do() {
  int64_t card = g_pika_server->db()->SCard(key_);
  if (card >= 0 && cursor_ >= card) {
    cursor_ = 0;
  }
  std::vector<std::string> members;
  nemo::SIterator *iter = g_pika_server->db()->SScan(key_, -1);
  iter->Skip(cursor_);
  if (!iter->Valid()) {
    delete iter;
    iter = g_pika_server->db()->SScan(key_, -1);
    cursor_ = 0;
  }
  for (; iter->Valid() && count_; iter->Next()) {
    if (pattern_ != "*" && !slash::stringmatchlen(pattern_.data(), pattern_.size(), iter->member().data(), iter->member().size(), 0)) {
      continue;
    }
    members.push_back(iter->member());
    count_--;
    cursor_++;
  }
  if (members.size() <= 0 || !iter->Valid()) {
    cursor_ = 0;
  }
  res_.AppendContent("*2");
  
  char buf[32];
  int64_t len = slash::ll2string(buf, sizeof(buf), cursor_);
  res_.AppendStringLen(len);
  res_.AppendContent(buf);

  res_.AppendArrayLen(members.size());
  std::vector<std::string>::const_iterator iter_member = members.begin();
  for (; iter_member != members.end(); iter_member++) {
    res_.AppendStringLen(iter_member->size());
    res_.AppendContent(*iter_member);
  }
  return;
}

void SRemCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSMembers);
    return;
  }
  key_ = argv[1];
  PikaCmdArgsType::iterator iter = argv.begin();
  iter++;
  members_.assign(++iter, argv.end());
  return;
}

void SRemCmd::Do() {
  int64_t count = 0, tmp;
  nemo::Status s;
  std::vector<std::string>::const_iterator iter = members_.begin();
  for (; iter != members_.end(); iter++) {
    s = g_pika_server->db()->SRem(key_, *iter, &tmp);
    if (s.ok()) {
      count++;
    }
  }
  res_.AppendInteger(count);
  return;
}

void SUnionCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSUnion);
    return;
  }
  PikaCmdArgsType::iterator iter = argv.begin();
  keys_.assign(++iter, argv.end());
  return;
}

void SUnionCmd::Do() {
  std::vector<std::string> members;
  g_pika_server->db()->SUnion(keys_, members);
  res_.AppendArrayLen(members.size());
  std::vector<std::string>::const_iterator iter = members.begin();
  for (; iter != members.end(); iter++) {
    res_.AppendStringLen(iter->size());
    res_.AppendContent(*iter);
  }
  return;
}

void SUnionstoreCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSUnionstore);
    return;
  }
  dest_key_ = argv[1];
  PikaCmdArgsType::iterator iter = argv.begin();
  iter++;
  keys_.assign(++iter, argv.end());
  return;
}

void SUnionstoreCmd::Do() {
  int64_t count = 0;
  nemo::Status s = g_pika_server->db()->SUnionStore(dest_key_, keys_, &count);
  if (s.ok()) {
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void SInterCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSInter);
    return;
  }
  PikaCmdArgsType::iterator iter = argv.begin();
  keys_.assign(++iter, argv.end());
  return;
}

void SInterCmd::Do() {
  std::vector<std::string> members;
  g_pika_server->db()->SInter(keys_, members);
  res_.AppendArrayLen(members.size());
  std::vector<std::string>::const_iterator iter = members.begin();
  for (; iter != members.end(); iter++) {
    res_.AppendStringLen(iter->size());
    res_.AppendContent(*iter);
  }
  return;
}

void SInterstoreCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSInterstore);
    return;
  }
  dest_key_ = argv[1];
  PikaCmdArgsType::iterator iter = argv.begin();
  iter++;
  keys_.assign(++iter, argv.end());
  return;
}

void SInterstoreCmd::Do() {
  int64_t count = 0;
  nemo::Status s = g_pika_server->db()->SInterStore(dest_key_, keys_, &count);
  if (s.ok()) {
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void SIsmemberCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSIsmember);
    return;
  }
  key_ = argv[1];
  member_ = argv[2];
  return;
}

void SIsmemberCmd::Do() {
  bool is_member = g_pika_server->db()->SIsMember(key_, member_);
  if (is_member) {
    res_.AppendContent(":1");
  } else {
    res_.AppendContent(":0");
  }
}

void SDiffCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSDiff);
    return;
  }
  PikaCmdArgsType::iterator iter = argv.begin();
  keys_.assign(++iter, argv.end());
  return;
}

void SDiffCmd::Do() {
  std::vector<std::string> members;
  g_pika_server->db()->SDiff(keys_, members);
  res_.AppendArrayLen(members.size());
  std::vector<std::string>::const_iterator iter = members.begin();
  for (; iter != members.end(); iter++) {
    res_.AppendStringLen(iter->size());
    res_.AppendContent(*iter);
  }
  return;
}

void SDiffstoreCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSDiffstore);
    return;
  }
  dest_key_ = argv[1];
  PikaCmdArgsType::iterator iter = argv.begin();
  iter++;
  keys_.assign(++iter, argv.end());
  return;
}

void SDiffstoreCmd::Do() {
  int64_t count = 0;
  nemo::Status s = g_pika_server->db()->SDiffStore(dest_key_, keys_, &count);
  if (s.ok()) {
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void SMoveCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSMove);
    return;
  }
  src_key_ = argv[1];
  dest_key_ = argv[2];
  member_ = argv[3];
  return;
}

void SMoveCmd::Do() {
  int64_t res = 0;
  nemo::Status s = g_pika_server->db()->SMove(src_key_, dest_key_, member_, &res);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(res);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void SRandmemberCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSRandmember);
    return;
  }
  key_ = argv[1];
  if (argv.size() > 3) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSRandmember);
    return;
  } else if (argv.size() < 3) {
    return;
  } else if (!slash::string2l(argv[2].data(), argv[2].size(), &count_)) {
    res_.SetRes(CmdRes::kInvalidInt);
  }
  return;
}

void SRandmemberCmd::Do() {
  std::vector<std::string> members;
  nemo::Status s = g_pika_server->db()->SRandMember(key_, members, count_);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendArrayLen(members.size());
    std::vector<std::string>::const_iterator iter = members.begin();
    for (; iter != members.end(); iter++) {
      res_.AppendStringLen(iter->size());
      res_.AppendContent(*iter);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}
