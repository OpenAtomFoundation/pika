#include "slash_string.h"
#include "nemo.h"
#include "pika_kv.h"
#include "pika_server.h"

extern PikaServer *g_pika_server;

void SetCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetStatus(CmdRes::kWrongNum, kCmdNameSet);
    return;
  }
  key_ = argv[1];
  value_ = argv[2];
  condition_ = SetCmd::kANY;
  sec_ = 0;
  PikaCmdArgsType::size_t index = 3;
  while (index != argv.size()) {
    std::string opt = slash::StringToLower(argv[index]);
    if (opt == "xx") {
      condition_ = SetCmd::kXX;
    } else if (opt == "nx") {
      condition_ = SetCmd::kNX;
    } else if (opt == "ex") {
      index++;
      if (index == argv.size()) {
        res_.SetStatus(CmdRes::kSyntaxErr);
        return;
      }
      if (!slash::string2l(argv[index].data(), argv[index].size(), &sec_)) {
        res_.SetStatus(CmdRes::kOutofRange);
        return;
      }
    } else {
      res_.SetStatus(CmdRes::kSyntaxErr);
      return;
    }
    index++;
  }
  return;
}

void SetCmd::Do() {
  nemo::Status s;
  int64_t res = 1;
  switch (condition_) {
    case SetCmd::kXX:
      s = g_pika_server->db()->Setxx(key_, value_, &res, sec_);
      break;
    case SetCmd::kNX:
      s = g_pika_server->db()->Setnx(key_, value_, &res, sec_);
      break;
    default:
      s = g_pika_server->db()->Set(key_, value_, sec_);
      break;
  }

  if (s.ok() || s.IsNotFound()) {
    if (res == 1) {
      res_.SetStatus(CmdRes::kOk);
    } else {
      res_.AppendArrayLen(-1);;
    }
  } else {
    res_.SetStatus(CmdRes::kErrOther, s.ToString());
  }
}

void GetCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetStatus(CmdRes::kWrongNum, kCmdNameSet);
    return;
  }
  key_ = argv[1];
  return;
}

void GetCmd::Do() {
  std::string value;
  nemo::Status s = g_pika_server->db()->Get(key_, &value);
  if (s.ok()) {
    res_.AppendStringLen(value.size());
    res_.AppendContent(value);
  } else if (s.IsNotFound()) {
    res_.AppendStringLen(-1);
  } else {
    res_.SetStatus(CmdRes::kErrOther, s.ToString());
  }
}

void DelCmd::Initial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetErr("wrong number of arguments for " + 
        kCmdNameDel + " command");
    return;
  }
  std::vector<std::string>::const_iterator iter = argv.begin();
  keys_.erase(++iter, argv.end());
  return;
}

void DelCmd::Do() {
  int64_t count = 0;
  nemo::Status s = g_pika_server->db()->MDel(keys_, &count);
  if (s.ok()) {
   char buf[32];
   snprintf(buf, sizeof(buf), ":%lld", count);
   res_.AppendContent(buf);
  } else {
    res_.SetErr(s.ToString());
  }
  return;
}

void IncrCmd::Initial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  res_.clear();
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetErr("wrong number of arguments for " + 
        kCmdNameIncr + " command");
    return;
  }
  key_ = argv[1];
  return;
}

void IncrCmd::Do() {
  std::string new_value;
  nemo::Status s = g_pika_server->db()->Incrby(key_, 1, new_value);
  if (s.ok()) {
   res_.AppendContent(":" + new_value);
  } else if (s.IsCorruption() && s.ToString() == "Corruption: value is not a integer") {
    res_.AppendContent("-ERR value is not an integer or out of range");
  } else if (s.IsInvalidArgument()) {
    res_.AppendContent("-ERR increment or decrement would overflow");
  } else {
    res_.SetErr(s.ToString());
  }
  return;
}

void IncrbyCmd::Initial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  res_.clear();
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetErr("wrong number of arguments for " + 
        kCmdNameIncrby + " command");
    return;
  }
  key_ = argv[1];
  if (!slash::string2l(argv[2].data(), argv[2].size(), &by_)) {
    res_.SetErr("-ERR value is not an integer or out of range");
    return;
  }
  return;
}

void IncrbyCmd::Do() {
  std::string new_value;
  nemo::Status s = g_pika_server->db()->Incrby(key_, by_, new_value);
  if (s.ok()) {
    res_.AppendContent(":" + new_value);
  } else if (s.IsCorruption() && s.ToString() == "Corruption: value is not a integer") {
    res_.AppendContent("-ERR value is not an integer or out of range");
  } else if (s.IsInvalidArgument()) {
    res_.AppendContent("-ERR increment or decrement would overflow");
  } else {
    res_.SetErr(s.ToString());
  }
  return;
}

void IncrbyfloatCmd::Initial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  res_.clear();
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetErr("wrong number of arguments for " + 
        kCmdNameIncrbyfloat + " command");
    return;
  }
  key_ = argv[1];
  if (!slash::string2d(argv[2].data(), argv[2].size(), &by_)) {
    res_.SetErr("-ERR value is not an float");
    return;
  }
  return;
}

void IncrbyfloatCmd::Do() {
  std::string new_value;
  nemo::Status s = g_pika_server->db()->Incrbyfloat(key_, by_, new_value);
  if (s.ok()) {
    res_.AppendStringLen(new_value.size());
    res_.AppendContent(new_value);
  } else if (s.IsCorruption() && s.ToString() == "Corruption: value is not a float"){
    res_.SetErr("-ERR value is not an float");
  } else if (s.IsInvalidArgument()) {
    res_.SetErr("-ERR increment or decrement would overflow");
  } else {
    res_.SetErr(s.ToString());
  }
  return;
}

void DecrCmd::Initial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  res_.clear();
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetErr("wrong number of arguments for " + 
        kCmdNameDecr + " command");
    return;
  }
  key_ = argv[1];
  return;
}

void DecrCmd::Do() {
  std::string new_value;
  nemo::Status s = g_pika_server->db()->Decrby(key_, 1, new_value);
  if (s.ok()) {
   res_.AppendContent(":" + new_value);
  } else if (s.IsCorruption() && s.ToString() == "Corruption: value is not a integer") {
    res_.AppendContent("-ERR value is not an integer or out of range");
  } else if (s.IsInvalidArgument()) {
    res_.AppendContent("-ERR increment or decrement would overflow");
  } else {
    res_.SetErr(s.ToString());
  }
  return;
}

void DecrbyCmd::Initial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  res_.clear();
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetErr("wrong number of arguments for " + 
        kCmdNameDecrby + " command");
    return;
  }
  key_ = argv[1];
  if (!slash::string2l(argv[2].data(), argv[2].size(), &by_)) {
    res_.SetErr("-ERR value is not an integer or out of range");
    return;
  }
  return;
}

void DecrbyCmd::Do() {
  std::string new_value;
  nemo::Status s = g_pika_server->db()->Decrby(key_, by_, new_value);
  if (s.ok()) {
    res_.AppendContent(":" + new_value);
  } else if (s.IsCorruption() && s.ToString() == "Corruption: value is not a integer") {
    res_.AppendContent("-ERR value is not an integer or out of range");
  } else if (s.IsInvalidArgument()) {
    res_.AppendContent("-ERR increment or decrement would overflow");
  } else {
    res_.SetErr(s.ToString());
  }
  return;
}

void GetsetCmd::Initial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  res_.clear();
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetErr("wrong number of arguments for " + 
        kCmdNameGetset + " command");
    return;
  }
  key_ = argv[1];
  new_value_ = argv[2];
  return;
}

void GetsetCmd::Do() {
  std::string old_value;
  nemo::Status s = g_pika_server->db()->GetSet(key_, new_value_, &old_value);
  if (s.ok()) {
    if (old_value.empty()) {
      res_.AppendContent("$-1");
    } else {
      res_.AppendStringLen(old_value.size());
      res_.AppendContent(old_value);
    }
  } else {
    res_.SetErr(s.ToString());
  }
  return;
}

void AppendCmd::Initial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  res_.clear();
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetErr("wrong number of arguments for " + 
        kCmdNameAppend + " command");
    return;
  }
  key_ = argv[1];
  value_ = argv[2];
  return;
}

void AppendCmd::Do() {
  int64_t new_len = 0;
  nemo::Status s = g_pika_server->db()->Append(key_, value_, &new_len);
  if (s.ok() || s.IsNotFound()) {
    char buf[32];
    snprintf(buf, sizeof(buf), ":%lld", new_len);
    res_.AppendContent(buf);
  } else {
    res_.SetErr(s.ToString());
  }
  return;
}

void MgetCmd::Initial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  res_.clear();
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetErr("wrong number of arguments for " + 
        kCmdNameMget + " command");
    return;
  }
  keys_ = argv;
  keys_.erase(keys_.begin());
  return;
}

void MgetCmd::Do() {
  std::vector<nemo::KVS> kvs_v;
  nemo::Status s = g_pika_server->db()->MGet(keys_, kvs_v);
  res_.AppendArrayLen(kvs_v.size());
  std::vector<nemo::KVS>::const_iterator iter;
  for (iter = kvs_v.begin(); iter != kvs_v.end(); iter++) {
    if ((iter->status).ok()) {
      res_.AppendStringLen((iter->val).size());
      res_.AppendContent(iter->val);
    } else {
      res_.AppendContent("$-1");
    }
  }
  return;
}









