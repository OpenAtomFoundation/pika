#include "slash_string.h"
#include "nemo.h"
#include "pika_kv.h"
#include "pika_server.h"

extern PikaServer *g_pika_server;

void SetCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSet);
    return;
  }
  PikaCmdArgsType::iterator it = argv.begin();
  ++it;//Remember the first args is the opt name
  key_ = *it++;
  value_ = *it++;
  while (it != argv.end()) {
    std::string opt = slash::StringToLower(*it++);
    if (opt == "xx") {
      condition_ = SetCmd::kXX;
    } else if (opt == "nx") {
      condition_ = SetCmd::kNX;
    } else if (opt == "ex") {
      if (it == argv.end()) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      if (!slash::string2l((*it).data(), (*it).size(), &sec_)) {
        res_.SetRes(CmdRes::kOutofRange);
        return;
      }
      ++it;
    } else {
      res_.SetRes(CmdRes::kSyntaxErr);
      return;
    }
  }
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
      res_.SetRes(CmdRes::kOk);
    } else {
      res_.AppendArrayLen(-1);;
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void GetCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSet);
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
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}
