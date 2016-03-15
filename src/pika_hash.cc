#include "slash_string.h"
#include "nemo.h"
#include "pika_hash.h"
#include "pika_server.h"

extern PikaServer *g_pika_server;

void HDelCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHDel);
    return;
  }
  key_ = argv[1];
  size_t index = 2, argc = argv.size();
  PikaCmdArgsType::iterator iter = argv.begin();
  iter++; 
  iter++;
  fields_.assign(iter, argv.end());
  return;
}

void HDelCmd::Do() {
  int64_t num = 0;
  nemo::Status s;
  size_t index = 0, fields_num = fields_.size();
  for (size_t index = 0; index != fields_num; index++) {
    s = g_pika_server->db()->HDel(key_, fields_[index]);
    if (s.ok()) {
      num++;
    }
  }
  res_.AppendInteger(num);
  return;
}

void HSetCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHSet);
    return;
  }
  key_ = argv[1];
  field_ = argv[2];
  value_ = argv[3];
  return;
}

void HSetCmd::Do() {

  return;
}

void HGetCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHGet);
    return;
  }
  key_ = argv[1];
  field_ = argv[2];
  return;
}

void HGetCmd::Do() {
  std::string value;
  nemo::Status s = g_pika_server->db()->HGet(key_, field_, &value);
  if (s.ok()) {
    res_.AppendStringLen(value.size());
    res_.AppendContent(value);
  } else if (s.IsNotFound()) {
    res_.AppendContent("$-1");
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}














