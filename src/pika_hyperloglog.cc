// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <vector>
#include "slash/include/slash_string.h"
#include "nemo.h"
#include "include/pika_server.h"
#include "include/pika_hyperloglog.h"

extern PikaServer *g_pika_server;

void PfAddCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePfAdd);
    return;
  }
  if (argv.size() > 1) {
    key_ = argv[1];
    size_t pos = 2;
    while (pos < argv.size()) {
      values_.push_back(argv[pos++]);
    }
  }
}

void PfAddCmd::Do() {
  bool update = false;
  rocksdb::Status s = g_pika_server->bdb()->PfAdd(key_, values_, &update);
  if (s.ok() && update) {
    res_.AppendInteger(1);
  } else if (s.ok() && !update) {
    res_.AppendInteger(0);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void PfCountCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePfCount);
    return;
  }
  size_t pos = 1;
  while (pos < argv.size()) {
    keys_.push_back(argv[pos++]);
  }
}

void PfCountCmd::Do() {
  int64_t value_ = 0;
  rocksdb::Status s = g_pika_server->bdb()->PfCount(keys_, &value_);
  if (s.ok()) {
    res_.AppendInteger(value_);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void PfMergeCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePfMerge);
    return;
  }
  size_t pos = 1;
  while (pos < argv.size()) {
    keys_.push_back(argv[pos++]);
  }
}

void PfMergeCmd::Do() {
  rocksdb::Status s = g_pika_server->bdb()->PfMerge(keys_);
  if (s.ok()) {
    res_.SetRes(CmdRes::kOk);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}
