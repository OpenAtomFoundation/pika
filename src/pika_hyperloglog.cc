// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_hyperloglog.h"

void PfAddCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePfAdd);
    return;
  }
  if (argv_.size() > 1) {
    key_ = argv_[1];
    size_t pos = 2;
    while (pos < argv_.size()) {
      values_.push_back(argv_[pos++]);
    }
  }
}

void PfAddCmd::Do() {
  bool update = false;
  rocksdb::Status s = db_->storage()->PfAdd(key_, values_, &update);
  if (s.ok() && update) {
    res_.AppendInteger(1);
  } else if (s.ok() && !update) {
    res_.AppendInteger(0);
  } else if (s_.ToString() == ErrTypeMessage) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void PfCountCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePfCount);
    return;
  }
  size_t pos = 1;
  while (pos < argv_.size()) {
    keys_.push_back(argv_[pos++]);
  }
}

void PfCountCmd::Do() {
  int64_t value_ = 0;
  rocksdb::Status s = db_->storage()->PfCount(keys_, &value_);
  if (s.ok()) {
    res_.AppendInteger(value_);
  } else if (s_.ToString() == ErrTypeMessage) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void PfMergeCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePfMerge);
    return;
  }
  size_t pos = 1;
  while (pos < argv_.size()) {
    keys_.push_back(argv_[pos++]);
  }
}

void PfMergeCmd::Do() {
  rocksdb::Status s = db_->storage()->PfMerge(keys_, value_to_dest_);
  if (s.ok()) {
    res_.SetRes(CmdRes::kOk);
  } else if (s_.ToString() == ErrTypeMessage) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}
void PfMergeCmd::DoBinlog() {
  PikaCmdArgsType set_args;
  //used "set" instead of "SET" to distinguish the binlog of SetCmd
  set_args.emplace_back("set");
  set_args.emplace_back(keys_[0]);
  set_args.emplace_back(value_to_dest_);
  set_cmd_->Initial(set_args,  db_name_);
  set_cmd_->SetConn(GetConn());
  set_cmd_->SetResp(resp_.lock());
  //value of this binlog might be strange, it's an string with size of 128KB
  set_cmd_->DoBinlog();
}
