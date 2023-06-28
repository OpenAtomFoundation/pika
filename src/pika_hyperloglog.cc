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

void PfAddCmd::Do(std::shared_ptr<Slot> slot) {
  bool update = false;
  rocksdb::Status s = slot->db()->PfAdd(key_, values_, &update);
  if (s.ok() && update) {
    res_.AppendInteger(1);
  } else if (s.ok() && !update) {
    res_.AppendInteger(0);
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

void PfCountCmd::Do(std::shared_ptr<Slot> slot) {
  int64_t value_ = 0;
  rocksdb::Status s = slot->db()->PfCount(keys_, &value_);
  if (s.ok()) {
    res_.AppendInteger(value_);
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

void PfMergeCmd::Do(std::shared_ptr<Slot> slot) {
  rocksdb::Status s = slot->db()->PfMerge(keys_, value_to_dest_);
  if (s.ok()) {
    res_.SetRes(CmdRes::kOk);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}
void PfMergeCmd::DoBinlog(const std::shared_ptr<SyncMasterSlot>& slot) {
  PikaCmdArgsType set_args;
  //mset and msetnx use "set", setcmd use "SET", bitop use "seT", pfmerge use "sEt"
  set_args.push_back("sEt");
  set_args.push_back(keys_[0]);
  set_args.push_back(value_to_dest_);
  std::cout << "sizeof value_to_dest in pfmerge:" << value_to_dest_.size() << std::endl;
  set_cmd_->Initial(std::move(set_args),  db_name_);
  set_cmd_->SetConn(GetConn());
  set_cmd_->SetResp(resp_.lock());
  //value of this binlog might be strange if you print it out(eg: sEt hll_out XshellXshellXshellXshell ), but it's ok.
  set_cmd_->DoBinlog(slot);
}
