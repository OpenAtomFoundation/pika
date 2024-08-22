// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_pkhash.h"

#include "pstd/include/pstd_string.h"

#include "include/pika_cache.h"
#include "include/pika_conf.h"
#include "include/pika_slot_command.h"

extern std::unique_ptr<PikaConf> g_pika_conf;

void PKHExpireCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePKHExpire);
    return;
  }
  key_ = argv_[1];
  auto iter = argv_.begin();
  // ttl
  if (pstd::string2int(argv_[2].data(), argv_[2].size(), &ttl_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  iter++;
  iter++;
  iter++;
  iter++;
  iter++;
  if (pstd::string2int(argv_[4].data(), argv_[4].size(), &numfields_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  fields_.assign(iter, argv_.end());
}

void PKHExpireCmd::Do() {
  std::vector<int32_t> rets;
  s_ = db_->storage()->PKHExpire(key_, ttl_, numfields_, fields_, &rets);
  if (s_.ok()) {
    res_.AppendArrayLenUint64(rets.size());
    for (const auto& ret : rets) {
      res_.AppendInteger(ret);
    }
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void PKHExpireatCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePKHExpire);
    return;
  }

  key_ = argv_[1];
  auto iter = argv_.begin();
  if (pstd::string2int(argv_[2].data(), argv_[2].size(), &timestamp_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }

  iter++;
  iter++;
  iter++;
  iter++;
  iter++;

  if (pstd::string2int(argv_[4].data(), argv_[4].size(), &numfields_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }

  fields_.assign(iter, argv_.end());
}
void PKHExpireatCmd::Do() {
  std::vector<int32_t> rets;
  s_ = db_->storage()->PKHExpireat(key_, timestamp_, numfields_, fields_, &rets);
  if (s_.ok()) {
    res_.AppendArrayLenUint64(rets.size());
    for (const auto& ret : rets) {
      res_.AppendInteger(ret);
    }
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void PKHExpiretimeCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePKHExpire);
    return;
  }

  key_ = argv_[1];
  auto iter = argv_.begin();

  iter++;
  iter++;
  iter++;
  iter++;

  if (pstd::string2int(argv_[3].data(), argv_[3].size(), &numfields_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }

  fields_.assign(iter, argv_.end());
}
void PKHExpiretimeCmd::Do() {
  std::vector<int64_t> timestamps;
  s_ = db_->storage()->PKHExpiretime(key_, numfields_, fields_, &timestamps);
  if (s_.ok()) {
    res_.AppendArrayLenUint64(timestamps.size());
    for (const auto& timestamp : timestamps) {
      res_.AppendInteger(timestamp);
    }
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void PKHPersistCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePKHExpire);
    return;
  }

  key_ = argv_[1];
  auto iter = argv_.begin();
  iter++;
  iter++;
  iter++;
  iter++;
  if (pstd::string2int(argv_[3].data(), argv_[3].size(), &numfields_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }

  fields_.assign(iter, argv_.end());
}
void PKHPersistCmd::Do() {
  std::vector<int32_t> rets;
  s_ = db_->storage()->PKHPersist(key_, numfields_, fields_, &rets);
  if (s_.ok()) {
    res_.AppendArrayLenUint64(rets.size());
    for (const auto& ret : rets) {
      res_.AppendInteger(ret);
    }
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void PKHTTLCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePKHExpire);
    return;
  }

  key_ = argv_[1];
  auto iter = argv_.begin();
  iter++;
  iter++;
  iter++;
  iter++;
  if (pstd::string2int(argv_[3].data(), argv_[3].size(), &numfields_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  //  TODO(DDD) fields_.assign(argv_.begin() + 4, argv_.end());
  fields_.assign(iter, argv_.end());
}
void PKHTTLCmd::Do() {
  std::vector<int64_t> ttls;
  s_ = db_->storage()->PKHTTL(key_, numfields_, fields_, &ttls);
  if (s_.ok()) {
    res_.AppendArrayLenUint64(ttls.size());
    for (const auto& ttl : ttls) {
      res_.AppendInteger(ttl);
    }
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void PKHGetCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHGet);
    return;
  }
  key_ = argv_[1];
  field_ = argv_[2];
}

void PKHGetCmd::Do() {
  std::string value;
  s_ = db_->storage()->PKHGet(key_, field_, &value);
  if (s_.ok()) {
    res_.AppendStringLenUint64(value.size());
    res_.AppendContent(value);
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else if (s_.IsNotFound()) {
    res_.AppendContent("$-1");
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void PKHGetCmd::ReadCache() {
  std::string value;
  auto s = db_->cache()->HGet(key_, field_, &value);
  if (s.ok()) {
    res_.AppendStringLen(value.size());
    res_.AppendContent(value);
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void PKHGetCmd::DoThroughDB() {
  res_.clear();
  Do();
}
void PKHGetCmd::DoUpdateCache() {
}

void PKHSetCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHSet);
    return;
  }
  key_ = argv_[1];
  field_ = argv_[2];
  value_ = argv_[3];
}

void PKHSetCmd::Do() {
  int32_t ret = 0;
  s_ = db_->storage()->PKHSet(key_, field_, value_, &ret);
  if (s_.ok()) {
    res_.AppendContent(":" + std::to_string(ret));
    AddSlotKey("h", key_, db_);
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void PKHSetCmd::DoThroughDB() { Do(); }

void PKHSetCmd::DoUpdateCache() {
}