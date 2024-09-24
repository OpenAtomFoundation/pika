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
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePKHSet);
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
void PKHGetCmd::DoUpdateCache() {}

void PKHSetCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePKHSet);
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

void PKHSetCmd::DoUpdateCache() {}
// 下面是新的命令。

void PKHSetexCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePKHSetex);
    return;
  }
  key_ = argv_[1];
  field_ = argv_[2];
  value_ = argv_[3];
}

void PKHSetexCmd::Do() {
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

void PKHSetexCmd::DoThroughDB() { Do(); }

void PKHSetexCmd::DoUpdateCache() {}

void PKHExistsCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePKHExists);
    return;
  }
  key_ = argv_[1];
  field_ = argv_[2];
}

void PKHExistsCmd::Do() {
  s_ = db_->storage()->HExists(key_, field_);
  if (s_.ok()) {
    res_.AppendContent(":1");
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else if (s_.IsNotFound()) {
    res_.AppendContent(":0");
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void PKHExistsCmd::DoThroughDB() { Do(); }

void PKHExistsCmd::DoUpdateCache() {}

void PKHDelCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePKHDel);
    return;
  }
  key_ = argv_[1];
  auto iter = argv_.begin();
  iter++;
  iter++;
  fields_.assign(iter, argv_.end());
}

void PKHDelCmd::Do() {
  s_ = db_->storage()->HDel(key_, fields_, &deleted_);

  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendInteger(deleted_);
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void PKHDelCmd::DoThroughDB() { Do(); }

void PKHDelCmd::DoUpdateCache() {}

void PKHLenCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePKHLen);
    return;
  }
  key_ = argv_[1];
}

void PKHLenCmd::Do() {
  int32_t len = 0;
  s_ = db_->storage()->HLen(key_, &len);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendInteger(len);
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, "something wrong in hlen");
  }
}

void PKHLenCmd::DoThroughDB() { Do(); }

void PKHLenCmd::DoUpdateCache() {}

void PKHStrLenCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePKHStrlen);
    return;
  }
  key_ = argv_[1];
  field_ = argv_[2];
}

void PKHStrLenCmd::Do() {
  int32_t len = 0;
  s_ = db_->storage()->HStrlen(key_, field_, &len);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendInteger(len);
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, "something wrong in hstrlen");
  }
}

void PKHStrLenCmd::DoThroughDB() {
  res_.clear();
  Do();
}

void PKHStrLenCmd::DoUpdateCache() {}


void PKHIncrbyCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePKHIncrby);
    return;
  }
  key_ = argv_[1];
  field_ = argv_[2];
  if (argv_[3].find(' ') != std::string::npos || (pstd::string2int(argv_[3].data(), argv_[3].size(), &by_) == 0)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
}

void PKHIncrbyCmd::Do() {
  int64_t new_value = 0;
  s_ = db_->storage()->HIncrby(key_, field_, by_, &new_value);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendContent(":" + std::to_string(new_value));
    AddSlotKey("h", key_, db_);
  } else if (s_.IsInvalidArgument() &&
             s_.ToString().substr(0, std::char_traits<char>::length(ErrTypeMessage)) == ErrTypeMessage) {
    res_.SetRes(CmdRes::kMultiKey);
  } else if (s_.IsCorruption() && s_.ToString() == "Corruption: hash value is not an integer") {
    res_.SetRes(CmdRes::kInvalidInt);
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kOverFlow);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void PKHIncrbyCmd::DoThroughDB() { Do(); }

void PKHIncrbyCmd::DoUpdateCache() {}


void PKHMSetCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePKHMSet);
    return;
  }
  key_ = argv_[1];
  size_t argc = argv_.size();
  if (argc % 2 != 0) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePKHMSet);
    return;
  }
  size_t index = 2;
  fvs_.clear();
  for (; index < argc; index += 2) {
    fvs_.push_back({argv_[index], argv_[index + 1]});
  }
}

void PKHMSetCmd::Do() {
  s_ = db_->storage()->HMSet(key_, fvs_);
  if (s_.ok()) {
    res_.SetRes(CmdRes::kOk);
    AddSlotKey("h", key_, db_);
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void PKHMSetCmd::DoThroughDB() { Do(); }

void PKHMSetCmd::DoUpdateCache() {}

void PKHMGetCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePKHMGet);
    return;
  }
  key_ = argv_[1];
  auto iter = argv_.begin();
  iter++;
  iter++;
  fields_.assign(iter, argv_.end());
}

void PKHMGetCmd::Do() {
  std::vector<storage::ValueStatus> vss;
  s_ = db_->storage()->HMGet(key_, fields_, &vss);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendArrayLenUint64(vss.size());
    for (const auto& vs : vss) {
      if (vs.status.ok()) {
        res_.AppendStringLenUint64(vs.value.size());
        res_.AppendContent(vs.value);
      } else {
        res_.AppendContent("$-1");
      }
    }
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void PKHMGetCmd::DoThroughDB() { Do(); }

void PKHMGetCmd::DoUpdateCache() {}


void PKHKeysCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePKHKeys);
    return;
  }
  key_ = argv_[1];
}

void PKHKeysCmd::Do() {
  std::vector<std::string> fields;
  s_ = db_->storage()->HKeys(key_, &fields);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendArrayLenUint64(fields.size());
    for (const auto& field : fields) {
      res_.AppendString(field);
    }
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void PKHKeysCmd::DoThroughDB() { Do(); }

void PKHKeysCmd::DoUpdateCache() {}


void PKHValsCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePKHVals);
    return;
  }
  key_ = argv_[1];
}

void PKHValsCmd::Do() {
  std::vector<std::string> values;
  s_ = db_->storage()->HVals(key_, &values);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendArrayLenUint64(values.size());
    for (const auto& value : values) {
      res_.AppendStringLenUint64(value.size());
      res_.AppendContent(value);
    }
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void PKHValsCmd::DoThroughDB() { Do(); }

void PKHValsCmd::DoUpdateCache() {}


void PKHGetAllCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePKHGetall);
    return;
  }
  key_ = argv_[1];
}

void PKHGetAllCmd::Do() {
  int64_t total_fv = 0;
  int64_t cursor = 0;
  int64_t next_cursor = 0;
  size_t raw_limit = g_pika_conf->max_client_response_size();
  std::string raw;
  std::vector<storage::FieldValue> fvs;

  do {
    fvs.clear();
    s_ = db_->storage()->HScan(key_, cursor, "*", PIKA_SCAN_STEP_LENGTH, &fvs, &next_cursor);
    if (!s_.ok()) {
      raw.clear();
      total_fv = 0;
      break;
    } else {
      for (const auto& fv : fvs) {
        RedisAppendLenUint64(raw, fv.field.size(), "$");
        RedisAppendContent(raw, fv.field);
        RedisAppendLenUint64(raw, fv.value.size(), "$");
        RedisAppendContent(raw, fv.value);
      }
      if (raw.size() >= raw_limit) {
        res_.SetRes(CmdRes::kErrOther, "Response exceeds the max-client-response-size limit");
        return;
      }
      total_fv += static_cast<int64_t>(fvs.size());
      cursor = next_cursor;
    }
  } while (cursor != 0);

  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendArrayLen(total_fv * 2);
    res_.AppendStringRaw(raw);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void PKHGetAllCmd::DoThroughDB() { Do(); }

void PKHGetAllCmd::DoUpdateCache() {}


void PKHScanCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePKHScan);
    return;
  }
  key_ = argv_[1];
  if (pstd::string2int(argv_[2].data(), argv_[2].size(), &cursor_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  size_t index = 3;
  size_t argc = argv_.size();

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

void PKHScanCmd::Do() {
  int64_t next_cursor = 0;
  std::vector<storage::FieldValue> field_values;
  auto s = db_->storage()->HScan(key_, cursor_, pattern_, count_, &field_values, &next_cursor);

  if (s.ok() || s.IsNotFound()) {
    res_.AppendContent("*2");
    char buf[32];
    int32_t len = pstd::ll2string(buf, sizeof(buf), next_cursor);
    res_.AppendStringLen(len);
    res_.AppendContent(buf);

    res_.AppendArrayLenUint64(field_values.size() * 2);
    for (const auto& field_value : field_values) {
      res_.AppendString(field_value.field);
      res_.AppendString(field_value.value);
    }
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void PKHScanCmd::DoThroughDB() { Do(); }

void PKHScanCmd::DoUpdateCache() {}

