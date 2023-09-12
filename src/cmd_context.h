/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <span>
#include <string>
#include <vector>
#include "client.h"
#include "common.h"

namespace pikiwidb {

const std::string NewLine = "\r\n";

class CmdRes {
 public:
  enum CmdRet {
    kNone = 0,
    kOk,
    kPong,
    kSyntaxErr,
    kInvalidInt,
    kInvalidBitInt,
    kInvalidBitOffsetInt,
    kInvalidBitPosArgument,
    kWrongBitOpNotNum,
    kInvalidFloat,
    kOverFlow,
    kNotFound,
    kOutOfRange,
    kInvalidPwd,
    kNoneBgsave,
    kPurgeExist,
    kInvalidParameter,
    kWrongNum,
    kInvalidIndex,
    kInvalidDbType,
    kInvalidDB,
    kInconsistentHashTag,
    kErrOther,
    KIncrByOverFlow,
  };

  CmdRes() = default;

  bool none() const { return ret_ == kNone && message_.empty(); }

  bool ok() const { return ret_ == kOk || ret_ == kNone; }

  void clear() {
    message_.clear();
    ret_ = kNone;
  }

  std::string raw_message() const { return message_; }

  std::string message() const;

  // Inline functions for Create Redis protocol
  inline void AppendStringLen(int64_t ori) { RedisAppendLen(message_, ori, "$"); }
  inline void AppendArrayLen(int64_t ori) { RedisAppendLen(message_, ori, "*"); }
  inline void AppendInteger(int64_t ori) { RedisAppendLen(message_, ori, ":"); }
  inline void AppendContent(const std::string& value) { RedisAppendContent(message_, value); }
  inline void AppendString(const std::string& value) {
    if (value.empty()) {
      AppendStringLen(-1);
    } else {
      AppendStringLen(value.size());
      AppendContent(value);
    }
  }

  inline void AppendStringRaw(const std::string& value) { message_.append(value); }

  void AppendStringVector(const std::vector<std::string>& strArray) {
    if (strArray.empty()) {
      AppendArrayLen(-1);
      return;
    }
    AppendArrayLen(strArray.size());
    for (const auto& item : strArray) {
      AppendString(item);
    }
  }

  inline void RedisAppendContent(std::string& str, const std::string& value) {
    str.append(value.data(), value.size());
    str.append(NewLine);
  }

  void RedisAppendLen(std::string& str, int64_t ori, const std::string& prefix) {
    char buf[32];
    pikiwidb::Number2Str<int64_t>(buf, sizeof buf, ori);
    str.append(prefix);
    str.append(buf);
    str.append(NewLine);
  }

  void SetRes(CmdRet _ret, const std::string& content = "") {
    ret_ = _ret;
    if (!content.empty()) {
      message_ = content;
    }
  }

 private:
  std::string message_;
  CmdRet ret_ = kNone;
};

struct CmdContext : public CmdRes {
  // the client corresponding to this connection
  PClient* client_;

  // All parameters of this command (including the command itself)
  // e.g：["set","key","value"]
  std::span<std::string> argv_;

  // key in this command
  // I haven't thought of a situation where there are multiple keys in one command.
  // If there are later, you can change it to vector.
  std::string key_;

  // the sub command of this command
  std::string subCmd_;
};

}  // namespace pikiwidb
