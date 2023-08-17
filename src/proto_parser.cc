/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "proto_parser.h"
#include "common.h"

#include <assert.h>

// 1 request -> multi strlist
// 2 multi -> * number crlf
// 3 strlist -> str strlist | empty
// 4 str -> strlen strval
// 5 strlen -> $ number crlf
// 6 strval -> string crlf

namespace pikiwidb {
void PProtoParser::Reset() {
  multi_ = -1;
  paramLen_ = -1;
  numOfParam_ = 0;

  // Optimize: Most redis command has 3 args
  while (params_.size() > 3) {
    params_.pop_back();
  }
}

PParseResult PProtoParser::ParseRequest(const char*& ptr, const char* end) {
  if (multi_ == -1) {
    auto parseRet = parseMulti(ptr, end, multi_);
    if (parseRet == PParseResult::error || multi_ < -1) {
      return PParseResult::error;
    }

    if (parseRet != PParseResult::ok) {
      return PParseResult::wait;
    }
  }

  return parseStrlist(ptr, end, params_);
}

PParseResult PProtoParser::parseMulti(const char*& ptr, const char* end, int& result) {
  if (end - ptr < 3) {
    return PParseResult::wait;
  }

  if (*ptr != '*') {
    return PParseResult::error;
  }

  ++ptr;

  return GetIntUntilCRLF(ptr, end - ptr, result);
}

PParseResult PProtoParser::parseStrlist(const char*& ptr, const char* end, std::vector<PString>& results) {
  while (static_cast<int>(numOfParam_) < multi_) {
    if (results.size() < numOfParam_ + 1) {
      results.resize(numOfParam_ + 1);
    }

    auto parseRet = parseStr(ptr, end, results[numOfParam_]);

    if (parseRet == PParseResult::ok) {
      ++numOfParam_;
    } else {
      return parseRet;
    }
  }

  results.resize(numOfParam_);
  return PParseResult::ok;
}

PParseResult PProtoParser::parseStr(const char*& ptr, const char* end, PString& result) {
  if (paramLen_ == -1) {
    auto parseRet = parseStrlen(ptr, end, paramLen_);
    if (parseRet == PParseResult::error || paramLen_ < -1) {
      return PParseResult::error;
    }

    if (parseRet != PParseResult::ok) {
      return PParseResult::wait;
    }
  }

  return parseStrval(ptr, end, result);
}

PParseResult PProtoParser::parseStrval(const char*& ptr, const char* end, PString& result) {
  assert(paramLen_ >= 0);

  if (static_cast<int>(end - ptr) < paramLen_ + 2) {
    return PParseResult::wait;
  }

  auto tail = ptr + paramLen_;
  if (tail[0] != '\r' || tail[1] != '\n') {
    return PParseResult::error;
  }

  result.assign(ptr, tail - ptr);
  ptr = tail + 2;
  paramLen_ = -1;

  return PParseResult::ok;
}

PParseResult PProtoParser::parseStrlen(const char*& ptr, const char* end, int& result) {
  if (end - ptr < 3) {
    return PParseResult::wait;
  }

  if (*ptr != '$') {
    return PParseResult::error;
  }

  ++ptr;

  const auto ret = GetIntUntilCRLF(ptr, end - ptr, result);
  if (ret != PParseResult::ok) {
    --ptr;
  }

  return ret;
}

}  // namespace pikiwidb
