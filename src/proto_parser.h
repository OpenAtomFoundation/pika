/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <vector>

#include "common.h"
#include "pstring.h"

namespace pikiwidb {

class PProtoParser {
 public:
  void Reset();
  PParseResult ParseRequest(const char*& ptr, const char* end);

  const std::vector<PString>& GetParams() const { return params_; }
  void SetParams(std::vector<PString> p) { params_ = std::move(p); }

  bool IsInitialState() const { return multi_ == -1; }

 private:
  PParseResult parseMulti(const char*& ptr, const char* end, int& result);
  PParseResult parseStrlist(const char*& ptr, const char* end, std::vector<PString>& results);
  PParseResult parseStr(const char*& ptr, const char* end, PString& result);
  PParseResult parseStrval(const char*& ptr, const char* end, PString& result);
  PParseResult parseStrlen(const char*& ptr, const char* end, int& result);

  int multi_ = -1;
  int paramLen_ = -1;

  size_t numOfParam_ = 0;  // for optimize
  std::vector<PString> params_;
};

}  // namespace pikiwidb

