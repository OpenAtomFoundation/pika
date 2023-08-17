/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <stdio.h>
#include <strings.h>
#include <algorithm>
#include <cstddef>
#include <functional>
#include <vector>
#include "pstring.h"

#define CRLF "\r\n"

namespace pikiwidb {

const int kStringMaxBytes = 1 * 1024 * 1024 * 1024;

enum PType {
  PType_invalid,
  PType_string,
  PType_list,
  PType_set,
  PType_sortedSet,
  PType_hash,
  // < 16
};

enum PEncode {
  PEncode_invalid,

  PEncode_raw,  // string
  PEncode_int,  // string as int

  PEncode_list,

  PEncode_set,
  PEncode_hash,

  PEncode_zset,
};

inline const char* EncodingStringInfo(unsigned encode) {
  switch (encode) {
    case PEncode_raw:
      return "raw";

    case PEncode_int:
      return "int";

    case PEncode_list:
      return "list";

    case PEncode_set:
      return "set";

    case PEncode_hash:
      return "hash";

    case PEncode_zset:
      return "zset";

    default:
      break;
  }

  return "unknown";
}

enum PError {
  PError_nop = -1,
  PError_ok = 0,
  PError_type = 1,
  PError_exist = 2,
  PError_notExist = 3,
  PError_param = 4,
  PError_unknowCmd = 5,
  PError_nan = 6,
  PError_syntax = 7,
  PError_dirtyExec = 8,
  PError_watch = 9,
  PError_noMulti = 10,
  PError_invalidDB = 11,
  PError_readonlySlave = 12,
  PError_needAuth = 13,
  PError_errAuth = 14,
  PError_nomodule = 15,
  PError_moduleinit = 16,
  PError_moduleuninit = 17,
  PError_modulerepeat = 18,
  PError_max,
};

extern struct PErrorInfo {
  int len;
  const char* errorStr;
} g_errorInfo[];

template <typename T>
inline std::size_t Number2Str(char* ptr, std::size_t nBytes, T val) {
  if (!ptr || nBytes < 2) {
    return 0;
  }

  if (val == 0) {
    ptr[0] = '0';
    ptr[1] = 0;
    return 1;
  }

  bool negative = false;
  if (val < 0) {
    negative = true;
    val = -val;
  }

  std::size_t off = 0;
  while (val > 0) {
    if (off >= nBytes) {
      return 0;
    }

    ptr[off++] = val % 10 + '0';
    val /= 10;
  }

  if (negative) {
    if (off >= nBytes) {
      return 0;
    }

    ptr[off++] = '-';
  }

  std::reverse(ptr, ptr + off);
  ptr[off] = 0;

  return off;
}

int Double2Str(char* ptr, std::size_t nBytes, double val);
bool TryStr2Long(const char* ptr, std::size_t nBytes, long& val);  // only for decimal
bool Strtol(const char* ptr, std::size_t nBytes, long* outVal);
bool Strtoll(const char* ptr, std::size_t nBytes, long long* outVal);
bool Strtof(const char* ptr, std::size_t nBytes, float* outVal);
bool Strtod(const char* ptr, std::size_t nBytes, double* outVal);
const char* Strstr(const char* ptr, std::size_t nBytes, const char* pattern, std::size_t nBytes2);
const char* SearchCRLF(const char* ptr, std::size_t nBytes);

class UnboundedBuffer;

std::size_t FormatInt(long value, UnboundedBuffer* reply);
std::size_t FormatSingle(const char* str, std::size_t len, UnboundedBuffer* reply);
std::size_t FormatSingle(const PString& str, UnboundedBuffer* reply);
std::size_t FormatBulk(const char* str, std::size_t len, UnboundedBuffer* reply);
std::size_t FormatBulk(const PString& str, UnboundedBuffer* reply);
std::size_t PreFormatMultiBulk(std::size_t nBulk, UnboundedBuffer* reply);

std::size_t FormatEmptyBulk(UnboundedBuffer* reply);
std::size_t FormatNull(UnboundedBuffer* reply);
std::size_t FormatNullArray(UnboundedBuffer* reply);
std::size_t FormatOK(UnboundedBuffer* reply);
std::size_t Format1(UnboundedBuffer* reply);
std::size_t Format0(UnboundedBuffer* reply);

void ReplyError(PError err, UnboundedBuffer* reply);

inline void AdjustIndex(long& start, long& end, size_t size) {
  if (size == 0) {
    end = 0, start = 1;
    return;
  }

  if (start < 0) {
    start += size;
  }
  if (start < 0) {
    start = 0;
  }
  if (end < 0) {
    end += size;
  }

  if (end >= static_cast<long>(size)) {
    end = size - 1;
  }
}

struct NocaseComp {
  bool operator()(const PString& s1, const PString& s2) const { return strcasecmp(s1.c_str(), s2.c_str()) < 0; }

  bool operator()(const char* s1, const PString& s2) const { return strcasecmp(s1, s2.c_str()) < 0; }

  bool operator()(const PString& s1, const char* s2) const { return strcasecmp(s1.c_str(), s2) < 0; }
};

enum class PParseResult : int8_t {
  ok,
  wait,
  error,
};

PParseResult GetIntUntilCRLF(const char*& ptr, std::size_t nBytes, int& val);

std::vector<PString> SplitString(const PString& str, char seperator);

// The defer class for C++11
class ExecuteOnScopeExit {
 public:
  ExecuteOnScopeExit() {}

  ExecuteOnScopeExit(ExecuteOnScopeExit&& e) { func_ = std::move(e.func_); }

  ExecuteOnScopeExit(const ExecuteOnScopeExit& e) = delete;
  void operator=(const ExecuteOnScopeExit& f) = delete;

  template <typename F, typename... Args>
  ExecuteOnScopeExit(F&& f, Args&&... args) {
    auto temp = std::bind(std::forward<F>(f), std::forward<Args>(args)...);
    func_ = [temp]() { (void)temp(); };
  }

  ~ExecuteOnScopeExit() noexcept {
    if (func_) {
      func_();
    }
  }

 private:
  std::function<void()> func_;
};

#define CONCAT(a, b) a##b
#define _MAKE_DEFER_HELPER_(line) pikiwidb::ExecuteOnScopeExit CONCAT(defer, line) = [&]()

#define DEFER _MAKE_DEFER_HELPER_(__LINE__)

bool NotGlobRegex(const char* pattern, std::size_t plen);

}  // namespace pikiwidb

int64_t Now();
