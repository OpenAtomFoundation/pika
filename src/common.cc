/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "common.h"
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <limits>
#include "unbounded_buffer.h"

namespace pikiwidb {

struct PErrorInfo g_errorInfo[] = {
    {sizeof "+OK\r\n" - 1, "+OK\r\n"},
    {sizeof "-ERR Operation against a key holding the wrong kind of value\r\n" - 1,
     "-ERR Operation against a key holding the wrong kind of value\r\n"},
    {sizeof "-ERR already exist" - 1, "-ERR already exist"},
    {sizeof "-ERR no such key\r\n" - 1, "-ERR no such key\r\n"},
    {sizeof "-ERR wrong number of arguments\r\n" - 1, "-ERR wrong number of arguments\r\n"},
    {sizeof "-ERR Unknown command\r\n" - 1, "-ERR Unknown command\r\n"},
    {sizeof "-ERR value is not an integer or out of range\r\n" - 1, "-ERR value is not an integer or out of range\r\n"},
    {sizeof "-ERR syntax error\r\n" - 1, "-ERR syntax error\r\n"},

    {sizeof "-EXECABORT Transaction discarded because of previous errors.\r\n" - 1,
     "-EXECABORT Transaction discarded because of previous errors.\r\n"},
    {sizeof "-WATCH inside MULTI is not allowed\r\n" - 1, "-WATCH inside MULTI is not allowed\r\n"},
    {sizeof "-EXEC without MULTI\r\n" - 1, "-EXEC without MULTI\r\n"},
    {sizeof "-ERR invalid DB index\r\n" - 1, "-ERR invalid DB index\r\n"},
    {sizeof "-READONLY You can't write against a read only slave.\r\n" - 1,
     "-READONLY You can't write against a read only slave.\r\n"},
    {sizeof "-ERR operation not permitted\r\n" - 1, "-ERR operation not permitted\r\n"},
    {sizeof "-ERR invalid password\r\n" - 1, "-ERR invalid password\r\n"},
    {sizeof "-ERR no such module\r\n" - 1, "-ERR no such module\r\n"},
    {sizeof "-ERR init module failed\r\n" - 1, "-ERR init module failed\r\n"},
    {sizeof "-ERR uninit module failed\r\n" - 1, "-ERR uninit module failed\r\n"},
    {sizeof "-ERR module already loaded\r\n" - 1, "-ERR module already loaded\r\n"},
};

int Double2Str(char* ptr, std::size_t nBytes, double val) { return snprintf(ptr, nBytes - 1, "%.6g", val); }

bool TryStr2Long(const char* ptr, size_t nBytes, long& val) {
  bool negtive = false;
  size_t i = 0;

  if (ptr[0] == '-' || ptr[0] == '+') {
    if (nBytes <= 1 || !isdigit(ptr[1])) {
      return false;
    }

    negtive = (ptr[0] == '-');
    i = 1;
  }

  val = 0;
  for (; i < nBytes; ++i) {
    if (!isdigit(ptr[i])) {
      break;
    }

    if (!negtive && val > std::numeric_limits<long>::max() / 10) {
      std::cerr << "long will overflow " << val << std::endl;
      return false;
    }

    if (negtive && val > (-(std::numeric_limits<long>::min() + 1)) / 10) {
      std::cerr << "long will underflow " << val << std::endl;
      return false;
    }

    val *= 10;

    if (!negtive && val > std::numeric_limits<long>::max() - (ptr[i] - '0')) {
      std::cerr << "long will overflow " << val << std::endl;
      return false;
    }

    if (negtive && (val - 1) > (-(std::numeric_limits<long>::min() + 1)) - (ptr[i] - '0')) {
      std::cerr << "long will underflow " << val << std::endl;
      return false;
    }

    val += ptr[i] - '0';
  }

  if (negtive) {
    val *= -1;
  }

  return true;
}

bool Strtol(const char* ptr, size_t nBytes, long* outVal) {
  if (nBytes == 0 || nBytes > 20) {  // include the sign
    return false;
  }

  errno = 0;
  char* pEnd = 0;
  *outVal = strtol(ptr, &pEnd, 0);

  if (errno == ERANGE || errno == EINVAL) {
    return false;
  }

  return pEnd == ptr + nBytes;
}

bool Strtoll(const char* ptr, size_t nBytes, long long* outVal) {
  if (nBytes == 0 || nBytes > 20) {
    return false;
  }

  errno = 0;
  char* pEnd = 0;
  *outVal = strtoll(ptr, &pEnd, 0);

  if (errno == ERANGE || errno == EINVAL) {
    return false;
  }

  return pEnd == ptr + nBytes;
}

bool Strtof(const char* ptr, size_t nBytes, float* outVal) {
  if (nBytes == 0 || nBytes > 20) {
    return false;
  }

  errno = 0;
  char* pEnd = 0;
  *outVal = strtof(ptr, &pEnd);

  if (errno == ERANGE || errno == EINVAL) {
    return false;
  }

  return pEnd == ptr + nBytes;
}

bool Strtod(const char* ptr, size_t nBytes, double* outVal) {
  if (nBytes == 0 || nBytes > 20) {
    return false;
  }

  errno = 0;
  char* pEnd = 0;
  *outVal = strtod(ptr, &pEnd);

  if (errno == ERANGE || errno == EINVAL) {
    return false;
  }

  return pEnd == ptr + nBytes;
}

const char* Strstr(const char* ptr, size_t nBytes, const char* pattern, size_t nBytes2) {
  if (!pattern || *pattern == 0) {
    return nullptr;
  }

  const char* ret = std::search(ptr, ptr + nBytes, pattern, pattern + nBytes2);
  return ret == ptr + nBytes ? nullptr : ret;
}

const char* SearchCRLF(const char* ptr, size_t nBytes) { return Strstr(ptr, nBytes, CRLF, 2); }

size_t FormatInt(long value, UnboundedBuffer* reply) {
  if (!reply) {
    return 0;
  }

  char val[32];
  int len = snprintf(val, sizeof val, "%ld" CRLF, value);

  size_t oldSize = reply->ReadableSize();
  reply->PushData(":", 1);
  reply->PushData(val, len);

  return reply->ReadableSize() - oldSize;
}

size_t FormatSingle(const char* str, size_t len, UnboundedBuffer* reply) {
  if (!reply) {
    return 0;
  }
  size_t oldSize = reply->ReadableSize();
  reply->PushData("+", 1);
  reply->PushData(str, len);
  reply->PushData(CRLF, 2);

  return reply->ReadableSize() - oldSize;
}

size_t FormatSingle(const PString& str, UnboundedBuffer* reply) { return FormatSingle(str.c_str(), str.size(), reply); }

size_t FormatBulk(const char* str, size_t len, UnboundedBuffer* reply) {
  if (!reply) {
    return 0;
  }

  size_t oldSize = reply->ReadableSize();
  reply->PushData("$", 1);

  char val[32];
  int tmp = snprintf(val, sizeof val - 1, "%lu" CRLF, len);
  reply->PushData(val, tmp);

  if (str && len > 0) {
    reply->PushData(str, len);
  }

  reply->PushData(CRLF, 2);

  return reply->ReadableSize() - oldSize;
}

size_t FormatBulk(const PString& str, UnboundedBuffer* reply) { return FormatBulk(str.c_str(), str.size(), reply); }

size_t PreFormatMultiBulk(size_t nBulk, UnboundedBuffer* reply) {
  if (!reply) {
    return 0;
  }

  size_t oldSize = reply->ReadableSize();
  reply->PushData("*", 1);

  char val[32];
  int tmp = snprintf(val, sizeof val - 1, "%lu" CRLF, nBulk);
  reply->PushData(val, tmp);

  return reply->ReadableSize() - oldSize;
}

std::size_t FormatEmptyBulk(UnboundedBuffer* reply) { return reply->PushData("$0" CRLF CRLF, 6); }

void ReplyError(PError err, UnboundedBuffer* reply) {
  if (!reply) {
    return;
  }

  const PErrorInfo& info = g_errorInfo[err];

  reply->PushData(info.errorStr, info.len);
}

size_t FormatNull(UnboundedBuffer* reply) {
  if (!reply) {
    return 0;
  }

  size_t oldSize = reply->ReadableSize();
  reply->PushData("$-1" CRLF, 5);

  return reply->ReadableSize() - oldSize;
}

size_t FormatNullArray(UnboundedBuffer* reply) {
  if (!reply) {
    return 0;
  }

  size_t oldSize = reply->ReadableSize();
  reply->PushData("*-1" CRLF, 5);

  return reply->ReadableSize() - oldSize;
}

size_t FormatOK(UnboundedBuffer* reply) {
  if (!reply) {
    return 0;
  }

  size_t oldSize = reply->ReadableSize();
  reply->PushData("+OK" CRLF, 5);

  return reply->ReadableSize() - oldSize;
}

size_t Format1(UnboundedBuffer* reply) {
  if (!reply) {
    return 0;
  }

  const char* val = ":1\r\n";

  size_t oldSize = reply->ReadableSize();
  reply->PushData(val, 4);

  return reply->ReadableSize() - oldSize;
}

size_t Format0(UnboundedBuffer* reply) {
  if (!reply) {
    return 0;
  }

  const char* val = ":0\r\n";

  size_t oldSize = reply->ReadableSize();
  reply->PushData(val, 4);

  return reply->ReadableSize() - oldSize;
}

PParseResult GetIntUntilCRLF(const char*& ptr, std::size_t nBytes, int& val) {
  if (nBytes < 3) {
    return PParseResult::wait;
  }

  std::size_t i = 0;
  bool negtive = false;
  if (ptr[0] == '-') {
    negtive = true;
    ++i;
  } else if (ptr[0] == '+') {
    ++i;
  }

  int value = 0;
  for (; i < nBytes; ++i) {
    if (isdigit(ptr[i])) {
      value *= 10;
      value += ptr[i] - '0';
    } else {
      if (ptr[i] != '\r' || (i + 1 < nBytes && ptr[i + 1] != '\n')) {
        return PParseResult::error;
      }

      if (i + 1 == nBytes) {
        return PParseResult::wait;
      }

      break;
    }
  }

  if (negtive) {
    value *= -1;
  }

  ptr += i;
  ptr += 2;
  val = value;
  return PParseResult::ok;
}

std::vector<PString> SplitString(const PString& str, char seperator) {
  std::vector<PString> results;

  PString::size_type start = 0;
  PString::size_type sep = str.find(seperator);
  while (sep != PString::npos) {
    if (start < sep) {
      results.emplace_back(str.substr(start, sep - start));
    }

    start = sep + 1;
    sep = str.find(seperator, start);
  }

  if (start != str.size()) {
    results.emplace_back(str.substr(start));
  }

  return results;
}

bool NotGlobRegex(const char* pattern, std::size_t plen) {
  for (std::size_t i(0); i < plen; ++i) {
    if (pattern[i] == '?' || pattern[i] == '\\' || pattern[i] == '[' || pattern[i] == ']' || pattern[i] == '*' ||
        pattern[i] == '^' || pattern[i] == '-') {
      return false;  //  may be regex, may not, who cares?
    }
  }

  return true;  // must not be regex
}

}  // namespace pikiwidb

int64_t Now() {
  using namespace std::chrono;
  auto now = system_clock::now();
  return duration_cast<milliseconds>(now.time_since_epoch()).count();
}
