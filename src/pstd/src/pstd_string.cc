// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
#include "pstd/include/pstd_string.h"

#include <sys/time.h>
#include <unistd.h>
#include <cctype>
#include <cfloat>
#include <climits>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sstream>

#include <algorithm>

#include "pstd/include/pstd_defer.h"

namespace pstd {

/* Glob-style pattern matching. */
int stringmatchlen(const char* pattern, int patternLen, const char* string, int stringLen, int nocase) {
  while (patternLen != 0) {
    switch (pattern[0]) {
      case '*':
        while (pattern[1] == '*') {
          pattern++;
          patternLen--;
        }
        if (patternLen == 1) {
          return 1; /* match */
        }
        while (stringLen != 0) {
          if (stringmatchlen(pattern + 1, patternLen - 1, string, stringLen, nocase) != 0) {
            return 1; /* match */
          }
          string++;
          stringLen--;
        }
        return 0; /* no match */
        break;
      case '?':
        if (stringLen == 0) {
          return 0; /* no match */
        }
        string++;
        stringLen--;
        break;
      case '[': {
        int nott;
        int match;

        pattern++;
        patternLen--;
        nott = static_cast<int>(pattern[0] == '^');
        if (nott != 0) {
          pattern++;
          patternLen--;
        }
        match = 0;
        while (true) {
          if (pattern[0] == '\\') {
            pattern++;
            patternLen--;
            if (pattern[0] == string[0]) {
              match = 1;
            }
          } else if (pattern[0] == ']') {
            break;
          } else if (patternLen == 0) {
            pattern--;
            patternLen++;
            break;
          } else if (pattern[1] == '-' && patternLen >= 3) {
            int start = pattern[0];
            int end = pattern[2];
            int c = string[0];
            if (start > end) {
              int t = start;
              start = end;
              end = t;
            }
            if (nocase != 0) {
              start = tolower(start);
              end = tolower(end);
              c = tolower(c);
            }
            pattern += 2;
            patternLen -= 2;
            if (c >= start && c <= end) {
              match = 1;
            }
          } else {
            if (nocase == 0) {
              if (pattern[0] == string[0]) {
                match = 1;
              }
            } else {
              if (tolower(static_cast<int>(pattern[0])) == tolower(static_cast<int>(string[0]))) {
                match = 1;
              }
            }
          }
          pattern++;
          patternLen--;
        }
        if (nott != 0) {
          match = static_cast<int>(match == 0);
        }
        if (match == 0) {
          return 0; /* no match */
        }
        string++;
        stringLen--;
        break;
      }
      case '\\':
        if (patternLen >= 2) {
          pattern++;
          patternLen--;
        }
        /* fall through */
      default:
        if (nocase == 0) {
          if (pattern[0] != string[0]) {
            return 0; /* no match */
          }
        } else {
          if (tolower(static_cast<int>(pattern[0])) != tolower(static_cast<int>(string[0]))) {
            return 0; /* no match */
          }
        }
        string++;
        stringLen--;
        break;
    }
    pattern++;
    patternLen--;
    if (stringLen == 0) {
      while (*pattern == '*') {
        pattern++;
        patternLen--;
      }
      break;
    }
  }
  if (patternLen == 0 && stringLen == 0) {
    return 1;
  }
  return 0;
}

int stringmatch(const char* pattern, const char* string, int nocase) {
  return stringmatchlen(pattern, static_cast<int32_t>(strlen(pattern)), 
				        string, static_cast<int32_t>(strlen(string)), nocase);
}

/* Convert a string representing an amount of memory into the number of
 * bytes, so for instance memtoll("1Gi") will return 1073741824 that is
 * (1024*1024*1024).
 *
 * On parsing error, if *err is not null, it's set to 1, otherwise it's
 * set to 0 */
long long memtoll(const char* p, int* err) {
  const char* u;
  char buf[128];
  long mul; /* unit multiplier */
  long long val;
  unsigned int digits;

  if (err) {
    *err = 0;
  }
  /* Search the first non digit character. */
  u = p;
  if (*u == '-') {
    u++;
  }
  while ((*u != 0) && (isdigit(*u) != 0)) {
    u++;
  }
  if (*u == '\0' || (strcasecmp(u, "b") == 0)) {
    mul = 1;
  } else if (strcasecmp(u, "k") == 0) {
    mul = 1000;
  } else if (strcasecmp(u, "kb") == 0) {
    mul = 1024;
  } else if (strcasecmp(u, "m") == 0) {
    mul = 1000 * 1000;
  } else if (strcasecmp(u, "mb") == 0) {
    mul = 1024 * 1024;
  } else if (strcasecmp(u, "g") == 0) {
    mul = 1000L * 1000 * 1000;
  } else if (strcasecmp(u, "gb") == 0) {
    mul = 1024L * 1024 * 1024;
  } else {
    if (err) {
      *err = 1;
    }
    mul = 1;
  }
  digits = u - p;
  if (digits >= sizeof(buf)) {
    if (err) {
      *err = 1;
    }
    return LLONG_MAX;
  }
  memcpy(buf, p, digits);
  buf[digits] = '\0';
  val = strtoll(buf, nullptr, 10);
  return val * mul;
}

/* Return the number of digits of 'v' when converted to string in radix 10.
 * See ll2string() for more information. */
uint32_t digits10(uint64_t v) {
  if (v < 10) {
    return 1;
  }
  if (v < 100) {
    return 2;
  }
  if (v < 1000) {
    return 3;
  }
  if (v < 1000000000000UL) {
    if (v < 100000000UL) {
      if (v < 1000000) {
        if (v < 10000) {
          return 4;
        }
        return 5 + static_cast<int>(v >= 100000);
      }
      return 7 + static_cast<int>(v >= 10000000UL);
    }
    if (v < 10000000000UL) {
      return 9 + static_cast<int>(v >= 1000000000UL);
    }
    return 11 + static_cast<int>(v >= 100000000000UL);
  }
  return 12 + digits10(v / 1000000000000UL);
}

/* Convert a long long into a string. Returns the number of
 * characters needed to represent the number.
 * If the buffer is not big enough to store the string, 0 is returned.
 *
 * Based on the following article (that apparently does not provide a
 * novel approach but only publicizes an already used technique):
 *
 * https://www.facebook.com/notes/facebook-engineering/three-optimization-tips-for-c/10151361643253920
 *
 * Modified in order to handle signed integers since the original code was
 * designed for unsigned integers. */
int ll2string(char* dst, size_t dstlen, long long svalue) {
  static const char digits[201] =
      "0001020304050607080910111213141516171819"
      "2021222324252627282930313233343536373839"
      "4041424344454647484950515253545556575859"
      "6061626364656667686970717273747576777879"
      "8081828384858687888990919293949596979899";
  int negative;
  unsigned long long value;

  /* The main loop works with 64bit unsigned integers for simplicity, so
   * we convert the number here and remember if it is negative. */
  if (svalue < 0) {
    if (svalue != LLONG_MIN) {
      value = -svalue;
    } else {
      value = (static_cast<unsigned long long>(LLONG_MAX) + 1);
    }
    negative = 1;
  } else {
    value = svalue;
    negative = 0;
  }

  /* Check length. */
  uint32_t const length = digits10(value) + negative;
  if (length >= dstlen) {
    return 0;
  }

  /* Null term. */
  uint32_t next = length;
  dst[next] = '\0';
  next--;
  while (value >= 100) {
    int const i = static_cast<int32_t>((value % 100) * 2);
    value /= 100;
    dst[next] = digits[i + 1];
    dst[next - 1] = digits[i];
    next -= 2;
  }

  /* Handle last 1-2 digits. */
  if (value < 10) {
    dst[next] = static_cast<char>('0' + value);
  } else {
    auto i = static_cast<uint32_t>(value) * 2;
    dst[next] = digits[i + 1];
    dst[next - 1] = digits[i];
  }

  /* Add sign. */
  if (negative != 0) {
    dst[0] = '-';
  }
  return static_cast<int32_t>(length);
}

/* Convert a string into a long long. Returns 1 if the string could be parsed
 * into a (non-overflowing) long long, 0 otherwise. The value will be set to
 * the parsed value when appropriate. */
int string2int(const char* s, size_t slen, long long* value) {
  const char* p = s;
  size_t plen = 0;
  int negative = 0;
  unsigned long long v;

  if (plen == slen) {
    return 0;
  }

  /* Special case: first and only digit is 0. */
  if (slen == 1 && p[0] == '0') {
    if (value) {
      *value = 0;
    }
    return 1;
  }

  if (p[0] == '-') {
    negative = 1;
    p++;
    plen++;

    /* Abort on only a negative sign. */
    if (plen == slen) {
      return 0;
    }
  }

  while (plen < slen && p[0] == '0') {
    p++;
    plen++;
  }

  if (plen == slen) {
    if (value) {
      *value = 0;
    }
    return 1;
  }

  /* First digit should be 1-9, otherwise the string should just be 0. */
  if (p[0] >= '1' && p[0] <= '9') {
    v = p[0] - '0';
    p++;
    plen++;
  } else if (p[0] == '0' && slen == 1) {
    *value = 0;
    return 1;
  } else {
    return 0;
  }

  while (plen < slen && p[0] >= '0' && p[0] <= '9') {
    if (v > (ULLONG_MAX / 10)) { /* Overflow. */
      return 0;
    }
    v *= 10;

    if (v > (ULLONG_MAX - (p[0] - '0'))) { /* Overflow. */
      return 0;
    }
    v += p[0] - '0';

    p++;
    plen++;
  }

  /* Return if not all bytes were used. */
  if (plen < slen) {
    return 0;
  }

  if (negative != 0) {
    if (v > (static_cast<unsigned long long>(-(LLONG_MIN + 1)) + 1)) { /* Overflow. */
      return 0;
    }
    if (value) {
      *value = static_cast<long long>(-v);
    }
  } else {
    if (v > LLONG_MAX) { /* Overflow. */
      return 0;
    }
    if (value) {
      *value = static_cast<long long>(v);
    }
  }
  return 1;
}

/* Convert a string into a long. Returns 1 if the string could be parsed into a
 * (non-overflowing) long, 0 otherwise. The value will be set to the parsed
 * value when appropriate. */
int string2int(const char* s, size_t slen, long* lval) {
  long long llval;

  if (string2int(s, slen, &llval) == 0) {
    return 0;
  }

  if (llval < LONG_MIN || llval > LONG_MAX) {
    return 0;
  }

  *lval = static_cast<long>(llval);
  return 1;
}

/* Convert a string into a unsigned long. Returns 1 if the string could be parsed into a
 * (non-overflowing) unsigned long, 0 otherwise. The value will be set to the parsed
 * value when appropriate. */
int string2int(const char* s, size_t slen, unsigned long* lval) {
  long long llval;

  if (string2int(s, slen, &llval) == 0) {
    return 0;
  }

  if (llval > static_cast<long long>(ULONG_MAX)) {
    return 0;
  }

  *lval = static_cast<unsigned long>(llval);
  return 1;
}

/* Convert a double to a string representation. Returns the number of bytes
 * required. The representation should always be parsable by strtod(3). */
int d2string(char* buf, size_t len, double value) {
  if (std::isnan(value)) {
    len = snprintf(buf, len, "nan");
  } else if (std::isinf(value)) {
    if (value < 0) {
      len = snprintf(buf, len, "-inf");
    } else {
      len = snprintf(buf, len, "inf");
    }
  } else if (value == 0) {
    /* See: http://en.wikipedia.org/wiki/Signed_zero, "Comparisons". */
    if (1.0 / value < 0) {
      len = snprintf(buf, len, "-0");
    } else {
      len = snprintf(buf, len, "0");
    }
  } else {
#if (DBL_MANT_DIG >= 52) && (LLONG_MAX == 0x7fffffffffffffffLL)
    /* Check if the float is in a safe range to be casted into a
     * long long. We are assuming that long long is 64 bit here.
     * Also we are assuming that there are no implementations around where
     * double has precision < 52 bit.
     *
     * Under this assumptions we test if a double is inside an interval
     * where casting to long long is safe. Then using two castings we
     * make sure the decimal part is zero. If all this is true we use
     * integer printing function that is much faster. */
    double min = -4503599627370495; /* (2^52)-1 */
    double max = 4503599627370496;  /* -(2^52) */
    if (value > min && value < max && value == (static_cast<double>(static_cast<long long>(value)))) {
      len = ll2string(buf, len, static_cast<long long>(value));
    } else  // NOLINT
#endif
      len = snprintf(buf, len, "%.17g", value);
  }

  return static_cast<int32_t>(len);
}

int string2d(const char* s, size_t slen, double* dval) {
  char* pEnd;
  double d = strtod(s, &pEnd);
  if (pEnd != s + slen) {
    return 0;
  }

  if (dval) {
    *dval = d;
  }
  return 1;
}

/* Generate the Redis "Run ID", a SHA1-sized random number that identifies a
 * given execution of Redis, so that if you are talking with an instance
 * having run_id == A, and you reconnect and it has run_id == B, you can be
 * sure that it is either a different instance or it was restarted. */
std::string getRandomHexChars(const size_t len) {
  FILE* fp = fopen("/dev/urandom", "r");
  DEFER {
    if (fp) {
      fclose(fp);
      fp = nullptr;
    }
  };

  char charset[] = "0123456789abcdef";
  unsigned int j{0};
  std::string buf(len, '\0');
  char* p = buf.data();

  if (!fp || !fread(p, len, 1, fp)) {
    /* If we can't read from /dev/urandom, do some reasonable effort
     * in order to create some entropy, since this function is used to
     * generate run_id and cluster instance IDs */
    char* x = p;
    unsigned int l = len;
    struct timeval tv;
    pid_t pid = getpid();

    /* Use time and PID to fill the initial array. */
    gettimeofday(&tv, nullptr);
    if (l >= sizeof(tv.tv_usec)) {
      memcpy(x, &tv.tv_usec, sizeof(tv.tv_usec));
      l -= sizeof(tv.tv_usec);
      x += sizeof(tv.tv_usec);
    }
    if (l >= sizeof(tv.tv_sec)) {
      memcpy(x, &tv.tv_sec, sizeof(tv.tv_sec));
      l -= sizeof(tv.tv_sec);
      x += sizeof(tv.tv_sec);
    }
    if (l >= sizeof(pid)) {
      memcpy(x, &pid, sizeof(pid));
      l -= sizeof(pid);
      x += sizeof(pid);
    }
    /* Finally xor it with rand() output, that was already seeded with
     * time() at startup. */
    for (j = 0; j < len; j++) {
      p[j] = static_cast<char>(p[j] ^ rand());
    }
  }
  /* Turn it into hex digits taking just 4 bits out of 8 for every byte. */
  for (j = 0; j < len; j++) {
    p[j] = charset[p[j] & 0x0F];
  }
  return std::string(p, len);
}

std::vector<std::string>& StringSplit(const std::string& s, char delim, std::vector<std::string>& elems) {
  elems.clear();
  std::stringstream ss(s);
  std::string item;
  while (std::getline(ss, item, delim)) {
    if (!item.empty()) {
      elems.push_back(item);
    }
  }
  return elems;
}

std::string StringConcat(const std::vector<std::string>& elems, char delim) {
  std::string result;
  auto it = elems.begin();
  while (it != elems.end()) {
    result.append(*it);
    result.append(1, delim);
    ++it;
  }
  if (!result.empty()) {
    result.resize(result.size() - 1);
  }
  return result;
}

std::string& StringToLower(std::string& ori) {
  std::transform(ori.begin(), ori.end(), ori.begin(), ::tolower);
  return ori;
}

std::string& StringToUpper(std::string& ori) {
  std::transform(ori.begin(), ori.end(), ori.begin(), ::toupper);
  return ori;
}

std::string IpPortString(const std::string& ip, int port) {
  if (ip.empty()) {
    return {};
  }
  char buf[10];
  if (ll2string(buf, sizeof(buf), port) <= 0) {
    return {};
  }
  return (ip + ":" + buf);
}

std::string ToRead(const std::string& str) {
  std::string read;
  if (str.empty()) {
    return read;
  }
  read.append(1, '"');
  char buf[16];
  std::string::const_iterator iter = str.begin();
  while (iter != str.end()) {
    switch (*iter) {
      case '\\':
      case '"':
        read.append(1, '\\');
        read.append(1, *iter);
        break;
      case '\n':
        read.append("\\n");
        break;
      case '\r':
        read.append("\\r");
        break;
      case '\t':
        read.append("\\t");
        break;
      case '\a':
        read.append("\\a");
        break;
      case '\b':
        read.append("\\b");
        break;
      default:
        if (isprint(*iter) != 0) {
          read.append(1, *iter);
        } else {
          snprintf(buf, sizeof(buf), "\\x%02x", static_cast<unsigned char>(*iter));
          read.append(buf);
        }
        break;
    }
    iter++;
  }
  read.append(1, '"');
  return read;
}

bool ParseIpPortString(const std::string& ip_port, std::string& ip, int& port) {
  if (ip_port.empty()) {
    return false;
  }
  size_t pos = ip_port.find(':');
  if (pos == std::string::npos) {
    return false;
  }
  ip = ip_port.substr(0, pos);
  std::string port_str = ip_port.substr(pos + 1);
  long lport = 0;
  if (1 != string2int(port_str.data(), port_str.size(), &lport)) {
    return false;
  }
  port = static_cast<int>(lport);
  return true;
}

// Trim charlist
std::string StringTrim(const std::string& ori, const std::string& charlist) {
  if (ori.empty()) {
    return ori;
  }

  size_t pos = 0;
  size_t rpos = ori.size() - 1;
  while (pos < ori.size()) {
    bool meet = false;
    for (char c : charlist) {
      if (ori.at(pos) == c) {
        meet = true;
        break;
      }
    }
    if (!meet) {
      break;
    }
    ++pos;
  }
  while (rpos > 0) {
    bool meet = false;
    for (char c : charlist) {
      if (ori.at(rpos) == c) {
        meet = true;
        break;
      }
    }
    if (!meet) {
      break;
    }
    --rpos;
  }
  return ori.substr(pos, rpos - pos + 1);
}

bool isspace(const std::string& str) {
  return std::count_if(str.begin(), str.end(), [](unsigned char c) { return std::isspace(c); });
}

}  // namespace pstd
