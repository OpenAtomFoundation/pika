//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <ctype.h>
#include <stdint.h>
#include <limits.h>

#include "src/coding.h"
#include "storage/util.h"

namespace storage {

/* Return the number of digits of 'v' when converted to string in radix 10.
 * See ll2string() for more information. */
uint32_t Digits10(uint64_t v) {
  if (v < 10) return 1;
  if (v < 100) return 2;
  if (v < 1000) return 3;
  if (v < 1000000000000UL) {
    if (v < 100000000UL) {
      if (v < 1000000) {
        if (v < 10000)
          return 4;
        return 5 + (v >= 100000);
      }
      return 7 + (v >= 10000000UL);
    }
    if (v < 10000000000UL) {
      return 9 + (v >= 1000000000UL);
    }
    return 11 + (v >= 100000000000UL);
  }
  return 12 + Digits10(v / 1000000000000UL);
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
int Int64ToStr(char* dst, size_t dstlen, int64_t svalue) {
    static const char digits[201] =
        "0001020304050607080910111213141516171819"
        "2021222324252627282930313233343536373839"
        "4041424344454647484950515253545556575859"
        "6061626364656667686970717273747576777879"
        "8081828384858687888990919293949596979899";
    int negative;
    uint64_t value;

    /* The main loop works with 64bit unsigned integers for simplicity, so
     * we convert the number here and remember if it is negative. */
    if (svalue < 0) {
        if (svalue != LLONG_MIN) {
            value = -svalue;
        } else {
            value = ((uint64_t) LLONG_MAX)+1;
        }
        negative = 1;
    } else {
        value = svalue;
        negative = 0;
    }

    /* Check length. */
    uint32_t const length = Digits10(value)+negative;
    if (length >= dstlen) return 0;

    /* Null term. */
    uint32_t next = length;
    dst[next] = '\0';
    next--;
    while (value >= 100) {
        int const i = (value % 100) * 2;
        value /= 100;
        dst[next] = digits[i + 1];
        dst[next - 1] = digits[i];
        next -= 2;
    }

    /* Handle last 1-2 digits. */
    if (value < 10) {
        dst[next] = '0' + (uint32_t) value;
    } else {
        int i = (uint32_t) value * 2;
        dst[next] = digits[i + 1];
        dst[next - 1] = digits[i];
    }

    /* Add sign. */
    if (negative) dst[0] = '-';
    return length;
}

/* Convert a string into a long long. Returns 1 if the string could be parsed
 * into a (non-overflowing) long long, 0 otherwise. The value will be set to
 * the parsed value when appropriate. */
int StrToInt64(const char *s, size_t slen, int64_t *value) {
    const char *p = s;
    size_t plen = 0;
    int negative = 0;
    uint64_t v;

    if (plen == slen)
        return 0;

    /* Special case: first and only digit is 0. */
    if (slen == 1 && p[0] == '0') {
        if (value != NULL) *value = 0;
        return 1;
    }

    if (p[0] == '-') {
        negative = 1;
        p++; plen++;

        /* Abort on only a negative sign. */
        if (plen == slen)
            return 0;
    }

    while (plen < slen && p[0] == '0') {
        p++; plen++;
    }

    if (plen == slen) {
        if (value != NULL) *value = 0;
        return 1;
    }

    /* First digit should be 1-9, otherwise the string should just be 0. */
    if (p[0] >= '1' && p[0] <= '9') {
        v = p[0]-'0';
        p++; plen++;
    } else if (p[0] == '0' && slen == 1) {
        *value = 0;
        return 1;
    } else {
        return 0;
    }

    while (plen < slen && p[0] >= '0' && p[0] <= '9') {
        if (v > (ULLONG_MAX / 10)) /* Overflow. */
            return 0;
        v *= 10;

        if (v > (ULLONG_MAX - (p[0]-'0'))) /* Overflow. */
            return 0;
        v += p[0]-'0';

        p++; plen++;
    }

    /* Return if not all bytes were used. */
    if (plen < slen)
        return 0;

    if (negative) {
        if (v > ((uint64_t)(-(LLONG_MIN+1))+1)) /* Overflow. */
            return 0;
        if (value != NULL) *value = -v;
    } else {
        if (v > LLONG_MAX) /* Overflow. */
            return 0;
        if (value != NULL) *value = v;
    }
    return 1;
}

/* Glob-style pattern matching. */
int StringMatch(const char *pattern, int pattern_len, const char* str,
                int string_len, int nocase) {
  while (pattern_len) {
    switch (pattern[0]) {
      case '*':
        while (pattern[1] == '*') {
          pattern++;
          pattern_len--;
        }
        if (pattern_len == 1)
          return 1; /* match */
        while (string_len) {
          if (StringMatch(pattern+1, pattern_len-1,
                          str, string_len, nocase))
            return 1; /* match */
          str++;
          string_len--;
        }
        return 0; /* no match */
        break;
      case '?':
        if (string_len == 0)
          return 0; /* no match */
        str++;
        string_len--;
        break;
      case '[':
        {
        int not_flag, match;
        pattern++;
        pattern_len--;
        not_flag = pattern[0] == '^';
        if (not_flag) {
          pattern++;
          pattern_len--;
        }
        match = 0;
        while (1) {
          if (pattern[0] == '\\') {
            pattern++;
            pattern_len--;
            if (pattern[0] == str[0])
              match = 1;
            } else if (pattern[0] == ']') {
              break;
            } else if (pattern_len == 0) {
              pattern--;
              pattern_len++;
              break;
            } else if (pattern[1] == '-' && pattern_len >= 3) {
              int start = pattern[0];
              int end = pattern[2];
              int c = str[0];
              if (start > end) {
                int t = start;
                start = end;
                end = t;
              }
              if (nocase) {
                start = tolower(start);
                end = tolower(end);
                c = tolower(c);
              }
              pattern += 2;
              pattern_len -= 2;
              if (c >= start && c <= end)
                match = 1;
              } else {
                if (!nocase) {
                  if (pattern[0] == str[0])
                    match = 1;
                } else {
                  if (tolower(static_cast<int>(pattern[0])) ==
                      tolower(static_cast<int>(str[0])))
                    match = 1;
                }
              }
              pattern++;
              pattern_len--;
            }
            if (not_flag)
                match = !match;
            if (!match)
                return 0; /* no match */
            str++;
            string_len--;
            break;
        }
      case '\\':
        if (pattern_len >= 2) {
          pattern++;
          pattern_len--;
        }
        /* fall through */
      default:
        if (!nocase) {
          if (pattern[0] != str[0])
            return 0; /* no match */
        } else {
          if (tolower(static_cast<int>(pattern[0])) !=
              tolower(static_cast<int>(str[0])))
            return 0; /* no match */
        }
        str++;
        string_len--;
        break;
      }
      pattern++;
      pattern_len--;
      if (string_len == 0) {
        while (*pattern == '*') {
          pattern++;
          pattern_len--;
        }
        break;
      }
    }
    if (pattern_len == 0 && string_len == 0)
      return 1;
    return 0;
}

int StrToLongDouble(const char* s, size_t slen, long double* ldval) {
    char *pEnd;
    std::string t(s, slen);
    if (t.find(" ") != std::string::npos) {
      return -1;
    }
    long double d = strtold(s, &pEnd);
    if (pEnd != s + slen)
        return -1;

    if (ldval != NULL) *ldval = d;
    return 0;
}

int LongDoubleToStr(long double ldval, std::string* value) {
    char buf[256];
    int len;
    if (std::isnan(ldval)) {
      return -1;
    } else if (std::isinf(ldval)) {
      /* Libc in odd systems (Hi Solaris!) will format infinite in a
      * different way, so better to handle it in an explicit way. */
      if (ldval > 0) {
        memcpy(buf, "inf", 3);
        len = 3;
      } else {
        memcpy(buf, "-inf", 4);
        len = 4;
      }
      return -1;
    } else {
      /* We use 17 digits precision since with 128 bit floats that precision
       * after rounding is able to represent most small decimal numbers in a
       * way that is "non surprising" for the user (that is, most small
       * decimal numbers will be represented in a way that when converted
       * back into a string are exactly the same as what the user typed.) */
      len = snprintf(buf, sizeof(buf), "%.17Lf", ldval);
      /* Now remove trailing zeroes after the '.' */
      if (strchr(buf, '.') != NULL) {
          char *p = buf+len-1;
          while (*p == '0') {
              p--;
              len--;
          }
          if (*p == '.') len--;
      }
      value->assign(buf, len);
      return 0;
    }
}

int do_mkdir(const char *path, mode_t mode) {
  struct stat st;
  int status = 0;

  if (stat(path, &st) != 0) {
    /* Directory does not exist. EEXIST for race
     * condition */
    if (mkdir(path, mode) != 0 && errno != EEXIST)
      status = -1;
  } else if (!S_ISDIR(st.st_mode)) {
    errno = ENOTDIR;
    status = -1;
  }

  return (status);
}

/**
** mkpath - ensure all directories in path exist
** Algorithm takes the pessimistic view and works top-down to ensure
** each directory in path exists, rather than optimistically creating
** the last element and working backwards.
*/
int mkpath(const char *path, mode_t mode) {
  char           *pp;
  char           *sp;
  int             status;
  char           *copypath = strdup(path);

  status = 0;
  pp = copypath;
  while (status == 0 && (sp = strchr(pp, '/')) != 0) {
    if (sp != pp) {
      /* Neither root nor double slash in path */
      *sp = '\0';
      status = do_mkdir(copypath, mode);
      *sp = '/';
    }
    pp = sp + 1;
  }
  if (status == 0)
    status = do_mkdir(path, mode);
  free(copypath);
  return (status);
}

int delete_dir(const char* dirname) {
    char chBuf[256];
    DIR * dir = NULL;
    struct dirent *ptr;
    int ret = 0;
    dir = opendir(dirname);
    if (NULL == dir) {
        return -1;
    }
    while ((ptr = readdir(dir)) != NULL) {
        ret = strcmp(ptr->d_name, ".");
        if (0 == ret) {
            continue;
        }
        ret = strcmp(ptr->d_name, "..");
        if (0 == ret) {
            continue;
        }
        snprintf(chBuf, sizeof(256), "%s/%s", dirname, ptr->d_name);
        ret = is_dir(chBuf);
        if (0 == ret) {
            // is dir
            ret = delete_dir(chBuf);
            if (0 != ret) {
                return -1;
            }
        } else if (1 == ret) {
            // is file
            ret = remove(chBuf);
            if (0 != ret) {
                return -1;
            }
        }
    }
    (void)closedir(dir);
    ret = remove(dirname);
    if (0 != ret) {
        return -1;
    }
    return 0;
}

int is_dir(const char* filename) {
    struct stat buf;
    int ret = stat(filename, &buf);
    if (0 == ret) {
        if (buf.st_mode & S_IFDIR) {
            // folder
            return 0;
        } else {
            // file
            return 1;
        }
    }
    return -1;
}

int CalculateMetaStartAndEndKey(const std::string& key,
                                std::string* meta_start_key,
                                std::string* meta_end_key) {
  size_t needed = key.size() + 1;
  char* dst = new char[needed];
  const char* start = dst;
  memcpy(dst, key.data(), key.size());
  dst += key.size();
  meta_start_key->assign(start, key.size());
  *dst = static_cast<uint8_t>(0xff);
  meta_end_key->assign(start, key.size() + 1);
  delete[] start;
  return 0;
}

int CalculateDataStartAndEndKey(const std::string& key,
                                std::string* data_start_key,
                                std::string* data_end_key) {
  size_t needed = sizeof(int32_t) + key.size() + 1;
  char* dst = new char[needed];
  const char* start = dst;
  EncodeFixed32(dst, key.size());
  dst += sizeof(int32_t);
  memcpy(dst, key.data(), key.size());
  dst += key.size();
  data_start_key->assign(start, sizeof(int32_t) + key.size());
  *dst = static_cast<uint8_t>(0xff);
  data_end_key->assign(start, sizeof(int32_t) + key.size() + 1);
  delete[] start;
  return 0;
}

bool isTailWildcard(const std::string& pattern) {
  if (pattern.size() < 2) {
    return false;
  } else {
    if (pattern.back() != '*') {
      return false;
    } else {
      for (uint32_t idx = 0; idx < pattern.size() - 1; ++idx) {
        if (pattern[idx] == '*' || pattern[idx] == '?'
          || pattern[idx] == '[' || pattern[idx] == ']') {
          return false;
        }
      }
    }
  }
  return true;
}

}  //  namespace storage
