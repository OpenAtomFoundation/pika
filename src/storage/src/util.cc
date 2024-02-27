//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <unistd.h>
#include <cctype>
#include <climits>
#include <cstdint>
#include <cstring>
#include <memory>

#include "pstd/include/pstd_string.h"
#include "pstd/include/pika_codis_slot.h"
#include "src/base_key_format.h"
#include "src/base_data_key_format.h"
#include "src/coding.h"
#include "storage/storage_define.h"
#include "storage/util.h"

namespace storage {

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
  return pstd::ll2string(dst, dstlen, svalue);
}

/* Convert a string into a long long. Returns 1 if the string could be parsed
 * into a (non-overflowing) long long, 0 otherwise. The value will be set to
 * the parsed value when appropriate. */
int StrToInt64(const char* s, size_t slen, int64_t* value) {
  return pstd::string2int(s, slen, value);
}

/* Glob-style pattern matching. */
int StringMatch(const char* pattern, uint64_t pattern_len, const char* str, uint64_t string_len, int nocase) {
  return pstd::stringmatchlen(pattern, static_cast<int32_t>(pattern_len), str, static_cast<int32_t>(string_len), nocase);
}

int StrToLongDouble(const char* s, size_t slen, long double* ldval) {
  char* pEnd;
  std::string t(s, slen);
  if (t.find(' ') != std::string::npos) {
    return -1;
  }
  long double d = strtold(s, &pEnd);
  if (pEnd != s + slen) {
    return -1;
  }

  if (ldval) {
    *ldval = d;
  }
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
      strcpy(buf, "inf");
      len = 3;
    } else {
      strcpy(buf, "-inf");
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
    if (strchr(buf, '.')) {
      char* p = buf + len - 1;
      while (*p == '0') {
        p--;
        len--;
      }
      if (*p == '.') {
        len--;
      }
    }
    value->assign(buf, len);
    return 0;
  }
}

int do_mkdir(const char* path, mode_t mode) {
  struct stat st;
  int status = 0;

  if (stat(path, &st) != 0) {
    /* Directory does not exist. EEXIST for race
     * condition */
    if (mkdir(path, mode) != 0 && errno != EEXIST) {
      status = -1;
    }
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
int mkpath(const char* path, mode_t mode) {
  char* pp;
  char* sp;
  int status;
  char* copypath = strdup(path);

  status = 0;
  pp = copypath;
  while (status == 0 && (sp = strchr(pp, '/')) != nullptr) {
    if (sp != pp) {
      /* Neither root nor double slash in path */
      *sp = '\0';
      status = do_mkdir(copypath, mode);
      *sp = '/';
    }
    pp = sp + 1;
  }
  if (status == 0) {
    status = do_mkdir(path, mode);
  }
  free(copypath);
  return (status);
}

int delete_dir(const char* dirname) {
  char chBuf[256];
  DIR* dir = nullptr;
  struct dirent* ptr;
  int ret = 0;
  dir = opendir(dirname);
  if (nullptr == dir) {
    return -1;
  }
  while ((ptr = readdir(dir)) != nullptr) {
    ret = strcmp(ptr->d_name, ".");
    if (0 == ret) {
      continue;
    }
    ret = strcmp(ptr->d_name, "..");
    if (0 == ret) {
      continue;
    }
    snprintf(chBuf, sizeof(chBuf), "%s/%s", dirname, ptr->d_name);
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
    if ((buf.st_mode & S_IFDIR) != 0) {
      // folder
      return 0;
    } else {
      // file
      return 1;
    }
  }
  return -1;
}

int CalculateStartAndEndKey(const std::string& key, std::string* start_key, std::string* end_key) {
  if (key.empty()) {
    return 0;
  }
  size_t usize = kPrefixReserveLength + key.size() + kEncodedKeyDelimSize;
  size_t nzero = std::count(key.begin(), key.end(), kNeedTransformCharacter);
  usize += nzero;
  auto dst = std::make_unique<char[]>(usize);
  char* ptr = dst.get();
  memset(ptr, kNeedTransformCharacter, kPrefixReserveLength);
  ptr += kPrefixReserveLength;
  ptr = storage::EncodeUserKey(Slice(key), ptr, nzero);
  if (start_key) {
    *start_key = std::string(dst.get(), ptr);
  }
  if (end_key) {
    *end_key = std::string(dst.get(), ptr);
    // Encoded key's last two character is "\u0000\u0000",
    // so directly upgrade end_key's back character to '\u0001'.
    end_key->back() = '\u0001';
  }
  return 0;
}

// requires:
// 1. pattern's length >= 2
// 2. tail character is '*'
// 3. other position's charactor cannot be *, ?, [,]
bool isTailWildcard(const std::string& pattern) {
  if (pattern.size() < 2) {
    return false;
  } else {
    if (pattern.back() != '*') {
      return false;
    } else {
      for (uint32_t idx = 0; idx < pattern.size() - 1; ++idx) {
        if (pattern[idx] == '*' || pattern[idx] == '?' || pattern[idx] == '[' || pattern[idx] == ']') {
          return false;
        }
      }
    }
  }
  return true;
}

void GetFilepath(const char* path, const char* filename, char* filepath) {
  strcpy(filepath, path);  // NOLINT
  if (filepath[strlen(path) - 1] != '/') {
    strcat(filepath, "/");  // NOLINT
  }
  strcat(filepath, filename);  // NOLINT
}

bool DeleteFiles(const char* path) {
  DIR* dir;
  struct dirent* dirinfo;
  struct stat statbuf;
  char filepath[256] = {0};
  lstat(path, &statbuf);

  if (S_ISREG(statbuf.st_mode))  // 判断是否是常规文件
  {
    remove(path);
  } else if (S_ISDIR(statbuf.st_mode))  // 判断是否是目录
  {
    if (!(dir = opendir(path))) {
      return true;
    }
    while ((dirinfo = readdir(dir)) != nullptr) {
      GetFilepath(path, dirinfo->d_name, filepath);
      if (strcmp(dirinfo->d_name, ".") == 0 || strcmp(dirinfo->d_name, "..") == 0) {  // 判断是否是特殊目录
        continue;
      }
      DeleteFiles(filepath);
      rmdir(filepath);
    }
    closedir(dir);
  }
  return false;
}

}  //  namespace storage
