//  Copyright (c) 2017-present The storage Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_UTIL_H_
#define SRC_UTIL_H_

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <cmath>
#include <cstring>
#include <string>

namespace storage {

int32_t Int64ToStr(char* dst, size_t dstlen, int64_t svalue);
int32_t StrToInt64(const char* s, size_t slen, int64_t* value);
int32_t StringMatch(const char* pattern, uint64_t pattern_len, const char* string, uint64_t string_len, int32_t nocase);
int32_t StrToLongDouble(const char* s, size_t slen, long double* ldval);
int32_t LongDoubleToStr(long double ldval, std::string* value);
int32_t do_mkdir(const char* path, mode_t mode);
int32_t mkpath(const char* path, mode_t mode);
int32_t delete_dir(const char* dirname);
int32_t is_dir(const char* filename);
int32_t CalculateMetaStartAndEndKey(const std::string& key, std::string* meta_start_key, std::string* meta_end_key);
int32_t CalculateDataStartAndEndKey(const std::string& key, std::string* data_start_key, std::string* data_end_key);
bool isTailWildcard(const std::string& pattern);
void GetFilepath(const char* path, const char* filename, char* filepath);
bool DeleteFiles(const char* path);
}  // namespace storage

#endif  //  SRC_UTIL_H_
