//  Copyright (c) 2018-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef INCLUDE_UTILS_H_
#define INCLUDE_UTILS_H_

#include "iostream"
#include "vector"

#include "slash/include/env.h"

#define COMMA_STR ","
#define COLON_STR ":"
#define NEG_STR "-"
#define NEG_CHAR '-'
#define SPACE_STR " "
#define WRITE2FILE "write2file"

bool Exists(const std::string& base, const std::string& pattern);
bool CheckFilesStr(const std::string& files_str);
bool GetFileList(const std::string& files_str, std::vector<uint32_t>* files);
bool CheckBinlogSequential(const std::vector<uint32_t>& files);
bool CheckBinlogExists(const std::string& binlog_path, const std::vector<uint32_t>& files);

#endif  //  INCLUDE_UTILS_H_
