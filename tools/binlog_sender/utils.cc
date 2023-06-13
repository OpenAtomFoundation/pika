//  Copyright (c) 2018-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "utils.h"

bool Exists(const std::string& base, const std::string& pattern) {
  std::string::size_type n;
  n = base.find(pattern);
  return n != std::string::npos;
}

bool CheckFilesStr(const std::string& files_str) {
  int32_t neg_count = 0;
  for (const auto& c : files_str) {
    if (isdigit(c) != 0) {
      continue;
    } else if (c == NEG_CHAR) {
      neg_count++;
    } else {
      return false;
    }
  }
  return neg_count <= 1;
}

bool GetFileList(const std::string& files_str, std::vector<uint32_t>* files) {
  std::string::size_type pos;
  if (Exists(files_str, NEG_STR)) {
    pos = files_str.find('-');
    uint32_t start_file = atoi(files_str.substr(0, pos).data());
    uint32_t end_file = atoi(files_str.substr(pos + 1).data());
    for (uint32_t file_num = start_file; file_num <= end_file; ++file_num) {
      files->push_back(file_num);
    }
  } else {
    files->push_back(atoi(files_str.data()));
  }
  return !files->empty();
}

bool CheckBinlogExists(const std::string& binlog_path, const std::vector<uint32_t>& files) {
  std::string filename = binlog_path + WRITE2FILE;
  for (unsigned int file : files) {
    std::string binlog_file = filename + std::to_string(file);
    if (!pstd::FileExists(binlog_file)) {
      return false;
    }
  }
  return true;
}
