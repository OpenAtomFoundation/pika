/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "config_parser.h"
#include <vector>
#include "memory_file.h"

static const int SPACE = ' ';
static const int TAB = '\t';
static const int NEWLINE = '\n';
static const int COMMENT = '#';

static size_t SkipBlank(const char* data, size_t len, size_t off) {
  while (++off < len) {
    if (SPACE != data[off] && TAB != data[off]) {
      --off;
      break;
    }
  }

  return off;
}

bool ConfigParser::Load(const char* FileName) {
  InputMemoryFile file;
  if (!file.Open(FileName)) {
    return false;  // no such file
  }

  data_.clear();

  size_t maxLen = size_t(-1);
  const char* data = file.Read(maxLen);

  bool bReadKey = true;
  std::string key, value;
  key.reserve(64);
  value.reserve(64);

  size_t off = 0;
  while (off < maxLen) {
    switch (data[off]) {
      case COMMENT:
        while (++off < maxLen) {
          if (NEWLINE == data[off]) {
            --off;
            break;
          }
        }
        break;

      case NEWLINE:
        bReadKey = true;

        if (!key.empty()) {
          data_[key].push_back(value);

          key.clear();
          value.clear();
        }

        off = SkipBlank(data, maxLen, off);
        break;

      case SPACE:
      case TAB:
        // 支持value中有空格
        if (bReadKey) {
          bReadKey = false;
          off = SkipBlank(data, maxLen, off);  // 跳过所有分界空格
        } else {
          value += data[off];
        }
        break;

      case '\r':
        break;

      default:
        if (bReadKey) {
          key += data[off];
        } else {
          value += data[off];
        }

        break;
    }

    ++off;
  }

  file.Close();
  return true;
}

const std::vector<std::string>& ConfigParser::GetDataVector(const char* key) const {
  auto it = data_.find(key);
  if (it == data_.end()) {
    static const std::vector<std::string> kEmpty;
    return kEmpty;
  }

  return it->second;
}

#ifdef CONFIG_DEBUG
int main() {
  ConfigParser csv;
  csv.Load("config");
  csv.Print();

  std::cout << "=====================" << std::endl;
}
#endif
