/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <map>
#include <sstream>
#include <string>
#include <vector>

#ifdef CONFIG_DEBUG
#include <iostream>
#endif

class ConfigParser {
 public:
  bool Load(const char* FileName);

  template <typename T>
  T GetData(const char* key, const T& default_ = T()) const;

  const std::vector<std::string>& GetDataVector(const char* key) const;

#ifdef CONFIG_DEBUG
  void Print() {
    std::cout << "//////////////////" << std::endl;
    std::map<std::string, std::string>::const_iterator it = data_.begin();
    while (it != data_.end()) {
      std::cout << it->first << ":" << it->second << "\n";
      ++it;
    }
  }
#endif

 private:
  typedef std::map<std::string, std::vector<std::string> > Data;

  Data data_;

  template <typename T>
  T toType(const std::string& data) const;
};

template <typename T>
inline T ConfigParser::toType(const std::string& data) const {
  T t;
  std::istringstream os(data);
  os >> t;
  return t;
}

template <>
inline const char* ConfigParser::toType<const char*>(const std::string& data) const {
  return data.c_str();
}

template <>
inline std::string ConfigParser::toType<std::string>(const std::string& data) const {
  return data;
}

template <typename T>
inline T ConfigParser::GetData(const char* key, const T& default_) const {
  auto it = data_.find(key);
  if (it == data_.end()) {
    return default_;
  }

  return toType<T>(it->second[0]);  // only return first value
}
