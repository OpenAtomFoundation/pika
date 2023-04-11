//  Copyright (c) 2018-present The pika-tools Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "utils.h"
#include "iostream"

void EncodeKeyValue(const std::string& key, const std::string& value, std::string* dst) {
  dst->clear();
  int32_t key_size = key.size();
  int32_t value_size = value.size();
  dst->append(reinterpret_cast<const char*>(&key_size), sizeof(int32_t));
  dst->append(key);
  dst->append(reinterpret_cast<const char*>(&value_size), sizeof(int32_t));
  dst->append(value);
  return;
}

void DecodeKeyValue(const std::string& dst, std::string* key, std::string* value) {
  const char* p = dst.data();
  int32_t key_size = *(reinterpret_cast<const int32_t*>(p));
  p += sizeof(int32_t);
  key->assign(p, key_size);
  p += key_size;

  int32_t value_size = *(reinterpret_cast<const int32_t*>(p));
  p += sizeof(int32_t);
  value->assign(p, value_size);
  p += value_size;
  return;
}
