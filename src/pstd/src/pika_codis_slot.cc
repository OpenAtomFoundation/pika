//  Copyright (c) 2023-present The storage Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <zlib.h>

#include "pstd/include/pika_codis_slot.h"

// get slot tag
static const char *GetSlotsTag(const std::string &str, int *plen) {
  const char *s = str.data();
  int i, j, n = static_cast<int32_t>(str.length());
  for (i = 0; i < n && s[i] != '{'; i++) {
  }
  if (i == n) {
    return nullptr;
  }
  i++;
  for (j = i; j < n && s[j] != '}'; j++) {
  }
  if (j == n) {
    return nullptr;
  }
  if (plen != nullptr) {
    *plen = j - i;
  }
  return s + i;
}

// get slot number of the key
uint32_t GetSlotID(int slot_num, const std::string &str) { return GetSlotsID(slot_num, str, nullptr, nullptr); }

// get the slot number by key
uint32_t GetSlotsID(int slot_num, const std::string &str, uint32_t *pcrc, int *phastag) {
  const char *s = str.data();
  int taglen; int hastag = 0;
  const char *tag = GetSlotsTag(str, &taglen);
  if (tag == nullptr) {
    tag = s, taglen = static_cast<int32_t>(str.length());
  } else {
    hastag = 1;
  }
  auto crc = crc32(0L, (const Bytef*)tag, taglen);
  if (pcrc != nullptr) {
    *pcrc = uint32_t(crc);
  }
  if (phastag != nullptr) {
    *phastag = hastag;
  }
  return static_cast<uint32_t>(crc) % slot_num;
}
