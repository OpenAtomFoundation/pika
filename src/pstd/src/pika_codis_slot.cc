//  Copyright (c) 2023-present The storage Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "pstd/include/pika_codis_slot.h"

uint32_t crc32tab[256];
void CRC32TableInit(uint32_t poly) {
  int i, j;
  for (i = 0; i < 256; i++) {
    uint32_t crc = i;
    for (j = 0; j < 8; j++) {
      if (crc & 1) {
        crc = (crc >> 1) ^ poly;
      } else {
        crc = (crc >> 1);
      }
    }
    crc32tab[i] = crc;
  }
}

void InitCRC32Table() {
  CRC32TableInit(0xedb88320);
}

uint32_t CRC32Update(uint32_t crc, const char *buf, int len) {
  int i;
  crc = ~crc;
  for (i = 0; i < len; i++) {
    crc = crc32tab[static_cast<uint8_t>(static_cast<char>(crc) ^ buf[i])] ^ (crc >> 8);
  }
  return ~crc;
}

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

uint32_t CRC32CheckSum(const char *buf, int len) { return CRC32Update(0, buf, len); }

// get slot number of the key
int GetSlotID(const std::string &str) { return GetSlotsID(str, nullptr, nullptr); }

// get the slot number by key
int GetSlotsID(const std::string &str, uint32_t *pcrc, int *phastag) {
  const char *s = str.data();
  int taglen; int hastag = 0;
  const char *tag = GetSlotsTag(str, &taglen);
  if (tag == nullptr) {
    tag = s, taglen = static_cast<int32_t>(str.length());
  } else {
    hastag = 1;
  }
  uint32_t crc = CRC32CheckSum(tag, taglen);
  if (pcrc != nullptr) {
    *pcrc = crc;
  }
  if (phastag != nullptr) {
    *phastag = hastag;
  }
  return crc % g_pika_conf->default_slot_num();
}
