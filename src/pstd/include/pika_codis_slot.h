// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_CODIS_SLOT_H_
#define PIKA_CODIS_SLOT_H_

#include <stdint.h>
#include <memory>

#include "pstd/include/pika_conf.h"

extern std::unique_ptr<PikaConf> g_pika_conf;

extern uint32_t crc32tab[256];
void CRC32TableInit(uint32_t poly);

extern void InitCRC32Table();

extern uint32_t CRC32Update(uint32_t crc, const char* buf, int len);

extern uint32_t CRC32CheckSum(const char* buf, int len);

// get the slot number by key
int GetSlotsID(const std::string& str, uint32_t* pcrc, int* phastag);

// get slot number of the key
int GetSlotID(const std::string& str);

#endif

