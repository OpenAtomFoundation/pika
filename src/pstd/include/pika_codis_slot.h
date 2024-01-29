// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_CODIS_SLOT_H_
#define PIKA_CODIS_SLOT_H_

#include <stdint.h>
#include <string>
#include <memory>

// get the slot number by key
uint32_t GetSlotsID(int slot_num, const std::string& str, uint32_t* pcrc, int* phastag);

// get slot number of the key
uint32_t GetSlotID(int slot_num, const std::string& str);

#endif

