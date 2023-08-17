/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <stdint.h>
#include "pstring.h"

namespace pikiwidb {

struct PObject;

class PDumpInterface {
 public:
  virtual ~PDumpInterface() {}

  virtual PObject Get(const PString& key) = 0;
  virtual bool Put(const PString& key, const PObject& obj, int64_t ttl = 0) = 0;
  virtual bool Put(const PString& key) = 0;
  virtual bool Delete(const PString& key) = 0;

  // std::vector<PObject> MultiGet(const PString& key);
  // bool MultiPut(const PString& key, const PObject& obj, int64_t ttl = 0);
  // SaveAllRedisTolevelDB();
  // LoadAllLeveldbToRedis();
};

}  // namespace pikiwidb

