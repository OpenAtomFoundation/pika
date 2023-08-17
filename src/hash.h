/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include "helper.h"
#include "pstring.h"

#include <unordered_map>

namespace pikiwidb {

using PHash = std::unordered_map<PString, PString, my_hash, std::equal_to<PString> >;

size_t HScanKey(const PHash& hash, size_t cursor, size_t count, std::vector<PString>& res);

}  // namespace pikiwidb

