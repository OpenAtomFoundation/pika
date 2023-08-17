/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <list>
#include "pstring.h"

namespace pikiwidb {

enum class ListPosition {
  head,
  tail,
};

using PList = std::list<PString>;
}  // namespace pikiwidb

