// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "pink/include/period_thread.h"

#include <unistd.h>

namespace pink {

PeriodThread::PeriodThread(struct timeval period) :
  period_(period) {
}

void *PeriodThread::ThreadMain() {
  PeriodMain();
  select(0, NULL, NULL, NULL, &period_);
  return NULL;
}

}  // namespace pink
