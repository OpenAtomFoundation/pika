/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <sys/time.h>
#include <fstream>
#include <sstream>

#include "log.h"
#include "slow_log.h"

namespace pikiwidb {

PSlowLog& PSlowLog::Instance() {
  static PSlowLog slog;

  return slog;
}

PSlowLog::PSlowLog() : threshold_(0), logger_(nullptr) {}

PSlowLog::~PSlowLog() {}

void PSlowLog::SetThreshold(unsigned int v) { threshold_ = v; }

void PSlowLog::SetLogLimit(std::size_t maxCount) { logMaxCount_ = maxCount; }

void PSlowLog::Begin() {
  if (!threshold_) {
    return;
  }

  timeval begin;
  gettimeofday(&begin, 0);
  beginUs_ = begin.tv_sec * 1000000 + begin.tv_usec;
}

void PSlowLog::EndAndStat(const std::vector<PString>& cmds) {
  if (!threshold_ || beginUs_ == 0) {
    return;
  }

  timeval end;
  gettimeofday(&end, 0);
  auto used = end.tv_sec * 1000000 + end.tv_usec - beginUs_;

  if (used >= threshold_) {
    INFO("+ Used:(us) {}", used);

    for (const auto& param : cmds) {
      INFO("{}", param);
    }

    if (cmds[0] == "slowlog") {
      return;
    }

    SlowLogItem item;
    item.used = static_cast<unsigned>(used);
    item.cmds = cmds;

    logs_.emplace_front(std::move(item));
    if (logs_.size() > logMaxCount_) {
      logs_.pop_back();
    }
  }
}

}  // namespace pikiwidb
