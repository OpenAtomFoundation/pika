/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <deque>
#include <vector>

#include "pstring.h"

class Logger;

namespace pikiwidb {

struct SlowLogItem {
  unsigned used;
  std::vector<PString> cmds;

  SlowLogItem() : used(0) {}

  SlowLogItem(SlowLogItem&& item) : used(item.used), cmds(std::move(item.cmds)) {}
};

class PSlowLog {
 public:
  static PSlowLog& Instance();

  PSlowLog(const PSlowLog&) = delete;
  void operator=(const PSlowLog&) = delete;

  void Begin();
  void EndAndStat(const std::vector<PString>& cmds);

  void SetThreshold(unsigned int);
  void SetLogLimit(std::size_t maxCount);

  void ClearLogs() { logs_.clear(); }
  std::size_t GetLogsCount() const { return logs_.size(); }
  const std::deque<SlowLogItem>& GetLogs() const { return logs_; }

 private:
  PSlowLog();
  ~PSlowLog();

  unsigned int threshold_;
  long long beginUs_;
  Logger* logger_;

  std::size_t logMaxCount_;
  std::deque<SlowLogItem> logs_;
};

}  // namespace pikiwidb

