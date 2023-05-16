// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_STATISTIC_H_
#define PIKA_STATISTIC_H_

#include <atomic>
#include <shared_mutex>
#include <string>
#include <unordered_map>

class QpsStatistic {
 public:
  QpsStatistic();
  QpsStatistic(const QpsStatistic& other);
  ~QpsStatistic() = default;

  void IncreaseQueryNum(bool is_write);

  void ResetLastSecQuerynum();

  std::atomic<uint64_t> querynum;
  std::atomic<uint64_t> write_querynum;

  std::atomic<uint64_t> last_querynum;
  std::atomic<uint64_t> last_write_querynum;

  std::atomic<uint64_t> last_sec_querynum;
  std::atomic<uint64_t> last_sec_write_querynum;

  std::atomic<uint64_t> last_time_us;
};

struct ServerStatistic {
  ServerStatistic();
  ~ServerStatistic();

  std::atomic<uint64_t> accumulative_connections;
  std::unordered_map<std::string, std::atomic<uint64_t>> exec_count_table;
  QpsStatistic qps;
};

struct Statistic {
  Statistic();

  QpsStatistic TableStat(const std::string& table_name);
  std::unordered_map<std::string, QpsStatistic> AllTableStat();

  void UpdateTableQps(const std::string& table_name, const std::string& command, bool is_write);
  void ResetTableLastSecQuerynum();

  // statistic shows accumulated data of all tables
  ServerStatistic server_stat;

  // statistic shows accumulated data of every single table
  std::shared_mutex table_stat_rw;
  std::unordered_map<std::string, QpsStatistic> table_stat;
};

#endif  // PIKA_STATISTIC_H_
