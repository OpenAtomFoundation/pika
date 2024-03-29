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
  ServerStatistic() = default;
  ~ServerStatistic() = default;

  std::atomic<uint64_t> accumulative_connections;
  std::unordered_map<std::string, std::atomic<uint64_t>> exec_count_db;
  QpsStatistic qps;
};

struct Statistic {
  Statistic();

  QpsStatistic DBStat(const std::string& db_name);
  std::unordered_map<std::string, QpsStatistic> AllDBStat();

  void UpdateDBQps(const std::string& db_name, const std::string& command, bool is_write);
  void ResetDBLastSecQuerynum();

  // statistic shows accumulated data of all tables
  ServerStatistic server_stat;

  // statistic shows accumulated data of every single table
  std::shared_mutex db_stat_rw;
  std::unordered_map<std::string, QpsStatistic> db_stat;
};

struct DiskStatistic {
  std::atomic<uint64_t> db_size_ = 0;
  std::atomic<uint64_t> log_size_ = 0;
};

#endif  // PIKA_STATISTIC_H_
