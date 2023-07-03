// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_statistic.h"

#include "include/pika_command.h"
#include "pstd/include/env.h"

/* QpsStatistic */

QpsStatistic::QpsStatistic()
    : querynum(0),
      write_querynum(0),
      last_querynum(0),
      last_write_querynum(0),
      last_sec_querynum(0),
      last_sec_write_querynum(0),
      last_time_us(0) {}

QpsStatistic::QpsStatistic(const QpsStatistic& other) {
  querynum = other.querynum.load();
  write_querynum = other.write_querynum.load();
  last_querynum = other.last_querynum.load();
  last_write_querynum = other.last_write_querynum.load();
  last_sec_querynum = other.last_sec_querynum.load();
  last_sec_write_querynum = other.last_sec_write_querynum.load();
  last_time_us = other.last_time_us.load();
}

void QpsStatistic::IncreaseQueryNum(bool is_write) {
  querynum++;
  if (is_write) {
    write_querynum++;
  }
}

void QpsStatistic::ResetLastSecQuerynum() {
  uint64_t last_query = last_querynum.load();
  uint64_t last_write_query = last_write_querynum.load();
  uint64_t cur_query = querynum.load();
  uint64_t cur_write_query = write_querynum.load();
  uint64_t last_time = last_time_us.load();
  if (cur_write_query < last_write_query) {
    cur_write_query = last_write_query;
  }
  if (cur_query < last_query) {
    cur_query = last_query;
  }
  uint64_t delta_query = cur_query - last_query;
  uint64_t delta_write_query = cur_write_query - last_write_query;
  uint64_t cur_time_us = pstd::NowMicros();
  if (cur_time_us <= last_time) {
    cur_time_us = last_time + 1;
  }
  uint64_t delta_time_us = cur_time_us - last_time;
  last_sec_querynum.store(delta_query * 1000000 / (delta_time_us));
  last_sec_write_querynum.store(delta_write_query * 1000000 / (delta_time_us));
  last_querynum.store(cur_query);
  last_write_querynum.store(cur_write_query);

  last_time_us.store(cur_time_us);
}

/* ServerStatistic */

ServerStatistic::ServerStatistic() = default;

ServerStatistic::~ServerStatistic() = default;

/* Statistic */

Statistic::Statistic() {
  pthread_rwlockattr_t db_stat_rw_attr;
  pthread_rwlockattr_init(&db_stat_rw_attr);
}

QpsStatistic Statistic::DBStat(const std::string& db_name) {
  std::shared_lock l(db_stat_rw);
  return db_stat[db_name];
}

std::unordered_map<std::string, QpsStatistic> Statistic::AllDBStat() {
  std::shared_lock l(db_stat_rw);
  return db_stat;
}

void Statistic::UpdateDBQps(const std::string& db_name,
                            const std::string& command, bool is_write) {
  bool db_exist = true;
  std::unordered_map<std::string, QpsStatistic>::iterator iter;
  {
    std::shared_lock l(db_stat_rw);
    auto search = db_stat.find(db_name);
    if (search == db_stat.end()) {
      db_exist = false;
    } else {
      iter = search;
    }
  }
  if (db_exist) {
    iter->second.IncreaseQueryNum(is_write);
  } else {
    {
      std::lock_guard l(db_stat_rw);
      db_stat[db_name].IncreaseQueryNum(is_write);
    }
  }
}

void Statistic::ResetDBLastSecQuerynum() {
  std::shared_lock l(db_stat_rw);
  for (auto& stat : db_stat) {
    stat.second.ResetLastSecQuerynum();
  }
}
