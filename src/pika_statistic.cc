// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_statistic.h"

#include "slash/include/env.h"

#include "include/pika_command.h"

/* QpsStatistic */

QpsStatistic::QpsStatistic()
    : querynum(0),
      write_querynum(0),
      last_querynum(0),
      last_write_querynum(0),
      last_sec_querynum(0),
      last_sec_write_querynum(0),
      last_time_us(0) {
}

QpsStatistic::QpsStatistic(const QpsStatistic& other) {
  querynum = other.querynum.load();
  write_querynum = other.write_querynum.load();
  last_querynum = other.last_querynum.load();
  last_write_querynum = other.last_write_querynum.load();
  last_sec_querynum = other.last_sec_querynum.load();
  last_sec_write_querynum = other.last_sec_write_querynum.load();
  last_time_us = other.last_time_us.load();
}

QpsStatistic::~QpsStatistic() {
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
  uint64_t cur_time_us = slash::NowMicros();
  if (cur_time_us <= last_time) {
    cur_time_us = last_time + 1;
  }
  uint64_t delta_time_us = cur_time_us - last_time;
  last_sec_querynum.store(delta_query
       * 1000000 / (delta_time_us));
  last_sec_write_querynum.store(delta_write_query
       * 1000000 / (delta_time_us));
  last_querynum.store(cur_query);
  last_write_querynum.store(cur_write_query);

  last_time_us.store(cur_time_us);
}

/* ServerStatistic */

ServerStatistic::ServerStatistic()
    : accumulative_connections(0) {
  CmdTable* cmds = new CmdTable();
  cmds->reserve(300);
  InitCmdTable(cmds);
  CmdTable::const_iterator it = cmds->begin();
  for (; it != cmds->end(); ++it) {
    std::string tmp = it->first;
    exec_count_table[slash::StringToUpper(tmp)].store(0);
  }
  DestoryCmdTable(cmds);
  delete cmds;
}

ServerStatistic::~ServerStatistic() {
}

/* Statistic */

Statistic::Statistic() {
  pthread_rwlockattr_t table_stat_rw_attr;
  pthread_rwlockattr_init(&table_stat_rw_attr);
  pthread_rwlockattr_setkind_np(&table_stat_rw_attr,
      PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
  pthread_rwlock_init(&table_stat_rw, &table_stat_rw_attr);
}

QpsStatistic Statistic::TableStat(const std::string& table_name) {
  slash::RWLock(&table_stat_rw, false);
  return table_stat[table_name];
}

std::unordered_map<std::string, QpsStatistic> Statistic::AllTableStat() {
  slash::RWLock(&table_stat_rw, false);
  return table_stat;
}

void Statistic::UpdateTableQps(
    const std::string& table_name, const std::string& command, bool is_write) {
  bool table_exist = true;
  std::unordered_map<std::string, QpsStatistic>::iterator iter;
  {
    slash::RWLock(&table_stat_rw, false);
    auto search = table_stat.find(table_name);
    if (search == table_stat.end()) {
      table_exist = false;
    } else {
      iter = search;
    }
  }
  if (table_exist) {
    iter->second.IncreaseQueryNum(is_write);
  } else {
    {
      slash::RWLock(&table_stat_rw, true);
      table_stat[table_name].IncreaseQueryNum(is_write);
    }
  }
}

void Statistic::ResetTableLastSecQuerynum() {
  slash::RWLock(&table_stat_rw, false);
  for (auto& stat : table_stat) {
    stat.second.ResetLastSecQuerynum();
  }
}






