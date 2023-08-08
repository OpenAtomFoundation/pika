// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <glog/logging.h>

#include "pstd/include/env.h"
#include "pstd/include/scope_record_lock.h"

#include "include/pika_zset_auto_del_thread.h"
#include "include/pika_server.h"
#include "include/pika_conf.h"
#include "include/pika_commonfunc.h"


extern PikaServer *g_pika_server;
extern std::unique_ptr<PikaConf> g_pika_conf;
std::mutex mutex;

PikaZsetAutoDelThread::PikaZsetAutoDelThread()
    : should_exit_(false)
    , task_cond_(&mutexs_)
    , current_task_type_(ZSET_NO_TASK)
    , current_cursor_(0)
    , stop_manual_task_(false)
    , zset_db_keys_num_(0)
    , auto_del_keys_num_(0)
    , start_check_all_zset_time_(0)
    , last_finish_check_all_zset_time_(0)
    , last_spend_time_(0)
    , last_all_keys_num_(0)
    , last_del_keys_num_(0)
    , last_compact_zset_db_(false)
{
    set_thread_name("PikaZsetAutoDelThread");
}

PikaZsetAutoDelThread::~PikaZsetAutoDelThread() {
  {
    std::unique_lock<std::mutex> lock(mutex);
    should_exit_ = true;
    task_cond_.Signal();
  }

  StopThread();
}

void PikaZsetAutoDelThread::RequestCronTask() {
  std::unique_lock<std::mutex> lock(mutex);
 // if (task_queue_.empty()) {
    ZsetTaskItem item;
    item.task_type = ZSET_CRON_TASK;
    item.speed_factor = g_pika_conf->zset_auto_del_cron_speed_factor();
    //task_queue_.push_back(item);
    //task_cond_.Signal();
    DoZsetAutoDelTask(item);
  //}
}

void PikaZsetAutoDelThread::RequestManualTask(int64_t cursor, double speed_factor) {
  LOG(INFO) << "start manual zset auto delete task";
  std::unique_lock<std::mutex> lock(mutex);
  if (!task_queue_.empty()) {
    task_queue_.clear();
  }

  ZsetTaskItem item;
  item.task_type = ZSET_MANUAL_TASK;
  item.cursor = cursor;
  item.speed_factor = speed_factor;
  task_queue_.push_back(item);
  DoZsetAutoDelTask(item);
}

void PikaZsetAutoDelThread::StopManualTask() {
  LOG(INFO) << "stop manual zset auto delete task";
  stop_manual_task_ = true;
}

void PikaZsetAutoDelThread::GetZsetInfo(ZsetInfo &info) {
  info.last_finish_time = last_finish_check_all_zset_time_;
  info.last_spend_time = last_spend_time_;
  info.last_all_keys_num = last_all_keys_num_;
  info.last_del_keys_num = last_del_keys_num_;
  info.last_compact_zset_db = last_compact_zset_db_;
  info.current_task_type = current_task_type_;
  info.current_task_start_time = start_check_all_zset_time_;
  info.current_task_spend_time = static_cast<int64_t>(time(nullptr)) - start_check_all_zset_time_;
  info.current_cursor = current_cursor_;
}

void PikaZsetAutoDelThread::CompactZsetDB() {
  struct statfs disk_info;
  int ret = statfs(g_pika_conf->db_path().c_str(), &disk_info);
  if (ret == -1) {
    LOG(WARNING) << "statfs error: " << strerror(errno);
    return;
  }

  uint64_t total_size = disk_info.f_bsize * disk_info.f_blocks;
  uint64_t free_size = disk_info.f_bsize * disk_info.f_bfree;
//  std::shared_ptr<Slot> slot;
//  slot = g_pika_server->GetSlotByDBName("db0");
//  rocksdb::Status s = slot->db()->Compact(storage::kZSets);
//  if (s.ok()) {
//    LOG(INFO) << "zset auto delete compactRange, freesize: " << free_size/1048576 << "MB, disksize: " << total_size/1048576 << "MB";
//  } else {
//    LOG(INFO) << "zset auto delete compactRange Failed, freesize: " << free_size/1048576 << "MB, disksize: " << total_size/1048576
//              << "MB, error: " << s.ToString();
//  }
}

void PikaZsetAutoDelThread::TrimAllZsetKeysFinished() {
  double delete_ratio = (0 == zset_db_keys_num_) ? 0 : static_cast<double>(auto_del_keys_num_) / zset_db_keys_num_;
  LOG(INFO) << "scan zset db finished, check keys:" << zset_db_keys_num_
            << ", delete keys:" << auto_del_keys_num_
            << ", delete ratio:" << delete_ratio
            << ", compact ratio:" << g_pika_conf->zset_compact_del_ratio();

//  if (g_pika_conf->zset_compact_del_ratio() < delete_ratio
//    && g_pika_conf->zset_compact_del_num() < auto_del_keys_num_) {
//    CompactZsetDB();
//    last_compact_zset_db_ = true;
//  } else {
//    last_compact_zset_db_ = false;
//  }
  last_finish_check_all_zset_time_ = static_cast<int64_t>(time(nullptr));;
  last_spend_time_ = last_finish_check_all_zset_time_ - start_check_all_zset_time_;
  last_all_keys_num_ = zset_db_keys_num_;
  last_del_keys_num_ = auto_del_keys_num_;

  zset_db_keys_num_ = 0;
  auto_del_keys_num_ = 0;
}

void PikaZsetAutoDelThread::WriteZsetAutoDelBinlog(const std::string &key, int start, int end) {
  std::string raw_args;
  RedisAppendLen(raw_args, 4, "*");
  RedisAppendLen(raw_args, 15, "$");
  RedisAppendContent(raw_args, "ZREMRANGEBYRANK");
  RedisAppendLen(raw_args, key.size(), "$");
  RedisAppendContent(raw_args, key);
  RedisAppendLen(raw_args, std::to_string(start).size(), "$");
  RedisAppendContent(raw_args, std::to_string(start));
  RedisAppendLen(raw_args, std::to_string(end).size(), "$");
  RedisAppendContent(raw_args, std::to_string(end));
  // PikaCommonFunc::BinlogPut(key, raw_args);
}

bool PikaZsetAutoDelThread::BatchTrimZsetKeys(double speed_factor) {
  bool db_scan_finished = false;
  std::vector<std::string> keys;
  int64_t count = g_pika_conf->zset_auto_del_scan_round_num();
  std::shared_ptr<Slot> slot;
  slot = g_pika_server->GetSlotByDBName("db0");
  int64_t next_cursor = slot->db()->ScanZset(current_cursor_, "*", count, &keys);
  if (next_cursor == 0) {
    db_scan_finished = true;
  }
  zset_db_keys_num_ += keys.size();

  int zset_auto_del_threshold = g_pika_conf->zset_auto_del_threshold();
  int zset_auto_del_num = g_pika_conf->zset_auto_del_num();
  for (auto& key : keys) {
    uint64_t start_us = 0;
    if (speed_factor > 0) {
      start_us = pstd::NowMicros();
    }

    int32_t zset_size = 0;
    rocksdb::Status s = slot->db()->ZCard(key, &zset_size);
    if (s.ok() && zset_size > zset_auto_del_threshold) {
      zset_auto_del_num = zset_auto_del_num > zset_auto_del_threshold ? zset_auto_del_threshold : zset_auto_del_num;

      int need_delete_nums = 0;
      if (zset_size - zset_auto_del_threshold > zset_auto_del_num) {
        need_delete_nums = zset_size - zset_auto_del_threshold + zset_auto_del_num;
      } else {
        need_delete_nums = zset_auto_del_num;
      }

      {
        //pstd::lock::ScopeRecordLock l(lock_mgr_, key);
        int32_t count = 0;
        int start = (g_pika_conf->zset_auto_del_direction() == 0) ? 0 : -need_delete_nums;
        int end = (g_pika_conf->zset_auto_del_direction() == 0) ? need_delete_nums - 1 : -1;
        s = slot->db()->ZRemrangebyrank(key, start, end, &count);
        if (s.ok()) {
          //WriteZsetAutoDelBinlog(key, start, end);
          ++auto_del_keys_num_;
        }
      }
      // sleep for a moment to avoid a lot of disk IO
      if (speed_factor > 0) {
        uint64_t duration = pstd::NowMicros() - start_us;
        usleep(static_cast<int64_t>(duration * speed_factor));
      }
    }
  }
  current_cursor_ = next_cursor;

  if (db_scan_finished) {
    TrimAllZsetKeysFinished();
    return true;
  }
  return false;
}

void PikaZsetAutoDelThread::DoZsetCronTask(double speed_factor) {
  if (current_cursor_ == 0) {
    start_check_all_zset_time_ = static_cast<int64_t>(time(nullptr));;
  }
  BatchTrimZsetKeys(speed_factor);
}

void PikaZsetAutoDelThread::DoZsetManualTask(int64_t cursor, double speed_factor) {
  start_check_all_zset_time_ = static_cast<int64_t>(time(nullptr));

  // reset key num when do zset manual task
  zset_db_keys_num_ = 0;
  auto_del_keys_num_ = 0;

  //stop_manual_task_ = false;
  current_cursor_ = cursor;
  std::cout << "current_cursor_: " << current_cursor_ << std::endl;
  //while (!stop_manual_task_) {
    // return true means trim all zset keys finish
    if (BatchTrimZsetKeys(speed_factor)) {
      return;
    }
  //}
}

void PikaZsetAutoDelThread::DoZsetAutoDelTask(ZsetTaskItem &task_item) {
  current_task_type_ = task_item.task_type;
    std::cout << "cursor: " << task_item.cursor << std::endl;
    std::cout << "factor: " << task_item.speed_factor << std::endl;
    std::cout << "type: " << task_item.task_type << std::endl;
  switch (current_task_type_) {
    case ZSET_CRON_TASK:
      //DoZsetCronTask(task_item.speed_factor);
      DoZsetManualTask(task_item.cursor, task_item.speed_factor);
      break;
    case ZSET_MANUAL_TASK:
      DoZsetManualTask(task_item.cursor, task_item.speed_factor);
      break;
    default:
      break;
  }
  current_task_type_ = ZSET_NO_TASK;
}

void* PikaZsetAutoDelThread::ThreadMain() {
  std::cout << "yy" << std::endl;
  while (!should_exit_) {
    ZsetTaskItem task_item;
    {
      std::unique_lock<std::mutex> lock(mutex);
      while (!should_exit_ && task_queue_.empty()) {
          std::cout << "px" << std::endl;
        task_cond_.Wait();
      }

      if (should_exit_) {
        return nullptr;
      }

      task_item = task_queue_.front();
      task_queue_.pop_front();
    }
    std::cout << "type: " << task_item.task_type << std::endl;
    DoZsetAutoDelTask(task_item);
  }

  return nullptr;
}
