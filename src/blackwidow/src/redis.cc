//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "src/redis.h"

namespace blackwidow {

Redis::Redis(BlackWidow* const bw, const DataType& type)
    : bw_(bw),
      type_(type),
      lock_mgr_(new LockMgr(1000, 0, std::make_shared<MutexFactoryImpl>())),
      db_(nullptr),
      small_compaction_threshold_(5000) {
  statistics_store_ = new LRUCache<std::string, size_t>();
  scan_cursors_store_ = new LRUCache<std::string, std::string>();
  scan_cursors_store_->SetCapacity(5000);
  default_compact_range_options_.exclusive_manual_compaction = false;
  default_compact_range_options_.change_level = true;
  handles_.clear();
}

Redis::~Redis() {
  std::vector<rocksdb::ColumnFamilyHandle*> tmp_handles = handles_;
  handles_.clear();
  for (auto handle : tmp_handles) {
    delete handle;
  }
  delete db_;
  delete lock_mgr_;
  delete statistics_store_;
  delete scan_cursors_store_;
}

Status Redis::GetScanStartPoint(const Slice& key,
                                const Slice& pattern,
                                int64_t cursor,
                                std::string* start_point) {
  std::string index_key = key.ToString() + "_"
      + pattern.ToString() + "_" + std::to_string(cursor);
  return scan_cursors_store_->Lookup(index_key, start_point);
}

Status Redis::StoreScanNextPoint(const Slice& key,
                                 const Slice& pattern,
                                 int64_t cursor,
                                 const std::string& next_point) {
  std::string index_key = key.ToString() + "_"
      + pattern.ToString() +  "_" + std::to_string(cursor);
  return scan_cursors_store_->Insert(index_key, next_point);
}

Status Redis::SetMaxCacheStatisticKeys(size_t max_cache_statistic_keys) {
  statistics_store_->SetCapacity(max_cache_statistic_keys);
  return Status::OK();
}

Status Redis::SetSmallCompactionThreshold(size_t small_compaction_threshold) {
  small_compaction_threshold_ = small_compaction_threshold;
  return Status::OK();
}

Status Redis::UpdateSpecificKeyStatistics(const std::string& key,
                                          size_t count) {
  if (statistics_store_->Capacity() && count) {
    size_t total = 0;
    statistics_store_->Lookup(key, &total);
    statistics_store_->Insert(key, total + count);
    AddCompactKeyTaskIfNeeded(key, total + count);
  }
  return Status::OK();
}

Status Redis::AddCompactKeyTaskIfNeeded(const std::string& key,
                                        size_t total) {
  if (total < small_compaction_threshold_) {
    return Status::OK();
  } else {
    bw_->AddBGTask({type_, kCompactKey, key});
    statistics_store_->Remove(key);
  }
  return Status::OK();
}

Status Redis::SetOptions(const OptionType& option_type,
    const std::unordered_map<std::string, std::string>& options) {
  if (option_type == OptionType::kDB) {
    return db_->SetDBOptions(options);
  }
  if (handles_.size() == 0) {
    return db_->SetOptions(db_->DefaultColumnFamily(), options);
  }
  Status s;
  for (auto handle : handles_) {
    s = db_->SetOptions(handle, options);
    if(!s.ok()) break;
  }
  return s;
}

}  // namespace blackwidow
