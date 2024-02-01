//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_REDIS_H_
#define SRC_REDIS_H_

#include <memory>
#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

#include "pstd/include/env.h"

#include "src/lock_mgr.h"
#include "src/lru_cache.h"
#include "src/mutex_impl.h"
#include "storage/storage.h"

namespace storage {
using Status = rocksdb::Status;
using Slice = rocksdb::Slice;

class Redis {
 public:
  Redis(Storage* storage, const DataType& type);
  virtual ~Redis();

  rocksdb::DB* GetDB() { return db_; }

  struct KeyStatistics {
    size_t window_size;
    std::deque<uint64_t> durations;

    uint64_t modify_count;

    KeyStatistics() : KeyStatistics(10) {}

    KeyStatistics(size_t size) : window_size(size + 2), modify_count(0) {}

    void AddDuration(uint64_t duration) {
      durations.push_back(duration);
      while (durations.size() > window_size) {
        durations.pop_front();
      }
    }
    uint64_t AvgDuration() {
      if (durations.size () < window_size) {
        return 0;
      }
      uint64_t min = durations[0];
      uint64_t max = durations[0];
      uint64_t sum = 0;
      for (auto duration : durations) {
        if (duration < min) {
          min = duration;
        }
        if (duration > max) {
          max = duration;
        }
        sum += duration;
      }
      return (sum - max - min) / (durations.size() - 2);
    }
    void AddModifyCount(uint64_t count) {
      modify_count += count;
    }
    uint64_t ModifyCount() {
      return modify_count;
    }
  };

  struct KeyStatisticsDurationGuard {
    Redis* ctx;
    std::string key;
    uint64_t start_us;
    KeyStatisticsDurationGuard(Redis* that, const std::string& key): ctx(that), key(key), start_us(pstd::NowMicros()) {
    }
    ~KeyStatisticsDurationGuard() {
      uint64_t end_us = pstd::NowMicros();
      uint64_t duration = end_us > start_us ? end_us - start_us : 0;
      ctx->UpdateSpecificKeyDuration(key, duration);
    }
  };

  Status SetOptions(const OptionType& option_type, const std::unordered_map<std::string, std::string>& options);
  void SetWriteWalOptions(const bool is_wal_disable);
  void SetCompactRangeOptions(const bool is_canceled);

  // Common Commands
  virtual Status Open(const StorageOptions& storage_options, const std::string& db_path) = 0;
  virtual Status CompactRange(const rocksdb::Slice* begin, const rocksdb::Slice* end,
                              const ColumnFamilyType& type = kMetaAndData) = 0;
  virtual Status GetProperty(const std::string& property, uint64_t* out) = 0;
  virtual Status ScanKeyNum(KeyInfo* key_info) = 0;
  virtual Status ScanKeys(const std::string& pattern, std::vector<std::string>* keys) = 0;
  virtual Status PKPatternMatchDel(const std::string& pattern, int32_t* ret) = 0;

  // Keys Commands
  virtual Status Expire(const Slice& key, int32_t ttl) = 0;
  virtual Status Del(const Slice& key) = 0;
  virtual bool Scan(const std::string& start_key, const std::string& pattern, std::vector<std::string>* keys,
                    int64_t* count, std::string* next_key) = 0;
  virtual bool PKExpireScan(const std::string& start_key, int32_t min_timestamp, int32_t max_timestamp,
                            std::vector<std::string>* keys, int64_t* leftover_visits, std::string* next_key) = 0;
  virtual Status Expireat(const Slice& key, int32_t timestamp) = 0;
  virtual Status Persist(const Slice& key) = 0;
  virtual Status TTL(const Slice& key, int64_t* timestamp) = 0;

  Status SetMaxCacheStatisticKeys(size_t max_cache_statistic_keys);
  Status SetSmallCompactionThreshold(uint64_t small_compaction_threshold);
  Status SetSmallCompactionDurationThreshold(uint64_t small_compaction_duration_threshold);
  std::vector<rocksdb::ColumnFamilyHandle*> GetHandles(){ return handles_;};
  void GetRocksDBInfo(std::string &info, const char *prefix);

 protected:
  Storage* const storage_;
  DataType type_;
  std::shared_ptr<LockMgr> lock_mgr_;
  rocksdb::DB* db_ = nullptr;

  std::vector<rocksdb::ColumnFamilyHandle*> handles_;
  rocksdb::WriteOptions default_write_options_;
  rocksdb::ReadOptions default_read_options_;
  rocksdb::CompactRangeOptions default_compact_range_options_;

  // For Scan
  std::unique_ptr<LRUCache<std::string, std::string>> scan_cursors_store_;

  Status GetScanStartPoint(const Slice& key, const Slice& pattern, int64_t cursor, std::string* start_point);
  Status StoreScanNextPoint(const Slice& key, const Slice& pattern, int64_t cursor, const std::string& next_point);

  // For Statistics
  std::atomic_uint64_t small_compaction_threshold_;
  std::atomic_uint64_t small_compaction_duration_threshold_;
  std::unique_ptr<LRUCache<std::string, KeyStatistics>> statistics_store_;

  Status UpdateSpecificKeyStatistics(const std::string& key, uint64_t count);
  Status UpdateSpecificKeyDuration(const std::string& key, uint64_t duration);
  Status AddCompactKeyTaskIfNeeded(const std::string& key, uint64_t count, uint64_t duration);
};

}  //  namespace storage
#endif  //  SRC_REDIS_H_
