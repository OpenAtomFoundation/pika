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

#include "src/lock_mgr.h"
#include "src/lru_cache.h"
#include "src/mutex_impl.h"
#include "storage/storage.h"

namespace storage {
using Status = rocksdb::Status;
using Slice = rocksdb::Slice;

class Redis {
 public:
  Redis(Storage* const storage, const DataType& type);
  virtual ~Redis();

  rocksdb::DB* GetDB() { return db_; }

  Status SetOptions(const OptionType& option_type, const std::unordered_map<std::string, std::string>& options);

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
  Status SetSmallCompactionThreshold(size_t small_compaction_threshold);

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
  std::shared_ptr<LRUCache<std::string, std::string>> scan_cursors_store_;

  Status GetScanStartPoint(const Slice& key, const Slice& pattern, int64_t cursor, std::string* start_point);
  Status StoreScanNextPoint(const Slice& key, const Slice& pattern, int64_t cursor, const std::string& next_point);

  // For Statistics
  std::atomic<size_t> small_compaction_threshold_;
  std::shared_ptr<LRUCache<std::string, size_t>> statistics_store_;

  Status UpdateSpecificKeyStatistics(const std::string& key, size_t count);
  Status AddCompactKeyTaskIfNeeded(const std::string& key, size_t total);
};

}  //  namespace storage
#endif  //  SRC_REDIS_H_
