//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "src/redis.h"
#include <sstream>

namespace storage {

Redis::Redis(Storage* const s, const DataType& type)
    : storage_(s),
      type_(type),
      lock_mgr_(std::make_shared<LockMgr>(1000, 0, std::make_shared<MutexFactoryImpl>())),
      small_compaction_threshold_(5000) {
  statistics_store_ = std::make_unique<LRUCache<std::string, size_t>>();
  scan_cursors_store_ = std::make_unique<LRUCache<std::string, std::string>>();
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
}

Status Redis::GetScanStartPoint(const Slice& key, const Slice& pattern, int64_t cursor, std::string* start_point) {
  std::string index_key = key.ToString() + "_" + pattern.ToString() + "_" + std::to_string(cursor);
  return scan_cursors_store_->Lookup(index_key, start_point);
}

Status Redis::StoreScanNextPoint(const Slice& key, const Slice& pattern, int64_t cursor,
                                 const std::string& next_point) {
  std::string index_key = key.ToString() + "_" + pattern.ToString() + "_" + std::to_string(cursor);
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

Status Redis::UpdateSpecificKeyStatistics(const std::string& key, size_t count) {
  if ((statistics_store_->Capacity() != 0U) && (count != 0U)) {
    size_t total = 0;
    statistics_store_->Lookup(key, &total);
    statistics_store_->Insert(key, total + count);
    AddCompactKeyTaskIfNeeded(key, total + count);
  }
  return Status::OK();
}

Status Redis::AddCompactKeyTaskIfNeeded(const std::string& key, size_t total) {
  if (total < small_compaction_threshold_) {
    return Status::OK();
  } else {
    storage_->AddBGTask({type_, kCompactKey, key});
    statistics_store_->Remove(key);
  }
  return Status::OK();
}

Status Redis::SetOptions(const OptionType& option_type, const std::unordered_map<std::string, std::string>& options) {
  if (option_type == OptionType::kDB) {
    return db_->SetDBOptions(options);
  }
  if (handles_.empty()) {
    return db_->SetOptions(db_->DefaultColumnFamily(), options);
  }
  Status s;
  for (auto handle : handles_) {
    s = db_->SetOptions(handle, options);
    if (!s.ok()) {
      break;
    }
  }
  return s;
}

void Redis::GetRocksDBInfo(std::string &info, const char *prefix) {
    std::ostringstream string_stream;
    string_stream << "#" << prefix << " RocksDB" << "\r\n";

    auto write_stream_key_value=[&](const Slice& property, const char *metric) {
        uint64_t value;
        db_->GetAggregatedIntProperty(property, &value);
        string_stream << prefix << metric << ':' << value << "\r\n";
    };

    // memtables num
    write_stream_key_value(rocksdb::DB::Properties::kNumImmutableMemTable, "num_immutable_mem_table");
    write_stream_key_value(rocksdb::DB::Properties::kNumImmutableMemTableFlushed, "num_immutable_mem_table_flushed");
    write_stream_key_value(rocksdb::DB::Properties::kMemTableFlushPending, "mem_table_flush_pending");
    write_stream_key_value(rocksdb::DB::Properties::kNumRunningFlushes, "num_running_flushes");

    // compaction
    write_stream_key_value(rocksdb::DB::Properties::kCompactionPending, "compaction_pending");
    write_stream_key_value(rocksdb::DB::Properties::kNumRunningCompactions, "num_running_compactions");

    // background errors
    write_stream_key_value(rocksdb::DB::Properties::kBackgroundErrors, "background_errors");

    // memtables size
    write_stream_key_value(rocksdb::DB::Properties::kCurSizeActiveMemTable, "cur_size_active_mem_table");
    write_stream_key_value(rocksdb::DB::Properties::kCurSizeAllMemTables, "cur_size_all_mem_tables");
    write_stream_key_value(rocksdb::DB::Properties::kSizeAllMemTables, "size_all_mem_tables");

    // keys
    write_stream_key_value(rocksdb::DB::Properties::kEstimateNumKeys, "estimate_num_keys");

    // table readers mem
    write_stream_key_value(rocksdb::DB::Properties::kEstimateTableReadersMem, "estimate_table_readers_mem");

    // snapshot
    write_stream_key_value(rocksdb::DB::Properties::kNumSnapshots, "num_snapshots");

    // version
    write_stream_key_value(rocksdb::DB::Properties::kNumLiveVersions, "num_live_versions");
    write_stream_key_value(rocksdb::DB::Properties::kCurrentSuperVersionNumber, "current_super_version_number");

    // live data size
    write_stream_key_value(rocksdb::DB::Properties::kEstimateLiveDataSize, "estimate_live_data_size");

    // sst files
    write_stream_key_value(rocksdb::DB::Properties::kTotalSstFilesSize, "total_sst_files_size");
    write_stream_key_value(rocksdb::DB::Properties::kLiveSstFilesSize, "live_sst_files_size");

    // block cache
    write_stream_key_value(rocksdb::DB::Properties::kBlockCacheCapacity, "block_cache_capacity");
    write_stream_key_value(rocksdb::DB::Properties::kBlockCacheUsage, "block_cache_usage");
    write_stream_key_value(rocksdb::DB::Properties::kBlockCachePinnedUsage, "block_cache_pinned_usage");

    // blob files
    write_stream_key_value(rocksdb::DB::Properties::kNumBlobFiles, "num_blob_files");
    write_stream_key_value(rocksdb::DB::Properties::kBlobStats, "blob_stats");
    write_stream_key_value(rocksdb::DB::Properties::kTotalBlobFileSize, "total_blob_file_size");
    write_stream_key_value(rocksdb::DB::Properties::kLiveBlobFileSize, "live_blob_file_size");

    info.append(string_stream.str());
}

}  // namespace storage
