//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <sstream>

#include "rocksdb/env.h"

#include "src/redis.h"
#include "src/lists_filter.h"
#include "src/base_filter.h"
#include "src/zsets_filter.h"

namespace storage {

constexpr const char* ErrTypeMessage = "WRONGTYPE";

const rocksdb::Comparator* ListsDataKeyComparator() {
  static ListsDataKeyComparatorImpl ldkc;
  return &ldkc;
}

rocksdb::Comparator* ZSetsScoreKeyComparator() {
  static ZSetsScoreKeyComparatorImpl zsets_score_key_compare;
  return &zsets_score_key_compare;
}

Redis::Redis(Storage* const s, int32_t index)
    : storage_(s), index_(index),
      lock_mgr_(std::make_shared<LockMgr>(1000, 0, std::make_shared<MutexFactoryImpl>())),
      small_compaction_threshold_(5000),
      small_compaction_duration_threshold_(10000) {
  statistics_store_ = std::make_unique<LRUCache<std::string, KeyStatistics>>();
  scan_cursors_store_ = std::make_unique<LRUCache<std::string, std::string>>();
  spop_counts_store_ = std::make_unique<LRUCache<std::string, size_t>>();
  default_compact_range_options_.exclusive_manual_compaction = false;
  default_compact_range_options_.change_level = true;
  spop_counts_store_->SetCapacity(1000);
  scan_cursors_store_->SetCapacity(5000);
  //env_ = rocksdb::Env::Instance();
  handles_.clear();
}

Redis::~Redis() {
  rocksdb::CancelAllBackgroundWork(db_, true);
  std::vector<rocksdb::ColumnFamilyHandle*> tmp_handles = handles_;
  handles_.clear();
  for (auto handle : tmp_handles) {
    delete handle;
  }
  // delete env_;
  delete db_;

  if (default_compact_range_options_.canceled) {
    delete default_compact_range_options_.canceled;
  }
}

Status Redis::Open(const StorageOptions& storage_options, const std::string& db_path) {
  statistics_store_->SetCapacity(storage_options.statistics_max_size);
  small_compaction_threshold_ = storage_options.small_compaction_threshold;

  rocksdb::BlockBasedTableOptions table_ops(storage_options.table_options);
  table_ops.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, true));

  rocksdb::DBOptions db_ops(storage_options.options);
  db_ops.create_missing_column_families = true;
  // db_ops.env = env_;

  /*
   * Because zset, set, the hash, list, stream type meta
   * information exists kMetaCF, so we delete the various
   * types of MetaCF before
   */
  // meta & string column-family options
  rocksdb::ColumnFamilyOptions meta_cf_ops(storage_options.options);
  meta_cf_ops.compaction_filter_factory = std::make_shared<MetaFilterFactory>();
  rocksdb::BlockBasedTableOptions meta_table_ops(table_ops);

  rocksdb::BlockBasedTableOptions string_table_ops(table_ops);
  if (!storage_options.share_block_cache && storage_options.block_cache_size > 0) {
    meta_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
  }
  meta_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(meta_table_ops));

  // hash column-family options
  rocksdb::ColumnFamilyOptions hash_data_cf_ops(storage_options.options);
  hash_data_cf_ops.compaction_filter_factory = std::make_shared<HashesDataFilterFactory>(&db_, &handles_, RedisType::kHash);
  rocksdb::BlockBasedTableOptions hash_data_cf_table_ops(table_ops);
  if (!storage_options.share_block_cache && storage_options.block_cache_size > 0) {
    hash_data_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
  }
  hash_data_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(hash_data_cf_table_ops));

  // list column-family options
  rocksdb::ColumnFamilyOptions list_data_cf_ops(storage_options.options);
  list_data_cf_ops.compaction_filter_factory = std::make_shared<ListsDataFilterFactory>(&db_, &handles_, RedisType::kList);
  list_data_cf_ops.comparator = ListsDataKeyComparator();

  rocksdb::BlockBasedTableOptions list_data_cf_table_ops(table_ops);
  if (!storage_options.share_block_cache && storage_options.block_cache_size > 0) {
    list_data_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
  }
  list_data_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(list_data_cf_table_ops));

  // set column-family options
  rocksdb::ColumnFamilyOptions set_data_cf_ops(storage_options.options);
  set_data_cf_ops.compaction_filter_factory = std::make_shared<SetsMemberFilterFactory>(&db_, &handles_, RedisType::kSet);
  rocksdb::BlockBasedTableOptions set_data_cf_table_ops(table_ops);
  if (!storage_options.share_block_cache && storage_options.block_cache_size > 0) {
    set_data_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
  }
  set_data_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(set_data_cf_table_ops));

  // zset column-family options
  rocksdb::ColumnFamilyOptions zset_data_cf_ops(storage_options.options);
  rocksdb::ColumnFamilyOptions zset_score_cf_ops(storage_options.options);
  zset_data_cf_ops.compaction_filter_factory = std::make_shared<ZSetsDataFilterFactory>(&db_, &handles_, RedisType::kZset);
  zset_score_cf_ops.compaction_filter_factory = std::make_shared<ZSetsScoreFilterFactory>(&db_, &handles_, RedisType::kZset);
  zset_score_cf_ops.comparator = ZSetsScoreKeyComparator();

  rocksdb::BlockBasedTableOptions zset_meta_cf_table_ops(table_ops);
  rocksdb::BlockBasedTableOptions zset_data_cf_table_ops(table_ops);
  rocksdb::BlockBasedTableOptions zset_score_cf_table_ops(table_ops);
  if (!storage_options.share_block_cache && storage_options.block_cache_size > 0) {
    zset_data_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
  }
  zset_data_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(zset_data_cf_table_ops));
  zset_score_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(zset_score_cf_table_ops));

  // stream column-family options
  rocksdb::ColumnFamilyOptions stream_data_cf_ops(storage_options.options);
  stream_data_cf_ops.compaction_filter_factory = std::make_shared<BaseDataFilterFactory>(&db_, &handles_, RedisType::kStream);
  rocksdb::BlockBasedTableOptions stream_data_cf_table_ops(table_ops);
  if (!storage_options.share_block_cache && storage_options.block_cache_size > 0) {
    stream_data_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
  }
  stream_data_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(stream_data_cf_table_ops));

  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  // meta & string cf
  column_families.emplace_back(rocksdb::kDefaultColumnFamilyName, meta_cf_ops);
  // hash CF
  column_families.emplace_back("hash_data_cf", hash_data_cf_ops);
  // set CF
  column_families.emplace_back("set_data_cf", set_data_cf_ops);
  // list CF
  column_families.emplace_back("list_data_cf", list_data_cf_ops);
  // zset CF
  column_families.emplace_back("zset_data_cf", zset_data_cf_ops);
  column_families.emplace_back("zset_score_cf", zset_score_cf_ops);
  // stream CF
  column_families.emplace_back("stream_data_cf", stream_data_cf_ops);
  return rocksdb::DB::Open(db_ops, db_path, column_families, &handles_, &db_);
}

Status Redis::GetScanStartPoint(const DataType& type, const Slice& key, const Slice& pattern, int64_t cursor, std::string* start_point) {
  std::string index_key;
  index_key.append(1, DataTypeTag[type]);
  index_key.append("_");
  index_key.append(key.ToString());
  index_key.append("_");
  index_key.append(pattern.ToString());
  index_key.append("_");
  index_key.append(std::to_string(cursor));
  return scan_cursors_store_->Lookup(index_key, start_point);
}

Status Redis::StoreScanNextPoint(const DataType& type, const Slice& key, const Slice& pattern, int64_t cursor,
                                 const std::string& next_point) {
  std::string index_key;
  index_key.append(1, DataTypeTag[type]);
  index_key.append("_");
  index_key.append(key.ToString());
  index_key.append("_");
  index_key.append(pattern.ToString());
  index_key.append("_");
  index_key.append(std::to_string(cursor));
  return scan_cursors_store_->Insert(index_key, next_point);
}

Status Redis::SetMaxCacheStatisticKeys(size_t max_cache_statistic_keys) {
  statistics_store_->SetCapacity(max_cache_statistic_keys);
  return Status::OK();
}

/*
 * compactrange no longer supports compact for a single data type
 */
Status Redis::CompactRange(const rocksdb::Slice* begin, const rocksdb::Slice* end) {
  db_->CompactRange(default_compact_range_options_, begin, end);
  db_->CompactRange(default_compact_range_options_, handles_[kHashesDataCF], begin, end);
  db_->CompactRange(default_compact_range_options_, handles_[kSetsDataCF], begin, end);
  db_->CompactRange(default_compact_range_options_, handles_[kListsDataCF], begin, end);
  db_->CompactRange(default_compact_range_options_, handles_[kZsetsDataCF], begin, end);
  db_->CompactRange(default_compact_range_options_, handles_[kZsetsScoreCF], begin, end);
  db_->CompactRange(default_compact_range_options_, handles_[kStreamsDataCF], begin, end);
  return Status::OK();
}

Status Redis::SetSmallCompactionThreshold(uint64_t small_compaction_threshold) {
  small_compaction_threshold_ = small_compaction_threshold;
  return Status::OK();
}

Status Redis::SetSmallCompactionDurationThreshold(uint64_t small_compaction_duration_threshold) {
  small_compaction_duration_threshold_ = small_compaction_duration_threshold;
  return Status::OK();
}

Status Redis::UpdateSpecificKeyStatistics(const DataType& dtype, const std::string& key, uint64_t count) {
  if ((statistics_store_->Capacity() != 0U) && (count != 0U) && (small_compaction_threshold_ != 0U)) {
    KeyStatistics data;
    std::string lkp_key;
    lkp_key.append(1, DataTypeTag[dtype]);
    lkp_key.append(key);
    statistics_store_->Lookup(lkp_key, &data);
    data.AddModifyCount(count);
    statistics_store_->Insert(lkp_key, data);
    AddCompactKeyTaskIfNeeded(dtype, key, data.ModifyCount(), data.AvgDuration());
  }
  return Status::OK();
}

Status Redis::UpdateSpecificKeyDuration(const DataType& dtype, const std::string& key, uint64_t duration) {
  if ((statistics_store_->Capacity() != 0U) && (duration != 0U) && (small_compaction_duration_threshold_ != 0U)) {
    KeyStatistics data;
    std::string lkp_key;
    lkp_key.append(1, DataTypeTag[dtype]);
    lkp_key.append(key);
    statistics_store_->Lookup(lkp_key, &data);
    data.AddDuration(duration);
    statistics_store_->Insert(lkp_key, data);
    AddCompactKeyTaskIfNeeded(dtype, key, data.ModifyCount(), data.AvgDuration());
  }
  return Status::OK();
}

Status Redis::AddCompactKeyTaskIfNeeded(const DataType& dtype, const std::string& key, uint64_t total, uint64_t duration) {
  if (total < small_compaction_threshold_ || duration < small_compaction_duration_threshold_) {
    return Status::OK();
  } else {
    std::string lkp_key(1, DataTypeTag[dtype]);
    lkp_key.append(key);
    storage_->AddBGTask({dtype, kCompactRange, {key}});
    statistics_store_->Remove(lkp_key);
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

void Redis::GetRocksDBInfo(std::string& info, const char* prefix) {
    std::ostringstream string_stream;
    string_stream << "#" << prefix << "RocksDB" << "\r\n";

    auto write_stream_key_value=[&](const Slice& property, const char *metric) {
        uint64_t value;
        db_->GetAggregatedIntProperty(property, &value);
        string_stream << prefix << metric << ':' << value << "\r\n";
    };

    auto mapToString=[&](const std::map<std::string, std::string>& map_data, const char *prefix) {
      for (const auto& kv : map_data) {
        std::string str_data;
        str_data += kv.first + ": " + kv.second + "\r\n";
        string_stream << prefix << str_data;
      }
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

    // pending compaction bytes
    write_stream_key_value(rocksdb::DB::Properties::kEstimatePendingCompactionBytes, "estimate_pending_compaction_bytes");

    // block cache
    write_stream_key_value(rocksdb::DB::Properties::kBlockCacheCapacity, "block_cache_capacity");
    write_stream_key_value(rocksdb::DB::Properties::kBlockCacheUsage, "block_cache_usage");
    write_stream_key_value(rocksdb::DB::Properties::kBlockCachePinnedUsage, "block_cache_pinned_usage");

    // blob files
    write_stream_key_value(rocksdb::DB::Properties::kNumBlobFiles, "num_blob_files");
    write_stream_key_value(rocksdb::DB::Properties::kBlobStats, "blob_stats");
    write_stream_key_value(rocksdb::DB::Properties::kTotalBlobFileSize, "total_blob_file_size");
    write_stream_key_value(rocksdb::DB::Properties::kLiveBlobFileSize, "live_blob_file_size");

    // column family stats
    std::map<std::string, std::string> mapvalues;
    db_->rocksdb::DB::GetMapProperty(rocksdb::DB::Properties::kCFStats,&mapvalues);
    mapToString(mapvalues,prefix);
    info.append(string_stream.str());
}

void Redis::SetWriteWalOptions(const bool is_wal_disable) {
  default_write_options_.disableWAL = is_wal_disable;
}

void Redis::SetCompactRangeOptions(const bool is_canceled) {
  if (!default_compact_range_options_.canceled) {
    default_compact_range_options_.canceled = new std::atomic<bool>(is_canceled);
  } else {
    default_compact_range_options_.canceled->store(is_canceled);
  } 
}

Status Redis::GetProperty(const std::string& property, uint64_t* out) {
  std::string value;
  for (const auto& handle : handles_) {
    db_->GetProperty(handle, property, &value);
    *out += std::strtoull(value.c_str(), nullptr, 10);
  }
  return Status::OK();
}

Status Redis::ScanKeyNum(std::vector<KeyInfo>* key_infos) {
  key_infos->resize(5);
  rocksdb::Status s;
  s = ScanStringsKeyNum(&((*key_infos)[0]));
  if (!s.ok()) {
    return s;
  }
  s = ScanHashesKeyNum(&((*key_infos)[1]));
  if (!s.ok()) {
    return s;
  }
  s = ScanListsKeyNum(&((*key_infos)[2]));
  if (!s.ok()) {
    return s;
  }
  s = ScanZsetsKeyNum(&((*key_infos)[3]));
  if (!s.ok()) {
    return s;
  }
  s = ScanSetsKeyNum(&((*key_infos)[4]));
  if (!s.ok()) {
    return s;
  }
  s = ScanSetsKeyNum(&((*key_infos)[5]));
  if (!s.ok()) {
    return s;
  }

  return Status::OK();
}

void Redis::ScanDatabase() {
  ScanStrings();
  ScanHashes();
  ScanLists();
  ScanZsets();
  ScanSets();
}

}  // namespace storage
