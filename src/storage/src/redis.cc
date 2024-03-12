//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <sstream>

#include "rocksdb/env.h"

#include "src/redis.h"
#include "src/strings_filter.h"
#include "src/lists_filter.h"
#include "src/base_filter.h"
#include "src/zsets_filter.h"
#include "pstd/include/pstd_string.h"

namespace storage {
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

  // string column-family options
  rocksdb::ColumnFamilyOptions string_cf_ops(storage_options.options);
  string_cf_ops.compaction_filter_factory = std::make_shared<StringsFilterFactory>();

  rocksdb::BlockBasedTableOptions string_table_ops(table_ops);
  if (!storage_options.share_block_cache && storage_options.block_cache_size > 0) {
    string_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
  }
  string_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(string_table_ops));


  // hash column-family options
  rocksdb::ColumnFamilyOptions hash_meta_cf_ops(storage_options.options);
  rocksdb::ColumnFamilyOptions hash_data_cf_ops(storage_options.options);
  hash_meta_cf_ops.compaction_filter_factory = std::make_shared<HashesMetaFilterFactory>();
  hash_data_cf_ops.compaction_filter_factory = std::make_shared<HashesDataFilterFactory>(&db_, &handles_, kHashesMetaCF);

  rocksdb::BlockBasedTableOptions hash_meta_cf_table_ops(table_ops);
  rocksdb::BlockBasedTableOptions hash_data_cf_table_ops(table_ops);
  if (!storage_options.share_block_cache && storage_options.block_cache_size > 0) {
    hash_meta_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
    hash_data_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
  }
  hash_meta_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(hash_meta_cf_table_ops));
  hash_data_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(hash_data_cf_table_ops));

  // list column-family options
  rocksdb::ColumnFamilyOptions list_meta_cf_ops(storage_options.options);
  rocksdb::ColumnFamilyOptions list_data_cf_ops(storage_options.options);
  list_meta_cf_ops.compaction_filter_factory = std::make_shared<ListsMetaFilterFactory>();
  list_data_cf_ops.compaction_filter_factory = std::make_shared<ListsDataFilterFactory>(&db_, &handles_, kListsMetaCF);
  list_data_cf_ops.comparator = ListsDataKeyComparator();

  rocksdb::BlockBasedTableOptions list_meta_cf_table_ops(table_ops);
  rocksdb::BlockBasedTableOptions list_data_cf_table_ops(table_ops);
  if (!storage_options.share_block_cache && storage_options.block_cache_size > 0) {
    list_meta_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
    list_data_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
  }
  list_meta_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(list_meta_cf_table_ops));
  list_data_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(list_data_cf_table_ops));

  // set column-family options
  rocksdb::ColumnFamilyOptions set_meta_cf_ops(storage_options.options);
  rocksdb::ColumnFamilyOptions set_data_cf_ops(storage_options.options);
  set_meta_cf_ops.compaction_filter_factory = std::make_shared<SetsMetaFilterFactory>();
  set_data_cf_ops.compaction_filter_factory = std::make_shared<SetsMemberFilterFactory>(&db_, &handles_, kSetsMetaCF);

  rocksdb::BlockBasedTableOptions set_meta_cf_table_ops(table_ops);
  rocksdb::BlockBasedTableOptions set_data_cf_table_ops(table_ops);
  if (!storage_options.share_block_cache && storage_options.block_cache_size > 0) {
    set_meta_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
    set_data_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
  }
  set_meta_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(set_meta_cf_table_ops));
  set_data_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(set_data_cf_table_ops));

  // zset column-family options
  rocksdb::ColumnFamilyOptions zset_meta_cf_ops(storage_options.options);
  rocksdb::ColumnFamilyOptions zset_data_cf_ops(storage_options.options);
  rocksdb::ColumnFamilyOptions zset_score_cf_ops(storage_options.options);
  zset_meta_cf_ops.compaction_filter_factory = std::make_shared<ZSetsMetaFilterFactory>();
  zset_data_cf_ops.compaction_filter_factory = std::make_shared<ZSetsDataFilterFactory>(&db_, &handles_, kZsetsMetaCF);
  zset_score_cf_ops.compaction_filter_factory = std::make_shared<ZSetsScoreFilterFactory>(&db_, &handles_, kZsetsMetaCF);
  zset_score_cf_ops.comparator = ZSetsScoreKeyComparator();

  rocksdb::BlockBasedTableOptions zset_meta_cf_table_ops(table_ops);
  rocksdb::BlockBasedTableOptions zset_data_cf_table_ops(table_ops);
  rocksdb::BlockBasedTableOptions zset_score_cf_table_ops(table_ops);
  if (!storage_options.share_block_cache && storage_options.block_cache_size > 0) {
    zset_meta_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
    zset_data_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
    zset_meta_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
  }
  zset_meta_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(zset_meta_cf_table_ops));
  zset_data_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(zset_data_cf_table_ops));
  zset_score_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(zset_score_cf_table_ops));

  // stream column-family options
  rocksdb::ColumnFamilyOptions stream_meta_cf_ops(storage_options.options);
  rocksdb::ColumnFamilyOptions stream_data_cf_ops(storage_options.options);

  rocksdb::BlockBasedTableOptions stream_meta_cf_table_ops(table_ops);
  rocksdb::BlockBasedTableOptions stream_data_cf_table_ops(table_ops);
  if (!storage_options.share_block_cache && storage_options.block_cache_size > 0) {
    stream_meta_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
    stream_data_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
  }
  stream_meta_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(stream_meta_cf_table_ops));
  stream_data_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(stream_data_cf_table_ops));

  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  column_families.emplace_back(rocksdb::kDefaultColumnFamilyName, string_cf_ops);
  // hash CF
  column_families.emplace_back("hash_meta_cf", hash_meta_cf_ops);
  column_families.emplace_back("hash_data_cf", hash_data_cf_ops);
  // set CF
  column_families.emplace_back("set_meta_cf", set_meta_cf_ops);
  column_families.emplace_back("set_data_cf", set_data_cf_ops);
  // list CF
  column_families.emplace_back("list_meta_cf", list_meta_cf_ops);
  column_families.emplace_back("list_data_cf", list_data_cf_ops);
  // zset CF
  column_families.emplace_back("zset_meta_cf", zset_meta_cf_ops);
  column_families.emplace_back("zset_data_cf", zset_data_cf_ops);
  column_families.emplace_back("zset_score_cf", zset_score_cf_ops);
  // stream CF
  column_families.emplace_back("stream_meta_cf", stream_meta_cf_ops);
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

void SelectColumnFamilyHandles(const DataType &option_type, const ColumnFamilyType &type, std::vector<int>& handleIdxVec) {
  switch (option_type) {
    case DataType::kStrings:
      handleIdxVec.push_back(kStringsCF);
      break;
    case DataType::kHashes:
      if (type == kMeta || type == kMetaAndData) {
        handleIdxVec.push_back(kHashesMetaCF);
      }
      if (type == kData || type == kMetaAndData) {
        handleIdxVec.push_back(kHashesDataCF);
      }
      break;
    case DataType::kSets:
      if (type == kMeta || type == kMetaAndData) {
        handleIdxVec.push_back(kSetsMetaCF);
      }
      if (type == kData || type == kMetaAndData) {
        handleIdxVec.push_back(kSetsDataCF);
      }
      break;
    case DataType::kLists:
      if (type == kMeta || type == kMetaAndData) {
        handleIdxVec.push_back(kListsMetaCF);
      }
      if (type == kData || type == kMetaAndData) {
        handleIdxVec.push_back(kListsDataCF);
      }
      break;
    case DataType::kZSets:
      if (type == kMeta || type == kMetaAndData) {
        handleIdxVec.push_back(kZsetsMetaCF);
      }
      if (type == kData || type == kMetaAndData) {
        handleIdxVec.push_back(kZsetsDataCF);
        handleIdxVec.push_back(kZsetsScoreCF);
      }
      break;
    case DataType::kStreams:
      if (type == kMeta || type == kMetaAndData) {
        handleIdxVec.push_back(kStreamsMetaCF);
      }
      if (type == kData || type == kMetaAndData) {
        handleIdxVec.push_back(kStreamsDataCF);
      }
      break;
    default:
      enum ColumnFamilyIndex s;
      for (s = kStringsCF; s <= kStreamsDataCF; s = (ColumnFamilyIndex)(s + 1)) {
        handleIdxVec.push_back(s);
      }
      break;
  }
}

Status Redis::CompactRange(const DataType& option_type, const rocksdb::Slice* begin, const rocksdb::Slice* end, std::vector<Status>* compact_result_vec, const ColumnFamilyType& type) {
  std::vector<int> handleIdxVec;
  SelectColumnFamilyHandles(option_type, type, handleIdxVec);
  if (handleIdxVec.size() == 0) {
    return Status::Corruption("Invalid data type");
  }

  if (compact_result_vec) {
    compact_result_vec->clear();
  }
  
  for (auto idx : handleIdxVec) {
    auto s = db_->CompactRange(default_compact_range_options_, handles_[idx], begin, end);
    if (compact_result_vec) {
      if (!s.ok()) {
        compact_result_vec->push_back(Status::Corruption(handles_[idx]->GetName()+ " CompactRange error: " + s.ToString()));
        continue;
      }
      compact_result_vec->push_back(s);
    }
  }
  return Status::OK();
}

Status Redis::FullCompact(std::vector<Status>* compact_result_vec, const ColumnFamilyType &type) {
  return CompactRange(DataType::kAll, nullptr, nullptr, compact_result_vec, type);
}

Status Redis::LongestNotCompactiontSstCompact(const DataType &option_type, std::vector<Status>* compact_result_vec, const ColumnFamilyType &type) {
  std::vector<int> handleIdxVec;
  SelectColumnFamilyHandles(option_type, type, handleIdxVec);
  if (handleIdxVec.size() == 0) {
    return Status::Corruption("Invalid data type");
  }

  if (compact_result_vec) {
    compact_result_vec->clear();
  }

  for (auto idx : handleIdxVec) {
    rocksdb::TablePropertiesCollection props;
    Status s = db_->GetPropertiesOfAllTables(handles_[idx], &props);
    if (!s.ok()) {
      if (compact_result_vec) {
        compact_result_vec->push_back(Status::Corruption(handles_[idx]->GetName() + " LongestNotCompactiontSstCompact GetPropertiesOfAllTables error: " + s.ToString()));
      }
      continue;
    }
  
    // The main goal of compaction was reclaimed the disk space and removed
    // the tombstone. It seems that compaction scheduler was unnecessary here when
    // the live files was too few, Hard code to 1 here.
    if (props.size() <= 1) {
        LOG(WARNING) << "LongestNotCompactiontSstCompact " << handles_[idx]->GetName() << " only one file";
        if (compact_result_vec) {
          compact_result_vec->push_back(Status::OK());
        }
        continue;
    }
  
    size_t max_files_to_compact = 1;
    const StorageOptions& storageOptions = storage_->GetStorageOptions();
    if (props.size() / storageOptions.compact_param_.num_sst_docompact_once_ > max_files_to_compact) {
        max_files_to_compact = props.size() / storageOptions.compact_param_.num_sst_docompact_once_;
    }
  
    int64_t now =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count();
  
    auto force_compact_min_ratio = static_cast<double>(storageOptions.compact_param_.force_compact_min_delete_ratio_) / 100.0;
    auto best_delete_min_ratio = static_cast<double>(storageOptions.compact_param_.best_delete_min_ratio_) / 100.0;
  
    std::string best_filename;
    double best_delete_ratio = 0;
    int64_t total_keys = 0, deleted_keys = 0;
    rocksdb::Slice start_key, stop_key, best_start_key, best_stop_key;
    Status compact_result;
    for (const auto& iter : props) {
        uint64_t file_creation_time = iter.second->file_creation_time;
        if (file_creation_time == 0) {
            // Fallback to the file Modification time to prevent repeatedly compacting the same file,
            // file_creation_time is 0 which means the unknown condition in rocksdb
            auto s = rocksdb::Env::Default()->GetFileModificationTime(iter.first, &file_creation_time);
            if (!s.ok()) {
                LOG(WARNING) << handles_[idx]->GetName() << " Failed to get the file creation time: " << iter.first << " in " << handles_[idx]->GetName()
                           << ", err: " << s.ToString();
                continue;
            }
        }
  
        for (const auto& property_iter : iter.second->user_collected_properties) {
            if (property_iter.first == "total_keys") {
                if (!pstd::string2int(property_iter.second.c_str(), property_iter.second.length(), &total_keys)) {
                    LOG(WARNING) << handles_[idx]->GetName() << " " << iter.first << " Parse total_keys error";
                    continue;
                }
            }
            if (property_iter.first == "deleted_keys") {
                if (!pstd::string2int(property_iter.second.c_str(), property_iter.second.length(), &deleted_keys)) {
                    LOG(WARNING) << handles_[idx]->GetName() << " " << iter.first << " Parse deleted_keys error";
                    continue;
                }
            }
            if (property_iter.first == "start_key") {
                start_key = property_iter.second;
            }
            if (property_iter.first == "stop_key") {
                stop_key = property_iter.second;
            }
        }
  
        if (start_key.empty() || stop_key.empty()) {
            continue;
        }
        double delete_ratio = static_cast<double>(deleted_keys) / static_cast<double>(total_keys);
  
        // pick the file according to force compact policy
        if (file_creation_time < static_cast<uint64_t>(now/1000 - storageOptions.compact_param_.force_compact_file_age_seconds_) &&
            delete_ratio >= force_compact_min_ratio) {
            compact_result = db_->CompactRange(default_compact_range_options_, &start_key, &stop_key);
            max_files_to_compact--;
            continue;
        }
  
        // don't compact the SST created in x `dont_compact_sst_created_in_seconds_`.
        if (file_creation_time > static_cast<uint64_t>(now - storageOptions.compact_param_.dont_compact_sst_created_in_seconds_)) {
            continue;
        }
  
        // pick the file which has highest delete ratio
        if (total_keys != 0 && delete_ratio > best_delete_ratio) {
            best_delete_ratio = delete_ratio;
            best_filename     = iter.first;
            best_start_key    = start_key;
            start_key.clear();
            best_stop_key = stop_key;
            stop_key.clear();
        }
    }

    if (best_delete_ratio > best_delete_min_ratio && !best_start_key.empty() && !best_stop_key.empty()) {
        compact_result = db_->CompactRange(default_compact_range_options_, handles_[idx], &best_start_key, &best_stop_key);
    }

    if (!compact_result.ok()) {
      if (compact_result_vec) {
        compact_result_vec->push_back(Status::Corruption(handles_[idx]->GetName() + " Failed to do compaction " + compact_result.ToString()));
      }
      continue;
    }

    if (compact_result_vec) {
      compact_result_vec->push_back(Status::OK());
    }
  }
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
