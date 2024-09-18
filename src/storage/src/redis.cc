//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <sstream>

#include "pstd_coding.h"
#include "rocksdb/env.h"

#include "src/redis.h"
#include "src/lists_filter.h"
#include "src/base_filter.h"
#include "src/zsets_filter.h"
#include "pstd/include/pstd_string.h"
#include "pstd/include/pstd_defer.h"

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

class MyTablePropertiesCollector : public rocksdb::TablePropertiesCollector {
 public:
  MyTablePropertiesCollector() = default;

  rocksdb::Status AddUserKey(const rocksdb::Slice& key, const rocksdb::Slice& value, rocksdb::EntryType type,
                             rocksdb::SequenceNumber seq, uint64_t file_size) override {
    total_keys++;
    switch (type) {
      case rocksdb::EntryType::kEntryPut: {
        if (start_key.compare(key) > 0 || start_key.empty()) {
          start_key = key;
        }
        if (stop_key.compare(key) < 0) {
          stop_key = key;
        }
        break;
      }
      case rocksdb::EntryType::kEntryDelete: {
        deleted_keys++;
        break;
      }
      default:
        break;
    }

    return rocksdb::Status::OK();
  }

  rocksdb::Status Finish(rocksdb::UserCollectedProperties* properties) override {
    std::string encoded;
    pstd::PutFixed64(&encoded, total_keys);
    properties->emplace("total_keys", encoded);
    pstd::PutFixed64(&encoded, deleted_keys);
    properties->emplace("deleted_keys", encoded);
    properties->emplace("start_key", start_key.ToString());
    properties->emplace("stop_key", stop_key.ToString());
    return rocksdb::Status::OK();
  }

  rocksdb::UserCollectedProperties GetReadableProperties() const override {
    rocksdb::UserCollectedProperties properties;
    std::string encoded;
    pstd::PutFixed64(&encoded, total_keys);
    properties.emplace("total_keys", encoded);
    pstd::PutFixed64(&encoded, deleted_keys);
    properties.emplace("deleted_keys", encoded);
    properties.emplace("start_key", start_key.ToString());
    properties.emplace("stop_key", stop_key.ToString());

    return properties;
  }

  const char* Name() const override { return "MyTablePropertiesCollector"; }

 private:
  uint64_t total_keys;
  uint64_t deleted_keys;
  rocksdb::Slice start_key;
  rocksdb::Slice stop_key;
};

class MyTablePropertiesCollectorFactory : public rocksdb::TablePropertiesCollectorFactory {
 public:
  MyTablePropertiesCollectorFactory() = default;
  rocksdb::TablePropertiesCollector* CreateTablePropertiesCollector(
      rocksdb::TablePropertiesCollectorFactory::Context context) override {
    return new MyTablePropertiesCollector();
  }

  const char* Name() const override { return "MyTablePropertiesCollectorFactory"; }
};

Status Redis::Open(const StorageOptions& storage_options, const std::string& db_path) {
  statistics_store_->SetCapacity(storage_options.statistics_max_size);
  small_compaction_threshold_ = storage_options.small_compaction_threshold;

  rocksdb::BlockBasedTableOptions table_ops(storage_options.table_options);
  table_ops.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, true));

  auto my_factory = std::make_shared<MyTablePropertiesCollectorFactory>();
  rocksdb::Options ops(storage_options.options);
  ops.create_missing_column_families = true;
  ops.table_properties_collector_factories.emplace_back(my_factory);

  /*
   * Because zset, set, the hash, list, stream type meta
   * information exists kMetaCF, so we delete the various
   * types of MetaCF before
   */
  // meta & string column-family options
  rocksdb::ColumnFamilyOptions meta_cf_ops(storage_options.options);
  meta_cf_ops.table_properties_collector_factories.emplace_back(my_factory);
  meta_cf_ops.compaction_filter_factory = std::make_shared<MetaFilterFactory>();
  rocksdb::BlockBasedTableOptions meta_table_ops(table_ops);

  rocksdb::BlockBasedTableOptions string_table_ops(table_ops);
  if (!storage_options.share_block_cache && storage_options.block_cache_size > 0) {
    meta_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
  }
  meta_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(meta_table_ops));

  // hash column-family options
  rocksdb::ColumnFamilyOptions hash_data_cf_ops(storage_options.options);
  hash_data_cf_ops.table_properties_collector_factories.emplace_back(my_factory);
  hash_data_cf_ops.compaction_filter_factory = std::make_shared<HashesDataFilterFactory>(&db_, &handles_, DataType::kHashes);
  rocksdb::BlockBasedTableOptions hash_data_cf_table_ops(table_ops);
  if (!storage_options.share_block_cache && storage_options.block_cache_size > 0) {
    hash_data_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
  }
  hash_data_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(hash_data_cf_table_ops));

  // list column-family options
  rocksdb::ColumnFamilyOptions list_data_cf_ops(storage_options.options);
  list_data_cf_ops.table_properties_collector_factories.emplace_back(my_factory);
  list_data_cf_ops.compaction_filter_factory = std::make_shared<ListsDataFilterFactory>(&db_, &handles_, DataType::kLists);
  list_data_cf_ops.comparator = ListsDataKeyComparator();

  rocksdb::BlockBasedTableOptions list_data_cf_table_ops(table_ops);
  if (!storage_options.share_block_cache && storage_options.block_cache_size > 0) {
    list_data_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
  }
  list_data_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(list_data_cf_table_ops));

  // set column-family options
  rocksdb::ColumnFamilyOptions set_data_cf_ops(storage_options.options);
  set_data_cf_ops.table_properties_collector_factories.emplace_back(my_factory);
  set_data_cf_ops.compaction_filter_factory = std::make_shared<SetsMemberFilterFactory>(&db_, &handles_, DataType::kSets);
  rocksdb::BlockBasedTableOptions set_data_cf_table_ops(table_ops);
  if (!storage_options.share_block_cache && storage_options.block_cache_size > 0) {
    set_data_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
  }
  set_data_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(set_data_cf_table_ops));

  // zset column-family options
  rocksdb::ColumnFamilyOptions zset_data_cf_ops(storage_options.options);
  rocksdb::ColumnFamilyOptions zset_score_cf_ops(storage_options.options);
  zset_data_cf_ops.table_properties_collector_factories.emplace_back(my_factory);
  zset_score_cf_ops.table_properties_collector_factories.emplace_back(my_factory);
  zset_data_cf_ops.compaction_filter_factory = std::make_shared<ZSetsDataFilterFactory>(&db_, &handles_, DataType::kZSets);
  zset_score_cf_ops.compaction_filter_factory = std::make_shared<ZSetsScoreFilterFactory>(&db_, &handles_, DataType::kZSets);
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
  stream_data_cf_ops.table_properties_collector_factories.emplace_back(my_factory);
  stream_data_cf_ops.compaction_filter_factory = std::make_shared<BaseDataFilterFactory>(&db_, &handles_, DataType::kStreams);
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
  return rocksdb::DB::Open(ops, db_path, column_families, &handles_, &db_);
}

Status Redis::GetScanStartPoint(const DataType& type, const Slice& key, const Slice& pattern, int64_t cursor, std::string* start_point) {
  std::string index_key;
  index_key.append(1, DataTypeTag[static_cast<int>(type)]);
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
  index_key.append(1, DataTypeTag[static_cast<int>(type)]);
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

void SelectColumnFamilyHandles(const DataType& option_type, const ColumnFamilyType& type,
                               std::vector<int>& handleIdxVec) {
  switch (option_type) {
    case DataType::kStrings:
      handleIdxVec.push_back(kMetaCF);
      break;
    case DataType::kHashes:
      if (type == kMeta || type == kMetaAndData) {
        handleIdxVec.push_back(kMetaCF);
      }
      if (type == kData || type == kMetaAndData) {
        handleIdxVec.push_back(kHashesDataCF);
      }
      break;
    case DataType::kSets:
      if (type == kMeta || type == kMetaAndData) {
        handleIdxVec.push_back(kMetaCF);
      }
      if (type == kData || type == kMetaAndData) {
        handleIdxVec.push_back(kSetsDataCF);
      }
      break;
    case DataType::kLists:
      if (type == kMeta || type == kMetaAndData) {
        handleIdxVec.push_back(kMetaCF);
      }
      if (type == kData || type == kMetaAndData) {
        handleIdxVec.push_back(kListsDataCF);
      }
      break;
    case DataType::kZSets:
      if (type == kMeta || type == kMetaAndData) {
        handleIdxVec.push_back(kMetaCF);
      }
      if (type == kData || type == kMetaAndData) {
        handleIdxVec.push_back(kZsetsDataCF);
        handleIdxVec.push_back(kZsetsScoreCF);
      }
      break;
    case DataType::kStreams:
      if (type == kMeta || type == kMetaAndData) {
        handleIdxVec.push_back(kMetaCF);
      }
      if (type == kData || type == kMetaAndData) {
        handleIdxVec.push_back(kStreamsDataCF);
      }
      break;
    case DataType::kAll:
      enum ColumnFamilyIndex s;
      for (s = kMetaCF; s <= kStreamsDataCF; s = (ColumnFamilyIndex)(s + 1)) {
        handleIdxVec.push_back(s);
      }
      break;
    default:
      break;
  }
}

Status Redis::LongestNotCompactiontSstCompact(const DataType& option_type, std::vector<Status>* compact_result_vec,
                                              const ColumnFamilyType& type) {
  bool no_compact = false;
  bool to_comapct = true;
  if (!in_compact_flag_.compare_exchange_weak(no_compact, to_comapct, std::memory_order_relaxed,
                                              std::memory_order_relaxed)) {
    return Status::Corruption("compact running");
  }

  DEFER { in_compact_flag_.store(false); };
  std::vector<int> handleIdxVec;
  SelectColumnFamilyHandles(option_type, type, handleIdxVec);
  if (handleIdxVec.size() == 0) {
    return Status::Corruption("Invalid data type");
  }

  if (compact_result_vec) {
    compact_result_vec->clear();
  }

  pstd::Slice encoded;
  for (auto idx : handleIdxVec) {
    rocksdb::TablePropertiesCollection props;
    Status s = db_->GetPropertiesOfAllTables(handles_[idx], &props);
    if (!s.ok()) {
      if (compact_result_vec) {
        compact_result_vec->push_back(
            Status::Corruption(handles_[idx]->GetName() +
                               " LongestNotCompactiontSstCompact GetPropertiesOfAllTables error: " + s.ToString()));
      }
      continue;
    }

    // The main goal of compaction was reclaimed the disk space and removed
    // the tombstone. It seems that compaction scheduler was unnecessary here when
    // the live files was too few, Hard code to 1 here.
    if (props.size() < 1) {
      // LOG(WARNING) << "LongestNotCompactiontSstCompact " << handles_[idx]->GetName() << " only one file";
      if (compact_result_vec) {
        compact_result_vec->push_back(Status::OK());
      }
      continue;
    }

    size_t max_files_to_compact = 1;
    const StorageOptions& storageOptions = storage_->GetStorageOptions();
    if (props.size() / storageOptions.compact_param_.compact_every_num_of_files_ > max_files_to_compact) {
      max_files_to_compact = props.size() / storageOptions.compact_param_.compact_every_num_of_files_;
    }

    int64_t now =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count();

    auto force_compact_min_ratio =
        static_cast<double>(storageOptions.compact_param_.force_compact_min_delete_ratio_) / 100.0;
    auto best_delete_min_ratio = static_cast<double>(storageOptions.compact_param_.best_delete_min_ratio_) / 100.0;

    std::string best_filename;
    double best_delete_ratio = 0;
    uint64_t total_keys = 0, deleted_keys = 0;
    rocksdb::Slice start_key, stop_key, best_start_key, best_stop_key;
    Status compact_result;
    for (const auto& iter : props) {
      uint64_t file_creation_time = iter.second->file_creation_time;
      if (file_creation_time == 0) {
        // Fallback to the file Modification time to prevent repeatedly compacting the same file,
        // file_creation_time is 0 which means the unknown condition in rocksdb
        auto s = rocksdb::Env::Default()->GetFileModificationTime(iter.first, &file_creation_time);
        if (!s.ok()) {
          LOG(WARNING) << handles_[idx]->GetName() << " Failed to get the file creation time: " << iter.first << " in "
                       << handles_[idx]->GetName() << ", err: " << s.ToString();
          continue;
        }
      }

      for (const auto& property_iter : iter.second->user_collected_properties) {
        if (property_iter.first == "total_keys") {
          encoded = property_iter.second;
          pstd::GetFixed64(&encoded, &total_keys);
        }
        if (property_iter.first == "deleted_keys") {
          encoded = property_iter.second;
          pstd::GetFixed64(&encoded, &deleted_keys);
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
      if (file_creation_time <
              static_cast<uint64_t>(now / 1000 - storageOptions.compact_param_.force_compact_file_age_seconds_) &&
          delete_ratio >= force_compact_min_ratio) {
        compact_result = db_->CompactRange(default_compact_range_options_, &start_key, &stop_key);
        if (--max_files_to_compact == 0) {
          break;
        }
        continue;
      }

      // don't compact the SST created in x `dont_compact_sst_created_in_seconds_`.
      if (file_creation_time >
          static_cast<uint64_t>(now / 1000 - storageOptions.compact_param_.dont_compact_sst_created_in_seconds_)) {
        continue;
      }

      // pick the file which has highest delete ratio
      if (total_keys != 0 && delete_ratio > best_delete_ratio) {
        best_delete_ratio = delete_ratio;
        best_filename = iter.first;
        best_start_key = start_key;
        start_key.clear();
        best_stop_key = stop_key;
        stop_key.clear();
      }
    }

    if (best_delete_ratio > best_delete_min_ratio && !best_start_key.empty() && !best_stop_key.empty()) {
      compact_result =
          db_->CompactRange(default_compact_range_options_, handles_[idx], &best_start_key, &best_stop_key);
    }

    if (!compact_result.ok()) {
      if (compact_result_vec) {
        compact_result_vec->push_back(
            Status::Corruption(handles_[idx]->GetName() + " Failed to do compaction " + compact_result.ToString()));
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
    lkp_key.append(1, DataTypeTag[static_cast<int>(dtype)]);
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
    lkp_key.append(1, DataTypeTag[static_cast<int>(dtype)]);
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
    std::string lkp_key(1, DataTypeTag[static_cast<int>(dtype)]);
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

    auto write_aggregated_int_property=[&](const Slice& property, const char *metric) {
      uint64_t value = 0;
      db_->GetAggregatedIntProperty(property, &value);
      string_stream << prefix << metric << ':' << value << "\r\n";
    };

    auto write_property=[&](const Slice& property, const char *metric) {
      if (handles_.size() == 0) {
        std::string value;
        db_->GetProperty(db_->DefaultColumnFamily(), property, &value);
        string_stream << prefix << metric << "_" << db_->DefaultColumnFamily()->GetName() << ':' << value << "\r\n";
      } else {
        for (auto handle : handles_) {
          std::string value;
          db_->GetProperty(handle, property, &value);
          string_stream << prefix << metric << "_" << handle->GetName() <<  ':' << value << "\r\n";
        }
      }
    };

    auto write_ticker_count = [&](uint32_t tick_type, const char *metric) {
      if (db_statistics_ == nullptr) {
        return;
      }
      uint64_t count = db_statistics_->getTickerCount(tick_type);
      string_stream << prefix << metric << ':' << count << "\r\n";
    };

    auto mapToString=[&](const std::map<std::string, std::string>& map_data, const char *prefix) {
      for (const auto& kv : map_data) {
        std::string str_data;
        str_data += kv.first + ": " + kv.second + "\r\n";
        string_stream << prefix << str_data;
      }
    };

    // memtables num
    write_aggregated_int_property(rocksdb::DB::Properties::kNumImmutableMemTable, "num_immutable_mem_table");
    write_aggregated_int_property(rocksdb::DB::Properties::kNumImmutableMemTableFlushed, "num_immutable_mem_table_flushed");
    write_aggregated_int_property(rocksdb::DB::Properties::kMemTableFlushPending, "mem_table_flush_pending");
    write_aggregated_int_property(rocksdb::DB::Properties::kNumRunningFlushes, "num_running_flushes");

    // compaction
    write_aggregated_int_property(rocksdb::DB::Properties::kCompactionPending, "compaction_pending");
    write_aggregated_int_property(rocksdb::DB::Properties::kNumRunningCompactions, "num_running_compactions");

    // background errors
    write_aggregated_int_property(rocksdb::DB::Properties::kBackgroundErrors, "background_errors");

    // memtables size
    write_aggregated_int_property(rocksdb::DB::Properties::kCurSizeActiveMemTable, "cur_size_active_mem_table");
    write_aggregated_int_property(rocksdb::DB::Properties::kCurSizeAllMemTables, "cur_size_all_mem_tables");
    write_aggregated_int_property(rocksdb::DB::Properties::kSizeAllMemTables, "size_all_mem_tables");

    // keys
    write_aggregated_int_property(rocksdb::DB::Properties::kEstimateNumKeys, "estimate_num_keys");

    // table readers mem
    write_aggregated_int_property(rocksdb::DB::Properties::kEstimateTableReadersMem, "estimate_table_readers_mem");

    // snapshot
    write_aggregated_int_property(rocksdb::DB::Properties::kNumSnapshots, "num_snapshots");

    // version
    write_aggregated_int_property(rocksdb::DB::Properties::kNumLiveVersions, "num_live_versions");
    write_aggregated_int_property(rocksdb::DB::Properties::kCurrentSuperVersionNumber, "current_super_version_number");

    // live data size
    write_aggregated_int_property(rocksdb::DB::Properties::kEstimateLiveDataSize, "estimate_live_data_size");

    // sst files
    write_property(rocksdb::DB::Properties::kNumFilesAtLevelPrefix+"0", "num_files_at_level0");
    write_property(rocksdb::DB::Properties::kNumFilesAtLevelPrefix+"1", "num_files_at_level1");
    write_property(rocksdb::DB::Properties::kNumFilesAtLevelPrefix+"2", "num_files_at_level2");
    write_property(rocksdb::DB::Properties::kNumFilesAtLevelPrefix+"3", "num_files_at_level3");
    write_property(rocksdb::DB::Properties::kNumFilesAtLevelPrefix+"4", "num_files_at_level4");
    write_property(rocksdb::DB::Properties::kNumFilesAtLevelPrefix+"5", "num_files_at_level5");
    write_property(rocksdb::DB::Properties::kNumFilesAtLevelPrefix+"6", "num_files_at_level6");
    write_property(rocksdb::DB::Properties::kCompressionRatioAtLevelPrefix+"0", "compression_ratio_at_level0");
    write_property(rocksdb::DB::Properties::kCompressionRatioAtLevelPrefix+"1", "compression_ratio_at_level1");
    write_property(rocksdb::DB::Properties::kCompressionRatioAtLevelPrefix+"2", "compression_ratio_at_level2");
    write_property(rocksdb::DB::Properties::kCompressionRatioAtLevelPrefix+"3", "compression_ratio_at_level3");
    write_property(rocksdb::DB::Properties::kCompressionRatioAtLevelPrefix+"4", "compression_ratio_at_level4");
    write_property(rocksdb::DB::Properties::kCompressionRatioAtLevelPrefix+"5", "compression_ratio_at_level5");
    write_property(rocksdb::DB::Properties::kCompressionRatioAtLevelPrefix+"6", "compression_ratio_at_level6");
    write_aggregated_int_property(rocksdb::DB::Properties::kTotalSstFilesSize, "total_sst_files_size");
    write_aggregated_int_property(rocksdb::DB::Properties::kLiveSstFilesSize, "live_sst_files_size");

    // pending compaction bytes
    write_aggregated_int_property(rocksdb::DB::Properties::kEstimatePendingCompactionBytes, "estimate_pending_compaction_bytes");

    // block cache
    write_aggregated_int_property(rocksdb::DB::Properties::kBlockCacheCapacity, "block_cache_capacity");
    write_aggregated_int_property(rocksdb::DB::Properties::kBlockCacheUsage, "block_cache_usage");
    write_aggregated_int_property(rocksdb::DB::Properties::kBlockCachePinnedUsage, "block_cache_pinned_usage");

    // blob files
    write_aggregated_int_property(rocksdb::DB::Properties::kNumBlobFiles, "num_blob_files");
    write_aggregated_int_property(rocksdb::DB::Properties::kBlobStats, "blob_stats");
    write_aggregated_int_property(rocksdb::DB::Properties::kTotalBlobFileSize, "total_blob_file_size");
    write_aggregated_int_property(rocksdb::DB::Properties::kLiveBlobFileSize, "live_blob_file_size");

    write_aggregated_int_property(rocksdb::DB::Properties::kBlobCacheCapacity, "blob_cache_capacity");
    write_aggregated_int_property(rocksdb::DB::Properties::kBlobCacheUsage, "blob_cache_usage");
    write_aggregated_int_property(rocksdb::DB::Properties::kBlobCachePinnedUsage, "blob_cache_pinned_usage");

    //rocksdb ticker
    {
      // memtables num
      write_ticker_count(rocksdb::Tickers::MEMTABLE_HIT, "memtable_hit");
      write_ticker_count(rocksdb::Tickers::MEMTABLE_MISS, "memtable_miss");

      write_ticker_count(rocksdb::Tickers::BYTES_WRITTEN, "bytes_written");
      write_ticker_count(rocksdb::Tickers::BYTES_READ, "bytes_read");
      write_ticker_count(rocksdb::Tickers::ITER_BYTES_READ, "iter_bytes_read");
      write_ticker_count(rocksdb::Tickers::GET_HIT_L0, "get_hit_l0");
      write_ticker_count(rocksdb::Tickers::GET_HIT_L1, "get_hit_l1");
      write_ticker_count(rocksdb::Tickers::GET_HIT_L2_AND_UP, "get_hit_l2_and_up");

      write_ticker_count(rocksdb::Tickers::BLOOM_FILTER_USEFUL, "bloom_filter_useful");
      write_ticker_count(rocksdb::Tickers::BLOOM_FILTER_FULL_POSITIVE, "bloom_filter_full_positive");
      write_ticker_count(rocksdb::Tickers::BLOOM_FILTER_FULL_TRUE_POSITIVE, "bloom_filter_full_true_positive");
      write_ticker_count(rocksdb::Tickers::BLOOM_FILTER_PREFIX_CHECKED, "bloom_filter_prefix_checked");
      write_ticker_count(rocksdb::Tickers::BLOOM_FILTER_PREFIX_USEFUL, "bloom_filter_prefix_useful");

      // compaction
      write_ticker_count(rocksdb::Tickers::COMPACTION_KEY_DROP_NEWER_ENTRY, "compaction_key_drop_newer_entry");
      write_ticker_count(rocksdb::Tickers::COMPACTION_KEY_DROP_OBSOLETE, "compaction_key_drop_obsolete");
      write_ticker_count(rocksdb::Tickers::COMPACTION_KEY_DROP_USER, "compaction_key_drop_user");
      write_ticker_count(rocksdb::Tickers::COMPACTION_OPTIMIZED_DEL_DROP_OBSOLETE, "compaction_optimized_del_drop_obsolete");
      write_ticker_count(rocksdb::Tickers::COMPACT_READ_BYTES, "compact_read_bytes");
      write_ticker_count(rocksdb::Tickers::COMPACT_WRITE_BYTES, "compact_write_bytes");
      write_ticker_count(rocksdb::Tickers::FLUSH_WRITE_BYTES, "flush_write_bytes");

      // keys
      write_ticker_count(rocksdb::Tickers::NUMBER_KEYS_READ, "number_keys_read");
      write_ticker_count(rocksdb::Tickers::NUMBER_KEYS_WRITTEN, "number_keys_written");
      write_ticker_count(rocksdb::Tickers::NUMBER_KEYS_UPDATED, "number_keys_updated");
      write_ticker_count(rocksdb::Tickers::NUMBER_OF_RESEEKS_IN_ITERATION, "number_of_reseeks_in_iteration");

      write_ticker_count(rocksdb::Tickers::NUMBER_DB_SEEK, "number_db_seek");
      write_ticker_count(rocksdb::Tickers::NUMBER_DB_NEXT, "number_db_next");
      write_ticker_count(rocksdb::Tickers::NUMBER_DB_PREV, "number_db_prev");
      write_ticker_count(rocksdb::Tickers::NUMBER_DB_SEEK_FOUND, "number_db_seek_found");
      write_ticker_count(rocksdb::Tickers::NUMBER_DB_NEXT_FOUND, "number_db_next_found");
      write_ticker_count(rocksdb::Tickers::NUMBER_DB_PREV_FOUND, "number_db_prev_found");
      write_ticker_count(rocksdb::Tickers::LAST_LEVEL_READ_BYTES, "last_level_read_bytes");
      write_ticker_count(rocksdb::Tickers::LAST_LEVEL_READ_COUNT, "last_level_read_count");
      write_ticker_count(rocksdb::Tickers::NON_LAST_LEVEL_READ_BYTES, "non_last_level_read_bytes");
      write_ticker_count(rocksdb::Tickers::NON_LAST_LEVEL_READ_COUNT, "non_last_level_read_count");

      // background errors
      write_ticker_count(rocksdb::Tickers::STALL_MICROS, "stall_micros");

      // sst files
      write_ticker_count(rocksdb::Tickers::NO_FILE_OPENS, "no_file_opens");
      write_ticker_count(rocksdb::Tickers::NO_FILE_ERRORS, "no_file_errors");

      // block cache
      write_ticker_count(rocksdb::Tickers::BLOCK_CACHE_INDEX_HIT, "block_cache_index_hit");
      write_ticker_count(rocksdb::Tickers::BLOCK_CACHE_INDEX_MISS, "block_cache_index_miss");
      write_ticker_count(rocksdb::Tickers::BLOCK_CACHE_FILTER_HIT, "block_cache_filter_hit");
      write_ticker_count(rocksdb::Tickers::BLOCK_CACHE_FILTER_MISS, "block_cache_filter_miss");
      write_ticker_count(rocksdb::Tickers::BLOCK_CACHE_DATA_HIT, "block_cache_data_hit");
      write_ticker_count(rocksdb::Tickers::BLOCK_CACHE_DATA_MISS, "block_cache_data_miss");
      write_ticker_count(rocksdb::Tickers::BLOCK_CACHE_BYTES_READ, "block_cache_bytes_read");
      write_ticker_count(rocksdb::Tickers::BLOCK_CACHE_BYTES_WRITE, "block_cache_bytes_write");

      // blob files
      write_ticker_count(rocksdb::Tickers::BLOB_DB_NUM_KEYS_WRITTEN, "blob_db_num_keys_written");
      write_ticker_count(rocksdb::Tickers::BLOB_DB_NUM_KEYS_READ, "blob_db_num_keys_read");
      write_ticker_count(rocksdb::Tickers::BLOB_DB_BYTES_WRITTEN, "blob_db_bytes_written");
      write_ticker_count(rocksdb::Tickers::BLOB_DB_BYTES_READ, "blob_db_bytes_read");
      write_ticker_count(rocksdb::Tickers::BLOB_DB_NUM_SEEK, "blob_db_num_seek");
      write_ticker_count(rocksdb::Tickers::BLOB_DB_NUM_NEXT, "blob_db_num_next");
      write_ticker_count(rocksdb::Tickers::BLOB_DB_NUM_PREV, "blob_db_num_prev");
      write_ticker_count(rocksdb::Tickers::BLOB_DB_BLOB_FILE_BYTES_WRITTEN, "blob_db_blob_file_bytes_written");
      write_ticker_count(rocksdb::Tickers::BLOB_DB_BLOB_FILE_BYTES_READ, "blob_db_blob_file_bytes_read");

      write_ticker_count(rocksdb::Tickers::BLOB_DB_GC_NUM_FILES, "blob_db_gc_num_files");
      write_ticker_count(rocksdb::Tickers::BLOB_DB_GC_NUM_NEW_FILES, "blob_db_gc_num_new_files");
      write_ticker_count(rocksdb::Tickers::BLOB_DB_GC_NUM_KEYS_RELOCATED, "blob_db_gc_num_keys_relocated");
      write_ticker_count(rocksdb::Tickers::BLOB_DB_GC_BYTES_RELOCATED, "blob_db_gc_bytes_relocated");

      write_ticker_count(rocksdb::Tickers::BLOB_DB_CACHE_MISS, "blob_db_cache_miss");
      write_ticker_count(rocksdb::Tickers::BLOB_DB_CACHE_HIT, "blob_db_cache_hit");
      write_ticker_count(rocksdb::Tickers::BLOB_DB_CACHE_BYTES_READ, "blob_db_cache_bytes_read");
      write_ticker_count(rocksdb::Tickers::BLOB_DB_CACHE_BYTES_WRITE, "blob_db_cache_bytes_write");
    }
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
  key_infos->resize(DataTypeNum);
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
  s = ScanStreamsKeyNum(&((*key_infos)[5]));
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
