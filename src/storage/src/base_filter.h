//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_BASE_FILTER_H_
#define SRC_BASE_FILTER_H_

#include <memory>
#include <string>
#include <vector>

#include "glog/logging.h"
#include "rocksdb/compaction_filter.h"
#include "src/base_data_key_format.h"
#include "src/base_meta_value_format.h"
#include "src/debug.h"
#ifdef USE_S3
#include "rocksdb/cloud/db_cloud.h"
#else
#include "rocksdb/db.h"
#endif

namespace storage {

class BaseMetaFilter : public rocksdb::CompactionFilter {
 public:
  BaseMetaFilter() = default;
  bool Filter(int level, const rocksdb::Slice& key, const rocksdb::Slice& value, std::string* new_value,
              bool* value_changed) const override {
    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    auto cur_time = static_cast<uint64_t>(unix_time);
    ParsedBaseMetaValue parsed_base_meta_value(value);
    TRACE("==========================START==========================");
    TRACE("[MetaFilter], key: %s, count = %d, timestamp: %llu, cur_time: %llu, version: %llu", key.ToString().c_str(),
          parsed_base_meta_value.Count(), parsed_base_meta_value.Etime(), cur_time,
          parsed_base_meta_value.Version());

    if (parsed_base_meta_value.Etime() != 0 && parsed_base_meta_value.Etime() < cur_time &&
        parsed_base_meta_value.Version() < cur_time) {
      TRACE("Drop[Stale & version < cur_time]");
      return true;
    }
    if (parsed_base_meta_value.Count() == 0 && parsed_base_meta_value.Version() < cur_time) {
      TRACE("Drop[Empty & version < cur_time]");
      return true;
    }
    TRACE("Reserve");
    return false;
  }

  const char* Name() const override { return "BaseMetaFilter"; }
};

class BaseMetaFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  BaseMetaFilterFactory() = default;
  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context& context) override {
    return std::unique_ptr<rocksdb::CompactionFilter>(new BaseMetaFilter());
  }
  const char* Name() const override { return "BaseMetaFilterFactory"; }
};

class BaseDataFilter : public rocksdb::CompactionFilter {
 public:
#ifdef USE_S3
  BaseDataFilter(rocksdb::DBCloud* db, std::vector<rocksdb::ColumnFamilyHandle*>* cf_handles_ptr, int meta_cf_index)
#else
  BaseDataFilter(rocksdb::DB* db, std::vector<rocksdb::ColumnFamilyHandle*>* cf_handles_ptr, int meta_cf_index)
#endif
      : db_(db),
        cf_handles_ptr_(cf_handles_ptr),
        meta_cf_index_(meta_cf_index)
        {}

  bool Filter(int level, const Slice& key, const rocksdb::Slice& value, std::string* new_value,
              bool* value_changed) const override {
    UNUSED(level);
    UNUSED(value);
    UNUSED(new_value);
    UNUSED(value_changed);
    ParsedBaseDataKey parsed_base_data_key(key);
    TRACE("==========================START==========================");
    TRACE("[DataFilter], key: %s, data = %s, version = %llu", parsed_base_data_key.Key().ToString().c_str(),
          parsed_base_data_key.Data().ToString().c_str(), parsed_base_data_key.Version());

    const char* ptr = key.data();
    int key_size = key.size();
    ptr = SeekUserkeyDelim(ptr + kPrefixReserveLength, key_size - kPrefixReserveLength);
    std::string meta_key_enc(key.data(), std::distance(key.data(), ptr));
    meta_key_enc.append(kSuffixReserveLength, kNeedTransformCharacter);

    if (meta_key_enc != cur_key_) {
      cur_key_ = meta_key_enc;
      std::string meta_value;
      // destroyed when close the database, Reserve Current key value
      if (cf_handles_ptr_->empty()) {
        return false;
      }
      Status s = db_->Get(default_read_options_, (*cf_handles_ptr_)[meta_cf_index_], cur_key_, &meta_value);
      if (s.ok()) {
        meta_not_found_ = false;
        ParsedBaseMetaValue parsed_base_meta_value(&meta_value);
        cur_meta_version_ = parsed_base_meta_value.Version();
        cur_meta_etime_ = parsed_base_meta_value.Etime();
      } else if (s.IsNotFound()) {
        meta_not_found_ = true;
      } else {
        cur_key_ = "";
        TRACE("Reserve[Get meta_key faild]");
        return false;
      }
    }

    if (meta_not_found_) {
      TRACE("Drop[Meta key not exist]");
      return true;
    }

    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    if (cur_meta_etime_ != 0 && cur_meta_etime_ < static_cast<uint64_t>(unix_time)) {
      TRACE("Drop[Timeout]");
      return true;
    }

    if (cur_meta_version_ > parsed_base_data_key.Version()) {
      TRACE("Drop[data_key_version < cur_meta_version]");
      return true;
    } else {
      TRACE("Reserve[data_key_version == cur_meta_version]");
      return false;
    }
  }

  /*
  // Only judge by meta value ttl
  virtual rocksdb::CompactionFilter::Decision FilterBlobByKey(int level, const Slice& key,
      uint64_t expire_time, std::string* new_value, std::string* skip_until) const override {
    UNUSED(level);
    UNUSED(expire_time);
    UNUSED(new_value);
    UNUSED(skip_until);
    bool unused_value_changed;
    bool should_remove = Filter(level, key, Slice{}, new_value, &unused_value_changed);
    if (should_remove) {
      return CompactionFilter::Decision::kRemove;
    }
    return CompactionFilter::Decision::kKeep;
  }
  */

  const char* Name() const override { return "BaseDataFilter"; }

 private:
#ifdef USE_S3
  rocksdb::DBCloud* db_ = nullptr;
#else
  rocksdb::DB* db_ = nullptr;
#endif
  std::vector<rocksdb::ColumnFamilyHandle*>* cf_handles_ptr_ = nullptr;
  rocksdb::ReadOptions default_read_options_;
  mutable std::string cur_key_;
  mutable bool meta_not_found_ = false;
  mutable uint64_t cur_meta_version_ = 0;
  mutable uint64_t cur_meta_etime_ = 0;
  int meta_cf_index_ = 0;
};

class BaseDataFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
#ifdef USE_S3
  BaseDataFilterFactory(rocksdb::DBCloud** db_ptr, std::vector<rocksdb::ColumnFamilyHandle*>* handles_ptr, int meta_cf_index)
#else
  BaseDataFilterFactory(rocksdb::DB** db_ptr, std::vector<rocksdb::ColumnFamilyHandle*>* handles_ptr, int meta_cf_index)
#endif
      : db_ptr_(db_ptr), cf_handles_ptr_(handles_ptr), meta_cf_index_(meta_cf_index) {}
  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context& context) override {
    return std::make_unique<BaseDataFilter>(BaseDataFilter(*db_ptr_, cf_handles_ptr_, meta_cf_index_));
  }
  const char* Name() const override { return "BaseDataFilterFactory"; }

 private:
#ifdef USE_S3
  rocksdb::DBCloud** db_ptr_ = nullptr;
#else
  rocksdb::DB** db_ptr_ = nullptr;
#endif
  std::vector<rocksdb::ColumnFamilyHandle*>* cf_handles_ptr_ = nullptr;
  int meta_cf_index_ = 0;
};

using HashesMetaFilter = BaseMetaFilter;
using HashesMetaFilterFactory = BaseMetaFilterFactory;
using HashesDataFilter = BaseDataFilter;
using HashesDataFilterFactory = BaseDataFilterFactory;

using SetsMetaFilter = BaseMetaFilter;
using SetsMetaFilterFactory = BaseMetaFilterFactory;
using SetsMemberFilter = BaseDataFilter;
using SetsMemberFilterFactory = BaseDataFilterFactory;

using ZSetsMetaFilter = BaseMetaFilter;
using ZSetsMetaFilterFactory = BaseMetaFilterFactory;
using ZSetsDataFilter = BaseDataFilter;
using ZSetsDataFilterFactory = BaseDataFilterFactory;

}  //  namespace storage
#endif  // SRC_BASE_FILTER_H_
