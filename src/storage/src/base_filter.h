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
#include "src/base_value_format.h"
#include "src/base_meta_value_format.h"
#include "src/lists_meta_value_format.h"
#include "src/strings_value_format.h"
#include "src/zsets_data_key_format.h"
#include "src/debug.h"

namespace storage {

class BaseMetaFilter : public rocksdb::CompactionFilter {
 public:
  BaseMetaFilter() = default;
  bool Filter(int level, const rocksdb::Slice& key, const rocksdb::Slice& value, std::string* new_value,
              bool* value_changed) const override {
    int64_t unix_time = g_storage_logictime->Now();
    auto cur_time = static_cast<uint64_t>(unix_time);
    /*
     * For the filtering of meta information, because the field designs of string
     * and list are different, their filtering policies are written separately.
     * The field designs of the remaining zset,set,hash and stream in meta-value
     * are the same, so the same filtering strategy is used
     */
    auto type = static_cast<enum DataType>(static_cast<uint8_t>(value[0]));
    DEBUG("==========================START==========================");
    if (type == DataType::kStrings) {
      ParsedStringsValue parsed_strings_value(value);
      DEBUG("[StringsFilter]  key: {}, value = {}, timestamp: {}, cur_time: {}", key.ToString().c_str(),
            parsed_strings_value.UserValue().ToString().c_str(), parsed_strings_value.Etime(), cur_time);
      if (parsed_strings_value.Etime() != 0 && parsed_strings_value.Etime() < cur_time) {
        DEBUG("Drop[Stale]");
        return true;
      } else {
        DEBUG("Reserve");
        return false;
      }
    } else if (type == DataType::kLists) {
      ParsedListsMetaValue parsed_lists_meta_value(value);
      DEBUG("[ListMetaFilter], key: {}, count = {}, timestamp: {}, cur_time: {}, version: {}", key.ToString().c_str(),
            parsed_lists_meta_value.Count(), parsed_lists_meta_value.Etime(), cur_time,
            parsed_lists_meta_value.Version());

      if (parsed_lists_meta_value.Etime() != 0 && parsed_lists_meta_value.Etime() < cur_time &&
          parsed_lists_meta_value.Version() < cur_time) {
        DEBUG("Drop[Stale & version < cur_time]");
        return true;
      }
      if (parsed_lists_meta_value.Count() == 0 && parsed_lists_meta_value.Version() < cur_time) {
        DEBUG("Drop[Empty & version < cur_time]");
        return true;
      }
      DEBUG("Reserve");
      return false;
    } else {
      ParsedBaseMetaValue parsed_base_meta_value(value);
      DEBUG("[MetaFilter]  key: {}, count = {}, timestamp: {}, cur_time: {}, version: {}", key.ToString().c_str(),
            parsed_base_meta_value.Count(), parsed_base_meta_value.Etime(), cur_time, parsed_base_meta_value.Version());

      if (parsed_base_meta_value.Etime() != 0 && parsed_base_meta_value.Etime() < cur_time &&
          parsed_base_meta_value.Version() < cur_time) {
        DEBUG("Drop[Stale & version < cur_time]");
        return true;
      }
      if (parsed_base_meta_value.Count() == 0 && parsed_base_meta_value.Version() < cur_time) {
        DEBUG("Drop[Empty & version < cur_time]");
        return true;
      }
      DEBUG("Reserve");
      return false;
    }
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
  BaseDataFilter(rocksdb::DB* db, std::vector<rocksdb::ColumnFamilyHandle*>* cf_handles_ptr, enum DataType type)
      : db_(db),
        cf_handles_ptr_(cf_handles_ptr),
        type_(type)
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
      cur_meta_etime_ = 0;
      cur_meta_version_ = 0;
      meta_not_found_ = true;
      cur_key_ = meta_key_enc;
      std::string meta_value;
      // destroyed when close the database, Reserve Current key value
      if (cf_handles_ptr_->empty()) {
        return false;
      }
      Status s = db_->Get(default_read_options_, (*cf_handles_ptr_)[0], cur_key_, &meta_value);
      if (s.ok()) {
        /*
         * The elimination policy for keys of the Data type is that if the key
         * type obtained from MetaCF is inconsistent with the key type in Data,
         * it needs to be eliminated
         */
        auto type = static_cast<enum DataType>(static_cast<uint8_t>(meta_value[0]));
        if (type != type_) {
          return true;
        } else if (type == DataType::kHashes || type == DataType::kSets || type == DataType::kStreams || type == DataType::kZSets) {
          ParsedBaseMetaValue parsed_base_meta_value(&meta_value);
          meta_not_found_ = false;
          cur_meta_version_ = parsed_base_meta_value.Version();
          cur_meta_etime_ = parsed_base_meta_value.Etime();
        } else {
          return true;
        }
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
  rocksdb::DB* db_ = nullptr;
  std::vector<rocksdb::ColumnFamilyHandle*>* cf_handles_ptr_ = nullptr;
  rocksdb::ReadOptions default_read_options_;
  mutable std::string cur_key_;
  mutable bool meta_not_found_ = false;
  mutable uint64_t cur_meta_version_ = 0;
  mutable uint64_t cur_meta_etime_ = 0;
  enum DataType type_ = DataType::kNones;
};

class BaseDataFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  BaseDataFilterFactory(rocksdb::DB** db_ptr, std::vector<rocksdb::ColumnFamilyHandle*>* handles_ptr, enum DataType type)
      : db_ptr_(db_ptr), cf_handles_ptr_(handles_ptr), type_(type) {}
  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context& context) override {
    return std::make_unique<BaseDataFilter>(BaseDataFilter(*db_ptr_, cf_handles_ptr_, type_));
  }
  const char* Name() const override { return "BaseDataFilterFactory"; }

 private:
  rocksdb::DB** db_ptr_ = nullptr;
  std::vector<rocksdb::ColumnFamilyHandle*>* cf_handles_ptr_ = nullptr;
  enum DataType type_ = DataType::kNones;
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

using SetsMemberFilter = BaseDataFilter;
using SetsMemberFilterFactory = BaseDataFilterFactory;

using MetaFilter = BaseMetaFilter;
using MetaFilterFactory = BaseMetaFilterFactory;
}  //  namespace storage
#endif  // SRC_BASE_FILTER_H_
