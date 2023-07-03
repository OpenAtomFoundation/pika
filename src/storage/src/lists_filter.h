//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_LISTS_FILTER_H_
#define SRC_LISTS_FILTER_H_

#include <memory>
#include <string>
#include <vector>

#include "rocksdb/compaction_filter.h"
#include "rocksdb/db.h"
#include "src/debug.h"
#include "src/lists_data_key_format.h"
#include "src/lists_meta_value_format.h"

namespace storage {

class ListsMetaFilter : public rocksdb::CompactionFilter {
 public:
  ListsMetaFilter() = default;
  bool Filter(int level, const rocksdb::Slice& key, const rocksdb::Slice& value,
              std::string* new_value, bool* value_changed) const override {
    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    auto cur_time = static_cast<int32_t>(unix_time);
    ParsedListsMetaValue parsed_lists_meta_value(value);
    TRACE("==========================START==========================");
    TRACE(
        "[ListMetaFilter], key: %s, count = %llu, timestamp: %d, cur_time: %d, "
        "version: %d",
        key.ToString().c_str(), parsed_lists_meta_value.count(),
        parsed_lists_meta_value.timestamp(), cur_time,
        parsed_lists_meta_value.version());

    if (parsed_lists_meta_value.timestamp() != 0 &&
        parsed_lists_meta_value.timestamp() < cur_time &&
        parsed_lists_meta_value.version() < cur_time) {
      TRACE("Drop[Stale & version < cur_time]");
      return true;
    }
    if (parsed_lists_meta_value.count() == 0 &&
        parsed_lists_meta_value.version() < cur_time) {
      TRACE("Drop[Empty & version < cur_time]");
      return true;
    }
    TRACE("Reserve");
    return false;
  }

  const char* Name() const override { return "ListsMetaFilter"; }
};

class ListsMetaFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  ListsMetaFilterFactory() = default;
  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context& context) override {
    return std::unique_ptr<rocksdb::CompactionFilter>(new ListsMetaFilter());
  }
  const char* Name() const override { return "ListsMetaFilterFactory"; }
};

class ListsDataFilter : public rocksdb::CompactionFilter {
 public:
  ListsDataFilter(rocksdb::DB* db,
                  std::vector<rocksdb::ColumnFamilyHandle*>* cf_handles_ptr)
      : db_(db), cf_handles_ptr_(cf_handles_ptr) {}

  bool Filter(int level, const rocksdb::Slice& key, const rocksdb::Slice& value,
              std::string* new_value, bool* value_changed) const override {
    ParsedListsDataKey parsed_lists_data_key(key);
    TRACE("==========================START==========================");
    TRACE("[DataFilter], key: %s, index = %llu, data = %s, version = %d",
          parsed_lists_data_key.key().ToString().c_str(),
          parsed_lists_data_key.index(), value.ToString().c_str(),
          parsed_lists_data_key.version());

    if (parsed_lists_data_key.key().ToString() != cur_key_) {
      cur_key_ = parsed_lists_data_key.key().ToString();
      std::string meta_value;
      // destroyed when close the database, Reserve Current key value
      if (cf_handles_ptr_->empty()) {
        return false;
      }
      rocksdb::Status s = db_->Get(default_read_options_, (*cf_handles_ptr_)[0],
                                   cur_key_, &meta_value);
      if (s.ok()) {
        meta_not_found_ = false;
        ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
        cur_meta_version_ = parsed_lists_meta_value.version();
        cur_meta_timestamp_ = parsed_lists_meta_value.timestamp();
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
    if (cur_meta_timestamp_ != 0 &&
        cur_meta_timestamp_ < static_cast<int32_t>(unix_time)) {
      TRACE("Drop[Timeout]");
      return true;
    }

    if (cur_meta_version_ > parsed_lists_data_key.version()) {
      TRACE("Drop[list_data_key_version < cur_meta_version]");
      return true;
    } else {
      TRACE("Reserve[list_data_key_version == cur_meta_version]");
      return false;
    }
  }

  const char* Name() const override { return "ListsDataFilter"; }

 private:
  rocksdb::DB* db_ = nullptr;
  std::vector<rocksdb::ColumnFamilyHandle*>* cf_handles_ptr_ = nullptr;
  rocksdb::ReadOptions default_read_options_;
  mutable std::string cur_key_;
  mutable bool meta_not_found_ = false;
  mutable int32_t cur_meta_version_ = 0;
  mutable int32_t cur_meta_timestamp_ = 0;
};

class ListsDataFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  ListsDataFilterFactory(rocksdb::DB** db_ptr,
                         std::vector<rocksdb::ColumnFamilyHandle*>* handles_ptr)
      : db_ptr_(db_ptr), cf_handles_ptr_(handles_ptr) {}

  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context& context) override {
    return std::unique_ptr<rocksdb::CompactionFilter>(
        new ListsDataFilter(*db_ptr_, cf_handles_ptr_));
  }
  const char* Name() const override { return "ListsDataFilterFactory"; }

 private:
  rocksdb::DB** db_ptr_ = nullptr;
  std::vector<rocksdb::ColumnFamilyHandle*>* cf_handles_ptr_ = nullptr;
};

}  //  namespace storage
#endif  // SRC_LISTS_FILTER_H_
