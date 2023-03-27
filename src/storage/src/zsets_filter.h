//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_ZSETS_FILTER_H_
#define SRC_ZSETS_FILTER_H_

#include <string>
#include <vector>
#include <memory>

#include "rocksdb/compaction_filter.h"

#include "base_filter.h"
#include "base_meta_value_format.h"
#include "zsets_data_key_format.h"


namespace storage {

class ZSetsScoreFilter : public rocksdb::CompactionFilter {
 public:
  ZSetsScoreFilter(rocksdb::DB* db,
                   std::vector<rocksdb::ColumnFamilyHandle*>* handles_ptr) :
    db_(db),
    cf_handles_ptr_(handles_ptr),
    meta_not_found_(false),
    cur_meta_version_(0),
    cur_meta_timestamp_(0) {}

  bool Filter(int level, const rocksdb::Slice& key,
              const rocksdb::Slice& value,
              std::string* new_value,
              bool* value_changed) const override {
    ParsedZSetsScoreKey parsed_zsets_score_key(key);
    Trace("==========================START==========================");
    Trace("[ScoreFilter], key: %s, score = %lf, member = %s, version = %d",
          parsed_zsets_score_key.key().ToString().c_str(),
          parsed_zsets_score_key.score(),
          parsed_zsets_score_key.member().ToString().c_str(),
          parsed_zsets_score_key.version());

    if (parsed_zsets_score_key.key().ToString() != cur_key_) {
      cur_key_ = parsed_zsets_score_key.key().ToString();
      std::string meta_value;
      // destroyed when close the database, Reserve Current key value
      if (cf_handles_ptr_->size() == 0) {
        return false;
      }
      Status s = db_->Get(default_read_options_,
              (*cf_handles_ptr_)[0], cur_key_, &meta_value);
      if (s.ok()) {
        meta_not_found_ = false;
        ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
        cur_meta_version_ = parsed_zsets_meta_value.version();
        cur_meta_timestamp_ = parsed_zsets_meta_value.timestamp();
      } else if (s.IsNotFound()) {
        meta_not_found_ = true;
      } else {
        cur_key_ = "";
        Trace("Reserve[Get meta_key faild]");
        return false;
      }
    }

    if (meta_not_found_) {
      Trace("Drop[Meta key not exist]");
      return true;
    }

    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    if (cur_meta_timestamp_ != 0 &&
        cur_meta_timestamp_ < static_cast<int32_t>(unix_time)) {
      Trace("Drop[Timeout]");
      return true;
    }
    if (cur_meta_version_ > parsed_zsets_score_key.version()) {
      Trace("Drop[score_key_version < cur_meta_version]");
      return true;
    } else {
      Trace("Reserve[score_key_version == cur_meta_version]");
      return false;
    }
  }

  const char* Name() const override { return "ZSetsScoreFilter";}

 private:
  rocksdb::DB* db_;
  std::vector<rocksdb::ColumnFamilyHandle*>* cf_handles_ptr_;
  rocksdb::ReadOptions default_read_options_;
  mutable std::string cur_key_;
  mutable bool meta_not_found_;
  mutable int32_t cur_meta_version_;
  mutable int32_t cur_meta_timestamp_;
};

class ZSetsScoreFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  ZSetsScoreFilterFactory(rocksdb::DB** db_ptr,
      std::vector<rocksdb::ColumnFamilyHandle*>* handles_ptr)
    : db_ptr_(db_ptr), cf_handles_ptr_(handles_ptr) {}

  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context& context) override {
    return std::unique_ptr<rocksdb::CompactionFilter>(
        new ZSetsScoreFilter(*db_ptr_, cf_handles_ptr_));
  }

  const char* Name() const override {
    return "ZSetsScoreFilterFactory";
  }

 private:
  rocksdb::DB** db_ptr_;
  std::vector<rocksdb::ColumnFamilyHandle*>* cf_handles_ptr_;
};

}  //  namespace storage
#endif  // SRC_ZSETS_FILTER_H_
