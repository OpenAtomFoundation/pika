//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_ZSETS_FILTER_H_
#define SRC_ZSETS_FILTER_H_

#include <memory>
#include <string>
#include <vector>

#include "rocksdb/compaction_filter.h"

#include "base_filter.h"
#include "base_meta_value_format.h"
#include "zsets_data_key_format.h"

namespace storage {

class ZSetsScoreFilter : public rocksdb::CompactionFilter {
 public:
  ZSetsScoreFilter(rocksdb::DB* db, std::vector<rocksdb::ColumnFamilyHandle*>* handles_ptr, int meta_cf_index)
      : db_(db), cf_handles_ptr_(handles_ptr), meta_cf_index_(meta_cf_index) {}

  bool Filter(int level, const rocksdb::Slice& key, const rocksdb::Slice& value, std::string* new_value,
              bool* value_changed) const override {
    UNUSED(level);
    UNUSED(value);
    UNUSED(new_value);
    UNUSED(value_changed);
    ParsedZSetsScoreKey parsed_zsets_score_key(key);
    TRACE("==========================START==========================");
    TRACE("[ScoreFilter], key: %s, score = %lf, member = %s, version = %ld",
          parsed_zsets_score_key.key().ToString().c_str(), parsed_zsets_score_key.score(),
          parsed_zsets_score_key.member().ToString().c_str(), parsed_zsets_score_key.Version());

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
        ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
        cur_meta_version_ = parsed_zsets_meta_value.Version();
        cur_meta_etime_ = parsed_zsets_meta_value.Etime();
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
    if (cur_meta_version_ > parsed_zsets_score_key.Version()) {
      TRACE("Drop[score_key_version < cur_meta_version]");
      return true;
    } else {
      TRACE("Reserve[score_key_version == cur_meta_version]");
      return false;
    }
  }

  /*
  // Only judge by meta value ttl
  virtual rocksdb::CompactionFilter::Decision FilterBlobByKey(int level, const Slice& key,
      std::string* new_value, std::string* skip_until) const {
    UNUSED(level);
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


  const char* Name() const override { return "ZSetsScoreFilter"; }

 private:
  rocksdb::DB* db_ = nullptr;
  std::vector<rocksdb::ColumnFamilyHandle*>* cf_handles_ptr_ = nullptr;
  rocksdb::ReadOptions default_read_options_;
  mutable std::string cur_key_;
  mutable bool meta_not_found_ = false;
  mutable uint64_t cur_meta_version_ = 0;
  mutable uint64_t cur_meta_etime_ = 0;
  int meta_cf_index_ = 0;
};

class ZSetsScoreFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  ZSetsScoreFilterFactory(rocksdb::DB** db_ptr, std::vector<rocksdb::ColumnFamilyHandle*>* handles_ptr, int meta_cf_index)
      : db_ptr_(db_ptr), cf_handles_ptr_(handles_ptr), meta_cf_index_(meta_cf_index) {}

  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context& context) override {
    return std::make_unique<ZSetsScoreFilter>(*db_ptr_, cf_handles_ptr_, meta_cf_index_);
  }

  const char* Name() const override { return "ZSetsScoreFilterFactory"; }

 private:
  rocksdb::DB** db_ptr_ = nullptr;
  std::vector<rocksdb::ColumnFamilyHandle*>* cf_handles_ptr_ = nullptr;
  int meta_cf_index_ = 0;
};

}  //  namespace storage
#endif  // SRC_ZSETS_FILTER_H_
