//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_OPTIONS_HELPER_H
#define SRC_OPTIONS_HELPER_H

#include <rocksdb/options.h>

#include <cstddef>

namespace storage {

enum class MemberType {
  kInt,
  kUint,
  kUint64T,
  kSizeT,
  kUnknown,
};

struct MemberTypeInfo {
  int offset;
  MemberType type;
};

// offset_of is used to get the offset of a class data member with non
// standard-layout http://en.cppreference.com/w/cpp/concept/StandardLayoutType
// https://gist.github.com/graphitemaster/494f21190bb2c63c5516
template <typename T1, typename T2>
inline int offset_of(T1 T2::*member) {
  static T2 obj;
  return int(size_t(&(obj.*member)) - size_t(&obj));
}

static std::unordered_map<std::string, MemberTypeInfo>
    mutable_db_options_member_type_info = {
        {"max_background_jobs",
         {offsetof(struct rocksdb::DBOptions, max_background_jobs),
          MemberType::kInt}},
        {"max_background_compactions",
         {offsetof(struct rocksdb::DBOptions, max_background_compactions),
          MemberType::kInt}},
        // {"base_background_compactions", {offsetof(struct rocksdb::DBOptions,
        // base_background_compactions), MemberType::kInt}},
        {"max_open_files",
         {offsetof(struct rocksdb::DBOptions, max_open_files),
          MemberType::kInt}},
        {"bytes_per_sync",
         {offsetof(struct rocksdb::DBOptions, bytes_per_sync),
          MemberType::kUint64T}},
        {"delayed_write_rate",
         {offsetof(struct rocksdb::DBOptions, delayed_write_rate),
          MemberType::kUint64T}},
        {"max_total_wal_size",
         {offsetof(struct rocksdb::DBOptions, max_total_wal_size),
          MemberType::kUint64T}},
        {"wal_bytes_per_sync",
         {offsetof(struct rocksdb::DBOptions, wal_bytes_per_sync),
          MemberType::kUint64T}},
        {"stats_dump_period_sec",
         {offsetof(struct rocksdb::DBOptions, stats_dump_period_sec),
          MemberType::kUint}},
};

static std::unordered_map<std::string, MemberTypeInfo>
    mutable_cf_options_member_type_info = {
        {"max_write_buffer_number",
         {offset_of(&rocksdb::ColumnFamilyOptions::max_write_buffer_number),
          MemberType::kInt}},
        {"write_buffer_size",
         {offset_of(&rocksdb::ColumnFamilyOptions::write_buffer_size),
          MemberType::kSizeT}},
        {"target_file_size_base",
         {offset_of(&rocksdb::ColumnFamilyOptions::target_file_size_base),
          MemberType::kUint64T}},
        {"target_file_size_multiplier",
         {offset_of(&rocksdb::ColumnFamilyOptions::target_file_size_multiplier),
          MemberType::kInt}},
        {"arena_block_size",
         {offset_of(&rocksdb::ColumnFamilyOptions::arena_block_size),
          MemberType::kSizeT}},
        {"level0_file_num_compaction_trigger",
         {offset_of(&rocksdb::ColumnFamilyOptions::
                        level0_file_num_compaction_trigger),
          MemberType::kInt}},
        {"level0_slowdown_writes_trigger",
         {offset_of(
              &rocksdb::ColumnFamilyOptions::level0_slowdown_writes_trigger),
          MemberType::kInt}},
        {"level0_stop_writes_trigger",
         {offset_of(&rocksdb::ColumnFamilyOptions::level0_stop_writes_trigger),
          MemberType::kInt}},
        {"max_compaction_bytes",
         {offset_of(&rocksdb::ColumnFamilyOptions::max_compaction_bytes),
          MemberType::kUint64T}},
        {"soft_pending_compaction_bytes_limit",
         {offset_of(&rocksdb::ColumnFamilyOptions::
                        soft_pending_compaction_bytes_limit),
          MemberType::kUint64T}},
        {"hard_pending_compaction_bytes_limit",
         {offset_of(&rocksdb::ColumnFamilyOptions::
                        hard_pending_compaction_bytes_limit),
          MemberType::kUint64T}},
};

extern bool ParseOptionMember(const MemberType& member_type,
                              const std::string& value, char* member_address);

}  //  namespace storage
#endif  //  SRC_OPTIONS_HELPER_H
