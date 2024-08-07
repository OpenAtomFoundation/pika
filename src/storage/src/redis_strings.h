//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_REDIS_STRINGS_H_
#define SRC_REDIS_STRINGS_H_

#include <algorithm>
#include <string>
#include <vector>
#include "src/redis.h"

namespace storage {

class RedisStrings : public Redis {
 public:
  RedisStrings(Storage* s, const DataType& type);
  ~RedisStrings() override = default;

  // Common Commands
  Status Open(const StorageOptions& storage_options, const std::string& db_path) override;
  Status CompactRange(const rocksdb::Slice* begin, const rocksdb::Slice* end,
                      const ColumnFamilyType& type = kMetaAndData) override;
  Status GetProperty(const std::string& property, uint64_t* out) override;
  Status ScanKeyNum(KeyInfo* key_info) override;
  Status ScanKeys(const std::string& pattern, std::vector<std::string>* keys) override;
  Status PKPatternMatchDelWithRemoveKeys(const DataType& data_type, const std::string& pattern, int64_t* ret, std::vector<std::string>* remove_keys, const int64_t& max_count) override;
  // Strings Command
  Status Append(const Slice& key, const Slice& value, int32_t* ret, int32_t* expired_timestamp_sec, std::string& out_new_value);
  Status BitCount(const Slice& key, int64_t start_offset, int64_t end_offset, int32_t* ret, bool have_range);
  Status BitOp(BitOpType op, const std::string& dest_key, const std::vector<std::string>& src_keys, std::string &value_to_dest, int64_t* ret);
  Status Decrby(const Slice& key, int64_t value, int64_t* ret);
  Status Get(const Slice& key, std::string* value);
  Status GetWithTTL(const Slice& key, std::string* value, int64_t* ttl);
  Status GetBit(const Slice& key, int64_t offset, int32_t* ret);
  Status Getrange(const Slice& key, int64_t start_offset, int64_t end_offset, std::string* ret);
  Status GetrangeWithValue(const Slice& key, int64_t start_offset, int64_t end_offset, std::string* ret, std::string* value, int64_t* ttl);
  Status GetSet(const Slice& key, const Slice& value, std::string* old_value);
  Status Incrby(const Slice& key, int64_t value, int64_t* ret, int32_t* expired_timestamp_sec);
  Status Incrbyfloat(const Slice& key, const Slice& value, std::string* ret, int32_t* expired_timestamp_sec);
  Status MGet(const std::vector<std::string>& keys, std::vector<ValueStatus>* vss);
  Status MGetWithTTL(const std::vector<std::string>& keys, std::vector<ValueStatus>* vss);
  Status MSet(const std::vector<KeyValue>& kvs);
  Status MSetnx(const std::vector<KeyValue>& kvs, int32_t* ret);
  Status Set(const Slice& key, const Slice& value);
  Status Setxx(const Slice& key, const Slice& value, int32_t* ret, int32_t ttl = 0);
  Status SetBit(const Slice& key, int64_t offset, int32_t value, int32_t* ret);
  Status Setex(const Slice& key, const Slice& value, int32_t ttl);
  Status Setnx(const Slice& key, const Slice& value, int32_t* ret, int32_t ttl = 0);
  Status Setvx(const Slice& key, const Slice& value, const Slice& new_value, int32_t* ret, int32_t ttl = 0);
  Status Delvx(const Slice& key, const Slice& value, int32_t* ret);
  Status Setrange(const Slice& key, int64_t start_offset, const Slice& value, int32_t* ret);
  Status Strlen(const Slice& key, int32_t* len);

  Status BitPos(const Slice& key, int32_t bit, int64_t* ret);
  Status BitPos(const Slice& key, int32_t bit, int64_t start_offset, int64_t* ret);
  Status BitPos(const Slice& key, int32_t bit, int64_t start_offset, int64_t end_offset, int64_t* ret);
  Status PKSetexAt(const Slice& key, const Slice& value, int32_t timestamp);
  Status PKScanRange(const Slice& key_start, const Slice& key_end, const Slice& pattern, int32_t limit,
                     std::vector<KeyValue>* kvs, std::string* next_key);
  Status PKRScanRange(const Slice& key_start, const Slice& key_end, const Slice& pattern, int32_t limit,
                      std::vector<KeyValue>* kvs, std::string* next_key);

  // Keys Commands
  Status Expire(const Slice& key, int32_t ttl) override;
  Status Del(const Slice& key) override;
  bool Scan(const std::string& start_key, const std::string& pattern, std::vector<std::string>* keys, int64_t* count,
            std::string* next_key) override;
  bool PKExpireScan(const std::string& start_key, int32_t min_timestamp, int32_t max_timestamp,
                    std::vector<std::string>* keys, int64_t* leftover_visits, std::string* next_key) override;
  Status Expireat(const Slice& key, int32_t timestamp) override;
  Status Persist(const Slice& key) override;
  Status TTL(const Slice& key, int64_t* timestamp) override;

  // Iterate all data
  void ScanDatabase();
};

}  //  namespace storage
#endif  //  SRC_REDIS_STRINGS_H_
