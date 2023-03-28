//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_REDIS_HASHES_H_
#define SRC_REDIS_HASHES_H_

#include <string>
#include <vector>
#include <unordered_set>

#include "src/redis.h"

namespace storage {

class RedisHashes : public Redis {
 public:
  RedisHashes(Storage* const s, const DataType& type);
  ~RedisHashes() = default;

  // Common Commands
  Status Open(const StorageOptions& storage_options,
              const std::string& db_path) override;
  Status CompactRange(const rocksdb::Slice* begin,
                      const rocksdb::Slice* end,
                      const ColumnFamilyType& type = kMetaAndData) override;
  Status GetProperty(const std::string& property, uint64_t* out) override;
  Status ScanKeyNum(KeyInfo* key_info) override;
  Status ScanKeys(const std::string& pattern,
                  std::vector<std::string>* keys) override;
  Status PKPatternMatchDel(const std::string& pattern, int32_t* ret) override;

  // Hashes Commands
  Status HDel(const Slice& key, const std::vector<std::string>& fields,
              int32_t* ret);
  Status HExists(const Slice& key, const Slice& field);
  Status HGet(const Slice& key, const Slice& field, std::string* value);
  Status HGetall(const Slice& key,
                 std::vector<FieldValue>* fvs);
  Status HIncrby(const Slice& key, const Slice& field, int64_t value,
                 int64_t* ret);
  Status HIncrbyfloat(const Slice& key, const Slice& field,
                      const Slice& by, std::string* new_value);
  Status HKeys(const Slice& key,
               std::vector<std::string>* fields);
  Status HLen(const Slice& key, int32_t* ret);
  Status HMGet(const Slice& key, const std::vector<std::string>& fields,
               std::vector<ValueStatus>* vss);
  Status HMSet(const Slice& key,
               const std::vector<FieldValue>& fvs);
  Status HSet(const Slice& key, const Slice& field, const Slice& value,
              int32_t* ret);
  Status HSetnx(const Slice& key, const Slice& field, const Slice& value,
                int32_t* ret);
  Status HVals(const Slice& key,
               std::vector<std::string>* values);
  Status HStrlen(const Slice& key, const Slice& field, int32_t* len);
  Status HScan(const Slice& key, int64_t cursor,
               const std::string& pattern, int64_t count,
               std::vector<FieldValue>* field_values, int64_t* next_cursor);
  Status HScanx(const Slice& key, const std::string start_field,
                const std::string& pattern, int64_t count,
                std::vector<FieldValue>* field_values,
                std::string* next_field);
  Status PKHScanRange(const Slice& key,
                      const Slice& field_start,
                      const std::string& field_end,
                      const Slice& pattern, int32_t limit,
                      std::vector<FieldValue>* field_values,
                      std::string* next_field);
  Status PKHRScanRange(const Slice& key,
                       const Slice& field_start, const std::string& field_end,
                       const Slice& pattern, int32_t limit,
                       std::vector<FieldValue>* field_values,
                       std::string* next_field);
  Status PKScanRange(const Slice& key_start, const Slice& key_end,
                     const Slice& pattern, int32_t limit,
                     std::vector<std::string>* keys, std::string* next_key);
  Status PKRScanRange(const Slice& key_start, const Slice& key_end,
                      const Slice& pattern, int32_t limit,
                      std::vector<std::string>* keys, std::string* next_key);


  // Keys Commands
  Status Expire(const Slice& key, int32_t ttl) override;
  Status Del(const Slice& key) override;
  bool Scan(const std::string& start_key, const std::string& pattern,
            std::vector<std::string>* keys,
            int64_t* count, std::string* next_key) override;
  bool PKExpireScan(const std::string& start_key,
                    int32_t min_timestamp, int32_t max_timestamp,
                    std::vector<std::string>* keys,
                    int64_t* leftover_visits,
                    std::string* next_key) override;
  Status Expireat(const Slice& key, int32_t timestamp) override;
  Status Persist(const Slice& key) override;
  Status TTL(const Slice& key, int64_t* timestamp) override;

  // Iterate all data
  void ScanDatabase();
};

}  //  namespace storage
#endif  //  SRC_REDIS_HASHES_H_
