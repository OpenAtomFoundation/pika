//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_REDIS_SETS_H_
#define SRC_REDIS_SETS_H_

#include <string>
#include <unordered_set>
#include <vector>

#include "src/custom_comparator.h"
#include "src/lru_cache.h"
#include "src/redis.h"

namespace storage {

class RedisSets : public Redis {
 public:
  RedisSets(Storage* s, const DataType& type);
  ~RedisSets() override;

  // Common Commands
  Status Open(const StorageOptions& storage_options, const std::string& db_path) override;
  Status CompactRange(const rocksdb::Slice* begin, const rocksdb::Slice* end,
                      const ColumnFamilyType& type = kMetaAndData) override;
  Status GetProperty(const std::string& property, uint64_t* out) override;
  Status ScanKeyNum(KeyInfo* key_info) override;
  Status ScanKeys(const std::string& pattern, std::vector<std::string>* keys) override;
  Status PKPatternMatchDel(const std::string& pattern, int32_t* ret) override;

  // Setes Commands
  Status SAdd(const Slice& key, const std::vector<std::string>& members, int32_t* ret);
  Status SCard(const Slice& key, int32_t* ret);
  Status SDiff(const std::vector<std::string>& keys, std::vector<std::string>* members);
  Status SDiffstore(const Slice& destination, const std::vector<std::string>& keys, std::vector<std::string>& value_to_dest, int32_t* ret);
  Status SInter(const std::vector<std::string>& keys, std::vector<std::string>* members);
  Status SInterstore(const Slice& destination, const std::vector<std::string>& keys, std::vector<std::string>& value_to_dest, int32_t* ret);
  Status SIsmember(const Slice& key, const Slice& member, int32_t* ret);
  Status SMembers(const Slice& key, std::vector<std::string>* members);
  Status SMembersWithTTL(const Slice& key, std::vector<std::string>* members, int64_t* ttl);
  Status SMove(const Slice& source, const Slice& destination, const Slice& member, int32_t* ret);
  Status SPop(const Slice& key, std::vector<std::string>* members, int64_t cnt);
  Status SRandmember(const Slice& key, int32_t count, std::vector<std::string>* members);
  Status SRem(const Slice& key, const std::vector<std::string>& members, int32_t* ret);
  Status SUnion(const std::vector<std::string>& keys, std::vector<std::string>* members);
  Status SUnionstore(const Slice& destination, const std::vector<std::string>& keys, std::vector<std::string>& value_to_dest, int32_t* ret);
  Status SScan(const Slice& key, int64_t cursor, const std::string& pattern, int64_t count,
               std::vector<std::string>* members, int64_t* next_cursor);
  Status PKScanRange(const Slice& key_start, const Slice& key_end, const Slice& pattern, int32_t limit,
                     std::vector<std::string>* keys, std::string* next_key);
  Status PKRScanRange(const Slice& key_start, const Slice& key_end, const Slice& pattern, int32_t limit,
                      std::vector<std::string>* keys, std::string* next_key);

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
#endif  //  SRC_REDIS_SETS_H_
