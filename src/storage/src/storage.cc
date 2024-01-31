//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "storage/storage.h"
#include "storage/util.h"

#include <glog/logging.h>

#include <utility>

#include "scope_snapshot.h"
#include "src/lru_cache.h"
#include "src/mutex_impl.h"
#include "src/options_helper.h"
#include "src/redis_hashes.h"
#include "src/redis_hyperloglog.h"
#include "src/redis_lists.h"
#include "src/redis_sets.h"
#include "src/redis_strings.h"
#include "src/redis_zsets.h"

namespace storage {

Status StorageOptions::ResetOptions(const OptionType& option_type,
                                    const std::unordered_map<std::string, std::string>& options_map) {
  std::unordered_map<std::string, MemberTypeInfo>& options_member_type_info = mutable_cf_options_member_type_info;
  char* opt = reinterpret_cast<char*>(static_cast<rocksdb::ColumnFamilyOptions*>(&options));
  if (option_type == OptionType::kDB) {
    options_member_type_info = mutable_db_options_member_type_info;
    opt = reinterpret_cast<char*>(static_cast<rocksdb::DBOptions*>(&options));
  }
  for (const auto& option_member : options_map) {
    try {
      auto iter = options_member_type_info.find(option_member.first);
      if (iter == options_member_type_info.end()) {
        return Status::InvalidArgument("Unsupport option member: " + option_member.first);
      }
      const auto& member_info = iter->second;
      if (!ParseOptionMember(member_info.type, option_member.second, opt + member_info.offset)) {
        return Status::InvalidArgument("Error parsing option member " + option_member.first);
      }
    } catch (std::exception& e) {
      return Status::InvalidArgument("Error parsing option member " + option_member.first + ":" +
                                     std::string(e.what()));
    }
  }
  return Status::OK();
}

Storage::Storage() {
  cursors_store_ = std::make_unique<LRUCache<std::string, std::string>>();
  cursors_store_->SetCapacity(5000);

  Status s = StartBGThread();
  if (!s.ok()) {
    LOG(FATAL) << "start bg thread failed, " << s.ToString();
  }
}

Storage::~Storage() {
  bg_tasks_should_exit_ = true;
  bg_tasks_cond_var_.notify_one();

  if (is_opened_) {
    rocksdb::CancelAllBackgroundWork(strings_db_->GetDB(), true);
    rocksdb::CancelAllBackgroundWork(hashes_db_->GetDB(), true);
    rocksdb::CancelAllBackgroundWork(sets_db_->GetDB(), true);
    rocksdb::CancelAllBackgroundWork(lists_db_->GetDB(), true);
    rocksdb::CancelAllBackgroundWork(zsets_db_->GetDB(), true);
  }

  int ret = 0;
  if ((ret = pthread_join(bg_tasks_thread_id_, nullptr)) != 0) {
    LOG(ERROR) << "pthread_join failed with bgtask thread error " << ret;
  }
}

static std::string AppendSubDirectory(const std::string& db_path, const std::string& sub_db) {
  if (db_path.back() == '/') {
    return db_path + sub_db;
  } else {
    return db_path + "/" + sub_db;
  }
}

Status Storage::Open(const StorageOptions& storage_options, const std::string& db_path) {
  mkpath(db_path.c_str(), 0755);

  strings_db_ = std::make_unique<RedisStrings>(this, kStrings);
  Status s = strings_db_->Open(storage_options, AppendSubDirectory(db_path, "strings"));
  if (!s.ok()) {
    LOG(FATAL) << "open kv db failed, " << s.ToString();
  }

  hashes_db_ = std::make_unique<RedisHashes>(this, kHashes);
  s = hashes_db_->Open(storage_options, AppendSubDirectory(db_path, "hashes"));
  if (!s.ok()) {
    LOG(FATAL) << "open hashes db failed, " << s.ToString();
  }

  sets_db_ = std::make_unique<RedisSets>(this, kSets);
  s = sets_db_->Open(storage_options, AppendSubDirectory(db_path, "sets"));
  if (!s.ok()) {
    LOG(FATAL) << "open set db failed, " << s.ToString();
  }

  lists_db_ = std::make_unique<RedisLists>(this, kLists);
  s = lists_db_->Open(storage_options, AppendSubDirectory(db_path, "lists"));
  if (!s.ok()) {
    LOG(FATAL) << "open list db failed, " << s.ToString();
  }

  zsets_db_ = std::make_unique<RedisZSets>(this, kZSets);
  s = zsets_db_->Open(storage_options, AppendSubDirectory(db_path, "zsets"));
  if (!s.ok()) {
    LOG(FATAL) << "open zset db failed, " << s.ToString();
  }
  is_opened_.store(true);
  return Status::OK();
}

Status Storage::GetStartKey(const DataType& dtype, int64_t cursor, std::string* start_key) {
  std::string index_key = DataTypeTag[dtype] + std::to_string(cursor);
  return cursors_store_->Lookup(index_key, start_key);
}

Status Storage::StoreCursorStartKey(const DataType& dtype, int64_t cursor, const std::string& next_key) {
  std::string index_key = DataTypeTag[dtype] + std::to_string(cursor);
  return cursors_store_->Insert(index_key, next_key);
}

// Strings Commands
Status Storage::Set(const Slice& key, const Slice& value) { return strings_db_->Set(key, value); }

Status Storage::Setxx(const Slice& key, const Slice& value, int32_t* ret, const int32_t ttl) {
  return strings_db_->Setxx(key, value, ret, ttl);
}

Status Storage::Get(const Slice& key, std::string* value) { return strings_db_->Get(key, value); }

Status Storage::GetWithTTL(const Slice& key, std::string* value, int64_t* ttl) {
  return strings_db_->GetWithTTL(key, value, ttl);
}

Status Storage::GetSet(const Slice& key, const Slice& value, std::string* old_value) {
  return strings_db_->GetSet(key, value, old_value);
}

Status Storage::SetBit(const Slice& key, int64_t offset, int32_t value, int32_t* ret) {
  return strings_db_->SetBit(key, offset, value, ret);
}

Status Storage::GetBit(const Slice& key, int64_t offset, int32_t* ret) { return strings_db_->GetBit(key, offset, ret); }

Status Storage::MSet(const std::vector<KeyValue>& kvs) { return strings_db_->MSet(kvs); }

Status Storage::MGet(const std::vector<std::string>& keys, std::vector<ValueStatus>* vss) {
  return strings_db_->MGet(keys, vss);
}

Status Storage::MGetWithTTL(const std::vector<std::string>& keys, std::vector<ValueStatus>* vss) {
  return strings_db_->MGetWithTTL(keys, vss);
}

Status Storage::Setnx(const Slice& key, const Slice& value, int32_t* ret, const int32_t ttl) {
  return strings_db_->Setnx(key, value, ret, ttl);
}

Status Storage::MSetnx(const std::vector<KeyValue>& kvs, int32_t* ret) { return strings_db_->MSetnx(kvs, ret); }

Status Storage::Setvx(const Slice& key, const Slice& value, const Slice& new_value, int32_t* ret, const int32_t ttl) {
  return strings_db_->Setvx(key, value, new_value, ret, ttl);
}

Status Storage::Delvx(const Slice& key, const Slice& value, int32_t* ret) {
  return strings_db_->Delvx(key, value, ret);
}

Status Storage::Setrange(const Slice& key, int64_t start_offset, const Slice& value, int32_t* ret) {
  return strings_db_->Setrange(key, start_offset, value, ret);
}

Status Storage::Getrange(const Slice& key, int64_t start_offset, int64_t end_offset, std::string* ret) {
  return strings_db_->Getrange(key, start_offset, end_offset, ret);
}

Status Storage::GetrangeWithValue(const Slice& key, int64_t start_offset, int64_t end_offset,
                                     std::string* ret, std::string* value, int64_t* ttl) {
  return strings_db_->GetrangeWithValue(key, start_offset, end_offset, ret, value, ttl);
}

Status Storage::Append(const Slice& key, const Slice& value, int32_t* ret) {
  return strings_db_->Append(key, value, ret);
}

Status Storage::BitCount(const Slice& key, int64_t start_offset, int64_t end_offset, int32_t* ret, bool have_range) {
  return strings_db_->BitCount(key, start_offset, end_offset, ret, have_range);
}

Status Storage::BitOp(BitOpType op, const std::string& dest_key, const std::vector<std::string>& src_keys,
                      std::string &value_to_dest, int64_t* ret) {
  return strings_db_->BitOp(op, dest_key, src_keys, value_to_dest, ret);
}

Status Storage::BitPos(const Slice& key, int32_t bit, int64_t* ret) { return strings_db_->BitPos(key, bit, ret); }

Status Storage::BitPos(const Slice& key, int32_t bit, int64_t start_offset, int64_t* ret) {
  return strings_db_->BitPos(key, bit, start_offset, ret);
}

Status Storage::BitPos(const Slice& key, int32_t bit, int64_t start_offset, int64_t end_offset, int64_t* ret) {
  return strings_db_->BitPos(key, bit, start_offset, end_offset, ret);
}

Status Storage::Decrby(const Slice& key, int64_t value, int64_t* ret) { return strings_db_->Decrby(key, value, ret); }

Status Storage::Incrby(const Slice& key, int64_t value, int64_t* ret) { return strings_db_->Incrby(key, value, ret); }

Status Storage::Incrbyfloat(const Slice& key, const Slice& value, std::string* ret) {
  return strings_db_->Incrbyfloat(key, value, ret);
}

Status Storage::Setex(const Slice& key, const Slice& value, int32_t ttl) { return strings_db_->Setex(key, value, ttl); }

Status Storage::Strlen(const Slice& key, int32_t* len) { return strings_db_->Strlen(key, len); }

Status Storage::PKSetexAt(const Slice& key, const Slice& value, int32_t timestamp) {
  return strings_db_->PKSetexAt(key, value, timestamp);
}

// Hashes Commands
Status Storage::HSet(const Slice& key, const Slice& field, const Slice& value, int32_t* res) {
  return hashes_db_->HSet(key, field, value, res);
}

Status Storage::HGet(const Slice& key, const Slice& field, std::string* value) {
  return hashes_db_->HGet(key, field, value);
}

Status Storage::HMSet(const Slice& key, const std::vector<FieldValue>& fvs) { return hashes_db_->HMSet(key, fvs); }

Status Storage::HMGet(const Slice& key, const std::vector<std::string>& fields, std::vector<ValueStatus>* vss) {
  return hashes_db_->HMGet(key, fields, vss);
}

Status Storage::HGetall(const Slice& key, std::vector<FieldValue>* fvs) { return hashes_db_->HGetall(key, fvs); }

Status Storage::HGetallWithTTL(const Slice& key, std::vector<FieldValue>* fvs, int64_t* ttl) {
  return hashes_db_->HGetallWithTTL(key, fvs, ttl);
}

Status Storage::HKeys(const Slice& key, std::vector<std::string>* fields) { return hashes_db_->HKeys(key, fields); }

Status Storage::HVals(const Slice& key, std::vector<std::string>* values) { return hashes_db_->HVals(key, values); }

Status Storage::HSetnx(const Slice& key, const Slice& field, const Slice& value, int32_t* ret) {
  return hashes_db_->HSetnx(key, field, value, ret);
}

Status Storage::HLen(const Slice& key, int32_t* ret) { return hashes_db_->HLen(key, ret); }

Status Storage::HStrlen(const Slice& key, const Slice& field, int32_t* len) {
  return hashes_db_->HStrlen(key, field, len);
}

Status Storage::HExists(const Slice& key, const Slice& field) { return hashes_db_->HExists(key, field); }

Status Storage::HIncrby(const Slice& key, const Slice& field, int64_t value, int64_t* ret) {
  return hashes_db_->HIncrby(key, field, value, ret);
}

Status Storage::HIncrbyfloat(const Slice& key, const Slice& field, const Slice& by, std::string* new_value) {
  return hashes_db_->HIncrbyfloat(key, field, by, new_value);
}

Status Storage::HDel(const Slice& key, const std::vector<std::string>& fields, int32_t* ret) {
  return hashes_db_->HDel(key, fields, ret);
}

Status Storage::HScan(const Slice& key, int64_t cursor, const std::string& pattern, int64_t count,
                      std::vector<FieldValue>* field_values, int64_t* next_cursor) {
  return hashes_db_->HScan(key, cursor, pattern, count, field_values, next_cursor);
}

Status Storage::HScanx(const Slice& key, const std::string& start_field, const std::string& pattern, int64_t count,
                       std::vector<FieldValue>* field_values, std::string* next_field) {
  return hashes_db_->HScanx(key, start_field, pattern, count, field_values, next_field);
}

Status Storage::PKHScanRange(const Slice& key, const Slice& field_start, const std::string& field_end,
                             const Slice& pattern, int32_t limit, std::vector<FieldValue>* field_values,
                             std::string* next_field) {
  return hashes_db_->PKHScanRange(key, field_start, field_end, pattern, limit, field_values, next_field);
}

Status Storage::PKHRScanRange(const Slice& key, const Slice& field_start, const std::string& field_end,
                              const Slice& pattern, int32_t limit, std::vector<FieldValue>* field_values,
                              std::string* next_field) {
  return hashes_db_->PKHRScanRange(key, field_start, field_end, pattern, limit, field_values, next_field);
}

// Sets Commands
Status Storage::SAdd(const Slice& key, const std::vector<std::string>& members, int32_t* ret) {
  return sets_db_->SAdd(key, members, ret);
}

Status Storage::SCard(const Slice& key, int32_t* ret) { return sets_db_->SCard(key, ret); }

Status Storage::SDiff(const std::vector<std::string>& keys, std::vector<std::string>* members) {
  return sets_db_->SDiff(keys, members);
}

Status Storage::SDiffstore(const Slice& destination, const std::vector<std::string>& keys, std::vector<std::string>& value_to_dest, int32_t* ret) {
  return sets_db_->SDiffstore(destination, keys, value_to_dest, ret);
}

Status Storage::SInter(const std::vector<std::string>& keys, std::vector<std::string>* members) {
  return sets_db_->SInter(keys, members);
}

Status Storage::SInterstore(const Slice& destination, const std::vector<std::string>& keys, std::vector<std::string>& value_to_dest, int32_t* ret) {
  return sets_db_->SInterstore(destination, keys, value_to_dest, ret);
}

Status Storage::SIsmember(const Slice& key, const Slice& member, int32_t* ret) {
  return sets_db_->SIsmember(key, member, ret);
}

Status Storage::SMembers(const Slice& key, std::vector<std::string>* members) {
  return sets_db_->SMembers(key, members);
}

Status Storage::SMembersWithTTL(const Slice& key, std::vector<std::string>* members, int64_t *ttl) {
  return sets_db_->SMembersWithTTL(key, members, ttl);
}

Status Storage::SMove(const Slice& source, const Slice& destination, const Slice& member, int32_t* ret) {
  return sets_db_->SMove(source, destination, member, ret);
}

Status Storage::SPop(const Slice& key, std::vector<std::string>* members, int64_t count) {
  Status status = sets_db_->SPop(key, members, count);
  return status;
}

Status Storage::SRandmember(const Slice& key, int32_t count, std::vector<std::string>* members) {
  return sets_db_->SRandmember(key, count, members);
}

Status Storage::SRem(const Slice& key, const std::vector<std::string>& members, int32_t* ret) {
  return sets_db_->SRem(key, members, ret);
}

Status Storage::SUnion(const std::vector<std::string>& keys, std::vector<std::string>* members) {
  return sets_db_->SUnion(keys, members);
}

Status Storage::SUnionstore(const Slice& destination, const std::vector<std::string>& keys, std::vector<std::string>& value_to_dest, int32_t* ret) {
  return sets_db_->SUnionstore(destination, keys, value_to_dest, ret);
}

Status Storage::SScan(const Slice& key, int64_t cursor, const std::string& pattern, int64_t count,
                      std::vector<std::string>* members, int64_t* next_cursor) {
  return sets_db_->SScan(key, cursor, pattern, count, members, next_cursor);
}

Status Storage::LPush(const Slice& key, const std::vector<std::string>& values, uint64_t* ret) {
  return lists_db_->LPush(key, values, ret);
}

Status Storage::RPush(const Slice& key, const std::vector<std::string>& values, uint64_t* ret) {
  return lists_db_->RPush(key, values, ret);
}

Status Storage::LRange(const Slice& key, int64_t start, int64_t stop, std::vector<std::string>* ret) {
  return lists_db_->LRange(key, start, stop, ret);
}

Status Storage::LRangeWithTTL(const Slice& key, int64_t start, int64_t stop, std::vector<std::string>* ret, int64_t *ttl) {
  return lists_db_->LRangeWithTTL(key, start, stop, ret, ttl);
}

Status Storage::LTrim(const Slice& key, int64_t start, int64_t stop) { return lists_db_->LTrim(key, start, stop); }

Status Storage::LLen(const Slice& key, uint64_t* len) { return lists_db_->LLen(key, len); }

Status Storage::LPop(const Slice& key, int64_t count, std::vector<std::string>* elements) { return lists_db_->LPop(key, count, elements); }

Status Storage::RPop(const Slice& key, int64_t count, std::vector<std::string>* elements) { return lists_db_->RPop(key, count, elements); }

Status Storage::LIndex(const Slice& key, int64_t index, std::string* element) {
  return lists_db_->LIndex(key, index, element);
}

Status Storage::LInsert(const Slice& key, const BeforeOrAfter& before_or_after, const std::string& pivot,
                        const std::string& value, int64_t* ret) {
  return lists_db_->LInsert(key, before_or_after, pivot, value, ret);
}

Status Storage::LPushx(const Slice& key, const std::vector<std::string>& values, uint64_t* len) {
  return lists_db_->LPushx(key, values, len);
}

Status Storage::RPushx(const Slice& key, const std::vector<std::string>& values, uint64_t* len) {
  return lists_db_->RPushx(key, values, len);
}

Status Storage::LRem(const Slice& key, int64_t count, const Slice& value, uint64_t* ret) {
  return lists_db_->LRem(key, count, value, ret);
}

Status Storage::LSet(const Slice& key, int64_t index, const Slice& value) { return lists_db_->LSet(key, index, value); }

Status Storage::RPoplpush(const Slice& source, const Slice& destination, std::string* element) {
  return lists_db_->RPoplpush(source, destination, element);
}

Status Storage::ZPopMax(const Slice& key, const int64_t count, std::vector<ScoreMember>* score_members) {
  return zsets_db_->ZPopMax(key, count, score_members);
}

Status Storage::ZPopMin(const Slice& key, const int64_t count, std::vector<ScoreMember>* score_members) {
  return zsets_db_->ZPopMin(key, count, score_members);
}

Status Storage::ZAdd(const Slice& key, const std::vector<ScoreMember>& score_members, int32_t* ret) {
  return zsets_db_->ZAdd(key, score_members, ret);
}

Status Storage::ZCard(const Slice& key, int32_t* ret) { return zsets_db_->ZCard(key, ret); }

Status Storage::ZCount(const Slice& key, double min, double max, bool left_close, bool right_close, int32_t* ret) {
  return zsets_db_->ZCount(key, min, max, left_close, right_close, ret);
}

Status Storage::ZIncrby(const Slice& key, const Slice& member, double increment, double* ret) {
  return zsets_db_->ZIncrby(key, member, increment, ret);
}

Status Storage::ZRange(const Slice& key, int32_t start, int32_t stop, std::vector<ScoreMember>* score_members) {
  return zsets_db_->ZRange(key, start, stop, score_members);
}
Status Storage::ZRangeWithTTL(const Slice& key, int32_t start, int32_t stop, std::vector<ScoreMember>* score_members,
                                 int64_t *ttl) {
  return zsets_db_->ZRangeWithTTL(key, start, stop, score_members, ttl);
}

Status Storage::ZRangebyscore(const Slice& key, double min, double max, bool left_close, bool right_close,
                              std::vector<ScoreMember>* score_members) {
  // maximum number of zset is std::numeric_limits<int32_t>::max()
  return zsets_db_->ZRangebyscore(key, min, max, left_close, right_close, std::numeric_limits<int32_t>::max(), 0,
                                  score_members);
}

Status Storage::ZRangebyscore(const Slice& key, double min, double max, bool left_close, bool right_close,
                              int64_t count, int64_t offset, std::vector<ScoreMember>* score_members) {
  return zsets_db_->ZRangebyscore(key, min, max, left_close, right_close, count, offset, score_members);
}

Status Storage::ZRank(const Slice& key, const Slice& member, int32_t* rank) {
  return zsets_db_->ZRank(key, member, rank);
}

Status Storage::ZRem(const Slice& key, const std::vector<std::string>& members, int32_t* ret) {
  return zsets_db_->ZRem(key, members, ret);
}

Status Storage::ZRemrangebyrank(const Slice& key, int32_t start, int32_t stop, int32_t* ret) {
  return zsets_db_->ZRemrangebyrank(key, start, stop, ret);
}

Status Storage::ZRemrangebyscore(const Slice& key, double min, double max, bool left_close, bool right_close,
                                 int32_t* ret) {
  return zsets_db_->ZRemrangebyscore(key, min, max, left_close, right_close, ret);
}

Status Storage::ZRevrangebyscore(const Slice& key, double min, double max, bool left_close, bool right_close,
                                 int64_t count, int64_t offset, std::vector<ScoreMember>* score_members) {
  return zsets_db_->ZRevrangebyscore(key, min, max, left_close, right_close, count, offset, score_members);
}

Status Storage::ZRevrange(const Slice& key, int32_t start, int32_t stop, std::vector<ScoreMember>* score_members) {
  return zsets_db_->ZRevrange(key, start, stop, score_members);
}

Status Storage::ZRevrangebyscore(const Slice& key, double min, double max, bool left_close, bool right_close,
                                 std::vector<ScoreMember>* score_members) {
  // maximum number of zset is std::numeric_limits<int32_t>::max()
  return zsets_db_->ZRevrangebyscore(key, min, max, left_close, right_close, std::numeric_limits<int32_t>::max(), 0,
                                     score_members);
}

Status Storage::ZRevrank(const Slice& key, const Slice& member, int32_t* rank) {
  return zsets_db_->ZRevrank(key, member, rank);
}

Status Storage::ZScore(const Slice& key, const Slice& member, double* ret) {
  return zsets_db_->ZScore(key, member, ret);
}

Status Storage::ZUnionstore(const Slice& destination, const std::vector<std::string>& keys,
                            const std::vector<double>& weights, const AGGREGATE agg, std::map<std::string, double>& value_to_dest, int32_t* ret) {
  return zsets_db_->ZUnionstore(destination, keys, weights, agg, value_to_dest, ret);
}

Status Storage::ZInterstore(const Slice& destination, const std::vector<std::string>& keys,
                            const std::vector<double>& weights, const AGGREGATE agg, std::vector<ScoreMember>& value_to_dest, int32_t* ret) {
  return zsets_db_->ZInterstore(destination, keys, weights, agg, value_to_dest, ret);
}

Status Storage::ZRangebylex(const Slice& key, const Slice& min, const Slice& max, bool left_close, bool right_close,
                            std::vector<std::string>* members) {
  return zsets_db_->ZRangebylex(key, min, max, left_close, right_close, members);
}

Status Storage::ZLexcount(const Slice& key, const Slice& min, const Slice& max, bool left_close, bool right_close,
                          int32_t* ret) {
  return zsets_db_->ZLexcount(key, min, max, left_close, right_close, ret);
}

Status Storage::ZRemrangebylex(const Slice& key, const Slice& min, const Slice& max, bool left_close, bool right_close,
                               int32_t* ret) {
  return zsets_db_->ZRemrangebylex(key, min, max, left_close, right_close, ret);
}

Status Storage::ZScan(const Slice& key, int64_t cursor, const std::string& pattern, int64_t count,
                      std::vector<ScoreMember>* score_members, int64_t* next_cursor) {
  return zsets_db_->ZScan(key, cursor, pattern, count, score_members, next_cursor);
}

// Keys Commands
int32_t Storage::Expire(const Slice& key, int32_t ttl, std::map<DataType, Status>* type_status) {
  int32_t ret = 0;
  bool is_corruption = false;

  // Strings
  Status s = strings_db_->Expire(key, ttl);
  if (s.ok()) {
    ret++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kStrings] = s;
  }

  // Hash
  s = hashes_db_->Expire(key, ttl);
  if (s.ok()) {
    ret++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kHashes] = s;
  }

  // Sets
  s = sets_db_->Expire(key, ttl);
  if (s.ok()) {
    ret++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kSets] = s;
  }

  // Lists
  s = lists_db_->Expire(key, ttl);
  if (s.ok()) {
    ret++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kLists] = s;
  }

  // Zsets
  s = zsets_db_->Expire(key, ttl);
  if (s.ok()) {
    ret++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kZSets] = s;
  }

  if (is_corruption) {
    return -1;
  } else {
    return ret;
  }
}

int64_t Storage::Del(const std::vector<std::string>& keys, std::map<DataType, Status>* type_status) {
  Status s;
  int64_t count = 0;
  bool is_corruption = false;

  for (const auto& key : keys) {
    // Strings
    Status s = strings_db_->Del(key);
    if (s.ok()) {
      count++;
    } else if (!s.IsNotFound()) {
      is_corruption = true;
      (*type_status)[DataType::kStrings] = s;
    }

    // Hashes
    s = hashes_db_->Del(key);
    if (s.ok()) {
      count++;
    } else if (!s.IsNotFound()) {
      is_corruption = true;
      (*type_status)[DataType::kHashes] = s;
    }

    // Sets
    s = sets_db_->Del(key);
    if (s.ok()) {
      count++;
    } else if (!s.IsNotFound()) {
      is_corruption = true;
      (*type_status)[DataType::kSets] = s;
    }

    // Lists
    s = lists_db_->Del(key);
    if (s.ok()) {
      count++;
    } else if (!s.IsNotFound()) {
      is_corruption = true;
      (*type_status)[DataType::kLists] = s;
    }

    // ZSets
    s = zsets_db_->Del(key);
    if (s.ok()) {
      count++;
    } else if (!s.IsNotFound()) {
      is_corruption = true;
      (*type_status)[DataType::kZSets] = s;
    }
  }

  if (is_corruption) {
    return -1;
  } else {
    return count;
  }
}

int64_t Storage::DelByType(const std::vector<std::string>& keys, const DataType& type) {
  Status s;
  int64_t count = 0;
  bool is_corruption = false;

  for (const auto& key : keys) {
    switch (type) {
      // Strings
      case DataType::kStrings: {
        s = strings_db_->Del(key);
        if (s.ok()) {
          count++;
        } else if (!s.IsNotFound()) {
          is_corruption = true;
        }
        break;
      }
      // Hashes
      case DataType::kHashes: {
        s = hashes_db_->Del(key);
        if (s.ok()) {
          count++;
        } else if (!s.IsNotFound()) {
          is_corruption = true;
        }
        break;
      }
      // Sets
      case DataType::kSets: {
        s = sets_db_->Del(key);
        if (s.ok()) {
          count++;
        } else if (!s.IsNotFound()) {
          is_corruption = true;
        }
        break;
      }
      // Lists
      case DataType::kLists: {
        s = lists_db_->Del(key);
        if (s.ok()) {
          count++;
        } else if (!s.IsNotFound()) {
          is_corruption = true;
        }
        break;
      }
      // ZSets
      case DataType::kZSets: {
        s = zsets_db_->Del(key);
        if (s.ok()) {
          count++;
        } else if (!s.IsNotFound()) {
          is_corruption = true;
        }
        break;
      }
      case DataType::kAll: {
        return -1;
      }
    }
  }

  if (is_corruption) {
    return -1;
  } else {
    return count;
  }
}

int64_t Storage::Exists(const std::vector<std::string>& keys, std::map<DataType, Status>* type_status) {
  int64_t count = 0;
  int32_t ret;
  uint64_t llen;
  std::string value;
  Status s;
  bool is_corruption = false;

  for (const auto& key : keys) {
    s = strings_db_->Get(key, &value);
    if (s.ok()) {
      count++;
    } else if (!s.IsNotFound()) {
      is_corruption = true;
      (*type_status)[DataType::kStrings] = s;
    }

    s = hashes_db_->HLen(key, &ret);
    if (s.ok()) {
      count++;
    } else if (!s.IsNotFound()) {
      is_corruption = true;
      (*type_status)[DataType::kHashes] = s;
    }

    s = sets_db_->SCard(key, &ret);
    if (s.ok()) {
      count++;
    } else if (!s.IsNotFound()) {
      is_corruption = true;
      (*type_status)[DataType::kSets] = s;
    }

    s = lists_db_->LLen(key, &llen);
    if (s.ok()) {
      count++;
    } else if (!s.IsNotFound()) {
      is_corruption = true;
      (*type_status)[DataType::kLists] = s;
    }

    s = zsets_db_->ZCard(key, &ret);
    if (s.ok()) {
      count++;
    } else if (!s.IsNotFound()) {
      is_corruption = true;
      (*type_status)[DataType::kZSets] = s;
    }
  }

  if (is_corruption) {
    return -1;
  } else {
    return count;
  }
}

int64_t Storage::Scan(const DataType& dtype, int64_t cursor, const std::string& pattern, int64_t count,
                      std::vector<std::string>* keys) {
  keys->clear();
  bool is_finish;
  int64_t leftover_visits = count;
  int64_t step_length = count;
  int64_t cursor_ret = 0;
  std::string start_key;
  std::string next_key;
  std::string prefix;

  prefix = isTailWildcard(pattern) ? pattern.substr(0, pattern.size() - 1) : "";

  if (cursor < 0) {
    return cursor_ret;
  } else {
    Status s = GetStartKey(dtype, cursor, &start_key);
    if (s.IsNotFound()) {
      // If want to scan all the databases, we start with the strings database
      start_key = (dtype == DataType::kAll ? DataTypeTag[kStrings] : DataTypeTag[dtype]) + prefix;
      cursor = 0;
    }
  }

  char key_type = start_key.at(0);
  start_key.erase(start_key.begin());
  switch (key_type) {
    case 'k':
      is_finish = strings_db_->Scan(start_key, pattern, keys, &leftover_visits, &next_key);
      if ((leftover_visits == 0) && !is_finish) {
        cursor_ret = cursor + step_length;
        StoreCursorStartKey(dtype, cursor_ret, std::string("k") + next_key);
        break;
      } else if (is_finish) {
        if (DataType::kStrings == dtype) {
          cursor_ret = 0;
          break;
        } else if (leftover_visits == 0) {
          cursor_ret = cursor + step_length;
          StoreCursorStartKey(dtype, cursor_ret, std::string("h") + prefix);
          break;
        }
      }
      start_key = prefix;
    case 'h':
      is_finish = hashes_db_->Scan(start_key, pattern, keys, &leftover_visits, &next_key);
      if ((leftover_visits == 0) && !is_finish) {
        cursor_ret = cursor + step_length;
        StoreCursorStartKey(dtype, cursor_ret, std::string("h") + next_key);
        break;
      } else if (is_finish) {
        if (DataType::kHashes == dtype) {
          cursor_ret = 0;
          break;
        } else if (leftover_visits == 0) {
          cursor_ret = cursor + step_length;
          StoreCursorStartKey(dtype, cursor_ret, std::string("s") + prefix);
          break;
        }
      }
      start_key = prefix;
    case 's':
      is_finish = sets_db_->Scan(start_key, pattern, keys, &leftover_visits, &next_key);
      if ((leftover_visits == 0) && !is_finish) {
        cursor_ret = cursor + step_length;
        StoreCursorStartKey(dtype, cursor_ret, std::string("s") + next_key);
        break;
      } else if (is_finish) {
        if (DataType::kSets == dtype) {
          cursor_ret = 0;
          break;
        } else if (leftover_visits == 0) {
          cursor_ret = cursor + step_length;
          StoreCursorStartKey(dtype, cursor_ret, std::string("l") + prefix);
          break;
        }
      }
      start_key = prefix;
    case 'l':
      is_finish = lists_db_->Scan(start_key, pattern, keys, &leftover_visits, &next_key);
      if ((leftover_visits == 0) && !is_finish) {
        cursor_ret = cursor + step_length;
        StoreCursorStartKey(dtype, cursor_ret, std::string("l") + next_key);
        break;
      } else if (is_finish) {
        if (DataType::kLists == dtype) {
          cursor_ret = 0;
          break;
        } else if (leftover_visits == 0) {
          cursor_ret = cursor + step_length;
          StoreCursorStartKey(dtype, cursor_ret, std::string("z") + prefix);
          break;
        }
      }
      start_key = prefix;
    case 'z':
      is_finish = zsets_db_->Scan(start_key, pattern, keys, &leftover_visits, &next_key);
      if ((leftover_visits == 0) && !is_finish) {
        cursor_ret = cursor + step_length;
        StoreCursorStartKey(dtype, cursor_ret, std::string("z") + next_key);
        break;
      } else if (is_finish) {
        cursor_ret = 0;
        break;
      }
  }
  return cursor_ret;
}

int64_t Storage::PKExpireScan(const DataType& dtype, int64_t cursor, int32_t min_ttl, int32_t max_ttl, int64_t count,
                              std::vector<std::string>* keys) {
  keys->clear();
  bool is_finish;
  int64_t leftover_visits = count;
  int64_t step_length = count;
  int64_t cursor_ret = 0;
  std::string start_key;
  std::string next_key;

  int64_t curtime;
  rocksdb::Env::Default()->GetCurrentTime(&curtime);

  if (cursor < 0) {
    return cursor_ret;
  } else {
    Status s = GetStartKey(dtype, cursor, &start_key);
    if (s.IsNotFound()) {
      // If want to scan all the databases, we start with the strings database
      start_key = std::string(1, dtype == DataType::kAll ? DataTypeTag[kStrings] : DataTypeTag[dtype]);
      cursor = 0;
    }
  }

  char key_type = start_key.at(0);
  start_key.erase(start_key.begin());
  switch (key_type) {
    case 'k':
      is_finish = strings_db_->PKExpireScan(start_key, static_cast<int32_t>(curtime + min_ttl),
                                            static_cast<int32_t>(curtime + max_ttl), keys, &leftover_visits, &next_key);
      if ((leftover_visits == 0) && !is_finish) {
        cursor_ret = cursor + step_length;
        StoreCursorStartKey(dtype, cursor_ret, std::string("k") + next_key);
        break;
      } else if (is_finish) {
        if (DataType::kStrings == dtype) {
          cursor_ret = 0;
          break;
        } else if (leftover_visits == 0) {
          cursor_ret = cursor + step_length;
          StoreCursorStartKey(dtype, cursor_ret, std::string("h"));
          break;
        }
      }
      start_key = "";
    case 'h':
      is_finish = hashes_db_->PKExpireScan(start_key, static_cast<int32_t>(curtime + min_ttl),
                                           static_cast<int32_t>(curtime + max_ttl), keys, &leftover_visits, &next_key);
      if ((leftover_visits == 0) && !is_finish) {
        cursor_ret = cursor + step_length;
        StoreCursorStartKey(dtype, cursor_ret, std::string("h") + next_key);
        break;
      } else if (is_finish) {
        if (DataType::kHashes == dtype) {
          cursor_ret = 0;
          break;
        } else if (leftover_visits == 0) {
          cursor_ret = cursor + step_length;
          StoreCursorStartKey(dtype, cursor_ret, std::string("s"));
          break;
        }
      }
      start_key = "";
    case 's':
      is_finish = sets_db_->PKExpireScan(start_key, static_cast<int32_t>(curtime + min_ttl),
                                         static_cast<int32_t>(curtime + max_ttl), keys, &leftover_visits, &next_key);
      if ((leftover_visits == 0) && !is_finish) {
        cursor_ret = cursor + step_length;
        StoreCursorStartKey(dtype, cursor_ret, std::string("s") + next_key);
        break;
      } else if (is_finish) {
        if (DataType::kSets == dtype) {
          cursor_ret = 0;
          break;
        } else if (leftover_visits == 0) {
          cursor_ret = cursor + step_length;
          StoreCursorStartKey(dtype, cursor_ret, std::string("l"));
          break;
        }
      }
      start_key = "";
    case 'l':
      is_finish = lists_db_->PKExpireScan(start_key, static_cast<int32_t>(curtime + min_ttl),
                                          static_cast<int32_t>(curtime + max_ttl), keys, &leftover_visits, &next_key);
      if ((leftover_visits == 0) && !is_finish) {
        cursor_ret = cursor + step_length;
        StoreCursorStartKey(dtype, cursor_ret, std::string("l") + next_key);
        break;
      } else if (is_finish) {
        if (DataType::kLists == dtype) {
          cursor_ret = 0;
          break;
        } else if (leftover_visits == 0) {
          cursor_ret = cursor + step_length;
          StoreCursorStartKey(dtype, cursor_ret, std::string("z"));
          break;
        }
      }
      start_key = "";
    case 'z':
      is_finish = zsets_db_->PKExpireScan(start_key, static_cast<int32_t>(curtime + min_ttl),
                                          static_cast<int32_t>(curtime + max_ttl), keys, &leftover_visits, &next_key);
      if ((leftover_visits == 0) && !is_finish) {
        cursor_ret = cursor + step_length;
        StoreCursorStartKey(dtype, cursor_ret, std::string("z") + next_key);
        break;
      } else if (is_finish) {
        cursor_ret = 0;
        break;
      }
  }
  return cursor_ret;
}

Status Storage::PKScanRange(const DataType& data_type, const Slice& key_start, const Slice& key_end,
                            const Slice& pattern, int32_t limit, std::vector<std::string>* keys,
                            std::vector<KeyValue>* kvs, std::string* next_key) {
  Status s;
  keys->clear();
  next_key->clear();
  switch (data_type) {
    case DataType::kStrings:
      s = strings_db_->PKScanRange(key_start, key_end, pattern, limit, kvs, next_key);
      break;
    case DataType::kHashes:
      s = hashes_db_->PKScanRange(key_start, key_end, pattern, limit, keys, next_key);
      break;
    case DataType::kLists:
      s = lists_db_->PKScanRange(key_start, key_end, pattern, limit, keys, next_key);
      break;
    case DataType::kZSets:
      s = zsets_db_->PKScanRange(key_start, key_end, pattern, limit, keys, next_key);
      break;
    case DataType::kSets:
      s = sets_db_->PKScanRange(key_start, key_end, pattern, limit, keys, next_key);
      break;
    default:
      s = Status::Corruption("Unsupported data types");
      break;
  }
  return s;
}

Status Storage::PKRScanRange(const DataType& data_type, const Slice& key_start, const Slice& key_end,
                             const Slice& pattern, int32_t limit, std::vector<std::string>* keys,
                             std::vector<KeyValue>* kvs, std::string* next_key) {
  Status s;
  keys->clear();
  next_key->clear();
  switch (data_type) {
    case DataType::kStrings:
      s = strings_db_->PKRScanRange(key_start, key_end, pattern, limit, kvs, next_key);
      break;
    case DataType::kHashes:
      s = hashes_db_->PKRScanRange(key_start, key_end, pattern, limit, keys, next_key);
      break;
    case DataType::kLists:
      s = lists_db_->PKRScanRange(key_start, key_end, pattern, limit, keys, next_key);
      break;
    case DataType::kZSets:
      s = zsets_db_->PKRScanRange(key_start, key_end, pattern, limit, keys, next_key);
      break;
    case DataType::kSets:
      s = sets_db_->PKRScanRange(key_start, key_end, pattern, limit, keys, next_key);
      break;
    default:
      s = Status::Corruption("Unsupported data types");
      break;
  }
  return s;
}

Status Storage::PKPatternMatchDel(const DataType& data_type, const std::string& pattern, int32_t* ret) {
  Status s;
  switch (data_type) {
    case DataType::kStrings:
      s = strings_db_->PKPatternMatchDel(pattern, ret);
      break;
    case DataType::kHashes:
      s = hashes_db_->PKPatternMatchDel(pattern, ret);
      break;
    case DataType::kLists:
      s = lists_db_->PKPatternMatchDel(pattern, ret);
      break;
    case DataType::kZSets:
      s = zsets_db_->PKPatternMatchDel(pattern, ret);
      break;
    case DataType::kSets:
      s = sets_db_->PKPatternMatchDel(pattern, ret);
      break;
    default:
      s = Status::Corruption("Unsupported data type");
      break;
  }
  return s;
}

Status Storage::Scanx(const DataType& data_type, const std::string& start_key, const std::string& pattern,
                      int64_t count, std::vector<std::string>* keys, std::string* next_key) {
  Status s;
  keys->clear();
  next_key->clear();
  switch (data_type) {
    case DataType::kStrings:
      strings_db_->Scan(start_key, pattern, keys, &count, next_key);
      break;
    case DataType::kHashes:
      hashes_db_->Scan(start_key, pattern, keys, &count, next_key);
      break;
    case DataType::kLists:
      lists_db_->Scan(start_key, pattern, keys, &count, next_key);
      break;
    case DataType::kZSets:
      zsets_db_->Scan(start_key, pattern, keys, &count, next_key);
      break;
    case DataType::kSets:
      sets_db_->Scan(start_key, pattern, keys, &count, next_key);
      break;
    default:
      Status::Corruption("Unsupported data types");
      break;
  }
  return s;
}

int32_t Storage::Expireat(const Slice& key, int32_t timestamp, std::map<DataType, Status>* type_status) {
  Status s;
  int32_t count = 0;
  bool is_corruption = false;

  s = strings_db_->Expireat(key, timestamp);
  if (s.ok()) {
    count++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kStrings] = s;
  }

  s = hashes_db_->Expireat(key, timestamp);
  if (s.ok()) {
    count++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kHashes] = s;
  }

  s = sets_db_->Expireat(key, timestamp);
  if (s.ok()) {
    count++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kSets] = s;
  }

  s = lists_db_->Expireat(key, timestamp);
  if (s.ok()) {
    count++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kLists] = s;
  }

  s = zsets_db_->Expireat(key, timestamp);
  if (s.ok()) {
    count++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kLists] = s;
  }

  if (is_corruption) {
    return -1;
  } else {
    return count;
  }
}

int32_t Storage::Persist(const Slice& key, std::map<DataType, Status>* type_status) {
  Status s;
  int32_t count = 0;
  bool is_corruption = false;

  s = strings_db_->Persist(key);
  if (s.ok()) {
    count++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kStrings] = s;
  }

  s = hashes_db_->Persist(key);
  if (s.ok()) {
    count++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kHashes] = s;
  }

  s = sets_db_->Persist(key);
  if (s.ok()) {
    count++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kSets] = s;
  }

  s = lists_db_->Persist(key);
  if (s.ok()) {
    count++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kLists] = s;
  }

  s = zsets_db_->Persist(key);
  if (s.ok()) {
    count++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kLists] = s;
  }

  if (is_corruption) {
    return -1;
  } else {
    return count;
  }
}

std::map<DataType, int64_t> Storage::TTL(const Slice& key, std::map<DataType, Status>* type_status) {
  Status s;
  std::map<DataType, int64_t> ret;
  int64_t timestamp = 0;

  s = strings_db_->TTL(key, &timestamp);
  if (s.ok() || s.IsNotFound()) {
    ret[DataType::kStrings] = timestamp;
  } else if (!s.IsNotFound()) {
    ret[DataType::kStrings] = -3;
    (*type_status)[DataType::kStrings] = s;
  }

  s = hashes_db_->TTL(key, &timestamp);
  if (s.ok() || s.IsNotFound()) {
    ret[DataType::kHashes] = timestamp;
  } else if (!s.IsNotFound()) {
    ret[DataType::kHashes] = -3;
    (*type_status)[DataType::kHashes] = s;
  }

  s = lists_db_->TTL(key, &timestamp);
  if (s.ok() || s.IsNotFound()) {
    ret[DataType::kLists] = timestamp;
  } else if (!s.IsNotFound()) {
    ret[DataType::kLists] = -3;
    (*type_status)[DataType::kLists] = s;
  }

  s = sets_db_->TTL(key, &timestamp);
  if (s.ok() || s.IsNotFound()) {
    ret[DataType::kSets] = timestamp;
  } else if (!s.IsNotFound()) {
    ret[DataType::kSets] = -3;
    (*type_status)[DataType::kSets] = s;
  }

  s = zsets_db_->TTL(key, &timestamp);
  if (s.ok() || s.IsNotFound()) {
    ret[DataType::kZSets] = timestamp;
  } else if (!s.IsNotFound()) {
    ret[DataType::kZSets] = -3;
    (*type_status)[DataType::kZSets] = s;
  }
  return ret;
}

Status Storage::GetType(const std::string& key, bool single, std::vector<std::string>& types) {
  types.clear();

  Status s;
  std::string value;
  s = strings_db_->Get(key, &value);
  if (s.ok()) {
    types.emplace_back("string");
  } else if (!s.IsNotFound()) {
    return s;
  }
  if (single && !types.empty()) {
    return s;
  }

  int32_t hashes_len = 0;
  s = hashes_db_->HLen(key, &hashes_len);
  if (s.ok() && hashes_len != 0) {
    types.emplace_back("hash");
  } else if (!s.IsNotFound()) {
    return s;
  }
  if (single && !types.empty()) {
    return s;
  }

  uint64_t lists_len = 0;
  s = lists_db_->LLen(key, &lists_len);
  if (s.ok() && lists_len != 0) {
    types.emplace_back("list");
  } else if (!s.IsNotFound()) {
    return s;
  }
  if (single && !types.empty()) {
    return s;
  }

  int32_t zsets_size = 0;
  s = zsets_db_->ZCard(key, &zsets_size);
  if (s.ok() && zsets_size != 0) {
    types.emplace_back("zset");
  } else if (!s.IsNotFound()) {
    return s;
  }
  if (single && !types.empty()) {
    return s;
  }

  int32_t sets_size = 0;
  s = sets_db_->SCard(key, &sets_size);
  if (s.ok() && sets_size != 0) {
    types.emplace_back("set");
  } else if (!s.IsNotFound()) {
    return s;
  }
  if (single && types.empty()) {
    types.emplace_back("none");
  }
  return Status::OK();
}

Status Storage::Keys(const DataType& data_type, const std::string& pattern, std::vector<std::string>* keys) {
  Status s;
  if (data_type == DataType::kStrings) {
    s = strings_db_->ScanKeys(pattern, keys);
    if (!s.ok()) {
      return s;
    }
  } else if (data_type == DataType::kHashes) {
    s = hashes_db_->ScanKeys(pattern, keys);
    if (!s.ok()) {
      return s;
    }
  } else if (data_type == DataType::kZSets) {
    s = zsets_db_->ScanKeys(pattern, keys);
    if (!s.ok()) {
      return s;
    }
  } else if (data_type == DataType::kSets) {
    s = sets_db_->ScanKeys(pattern, keys);
    if (!s.ok()) {
      return s;
    }
  } else if (data_type == DataType::kLists) {
    s = lists_db_->ScanKeys(pattern, keys);
    if (!s.ok()) {
      return s;
    }
  } else {
    s = strings_db_->ScanKeys(pattern, keys);
    if (!s.ok()) {
      return s;
    }
    s = hashes_db_->ScanKeys(pattern, keys);
    if (!s.ok()) {
      return s;
    }
    s = zsets_db_->ScanKeys(pattern, keys);
    if (!s.ok()) {
      return s;
    }
    s = sets_db_->ScanKeys(pattern, keys);
    if (!s.ok()) {
      return s;
    }
    s = lists_db_->ScanKeys(pattern, keys);
    if (!s.ok()) {
      return s;
    }
  }
  return s;
}

void Storage::ScanDatabase(const DataType& type) {
  switch (type) {
    case kStrings:
      strings_db_->ScanDatabase();
      break;
    case kHashes:
      hashes_db_->ScanDatabase();
      break;
    case kSets:
      sets_db_->ScanDatabase();
      break;
    case kZSets:
      zsets_db_->ScanDatabase();
      break;
    case kLists:
      lists_db_->ScanDatabase();
      break;
    case kAll:
      strings_db_->ScanDatabase();
      hashes_db_->ScanDatabase();
      sets_db_->ScanDatabase();
      zsets_db_->ScanDatabase();
      lists_db_->ScanDatabase();
      break;
  }
}

// HyperLogLog
Status Storage::PfAdd(const Slice& key, const std::vector<std::string>& values, bool* update) {
  *update = false;
  if (values.size() >= kMaxKeys) {
    return Status::InvalidArgument("Invalid the number of key");
  }

  std::string value;
  std::string registers;
  std::string result;
  Status s = strings_db_->Get(key, &value);
  if (s.ok()) {
    registers = value;
  } else if (s.IsNotFound()) {
    registers = "";
  } else {
    return s;
  }
  HyperLogLog log(kPrecision, registers);
  auto previous = static_cast<int32_t>(log.Estimate());
  for (const auto& value : values) {
    result = log.Add(value.data(), value.size());
  }
  HyperLogLog update_log(kPrecision, result);
  auto now = static_cast<int32_t>(update_log.Estimate());
  if (previous != now || (s.IsNotFound() && values.empty())) {
    *update = true;
  }
  s = strings_db_->Set(key, result);
  return s;
}

Status Storage::PfCount(const std::vector<std::string>& keys, int64_t* result) {
  if (keys.size() >= kMaxKeys || keys.empty()) {
    return Status::InvalidArgument("Invalid the number of key");
  }

  std::string value;
  std::string first_registers;
  Status s = strings_db_->Get(keys[0], &value);
  if (s.ok()) {
    first_registers = std::string(value.data(), value.size());
  } else if (s.IsNotFound()) {
    first_registers = "";
  }

  HyperLogLog first_log(kPrecision, first_registers);
  for (size_t i = 1; i < keys.size(); ++i) {
    std::string value;
    std::string registers;
    s = strings_db_->Get(keys[i], &value);
    if (s.ok()) {
      registers = value;
    } else if (s.IsNotFound()) {
      continue;
    } else {
      return s;
    }
    HyperLogLog log(kPrecision, registers);
    first_log.Merge(log);
  }
  *result = static_cast<int32_t>(first_log.Estimate());
  return Status::OK();
}

Status Storage::PfMerge(const std::vector<std::string>& keys, std::string& value_to_dest) {
  if (keys.size() >= kMaxKeys || keys.empty()) {
    return Status::InvalidArgument("Invalid the number of key");
  }

  Status s;
  std::string value;
  std::string first_registers;
  std::string result;
  s = strings_db_->Get(keys[0], &value);
  if (s.ok()) {
    first_registers = std::string(value.data(), value.size());
  } else if (s.IsNotFound()) {
    first_registers = "";
  }

  result = first_registers;
  HyperLogLog first_log(kPrecision, first_registers);
  for (size_t i = 1; i < keys.size(); ++i) {
    std::string value;
    std::string registers;
    s = strings_db_->Get(keys[i], &value);
    if (s.ok()) {
      registers = std::string(value.data(), value.size());
    } else if (s.IsNotFound()) {
      continue;
    } else {
      return s;
    }
    HyperLogLog log(kPrecision, registers);
    result = first_log.Merge(log);
  }
  s = strings_db_->Set(keys[0], result);
  value_to_dest = std::move(result);
  return s;
}

static void* StartBGThreadWrapper(void* arg) {
  auto s = reinterpret_cast<Storage*>(arg);
  s->RunBGTask();
  return nullptr;
}

Status Storage::StartBGThread() {
  int result = pthread_create(&bg_tasks_thread_id_, nullptr, StartBGThreadWrapper, this);
  if (result != 0) {
    char msg[128];
    snprintf(msg, sizeof(msg), "pthread create: %s", strerror(result));
    return Status::Corruption(msg);
  }
  return Status::OK();
}

Status Storage::AddBGTask(const BGTask& bg_task) {
  bg_tasks_mutex_.lock();
  if (bg_task.type == kAll) {
    // if current task it is global compact,
    // clear the bg_tasks_queue_;
    std::queue<BGTask> empty_queue;
    bg_tasks_queue_.swap(empty_queue);
  }
  bg_tasks_queue_.push(bg_task);
  bg_tasks_cond_var_.notify_one();
  bg_tasks_mutex_.unlock();
  return Status::OK();
}

Status Storage::RunBGTask() {
  BGTask task;
  while (!bg_tasks_should_exit_) {
    std::unique_lock lock(bg_tasks_mutex_);
    bg_tasks_cond_var_.wait(lock, [this]() { return !bg_tasks_queue_.empty() || bg_tasks_should_exit_; });

    if (!bg_tasks_queue_.empty()) {
      task = bg_tasks_queue_.front();
      bg_tasks_queue_.pop();
    }
    lock.unlock();

    if (bg_tasks_should_exit_) {
      return Status::Incomplete("bgtask return with bg_tasks_should_exit true");
    }

    if (task.operation == kCleanAll) {
      DoCompact(task.type);
    } else if (task.operation == kCompactRange) {
      if (task.argv.size() == 2) {
        DoCompactRange(task.type, task.argv.front(), task.argv.back());
      }
    }
  }
  return Status::OK();
}

Status Storage::Compact(const DataType& type, bool sync) {
  if (sync) {
    return DoCompact(type);
  } else {
    AddBGTask({type, kCleanAll});
  }
  return Status::OK();
}

Status Storage::DoCompact(const DataType& type) {
  if (type != kAll && type != kStrings && type != kHashes && type != kSets && type != kZSets && type != kLists) {
    return Status::InvalidArgument("");
  }

  Status s;
  if (type == kStrings) {
    current_task_type_ = Operation::kCleanStrings;
    s = strings_db_->CompactRange(nullptr, nullptr);
  } else if (type == kHashes) {
    current_task_type_ = Operation::kCleanHashes;
    s = hashes_db_->CompactRange(nullptr, nullptr);
  } else if (type == kSets) {
    current_task_type_ = Operation::kCleanSets;
    s = sets_db_->CompactRange(nullptr, nullptr);
  } else if (type == kZSets) {
    current_task_type_ = Operation::kCleanZSets;
    s = zsets_db_->CompactRange(nullptr, nullptr);
  } else if (type == kLists) {
    current_task_type_ = Operation::kCleanLists;
    s = lists_db_->CompactRange(nullptr, nullptr);
  } else {
    current_task_type_ = Operation::kCleanAll;
    s = strings_db_->CompactRange(nullptr, nullptr);
    s = hashes_db_->CompactRange(nullptr, nullptr);
    s = sets_db_->CompactRange(nullptr, nullptr);
    s = zsets_db_->CompactRange(nullptr, nullptr);
    s = lists_db_->CompactRange(nullptr, nullptr);
  }
  current_task_type_ = Operation::kNone;
  return s;
}

Status Storage::CompactRange(const DataType& type, const std::string& start, const std::string& end, bool sync) {
  if (sync) {
    return DoCompactRange(type, start, end);
  } else {
    AddBGTask({type, kCompactRange, {start, end}});
  }
  return Status::OK();
}

Status Storage::DoCompactRange(const DataType& type, const std::string& start, const std::string& end) {
  Status s;
  if (type == kStrings) {
    Slice slice_begin(start);
    Slice slice_end(end);
    s = strings_db_->CompactRange(&slice_begin, &slice_end);
    return s;
  }

  std::string meta_start_key;
  std::string meta_end_key;
  std::string data_start_key;
  std::string data_end_key;
  CalculateMetaStartAndEndKey(start, &meta_start_key, nullptr);
  CalculateMetaStartAndEndKey(end, nullptr, &meta_end_key);
  CalculateDataStartAndEndKey(start, &data_start_key, nullptr);
  CalculateDataStartAndEndKey(end, nullptr, &data_end_key);
  Slice slice_meta_begin(meta_start_key);
  Slice slice_meta_end(meta_end_key);
  Slice slice_data_begin(data_start_key);
  Slice slice_data_end(data_end_key);
  if (type == kSets) {
    s = sets_db_->CompactRange(&slice_meta_begin, &slice_meta_end, kMeta);
    s = sets_db_->CompactRange(&slice_data_begin, &slice_data_end, kData);
  } else if (type == kZSets) {
    s = zsets_db_->CompactRange(&slice_meta_begin, &slice_meta_end, kMeta);
    s = zsets_db_->CompactRange(&slice_data_begin, &slice_data_end, kData);
  } else if (type == kHashes) {
    s = hashes_db_->CompactRange(&slice_meta_begin, &slice_meta_end, kMeta);
    s = hashes_db_->CompactRange(&slice_data_begin, &slice_data_end, kData);
  } else if (type == kLists) {
    s = lists_db_->CompactRange(&slice_meta_begin, &slice_meta_end, kMeta);
    s = lists_db_->CompactRange(&slice_data_begin, &slice_data_end, kData);
  }
  return s;
}

Status Storage::SetMaxCacheStatisticKeys(uint32_t max_cache_statistic_keys) {
  std::vector<Redis*> dbs = {sets_db_.get(), zsets_db_.get(), hashes_db_.get(), lists_db_.get()};
  for (const auto& db : dbs) {
    db->SetMaxCacheStatisticKeys(max_cache_statistic_keys);
  }
  return Status::OK();
}

Status Storage::SetSmallCompactionThreshold(uint32_t small_compaction_threshold) {
  std::vector<Redis*> dbs = {sets_db_.get(), zsets_db_.get(), hashes_db_.get(), lists_db_.get()};
  for (const auto& db : dbs) {
    db->SetSmallCompactionThreshold(small_compaction_threshold);
  }
  return Status::OK();
}

Status Storage::SetSmallCompactionDurationThreshold(uint32_t small_compaction_duration_threshold) {
  std::vector<Redis*> dbs = {sets_db_.get(), zsets_db_.get(), hashes_db_.get(), lists_db_.get()};
  for (const auto& db : dbs) {
    db->SetSmallCompactionDurationThreshold(small_compaction_duration_threshold);
  }
  return Status::OK();
}

std::string Storage::GetCurrentTaskType() {
  int type = current_task_type_;
  switch (type) {
    case kCleanAll:
      return "All";
    case kCleanStrings:
      return "String";
    case kCleanHashes:
      return "Hash";
    case kCleanZSets:
      return "ZSet";
    case kCleanSets:
      return "Set";
    case kCleanLists:
      return "List";
    case kNone:
    default:
      return "No";
  }
}

Status Storage::GetUsage(const std::string& property, uint64_t* const result) {
  *result = GetProperty(ALL_DB, property);
  return Status::OK();
}

Status Storage::GetUsage(const std::string& property, std::map<std::string, uint64_t>* const type_result) {
  type_result->clear();
  (*type_result)[STRINGS_DB] = GetProperty(STRINGS_DB, property);
  (*type_result)[HASHES_DB] = GetProperty(HASHES_DB, property);
  (*type_result)[LISTS_DB] = GetProperty(LISTS_DB, property);
  (*type_result)[ZSETS_DB] = GetProperty(ZSETS_DB, property);
  (*type_result)[SETS_DB] = GetProperty(SETS_DB, property);
  return Status::OK();
}

uint64_t Storage::GetProperty(const std::string& db_type, const std::string& property) {
  uint64_t out = 0;
  uint64_t result = 0;
  if (db_type == ALL_DB || db_type == STRINGS_DB) {
    strings_db_->GetProperty(property, &out);
    result += out;
  }
  if (db_type == ALL_DB || db_type == HASHES_DB) {
    hashes_db_->GetProperty(property, &out);
    result += out;
  }
  if (db_type == ALL_DB || db_type == LISTS_DB) {
    lists_db_->GetProperty(property, &out);
    result += out;
  }
  if (db_type == ALL_DB || db_type == ZSETS_DB) {
    zsets_db_->GetProperty(property, &out);
    result += out;
  }
  if (db_type == ALL_DB || db_type == SETS_DB) {
    sets_db_->GetProperty(property, &out);
    result += out;
  }
  return result;
}

Status Storage::GetKeyNum(std::vector<KeyInfo>* key_infos) {
  KeyInfo key_info;
  // NOTE: keep the db order with string, hash, list, zset, set
  std::vector<Redis*> dbs = {strings_db_.get(), hashes_db_.get(), lists_db_.get(), zsets_db_.get(), sets_db_.get()};
  for (const auto& db : dbs) {
    // check the scanner was stopped or not, before scanning the next db
    if (scan_keynum_exit_) {
      break;
    }
    db->ScanKeyNum(&key_info);
    key_infos->push_back(key_info);
  }
  if (scan_keynum_exit_) {
    scan_keynum_exit_ = false;
    return Status::Corruption("exit");
  }
  return Status::OK();
}

Status Storage::StopScanKeyNum() {
  scan_keynum_exit_ = true;
  return Status::OK();
}

rocksdb::DB* Storage::GetDBByType(const std::string& type) {
  if (type == STRINGS_DB) {
    return strings_db_->GetDB();
  } else if (type == HASHES_DB) {
    return hashes_db_->GetDB();
  } else if (type == LISTS_DB) {
    return lists_db_->GetDB();
  } else if (type == SETS_DB) {
    return sets_db_->GetDB();
  } else if (type == ZSETS_DB) {
    return zsets_db_->GetDB();
  } else {
    return nullptr;
  }
}

Status Storage::SetOptions(const OptionType& option_type, const std::string& db_type,
                           const std::unordered_map<std::string, std::string>& options) {
  Status s;
  if (db_type == ALL_DB || db_type == STRINGS_DB) {
    s = strings_db_->SetOptions(option_type, options);
    if (!s.ok()) {
      return s;
    }
  }
  if (db_type == ALL_DB || db_type == HASHES_DB) {
    s = hashes_db_->SetOptions(option_type, options);
    if (!s.ok()) {
      return s;
    }
  }
  if (db_type == ALL_DB || db_type == LISTS_DB) {
    s = lists_db_->SetOptions(option_type, options);
    if (!s.ok()) {
      return s;
    }
  }
  if (db_type == ALL_DB || db_type == ZSETS_DB) {
    s = zsets_db_->SetOptions(option_type, options);
    if (!s.ok()) {
      return s;
    }
  }
  if (db_type == ALL_DB || db_type == SETS_DB) {
    s = sets_db_->SetOptions(option_type, options);
    if (!s.ok()) {
      return s;
    }
  }
  s = EnableDymayticOptions(option_type,db_type,options);
  return s;
}

void Storage::SetCompactRangeOptions(const bool is_canceled) {
  strings_db_->SetCompactRangeOptions(is_canceled);
  hashes_db_->SetCompactRangeOptions(is_canceled);
  lists_db_->SetCompactRangeOptions(is_canceled);
  sets_db_->SetCompactRangeOptions(is_canceled);
  zsets_db_->SetCompactRangeOptions(is_canceled);
}

Status Storage::EnableDymayticOptions(const OptionType& option_type, 
                            const std::string& db_type, const std::unordered_map<std::string, std::string>& options) {
  Status s;
  auto it = options.find("disable_auto_compactions");
  if (it != options.end() && it->second == "false") {
    s = EnableAutoCompaction(option_type,db_type,options);
    LOG(WARNING) << "EnableAutoCompaction " << (s.ok() ? "success" : "failed") 
                 << " when Options get disable_auto_compactions: " << it->second << ",db_type:" << db_type;
  }
  return s;
}

Status Storage::EnableAutoCompaction(const OptionType& option_type, 
                            const std::string& db_type, const std::unordered_map<std::string, std::string>& options){
  Status s;
  std::vector<std::string> cfs;
  std::vector<rocksdb::ColumnFamilyHandle*> cfhds;

  if (db_type == ALL_DB || db_type == STRINGS_DB) {
    cfhds = strings_db_->GetHandles();
    s = strings_db_.get()->GetDB()->EnableAutoCompaction(cfhds);
    if (!s.ok()) {
      return s;
    }
  }
  if (db_type == ALL_DB || db_type == HASHES_DB) {
    cfhds = hashes_db_->GetHandles();
    s = hashes_db_.get()->GetDB()->EnableAutoCompaction(cfhds);
    if (!s.ok()) {
      return s;
    }
  }
  if (db_type == ALL_DB || db_type == LISTS_DB) {
    cfhds = lists_db_->GetHandles();
    s = lists_db_.get()->GetDB()->EnableAutoCompaction(cfhds);
    if (!s.ok()) {
      return s;
    }
  }
  if (db_type == ALL_DB || db_type == ZSETS_DB) {
    cfhds = zsets_db_->GetHandles();
    s = zsets_db_.get()->GetDB()->EnableAutoCompaction(cfhds);
    if (!s.ok()) {
      return s;
    }
  }
  if (db_type == ALL_DB || db_type == SETS_DB) {
    cfhds = sets_db_->GetHandles();
    s = sets_db_.get()->GetDB()->EnableAutoCompaction(cfhds);
    if (!s.ok()) {
      return s;
    }
  }
  return s;
}

void Storage::GetRocksDBInfo(std::string& info) {
  strings_db_->GetRocksDBInfo(info, "strings_");
  hashes_db_->GetRocksDBInfo(info, "hashes_");
  lists_db_->GetRocksDBInfo(info, "lists_");
  sets_db_->GetRocksDBInfo(info, "sets_");
  zsets_db_->GetRocksDBInfo(info, "zsets_");
}

int64_t Storage::IsExist(const Slice& key, std::map<DataType, Status>* type_status) {
  std::string value;
  int32_t ret = 0;
  int64_t type_count = 0;
  Status s = strings_db_->Get(key, &value);
  (*type_status)[DataType::kStrings] = s;
  if (s.ok()) {
    type_count++;
  }
  s = hashes_db_->HLen(key, &ret);
  (*type_status)[DataType::kHashes] = s;
  if (s.ok()) {
    type_count++;
  }
  s = sets_db_->SCard(key, &ret);
  (*type_status)[DataType::kSets] = s;
  if (s.ok()) {
    type_count++;
  }
  uint64_t llen = 0;
  s = lists_db_->LLen(key, &llen);
  (*type_status)[DataType::kLists] = s;
  if (s.ok()) {
    type_count++;
  }

  s = zsets_db_->ZCard(key, &ret);
  (*type_status)[DataType::kZSets] = s;
  if (s.ok()) {
    type_count++;
  }
  return type_count;
}
  
  
void Storage::DisableWal(const bool is_wal_disable) {
  strings_db_->SetWriteWalOptions(is_wal_disable);
  hashes_db_->SetWriteWalOptions(is_wal_disable);
  lists_db_->SetWriteWalOptions(is_wal_disable);
  sets_db_->SetWriteWalOptions(is_wal_disable);
  zsets_db_->SetWriteWalOptions(is_wal_disable);
}

}  //  namespace storage
