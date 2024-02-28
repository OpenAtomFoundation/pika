//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <utility>
#include <algorithm>

#include <glog/logging.h>

#include "storage/util.h"
#include "storage/storage.h"
#include "scope_snapshot.h"
#include "src/lru_cache.h"
#include "src/mutex_impl.h"
#include "src/options_helper.h"
#include "src/redis_hyperloglog.h"
#include "src/type_iterator.h"
#include "src/redis.h"
#include "include/pika_conf.h"
#include "pstd/include/pika_codis_slot.h"

namespace storage {
extern std::string BitOpOperate(BitOpType op, const std::vector<std::string>& src_values, int64_t max_len);
class Redis;
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

// for unit test only
Storage::Storage() : Storage(3, 1024, true) {}

Storage::Storage(int db_instance_num, int slot_num, bool is_classic_mode) {
  cursors_store_ = std::make_unique<LRUCache<std::string, std::string>>();
  cursors_store_->SetCapacity(5000);
  slot_indexer_ = std::make_unique<SlotIndexer>(db_instance_num);
  is_classic_mode_ = is_classic_mode;
  db_instance_num_ = db_instance_num;
  slot_num_ = slot_num;

  Status s = StartBGThread();
  if (!s.ok()) {
    LOG(FATAL) << "start bg thread failed, " << s.ToString();
  }
}

Storage::~Storage() {
  bg_tasks_should_exit_ = true;
  bg_tasks_cond_var_.notify_one();

  if (is_opened_) {
    int ret = 0;
    if ((ret = pthread_join(bg_tasks_thread_id_, nullptr)) != 0) {
      LOG(ERROR) << "pthread_join failed with bgtask thread error " << ret;
    }
    for (auto& inst : insts_) {
      inst.reset();
    }
  }
}

static std::string AppendSubDirectory(const std::string& db_path, int index) {
  if (db_path.back() == '/') {
    return db_path + std::to_string(index);
  } else {
    return db_path + "/" + std::to_string(index);
  }
}

Status Storage::Open(const StorageOptions& storage_options, const std::string& db_path) {
  mkpath(db_path.c_str(), 0755);

  int inst_count = db_instance_num_;
  for (int index = 0; index < inst_count; index++) {
    insts_.emplace_back(std::make_unique<Redis>(this, index));
    Status s = insts_.back()->Open(storage_options, AppendSubDirectory(db_path, index));
    if (!s.ok()) {
      LOG(FATAL) << "open db failed" << s.ToString();
    }
  }

  is_opened_.store(true);
  return Status::OK();
}

Status Storage::LoadCursorStartKey(const DataType& dtype, int64_t cursor, char* type, std::string* start_key) {
  std::string index_key = DataTypeTag[dtype] + std::to_string(cursor);
  std::string index_value;
  Status s = cursors_store_->Lookup(index_key, &index_value);
  if (!s.ok() || index_value.size() < 3) {
    return s;
  }
  *type = index_value[0];
  *start_key = index_value.substr(1);
  return s;
}

Status Storage::StoreCursorStartKey(const DataType& dtype, int64_t cursor, char type, const std::string& next_key) {
  std::string index_key = DataTypeTag[dtype] + std::to_string(cursor);
  // format: data_type tag(1B) | start_key
  std::string index_value(1, type);
  index_value.append(next_key);
  return cursors_store_->Insert(index_key, index_value);
}

std::unique_ptr<Redis>& Storage::GetDBInstance(const Slice& key) {
  return GetDBInstance(key.ToString());
}

std::unique_ptr<Redis>& Storage::GetDBInstance(const std::string& key) {
  auto inst_index = slot_indexer_->GetInstanceID(GetSlotID(slot_num_, key));
  return insts_[inst_index];
}

// Strings Commands
Status Storage::Set(const Slice& key, const Slice& value) {
  auto& inst = GetDBInstance(key);
  return inst->Set(key, value);
}

Status Storage::Setxx(const Slice& key, const Slice& value, int32_t* ret, int64_t ttl) {
  auto& inst = GetDBInstance(key);
  return inst->Setxx(key, value, ret, ttl);
}

Status Storage::Get(const Slice& key, std::string* value) {
  auto& inst = GetDBInstance(key);
  return inst->Get(key, value);
}

Status Storage::GetWithTTL(const Slice& key, std::string* value, int64_t* ttl) {
  auto& inst = GetDBInstance(key);
  return inst->GetWithTTL(key, value, ttl);
}

Status Storage::GetSet(const Slice& key, const Slice& value, std::string* old_value) {
  auto& inst = GetDBInstance(key);
  return inst->GetSet(key, value, old_value);
}

Status Storage::SetBit(const Slice& key, int64_t offset, int32_t value, int32_t* ret) {
  auto& inst = GetDBInstance(key);
  return inst->SetBit(key, offset, value, ret);
}

Status Storage::GetBit(const Slice& key, int64_t offset, int32_t* ret) {
  auto& inst = GetDBInstance(key);
  return inst->GetBit(key, offset, ret);
}

Status Storage::MSet(const std::vector<KeyValue>& kvs) {
  Status s;
  for (const auto& kv : kvs) {
    auto& inst = GetDBInstance(kv.key);
    s = inst->Set(Slice(kv.key), Slice(kv.value));
    if (!s.ok()) {
      return s;
    }
  }
  return s;
}

Status Storage::MGet(const std::vector<std::string>& keys, std::vector<ValueStatus>* vss) {
  vss->clear();
  Status s;
  for(const auto& key : keys) {
    auto& inst = GetDBInstance(key);
    std::string value;
    s = inst->Get(key, &value);
    if (s.ok()) {
      vss->push_back({value, Status::OK()});
    } else if(s.IsNotFound()) {
      vss->push_back({std::string(), Status::NotFound()});
    } else {
      vss->clear();
      return s;
    }
  }
  return Status::OK();
}

Status Storage::MGetWithTTL(const std::vector<std::string>& keys, std::vector<ValueStatus>* vss) {
  vss->clear();
  Status s;
  for(const auto& key : keys) {
    auto& inst = GetDBInstance(key);
    std::string value;
    int64_t ttl;
    s = inst->GetWithTTL(key, &value, &ttl);
    if (s.ok()) {
      vss->push_back({value, Status::OK(), ttl});
    } else if(s.IsNotFound()) {
      vss->push_back({std::string(), Status::NotFound(), ttl});
    } else {
      vss->clear();
      return s;
    }
  }
  return Status::OK();
}

Status Storage::Setnx(const Slice& key, const Slice& value, int32_t* ret, int64_t ttl) {
  auto& inst = GetDBInstance(key);
  return inst->Setnx(key, value, ret, ttl);
}

// disallowed in codis, only runs in pika classic mode
// TODO: Not concurrent safe now, merge wuxianrong's bugfix after floyd's PR review finishes.
Status Storage::MSetnx(const std::vector<KeyValue>& kvs, int32_t* ret) {
  assert(is_classic_mode_);
  Status s;
  for (const auto& kv : kvs) {
    auto& inst = GetDBInstance(kv.key);
    std::string value;
    s = inst->Get(Slice(kv.key), &value);
    if (s.ok() || !s.IsNotFound()) {
      return s;
    }
  }

  for (const auto& kv : kvs) {
    auto& inst = GetDBInstance(kv.key);
    s = inst->Set(Slice(kv.key), Slice(kv.value));
    if (!s.ok()) {
      return s;
    }
  }
  if (s.ok()) {
    *ret = 1;
  }
  return s;
}

Status Storage::Setvx(const Slice& key, const Slice& value, const Slice& new_value, int32_t* ret, int64_t ttl) {
  auto& inst = GetDBInstance(key);
  return inst->Setvx(key, value, new_value, ret, ttl);
}

Status Storage::Delvx(const Slice& key, const Slice& value, int32_t* ret) {
  auto& inst = GetDBInstance(key);
  return inst->Delvx(key, value, ret);
}

Status Storage::Setrange(const Slice& key, int64_t start_offset, const Slice& value, int32_t* ret) {
  auto& inst = GetDBInstance(key);
  return inst->Setrange(key, start_offset, value, ret);
}

Status Storage::Getrange(const Slice& key, int64_t start_offset, int64_t end_offset, std::string* ret) {
  auto& inst = GetDBInstance(key);
  return inst->Getrange(key, start_offset, end_offset, ret);
}

Status Storage::GetrangeWithValue(const Slice& key, int64_t start_offset, int64_t end_offset,
                                     std::string* ret, std::string* value, int64_t* ttl) {
  auto& inst = GetDBInstance(key);
  return inst->GetrangeWithValue(key, start_offset, end_offset, ret, value, ttl);
}

Status Storage::Append(const Slice& key, const Slice& value, int32_t* ret) {
  auto& inst = GetDBInstance(key);
  return inst->Append(key, value, ret);
}

Status Storage::BitCount(const Slice& key, int64_t start_offset, int64_t end_offset, int32_t* ret, bool have_range) {
  auto& inst = GetDBInstance(key);
  return inst->BitCount(key, start_offset, end_offset, ret, have_range);
}

// disallowed in codis proxy, only runs in classic mode
Status Storage::BitOp(BitOpType op, const std::string& dest_key, const std::vector<std::string>& src_keys,
                      std::string &value_to_dest, int64_t* ret) {
  assert(is_classic_mode_);
  Status s;
  int64_t max_len = 0;
  int64_t value_len = 0;
  std::vector<std::string> src_vlaues;
  for (const auto& src_key : src_keys) {
    auto& inst = GetDBInstance(src_key);
    std::string value;
    s = inst->Get(Slice(src_key), &value);
    if (s.ok()) {
      src_vlaues.push_back(value);
      value_len = value.size();
    } else {
      if (!s.IsNotFound()) {
        return s;
      }
      src_vlaues.push_back("");
      value_len = 0;
    }
    max_len = std::max(max_len, value_len);
  }

  std::string dest_value = BitOpOperate(op, src_vlaues, max_len);
  value_to_dest = dest_value;
  *ret = dest_value.size();

  auto& dest_inst = GetDBInstance(dest_key);
  return dest_inst->Set(Slice(dest_key), Slice(dest_value));
}

Status Storage::BitPos(const Slice& key, int32_t bit, int64_t* ret) {
  auto& inst = GetDBInstance(key);
  return inst->BitPos(key, bit, ret);
}

Status Storage::BitPos(const Slice& key, int32_t bit, int64_t start_offset, int64_t* ret) {
  auto& inst = GetDBInstance(key);
  return inst->BitPos(key, bit, start_offset, ret);
}

Status Storage::BitPos(const Slice& key, int32_t bit, int64_t start_offset, int64_t end_offset, int64_t* ret) {
  auto& inst = GetDBInstance(key);
  return inst->BitPos(key, bit, start_offset, end_offset, ret);
}

Status Storage::Decrby(const Slice& key, int64_t value, int64_t* ret) {
  auto& inst = GetDBInstance(key);
  return inst->Decrby(key, value, ret);
}

Status Storage::Incrby(const Slice& key, int64_t value, int64_t* ret) {
  auto& inst = GetDBInstance(key);
  return inst->Incrby(key, value, ret);
}

Status Storage::Incrbyfloat(const Slice& key, const Slice& value, std::string* ret) {
  auto& inst = GetDBInstance(key);
  return inst->Incrbyfloat(key, value, ret);
}

Status Storage::Setex(const Slice& key, const Slice& value, int64_t ttl) {
  auto& inst = GetDBInstance(key);
  return inst->Setex(key, value, ttl);
}

Status Storage::Strlen(const Slice& key, int32_t* len) {
  auto& inst = GetDBInstance(key);
  return inst->Strlen(key, len);
}

Status Storage::PKSetexAt(const Slice& key, const Slice& value, int64_t timestamp) {
  auto& inst = GetDBInstance(key);
  return inst->PKSetexAt(key, value, timestamp);
}

// Hashes Commands
Status Storage::HSet(const Slice& key, const Slice& field, const Slice& value, int32_t* res) {
  auto& inst = GetDBInstance(key);
  return inst->HSet(key, field, value, res);
}

Status Storage::HGet(const Slice& key, const Slice& field, std::string* value) {
  auto& inst = GetDBInstance(key);
  return inst->HGet(key, field, value);
}

Status Storage::HMSet(const Slice& key, const std::vector<FieldValue>& fvs) {
  auto& inst = GetDBInstance(key);
  return inst->HMSet(key, fvs);
}

Status Storage::HMGet(const Slice& key, const std::vector<std::string>& fields, std::vector<ValueStatus>* vss) {
  auto& inst = GetDBInstance(key);
  return inst->HMGet(key, fields, vss);
}

Status Storage::HGetall(const Slice& key, std::vector<FieldValue>* fvs) {
  auto& inst = GetDBInstance(key);
  return inst->HGetall(key, fvs);
}

Status Storage::HGetallWithTTL(const Slice& key, std::vector<FieldValue>* fvs, int64_t* ttl) {
  auto& inst = GetDBInstance(key);
  return inst->HGetallWithTTL(key, fvs, ttl);
}

Status Storage::HKeys(const Slice& key, std::vector<std::string>* fields) {
  auto& inst = GetDBInstance(key);
  return inst->HKeys(key, fields);
}

Status Storage::HVals(const Slice& key, std::vector<std::string>* values) {
  auto& inst = GetDBInstance(key);
  return inst->HVals(key, values);
}

Status Storage::HSetnx(const Slice& key, const Slice& field, const Slice& value, int32_t* ret) {
  auto& inst = GetDBInstance(key);
  return inst->HSetnx(key, field, value, ret);
}

Status Storage::HLen(const Slice& key, int32_t* ret) {
  auto& inst = GetDBInstance(key);
  return inst->HLen(key, ret);
}

Status Storage::HStrlen(const Slice& key, const Slice& field, int32_t* len) {
  auto& inst = GetDBInstance(key);
  return inst->HStrlen(key, field, len);
}

Status Storage::HExists(const Slice& key, const Slice& field) {
  auto& inst = GetDBInstance(key);
  return inst->HExists(key, field);
}

Status Storage::HIncrby(const Slice& key, const Slice& field, int64_t value, int64_t* ret) {
  auto& inst = GetDBInstance(key);
  return inst->HIncrby(key, field, value, ret);
}

Status Storage::HIncrbyfloat(const Slice& key, const Slice& field, const Slice& by, std::string* new_value) {
  auto& inst = GetDBInstance(key);
  return inst->HIncrbyfloat(key, field, by, new_value);
}

Status Storage::HDel(const Slice& key, const std::vector<std::string>& fields, int32_t* ret) {
  auto& inst = GetDBInstance(key);
  return inst->HDel(key, fields, ret);
}

Status Storage::HScan(const Slice& key, int64_t cursor, const std::string& pattern, int64_t count,
                      std::vector<FieldValue>* field_values, int64_t* next_cursor) {
  auto& inst = GetDBInstance(key);
  return inst->HScan(key, cursor, pattern, count, field_values, next_cursor);
}

Status Storage::HScanx(const Slice& key, const std::string& start_field, const std::string& pattern, int64_t count,
                       std::vector<FieldValue>* field_values, std::string* next_field) {
  auto& inst = GetDBInstance(key);
  return inst->HScanx(key, start_field, pattern, count, field_values, next_field);
}

Status Storage::PKHScanRange(const Slice& key, const Slice& field_start, const std::string& field_end,
                             const Slice& pattern, int32_t limit, std::vector<FieldValue>* field_values,
                             std::string* next_field) {
  auto& inst = GetDBInstance(key);
  return inst->PKHScanRange(key, field_start, field_end, pattern, limit, field_values, next_field);
}

Status Storage::PKHRScanRange(const Slice& key, const Slice& field_start, const std::string& field_end,
                              const Slice& pattern, int32_t limit, std::vector<FieldValue>* field_values,
                              std::string* next_field) {
  auto& inst = GetDBInstance(key);
  return inst->PKHRScanRange(key, field_start, field_end, pattern, limit, field_values, next_field);
}

// Sets Commands
Status Storage::SAdd(const Slice& key, const std::vector<std::string>& members, int32_t* ret) {
  auto& inst = GetDBInstance(key);
  return inst->SAdd(key, members, ret);
}

Status Storage::SCard(const Slice& key, int32_t* ret) {
  auto& inst = GetDBInstance(key);
  return inst->SCard(key, ret);
}

Status Storage::SDiff(const std::vector<std::string>& keys, std::vector<std::string>* members) {
  if (keys.empty()) {
    return rocksdb::Status::Corruption("SDiff invalid parameter, no keys");
  }
  members->clear();

  Status s;
  // in codis mode, users should garentee keys will be hashed to same slot
  if (!is_classic_mode_) {
    auto& inst = GetDBInstance(keys[0]);
    s = inst->SDiff(keys, members);
    return s;
  }

  auto& inst = GetDBInstance(keys[0]);
  std::vector<std::string> keys0_members;
  s = inst->SMembers(Slice(keys[0]), &keys0_members);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }

  for (const auto& member : keys0_members) {
    int32_t exist = 0;
    for (int idx = 1; idx < keys.size(); idx++) {
      Slice pkey = Slice(keys[idx]);
      auto& inst = GetDBInstance(pkey);
      s = inst->SIsmember(pkey, Slice(member), &exist);
      if (!s.ok() && !s.IsNotFound()) {
        return s;
      }
      if (exist) break;
    }
    if (!exist) {
      members->push_back(member);
    }
  }
  return Status::OK();
}

Status Storage::SDiffstore(const Slice& destination, const std::vector<std::string>& keys, std::vector<std::string>& value_to_dest, int32_t* ret) {
  Status s;

  // in codis mode, users should garentee keys will be hashed to same slot
  if (!is_classic_mode_) {
    auto& inst = GetDBInstance(keys[0]);
    s = inst->SDiffstore(destination, keys, value_to_dest, ret);
    return s;
  }

  s = SDiff(keys, &value_to_dest);
  if (!s.ok()) {
    return s;
  }

  auto& inst = GetDBInstance(destination);
  s = inst->SetsDel(destination);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }

  s = inst->SAdd(destination, value_to_dest, ret);
  return s;
}

Status Storage::SInter(const std::vector<std::string>& keys, std::vector<std::string>* members) {
  Status s;
  members->clear();

  // in codis mode, users should garentee keys will be hashed to same slot
  if (!is_classic_mode_) {
    auto& inst = GetDBInstance(keys[0]);
    s = inst->SInter(keys, members);
    return s;
  }

  std::vector<std::string> key0_members;
  auto& inst = GetDBInstance(keys[0]);
  s = inst->SMembers(keys[0], &key0_members);
  if (s.IsNotFound()) {
    return Status::OK();
  }
  if (!s.ok()) {
    return s;
  }

  for (const auto member : key0_members) {
    int32_t exist = 1;
    for (int idx = 1; idx < keys.size(); idx++) {
      Slice pkey(keys[idx]);
      auto& inst = GetDBInstance(keys[idx]);
      s = inst->SIsmember(keys[idx], member, &exist);
      if (s.ok() && exist > 0) {
        continue;
      } else if (!s.IsNotFound()) {
        return s;
      } else {
        break;
      }
    }
    if (exist > 0) {
      members->push_back(member);
    }
  }
  return Status::OK();
}

Status Storage::SInterstore(const Slice& destination, const std::vector<std::string>& keys, std::vector<std::string>& value_to_dest, int32_t* ret) {
  Status s;

  // in codis mode, users should garentee keys will be hashed to same slot
  if (!is_classic_mode_) {
    auto& inst = GetDBInstance(keys[0]);
    s = inst->SInterstore(destination, keys, value_to_dest, ret);
    return s;
  }

  s = SInter(keys, &value_to_dest);
  if (!s.ok()) {
    return s;
  }

  auto& dest_inst = GetDBInstance(destination);
  s = dest_inst->SetsDel(destination);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }

  s = dest_inst->SAdd(destination, value_to_dest, ret);
  return s;
}

Status Storage::SIsmember(const Slice& key, const Slice& member, int32_t* ret) {
  auto& inst = GetDBInstance(key);
  return inst->SIsmember(key, member, ret);
}

Status Storage::SMembers(const Slice& key, std::vector<std::string>* members) {
  auto& inst = GetDBInstance(key);
  return inst->SMembers(key, members);
}

Status Storage::SMembersWithTTL(const Slice& key, std::vector<std::string>* members, int64_t *ttl) {
  auto& inst = GetDBInstance(key);
  return inst->SMembersWithTTL(key, members, ttl);
}

Status Storage::SMove(const Slice& source, const Slice& destination, const Slice& member, int32_t* ret) {
  Status s;

  // in codis mode, users should garentee keys will be hashed to same slot
  if (!is_classic_mode_) {
    auto& inst = GetDBInstance(source);
    s = inst->SMove(source, destination, member, ret);
  }

  auto& src_inst = GetDBInstance(source);
  s = src_inst->SIsmember(source, member, ret);
  if (s.IsNotFound()) {
    *ret = 0;
    return s;
  }
  if (!s.ok()) {
    return s;
  }

  s = src_inst->SRem(source, std::vector<std::string>{member.ToString()}, ret);
  if (!s.ok()) {
    return s;
  }
  auto& dest_inst = GetDBInstance(destination);
  int unused_ret;
  return dest_inst->SAdd(destination, std::vector<std::string>{member.ToString()}, &unused_ret);
}

Status Storage::SPop(const Slice& key, std::vector<std::string>* members, int64_t count) {
  auto& inst = GetDBInstance(key);
  Status status = inst->SPop(key, members, count);
  return status;
}

Status Storage::SRandmember(const Slice& key, int32_t count, std::vector<std::string>* members) {
  auto& inst = GetDBInstance(key);
  return inst->SRandmember(key, count, members);
}

Status Storage::SRem(const Slice& key, const std::vector<std::string>& members, int32_t* ret) {
  auto& inst = GetDBInstance(key);
  return inst->SRem(key, members, ret);
}

Status Storage::SUnion(const std::vector<std::string>& keys, std::vector<std::string>* members) {
  Status s;
  members->clear();
  // in codis mode, users should garentee keys will be hashed to same slot
  if (!is_classic_mode_) {
    auto& inst = GetDBInstance(keys[0]);
    return inst->SUnion(keys, members);
  }

  using Iter = std::vector<std::string>::iterator;
  using Uset = std::unordered_set<std::string>;
  Uset member_set;
  for (const auto& key : keys) {
    std::vector<std::string> vec;
    auto& inst = GetDBInstance(key);
    s = inst->SMembers(key, &vec);
    if (s.IsNotFound()) {
      continue;
    }
    if (!s.ok()) {
      return s;
    }
    std::copy(std::move_iterator<Iter>(vec.begin()),
              std::move_iterator<Iter>(vec.end()),
              std::insert_iterator<Uset>(member_set, member_set.begin()));
  }

  std::copy(member_set.begin(), member_set.end(), std::back_inserter(*members));
  return Status::OK();
}

Status Storage::SUnionstore(const Slice& destination, const std::vector<std::string>& keys, std::vector<std::string>& value_to_dest, int32_t* ret) {
  Status s;
  value_to_dest.clear();

  // in codis mode, users should garentee keys will be hashed to same slot
  if (!is_classic_mode_) {
    auto& inst = GetDBInstance(destination);
    s = inst->SUnionstore(destination, keys, value_to_dest, ret);
    return s;
  }

  s = SUnion(keys, &value_to_dest);
  if (!s.ok()) {
    return s;
  }
  *ret = value_to_dest.size();
  auto& dest_inst = GetDBInstance(destination);
  s = dest_inst->SetsDel(destination);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }
  int unused_ret;
  return dest_inst->SAdd(destination, value_to_dest, &unused_ret);
}

Status Storage::SScan(const Slice& key, int64_t cursor, const std::string& pattern, int64_t count,
                      std::vector<std::string>* members, int64_t* next_cursor) {
  auto& inst = GetDBInstance(key);
  return inst->SScan(key, cursor, pattern, count, members, next_cursor);
}

Status Storage::LPush(const Slice& key, const std::vector<std::string>& values, uint64_t* ret) {
  auto& inst = GetDBInstance(key);
  return inst->LPush(key, values, ret);
}

Status Storage::RPush(const Slice& key, const std::vector<std::string>& values, uint64_t* ret) {
  auto& inst = GetDBInstance(key);
  return inst->RPush(key, values, ret);
}

Status Storage::LRange(const Slice& key, int64_t start, int64_t stop, std::vector<std::string>* ret) {
  ret->clear();
  auto& inst = GetDBInstance(key);
  return inst->LRange(key, start, stop, ret);
}

Status Storage::LRangeWithTTL(const Slice& key, int64_t start, int64_t stop, std::vector<std::string>* ret, int64_t *ttl) {
  auto& inst = GetDBInstance(key);
  return inst->LRangeWithTTL(key, start, stop, ret, ttl);
}

Status Storage::LTrim(const Slice& key, int64_t start, int64_t stop) {
  auto& inst = GetDBInstance(key);
  return inst->LTrim(key, start, stop);
}

Status Storage::LLen(const Slice& key, uint64_t* len) {
  auto& inst = GetDBInstance(key);
  return inst->LLen(key, len);
}

Status Storage::LPop(const Slice& key, int64_t count, std::vector<std::string>* elements) {
  elements->clear();
  auto& inst = GetDBInstance(key);
  return inst->LPop(key, count, elements);
}

Status Storage::RPop(const Slice& key, int64_t count, std::vector<std::string>* elements) {
  elements->clear();
  auto& inst = GetDBInstance(key);
  return inst->RPop(key, count, elements);
}

Status Storage::LIndex(const Slice& key, int64_t index, std::string* element) {
  element->clear();
  auto& inst = GetDBInstance(key);
  return inst->LIndex(key, index, element);
}

Status Storage::LInsert(const Slice& key, const BeforeOrAfter& before_or_after, const std::string& pivot,
                        const std::string& value, int64_t* ret) {
  auto& inst = GetDBInstance(key);
  return inst->LInsert(key, before_or_after, pivot, value, ret);
}

Status Storage::LPushx(const Slice& key, const std::vector<std::string>& values, uint64_t* len) {
  auto& inst = GetDBInstance(key);
  return inst->LPushx(key, values, len);
}

Status Storage::RPushx(const Slice& key, const std::vector<std::string>& values, uint64_t* len) {
  auto& inst = GetDBInstance(key);
  return inst->RPushx(key, values, len);
}

Status Storage::LRem(const Slice& key, int64_t count, const Slice& value, uint64_t* ret) {
  auto& inst = GetDBInstance(key);
  return inst->LRem(key, count, value, ret);
}

Status Storage::LSet(const Slice& key, int64_t index, const Slice& value) {
  auto& inst = GetDBInstance(key);
  return inst->LSet(key, index, value);
}

Status Storage::RPoplpush(const Slice& source, const Slice& destination, std::string* element) {
  Status s;
  element->clear();

  // in codis mode, users should garentee keys will be hashed to same slot
  if (!is_classic_mode_) {
    auto& inst = GetDBInstance(source);
    s = inst->RPoplpush(source, destination, element);
    return s;
  }

  auto& source_inst = GetDBInstance(source);
  if (source.compare(destination) == 0) {
    s = source_inst->RPoplpush(source, destination, element);
    return s;
  }

  std::vector<std::string> elements;
  s = source_inst->RPop(source, 1, &elements);
  if (!s.ok()) {
    return s;
  }
  *element = elements.front();
  auto& dest_inst = GetDBInstance(destination);
  uint64_t ret;
  s = dest_inst->LPush(destination, elements, &ret);
  return s;
}

Status Storage::ZPopMax(const Slice& key, const int64_t count, std::vector<ScoreMember>* score_members) {
  score_members->clear();
  auto& inst = GetDBInstance(key);
  return inst->ZPopMax(key, count, score_members);
}

Status Storage::ZPopMin(const Slice& key, const int64_t count, std::vector<ScoreMember>* score_members) {
  score_members->clear();
  auto& inst = GetDBInstance(key);
  return inst->ZPopMin(key, count, score_members);
}

Status Storage::ZAdd(const Slice& key, const std::vector<ScoreMember>& score_members, int32_t* ret) {
  auto& inst = GetDBInstance(key);
  return inst->ZAdd(key, score_members, ret);
}

Status Storage::ZCard(const Slice& key, int32_t* ret) {
  auto& inst = GetDBInstance(key);
  return inst->ZCard(key, ret);
}

Status Storage::ZCount(const Slice& key, double min, double max, bool left_close, bool right_close, int32_t* ret) {
  auto& inst = GetDBInstance(key);
  return inst->ZCount(key, min, max, left_close, right_close, ret);
}

Status Storage::ZIncrby(const Slice& key, const Slice& member, double increment, double* ret) {
  auto& inst = GetDBInstance(key);
  return inst->ZIncrby(key, member, increment, ret);
}

Status Storage::ZRange(const Slice& key, int32_t start, int32_t stop, std::vector<ScoreMember>* score_members) {
  score_members->clear();
  auto& inst = GetDBInstance(key);
  return inst->ZRange(key, start, stop, score_members);
}
Status Storage::ZRangeWithTTL(const Slice& key, int32_t start, int32_t stop, std::vector<ScoreMember>* score_members,
                                 int64_t *ttl) {
  score_members->clear();
  auto& inst = GetDBInstance(key);
  return inst->ZRangeWithTTL(key, start, stop, score_members, ttl);
}

Status Storage::ZRangebyscore(const Slice& key, double min, double max, bool left_close, bool right_close,
                              std::vector<ScoreMember>* score_members) {
  // maximum number of zset is std::numeric_limits<int32_t>::max()
  score_members->clear();
  auto& inst = GetDBInstance(key);
  return inst->ZRangebyscore(key, min, max, left_close, right_close, std::numeric_limits<int32_t>::max(), 0,
                                  score_members);
}

Status Storage::ZRangebyscore(const Slice& key, double min, double max, bool left_close, bool right_close,
                              int64_t count, int64_t offset, std::vector<ScoreMember>* score_members) {
  score_members->clear();
  auto& inst = GetDBInstance(key);
  return inst->ZRangebyscore(key, min, max, left_close, right_close, count, offset, score_members);
}

Status Storage::ZRank(const Slice& key, const Slice& member, int32_t* rank) {
  auto& inst = GetDBInstance(key);
  return inst->ZRank(key, member, rank);
}

Status Storage::ZRem(const Slice& key, const std::vector<std::string>& members, int32_t* ret) {
  auto& inst = GetDBInstance(key);
  return inst->ZRem(key, members, ret);
}

Status Storage::ZRemrangebyrank(const Slice& key, int32_t start, int32_t stop, int32_t* ret) {
  auto& inst = GetDBInstance(key);
  return inst->ZRemrangebyrank(key, start, stop, ret);
}

Status Storage::ZRemrangebyscore(const Slice& key, double min, double max, bool left_close, bool right_close,
                                 int32_t* ret) {
  auto& inst = GetDBInstance(key);
  return inst->ZRemrangebyscore(key, min, max, left_close, right_close, ret);
}

Status Storage::ZRevrangebyscore(const Slice& key, double min, double max, bool left_close, bool right_close,
                                 int64_t count, int64_t offset, std::vector<ScoreMember>* score_members) {
  score_members->clear();
  auto& inst = GetDBInstance(key);
  return inst->ZRevrangebyscore(key, min, max, left_close, right_close, count, offset, score_members);
}

Status Storage::ZRevrange(const Slice& key, int32_t start, int32_t stop, std::vector<ScoreMember>* score_members) {
  score_members->clear();
  auto& inst = GetDBInstance(key);
  return inst->ZRevrange(key, start, stop, score_members);
}

Status Storage::ZRevrangebyscore(const Slice& key, double min, double max, bool left_close, bool right_close,
                                 std::vector<ScoreMember>* score_members) {
  // maximum number of zset is std::numeric_limits<int32_t>::max()
  score_members->clear();
  auto& inst = GetDBInstance(key);
  return inst->ZRevrangebyscore(key, min, max, left_close, right_close, std::numeric_limits<int32_t>::max(),
                                              0, score_members);
}

Status Storage::ZRevrank(const Slice& key, const Slice& member, int32_t* rank) {
  auto& inst = GetDBInstance(key);
  return inst->ZRevrank(key, member, rank);
}

Status Storage::ZScore(const Slice& key, const Slice& member, double* ret) {
  auto& inst = GetDBInstance(key);
  return inst->ZScore(key, member, ret);
}

Status Storage::ZUnionstore(const Slice& destination, const std::vector<std::string>& keys,
                            const std::vector<double>& weights, const AGGREGATE agg,
                            std::map<std::string, double>& value_to_dest, int32_t* ret) {
  value_to_dest.clear();
  Status s;

  // in codis mode, users should garentee keys will be hashed to same slot
  if (!is_classic_mode_) {
    auto& inst = GetDBInstance(keys[0]);
    s = inst->ZUnionstore(destination, keys, weights, agg, value_to_dest, ret);
    return s;
  }

  for (int idx = 0; idx < keys.size(); idx++) {
    Slice key = Slice(keys[idx]);
    auto& inst = GetDBInstance(key);
    std::map<std::string, double> member_to_score;
    double weight = idx >= weights.size() ? 1 : weights[idx];
    s = inst->ZGetAll(key, weight, &member_to_score);
    if (!s.ok() && !s.IsNotFound()) {
      return s;
    }
    for (const auto& key_score : member_to_score) {
      const std::string& member = key_score.first;
      double score = key_score.second;
      if (value_to_dest.find(member) == value_to_dest.end()) {
        value_to_dest[member] = score;
        continue;
      }
      switch (agg) {
        case SUM:
          score += value_to_dest[member];
          break;
        case MIN:
          score = std::min(value_to_dest[member], score);
          break;
        case MAX:
          score = std::max(value_to_dest[member], score);
          break;
      }
      value_to_dest[member] = (score == -0.0) ? 0 : score;
    }
  }

  BaseMetaKey base_destination(destination);
  auto& inst = GetDBInstance(destination);
  s = inst->ZsetsDel(destination);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }
  std::vector<ScoreMember> score_members;
  std::for_each(value_to_dest.begin(), value_to_dest.end(), [&score_members](auto kv) {
      score_members.emplace_back(kv.second, kv.first);
      });
  *ret = score_members.size();
  int unused_ret;
  return inst->ZAdd(destination, score_members, &unused_ret);
}

Status Storage::ZInterstore(const Slice& destination, const std::vector<std::string>& keys,
                            const std::vector<double>& weights, const AGGREGATE agg,
                            std::vector<ScoreMember>& value_to_dest, int32_t* ret) {
  Status s;
  value_to_dest.clear();

  // in codis mode, users should garentee keys will be hashed to same slot
  if (!is_classic_mode_) {
    auto& inst = GetDBInstance(keys[0]);
    s = inst->ZInterstore(destination, keys, weights, agg, value_to_dest, ret);
    return s;
  }

  Slice key = Slice(keys[0]);
  auto& inst = GetDBInstance(key);
  std::map<std::string, double> member_to_score;
  double weight = weights.empty() ? 1 : weights[0];
  s = inst->ZGetAll(key, weight, &member_to_score);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }

  for (const auto member_score : member_to_score) {
    std::string member = member_score.first;
    double score = member_score.second;
    bool reliable = true;

    for (int idx = 1; idx < keys.size(); idx++) {
      double weight = idx >= weights.size() ? 1 : weights[idx];
      auto& inst = GetDBInstance(keys[idx]);
      double ret_score;
      s = inst->ZScore(keys[idx], member, &ret_score);
      if (!s.ok() && !s.IsNotFound()) {
        return s;
      }
      if (s.IsNotFound()) {
        reliable = false;
        break;
      }
      switch (agg) {
        case SUM:
          score += ret_score * weight;
          break;
        case MIN:
          score = std::min(score, ret_score * weight);
          break;
        case MAX:
          score = std::max(score, ret_score * weight);
          break;
      }
    }
    if (reliable) {
      value_to_dest.emplace_back(score, member);
    }
  }

  BaseMetaKey base_destination(destination);
  auto& ninst = GetDBInstance(destination);

  s = ninst->ZsetsDel(destination);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }
  *ret = value_to_dest.size();
  int unused_ret;
  return ninst->ZAdd(destination, value_to_dest, &unused_ret);
}

Status Storage::ZRangebylex(const Slice& key, const Slice& min, const Slice& max, bool left_close,
                            bool right_close, std::vector<std::string>* members) {
  members->clear();
  auto& inst = GetDBInstance(key);
  return inst->ZRangebylex(key, min, max, left_close, right_close, members);
}

Status Storage::ZLexcount(const Slice& key, const Slice& min, const Slice& max, bool left_close,
                          bool right_close, int32_t* ret) {
  auto& inst = GetDBInstance(key);
  return inst->ZLexcount(key, min, max, left_close, right_close, ret);
}

Status Storage::ZRemrangebylex(const Slice& key, const Slice& min, const Slice& max,
                               bool left_close, bool right_close, int32_t* ret) {
  auto& inst = GetDBInstance(key);
  return inst->ZRemrangebylex(key, min, max, left_close, right_close, ret);
}

Status Storage::ZScan(const Slice& key, int64_t cursor, const std::string& pattern, int64_t count,
                      std::vector<ScoreMember>* score_members, int64_t* next_cursor) {
  score_members->clear();
  auto& inst = GetDBInstance(key);
  return inst->ZScan(key, cursor, pattern, count, score_members, next_cursor);
}

Status Storage::XAdd(const Slice& key, const std::string& serialized_message, StreamAddTrimArgs& args) {
  auto& inst = GetDBInstance(key);
  return inst->XAdd(key, serialized_message, args);
}

Status Storage::XDel(const Slice& key, const std::vector<streamID>& ids, int32_t& ret) {
  auto& inst = GetDBInstance(key);
  return inst->XDel(key, ids, ret);
}

Status Storage::XTrim(const Slice& key, StreamAddTrimArgs& args, int32_t& count) {
  auto& inst = GetDBInstance(key);
  return inst->XTrim(key, args, count);
}

Status Storage::XRange(const Slice& key, const StreamScanArgs& args, std::vector<IdMessage>& id_messages) {
  auto& inst = GetDBInstance(key);
  return inst->XRange(key, args, id_messages);
}

Status Storage::XRevrange(const Slice& key, const StreamScanArgs& args, std::vector<IdMessage>& id_messages) {
  auto& inst = GetDBInstance(key);
  return inst->XRevrange(key, args, id_messages);
}

Status Storage::XLen(const Slice& key, int32_t& len) {
  auto& inst = GetDBInstance(key);
  return inst->XLen(key, len);
}

Status Storage::XRead(const StreamReadGroupReadArgs& args, std::vector<std::vector<storage::IdMessage>>& results,
              std::vector<std::string>& reserved_keys) {
  Status s;
  for (int i = 0; i < args.unparsed_ids.size(); i++) {
    StreamReadGroupReadArgs single_args;
    single_args.keys.push_back(args.keys[i]);
    single_args.unparsed_ids.push_back(args.unparsed_ids[i]);
    single_args.count = args.count;
    single_args.block = args.block;
    single_args.group_name = args.group_name;
    single_args.consumer_name = args.consumer_name;
    single_args.noack_ = args.noack_;
    auto& inst = GetDBInstance(args.keys[i]);
    s = inst->XRead(single_args, results, reserved_keys);
    if (!s.ok() && !s.IsNotFound()) {
      return s;
    }
  }
  return s;
}

Status Storage::XInfo(const Slice& key, StreamInfoResult &result) {
  auto& inst = GetDBInstance(key);
  return inst->XInfo(key, result);
}

// Keys Commands
int32_t Storage::Expire(const Slice& key, int64_t ttl, std::map<DataType, Status>* type_status) {
  type_status->clear();
  int32_t ret = 0;
  bool is_corruption = false;

  auto& inst = GetDBInstance(key);
  // Strings
  Status s = inst->StringsExpire(key, ttl);
  if (s.ok()) {
    ret++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kStrings] = s;
  }

  // Hash
  s = inst->HashesExpire(key, ttl);
  if (s.ok()) {
    ret++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kHashes] = s;
  }

  // Sets
  s = inst->SetsExpire(key, ttl);
  if (s.ok()) {
    ret++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kSets] = s;
  }

  // Lists
  s = inst->ListsExpire(key, ttl);
  if (s.ok()) {
    ret++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kLists] = s;
  }

  // Zsets
  s = inst->ZsetsExpire(key, ttl);
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
    auto& inst = GetDBInstance(key);
    // Strings
    Status s = inst->StringsDel(key);
    if (s.ok()) {
      count++;
    } else if (!s.IsNotFound()) {
      is_corruption = true;
      (*type_status)[DataType::kStrings] = s;
    }

    // Hashes
    s = inst->HashesDel(key);
    if (s.ok()) {
      count++;
    } else if (!s.IsNotFound()) {
      is_corruption = true;
      (*type_status)[DataType::kHashes] = s;
    }

    // Sets
    s = inst->SetsDel(key);
    if (s.ok()) {
      count++;
    } else if (!s.IsNotFound()) {
      is_corruption = true;
      (*type_status)[DataType::kSets] = s;
    }

    // Lists
    s = inst->ListsDel(key);
    if (s.ok()) {
      count++;
    } else if (!s.IsNotFound()) {
      is_corruption = true;
      (*type_status)[DataType::kLists] = s;
    }

    // ZSets
    s = inst->ZsetsDel(key);
    if (s.ok()) {
      count++;
    } else if (!s.IsNotFound()) {
      is_corruption = true;
      (*type_status)[DataType::kZSets] = s;
    }

    // Streams
    s = inst->StreamsDel(key);
    if (s.ok()) {
      count++;
    } else if (!s.IsNotFound()) {
      is_corruption = true;
      (*type_status)[DataType::kStreams] = s;
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
    auto& inst = GetDBInstance(key);
    switch (type) {
      // Strings
      case DataType::kStrings: {
        s = inst->StringsDel(key);
        if (s.ok()) {
          count++;
        } else if (!s.IsNotFound()) {
          is_corruption = true;
        }
        break;
      }
      // Hashes
      case DataType::kHashes: {
        s = inst->HashesDel(key);
        if (s.ok()) {
          count++;
        } else if (!s.IsNotFound()) {
          is_corruption = true;
        }
        break;
      }
      // Sets
      case DataType::kSets: {
        s = inst->SetsDel(key);
        if (s.ok()) {
          count++;
        } else if (!s.IsNotFound()) {
          is_corruption = true;
        }
        break;
      }
      // Lists
      case DataType::kLists: {
        s = inst->ListsDel(key);
        if (s.ok()) {
          count++;
        } else if (!s.IsNotFound()) {
          is_corruption = true;
        }
        break;
      }
      // ZSets
      case DataType::kZSets: {
        s = inst->ZsetsDel(key);
        if (s.ok()) {
          count++;
        } else if (!s.IsNotFound()) {
          is_corruption = true;
        }
        break;
      }
      // Stream
      case DataType::kStreams: {
        s = inst->StreamsDel(key);
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
    auto& inst = GetDBInstance(key);
    s = inst->Get(key, &value);
    if (s.ok()) {
      count++;
    } else if (!s.IsNotFound()) {
      is_corruption = true;
      (*type_status)[DataType::kStrings] = s;
    }

    s = inst->HLen(key, &ret);
    if (s.ok()) {
      count++;
    } else if (!s.IsNotFound()) {
      is_corruption = true;
      (*type_status)[DataType::kHashes] = s;
    }

    s = inst->SCard(key, &ret);
    if (s.ok()) {
      count++;
    } else if (!s.IsNotFound()) {
      is_corruption = true;
      (*type_status)[DataType::kSets] = s;
    }

    s = inst->LLen(key, &llen);
    if (s.ok()) {
      count++;
    } else if (!s.IsNotFound()) {
      is_corruption = true;
      (*type_status)[DataType::kLists] = s;
    }

    s = inst->ZCard(key, &ret);
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
  assert(is_classic_mode_);
  keys->clear();
  bool is_finish;
  int64_t leftover_visits = count;
  int64_t step_length = count;
  int64_t cursor_ret = 0;
  std::string start_key;
  std::string next_key;
  std::string prefix;
  char key_type;

  // invalid cursor
  if (cursor < 0) {
    return cursor_ret;
  }

  // get seek by corsor
  prefix = isTailWildcard(pattern) ? pattern.substr(0, pattern.size() - 1) : "";
  Status s = LoadCursorStartKey(dtype, cursor, &key_type, &start_key);
  if (!s.ok()) {
    // If want to scan all the databases, we start with the strings database
    key_type = dtype == DataType::kAll ? DataTypeTag[DataType::kStrings] : DataTypeTag[dtype];
    start_key = prefix;
    cursor = 0;
  }

  // collect types to scan
  std::vector<char> types;
  if (DataType::kAll == dtype) {
    auto iter_end = std::end(DataTypeTag);
    auto pos = std::find(std::begin(DataTypeTag), iter_end, key_type);
    if (pos == iter_end) {
      LOG(WARNING) << "Invalid key_type: " << key_type;
      return 0;
    }
    std::copy(pos, iter_end, std::back_inserter(types));
  } else {
    types.push_back(DataTypeTag[dtype]);
  }

  for (const auto& type : types) {
    std::vector<IterSptr> inst_iters;
    for (const auto& inst : insts_) {
      IterSptr iter_sptr;
      iter_sptr.reset(inst->CreateIterator(type, pattern,
          nullptr/*lower_bound*/, nullptr/*upper_bound*/));
      inst_iters.push_back(iter_sptr);
    }

    BaseMetaKey base_start_key(start_key);
    MergingIterator miter(inst_iters);
    miter.Seek(base_start_key.Encode().ToString());
    while (miter.Valid() && count > 0) {
      keys->push_back(miter.Key());
      miter.Next();
      count--;
    }

    bool is_finish = !miter.Valid();
    if (miter.Valid() &&
      (miter.Key().compare(prefix) <= 0 ||
       miter.Key().substr(0, prefix.size()) == prefix)) {
      is_finish = false;
    }

    // for specific type scan, reach the end
    if (is_finish && dtype != DataType::kAll) {
      return cursor_ret;
    }

    // already get count's element, while iterator is still valid,
    // store cursor
    if (!is_finish) {
      next_key = miter.Key();
      cursor_ret = cursor + step_length;
      StoreCursorStartKey(dtype, cursor_ret, type, next_key);
      return cursor_ret;
    }

    // for all type scan, move to next type, reset start_key
    start_key = prefix;
  }
  return cursor_ret;
}

Status Storage::PKScanRange(const DataType& data_type, const Slice& key_start, const Slice& key_end,
                            const Slice& pattern, int32_t limit, std::vector<std::string>* keys,
                            std::vector<KeyValue>* kvs, std::string* next_key) {
  next_key->clear();
  std::string key;
  std::string value;

  BaseMetaKey base_key_start(key_start);
  BaseMetaKey base_key_end(key_end);
  Slice base_key_end_slice(base_key_end.Encode());

  bool start_no_limit = key_start.empty();
  bool end_no_limit = key_end.empty();
  if (!start_no_limit && !end_no_limit && key_start.compare(key_end) > 0) {
    return Status::InvalidArgument("error in given range");
  }

  std::vector<IterSptr> inst_iters;
  for (const auto& inst : insts_) {
    IterSptr iter_sptr;
    iter_sptr.reset(inst->CreateIterator(data_type, pattern.ToString(),
        nullptr/*lower_bound*/, nullptr/*upper_bound*/));
    inst_iters.push_back(iter_sptr);
  }

  MergingIterator miter(inst_iters);
  if (start_no_limit) {
    miter.SeekToFirst();
  } else {
    std::string temp = base_key_start.Encode().ToString();
    miter.Seek(temp);
  }

  while (miter.Valid() && limit > 0 &&
      (end_no_limit || miter.Key().compare(key_end.ToString()) <= 0)) {
    if (data_type == DataType::kStrings) {
      kvs->push_back({miter.Key(), miter.Value()});
    } else {
      keys->push_back(miter.Key());
    }
    limit--;
    miter.Next();
  }

  if (miter.Valid() && (end_no_limit || miter.Key().compare(key_end.ToString()) <= 0)) {
    *next_key = miter.Key();
  }
  return Status::OK();
}

Status Storage::PKRScanRange(const DataType& data_type, const Slice& key_start, const Slice& key_end,
                             const Slice& pattern, int32_t limit, std::vector<std::string>* keys,
                             std::vector<KeyValue>* kvs, std::string* next_key) {
  next_key->clear();
  std::string key, value;
  BaseMetaKey base_key_start(key_start);
  BaseMetaKey base_key_end(key_end);
  Slice base_key_start_slice = Slice(base_key_start.Encode());

  bool start_no_limit = key_start.empty();
  bool end_no_limit = key_end.empty();

  if (!start_no_limit && !end_no_limit && key_start.compare(key_end) < 0) {
    return Status::InvalidArgument("error in given range");
  }

  std::vector<IterSptr> inst_iters;
  for (const auto& inst : insts_) {
    IterSptr iter_sptr;
    iter_sptr.reset(inst->CreateIterator(data_type, pattern.ToString(),
        nullptr/*lower_bound*/, nullptr/*upper_bound*/));
    inst_iters.push_back(iter_sptr);
  }
  MergingIterator miter(inst_iters);
  if (start_no_limit) {
    miter.SeekToLast();
  } else {
    miter.SeekForPrev(base_key_start.Encode().ToString());
  }

  while (miter.Valid() && limit > 0 &&
      (end_no_limit || miter.Key().compare(key_end.ToString()) >= 0)) {
    if (data_type == DataType::kStrings) {
      kvs->push_back({miter.Key(), miter.Value()});
    } else {
      keys->push_back(miter.Key());
    }
    limit--;
    miter.Prev();
  }

  if (miter.Valid() && (end_no_limit || miter.Key().compare(key_end.ToString()) >= 0)) {
    *next_key = miter.Key();
  }
  return Status::OK();
}

Status Storage::PKPatternMatchDel(const DataType& data_type, const std::string& pattern, int32_t* ret) {
  Status s;
  for (const auto& inst : insts_) {
    switch (data_type) {
      case DataType::kStrings: {
        s = inst->StringsPKPatternMatchDel(pattern, ret);
        if (!s.ok()) {
          return s;
        }
      }
      case DataType::kHashes: {
        s = inst->HashesPKPatternMatchDel(pattern, ret);
        if (!s.ok()) {
          return s;
        }
      }
      case DataType::kLists: {
        s = inst->ListsPKPatternMatchDel(pattern, ret);
        if (!s.ok()) {
          return s;
        }
      }
      case DataType::kZSets: {
        s = inst->ZsetsPKPatternMatchDel(pattern, ret);
        if (!s.ok()) {
          return s;
        }
      }
      case DataType::kSets: {
        s = inst->SetsPKPatternMatchDel(pattern, ret);
        if (!s.ok()) {
          return s;
        }
      }
      default:
        s = Status::Corruption("Unsupported data types");
        break;
    }
  }
  return s;
}

Status Storage::Scanx(const DataType& data_type, const std::string& start_key, const std::string& pattern,
                      int64_t count, std::vector<std::string>* keys, std::string* next_key) {
  Status s;
  keys->clear();
  next_key->clear();

  std::vector<IterSptr> inst_iters;
  for (const auto& inst : insts_) {
    IterSptr iter_sptr;
    iter_sptr.reset(inst->CreateIterator(data_type, pattern,
        nullptr/*lower_bound*/, nullptr/*upper_bound*/));
    inst_iters.push_back(iter_sptr);
  }

  BaseMetaKey base_start_key(start_key);
  MergingIterator miter(inst_iters);
  miter.Seek(base_start_key.Encode().ToString());
  while (miter.Valid() && count > 0) {
    keys->push_back(miter.Key());
    miter.Next();
    count--;
  }

  std::string prefix = isTailWildcard(pattern) ? pattern.substr(0, pattern.size() - 1) : "";
  if (miter.Valid() && (miter.Key().compare(prefix) <= 0 || miter.Key().substr(0, prefix.size()) == prefix)) {
    *next_key = miter.Key();
  } else {
    *next_key = "";
  }
  return Status::OK();
}

int32_t Storage::Expireat(const Slice& key, int64_t timestamp, std::map<DataType, Status>* type_status) {
  Status s;
  int32_t count = 0;
  bool is_corruption = false;

  auto& inst = GetDBInstance(key);
  s = inst->StringsExpireat(key, timestamp);
  if (s.ok()) {
    count++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kStrings] = s;
  }

  s = inst->HashesExpireat(key, timestamp);
  if (s.ok()) {
    count++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kHashes] = s;
  }

  s = inst->SetsExpireat(key, timestamp);
  if (s.ok()) {
    count++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kSets] = s;
  }

  s = inst->ListsExpireat(key, timestamp);
  if (s.ok()) {
    count++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kLists] = s;
  }

  s = inst->ZsetsExpireat(key, timestamp);
  if (s.ok()) {
    count++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kZSets] = s;
  }

  if (is_corruption) {
    return -1;
  }
  return count;
}

int32_t Storage::Persist(const Slice& key, std::map<DataType, Status>* type_status) {
  Status s;
  int32_t count = 0;
  bool is_corruption = false;

  auto& inst = GetDBInstance(key);
  s = inst->StringsPersist(key);
  if (s.ok()) {
    count++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kStrings] = s;
  }

  s = inst->HashesPersist(key);
  if (s.ok()) {
    count++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kHashes] = s;
  }

  s = inst->SetsPersist(key);
  if (s.ok()) {
    count++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kSets] = s;
  }

  s = inst->ListsPersist(key);
  if (s.ok()) {
    count++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kLists] = s;
  }

  s = inst->ZsetsPersist(key);
  if (s.ok()) {
    count++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kZSets] = s;
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

  auto& inst = GetDBInstance(key);
  s = inst->StringsTTL(key, &timestamp);
  if (s.ok() || s.IsNotFound()) {
    ret[DataType::kStrings] = timestamp;
  } else if (!s.IsNotFound()) {
    ret[DataType::kStrings] = -3;
    (*type_status)[DataType::kStrings] = s;
  }

  s = inst->HashesTTL(key, &timestamp);
  if (s.ok() || s.IsNotFound()) {
    ret[DataType::kHashes] = timestamp;
  } else if (!s.IsNotFound()) {
    ret[DataType::kHashes] = -3;
    (*type_status)[DataType::kHashes] = s;
  }

  s = inst->ListsTTL(key, &timestamp);
  if (s.ok() || s.IsNotFound()) {
    ret[DataType::kLists] = timestamp;
  } else if (!s.IsNotFound()) {
    ret[DataType::kLists] = -3;
    (*type_status)[DataType::kLists] = s;
  }

  s = inst->SetsTTL(key, &timestamp);
  if (s.ok() || s.IsNotFound()) {
    ret[DataType::kSets] = timestamp;
  } else if (!s.IsNotFound()) {
    ret[DataType::kSets] = -3;
    (*type_status)[DataType::kSets] = s;
  }

  s = inst->ZsetsTTL(key, &timestamp);
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
  auto& inst = GetDBInstance(key);
  s = inst->Get(key, &value);
  if (s.ok()) {
    types.emplace_back("string");
  } else if (!s.IsNotFound()) {
    return s;
  }
  if (single && !types.empty()) {
    return s;
  }

  int32_t hashes_len = 0;
  s = inst->HLen(key, &hashes_len);
  if (s.ok() && hashes_len != 0) {
    types.emplace_back("hash");
  } else if (!s.IsNotFound()) {
    return s;
  }
  if (single && !types.empty()) {
    return s;
  }

  uint64_t lists_len = 0;
  s = inst->LLen(key, &lists_len);
  if (s.ok() && lists_len != 0) {
    types.emplace_back("list");
  } else if (!s.IsNotFound()) {
    return s;
  }
  if (single && !types.empty()) {
    return s;
  }

  int32_t zsets_size = 0;
  s = inst->ZCard(key, &zsets_size);
  if (s.ok() && zsets_size != 0) {
    types.emplace_back("zset");
  } else if (!s.IsNotFound()) {
    return s;
  }
  if (single && !types.empty()) {
    return s;
  }

  int32_t sets_size = 0;
  s = inst->SCard(key, &sets_size);
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
  keys->clear();
  std::vector<DataType> types;
  if (data_type == DataType::kAll) {
    types.push_back(DataType::kStrings);
    types.push_back(DataType::kHashes);
    types.push_back(DataType::kLists);
    types.push_back(DataType::kZSets);
    types.push_back(DataType::kSets);
    types.push_back(DataType::kStreams);
  } else {
    types.push_back(data_type);
  }

  for (const auto& type : types) {
    std::vector<IterSptr> inst_iters;
    for (const auto& inst : insts_) {
      IterSptr inst_iter;
      inst_iter.reset(inst->CreateIterator(type, pattern,
          nullptr/*lower_bound*/, nullptr/*upper_bound*/));
      inst_iters.push_back(inst_iter);
    }

    MergingIterator miter(inst_iters);
    miter.SeekToFirst();
    while (miter.Valid()) {
      keys->push_back(miter.Key());
      miter.Next();
    }
  }

  return Status::OK();
}

void Storage::ScanDatabase(const DataType& type) {
  for (const auto& inst : insts_) {
    switch (type) {
      case kStrings:
        inst->ScanStrings();
        break;
      case kHashes:
        inst->ScanHashes();
        break;
      case kSets:
        inst->ScanSets();
        break;
      case kZSets:
        inst->ScanZsets();
        break;
      case kLists:
        inst->ScanLists();
        break;
      case kStreams:
        // do noting
        break;
      case kAll:
        inst->ScanStrings();
        inst->ScanHashes();
        inst->ScanSets();
        inst->ScanZsets();
        inst->ScanLists();
        break;
    }
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
  auto& inst = GetDBInstance(key);
  Status s = inst->Get(key, &value);
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
  s = inst->Set(key, result);
  return s;
}

Status Storage::PfCount(const std::vector<std::string>& keys, int64_t* result) {
  if (keys.size() >= kMaxKeys || keys.empty()) {
    return Status::InvalidArgument("Invalid the number of key");
  }

  std::string value;
  std::string first_registers;
  auto& inst = GetDBInstance(keys[0]);
  Status s = inst->Get(keys[0], &value);
  if (s.ok()) {
    first_registers = std::string(value.data(), value.size());
  } else if (s.IsNotFound()) {
    first_registers = "";
  }

  HyperLogLog first_log(kPrecision, first_registers);
  for (size_t i = 1; i < keys.size(); ++i) {
    std::string value;
    std::string registers;
    auto& inst = GetDBInstance(keys[i]);
    s = inst->Get(keys[i], &value);
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
  auto& inst = GetDBInstance(keys[0]);
  s = inst->Get(keys[0], &value);
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
    auto& tmp_inst = GetDBInstance(keys[i]);
    s = tmp_inst->Get(keys[i], &value);
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
  auto& ninst = GetDBInstance(keys[0]);
  s = ninst->Set(keys[0], result);
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
    std::unique_lock<std::mutex> lock(bg_tasks_mutex_);
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
      DoCompactRange(task.type, "", "");
    } else if (task.operation == kCompactRange) {
      if (task.argv.size() == 1) {
        DoCompactSpecificKey(task.type, task.argv[0]);
      }
      if (task.argv.size() == 2) {
        DoCompactRange(task.type, task.argv.front(), task.argv.back());
      }
    }
  }
  return Status::OK();
}

Status Storage::Compact(const DataType& type, bool sync) {
  if (sync) {
    return DoCompactRange(type, "", "");
  } else {
    AddBGTask({type, kCleanAll});
  }
  return Status::OK();
}

// run compactrange for all rocksdb instance
Status Storage::DoCompactRange(const DataType& type, const std::string& start, const std::string& end) {
  if (type != kAll && type != kStrings && type != kHashes && type != kSets && type != kZSets && type != kLists) {
    return Status::InvalidArgument("");
  }

  std::string start_key, end_key;
  CalculateStartAndEndKey(start, &start_key, nullptr);
  CalculateStartAndEndKey(end, nullptr, &end_key);
  Slice slice_start_key(start_key);
  Slice slice_end_key(end_key);
  Slice* start_ptr = slice_start_key.empty() ? nullptr : &slice_start_key;
  Slice* end_ptr = slice_end_key.empty() ? nullptr : &slice_end_key;

  Status s;
  for (const auto& inst : insts_) {
    switch (type) {
      case DataType::kStrings:
        current_task_type_ = Operation::kCleanStrings;
        s = inst->CompactRange(type, start_ptr, end_ptr);
        break;
      case DataType::kHashes:
        current_task_type_ = Operation::kCleanHashes;
        s = inst->CompactRange(type, start_ptr, end_ptr);
        break;
      case DataType::kLists:
        current_task_type_ = Operation::kCleanLists;
        s = inst->CompactRange(type, start_ptr, end_ptr);
        break;
      case DataType::kSets:
        current_task_type_ = Operation::kCleanSets;
        s = inst->CompactRange(type, start_ptr, end_ptr);
        break;
      case DataType::kZSets:
        current_task_type_ = Operation::kCleanZSets;
        s = inst->CompactRange(type, start_ptr, end_ptr);
        break;
      default:
        current_task_type_ = Operation::kCleanAll;
        s = inst->CompactRange(DataType::kStrings, start_ptr, end_ptr);
        s = inst->CompactRange(DataType::kHashes, start_ptr, end_ptr);
        s = inst->CompactRange(DataType::kLists, start_ptr, end_ptr);
        s = inst->CompactRange(DataType::kSets, start_ptr, end_ptr);
        s = inst->CompactRange(DataType::kZSets, start_ptr, end_ptr);
    }
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

Status Storage::DoCompactSpecificKey(const DataType& type, const std::string& key) {
  Status s;
  auto& inst = GetDBInstance(key);

  std::string start_key;
  std::string end_key;
  CalculateStartAndEndKey(key, &start_key, &end_key);
  Slice slice_begin(start_key);
  Slice slice_end(end_key);
  s = inst->CompactRange(type, &slice_begin, &slice_end, kMeta);
  return s;
}

Status Storage::SetMaxCacheStatisticKeys(uint32_t max_cache_statistic_keys) {
  for (const auto& inst : insts_) {
    inst->SetMaxCacheStatisticKeys(max_cache_statistic_keys);
  }
  return Status::OK();
}

Status Storage::SetSmallCompactionThreshold(uint32_t small_compaction_threshold) {
  for (const auto& inst: insts_) {
    inst->SetSmallCompactionThreshold(small_compaction_threshold);
  }
  return Status::OK();
}

Status Storage::SetSmallCompactionDurationThreshold(uint32_t small_compaction_duration_threshold) {
  for (const auto& inst : insts_) {
    inst->SetSmallCompactionDurationThreshold(small_compaction_duration_threshold);
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
    case kCleanStreams:
      return "Stream";
    case kNone:
    default:
      return "No";
  }
}

Status Storage::GetUsage(const std::string& property, uint64_t* const result) {
  std::map<int, uint64_t> inst_result;
  GetUsage(property, &inst_result);
  for (const auto& it : inst_result) {
    *result += it.second;
  }
  return Status::OK();
}

Status Storage::GetUsage(const std::string& property, std::map<int, uint64_t>* const inst_result) {
  inst_result->clear();
  for (const auto& inst : insts_) {
    uint64_t value = 0;
    inst->GetProperty(property, &value);
    (*inst_result)[inst->GetIndex()] = value;
  }
  return Status::OK();
}

uint64_t Storage::GetProperty(const std::string& property) {
  uint64_t out = 0;
  uint64_t result = 0;
  Status s;
  for (const auto& inst : insts_) {
    s = inst->GetProperty(property, &out);
    result += out;
  }
  return result;
}

Status Storage::GetKeyNum(std::vector<KeyInfo>* key_infos) {
  KeyInfo key_info;
  key_infos->resize(5);
  for (const auto& db : insts_) {
    std::vector<KeyInfo> db_key_infos;
    // check the scanner was stopped or not, before scanning the next db
    if (scan_keynum_exit_) {
      break;
    }
    auto s = db->ScanKeyNum(&db_key_infos);
    if (!s.ok()) {
      return s;
    }
    std::transform(db_key_infos.begin(), db_key_infos.end(),
        key_infos->begin(), key_infos->begin(), std::plus<>{});
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

#ifdef USE_S3
rocksdb::DBCloud* Storage::GetDBByIndex(int index) {
#else
rocksdb::DB* Storage::GetDBByIndex(int index) {
#endif
  if (index < 0 || index >= db_instance_num_) {
    LOG(WARNING) << "Invalid DB Index: " << index << "total: "
                 << db_instance_num_;
    return nullptr;
  }
  return insts_[index]->GetDB();
}

Status Storage::SetOptions(const OptionType& option_type, const std::string& db_type,
    const std::unordered_map<std::string, std::string>& options) {
  Status s;
  for (const auto& inst : insts_) {
    s = inst->SetOptions(option_type, options);
    if (!s.ok()) {
      return s;
    }
  }
  s = EnableDymayticOptions(option_type, db_type, options);
  return s;
}

void Storage::SetCompactRangeOptions(const bool is_canceled) {
  for (const auto& inst : insts_) {
    inst->SetCompactRangeOptions(is_canceled);
  }
}

Status Storage::EnableDymayticOptions(const OptionType& option_type,
    const std::string& db_type, const std::unordered_map<std::string, std::string>& options) {
  Status s;
  auto it = options.find("disable_auto_compactions");
  if (it != options.end() && it->second == "false") {
    s = EnableAutoCompaction(option_type, db_type, options);
    LOG(WARNING) << "EnableAutoCompaction " << (s.ok() ? "success" : "failed")
                 << " when Options get disable_auto_compactions: " << it->second << " ,db_type: " << db_type;
  }
  return s;
}

Status Storage::EnableAutoCompaction(const OptionType& option_type,
    const std::string& db_type, const std::unordered_map<std::string, std::string>& options) {
  Status s;

  for (const auto& inst : insts_) {
    std::vector<rocksdb::ColumnFamilyHandle*> cfhds;
    if (db_type == ALL_DB || db_type == STRINGS_DB) {
      auto string_cfhds = inst->GetStringCFHandles();
      cfhds.insert(cfhds.end(), string_cfhds.begin(), string_cfhds.end());
    }

    if (db_type == ALL_DB || db_type == HASHES_DB) {
      auto hash_cfhds = inst->GetHashCFHandles();
      cfhds.insert(cfhds.end(), hash_cfhds.begin(), hash_cfhds.end());
    }

    if (db_type == ALL_DB || db_type == LISTS_DB) {
      auto list_cfhds = inst->GetListCFHandles();
      cfhds.insert(cfhds.end(), list_cfhds.begin(), list_cfhds.end());
    }

    if (db_type == ALL_DB || db_type == SETS_DB) {
      auto set_cfhds = inst->GetSetCFHandles();
      cfhds.insert(cfhds.end(), set_cfhds.begin(), set_cfhds.end());
    }

    if (db_type == ALL_DB || db_type == ZSETS_DB) {
      auto zset_cfhds = inst->GetZsetCFHandles();
      cfhds.insert(cfhds.end(), zset_cfhds.begin(), zset_cfhds.end());
    }
    s = inst->GetDB()->EnableAutoCompaction(cfhds);
    if (!s.ok()) {
      return s;
    }
  }

  return s;
}

void Storage::GetRocksDBInfo(std::string& info) {
  char temp[12] = {0};
  for (const auto& inst : insts_) {
    snprintf(temp, sizeof(temp), "instance:%2d", inst->GetIndex());
    inst->GetRocksDBInfo(info, temp);
  }
}

int64_t Storage::IsExist(const Slice& key, std::map<DataType, Status>* type_status) {
  std::string value;
  int32_t ret = 0;
  int64_t type_count = 0;
  auto& inst = GetDBInstance(key);
  Status s = inst->Get(key, &value);
  (*type_status)[DataType::kStrings] = s;
  if (s.ok()) {
    type_count++;
  }
  s = inst->HLen(key, &ret);
  (*type_status)[DataType::kHashes] = s;
  if (s.ok()) {
    type_count++;
  }
  s = inst->SCard(key, &ret);
  (*type_status)[DataType::kSets] = s;
  if (s.ok()) {
    type_count++;
  }
  uint64_t llen = 0;
  s = inst->LLen(key, &llen);
  (*type_status)[DataType::kLists] = s;
  if (s.ok()) {
    type_count++;
  }

  s = inst->ZCard(key, &ret);
  (*type_status)[DataType::kZSets] = s;
  if (s.ok()) {
    type_count++;
  }
  return type_count;
}


void Storage::DisableWal(const bool is_wal_disable) {
  for (const auto& inst : insts_) {
    inst->SetWriteWalOptions(is_wal_disable);
  }
}

}  //  namespace storage
