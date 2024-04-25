//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "src/redis.h"

#include <algorithm>
#include <map>
#include <memory>
#include <random>

#include <glog/logging.h>
#include <fmt/core.h>

#include "src/base_filter.h"
#include "src/scope_snapshot.h"
#include "src/scope_record_lock.h"
#include "src/base_data_value_format.h"
#include "pstd/include/env.h"
#include "pstd/include/pika_codis_slot.h"
#include "storage/util.h"

namespace storage {
rocksdb::Status Redis::ScanSetsKeyNum(KeyInfo* key_info) {
  uint64_t keys = 0;
  uint64_t expires = 0;
  uint64_t ttl_sum = 0;
  uint64_t invaild_keys = 0;

  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  int64_t curtime;
  rocksdb::Env::Default()->GetCurrentTime(&curtime);

  rocksdb::Iterator* iter = db_->NewIterator(iterator_options, handles_[kMetaCF]);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    if (!ExpectedMetaValue(Type::kSet, iter->value().ToString())) {
      continue;
    }
    ParsedSetsMetaValue parsed_sets_meta_value(iter->value());
    if (parsed_sets_meta_value.IsStale() || parsed_sets_meta_value.Count() == 0) {
      invaild_keys++;
    } else {
      keys++;
      if (!parsed_sets_meta_value.IsPermanentSurvival()) {
        expires++;
        ttl_sum += parsed_sets_meta_value.Etime() - curtime;
      }
    }
  }
  delete iter;

  key_info->keys = keys;
  key_info->expires = expires;
  key_info->avg_ttl = (expires != 0) ? ttl_sum / expires : 0;
  key_info->invaild_keys = invaild_keys;
  return rocksdb::Status::OK();
}

rocksdb::Status Redis::SetsPKPatternMatchDel(const std::string& pattern, int32_t* ret) {
  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  std::string key;
  std::string meta_value;
  int32_t total_delete = 0;
  rocksdb::Status s;
  rocksdb::WriteBatch batch;
  rocksdb::Iterator* iter = db_->NewIterator(iterator_options, handles_[kMetaCF]);
  iter->SeekToFirst();
  while (iter->Valid()) {
    if (!ExpectedMetaValue(Type::kSet, meta_value)) {
      continue;
    }
    ParsedBaseMetaKey parsed_meta_key(iter->key());
    meta_value = iter->value().ToString();
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (!parsed_sets_meta_value.IsStale() && (parsed_sets_meta_value.Count() != 0) &&
        (StringMatch(pattern.data(), pattern.size(), parsed_meta_key.Key().data(), parsed_meta_key.Key().size(), 0) != 0)) {
      parsed_sets_meta_value.InitialMetaValue();
      batch.Put(handles_[kMetaCF], iter->key(), meta_value);
    }
    if (static_cast<size_t>(batch.Count()) >= BATCH_DELETE_LIMIT) {
      s = db_->Write(default_write_options_, &batch);
      if (s.ok()) {
        total_delete += static_cast<int32_t>(batch.Count());
        batch.Clear();
      } else {
        *ret = total_delete;
        return s;
      }
    }
    iter->Next();
  }
  if (batch.Count() != 0U) {
    s = db_->Write(default_write_options_, &batch);
    if (s.ok()) {
      total_delete += static_cast<int32_t>(batch.Count());
      batch.Clear();
    }
  }

  *ret = total_delete;
  return s;
}

rocksdb::Status Redis::SAdd(const Slice& key, const std::vector<std::string>& members, int32_t* ret) {
  std::unordered_set<std::string> unique;
  std::vector<std::string> filtered_members;
  for (const auto& member : members) {
    if (unique.find(member) == unique.end()) {
      unique.insert(member);
      filtered_members.push_back(member);
    }
  }

  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  uint64_t version = 0;
  std::string meta_value;

  BaseMetaKey base_meta_key(key);
  rocksdb::Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    if (!ExpectedMetaValue(Type::kSet, meta_value)) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.IsStale() || parsed_sets_meta_value.Count() == 0) {
      version = parsed_sets_meta_value.InitialMetaValue();
      if (!parsed_sets_meta_value.check_set_count(static_cast<int32_t>(filtered_members.size()))) {
        return Status::InvalidArgument("set size overflow");
      }
      parsed_sets_meta_value.SetCount(static_cast<int32_t>(filtered_members.size()));
      batch.Put(handles_[kMetaCF], base_meta_key.Encode(), meta_value);
      for (const auto& member : filtered_members) {
        SetsMemberKey sets_member_key(key, version, member);
        BaseDataValue iter_value(Slice{});
        batch.Put(handles_[kSetsDataCF], sets_member_key.Encode(), iter_value.Encode());
      }
      *ret = static_cast<int32_t>(filtered_members.size());
    } else {
      int32_t cnt = 0;
      std::string member_value;
      version = parsed_sets_meta_value.Version();
      for (const auto& member : filtered_members) {
        SetsMemberKey sets_member_key(key, version, member);
        s = db_->Get(default_read_options_, handles_[kSetsDataCF], sets_member_key.Encode(), &member_value);
        if (s.ok()) {
        } else if (s.IsNotFound()) {
          cnt++;
          BaseDataValue iter_value(Slice{});
          batch.Put(handles_[kSetsDataCF], sets_member_key.Encode(), iter_value.Encode());
        } else {
          return s;
        }
      }
      *ret = cnt;
      if (cnt == 0) {
        return rocksdb::Status::OK();
      } else {
        if (!parsed_sets_meta_value.CheckModifyCount(cnt)){
          return Status::InvalidArgument("set size overflow");
        }
        parsed_sets_meta_value.ModifyCount(cnt);
        batch.Put(handles_[kMetaCF], base_meta_key.Encode(), meta_value);
      }
    }
  } else if (s.IsNotFound()) {
    char str[4];
    EncodeFixed32(str, filtered_members.size());
    SetsMetaValue sets_meta_value(Type::kSet, Slice(str, 4));
    version = sets_meta_value.UpdateVersion();
    batch.Put(handles_[kMetaCF], base_meta_key.Encode(), sets_meta_value.Encode());
    for (const auto& member : filtered_members) {
      SetsMemberKey sets_member_key(key, version, member);
      BaseDataValue i_val(Slice{});
      batch.Put(handles_[kSetsDataCF], sets_member_key.Encode(), i_val.Encode());
    }
    *ret = static_cast<int32_t>(filtered_members.size());
  } else {
    return s;
  }
  return db_->Write(default_write_options_, &batch);
}

rocksdb::Status Redis::SCard(const Slice& key, int32_t* ret) {
  *ret = 0;
  std::string meta_value;

  BaseMetaKey base_meta_key(key);
  rocksdb::Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    if (!ExpectedMetaValue(Type::kSet, meta_value)) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.IsStale()) {
      return rocksdb::Status::NotFound("Stale");
    } else {
      *ret = parsed_sets_meta_value.Count();
      if (*ret == 0) {
        return rocksdb::Status::NotFound("Deleted");
      }
    }
  }
  return s;
}

rocksdb::Status Redis::SDiff(const std::vector<std::string>& keys, std::vector<std::string>* members) {
  if (keys.empty()) {
    return rocksdb::Status::Corruption("SDiff invalid parameter, no keys");
  }

  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  uint64_t version = 0;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  std::vector<KeyVersion> vaild_sets;
  rocksdb::Status s;

  for (uint32_t idx = 1; idx < keys.size(); ++idx) {
    BaseMetaKey base_meta_key(keys[idx]);
    s = db_->Get(read_options, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
    if (s.ok()) {
      if (!ExpectedMetaValue(Type::kSet, meta_value)) {
        return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
      }
      ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
      if (!parsed_sets_meta_value.IsStale() && parsed_sets_meta_value.Count() != 0) {
        vaild_sets.push_back({keys[idx], parsed_sets_meta_value.Version()});
      }
    } else if (!s.IsNotFound()) {
      return s;
    }
  }

  BaseMetaKey base_meta_key0(keys[0]);
  s = db_->Get(read_options, handles_[kMetaCF], base_meta_key0.Encode(), &meta_value);
  if (s.ok()) {
    if (!ExpectedMetaValue(Type::kSet, meta_value)) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (!parsed_sets_meta_value.IsStale() && parsed_sets_meta_value.Count() != 0) {
      bool found;
      Slice prefix;
      std::string member_value;
      version = parsed_sets_meta_value.Version();
      SetsMemberKey sets_member_key(keys[0], version, Slice());
      prefix = sets_member_key.EncodeSeekKey();
      KeyStatisticsDurationGuard guard(this, DataType::kSets, keys[0]);
      auto iter = db_->NewIterator(read_options, handles_[kSetsDataCF]);
      for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
        ParsedSetsMemberKey parsed_sets_member_key(iter->key());
        Slice member = parsed_sets_member_key.member();

        found = false;
        for (const auto& key_version : vaild_sets) {
          SetsMemberKey sets_member_key(key_version.key, key_version.version, member);
          s = db_->Get(read_options, handles_[kSetsDataCF], sets_member_key.Encode(), &member_value);
          if (s.ok()) {
            found = true;
            break;
          } else if (!s.IsNotFound()) {
            delete iter;
            return s;
          }
        }
        if (!found) {
          members->push_back(member.ToString());
        }
      }
      delete iter;
    }
  } else if (!s.IsNotFound()) {
    return s;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Redis::SDiffstore(const Slice& destination, const std::vector<std::string>& keys, std::vector<std::string>& value_to_dest, int32_t* ret) {
  if (keys.empty()) {
    return rocksdb::Status::Corruption("SDiffsotre invalid parameter, no keys");
  }

  rocksdb::WriteBatch batch;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  uint64_t version = 0;
  ScopeRecordLock l(lock_mgr_, destination);
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  std::vector<KeyVersion> vaild_sets;
  rocksdb::Status s;

  for (uint32_t idx = 1; idx < keys.size(); ++idx) {
    BaseMetaKey base_meta_key(keys[idx]);
    s = db_->Get(read_options, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
    if (s.ok()) {
      if (!ExpectedMetaValue(Type::kSet, meta_value)) {
        return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
      }
      ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
      if (!parsed_sets_meta_value.IsStale() && parsed_sets_meta_value.Count() != 0) {
        vaild_sets.push_back({keys[idx], parsed_sets_meta_value.Version()});
      }
    } else if (!s.IsNotFound()) {
      return s;
    }
  }

  std::vector<std::string> members;
  BaseMetaKey base_meta_key0(keys[0]);
  s = db_->Get(read_options, handles_[kMetaCF], base_meta_key0.Encode(), &meta_value);
  if (s.ok()) {
    if (!ExpectedMetaValue(Type::kSet, meta_value)) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (!parsed_sets_meta_value.IsStale() && parsed_sets_meta_value.Count() != 0) {
      bool found;
      std::string member_value;
      version = parsed_sets_meta_value.Version();
      SetsMemberKey sets_member_key(keys[0], version, Slice());
      Slice prefix = sets_member_key.EncodeSeekKey();
      KeyStatisticsDurationGuard guard(this, DataType::kSets, keys[0]);
      auto iter = db_->NewIterator(read_options, handles_[kSetsDataCF]);
      for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
        ParsedSetsMemberKey parsed_sets_member_key(iter->key());
        Slice member = parsed_sets_member_key.member();

        found = false;
        for (const auto& key_version : vaild_sets) {
          SetsMemberKey sets_member_key(key_version.key, key_version.version, member);
          s = db_->Get(read_options, handles_[kSetsDataCF], sets_member_key.Encode(), &member_value);
          if (s.ok()) {
            found = true;
            break;
          } else if (!s.IsNotFound()) {
            delete iter;
            return s;
          }
        }
        if (!found) {
          members.push_back(member.ToString());
        }
      }
      delete iter;
    }
  } else if (!s.IsNotFound()) {
    return s;
  }

  uint32_t statistic = 0;
  BaseMetaKey base_destination(destination);
  s = db_->Get(read_options, handles_[kMetaCF], base_destination.Encode(), &meta_value);
  if (s.ok()) {
    if (!ExpectedMetaValue(Type::kSet, meta_value)) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    statistic = parsed_sets_meta_value.Count();
    version = parsed_sets_meta_value.InitialMetaValue();
    if (!parsed_sets_meta_value.check_set_count(static_cast<int32_t>(members.size()))) {
      return Status::InvalidArgument("set size overflow");
    }
    parsed_sets_meta_value.SetCount(static_cast<int32_t>(members.size()));
    batch.Put(handles_[kMetaCF], base_destination.Encode(), meta_value);
  } else if (s.IsNotFound()) {
    char str[4];
    EncodeFixed32(str, members.size());
    SetsMetaValue sets_meta_value(Type::kSet, Slice(str, 4));
    version = sets_meta_value.UpdateVersion();
    batch.Put(handles_[kMetaCF], base_destination.Encode(), sets_meta_value.Encode());
  } else {
    return s;
  }
  for (const auto& member : members) {
    SetsMemberKey sets_member_key(destination, version, member);
    BaseDataValue iter_value(Slice{});
    batch.Put(handles_[kSetsDataCF], sets_member_key.Encode(), iter_value.Encode());
  }
  *ret = static_cast<int32_t>(members.size());
  s = db_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(DataType::kSets, destination.ToString(), statistic);
  value_to_dest = std::move(members);
  return s;
}

rocksdb::Status Redis::SInter(const std::vector<std::string>& keys, std::vector<std::string>* members) {
  if (keys.empty()) {
    return rocksdb::Status::Corruption("SInter invalid parameter, no keys");
  }

  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  uint64_t version = 0;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  std::vector<KeyVersion> vaild_sets;
  rocksdb::Status s;

  for (uint32_t idx = 1; idx < keys.size(); ++idx) {
    BaseMetaKey base_meta_key(keys[idx]);
    s = db_->Get(read_options, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
    if (s.ok()) {
      if (!ExpectedMetaValue(Type::kSet, meta_value)) {
        return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
      }
      ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
      if (parsed_sets_meta_value.IsStale() || parsed_sets_meta_value.Count() == 0) {
        return rocksdb::Status::OK();
      } else {
        vaild_sets.push_back({keys[idx], parsed_sets_meta_value.Version()});
      }
    } else if (s.IsNotFound()) {
      return rocksdb::Status::OK();
    } else {
      return s;
    }
  }

  BaseMetaKey base_meta_key0(keys[0]);
  s = db_->Get(read_options, handles_[kMetaCF], base_meta_key0.Encode(), &meta_value);
  if (s.ok()) {
    if (!ExpectedMetaValue(Type::kSet, meta_value)) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.IsStale() || parsed_sets_meta_value.Count() == 0) {
      return rocksdb::Status::OK();
    } else {
      bool reliable;
      std::string member_value;
      version = parsed_sets_meta_value.Version();
      SetsMemberKey sets_member_key(keys[0], version, Slice());
      KeyStatisticsDurationGuard guard(this, DataType::kSets, keys[0]);
      Slice prefix = sets_member_key.EncodeSeekKey();
      auto iter = db_->NewIterator(read_options, handles_[kSetsDataCF]);
      for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
        ParsedSetsMemberKey parsed_sets_member_key(iter->key());
        Slice member = parsed_sets_member_key.member();

        reliable = true;
        for (const auto& key_version : vaild_sets) {
          SetsMemberKey sets_member_key(key_version.key, key_version.version, member);
          s = db_->Get(read_options, handles_[kSetsDataCF], sets_member_key.Encode(), &member_value);
          if (s.ok()) {
            continue;
          } else if (s.IsNotFound()) {
            reliable = false;
            break;
          } else {
            delete iter;
            return s;
          }
        }
        if (reliable) {
          members->push_back(member.ToString());
        }
      }
      delete iter;
    }
  } else if (s.IsNotFound()) {
    return rocksdb::Status::OK();
  } else {
    return s;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Redis::SInterstore(const Slice& destination, const std::vector<std::string>& keys, std::vector<std::string>& value_to_dest, int32_t* ret) {
  if (keys.empty()) {
    return rocksdb::Status::Corruption("SInterstore invalid parameter, no keys");
  }

  rocksdb::WriteBatch batch;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  uint64_t version = 0;
  bool have_invalid_sets = false;
  ScopeRecordLock l(lock_mgr_, destination);
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  std::vector<KeyVersion> vaild_sets;
  rocksdb::Status s;

  for (uint32_t idx = 1; idx < keys.size(); ++idx) {
    BaseMetaKey base_meta_key(keys[idx]);
    s = db_->Get(read_options, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
    if (s.ok()) {
      if (!ExpectedMetaValue(Type::kSet, meta_value)) {
        return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
      }
      ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
      if (parsed_sets_meta_value.IsStale() || parsed_sets_meta_value.Count() == 0) {
        have_invalid_sets = true;
        break;
      } else {
        vaild_sets.push_back({keys[idx], parsed_sets_meta_value.Version()});
      }
    } else if (s.IsNotFound()) {
      have_invalid_sets = true;
      break;
    } else {
      return s;
    }
  }

  std::vector<std::string> members;
  if (!have_invalid_sets) {
    BaseMetaKey base_meta_key0(keys[0]);
    s = db_->Get(read_options, handles_[kMetaCF], base_meta_key0.Encode(), &meta_value);
    if (s.ok()) {
      if (!ExpectedMetaValue(Type::kSet, meta_value)) {
        return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
      }
      ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
      if (parsed_sets_meta_value.IsStale() || parsed_sets_meta_value.Count() == 0) {
        have_invalid_sets = true;
      } else {
        bool reliable;
        std::string member_value;
        version = parsed_sets_meta_value.Version();
        SetsMemberKey sets_member_key(keys[0], version, Slice());
        Slice prefix = sets_member_key.EncodeSeekKey();
        KeyStatisticsDurationGuard guard(this, DataType::kSets, keys[0]);
        auto iter = db_->NewIterator(read_options, handles_[kSetsDataCF]);
        for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
          ParsedSetsMemberKey parsed_sets_member_key(iter->key());
          Slice member = parsed_sets_member_key.member();

          reliable = true;
          for (const auto& key_version : vaild_sets) {
            SetsMemberKey sets_member_key(key_version.key, key_version.version, member);
            s = db_->Get(read_options, handles_[kSetsDataCF], sets_member_key.Encode(), &member_value);
            if (s.ok()) {
              continue;
            } else if (s.IsNotFound()) {
              reliable = false;
              break;
            } else {
              delete iter;
              return s;
            }
          }
          if (reliable) {
            members.push_back(member.ToString());
          }
        }
        delete iter;
      }
    } else if (s.IsNotFound()) {
    } else {
      return s;
    }
  }

  uint32_t statistic = 0;
  BaseMetaKey base_destination(destination);
  s = db_->Get(read_options, handles_[kMetaCF], base_destination.Encode(), &meta_value);
  if (s.ok()) {
    if (!ExpectedMetaValue(Type::kSet, meta_value)) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    statistic = parsed_sets_meta_value.Count();
    version = parsed_sets_meta_value.InitialMetaValue();
    if (!parsed_sets_meta_value.check_set_count(static_cast<int32_t>(members.size()))) {
      return Status::InvalidArgument("set size overflow");
    }
    parsed_sets_meta_value.SetCount(static_cast<int32_t>(members.size()));
    batch.Put(handles_[kMetaCF], base_destination.Encode(), meta_value);
  } else if (s.IsNotFound()) {
    char str[4];
    EncodeFixed32(str, members.size());
    SetsMetaValue sets_meta_value(Type::kSet, Slice(str, 4));
    version = sets_meta_value.UpdateVersion();
    batch.Put(handles_[kMetaCF], base_destination.Encode(), sets_meta_value.Encode());
  } else {
    return s;
  }
  for (const auto& member : members) {
    SetsMemberKey sets_member_key(destination, version, member);
    BaseDataValue iter_value(Slice{});
    batch.Put(handles_[kSetsDataCF], sets_member_key.Encode(), iter_value.Encode());
  }
  *ret = static_cast<int32_t>(members.size());
  s = db_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(DataType::kSets, destination.ToString(), statistic);
  value_to_dest = std::move(members);
  return s;
}

rocksdb::Status Redis::SIsmember(const Slice& key, const Slice& member, int32_t* ret) {
  *ret = 0;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  uint64_t version = 0;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  BaseMetaKey base_meta_key(key);
  rocksdb::Status s = db_->Get(read_options, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    if (!ExpectedMetaValue(Type::kSet, meta_value)) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.IsStale()) {
      return rocksdb::Status::NotFound("Stale");
    } else if (parsed_sets_meta_value.Count() == 0) {
      return rocksdb::Status::NotFound();
    } else {
      std::string member_value;
      version = parsed_sets_meta_value.Version();
      SetsMemberKey sets_member_key(key, version, member);
      s = db_->Get(read_options, handles_[kSetsDataCF], sets_member_key.Encode(), &member_value);
      *ret = s.ok() ? 1 : 0;
    }
  } else if (s.IsNotFound()) {
    *ret = 0;
  }
  return s;
}

rocksdb::Status Redis::SMembers(const Slice& key, std::vector<std::string>* members) {
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  uint64_t version = 0;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  BaseMetaKey base_meta_key(key);
  rocksdb::Status s = db_->Get(read_options, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    if (!ExpectedMetaValue(Type::kSet, meta_value)) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.IsStale()) {
      return rocksdb::Status::NotFound("Stale");
    } else if (parsed_sets_meta_value.Count() == 0) {
      return rocksdb::Status::NotFound();
    } else {
      version = parsed_sets_meta_value.Version();
      SetsMemberKey sets_member_key(key, version, Slice());
      Slice prefix = sets_member_key.EncodeSeekKey();
      KeyStatisticsDurationGuard guard(this, DataType::kSets, key.ToString());
      auto iter = db_->NewIterator(read_options, handles_[kSetsDataCF]);
      for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
        ParsedSetsMemberKey parsed_sets_member_key(iter->key());
        members->push_back(parsed_sets_member_key.member().ToString());
      }
      delete iter;
    }
  }
  return s;
}

Status Redis::SMembersWithTTL(const Slice& key,
                              std::vector<std::string>* members,
                              int64_t* ttl) {
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  uint64_t version = 0;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  BaseMetaKey base_meta_key(key);
  rocksdb::Status s = db_->Get(read_options, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    if (!ExpectedMetaValue(Type::kSet, meta_value)) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.Count() == 0) {
      return Status::NotFound();
    } else if (parsed_sets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      // ttl
      *ttl = parsed_sets_meta_value.Etime();
      if (*ttl == 0) {
        *ttl = -1;
      } else {
        int64_t curtime;
        rocksdb::Env::Default()->GetCurrentTime(&curtime);
        *ttl = *ttl - curtime >= 0 ? *ttl - curtime : -2;
      }

      version = parsed_sets_meta_value.Version();
      SetsMemberKey sets_member_key(key, version, Slice());
      Slice prefix = sets_member_key.EncodeSeekKey();
      KeyStatisticsDurationGuard guard(this, DataType::kSets, key.ToString());
      auto iter = db_->NewIterator(read_options, handles_[kSetsDataCF]);
      for (iter->Seek(prefix);
           iter->Valid() && iter->key().starts_with(prefix);
           iter->Next()) {
        ParsedSetsMemberKey parsed_sets_member_key(iter->key());
        members->push_back(parsed_sets_member_key.member().ToString());
      }
      delete iter;
    }
  }
  return s;
}

rocksdb::Status Redis::SMove(const Slice& source, const Slice& destination, const Slice& member, int32_t* ret) {
  *ret = 0;
  rocksdb::WriteBatch batch;
  rocksdb::ReadOptions read_options;

  uint64_t version = 0;
  uint32_t statistic = 0;
  std::string meta_value;
  std::vector<std::string> keys{source.ToString(), destination.ToString()};
  MultiScopeRecordLock ml(lock_mgr_, keys);

  if (source == destination) {
    *ret = 1;
    return rocksdb::Status::OK();
  }

  BaseMetaKey base_source(source);
  rocksdb::Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_source.Encode(), &meta_value);
  if (s.ok()) {
    if (!ExpectedMetaValue(Type::kSet, meta_value)) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.IsStale()) {
      return rocksdb::Status::NotFound("Stale");
    } else if (parsed_sets_meta_value.Count() == 0) {
      return rocksdb::Status::NotFound();
    } else {
      std::string member_value;
      version = parsed_sets_meta_value.Version();
      SetsMemberKey sets_member_key(source, version, member);
      s = db_->Get(default_read_options_, handles_[kSetsDataCF], sets_member_key.Encode(), &member_value);
      if (s.ok()) {
        *ret = 1;
        if (!parsed_sets_meta_value.CheckModifyCount(-1)){
          return Status::InvalidArgument("set size overflow");
        }
        parsed_sets_meta_value.ModifyCount(-1);
        batch.Put(handles_[kMetaCF], base_source.Encode(), meta_value);
        batch.Delete(handles_[kSetsDataCF], sets_member_key.Encode());
        statistic++;
      } else if (s.IsNotFound()) {
        *ret = 0;
        return rocksdb::Status::NotFound();
      } else {
        return s;
      }
    }
  } else if (s.IsNotFound()) {
    *ret = 0;
    return rocksdb::Status::NotFound();
  } else {
    return s;
  }

  BaseMetaKey base_destination(destination);
  s = db_->Get(default_read_options_, handles_[kMetaCF], base_destination.Encode(), &meta_value);
  if (s.ok()) {
    if (!ExpectedMetaValue(Type::kSet, meta_value)) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.IsStale() || parsed_sets_meta_value.Count() == 0) {
      version = parsed_sets_meta_value.InitialMetaValue();
      parsed_sets_meta_value.SetCount(1);
      batch.Put(handles_[kMetaCF], base_destination.Encode(), meta_value);
      SetsMemberKey sets_member_key(destination, version, member);
      BaseDataValue i_val(Slice{});
      batch.Put(handles_[kSetsDataCF], sets_member_key.Encode(), i_val.Encode());
    } else {
      std::string member_value;
      version = parsed_sets_meta_value.Version();
      SetsMemberKey sets_member_key(destination, version, member);
      s = db_->Get(default_read_options_, handles_[kSetsDataCF], sets_member_key.Encode(), &member_value);
      if (s.IsNotFound()) {
        if (!parsed_sets_meta_value.CheckModifyCount(1)){
          return Status::InvalidArgument("set size overflow");
        }
        parsed_sets_meta_value.ModifyCount(1);
        BaseDataValue iter_value(Slice{});
        batch.Put(handles_[kMetaCF], base_destination.Encode(), meta_value);
        batch.Put(handles_[kSetsDataCF], sets_member_key.Encode(), iter_value.Encode());
      } else if (!s.ok()) {
        return s;
      }
    }
  } else if (s.IsNotFound()) {
    char str[4];
    EncodeFixed32(str, 1);
    SetsMetaValue sets_meta_value(Type::kSet, Slice(str, 4));
    version = sets_meta_value.UpdateVersion();
    batch.Put(handles_[kMetaCF], base_destination.Encode(), sets_meta_value.Encode());
    SetsMemberKey sets_member_key(destination, version, member);
    BaseDataValue iter_value(Slice{});
    batch.Put(handles_[kSetsDataCF], sets_member_key.Encode(), iter_value.Encode());
  } else {
    return s;
  }
  s = db_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(DataType::kSets, source.ToString(), 1);
  return s;
}

rocksdb::Status Redis::SPop(const Slice& key, std::vector<std::string>* members, int64_t cnt) {
  std::default_random_engine engine;

  std::string meta_value;
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  uint64_t start_us = pstd::NowMicros();

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    if (!ExpectedMetaValue(Type::kSet, meta_value)) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_sets_meta_value.Count() == 0) {
      return Status::NotFound();
    } else {
      int32_t length = parsed_sets_meta_value.Count();
      if (length < cnt) {
        int32_t size = parsed_sets_meta_value.Count();
        int32_t cur_index = 0;
        uint64_t version = parsed_sets_meta_value.Version();
        SetsMemberKey sets_member_key(key, version, Slice());
        auto iter = db_->NewIterator(default_read_options_, handles_[kSetsDataCF]);
        for (iter->Seek(sets_member_key.EncodeSeekKey());
            iter->Valid() && cur_index < size;
            iter->Next(), cur_index++) {

            batch.Delete(handles_[kSetsDataCF], iter->key());
            ParsedSetsMemberKey parsed_sets_member_key(iter->key());
            members->push_back(parsed_sets_member_key.member().ToString());

        }

        //parsed_sets_meta_value.ModifyCount(-cnt);
        //batch.Put(handles_[kMetaCF], key, meta_value);
        batch.Delete(handles_[kMetaCF], base_meta_key.Encode());
        delete iter;

      } else {
        engine.seed(time(nullptr));
        int32_t cur_index = 0;
        int32_t size = parsed_sets_meta_value.Count();
        int32_t target_index = -1;
        uint64_t version = parsed_sets_meta_value.Version();
        std::unordered_set<int32_t> sets_index;
        int32_t modnum = size;

        for (int64_t cur_round = 0;
            cur_round < cnt;
            cur_round++) {
          do {
            target_index = static_cast<int32_t>( engine() % modnum);
          } while (sets_index.find(target_index) != sets_index.end());
          sets_index.insert(target_index);
        }

        SetsMemberKey sets_member_key(key, version, Slice());
        int64_t del_count = 0;
        KeyStatisticsDurationGuard guard(this, DataType::kSets, key.ToString());
        auto iter = db_->NewIterator(default_read_options_, handles_[kSetsDataCF]);
        for (iter->Seek(sets_member_key.EncodeSeekKey());
            iter->Valid() && cur_index < size;
            iter->Next(), cur_index++) {
          if (del_count == cnt) {
            break;
          }
          if (sets_index.find(cur_index) != sets_index.end()) {
            del_count++;
            batch.Delete(handles_[kSetsDataCF], iter->key());
            ParsedSetsMemberKey parsed_sets_member_key(iter->key());
            members->push_back(parsed_sets_member_key.member().ToString());
          }
        }

        if (!parsed_sets_meta_value.CheckModifyCount(static_cast<int32_t>(-cnt))){
          return Status::InvalidArgument("set size overflow");
        }
        parsed_sets_meta_value.ModifyCount(static_cast<int32_t>(-cnt));
        batch.Put(handles_[kMetaCF], base_meta_key.Encode(), meta_value);
        delete iter;
      }
    }
  } else {
    return s;
  }
  return db_->Write(default_write_options_, &batch);
}

rocksdb::Status Redis::ResetSpopCount(const std::string& key) { return spop_counts_store_->Remove(key); }

rocksdb::Status Redis::AddAndGetSpopCount(const std::string& key, uint64_t* count) {
  size_t old_count = 0;
  spop_counts_store_->Lookup(key, &old_count);
  spop_counts_store_->Insert(key, old_count + 1);
  *count = old_count + 1;
  return rocksdb::Status::OK();
}

rocksdb::Status Redis::SRandmember(const Slice& key, int32_t count, std::vector<std::string>* members) {
  if (count == 0) {
    return rocksdb::Status::OK();
  }

  members->clear();
  auto last_seed = pstd::NowMicros();
  std::default_random_engine engine;

  std::string meta_value;
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  std::vector<int32_t> targets;
  std::unordered_set<int32_t> unique;


  BaseMetaKey base_meta_key(key);
  rocksdb::Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    if (!ExpectedMetaValue(Type::kSet, meta_value)) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.IsStale()) {
      return rocksdb::Status::NotFound("Stale");
    } else if (parsed_sets_meta_value.Count() == 0) {
      return rocksdb::Status::NotFound();
    } else {
      int32_t size = parsed_sets_meta_value.Count();
      uint64_t version = parsed_sets_meta_value.Version();
      if (count > 0) {
        count = count <= size ? count : size;
        while (targets.size() < static_cast<size_t>(count)) {
          engine.seed(last_seed);
          last_seed = static_cast<int64_t>(engine());
          auto pos = static_cast<int32_t>(last_seed % size);
          if (unique.find(pos) == unique.end()) {
            unique.insert(pos);
            targets.push_back(pos);
          }
        }
      } else {
        count = -count;
        while (targets.size() < static_cast<size_t>(count)) {
          engine.seed(last_seed);
          last_seed = static_cast<int64_t>(engine());
          targets.push_back(static_cast<int32_t>(last_seed % size));
        }
      }
      std::sort(targets.begin(), targets.end());

      int32_t cur_index = 0;
      int32_t idx = 0;
      SetsMemberKey sets_member_key(key, version, Slice());
      KeyStatisticsDurationGuard guard(this, DataType::kSets, key.ToString());
      auto iter = db_->NewIterator(default_read_options_, handles_[kSetsDataCF]);
      for (iter->Seek(sets_member_key.EncodeSeekKey()); iter->Valid() && cur_index < size; iter->Next(), cur_index++) {
        if (static_cast<size_t>(idx) >= targets.size()) {
          break;
        }
        ParsedSetsMemberKey parsed_sets_member_key(iter->key());
        while (static_cast<size_t>(idx) < targets.size() && cur_index == targets[idx]) {
          idx++;
          members->push_back(parsed_sets_member_key.member().ToString());
        }
      }

      std::shuffle(members->begin(), members->end(), engine);
      delete iter;
    }
  }
  return s;
}

rocksdb::Status Redis::SRem(const Slice& key, const std::vector<std::string>& members, int32_t* ret) {
  *ret = 0;
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  uint64_t version = 0;
  uint32_t statistic = 0;
  std::string meta_value;

  BaseMetaKey base_meta_key(key);
  rocksdb::Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    if (!ExpectedMetaValue(Type::kSet, meta_value)) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.IsStale()) {
      return rocksdb::Status::NotFound("stale");
    } else if (parsed_sets_meta_value.Count() == 0) {
      return rocksdb::Status::NotFound();
    } else {
      int32_t cnt = 0;
      std::string member_value;
      version = parsed_sets_meta_value.Version();
      for (const auto& member : members) {
        SetsMemberKey sets_member_key(key, version, member);
        s = db_->Get(default_read_options_, handles_[kSetsDataCF], sets_member_key.Encode(), &member_value);
        if (s.ok()) {
          cnt++;
          statistic++;
          batch.Delete(handles_[kSetsDataCF], sets_member_key.Encode());
        } else if (s.IsNotFound()) {
        } else {
          return s;
        }
      }
      *ret = cnt;
      if (!parsed_sets_meta_value.CheckModifyCount(-cnt)){
        return Status::InvalidArgument("set size overflow");
      }
      parsed_sets_meta_value.ModifyCount(-cnt);
      batch.Put(handles_[kMetaCF], base_meta_key.Encode(), meta_value);
    }
  } else if (s.IsNotFound()) {
    *ret = 0;
    return rocksdb::Status::NotFound();
  } else {
    return s;
  }
  s = db_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(DataType::kSets, key.ToString(), statistic);
  return s;
}

rocksdb::Status Redis::SUnion(const std::vector<std::string>& keys, std::vector<std::string>* members) {
  if (keys.empty()) {
    return rocksdb::Status::Corruption("SUnion invalid parameter, no keys");
  }

  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  std::vector<KeyVersion> vaild_sets;
  rocksdb::Status s;

  for (const auto & key : keys) {
    BaseMetaKey base_meta_key(key);
    s = db_->Get(read_options, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
    if (s.ok()) {
      if (!ExpectedMetaValue(Type::kSet, meta_value)) {
        return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
      }
      ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
      if (!parsed_sets_meta_value.IsStale() && parsed_sets_meta_value.Count() != 0) {
        vaild_sets.push_back({key, parsed_sets_meta_value.Version()});
      }
    } else if (!s.IsNotFound()) {
      return s;
    }
  }

  Slice prefix;
  std::map<std::string, bool> result_flag;
  for (const auto& key_version : vaild_sets) {
    SetsMemberKey sets_member_key(key_version.key, key_version.version, Slice());
    prefix = sets_member_key.EncodeSeekKey();
    KeyStatisticsDurationGuard guard(this, DataType::kSets, key_version.key);
    auto iter = db_->NewIterator(read_options, handles_[kSetsDataCF]);
    for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
      ParsedSetsMemberKey parsed_sets_member_key(iter->key());
      std::string member = parsed_sets_member_key.member().ToString();
      if (result_flag.find(member) == result_flag.end()) {
        members->push_back(member);
        result_flag[member] = true;
      }
    }
    delete iter;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Redis::SUnionstore(const Slice& destination, const std::vector<std::string>& keys, std::vector<std::string>& value_to_dest, int32_t* ret) {
  if (keys.empty()) {
    return rocksdb::Status::Corruption("SUnionstore invalid parameter, no keys");
  }

  rocksdb::WriteBatch batch;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  uint64_t version = 0;
  ScopeRecordLock l(lock_mgr_, destination);
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  std::vector<KeyVersion> vaild_sets;
  rocksdb::Status s;

  for (const auto & key : keys) {
    BaseMetaKey base_meta_key(key);
    s = db_->Get(read_options, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
    if (s.ok()) {
      if (!ExpectedMetaValue(Type::kSet, meta_value)) {
        return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
      }
      ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
      if (!parsed_sets_meta_value.IsStale() && parsed_sets_meta_value.Count() != 0) {
        vaild_sets.push_back({key, parsed_sets_meta_value.Version()});
      }
    } else if (!s.IsNotFound()) {
      return s;
    }
  }

  Slice prefix;
  std::vector<std::string> members;
  std::map<std::string, bool> result_flag;
  for (const auto& key_version : vaild_sets) {
    SetsMemberKey sets_member_key(key_version.key, key_version.version, Slice());
    prefix = sets_member_key.EncodeSeekKey();
    KeyStatisticsDurationGuard guard(this, DataType::kSets, key_version.key);
    auto iter = db_->NewIterator(read_options, handles_[kSetsDataCF]);
    for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
      ParsedSetsMemberKey parsed_sets_member_key(iter->key());
      std::string member = parsed_sets_member_key.member().ToString();
      if (result_flag.find(member) == result_flag.end()) {
        members.push_back(member);
        result_flag[member] = true;
      }
    }
    delete iter;
  }

  uint32_t statistic = 0;
  BaseMetaKey base_destination(destination);
  s = db_->Get(read_options, handles_[kMetaCF], base_destination.Encode(), &meta_value);
  if (s.ok()) {
    if (!ExpectedMetaValue(Type::kSet, meta_value)) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    statistic = parsed_sets_meta_value.Count();
    version = parsed_sets_meta_value.InitialMetaValue();
    if (!parsed_sets_meta_value.check_set_count(static_cast<int32_t>(members.size()))) {
      return Status::InvalidArgument("set size overflow");
    }
    parsed_sets_meta_value.SetCount(static_cast<int32_t>(members.size()));
    batch.Put(handles_[kMetaCF], destination, meta_value);
  } else if (s.IsNotFound()) {
    char str[4];
    EncodeFixed32(str, members.size());
    SetsMetaValue sets_meta_value(Type::kSet, Slice(str, 4));
    version = sets_meta_value.UpdateVersion();
    batch.Put(handles_[kMetaCF], base_destination.Encode(), sets_meta_value.Encode());
  } else {
    return s;
  }
  for (const auto& member : members) {
    SetsMemberKey sets_member_key(destination, version, member);
    BaseDataValue i_val(Slice{});
    batch.Put(handles_[kSetsDataCF], sets_member_key.Encode(), i_val.Encode());
  }
  *ret = static_cast<int32_t>(members.size());
  s = db_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(DataType::kSets, destination.ToString(), statistic);
  value_to_dest = std::move(members);
  return s;
}

rocksdb::Status Redis::SScan(const Slice& key, int64_t cursor, const std::string& pattern, int64_t count,
                        std::vector<std::string>* members, int64_t* next_cursor) {
  *next_cursor = 0;
  members->clear();
  if (cursor < 0) {
    *next_cursor = 0;
    return rocksdb::Status::OK();
  }

  int64_t rest = count;
  int64_t step_length = count;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  BaseMetaKey base_meta_key(key);
  rocksdb::Status s = db_->Get(read_options, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    if (!ExpectedMetaValue(Type::kSet, meta_value)) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.IsStale() || parsed_sets_meta_value.Count() == 0) {
      *next_cursor = 0;
      return rocksdb::Status::NotFound();
    } else {
      std::string sub_member;
      std::string start_point;
      uint64_t version = parsed_sets_meta_value.Version();
      s = GetScanStartPoint(DataType::kSets, key, pattern, cursor, &start_point);
      if (s.IsNotFound()) {
        cursor = 0;
        if (isTailWildcard(pattern)) {
          start_point = pattern.substr(0, pattern.size() - 1);
        }
      }
      if (isTailWildcard(pattern)) {
        sub_member = pattern.substr(0, pattern.size() - 1);
      }

      SetsMemberKey sets_member_prefix(key, version, sub_member);
      SetsMemberKey sets_member_key(key, version, start_point);
      std::string prefix = sets_member_prefix.EncodeSeekKey().ToString();
      KeyStatisticsDurationGuard guard(this, DataType::kSets, key.ToString());
      rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[kSetsDataCF]);
      for (iter->Seek(sets_member_key.EncodeSeekKey()); iter->Valid() && rest > 0 && iter->key().starts_with(prefix);
           iter->Next()) {
        ParsedSetsMemberKey parsed_sets_member_key(iter->key());
        std::string member = parsed_sets_member_key.member().ToString();
        if (StringMatch(pattern.data(), pattern.size(), member.data(), member.size(), 0) != 0) {
          members->push_back(member);
        }
        rest--;
      }

      if (iter->Valid() && (iter->key().compare(prefix) <= 0 || iter->key().starts_with(prefix))) {
        *next_cursor = cursor + step_length;
        ParsedSetsMemberKey parsed_sets_member_key(iter->key());
        std::string next_member = parsed_sets_member_key.member().ToString();
        StoreScanNextPoint(DataType::kSets, key, pattern, *next_cursor, next_member);
      } else {
        *next_cursor = 0;
      }
      delete iter;
    }
  } else {
    *next_cursor = 0;
    return s;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Redis::SetsExpire(const Slice& key, int64_t ttl) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);

  BaseMetaKey base_meta_key(key);
  rocksdb::Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    if (!ExpectedMetaValue(Type::kSet, meta_value)) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.IsStale()) {
      return rocksdb::Status::NotFound("Stale");
    } else if (parsed_sets_meta_value.Count() == 0) {
      return rocksdb::Status::NotFound();
    }

    if (ttl > 0) {
      parsed_sets_meta_value.SetRelativeTimestamp(ttl);
      s = db_->Put(default_write_options_, handles_[kMetaCF], base_meta_key.Encode(), meta_value);
    } else {
      parsed_sets_meta_value.InitialMetaValue();
      s = db_->Put(default_write_options_, handles_[kMetaCF], base_meta_key.Encode(), meta_value);
    }
  }
  return s;
}

rocksdb::Status Redis::SetsDel(const Slice& key) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);

  BaseMetaKey base_meta_key(key);
  rocksdb::Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    if (!ExpectedMetaValue(Type::kSet, meta_value)) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.IsStale()) {
      return rocksdb::Status::NotFound("Stale");
    } else if (parsed_sets_meta_value.Count() == 0) {
      return rocksdb::Status::NotFound();
    } else {
      uint32_t statistic = parsed_sets_meta_value.Count();
      parsed_sets_meta_value.InitialMetaValue();
      s = db_->Put(default_write_options_, handles_[kMetaCF], base_meta_key.Encode(), meta_value);
      UpdateSpecificKeyStatistics(DataType::kSets, key.ToString(), statistic);
    }
  }
  return s;
}

rocksdb::Status Redis::SetsExpireat(const Slice& key, int64_t timestamp) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);

  BaseMetaKey base_meta_key(key);
  rocksdb::Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    if (!ExpectedMetaValue(Type::kSet, meta_value)) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.IsStale()) {
      return rocksdb::Status::NotFound("Stale");
    } else if (parsed_sets_meta_value.Count() == 0) {
      return rocksdb::Status::NotFound();
    } else {
      if (timestamp > 0) {
        parsed_sets_meta_value.SetEtime(static_cast<uint64_t>(timestamp));
      } else {
        parsed_sets_meta_value.InitialMetaValue();
      }
      return db_->Put(default_write_options_, handles_[kMetaCF], base_meta_key.Encode(), meta_value);
    }
  }
  return s;
}

rocksdb::Status Redis::SetsPersist(const Slice& key) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);

  BaseMetaKey base_meta_key(key);
  rocksdb::Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    if (!ExpectedMetaValue(Type::kSet, meta_value)) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.IsStale()) {
      return rocksdb::Status::NotFound("Stale");
    } else if (parsed_sets_meta_value.Count() == 0) {
      return rocksdb::Status::NotFound();
    } else {
      uint64_t timestamp = parsed_sets_meta_value.Etime();
      if (timestamp == 0) {
        return rocksdb::Status::NotFound("Not have an associated timeout");
      } else {
        parsed_sets_meta_value.SetEtime(0);
        return db_->Put(default_write_options_, handles_[kMetaCF], base_meta_key.Encode(), meta_value);
      }
    }
  }
  return s;
}

rocksdb::Status Redis::SetsTTL(const Slice& key, int64_t* timestamp) {
  std::string meta_value;

  BaseMetaKey base_meta_key(key);
  rocksdb::Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    if (!ExpectedMetaValue(Type::kSet, meta_value)) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedSetsMetaValue parsed_setes_meta_value(&meta_value);
    if (parsed_setes_meta_value.IsStale()) {
      *timestamp = -2;
      return rocksdb::Status::NotFound("Stale");
    } else if (parsed_setes_meta_value.Count() == 0) {
      *timestamp = -2;
      return rocksdb::Status::NotFound();
    } else {
      *timestamp = parsed_setes_meta_value.Etime();
      if (*timestamp == 0) {
        *timestamp = -1;
      } else {
        int64_t curtime;
        rocksdb::Env::Default()->GetCurrentTime(&curtime);
        *timestamp = *timestamp - curtime >= 0 ? *timestamp - curtime : -2;
      }
    }
  } else if (s.IsNotFound()) {
    *timestamp = -2;
  }
  return s;
}

void Redis::ScanSets() {
  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;
  auto current_time = static_cast<int32_t>(time(nullptr));

  LOG(INFO) << "***************Sets Meta Data***************";
  auto meta_iter = db_->NewIterator(iterator_options, handles_[kMetaCF]);
  for (meta_iter->SeekToFirst(); meta_iter->Valid(); meta_iter->Next()) {
    if (!ExpectedMetaValue(Type::kSet, meta_iter->value().ToString())) {
      continue;
    }
    ParsedSetsMetaValue parsed_sets_meta_value(meta_iter->value());
    ParsedBaseMetaKey parsed_meta_key(meta_iter->key());
    int32_t survival_time = 0;
    if (parsed_sets_meta_value.Etime() != 0) {
      survival_time = parsed_sets_meta_value.Etime() - current_time > 0
                          ? parsed_sets_meta_value.Etime() - current_time
                          : -1;
    }

    LOG(INFO) << fmt::format("[key : {:<30}] [count : {:<10}] [timestamp : {:<10}] [version : {}] [survival_time : {}]",
                             parsed_meta_key.Key().ToString(), parsed_sets_meta_value.Count(), parsed_sets_meta_value.Etime(),
                             parsed_sets_meta_value.Version(), survival_time);
  }
  delete meta_iter;

  LOG(INFO) << "***************Sets Member Data***************";
  auto member_iter = db_->NewIterator(iterator_options, handles_[kSetsDataCF]);
  for (member_iter->SeekToFirst(); member_iter->Valid(); member_iter->Next()) {
    ParsedSetsMemberKey parsed_sets_member_key(member_iter->key());

    LOG(INFO) << fmt::format("[key : {:<30}] [member : {:<20}] [version : {}]", parsed_sets_member_key.Key().ToString(),
                             parsed_sets_member_key.member().ToString(), parsed_sets_member_key.Version());
  }
  delete member_iter;
}

}  //  namespace storage
