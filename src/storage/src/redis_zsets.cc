//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "src/redis_zsets.h"

#include <algorithm>
#include <limits>
#include <map>
#include <memory>

#include <glog/logging.h>
#include <fmt/core.h>

#include "iostream"
#include "src/scope_record_lock.h"
#include "src/scope_snapshot.h"
#include "src/zsets_filter.h"
#include "storage/util.h"

namespace storage {

rocksdb::Comparator* ZSetsScoreKeyComparator() {
  static ZSetsScoreKeyComparatorImpl zsets_score_key_compare;
  return &zsets_score_key_compare;
}

RedisZSets::RedisZSets(Storage* const s, const DataType& type) : Redis(s, type) {}

Status RedisZSets::Open(const StorageOptions& storage_options, const std::string& db_path) {
  statistics_store_->SetCapacity(storage_options.statistics_max_size);
  small_compaction_threshold_ = storage_options.small_compaction_threshold;

  rocksdb::Options ops(storage_options.options);
  Status s = rocksdb::DB::Open(ops, db_path, &db_);
  if (s.ok()) {
    rocksdb::ColumnFamilyHandle *dcf = nullptr;
    rocksdb::ColumnFamilyHandle *scf = nullptr;
    s = db_->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "data_cf", &dcf);
    if (!s.ok()) {
      return s;
    }
    rocksdb::ColumnFamilyOptions score_cf_ops;
    score_cf_ops.comparator = ZSetsScoreKeyComparator();
    s = db_->CreateColumnFamily(score_cf_ops, "score_cf", &scf);
    if (!s.ok()) {
      return s;
    }
    delete scf;
    delete dcf;
    delete db_;
  }

  rocksdb::DBOptions db_ops(storage_options.options);
  rocksdb::ColumnFamilyOptions meta_cf_ops(storage_options.options);
  rocksdb::ColumnFamilyOptions data_cf_ops(storage_options.options);
  rocksdb::ColumnFamilyOptions score_cf_ops(storage_options.options);
  meta_cf_ops.compaction_filter_factory = std::make_shared<ZSetsMetaFilterFactory>();
  data_cf_ops.compaction_filter_factory = std::make_shared<ZSetsDataFilterFactory>(&db_, &handles_);
  score_cf_ops.compaction_filter_factory = std::make_shared<ZSetsScoreFilterFactory>(&db_, &handles_);
  score_cf_ops.comparator = ZSetsScoreKeyComparator();

  // use the bloom filter policy to reduce disk reads
  rocksdb::BlockBasedTableOptions table_ops(storage_options.table_options);
  table_ops.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, true));
  rocksdb::BlockBasedTableOptions meta_cf_table_ops(table_ops);
  rocksdb::BlockBasedTableOptions data_cf_table_ops(table_ops);
  rocksdb::BlockBasedTableOptions score_cf_table_ops(table_ops);
  if (!storage_options.share_block_cache && storage_options.block_cache_size > 0) {
    meta_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
    data_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
    score_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
  }
  meta_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(meta_cf_table_ops));
  data_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(data_cf_table_ops));
  score_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(score_cf_table_ops));

  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  column_families.emplace_back(rocksdb::kDefaultColumnFamilyName, meta_cf_ops);
  column_families.emplace_back("data_cf", data_cf_ops);
  column_families.emplace_back("score_cf", score_cf_ops);
  return rocksdb::DB::Open(db_ops, db_path, column_families, &handles_, &db_);
}

Status RedisZSets::CompactRange(const rocksdb::Slice* begin, const rocksdb::Slice* end, const ColumnFamilyType& type) {
  if (type == kMeta || type == kMetaAndData) {
    db_->CompactRange(default_compact_range_options_, handles_[0], begin, end);
  }
  if (type == kData || type == kMetaAndData) {
    db_->CompactRange(default_compact_range_options_, handles_[1], begin, end);
    db_->CompactRange(default_compact_range_options_, handles_[2], begin, end);
  }
  return Status::OK();
}

Status RedisZSets::GetProperty(const std::string& property, uint64_t* out) {
  std::string value;
  db_->GetProperty(handles_[0], property, &value);
  *out = std::strtoull(value.c_str(), nullptr, 10);
  db_->GetProperty(handles_[1], property, &value);
  *out += std::strtoull(value.c_str(), nullptr, 10);
  db_->GetProperty(handles_[2], property, &value);
  *out += std::strtoull(value.c_str(), nullptr, 10);
  return Status::OK();
}

Status RedisZSets::ScanKeyNum(KeyInfo* key_info) {
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

  rocksdb::Iterator* iter = db_->NewIterator(iterator_options, handles_[0]);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(iter->value());
    if (parsed_zsets_meta_value.IsStale() || parsed_zsets_meta_value.count() == 0) {
      invaild_keys++;
    } else {
      keys++;
      if (!parsed_zsets_meta_value.IsPermanentSurvival()) {
        expires++;
        ttl_sum += parsed_zsets_meta_value.timestamp() - curtime;
      }
    }
  }
  delete iter;

  key_info->keys = keys;
  key_info->expires = expires;
  key_info->avg_ttl = (expires != 0) ? ttl_sum / expires : 0;
  key_info->invaild_keys = invaild_keys;
  return Status::OK();
}

Status RedisZSets::ScanKeys(const std::string& pattern, std::vector<std::string>* keys) {
  std::string key;
  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  rocksdb::Iterator* iter = db_->NewIterator(iterator_options, handles_[0]);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(iter->value());
    if (!parsed_zsets_meta_value.IsStale() && parsed_zsets_meta_value.count() != 0) {
      key = iter->key().ToString();
      if (StringMatch(pattern.data(), pattern.size(), key.data(), key.size(), 0) != 0) {
        keys->push_back(key);
      }
    }
  }
  delete iter;
  return Status::OK();
}

Status RedisZSets::PKPatternMatchDel(const std::string& pattern, int32_t* ret) {
  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  std::string key;
  std::string meta_value;
  int32_t total_delete = 0;
  Status s;
  rocksdb::WriteBatch batch;
  rocksdb::Iterator* iter = db_->NewIterator(iterator_options, handles_[0]);
  iter->SeekToFirst();
  while (iter->Valid()) {
    key = iter->key().ToString();
    meta_value = iter->value().ToString();
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (!parsed_zsets_meta_value.IsStale() && (parsed_zsets_meta_value.count() != 0) &&
        (StringMatch(pattern.data(), pattern.size(), key.data(), key.size(), 0) != 0)) {
      parsed_zsets_meta_value.InitialMetaValue();
      batch.Put(handles_[0], key, meta_value);
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

Status RedisZSets::ZPopMax(const Slice& key, const int64_t count, std::vector<ScoreMember>* score_members) {
  uint32_t statistic = 0;
  score_members->clear();
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  std::string meta_value;
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      int64_t num = parsed_zsets_meta_value.count();
      num = num <= count ? num : count;
      int32_t version = parsed_zsets_meta_value.version();
      ZSetsScoreKey zsets_score_key(key, version, std::numeric_limits<double>::max(), Slice());
      rocksdb::Iterator* iter = db_->NewIterator(default_read_options_, handles_[2]);
      int32_t del_cnt = 0;
      for (iter->SeekForPrev(zsets_score_key.Encode()); iter->Valid() && del_cnt < num; iter->Prev()) {
        ParsedZSetsScoreKey parsed_zsets_score_key(iter->key());
        score_members->emplace_back(
            ScoreMember{parsed_zsets_score_key.score(), parsed_zsets_score_key.member().ToString()});
        ZSetsMemberKey zsets_member_key(key, version, parsed_zsets_score_key.member());
        ++statistic;
        ++del_cnt;
        batch.Delete(handles_[1], zsets_member_key.Encode());
        batch.Delete(handles_[2], iter->key());
      }
      delete iter;
      parsed_zsets_meta_value.ModifyCount(-del_cnt);
      batch.Put(handles_[0], key, meta_value);
      s = db_->Write(default_write_options_, &batch);
      UpdateSpecificKeyStatistics(key.ToString(), statistic);
      return s;
    }
  } else {
    return s;
  }
}

Status RedisZSets::ZPopMin(const Slice& key, const int64_t count, std::vector<ScoreMember>* score_members) {
  uint32_t statistic = 0;
  score_members->clear();
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  std::string meta_value;
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      int64_t num = parsed_zsets_meta_value.count();
      num = num <= count ? num : count;
      int32_t version = parsed_zsets_meta_value.version();
      ZSetsScoreKey zsets_score_key(key, version, std::numeric_limits<double>::lowest(), Slice());
      rocksdb::Iterator* iter = db_->NewIterator(default_read_options_, handles_[2]);
      int32_t del_cnt = 0;
      for (iter->Seek(zsets_score_key.Encode()); iter->Valid() && del_cnt < num; iter->Next()) {
        ParsedZSetsScoreKey parsed_zsets_score_key(iter->key());
        score_members->emplace_back(
            ScoreMember{parsed_zsets_score_key.score(), parsed_zsets_score_key.member().ToString()});
        ZSetsMemberKey zsets_member_key(key, version, parsed_zsets_score_key.member());
        ++statistic;
        ++del_cnt;
        batch.Delete(handles_[1], zsets_member_key.Encode());
        batch.Delete(handles_[2], iter->key());
      }
      delete iter;
      parsed_zsets_meta_value.ModifyCount(-del_cnt);
      batch.Put(handles_[0], key, meta_value);
      s = db_->Write(default_write_options_, &batch);
      UpdateSpecificKeyStatistics(key.ToString(), statistic);
      return s;
    }
  } else {
    return s;
  }
}

Status RedisZSets::ZAdd(const Slice& key, const std::vector<ScoreMember>& score_members, int32_t* ret) {
  *ret = 0;
  uint32_t statistic = 0;
  std::unordered_set<std::string> unique;
  std::vector<ScoreMember> filtered_score_members;
  for (const auto& sm : score_members) {
    if (unique.find(sm.member) == unique.end()) {
      unique.insert(sm.member);
      filtered_score_members.push_back(sm);
    }
  }

  char score_buf[8];
  int32_t version = 0;
  std::string meta_value;
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    bool vaild = true;
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale() || parsed_zsets_meta_value.count() == 0) {
      vaild = false;
      version = parsed_zsets_meta_value.InitialMetaValue();
    } else {
      vaild = true;
      version = parsed_zsets_meta_value.version();
    }

    int32_t cnt = 0;
    std::string data_value;
    for (const auto& sm : filtered_score_members) {
      bool not_found = true;
      ZSetsMemberKey zsets_member_key(key, version, sm.member);
      if (vaild) {
        s = db_->Get(default_read_options_, handles_[1], zsets_member_key.Encode(), &data_value);
        if (s.ok()) {
          not_found = false;
          uint64_t tmp = DecodeFixed64(data_value.data());
          const void* ptr_tmp = reinterpret_cast<const void*>(&tmp);
          double old_score = *reinterpret_cast<const double*>(ptr_tmp);
          if (old_score == sm.score) {
            continue;
          } else {
            ZSetsScoreKey zsets_score_key(key, version, old_score, sm.member);
            batch.Delete(handles_[2], zsets_score_key.Encode());
            // delete old zsets_score_key and overwirte zsets_member_key
            // but in different column_families so we accumulative 1
            statistic++;
          }
        } else if (!s.IsNotFound()) {
          return s;
        }
      }

      const void* ptr_score = reinterpret_cast<const void*>(&sm.score);
      EncodeFixed64(score_buf, *reinterpret_cast<const uint64_t*>(ptr_score));
      batch.Put(handles_[1], zsets_member_key.Encode(), Slice(score_buf, sizeof(uint64_t)));

      ZSetsScoreKey zsets_score_key(key, version, sm.score, sm.member);
      batch.Put(handles_[2], zsets_score_key.Encode(), Slice());
      if (not_found) {
        cnt++;
      }
    }
    parsed_zsets_meta_value.ModifyCount(cnt);
    batch.Put(handles_[0], key, meta_value);
    *ret = cnt;
  } else if (s.IsNotFound()) {
    char buf[4];
    EncodeFixed32(buf, filtered_score_members.size());
    ZSetsMetaValue zsets_meta_value(Slice(buf, sizeof(int32_t)));
    version = zsets_meta_value.UpdateVersion();
    batch.Put(handles_[0], key, zsets_meta_value.Encode());
    for (const auto& sm : filtered_score_members) {
      ZSetsMemberKey zsets_member_key(key, version, sm.member);
      const void* ptr_score = reinterpret_cast<const void*>(&sm.score);
      EncodeFixed64(score_buf, *reinterpret_cast<const uint64_t*>(ptr_score));
      batch.Put(handles_[1], zsets_member_key.Encode(), Slice(score_buf, sizeof(uint64_t)));

      ZSetsScoreKey zsets_score_key(key, version, sm.score, sm.member);
      batch.Put(handles_[2], zsets_score_key.Encode(), Slice());
    }
    *ret = static_cast<int32_t>(filtered_score_members.size());
  } else {
    return s;
  }
  s = db_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(key.ToString(), statistic);
  return s;
}

Status RedisZSets::ZCard(const Slice& key, int32_t* card) {
  *card = 0;
  std::string meta_value;

  Status s = db_->Get(default_read_options_, key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      *card = 0;
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      *card = 0;
      return Status::NotFound();
    } else {
      *card = parsed_zsets_meta_value.count();
    }
  }
  return s;
}

Status RedisZSets::ZCount(const Slice& key, double min, double max, bool left_close, bool right_close, int32_t* ret) {
  *ret = 0;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot = nullptr;

  std::string meta_value;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  Status s = db_->Get(read_options, key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      int32_t version = parsed_zsets_meta_value.version();
      int32_t cnt = 0;
      int32_t cur_index = 0;
      int32_t stop_index = parsed_zsets_meta_value.count() - 1;
      ScoreMember score_member;
      ZSetsScoreKey zsets_score_key(key, version, min, Slice());
      rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[2]);
      for (iter->Seek(zsets_score_key.Encode()); iter->Valid() && cur_index <= stop_index; iter->Next(), ++cur_index) {
        bool left_pass = false;
        bool right_pass = false;
        ParsedZSetsScoreKey parsed_zsets_score_key(iter->key());
        if (parsed_zsets_score_key.key() != key) {
          break;
        }
        if (parsed_zsets_score_key.version() != version) {
          break;
        }
        if ((left_close && min <= parsed_zsets_score_key.score()) ||
            (!left_close && min < parsed_zsets_score_key.score())) {
          left_pass = true;
        }
        if ((right_close && parsed_zsets_score_key.score() <= max) ||
            (!right_close && parsed_zsets_score_key.score() < max)) {
          right_pass = true;
        }
        if (left_pass && right_pass) {
          cnt++;
        } else if (!right_pass) {
          break;
        }
      }
      delete iter;
      *ret = cnt;
    }
  }
  return s;
}

Status RedisZSets::ZIncrby(const Slice& key, const Slice& member, double increment, double* ret) {
  *ret = 0;
  uint32_t statistic = 0;
  double score = 0;
  char score_buf[8];
  int32_t version = 0;
  std::string meta_value;
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale() || parsed_zsets_meta_value.count() == 0) {
      version = parsed_zsets_meta_value.InitialMetaValue();
    } else {
      version = parsed_zsets_meta_value.version();
    }
    std::string data_value;
    ZSetsMemberKey zsets_member_key(key, version, member);
    s = db_->Get(default_read_options_, handles_[1], zsets_member_key.Encode(), &data_value);
    if (s.ok()) {
      uint64_t tmp = DecodeFixed64(data_value.data());
      const void* ptr_tmp = reinterpret_cast<const void*>(&tmp);
      double old_score = *reinterpret_cast<const double*>(ptr_tmp);
      score = old_score + increment;
      ZSetsScoreKey zsets_score_key(key, version, old_score, member);
      batch.Delete(handles_[2], zsets_score_key.Encode());
      // delete old zsets_score_key and overwirte zsets_member_key
      // but in different column_families so we accumulative 1
      statistic++;
    } else if (s.IsNotFound()) {
      score = increment;
      parsed_zsets_meta_value.ModifyCount(1);
      batch.Put(handles_[0], key, meta_value);
    } else {
      return s;
    }
  } else if (s.IsNotFound()) {
    char buf[8];
    EncodeFixed32(buf, 1);
    ZSetsMetaValue zsets_meta_value(Slice(buf, sizeof(int32_t)));
    version = zsets_meta_value.UpdateVersion();
    batch.Put(handles_[0], key, zsets_meta_value.Encode());
    score = increment;
  } else {
    return s;
  }
  ZSetsMemberKey zsets_member_key(key, version, member);
  const void* ptr_score = reinterpret_cast<const void*>(&score);
  EncodeFixed64(score_buf, *reinterpret_cast<const uint64_t*>(ptr_score));
  batch.Put(handles_[1], zsets_member_key.Encode(), Slice(score_buf, sizeof(uint64_t)));

  ZSetsScoreKey zsets_score_key(key, version, score, member);
  batch.Put(handles_[2], zsets_score_key.Encode(), Slice());
  *ret = score;
  s = db_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(key.ToString(), statistic);
  return s;
}

Status RedisZSets::ZRange(const Slice& key, int32_t start, int32_t stop, std::vector<ScoreMember>* score_members) {
  score_members->clear();
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot = nullptr;

  std::string meta_value;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  Status s = db_->Get(read_options, key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      int32_t count = parsed_zsets_meta_value.count();
      int32_t version = parsed_zsets_meta_value.version();
      int32_t start_index = start >= 0 ? start : count + start;
      int32_t stop_index = stop >= 0 ? stop : count + stop;
      start_index = start_index <= 0 ? 0 : start_index;
      stop_index = stop_index >= count ? count - 1 : stop_index;
      if (start_index > stop_index || start_index >= count || stop_index < 0) {
        return s;
      }
      int32_t cur_index = 0;
      ScoreMember score_member;
      ZSetsScoreKey zsets_score_key(key, version, std::numeric_limits<double>::lowest(), Slice());
      rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[2]);
      for (iter->Seek(zsets_score_key.Encode()); iter->Valid() && cur_index <= stop_index; iter->Next(), ++cur_index) {
        if (cur_index >= start_index) {
          ParsedZSetsScoreKey parsed_zsets_score_key(iter->key());
          score_member.score = parsed_zsets_score_key.score();
          score_member.member = parsed_zsets_score_key.member().ToString();
          score_members->push_back(score_member);
        }
      }
      delete iter;
    }
  }
  return s;
}

Status RedisZSets::ZRangebyscore(const Slice& key, double min, double max, bool left_close, bool right_close,
                                 int64_t count, int64_t offset, std::vector<ScoreMember>* score_members) {
  score_members->clear();
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot = nullptr;

  std::string meta_value;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else if (offset >= 0 && count != 0) {
      int32_t version = parsed_zsets_meta_value.version();
      int32_t index = 0;
      int32_t stop_index = parsed_zsets_meta_value.count() - 1;
      int64_t skipped = 0;
      ScoreMember score_member;
      ZSetsScoreKey zsets_score_key(key, version, min, Slice());
      rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[2]);
      for (iter->Seek(zsets_score_key.Encode()); iter->Valid() && index <= stop_index; iter->Next(), ++index) {
        bool left_pass = false;
        bool right_pass = false;
        ParsedZSetsScoreKey parsed_zsets_score_key(iter->key());
        if (parsed_zsets_score_key.key() != key) {
          break;
        }
        if (parsed_zsets_score_key.version() != version) {
          break;
        }
        if ((left_close && min <= parsed_zsets_score_key.score()) ||
            (!left_close && min < parsed_zsets_score_key.score())) {
          left_pass = true;
        }
        if ((right_close && parsed_zsets_score_key.score() <= max) ||
            (!right_close && parsed_zsets_score_key.score() < max)) {
          right_pass = true;
        }
        if (left_pass && right_pass) {
          // skip offset
          if (skipped < offset) {
            ++skipped;
            continue;
          }
          score_member.score = parsed_zsets_score_key.score();
          score_member.member = parsed_zsets_score_key.member().ToString();
          score_members->push_back(score_member);
          if (count > 0 && score_members->size() == static_cast<size_t>(count)) {
            break;
          }
        }
        if (!right_pass) {
          break;
        }
      }
      delete iter;
    }
  }
  return s;
}

Status RedisZSets::ZRank(const Slice& key, const Slice& member, int32_t* rank) {
  *rank = -1;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot = nullptr;

  std::string meta_value;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      bool found = false;
      int32_t version = parsed_zsets_meta_value.version();
      int32_t index = 0;
      int32_t stop_index = parsed_zsets_meta_value.count() - 1;
      ScoreMember score_member;
      ZSetsScoreKey zsets_score_key(key, version, std::numeric_limits<double>::lowest(), Slice());
      rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[2]);
      for (iter->Seek(zsets_score_key.Encode()); iter->Valid() && index <= stop_index; iter->Next(), ++index) {
        ParsedZSetsScoreKey parsed_zsets_score_key(iter->key());
        if (parsed_zsets_score_key.member().compare(member) == 0) {
          found = true;
          break;
        }
      }
      delete iter;
      if (found) {
        *rank = index;
        return Status::OK();
      } else {
        return Status::NotFound();
      }
    }
  }
  return s;
}

Status RedisZSets::ZRem(const Slice& key, const std::vector<std::string>& members, int32_t* ret) {
  *ret = 0;
  uint32_t statistic = 0;
  std::unordered_set<std::string> unique;
  std::vector<std::string> filtered_members;
  for (const auto& member : members) {
    if (unique.find(member) == unique.end()) {
      unique.insert(member);
      filtered_members.push_back(member);
    }
  }

  std::string meta_value;
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      int32_t del_cnt = 0;
      std::string data_value;
      int32_t version = parsed_zsets_meta_value.version();
      for (const auto& member : filtered_members) {
        ZSetsMemberKey zsets_member_key(key, version, member);
        s = db_->Get(default_read_options_, handles_[1], zsets_member_key.Encode(), &data_value);
        if (s.ok()) {
          del_cnt++;
          statistic++;
          uint64_t tmp = DecodeFixed64(data_value.data());
          const void* ptr_tmp = reinterpret_cast<const void*>(&tmp);
          double score = *reinterpret_cast<const double*>(ptr_tmp);
          batch.Delete(handles_[1], zsets_member_key.Encode());

          ZSetsScoreKey zsets_score_key(key, version, score, member);
          batch.Delete(handles_[2], zsets_score_key.Encode());
        } else if (!s.IsNotFound()) {
          return s;
        }
      }
      *ret = del_cnt;
      parsed_zsets_meta_value.ModifyCount(-del_cnt);
      batch.Put(handles_[0], key, meta_value);
    }
  } else {
    return s;
  }
  s = db_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(key.ToString(), statistic);
  return s;
}

Status RedisZSets::ZRemrangebyrank(const Slice& key, int32_t start, int32_t stop, int32_t* ret) {
  *ret = 0;
  uint32_t statistic = 0;
  std::string meta_value;
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      std::string member;
      int32_t del_cnt = 0;
      int32_t cur_index = 0;
      int32_t count = parsed_zsets_meta_value.count();
      int32_t version = parsed_zsets_meta_value.version();
      int32_t start_index = start >= 0 ? start : count + start;
      int32_t stop_index = stop >= 0 ? stop : count + stop;
      start_index = start_index <= 0 ? 0 : start_index;
      stop_index = stop_index >= count ? count - 1 : stop_index;
      if (start_index > stop_index || start_index >= count) {
        return s;
      }
      ZSetsScoreKey zsets_score_key(key, version, std::numeric_limits<double>::lowest(), Slice());
      rocksdb::Iterator* iter = db_->NewIterator(default_read_options_, handles_[2]);
      for (iter->Seek(zsets_score_key.Encode()); iter->Valid() && cur_index <= stop_index; iter->Next(), ++cur_index) {
        if (cur_index >= start_index) {
          ParsedZSetsScoreKey parsed_zsets_score_key(iter->key());
          ZSetsMemberKey zsets_member_key(key, version, parsed_zsets_score_key.member());
          batch.Delete(handles_[1], zsets_member_key.Encode());
          batch.Delete(handles_[2], iter->key());
          del_cnt++;
          statistic++;
        }
      }
      delete iter;
      *ret = del_cnt;
      parsed_zsets_meta_value.ModifyCount(-del_cnt);
      batch.Put(handles_[0], key, meta_value);
    }
  } else {
    return s;
  }
  s = db_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(key.ToString(), statistic);
  return s;
}

Status RedisZSets::ZRemrangebyscore(const Slice& key, double min, double max, bool left_close, bool right_close,
                                    int32_t* ret) {
  *ret = 0;
  uint32_t statistic = 0;
  std::string meta_value;
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    int32_t del_cnt = 0;
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      rocksdb::Iterator* iter = db_->NewIterator(default_read_options_, handles_[2]);
      int num_elements_before_delete = 0;
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        num_elements_before_delete++;
      }
      int32_t version = parsed_zsets_meta_value.version();
      ZSetsScoreKey zsets_score_key_min(key, version, min, Slice());
      ZSetsScoreKey zsets_score_key_max(key, version, max, Slice());
      iter->Seek(zsets_score_key_min.Encode());
      ParsedZSetsScoreKey parsed_zsets_score_key_min(iter->key());
      ZSetsMemberKey zsets_member_key_min(key, version, iter->key());
      iter->Seek(zsets_score_key_max.Encode());
      ParsedZSetsScoreKey parsed_zsets_score_key_max(iter->key());
      ZSetsMemberKey zsets_member_key_max(key, version, iter->key());
      rocksdb::Slice score_begin_key(zsets_score_key_min.Encode());
      rocksdb::Slice score_end_key(zsets_score_key_max.Encode());
      rocksdb::WriteOptions write_options;
      s = db_->DeleteRange(write_options, handles_[2], score_begin_key, score_end_key);
      rocksdb::Slice begin_key(zsets_member_key_min.Encode());
      rocksdb::Slice end_key(zsets_member_key_max.Encode());
      s = db_->DeleteRange(write_options, handles_[1], begin_key, end_key);
      int num_elements_after_delete = 0;
      iter = db_->NewIterator(default_read_options_, handles_[2]);
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        num_elements_after_delete++;
      }
      delete iter;
      del_cnt = num_elements_before_delete - num_elements_after_delete;
      *ret = del_cnt;
      parsed_zsets_meta_value.ModifyCount(-del_cnt);
      batch.Put(handles_[0], key, meta_value);
    }
  } else {
    return s;
  }
  s = db_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(key.ToString(), statistic);
  return s;
}

Status RedisZSets::ZRevrange(const Slice& key, int32_t start, int32_t stop, std::vector<ScoreMember>* score_members) {
  score_members->clear();
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot = nullptr;

  std::string meta_value;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  Status s = db_->Get(read_options, key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      int32_t count = parsed_zsets_meta_value.count();
      int32_t version = parsed_zsets_meta_value.version();
      int32_t start_index = stop >= 0 ? count - stop - 1 : -stop - 1;
      int32_t stop_index = start >= 0 ? count - start - 1 : -start - 1;
      start_index = start_index <= 0 ? 0 : start_index;
      stop_index = stop_index >= count ? count - 1 : stop_index;
      if (start_index > stop_index || start_index >= count || stop_index < 0) {
        return s;
      }
      int32_t cur_index = count - 1;
      ScoreMember score_member;
      ZSetsScoreKey zsets_score_key(key, version, std::numeric_limits<double>::max(), Slice());
      rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[2]);
      for (iter->SeekForPrev(zsets_score_key.Encode()); iter->Valid() && cur_index >= start_index;
           iter->Prev(), --cur_index) {
        if (cur_index <= stop_index) {
          ParsedZSetsScoreKey parsed_zsets_score_key(iter->key());
          score_member.score = parsed_zsets_score_key.score();
          score_member.member = parsed_zsets_score_key.member().ToString();
          score_members->push_back(score_member);
        }
      }
      delete iter;
    }
  }
  return s;
}

Status RedisZSets::ZRevrangebyscore(const Slice& key, double min, double max, bool left_close, bool right_close,
                                    int64_t count, int64_t offset, std::vector<ScoreMember>* score_members) {
  score_members->clear();
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot = nullptr;

  std::string meta_value;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else if (offset >= 0 && count != 0) {
      int32_t version = parsed_zsets_meta_value.version();
      int32_t left = parsed_zsets_meta_value.count();
      int64_t skipped = 0;
      ScoreMember score_member;
      ZSetsScoreKey zsets_score_key(key, version, std::nextafter(max, std::numeric_limits<double>::max()), Slice());
      rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[2]);
      for (iter->SeekForPrev(zsets_score_key.Encode()); iter->Valid() && left > 0; iter->Prev(), --left) {
        bool left_pass = false;
        bool right_pass = false;
        ParsedZSetsScoreKey parsed_zsets_score_key(iter->key());
        if (parsed_zsets_score_key.key() != key) {
          break;
        }
        if (parsed_zsets_score_key.version() != version) {
          break;
        }
        if ((left_close && min <= parsed_zsets_score_key.score()) ||
            (!left_close && min < parsed_zsets_score_key.score())) {
          left_pass = true;
        }
        if ((right_close && parsed_zsets_score_key.score() <= max) ||
            (!right_close && parsed_zsets_score_key.score() < max)) {
          right_pass = true;
        }
        if (left_pass && right_pass) {
          // skip offset
          if (skipped < offset) {
            ++skipped;
            continue;
          }
          score_member.score = parsed_zsets_score_key.score();
          score_member.member = parsed_zsets_score_key.member().ToString();
          score_members->push_back(score_member);
          if (count > 0 and score_members->size() == static_cast<size_t>(count)) {
            break;
          }
        }
        if (!left_pass) {
          break;
        }
      }
      delete iter;
    }
  }
  return s;
}

Status RedisZSets::ZRevrank(const Slice& key, const Slice& member, int32_t* rank) {
  *rank = -1;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot = nullptr;

  std::string meta_value;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  Status s = db_->Get(read_options, key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      bool found = false;
      int32_t rev_index = 0;
      int32_t left = parsed_zsets_meta_value.count();
      int32_t version = parsed_zsets_meta_value.version();
      ZSetsScoreKey zsets_score_key(key, version, std::numeric_limits<double>::max(), Slice());
      rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[2]);
      for (iter->SeekForPrev(zsets_score_key.Encode()); iter->Valid() && left >= 0; iter->Prev(), --left, ++rev_index) {
        ParsedZSetsScoreKey parsed_zsets_score_key(iter->key());
        if (parsed_zsets_score_key.member().compare(member) == 0) {
          found = true;
          break;
        }
      }
      delete iter;
      if (found) {
        *rank = rev_index;
      } else {
        return Status::NotFound();
      }
    }
  }
  return s;
}

Status RedisZSets::ZScore(const Slice& key, const Slice& member, double* score) {
  *score = 0;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot = nullptr;

  std::string meta_value;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  Status s = db_->Get(read_options, key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    int32_t version = parsed_zsets_meta_value.version();
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      std::string data_value;
      ZSetsMemberKey zsets_member_key(key, version, member);
      s = db_->Get(read_options, handles_[1], zsets_member_key.Encode(), &data_value);
      if (s.ok()) {
        uint64_t tmp = DecodeFixed64(data_value.data());
        const void* ptr_tmp = reinterpret_cast<const void*>(&tmp);
        *score = *reinterpret_cast<const double*>(ptr_tmp);
      } else {
        return s;
      }
    }
  } else if (!s.IsNotFound()) {
    return s;
  }
  return s;
}

Status RedisZSets::ZUnionstore(const Slice& destination, const std::vector<std::string>& keys,
                               const std::vector<double>& weights, const AGGREGATE agg, std::map<std::string, double>& value_to_dest, int32_t* ret) {
  *ret = 0;
  uint32_t statistic = 0;
  rocksdb::WriteBatch batch;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot = nullptr;

  int32_t version;
  std::string meta_value;
  ScoreMember sm;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  ScopeRecordLock l(lock_mgr_, destination);
  std::map<std::string, double> member_score_map;

  Status s;
  for (size_t idx = 0; idx < keys.size(); ++idx) {
    s = db_->Get(read_options, handles_[0], keys[idx], &meta_value);
    if (s.ok()) {
      ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
      if (!parsed_zsets_meta_value.IsStale() && parsed_zsets_meta_value.count() != 0) {
        int32_t cur_index = 0;
        int32_t stop_index = parsed_zsets_meta_value.count() - 1;
        double score = 0;
        double weight = idx < weights.size() ? weights[idx] : 1;
        version = parsed_zsets_meta_value.version();
        ZSetsScoreKey zsets_score_key(keys[idx], version, std::numeric_limits<double>::lowest(), Slice());
        rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[2]);
        for (iter->Seek(zsets_score_key.Encode()); iter->Valid() && cur_index <= stop_index;
             iter->Next(), ++cur_index) {
          ParsedZSetsScoreKey parsed_zsets_score_key(iter->key());
          sm.score = parsed_zsets_score_key.score();
          sm.member = parsed_zsets_score_key.member().ToString();
          if (member_score_map.find(sm.member) == member_score_map.end()) {
            score = weight * sm.score;
            member_score_map[sm.member] = (score == -0.0) ? 0 : score;
          } else {
            score = member_score_map[sm.member];
            switch (agg) {
              case SUM:
                score += weight * sm.score;
                break;
              case MIN:
                score = std::min(score, weight * sm.score);
                break;
              case MAX:
                score = std::max(score, weight * sm.score);
                break;
            }
            member_score_map[sm.member] = (score == -0.0) ? 0 : score;
          }
        }
        delete iter;
      }
    } else if (!s.IsNotFound()) {
      return s;
    }
  }

  s = db_->Get(read_options, handles_[0], destination, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    statistic = parsed_zsets_meta_value.count();
    version = parsed_zsets_meta_value.InitialMetaValue();
    parsed_zsets_meta_value.set_count(static_cast<int32_t>(member_score_map.size()));
    batch.Put(handles_[0], destination, meta_value);
  } else {
    char buf[4];
    EncodeFixed32(buf, member_score_map.size());
    ZSetsMetaValue zsets_meta_value(Slice(buf, sizeof(int32_t)));
    version = zsets_meta_value.UpdateVersion();
    batch.Put(handles_[0], destination, zsets_meta_value.Encode());
  }

  char score_buf[8];
  for (const auto& sm : member_score_map) {
    ZSetsMemberKey zsets_member_key(destination, version, sm.first);

    const void* ptr_score = reinterpret_cast<const void*>(&sm.second);
    EncodeFixed64(score_buf, *reinterpret_cast<const uint64_t*>(ptr_score));
    batch.Put(handles_[1], zsets_member_key.Encode(), Slice(score_buf, sizeof(uint64_t)));

    ZSetsScoreKey zsets_score_key(destination, version, sm.second, sm.first);
    batch.Put(handles_[2], zsets_score_key.Encode(), Slice());
  }
  *ret = static_cast<int32_t>(member_score_map.size());
  s = db_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(destination.ToString(), statistic);
  value_to_dest = std::move(member_score_map);
  return s;
}

Status RedisZSets::ZInterstore(const Slice& destination, const std::vector<std::string>& keys,
                               const std::vector<double>& weights, const AGGREGATE agg, std::vector<ScoreMember>& value_to_dest, int32_t* ret) {
  if (keys.empty()) {
    return Status::Corruption("ZInterstore invalid parameter, no keys");
  }

  *ret = 0;
  uint32_t statistic = 0;
  rocksdb::WriteBatch batch;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot = nullptr;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  ScopeRecordLock l(lock_mgr_, destination);

  std::string meta_value;
  int32_t version = 0;
  bool have_invalid_zsets = false;
  ScoreMember item;
  std::vector<KeyVersion> vaild_zsets;
  std::vector<ScoreMember> score_members;
  std::vector<ScoreMember> final_score_members;
  Status s;

  int32_t cur_index = 0;
  int32_t stop_index = 0;
  for (size_t idx = 0; idx < keys.size(); ++idx) {
    s = db_->Get(read_options, handles_[0], keys[idx], &meta_value);
    if (s.ok()) {
      ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
      if (parsed_zsets_meta_value.IsStale() || parsed_zsets_meta_value.count() == 0) {
        have_invalid_zsets = true;
      } else {
        vaild_zsets.push_back({keys[idx], parsed_zsets_meta_value.version()});
        if (idx == 0) {
          stop_index = parsed_zsets_meta_value.count() - 1;
        }
      }
    } else if (s.IsNotFound()) {
      have_invalid_zsets = true;
    } else {
      return s;
    }
  }

  if (!have_invalid_zsets) {
    ZSetsScoreKey zsets_score_key(vaild_zsets[0].key, vaild_zsets[0].version, std::numeric_limits<double>::lowest(),
                                  Slice());
    rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[2]);
    for (iter->Seek(zsets_score_key.Encode()); iter->Valid() && cur_index <= stop_index; iter->Next(), ++cur_index) {
      ParsedZSetsScoreKey parsed_zsets_score_key(iter->key());
      double score = parsed_zsets_score_key.score();
      std::string member = parsed_zsets_score_key.member().ToString();
      score_members.push_back({score, member});
    }
    delete iter;

    std::string data_value;
    for (const auto& sm : score_members) {
      bool reliable = true;
      item.member = sm.member;
      item.score = sm.score * (!weights.empty() ? weights[0] : 1);
      for (size_t idx = 1; idx < vaild_zsets.size(); ++idx) {
        double weight = idx < weights.size() ? weights[idx] : 1;
        ZSetsMemberKey zsets_member_key(vaild_zsets[idx].key, vaild_zsets[idx].version, item.member);
        s = db_->Get(read_options, handles_[1], zsets_member_key.Encode(), &data_value);
        if (s.ok()) {
          uint64_t tmp = DecodeFixed64(data_value.data());
          const void* ptr_tmp = reinterpret_cast<const void*>(&tmp);
          double score = *reinterpret_cast<const double*>(ptr_tmp);
          switch (agg) {
            case SUM:
              item.score += weight * score;
              break;
            case MIN:
              item.score = std::min(item.score, weight * score);
              break;
            case MAX:
              item.score = std::max(item.score, weight * score);
              break;
          }
        } else if (s.IsNotFound()) {
          reliable = false;
          break;
        } else {
          return s;
        }
      }
      if (reliable) {
        final_score_members.push_back(item);
      }
    }
  }

  s = db_->Get(read_options, handles_[0], destination, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    statistic = parsed_zsets_meta_value.count();
    version = parsed_zsets_meta_value.InitialMetaValue();
    parsed_zsets_meta_value.set_count(static_cast<int32_t>(final_score_members.size()));
    batch.Put(handles_[0], destination, meta_value);
  } else {
    char buf[4];
    EncodeFixed32(buf, final_score_members.size());
    ZSetsMetaValue zsets_meta_value(Slice(buf, sizeof(int32_t)));
    version = zsets_meta_value.UpdateVersion();
    batch.Put(handles_[0], destination, zsets_meta_value.Encode());
  }
  char score_buf[8];
  for (const auto& sm : final_score_members) {
    ZSetsMemberKey zsets_member_key(destination, version, sm.member);

    const void* ptr_score = reinterpret_cast<const void*>(&sm.score);
    EncodeFixed64(score_buf, *reinterpret_cast<const uint64_t*>(ptr_score));
    batch.Put(handles_[1], zsets_member_key.Encode(), Slice(score_buf, sizeof(uint64_t)));

    ZSetsScoreKey zsets_score_key(destination, version, sm.score, sm.member);
    batch.Put(handles_[2], zsets_score_key.Encode(), Slice());
  }
  *ret = static_cast<int32_t>(final_score_members.size());
  s = db_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(destination.ToString(), statistic);
  value_to_dest = std::move(final_score_members);
  return s;
}

Status RedisZSets::ZRangebylex(const Slice& key, const Slice& min, const Slice& max, bool left_close, bool right_close,
                               std::vector<std::string>* members) {
  members->clear();
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot = nullptr;

  std::string meta_value;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  bool left_no_limit = min.compare("-") == 0;
  bool right_not_limit = max.compare("+") == 0;

  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale() || parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      int32_t version = parsed_zsets_meta_value.version();
      int32_t cur_index = 0;
      int32_t stop_index = parsed_zsets_meta_value.count() - 1;
      ZSetsMemberKey zsets_member_key(key, version, Slice());
      rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[1]);
      for (iter->Seek(zsets_member_key.Encode()); iter->Valid() && cur_index <= stop_index; iter->Next(), ++cur_index) {
        bool left_pass = false;
        bool right_pass = false;
        ParsedZSetsMemberKey parsed_zsets_member_key(iter->key());
        Slice member = parsed_zsets_member_key.member();
        if (left_no_limit || (left_close && min.compare(member) <= 0) || (!left_close && min.compare(member) < 0)) {
          left_pass = true;
        }
        if (right_not_limit || (right_close && max.compare(member) >= 0) || (!right_close && max.compare(member) > 0)) {
          right_pass = true;
        }
        if (left_pass && right_pass) {
          members->push_back(member.ToString());
        }
        if (!right_pass) {
          break;
        }
      }
      delete iter;
    }
  }
  return s;
}

Status RedisZSets::ZLexcount(const Slice& key, const Slice& min, const Slice& max, bool left_close, bool right_close,
                             int32_t* ret) {
  std::vector<std::string> members;
  Status s = ZRangebylex(key, min, max, left_close, right_close, &members);
  *ret = static_cast<int32_t>(members.size());
  return s;
}

Status RedisZSets::ZRemrangebylex(const Slice& key, const Slice& min, const Slice& max, bool left_close,
                                  bool right_close, int32_t* ret) {
  *ret = 0;
  uint32_t statistic = 0;
  rocksdb::WriteBatch batch;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot = nullptr;

  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  ScopeRecordLock l(lock_mgr_, key);

  bool left_no_limit = min.compare("-") == 0;
  bool right_not_limit = max.compare("+") == 0;

  int32_t del_cnt = 0;
  std::string meta_value;
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale() || parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      int32_t version = parsed_zsets_meta_value.version();
      int32_t cur_index = 0;
      int32_t stop_index = parsed_zsets_meta_value.count() - 1;
      ZSetsMemberKey zsets_member_key(key, version, Slice());
      rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[1]);
      for (iter->Seek(zsets_member_key.Encode()); iter->Valid() && cur_index <= stop_index; iter->Next(), ++cur_index) {
        bool left_pass = false;
        bool right_pass = false;
        ParsedZSetsMemberKey parsed_zsets_member_key(iter->key());
        Slice member = parsed_zsets_member_key.member();
        if (left_no_limit || (left_close && min.compare(member) <= 0) || (!left_close && min.compare(member) < 0)) {
          left_pass = true;
        }
        if (right_not_limit || (right_close && max.compare(member) >= 0) || (!right_close && max.compare(member) > 0)) {
          right_pass = true;
        }
        if (left_pass && right_pass) {
          batch.Delete(handles_[1], iter->key());

          uint64_t tmp = DecodeFixed64(iter->value().data());
          const void* ptr_tmp = reinterpret_cast<const void*>(&tmp);
          double score = *reinterpret_cast<const double*>(ptr_tmp);
          ZSetsScoreKey zsets_score_key(key, version, score, member);
          batch.Delete(handles_[2], zsets_score_key.Encode());
          del_cnt++;
          statistic++;
        }
        if (!right_pass) {
          break;
        }
      }
      delete iter;
    }
    if (del_cnt > 0) {
      parsed_zsets_meta_value.ModifyCount(-del_cnt);
      batch.Put(handles_[0], key, meta_value);
      *ret = del_cnt;
    }
  } else {
    return s;
  }
  s = db_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(key.ToString(), statistic);
  return s;
}

Status RedisZSets::Expire(const Slice& key, int32_t ttl) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    }

    if (ttl > 0) {
      parsed_zsets_meta_value.SetRelativeTimestamp(ttl);
    } else {
      parsed_zsets_meta_value.InitialMetaValue();
    }
    s = db_->Put(default_write_options_, handles_[0], key, meta_value);
  }
  return s;
}

Status RedisZSets::Del(const Slice& key) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      uint32_t statistic = parsed_zsets_meta_value.count();
      parsed_zsets_meta_value.InitialMetaValue();
      s = db_->Put(default_write_options_, handles_[0], key, meta_value);
      UpdateSpecificKeyStatistics(key.ToString(), statistic);
    }
  }
  return s;
}

bool RedisZSets::Scan(const std::string& start_key, const std::string& pattern, std::vector<std::string>* keys,
                      int64_t* count, std::string* next_key) {
  std::string meta_key;
  bool is_finish = true;
  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  rocksdb::Iterator* it = db_->NewIterator(iterator_options, handles_[0]);

  it->Seek(start_key);
  while (it->Valid() && (*count) > 0) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(it->value());
    if (parsed_zsets_meta_value.IsStale() || parsed_zsets_meta_value.count() == 0) {
      it->Next();
      continue;
    } else {
      meta_key = it->key().ToString();
      if (StringMatch(pattern.data(), pattern.size(), meta_key.data(), meta_key.size(), 0) != 0) {
        keys->push_back(meta_key);
      }
      (*count)--;
      it->Next();
    }
  }

  std::string prefix = isTailWildcard(pattern) ? pattern.substr(0, pattern.size() - 1) : "";
  if (it->Valid() && (it->key().compare(prefix) <= 0 || it->key().starts_with(prefix))) {
    *next_key = it->key().ToString();
    is_finish = false;
  } else {
    *next_key = "";
  }
  delete it;
  return is_finish;
}

bool RedisZSets::PKExpireScan(const std::string& start_key, int32_t min_timestamp, int32_t max_timestamp,
                              std::vector<std::string>* keys, int64_t* leftover_visits, std::string* next_key) {
  bool is_finish = true;
  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  rocksdb::Iterator* it = db_->NewIterator(iterator_options, handles_[0]);
  it->Seek(start_key);
  while (it->Valid() && (*leftover_visits) > 0) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(it->value());
    if (parsed_zsets_meta_value.IsStale() || parsed_zsets_meta_value.count() == 0) {
      it->Next();
      continue;
    } else {
      if (min_timestamp < parsed_zsets_meta_value.timestamp() && parsed_zsets_meta_value.timestamp() < max_timestamp) {
        keys->push_back(it->key().ToString());
      }
      (*leftover_visits)--;
      it->Next();
    }
  }

  if (it->Valid()) {
    is_finish = false;
    *next_key = it->key().ToString();
  } else {
    *next_key = "";
  }
  delete it;
  return is_finish;
}

Status RedisZSets::Expireat(const Slice& key, int32_t timestamp) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      if (timestamp > 0) {
        parsed_zsets_meta_value.set_timestamp(timestamp);
      } else {
        parsed_zsets_meta_value.InitialMetaValue();
      }
      return db_->Put(default_write_options_, handles_[0], key, meta_value);
    }
  }
  return s;
}

Status RedisZSets::ZScan(const Slice& key, int64_t cursor, const std::string& pattern, int64_t count,
                         std::vector<ScoreMember>* score_members, int64_t* next_cursor) {
  *next_cursor = 0;
  score_members->clear();
  if (cursor < 0) {
    *next_cursor = 0;
    return Status::OK();
  }

  int64_t rest = count;
  int64_t step_length = count;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale() || parsed_zsets_meta_value.count() == 0) {
      *next_cursor = 0;
      return Status::NotFound();
    } else {
      std::string sub_member;
      std::string start_point;
      int32_t version = parsed_zsets_meta_value.version();
      s = GetScanStartPoint(key, pattern, cursor, &start_point);
      if (s.IsNotFound()) {
        cursor = 0;
        if (isTailWildcard(pattern)) {
          start_point = pattern.substr(0, pattern.size() - 1);
        }
      }
      if (isTailWildcard(pattern)) {
        sub_member = pattern.substr(0, pattern.size() - 1);
      }

      ZSetsMemberKey zsets_member_prefix(key, version, sub_member);
      ZSetsMemberKey zsets_member_key(key, version, start_point);
      std::string prefix = zsets_member_prefix.Encode().ToString();
      rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[1]);
      for (iter->Seek(zsets_member_key.Encode()); iter->Valid() && rest > 0 && iter->key().starts_with(prefix);
           iter->Next()) {
        ParsedZSetsMemberKey parsed_zsets_member_key(iter->key());
        std::string member = parsed_zsets_member_key.member().ToString();
        if (StringMatch(pattern.data(), pattern.size(), member.data(), member.size(), 0) != 0) {
          uint64_t tmp = DecodeFixed64(iter->value().data());
          const void* ptr_tmp = reinterpret_cast<const void*>(&tmp);
          double score = *reinterpret_cast<const double*>(ptr_tmp);
          score_members->push_back({score, member});
        }
        rest--;
      }

      if (iter->Valid() && (iter->key().compare(prefix) <= 0 || iter->key().starts_with(prefix))) {
        *next_cursor = cursor + step_length;
        ParsedZSetsMemberKey parsed_zsets_member_key(iter->key());
        std::string next_member = parsed_zsets_member_key.member().ToString();
        StoreScanNextPoint(key, pattern, *next_cursor, next_member);
      } else {
        *next_cursor = 0;
      }
      delete iter;
    }
  } else {
    *next_cursor = 0;
    return s;
  }
  return Status::OK();
}

Status RedisZSets::PKScanRange(const Slice& key_start, const Slice& key_end, const Slice& pattern, int32_t limit,
                               std::vector<std::string>* keys, std::string* next_key) {
  next_key->clear();

  std::string key;
  int32_t remain = limit;
  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  bool start_no_limit = key_start.compare("") == 0;
  bool end_no_limit = key_end.compare("") == 0;

  if (!start_no_limit && !end_no_limit && (key_start.compare(key_end) > 0)) {
    return Status::InvalidArgument("error in given range");
  }

  rocksdb::Iterator* it = db_->NewIterator(iterator_options, handles_[0]);
  if (start_no_limit) {
    it->SeekToFirst();
  } else {
    it->Seek(key_start);
  }

  while (it->Valid() && remain > 0 && (end_no_limit || it->key().compare(key_end) <= 0)) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(it->value());
    if (parsed_zsets_meta_value.IsStale() || parsed_zsets_meta_value.count() == 0) {
      it->Next();
    } else {
      key = it->key().ToString();
      if (StringMatch(pattern.data(), pattern.size(), key.data(), key.size(), 0) != 0) {
        keys->push_back(key);
      }
      remain--;
      it->Next();
    }
  }

  while (it->Valid() && (end_no_limit || it->key().compare(key_end) <= 0)) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(it->value());
    if (parsed_zsets_meta_value.IsStale() || parsed_zsets_meta_value.count() == 0) {
      it->Next();
    } else {
      *next_key = it->key().ToString();
      break;
    }
  }
  delete it;
  return Status::OK();
}

Status RedisZSets::PKRScanRange(const Slice& key_start, const Slice& key_end, const Slice& pattern, int32_t limit,
                                std::vector<std::string>* keys, std::string* next_key) {
  next_key->clear();

  std::string key;
  int32_t remain = limit;
  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  bool start_no_limit = key_start.compare("") == 0;
  bool end_no_limit = key_end.compare("") == 0;

  if (!start_no_limit && !end_no_limit && (key_start.compare(key_end) < 0)) {
    return Status::InvalidArgument("error in given range");
  }

  rocksdb::Iterator* it = db_->NewIterator(iterator_options, handles_[0]);
  if (start_no_limit) {
    it->SeekToLast();
  } else {
    it->SeekForPrev(key_start);
  }

  while (it->Valid() && remain > 0 && (end_no_limit || it->key().compare(key_end) >= 0)) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(it->value());
    if (parsed_zsets_meta_value.IsStale() || parsed_zsets_meta_value.count() == 0) {
      it->Prev();
    } else {
      key = it->key().ToString();
      if (StringMatch(pattern.data(), pattern.size(), key.data(), key.size(), 0) != 0) {
        keys->push_back(key);
      }
      remain--;
      it->Prev();
    }
  }

  while (it->Valid() && (end_no_limit || it->key().compare(key_end) >= 0)) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(it->value());
    if (parsed_zsets_meta_value.IsStale() || parsed_zsets_meta_value.count() == 0) {
      it->Prev();
    } else {
      *next_key = it->key().ToString();
      break;
    }
  }
  delete it;
  return Status::OK();
}

Status RedisZSets::Persist(const Slice& key) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      int32_t timestamp = parsed_zsets_meta_value.timestamp();
      if (timestamp == 0) {
        return Status::NotFound("Not have an associated timeout");
      } else {
        parsed_zsets_meta_value.set_timestamp(0);
        return db_->Put(default_write_options_, handles_[0], key, meta_value);
      }
    }
  }
  return s;
}

Status RedisZSets::TTL(const Slice& key, int64_t* timestamp) {
  std::string meta_value;
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      *timestamp = -2;
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      *timestamp = -2;
      return Status::NotFound();
    } else {
      *timestamp = parsed_zsets_meta_value.timestamp();
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

void RedisZSets::ScanDatabase() {
  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;
  auto current_time = static_cast<int32_t>(time(nullptr));

  LOG(INFO) << "***************ZSets Meta Data***************";
  auto meta_iter = db_->NewIterator(iterator_options, handles_[0]);
  for (meta_iter->SeekToFirst(); meta_iter->Valid(); meta_iter->Next()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(meta_iter->value());
    int32_t survival_time = 0;
    if (parsed_zsets_meta_value.timestamp() != 0) {
      survival_time = parsed_zsets_meta_value.timestamp() - current_time > 0
                          ? parsed_zsets_meta_value.timestamp() - current_time
                          : -1;
    }

    LOG(INFO) << fmt::format("[key : {:<30}] [count : {:<10}] [timestamp : {:<10}] [version : {}] [survival_time : {}]",
                             meta_iter->key().ToString(), parsed_zsets_meta_value.count(), parsed_zsets_meta_value.timestamp(),
                             parsed_zsets_meta_value.version(), survival_time);
  }
  delete meta_iter;

  LOG(INFO) << "***************ZSets Member To Score Data***************";
  auto member_iter = db_->NewIterator(iterator_options, handles_[1]);
  for (member_iter->SeekToFirst(); member_iter->Valid(); member_iter->Next()) {
    ParsedZSetsMemberKey parsed_zsets_member_key(member_iter->key());

    uint64_t tmp = DecodeFixed64(member_iter->value().data());
    const void* ptr_tmp = reinterpret_cast<const void*>(&tmp);
    double score = *reinterpret_cast<const double*>(ptr_tmp);

    LOG(INFO) << fmt::format("[key : {:<30}] [member : {:<20}] [score : {:<20}] [version : {}]",
                             parsed_zsets_member_key.key().ToString(), parsed_zsets_member_key.member().ToString(),
                             score, parsed_zsets_member_key.version());
  }
  delete member_iter;

  LOG(INFO) << "***************ZSets Score To Member Data***************";
  auto score_iter = db_->NewIterator(iterator_options, handles_[2]);
  for (score_iter->SeekToFirst(); score_iter->Valid(); score_iter->Next()) {
    ParsedZSetsScoreKey parsed_zsets_score_key(score_iter->key());
    
    LOG(INFO) << fmt::format("[key : {:<30}] [score : {:<20}] [member : {:<20}] [version : {}]",
                             parsed_zsets_score_key.key().ToString(), parsed_zsets_score_key.score(),
                              parsed_zsets_score_key.member().ToString(), parsed_zsets_score_key.version());
  }
  delete score_iter;
}

}  // namespace storage
