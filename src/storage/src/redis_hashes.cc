//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "src/redis.h"

#include <memory>

#include <fmt/core.h>
#include <glog/logging.h>

#include "pstd/include/pika_codis_slot.h"
#include "src/base_filter.h"
#include "src/scope_record_lock.h"
#include "src/scope_snapshot.h"
#include "src/base_data_key_format.h"
#include "src/base_data_value_format.h"
#include "storage/util.h"

namespace storage {
Status Redis::ScanHashesKeyNum(KeyInfo* key_info) {
  uint64_t keys = 0;
  uint64_t expires = 0;
  uint64_t ttl_sum = 0;
  uint64_t invaild_keys = 0;

  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  int64_t curtime = pstd::NowMillis();

  rocksdb::Iterator* iter = db_->NewIterator(iterator_options, handles_[kMetaCF]);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    if (!ExpectedMetaValue(DataType::kHashes, iter->value().ToString())) {
      continue;
    }
    ParsedHashesMetaValue parsed_hashes_meta_value(iter->value());
    if (parsed_hashes_meta_value.IsStale() || parsed_hashes_meta_value.Count() == 0) {
      invaild_keys++;
    } else {
      keys++;
      if (!parsed_hashes_meta_value.IsPermanentSurvival()) {
        expires++;
        ttl_sum += parsed_hashes_meta_value.Etime() - curtime;
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

Status Redis::HDel(const Slice& key, const std::vector<std::string>& fields, int32_t* ret) {
  uint32_t statistic = 0;
  std::vector<std::string> filtered_fields;
  std::unordered_set<std::string> field_set;
  for (const auto & iter : fields) {
    const std::string& field = iter;
    if (field_set.find(field) == field_set.end()) {
      field_set.insert(field);
      filtered_fields.push_back(iter);
    }
  }

  rocksdb::WriteBatch batch;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  int32_t del_cnt = 0;
  uint64_t version = 0;
  ScopeRecordLock l(lock_mgr_, key);
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(read_options, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok() && !ExpectedMetaValue(DataType::kHashes, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kHashes)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
    }
  }
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale() || parsed_hashes_meta_value.Count() == 0) {
      *ret = 0;
      return Status::OK();
    } else {
      std::string data_value;
      version = parsed_hashes_meta_value.Version();
      for (const auto& field : filtered_fields) {
        HashesDataKey hashes_data_key(key, version, field);
        s = db_->Get(read_options, handles_[kHashesDataCF], hashes_data_key.Encode(), &data_value);
        if (s.ok()) {
          del_cnt++;
          statistic++;
          batch.Delete(handles_[kHashesDataCF], hashes_data_key.Encode());
        } else if (s.IsNotFound()) {
          continue;
        } else {
          return s;
        }
      }
      *ret = del_cnt;
      if (!parsed_hashes_meta_value.CheckModifyCount(-del_cnt)){
        return Status::InvalidArgument("hash size overflow");
      }
      parsed_hashes_meta_value.ModifyCount(-del_cnt);
      batch.Put(handles_[kMetaCF], base_meta_key.Encode(), meta_value);
    }
  } else if (s.IsNotFound()) {
    *ret = 0;
    return Status::OK();
  } else {
    return s;
  }
  s = db_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(DataType::kHashes, key.ToString(), statistic);
  return s;
}

Status Redis::HExists(const Slice& key, const Slice& field) {
  std::string value;
  return HGet(key, field, &value);
}

Status Redis::HGet(const Slice& key, const Slice& field, std::string* value) {
  std::string meta_value;
  uint64_t version = 0;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(read_options, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok() && !ExpectedMetaValue(DataType::kHashes, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kHashes)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
    }
  }
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_hashes_meta_value.Count() == 0) {
      return Status::NotFound();
    } else {
      version = parsed_hashes_meta_value.Version();
      HashesDataKey data_key(key, version, field);
      s = db_->Get(read_options, handles_[kHashesDataCF], data_key.Encode(), value);
      if (s.ok()) {
        ParsedBaseDataValue parsed_internal_value(value);
        parsed_internal_value.StripSuffix();
      }
    }
  }
  return s;
}

Status Redis::HGetall(const Slice& key, std::vector<FieldValue>* fvs) {
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  uint64_t version = 0;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(read_options, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok() && !ExpectedMetaValue(DataType::kHashes, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kHashes)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
    }
  }
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_hashes_meta_value.Count() == 0) {
      return Status::NotFound();
    } else {
      version = parsed_hashes_meta_value.Version();
      HashesDataKey hashes_data_key(key, version, "");
      Slice prefix = hashes_data_key.EncodeSeekKey();
      KeyStatisticsDurationGuard guard(this, DataType::kHashes, key.ToString());
      auto iter = db_->NewIterator(read_options, handles_[kHashesDataCF]);
      for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key());
        ParsedBaseDataValue parsed_internal_value(iter->value());
        fvs->push_back({parsed_hashes_data_key.field().ToString(), parsed_internal_value.UserValue().ToString()});
      }
      delete iter;
    }
  }
  return s;
}

Status Redis::HGetallWithTTL(const Slice& key, std::vector<FieldValue>* fvs, int64_t* ttl) {
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  uint64_t version = 0;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(read_options, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok() && !ExpectedMetaValue(DataType::kHashes, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kHashes)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
    }
  }
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.Count() == 0) {
      return Status::NotFound();
    } else if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      // ttl
      *ttl = parsed_hashes_meta_value.Etime();
      if (*ttl == 0) {
        *ttl = -1;
      } else {
        int64_t curtime = pstd::NowMillis();
        *ttl = *ttl - curtime >= 0 ? *ttl - curtime : -2;
      }

      version = parsed_hashes_meta_value.Version();
      HashesDataKey hashes_data_key(key, version, "");
      Slice prefix = hashes_data_key.EncodeSeekKey();
      KeyStatisticsDurationGuard guard(this, DataType::kHashes, key.ToString());
      auto iter = db_->NewIterator(read_options, handles_[kHashesDataCF]);
      for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key());
        ParsedBaseDataValue parsed_internal_value(iter->value());
        fvs->push_back({parsed_hashes_data_key.field().ToString(), parsed_internal_value.UserValue().ToString()});
      }
      delete iter;
    }
  }
  return s;
}

Status Redis::HIncrby(const Slice& key, const Slice& field, int64_t value, int64_t* ret) {
  *ret = 0;
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  uint64_t version = 0;
  uint32_t statistic = 0;
  std::string old_value;
  std::string meta_value;


  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  char value_buf[32] = {0};
  char meta_value_buf[4] = {0};
  if (s.ok() && !ExpectedMetaValue(DataType::kHashes, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kHashes)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
    }
  }
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale() || parsed_hashes_meta_value.Count() == 0) {
      version = parsed_hashes_meta_value.UpdateVersion();
      parsed_hashes_meta_value.SetCount(1);
      parsed_hashes_meta_value.SetEtime(0);
      batch.Put(handles_[kMetaCF], base_meta_key.Encode(), meta_value);
      HashesDataKey hashes_data_key(key, version, field);
      Int64ToStr(value_buf, 32, value);
      batch.Put(handles_[kHashesDataCF], hashes_data_key.Encode(), value_buf);
      *ret = value;
    } else {
      version = parsed_hashes_meta_value.Version();
      HashesDataKey hashes_data_key(key, version, field);
      s = db_->Get(default_read_options_, handles_[kHashesDataCF], hashes_data_key.Encode(), &old_value);
      if (s.ok()) {
        ParsedBaseDataValue parsed_internal_value(&old_value);
        parsed_internal_value.StripSuffix();
        int64_t ival = 0;
        if (StrToInt64(old_value.data(), old_value.size(), &ival) == 0) {
          return Status::Corruption("hash value is not an integer");
        }
        if ((value >= 0 && LLONG_MAX - value < ival) || (value < 0 && LLONG_MIN - value > ival)) {
          return Status::InvalidArgument("Overflow");
        }
        *ret = ival + value;
        Int64ToStr(value_buf, 32, *ret);
        BaseDataValue internal_value(value_buf);
        batch.Put(handles_[kHashesDataCF], hashes_data_key.Encode(), internal_value.Encode());
        statistic++;
      } else if (s.IsNotFound()) {
        Int64ToStr(value_buf, 32, value);
        if (!parsed_hashes_meta_value.CheckModifyCount(1)){
          return Status::InvalidArgument("hash size overflow");
        }
        BaseDataValue internal_value(value_buf);
        parsed_hashes_meta_value.ModifyCount(1);
        batch.Put(handles_[kMetaCF], base_meta_key.Encode(), meta_value);
        batch.Put(handles_[kHashesDataCF], hashes_data_key.Encode(), internal_value.Encode());
        *ret = value;
      } else {
        return s;
      }
    }
  } else if (s.IsNotFound()) {
    EncodeFixed32(meta_value_buf, 1);
    HashesMetaValue hashes_meta_value(DataType::kHashes, Slice(meta_value_buf, 4));
    version = hashes_meta_value.UpdateVersion();
    batch.Put(handles_[kMetaCF], base_meta_key.Encode(), hashes_meta_value.Encode());
    HashesDataKey hashes_data_key(key, version, field);

    Int64ToStr(value_buf, 32, value);
    BaseDataValue internal_value(value_buf);
    batch.Put(handles_[kHashesDataCF], hashes_data_key.Encode(), internal_value.Encode());
    *ret = value;
  } else {
    return s;
  }
  s = db_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(DataType::kHashes, key.ToString(), statistic);
  return s;
}

Status Redis::HIncrbyfloat(const Slice& key, const Slice& field, const Slice& by, std::string* new_value) {
  new_value->clear();
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  uint64_t version = 0;
  uint32_t statistic = 0;
  std::string meta_value;
  std::string old_value_str;
  long double long_double_by;

  if (StrToLongDouble(by.data(), by.size(), &long_double_by) == -1) {
    return Status::Corruption("value is not a vaild float");
  }


  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  char meta_value_buf[4] = {0};
  if (s.ok() && !ExpectedMetaValue(DataType::kHashes, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kHashes)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
    }
  }
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale() || parsed_hashes_meta_value.Count() == 0) {
      version = parsed_hashes_meta_value.UpdateVersion();
      parsed_hashes_meta_value.SetCount(1);
      parsed_hashes_meta_value.SetEtime(0);
      batch.Put(handles_[kMetaCF], base_meta_key.Encode(), meta_value);
      HashesDataKey hashes_data_key(key, version, field);

      LongDoubleToStr(long_double_by, new_value);
      BaseDataValue inter_value(*new_value);
      batch.Put(handles_[kHashesDataCF], hashes_data_key.Encode(), inter_value.Encode());
    } else {
      version = parsed_hashes_meta_value.Version();
      HashesDataKey hashes_data_key(key, version, field);
      s = db_->Get(default_read_options_, handles_[kHashesDataCF], hashes_data_key.Encode(), &old_value_str);
      if (s.ok()) {
        long double total;
        long double old_value;
        ParsedBaseDataValue parsed_internal_value(&old_value_str);
        parsed_internal_value.StripSuffix();
        if (StrToLongDouble(old_value_str.data(), old_value_str.size(), &old_value) == -1) {
          return Status::Corruption("value is not a vaild float");
        }

        total = old_value + long_double_by;
        if (LongDoubleToStr(total, new_value) == -1) {
          return Status::InvalidArgument("Overflow");
        }
        BaseDataValue internal_value(*new_value);
        batch.Put(handles_[kHashesDataCF], hashes_data_key.Encode(), internal_value.Encode());
        statistic++;
      } else if (s.IsNotFound()) {
        LongDoubleToStr(long_double_by, new_value);
        if (!parsed_hashes_meta_value.CheckModifyCount(1)){
          return Status::InvalidArgument("hash size overflow");
        }
        parsed_hashes_meta_value.ModifyCount(1);
        BaseDataValue internal_value(*new_value);
        batch.Put(handles_[kMetaCF], base_meta_key.Encode(), meta_value);
        batch.Put(handles_[kHashesDataCF], hashes_data_key.Encode(), internal_value.Encode());
      } else {
        return s;
      }
    }
  } else if (s.IsNotFound()) {
    EncodeFixed32(meta_value_buf, 1);
    HashesMetaValue hashes_meta_value(DataType::kHashes, Slice(meta_value_buf, 4));
    version = hashes_meta_value.UpdateVersion();
    batch.Put(handles_[kMetaCF], base_meta_key.Encode(), hashes_meta_value.Encode());

    HashesDataKey hashes_data_key(key, version, field);
    LongDoubleToStr(long_double_by, new_value);
    BaseDataValue internal_value(*new_value);
    batch.Put(handles_[kHashesDataCF], hashes_data_key.Encode(), internal_value.Encode());
  } else {
    return s;
  }
  s = db_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(DataType::kHashes, key.ToString(), statistic);
  return s;
}

Status Redis::HKeys(const Slice& key, std::vector<std::string>* fields) {
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  uint64_t version = 0;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(read_options, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok() && !ExpectedMetaValue(DataType::kHashes, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kHashes)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
    }
  }
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_hashes_meta_value.Count() == 0) {
      return Status::NotFound();
    } else {
      version = parsed_hashes_meta_value.Version();
      HashesDataKey hashes_data_key(key, version, "");
      Slice prefix = hashes_data_key.EncodeSeekKey();
      KeyStatisticsDurationGuard guard(this, DataType::kHashes, key.ToString());
      auto iter = db_->NewIterator(read_options, handles_[kHashesDataCF]);
      for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key());
        fields->push_back(parsed_hashes_data_key.field().ToString());
      }
      delete iter;
    }
  }
  return s;
}

Status Redis::HLen(const Slice& key, int32_t* ret, std::string&& prefetch_meta) {
  *ret = 0;
  Status s;
  std::string meta_value(std::move(prefetch_meta));

  // meta_value is empty means no meta value get before,
  // we should get meta first
  if (meta_value.empty()) {
    BaseMetaKey base_meta_key(key);
    s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
    if (s.ok() && !ExpectedMetaValue(DataType::kHashes, meta_value)) {
      if (ExpectedStale(meta_value)) {
        s = Status::NotFound();
      } else {
        return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kHashes)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
      }
    }
  }
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      *ret = 0;
      return Status::NotFound("Stale");
    } else if (parsed_hashes_meta_value.Count() == 0) {
      return Status::NotFound();
    } else {
      *ret = parsed_hashes_meta_value.Count();
    }
  } else if (s.IsNotFound()) {
    *ret = 0;
  }
  return s;
}

Status Redis::HMGet(const Slice& key, const std::vector<std::string>& fields, std::vector<ValueStatus>* vss) {
  vss->clear();

  uint64_t version = 0;
  bool is_stale = false;
  std::string value;
  std::string meta_value;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(read_options, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok() && !ExpectedMetaValue(DataType::kHashes, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kHashes)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
    }
  }
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if ((is_stale = parsed_hashes_meta_value.IsStale()) || parsed_hashes_meta_value.Count() == 0) {
      for (size_t idx = 0; idx < fields.size(); ++idx) {
        vss->push_back({std::string(), Status::NotFound()});
      }
      return Status::NotFound(is_stale ? "Stale" : "");
    } else {
      version = parsed_hashes_meta_value.Version();
      for (const auto& field : fields) {
        HashesDataKey hashes_data_key(key, version, field);
        s = db_->Get(read_options, handles_[kHashesDataCF], hashes_data_key.Encode(), &value);
        if (s.ok()) {
          ParsedBaseDataValue parsed_internal_value(&value);
          parsed_internal_value.StripSuffix();
          vss->push_back({value, Status::OK()});
        } else if (s.IsNotFound()) {
          vss->push_back({std::string(), Status::NotFound()});
        } else {
          vss->clear();
          return s;
        }
      }
    }
    return Status::OK();
  } else if (s.IsNotFound()) {
    for (size_t idx = 0; idx < fields.size(); ++idx) {
      vss->push_back({std::string(), Status::NotFound()});
    }
  }
  return s;
}

Status Redis::HMSet(const Slice& key, const std::vector<FieldValue>& fvs) {
  uint32_t statistic = 0;
  std::unordered_set<std::string> fields;
  std::vector<FieldValue> filtered_fvs;
  for (auto iter = fvs.rbegin(); iter != fvs.rend(); ++iter) {
    std::string field = iter->field;
    if (fields.find(field) == fields.end()) {
      fields.insert(field);
      filtered_fvs.push_back(*iter);
    }
  }

  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  uint64_t version = 0;
  std::string meta_value;

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  char meta_value_buf[4] = {0};
  if (s.ok() && !ExpectedMetaValue(DataType::kHashes, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kHashes)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
    }
  }
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale() || parsed_hashes_meta_value.Count() == 0) {
      version = parsed_hashes_meta_value.InitialMetaValue();
      if (!parsed_hashes_meta_value.check_set_count(static_cast<int32_t>(filtered_fvs.size()))) {
        return Status::InvalidArgument("hash size overflow");
      }
      parsed_hashes_meta_value.SetCount(static_cast<int32_t>(filtered_fvs.size()));
      batch.Put(handles_[kMetaCF], base_meta_key.Encode(), meta_value);
      for (const auto& fv : filtered_fvs) {
        HashesDataKey hashes_data_key(key, version, fv.field);
        BaseDataValue inter_value(fv.value);
        batch.Put(handles_[kHashesDataCF], hashes_data_key.Encode(), inter_value.Encode());
      }
    } else {
      int32_t count = 0;
      std::string data_value;
      version = parsed_hashes_meta_value.Version();
      for (const auto& fv : filtered_fvs) {
        HashesDataKey hashes_data_key(key, version, fv.field);
        BaseDataValue inter_value(fv.value);
        s = db_->Get(default_read_options_, handles_[kHashesDataCF], hashes_data_key.Encode(), &data_value);
        if (s.ok()) {
          statistic++;
          batch.Put(handles_[kHashesDataCF], hashes_data_key.Encode(), inter_value.Encode());
        } else if (s.IsNotFound()) {
          count++;
          batch.Put(handles_[kHashesDataCF], hashes_data_key.Encode(), inter_value.Encode());
        } else {
          return s;
        }
      }
      if (!parsed_hashes_meta_value.CheckModifyCount(count)){
        return Status::InvalidArgument("hash size overflow");
      }
      parsed_hashes_meta_value.ModifyCount(count);
      batch.Put(handles_[kMetaCF], base_meta_key.Encode(), meta_value);
    }
  } else if (s.IsNotFound()) {
    EncodeFixed32(meta_value_buf, filtered_fvs.size());
    HashesMetaValue hashes_meta_value(DataType::kHashes, Slice(meta_value_buf, 4));
    version = hashes_meta_value.UpdateVersion();
    batch.Put(handles_[kMetaCF], base_meta_key.Encode(), hashes_meta_value.Encode());
    for (const auto& fv : filtered_fvs) {
      HashesDataKey hashes_data_key(key, version, fv.field);
      BaseDataValue inter_value(fv.value);
      batch.Put(handles_[kHashesDataCF], hashes_data_key.Encode(), inter_value.Encode());
    }
  }
  s = db_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(DataType::kHashes, key.ToString(), statistic);
  return s;
}

Status Redis::HSet(const Slice& key, const Slice& field, const Slice& value, int32_t* res) {
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  uint64_t version = 0;
  uint32_t statistic = 0;
  std::string meta_value;

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  char meta_value_buf[4] = {0};
  if (s.ok() && !ExpectedMetaValue(DataType::kHashes, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kHashes)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
    }
  }
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale() || parsed_hashes_meta_value.Count() == 0) {
      version = parsed_hashes_meta_value.InitialMetaValue();
      parsed_hashes_meta_value.SetCount(1);
      batch.Put(handles_[kMetaCF], base_meta_key.Encode(), meta_value);
      HashesDataKey data_key(key, version, field);
      BaseDataValue internal_value(value);
      batch.Put(handles_[kHashesDataCF], data_key.Encode(), internal_value.Encode());
      *res = 1;
    } else {
      version = parsed_hashes_meta_value.Version();
      std::string data_value;
      HashesDataKey hashes_data_key(key, version, field);
      s = db_->Get(default_read_options_, handles_[kHashesDataCF], hashes_data_key.Encode(), &data_value);
      if (s.ok()) {
        *res = 0;
        if (data_value == value.ToString()) {
          return Status::OK();
        } else {
          BaseDataValue internal_value(value);
          batch.Put(handles_[kHashesDataCF], hashes_data_key.Encode(), internal_value.Encode());
          statistic++;
        }
      } else if (s.IsNotFound()) {
        if (!parsed_hashes_meta_value.CheckModifyCount(1)){
          return Status::InvalidArgument("hash size overflow");
        }
        parsed_hashes_meta_value.ModifyCount(1);
        BaseDataValue internal_value(value);
        batch.Put(handles_[kMetaCF], base_meta_key.Encode(), meta_value);
        batch.Put(handles_[kHashesDataCF], hashes_data_key.Encode(), internal_value.Encode());
        *res = 1;
      } else {
        return s;
      }
    }
  } else if (s.IsNotFound()) {
    EncodeFixed32(meta_value_buf, 1);
    HashesMetaValue hashes_meta_value(DataType::kHashes, Slice(meta_value_buf, 4));
    version = hashes_meta_value.UpdateVersion();
    batch.Put(handles_[kMetaCF], base_meta_key.Encode(), hashes_meta_value.Encode());
    HashesDataKey data_key(key, version, field);
    BaseDataValue internal_value(value);
    batch.Put(handles_[kHashesDataCF], data_key.Encode(), internal_value.Encode());
    *res = 1;
  } else {
    return s;
  }
  s = db_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(DataType::kHashes, key.ToString(), statistic);
  return s;
}

Status Redis::HSetnx(const Slice& key, const Slice& field, const Slice& value, int32_t* ret) {
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  uint64_t version = 0;
  std::string meta_value;

  BaseMetaKey base_meta_key(key);
  BaseDataValue internal_value(value);
  Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  char meta_value_buf[4] = {0};
  if (s.ok() && !ExpectedMetaValue(DataType::kHashes, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kHashes)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
    }
  }
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale() || parsed_hashes_meta_value.Count() == 0) {
      version = parsed_hashes_meta_value.InitialMetaValue();
      parsed_hashes_meta_value.SetCount(1);
      batch.Put(handles_[kMetaCF], base_meta_key.Encode(), meta_value);
      HashesDataKey hashes_data_key(key, version, field);
      batch.Put(handles_[kHashesDataCF], hashes_data_key.Encode(), internal_value.Encode());
      *ret = 1;
    } else {
      version = parsed_hashes_meta_value.Version();
      HashesDataKey hashes_data_key(key, version, field);
      std::string data_value;
      s = db_->Get(default_read_options_, handles_[kHashesDataCF], hashes_data_key.Encode(), &data_value);
      if (s.ok()) {
        *ret = 0;
      } else if (s.IsNotFound()) {
        if (!parsed_hashes_meta_value.CheckModifyCount(1)){
          return Status::InvalidArgument("hash size overflow");
        }
        parsed_hashes_meta_value.ModifyCount(1);
        batch.Put(handles_[kMetaCF], base_meta_key.Encode(), meta_value);
        batch.Put(handles_[kHashesDataCF], hashes_data_key.Encode(), internal_value.Encode());
        *ret = 1;
      } else {
        return s;
      }
    }
  } else if (s.IsNotFound()) {
    EncodeFixed32(meta_value_buf, 1);
    HashesMetaValue hashes_meta_value(DataType::kHashes, Slice(meta_value_buf, 4));
    version = hashes_meta_value.UpdateVersion();
    batch.Put(handles_[kMetaCF], base_meta_key.Encode(), hashes_meta_value.Encode());
    HashesDataKey hashes_data_key(key, version, field);
    batch.Put(handles_[kHashesDataCF], hashes_data_key.Encode(), internal_value.Encode());
    *ret = 1;
  } else {
    return s;
  }
  return db_->Write(default_write_options_, &batch);
}

Status Redis::HVals(const Slice& key, std::vector<std::string>* values) {
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  uint64_t version = 0;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(read_options, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok() && !ExpectedMetaValue(DataType::kHashes, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kHashes)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
    }
  }
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_hashes_meta_value.Count() == 0) {
      return Status::NotFound();
    } else {
      version = parsed_hashes_meta_value.Version();
      HashesDataKey hashes_data_key(key, version, "");
      Slice prefix = hashes_data_key.EncodeSeekKey();
      KeyStatisticsDurationGuard guard(this, DataType::kHashes, key.ToString());
      auto iter = db_->NewIterator(read_options, handles_[kHashesDataCF]);
      for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
        ParsedBaseDataValue parsed_internal_value(iter->value());
        values->push_back(parsed_internal_value.UserValue().ToString());
      }
      delete iter;
    }
  }
  return s;
}

Status Redis::HStrlen(const Slice& key, const Slice& field, int32_t* len) {
  std::string value;
  Status s = HGet(key, field, &value);
  if (s.ok()) {
    *len = static_cast<int32_t>(value.size());
  } else {
    *len = 0;
  }
  return s;
}

Status Redis::HScan(const Slice& key, int64_t cursor, const std::string& pattern, int64_t count,
                       std::vector<FieldValue>* field_values, int64_t* next_cursor) {
  *next_cursor = 0;
  field_values->clear();
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

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(read_options, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok() && !ExpectedMetaValue(DataType::kHashes, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kHashes)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
    }
  }
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale() || parsed_hashes_meta_value.Count() == 0) {
      *next_cursor = 0;
      return Status::NotFound();
    } else {
      std::string sub_field;
      std::string start_point;
      uint64_t version = parsed_hashes_meta_value.Version();
      s = GetScanStartPoint(DataType::kHashes, key, pattern, cursor, &start_point);
      if (s.IsNotFound()) {
        cursor = 0;
        if (isTailWildcard(pattern)) {
          start_point = pattern.substr(0, pattern.size() - 1);
        }
      }
      if (isTailWildcard(pattern)) {
        sub_field = pattern.substr(0, pattern.size() - 1);
      }

      HashesDataKey hashes_data_prefix(key, version, sub_field);
      HashesDataKey hashes_start_data_key(key, version, start_point);
      std::string prefix = hashes_data_prefix.EncodeSeekKey().ToString();
      KeyStatisticsDurationGuard guard(this, DataType::kHashes, key.ToString());
      rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[kHashesDataCF]);
      for (iter->Seek(hashes_start_data_key.Encode()); iter->Valid() && rest > 0 && iter->key().starts_with(prefix);
           iter->Next()) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key());
        std::string field = parsed_hashes_data_key.field().ToString();
        if (StringMatch(pattern.data(), pattern.size(), field.data(), field.size(), 0) != 0) {
          ParsedBaseDataValue parsed_internal_value(iter->value());
          field_values->emplace_back(field, parsed_internal_value.UserValue().ToString());
        }
        rest--;
      }

      if (iter->Valid() && (iter->key().compare(prefix) <= 0 || iter->key().starts_with(prefix))) {
        *next_cursor = cursor + step_length;
        ParsedHashesDataKey parsed_hashes_data_key(iter->key());
        std::string next_field = parsed_hashes_data_key.field().ToString();
        StoreScanNextPoint(DataType::kHashes, key, pattern, *next_cursor, next_field);
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

Status Redis::HScanx(const Slice& key, const std::string& start_field, const std::string& pattern, int64_t count,
                           std::vector<FieldValue>* field_values, std::string* next_field) {
  next_field->clear();
  field_values->clear();

  int64_t rest = count;
  std::string meta_value;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(read_options, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok() && !ExpectedMetaValue(DataType::kHashes, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kHashes)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
    }
  }
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale() || parsed_hashes_meta_value.Count() == 0) {
      *next_field = "";
      return Status::NotFound();
    } else {
      uint64_t version = parsed_hashes_meta_value.Version();
      HashesDataKey hashes_data_prefix(key, version, Slice());
      HashesDataKey hashes_start_data_key(key, version, start_field);
      std::string prefix = hashes_data_prefix.EncodeSeekKey().ToString();
      KeyStatisticsDurationGuard guard(this, DataType::kHashes, key.ToString());
      rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[kHashesDataCF]);
      for (iter->Seek(hashes_start_data_key.Encode()); iter->Valid() && rest > 0 && iter->key().starts_with(prefix);
           iter->Next()) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key());
        std::string field = parsed_hashes_data_key.field().ToString();
        if (StringMatch(pattern.data(), pattern.size(), field.data(), field.size(), 0) != 0) {
          ParsedBaseDataValue parsed_value(iter->value());
          field_values->emplace_back(field, parsed_value.UserValue().ToString());
        }
        rest--;
      }

      if (iter->Valid() && iter->key().starts_with(prefix)) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key());
        *next_field = parsed_hashes_data_key.field().ToString();
      } else {
        *next_field = "";
      }
      delete iter;
    }
  } else {
    *next_field = "";
    return s;
  }
  return Status::OK();
}

Status Redis::PKHScanRange(const Slice& key, const Slice& field_start, const std::string& field_end,
                                 const Slice& pattern, int32_t limit, std::vector<FieldValue>* field_values,
                                 std::string* next_field) {
  next_field->clear();
  field_values->clear();

  int64_t remain = limit;
  std::string meta_value;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  bool start_no_limit = field_start.compare("") == 0;
  bool end_no_limit = field_end.empty();

  if (!start_no_limit && !end_no_limit && (field_start.compare(field_end) > 0)) {
    return Status::InvalidArgument("error in given range");
  }

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(read_options, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok() && !ExpectedMetaValue(DataType::kHashes, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kHashes)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
    }
  }
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale() || parsed_hashes_meta_value.Count() == 0) {
      return Status::NotFound();
    } else {
      uint64_t version = parsed_hashes_meta_value.Version();
      HashesDataKey hashes_data_prefix(key, version, Slice());
      HashesDataKey hashes_start_data_key(key, version, field_start);
      std::string prefix = hashes_data_prefix.EncodeSeekKey().ToString();
      KeyStatisticsDurationGuard guard(this, DataType::kHashes, key.ToString());
      rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[kHashesDataCF]);
      for (iter->Seek(start_no_limit ? prefix : hashes_start_data_key.Encode());
           iter->Valid() && remain > 0 && iter->key().starts_with(prefix); iter->Next()) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key());
        std::string field = parsed_hashes_data_key.field().ToString();
        if (!end_no_limit && field.compare(field_end) > 0) {
          break;
        }
        if (StringMatch(pattern.data(), pattern.size(), field.data(), field.size(), 0) != 0) {
          ParsedBaseDataValue parsed_internal_value(iter->value());
          field_values->push_back({field, parsed_internal_value.UserValue().ToString()});
        }
        remain--;
      }

      if (iter->Valid() && iter->key().starts_with(prefix)) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key());
        if (end_no_limit || parsed_hashes_data_key.field().compare(field_end) <= 0) {
          *next_field = parsed_hashes_data_key.field().ToString();
        }
      }
      delete iter;
    }
  } else {
    return s;
  }
  return Status::OK();
}

Status Redis::PKHRScanRange(const Slice& key, const Slice& field_start, const std::string& field_end,
                                  const Slice& pattern, int32_t limit, std::vector<FieldValue>* field_values,
                                  std::string* next_field) {
  next_field->clear();
  field_values->clear();

  int64_t remain = limit;
  std::string meta_value;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  bool start_no_limit = field_start.compare("") == 0;
  bool end_no_limit = field_end.empty();

  if (!start_no_limit && !end_no_limit && (field_start.compare(field_end) < 0)) {
    return Status::InvalidArgument("error in given range");
  }

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(read_options, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok() && !ExpectedMetaValue(DataType::kHashes, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kHashes)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
    }
  }
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale() || parsed_hashes_meta_value.Count() == 0) {
      return Status::NotFound();
    } else {
      uint64_t version = parsed_hashes_meta_value.Version();
      uint64_t start_key_version = start_no_limit ? version + 1 : version;
      std::string start_key_field = start_no_limit ? "" : field_start.ToString();
      HashesDataKey hashes_data_prefix(key, version, Slice());
      HashesDataKey hashes_start_data_key(key, start_key_version, start_key_field);
      std::string prefix = hashes_data_prefix.EncodeSeekKey().ToString();
      KeyStatisticsDurationGuard guard(this, DataType::kHashes, key.ToString());
      rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[kHashesDataCF]);
      for (iter->SeekForPrev(hashes_start_data_key.Encode().ToString());
           iter->Valid() && remain > 0 && iter->key().starts_with(prefix); iter->Prev()) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key());
        std::string field = parsed_hashes_data_key.field().ToString();
        if (!end_no_limit && field.compare(field_end) < 0) {
          break;
        }
        if (StringMatch(pattern.data(), pattern.size(), field.data(), field.size(), 0) != 0) {
          ParsedBaseDataValue parsed_value(iter->value());
          field_values->push_back({field, parsed_value.UserValue().ToString()});
        }
        remain--;
      }

      if (iter->Valid() && iter->key().starts_with(prefix)) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key());
        if (end_no_limit || parsed_hashes_data_key.field().compare(field_end) >= 0) {
          *next_field = parsed_hashes_data_key.field().ToString();
        }
      }
      delete iter;
    }
  } else {
    return s;
  }
  return Status::OK();
}

Status Redis::HashesExpire(const Slice& key, int64_t ttl, std::string&& prefetch_meta) {
  std::string meta_value(std::move(prefetch_meta));
  ScopeRecordLock l(lock_mgr_, key);
  BaseMetaKey base_meta_key(key);
  Status s;

  // meta_value is empty means no meta value get before,
  // we should get meta first
  if (meta_value.empty()) {
    s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
    if (s.ok() && !ExpectedMetaValue(DataType::kHashes, meta_value)) {
      if (ExpectedStale(meta_value)) {
        s = Status::NotFound();
      } else {
        return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kHashes)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
      }
    }
  }
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_hashes_meta_value.Count() == 0) {
      return Status::NotFound();
    }

    if (ttl > 0) {
      parsed_hashes_meta_value.SetRelativeTimestamp(ttl);
      s = db_->Put(default_write_options_, handles_[kMetaCF], base_meta_key.Encode(), meta_value);
    } else {
      parsed_hashes_meta_value.InitialMetaValue();
      s = db_->Put(default_write_options_, handles_[kMetaCF], base_meta_key.Encode(), meta_value);
    }
  }
  return s;
}

Status Redis::HashesDel(const Slice& key, std::string&& prefetch_meta) {
  std::string meta_value(std::move(prefetch_meta));
  ScopeRecordLock l(lock_mgr_, key);
  BaseMetaKey base_meta_key(key);
  Status s;

  // meta_value is empty means no meta value get before,
  // we should get meta first
  if (meta_value.empty()) {
    s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
    if (s.ok() && !ExpectedMetaValue(DataType::kHashes, meta_value)) {
      if (ExpectedStale(meta_value)) {
        s = Status::NotFound();
      } else {
        return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kHashes)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
      }
    }
  }
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_hashes_meta_value.Count() == 0) {
      return Status::NotFound();
    } else {
      uint32_t statistic = parsed_hashes_meta_value.Count();
      parsed_hashes_meta_value.InitialMetaValue();
      s = db_->Put(default_write_options_, handles_[kMetaCF], base_meta_key.Encode(), meta_value);
      UpdateSpecificKeyStatistics(DataType::kHashes, key.ToString(), statistic);
    }
  }
  return s;
}

Status Redis::HashesExpireat(const Slice& key, int64_t timestamp, std::string&& prefetch_meta) {
  std::string meta_value(std::move(prefetch_meta));
  ScopeRecordLock l(lock_mgr_, key);
  BaseMetaKey base_meta_key(key);
  Status s;

  // meta_value is empty means no meta value get before,
  // we should get meta first
  if (meta_value.empty()) {
    s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
    if (s.ok() && !ExpectedMetaValue(DataType::kHashes, meta_value)) {
      if (ExpectedStale(meta_value)) {
        s = Status::NotFound();
      } else {
        return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kHashes)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
      }
    }
  }
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_hashes_meta_value.Count() == 0) {
      return Status::NotFound();
    } else {
      if (timestamp > 0) {
        parsed_hashes_meta_value.SetEtime(static_cast<uint64_t>(timestamp));
      } else {
        parsed_hashes_meta_value.InitialMetaValue();
      }
      s = db_->Put(default_write_options_, handles_[kMetaCF], base_meta_key.Encode(), meta_value);
    }
  }
  return s;
}

Status Redis::HashesPersist(const Slice& key, std::string&& prefetch_meta) {
  std::string meta_value(std::move(prefetch_meta));
  ScopeRecordLock l(lock_mgr_, key);
  BaseMetaKey base_meta_key(key);
  Status s;

  // meta_value is empty means no meta value get before,
  // we should get meta first
  if (meta_value.empty()) {
    s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
    if (s.ok() && !ExpectedMetaValue(DataType::kHashes, meta_value)) {
      if (ExpectedStale(meta_value)) {
        s = Status::NotFound();
      } else {
        return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kHashes)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
      }
    }
  }
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_hashes_meta_value.Count() == 0) {
      return Status::NotFound();
    } else {
      uint64_t timestamp = parsed_hashes_meta_value.Etime();
      if (timestamp == 0) {
        return Status::NotFound("Not have an associated timeout");
      } else {
        parsed_hashes_meta_value.SetEtime(0);
        s = db_->Put(default_write_options_, handles_[kMetaCF], base_meta_key.Encode(), meta_value);
      }
    }
  }
  return s;
}

Status Redis::HashesTTL(const Slice& key, int64_t* timestamp, std::string&& prefetch_meta) {
  std::string meta_value(std::move(prefetch_meta));
  Status s;
  BaseMetaKey base_meta_key(key);

  // meta_value is empty means no meta value get before,
  // we should get meta first
  if (meta_value.empty()) {
    s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
    if (s.ok() && !ExpectedMetaValue(DataType::kHashes, meta_value)) {
      if (ExpectedStale(meta_value)) {
        s = Status::NotFound();
      } else {
        return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kHashes)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
      }
    }
  }
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      *timestamp = -2;
      return Status::NotFound("Stale");
    } else if (parsed_hashes_meta_value.Count() == 0) {
      *timestamp = -2;
      return Status::NotFound();
    } else {
      *timestamp = parsed_hashes_meta_value.Etime();
      if (*timestamp == 0) {
        *timestamp = -1;
      } else {
        int64_t curtime = pstd::NowMillis();
        *timestamp = *timestamp - curtime >= 0 ? *timestamp - curtime : -2;
      }
    }
  } else if (s.IsNotFound()) {
    *timestamp = -2;
  }
  return s;
}

void Redis::ScanHashes() {
  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;
  auto current_time = static_cast<int32_t>(time(nullptr));

  LOG(INFO) << "***************" << "rocksdb instance: " << index_ << " Hashes Meta Data***************";
  auto meta_iter = db_->NewIterator(iterator_options, handles_[kMetaCF]);
  for (meta_iter->SeekToFirst(); meta_iter->Valid(); meta_iter->Next()) {
    if (!ExpectedMetaValue(DataType::kHashes, meta_iter->value().ToString())) {
      continue;
    }
    ParsedHashesMetaValue parsed_hashes_meta_value(meta_iter->value());
    int32_t survival_time = 0;
    if (parsed_hashes_meta_value.Etime() != 0) {
      survival_time = parsed_hashes_meta_value.Etime() > current_time ? parsed_hashes_meta_value.Etime() - current_time : -1;
    }
    ParsedBaseMetaKey parsed_meta_key(meta_iter->key());

    LOG(INFO) << fmt::format("[key : {:<30}] [count : {:<10}] [timestamp : {:<10}] [version : {}] [survival_time : {}]",
                             parsed_meta_key.Key().ToString(), parsed_hashes_meta_value.Count(),
                             parsed_hashes_meta_value.Etime(), parsed_hashes_meta_value.Version(), survival_time);
  }
  delete meta_iter;

  LOG(INFO) << "***************Hashes Field Data***************";
  auto field_iter = db_->NewIterator(iterator_options, handles_[kHashesDataCF]);
  for (field_iter->SeekToFirst(); field_iter->Valid(); field_iter->Next()) {

    ParsedHashesDataKey parsed_hashes_data_key(field_iter->key());
    ParsedBaseDataValue parsed_internal_value(field_iter->value());

    LOG(INFO) << fmt::format("[key : {:<30}] [field : {:<20}] [value : {:<20}] [version : {}]",
                             parsed_hashes_data_key.Key().ToString(), parsed_hashes_data_key.field().ToString(),
                             parsed_internal_value.UserValue().ToString(), parsed_hashes_data_key.Version());
  }
  delete field_iter;
}

}  //  namespace storage
