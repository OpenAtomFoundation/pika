//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "src/redis.h"

#include <memory>

#include <fmt/core.h>
#include <glog/logging.h>

#include "pstd/include/pika_codis_slot.h"
#include "src/base_data_key_format.h"
#include "src/base_filter.h"
#include "src/pkhash_data_value_format.h"
#include "src/scope_record_lock.h"
#include "src/scope_snapshot.h"
#include "storage/util.h"

namespace storage {

Status Redis::PKHGet(const Slice& key, const Slice& field, std::string* value) {
  std::string meta_value;
  uint64_t version = 0;
  rocksdb::ReadOptions read_options;

  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(read_options, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok() && !ExpectedMetaValue(DataType::kPKHashes, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument("WRONGTYPE, key: " + key.ToString() +
                                     ", expect type: " + DataTypeStrings[static_cast<int>(DataType::kPKHashes)] +
                                     ", get type: " + DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
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
      s = db_->Get(read_options, handles_[kPKHashDataCF], data_key.Encode(), value);
      if (s.ok()) {
        ParsedPKHashDataValue parsed_internal_value(value);
        if (parsed_internal_value.IsStale()) {
          return Status::NotFound("Stale");
        }
        parsed_internal_value.StripSuffix();
      }
    }
  }
  return s;
}

Status Redis::PKHSet(const Slice& key, const Slice& field, const Slice& value, int32_t* res) {
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  int32_t version = 0;
  std::string meta_value;
  uint32_t statistic = 0;

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  char meta_value_buf[4] = {0};
  if (s.ok() && !ExpectedMetaValue(DataType::kPKHashes, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument("WRONGTYPE, key: " + key.ToString() +
                                     ", expect type: " + DataTypeStrings[static_cast<int>(DataType::kPKHashes)] +
                                     ", get type: " + DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
    }
  }
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.Count() == 0 || parsed_hashes_meta_value.IsStale()) {
      version = parsed_hashes_meta_value.InitialMetaValue();
      parsed_hashes_meta_value.SetCount(1);
      batch.Put(handles_[kMetaCF], base_meta_key.Encode(), meta_value);
      HashesDataKey data_key(key, version, field);
      PKHashDataValue ehashes_value(value);
      batch.Put(handles_[kPKHashDataCF], data_key.Encode(), ehashes_value.Encode());
      *res = 1;
    } else {
      version = parsed_hashes_meta_value.Version();
      std::string data_value;
      HashesDataKey hashes_data_key(key, version, field);
      s = db_->Get(default_read_options_, handles_[kPKHashDataCF], hashes_data_key.Encode(), &data_value);
      if (s.ok()) {
        ParsedPKHashDataValue parsed_internal_value(data_value);
        *res = 0;
        // if [field:value] already expire and then the [field:value] should be updated
        if (parsed_internal_value.IsStale()) {
          *res = 1;
          PKHashDataValue internal_value(value);
          batch.Put(handles_[kPKHashDataCF], hashes_data_key.Encode(), internal_value.Encode());
          statistic++;
        } else {
          if (data_value == value.ToString()) {
            return Status::OK();
          } else {
            PKHashDataValue internal_value(value);
            batch.Put(handles_[kPKHashDataCF], hashes_data_key.Encode(), internal_value.Encode());
            statistic++;
          }
        }
      } else if (s.IsNotFound()) {
        if (!parsed_hashes_meta_value.CheckModifyCount(1)) {
          return Status::InvalidArgument("hash size overflow");
        }

        parsed_hashes_meta_value.ModifyCount(1);
        batch.Put(handles_[kMetaCF], base_meta_key.Encode(), meta_value);
        PKHashDataValue ehashes_value(value);
        batch.Put(handles_[kPKHashDataCF], hashes_data_key.Encode(), ehashes_value.Encode());
        *res = 1;
      } else {
        return s;
      }
    }
  } else if (s.IsNotFound()) {
    EncodeFixed32(meta_value_buf, 1);
    HashesMetaValue hashes_meta_value(DataType::kPKHashes, Slice(meta_value_buf, 4));
    version = hashes_meta_value.UpdateVersion();
    batch.Put(handles_[kMetaCF], base_meta_key.Encode(), hashes_meta_value.Encode());
    HashesDataKey data_key(key, version, field);
    PKHashDataValue ehashes_value(value);
    batch.Put(handles_[kPKHashDataCF], data_key.Encode(), ehashes_value.Encode());
    *res = 1;
  } else {
    return s;
  }

  s = db_->Write(default_write_options_, &batch);

  UpdateSpecificKeyStatistics(DataType::kPKHashes, key.ToString(), statistic);
  return s;
}

// Pika Hash Commands
Status Redis::PKHExpire(const Slice& key, int32_t ttl, int32_t numfields, const std::vector<std::string>& fields,
                        std::vector<int32_t>* rets) {
  if (ttl <= 0) {
    return Status::InvalidArgument("invalid expire time, must be >= 0");
  }

  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  bool is_stale = false;
  int32_t version = 0;
  std::string meta_value;

  // const rocksdb::Snapshot* snapshot;
  // ScopeSnapshot ss(db_, &snapshot);

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);

  if (s.ok() && !ExpectedMetaValue(DataType::kPKHashes, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument("WRONGTYPE, key: " + key.ToString() +
                                     ", expect type: " + DataTypeStrings[static_cast<int>(DataType::kPKHashes)] +
                                     ", get type: " + DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
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

      for (const auto& field : fields) {
        HashesDataKey data_key(key, version, field);
        std::string data_value;
        s = db_->Get(default_read_options_, handles_[kPKHashDataCF], data_key.Encode(), &data_value);
        if (s.ok()) {
          ParsedPKHashDataValue parsed_internal_value(&data_value);
          if (parsed_internal_value.IsStale()) {
            rets->push_back(-2);
          } else {
            rets->push_back(1);
            parsed_internal_value.SetRelativeTimestamp(ttl);
            batch.Put(handles_[kPKHashDataCF], data_key.Encode(), data_value);
          }
        }
      }
      s = db_->Write(default_write_options_, &batch);

      return s;
    }
  } else if (s.IsNotFound()) {
    return Status::NotFound(is_stale ? "Stale" : "NotFound");
  }
  return s;
}

Status Redis::PKHExpireat(const Slice& key, int64_t timestamp, int32_t numfields,
                          const std::vector<std::string>& fields, std::vector<int32_t>* rets) {
  if (timestamp <= 0) {
    rets->assign(numfields, 2);
    return Status::InvalidArgument("invalid expire time, must be >= 0");
  }

  int64_t unix_time;
  rocksdb::Env::Default()->GetCurrentTime(&unix_time);
  if (timestamp < unix_time) {
    rets->assign(numfields, 2);
    return Status::InvalidArgument("invalid expire time, called with a past Unix time in seconds or milliseconds.");
  }

  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  bool is_stale = false;
  int32_t version = 0;
  std::string meta_value;

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);

  if (s.ok() && !ExpectedMetaValue(DataType::kPKHashes, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument("WRONGTYPE, key: " + key.ToString() +
                                     ", expect type: " + DataTypeStrings[static_cast<int>(DataType::kPKHashes)] +
                                     ", get type: " + DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
    }
  }
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      rets->assign(numfields, -2);
      return Status::NotFound("Stale");
    } else if (parsed_hashes_meta_value.Count() == 0) {
      rets->assign(numfields, -2);
      return Status::NotFound();
    } else {
      version = parsed_hashes_meta_value.Version();

      for (const auto& field : fields) {
        HashesDataKey data_key(key, version, field);
        std::string data_value;

        s = db_->Get(default_read_options_, handles_[kPKHashDataCF], data_key.Encode(), &data_value);
        if (s.ok()) {
          ParsedPKHashDataValue parsed_internal_value(&data_value);
          if (parsed_internal_value.IsStale()) {
            rets->push_back(-2);
          } else {
            parsed_internal_value.SetTimestamp(timestamp);
            batch.Put(handles_[kPKHashDataCF], data_key.Encode(), data_value);
            rets->push_back(1);
          }
        }
      }
      s = db_->Write(default_write_options_, &batch);
      return s;
    }
  } else if (s.IsNotFound()) {
    return Status::NotFound(is_stale ? "Stale" : "NotFound");
  }
  return s;
}

Status Redis::PKHExpiretime(const Slice& key, int32_t numfields, const std::vector<std::string>& fields,
                            std::vector<int64_t>* timestamps) {
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  bool is_stale = false;
  int32_t version = 0;
  std::string meta_value;

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);

  if (s.ok() && !ExpectedMetaValue(DataType::kPKHashes, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument("WRONGTYPE, key: " + key.ToString() +
                                     ", expect type: " + DataTypeStrings[static_cast<int>(DataType::kPKHashes)] +
                                     ", get type: " + DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
    }
  }
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      timestamps->assign(numfields, -2);
      return Status::NotFound("Stale");
    } else if (parsed_hashes_meta_value.Count() == 0) {
      timestamps->assign(numfields, -2);
      return Status::NotFound();
    } else {
      version = parsed_hashes_meta_value.Version();

      for (const auto& field : fields) {
        HashesDataKey data_key(key, version, field);
        std::string data_value;
        s = db_->Get(default_read_options_, handles_[kPKHashDataCF], data_key.Encode(), &data_value);
        if (s.ok()) {
          ParsedPKHashDataValue parsed_internal_value(&data_value);
          if (parsed_internal_value.IsStale()) {
            timestamps->push_back(-2);
          } else {
            int64_t etime = parsed_internal_value.Etime();
            if (etime == 0) {
              timestamps->push_back(-1);
            } else {
              timestamps->push_back(etime);
            }
          }
        }
      }
      return s;
    }
  } else if (s.IsNotFound()) {
    timestamps->assign(numfields, -2);
    return Status::NotFound(is_stale ? "Stale" : "NotFound");
  }
  return s;
}

Status Redis::PKHTTL(const Slice& key, int32_t numfields, const std::vector<std::string>& fields,
                     std::vector<int64_t>* ttls) {
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  bool is_stale = false;
  int32_t version = 0;
  std::string meta_value;

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);

  if (s.ok() && !ExpectedMetaValue(DataType::kPKHashes, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument("WRONGTYPE, key: " + key.ToString() +
                                     ", expect type: " + DataTypeStrings[static_cast<int>(DataType::kPKHashes)] +
                                     ", get type: " + DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
    }
  }
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      ttls->assign(numfields, -2);
      return Status::NotFound("Stale");
    } else if (parsed_hashes_meta_value.Count() == 0) {
      ttls->assign(numfields, -2);
      return Status::NotFound();
    } else {
      version = parsed_hashes_meta_value.Version();

      for (const auto& field : fields) {
        HashesDataKey data_key(key, version, field);
        std::string data_value;
        s = db_->Get(default_read_options_, handles_[kPKHashDataCF], data_key.Encode(), &data_value);
        if (s.ok()) {
          ParsedPKHashDataValue parsed_internal_value(&data_value);
          if (parsed_internal_value.IsStale()) {
            ttls->push_back(-2);
          } else {
            int64_t etime = parsed_internal_value.Etime();
            if (etime == 0) {
              ttls->push_back(-1);
            } else {
              int64_t unix_time;
              rocksdb::Env::Default()->GetCurrentTime(&unix_time);
              int64_t ttl = etime - unix_time;
              ttls->push_back(ttl);
            }
          }
        }
      }

      return s;
    }
  } else if (s.IsNotFound()) {
    return Status::NotFound(is_stale ? "Stale" : "NotFound");
  }
  return s;
}

Status Redis::PKHPersist(const Slice& key, int32_t numfields, const std::vector<std::string>& fields,
                         std::vector<int32_t>* rets) {
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  bool is_stale = false;
  int32_t version = 0;
  std::string meta_value;

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);

  if (s.ok() && !ExpectedMetaValue(DataType::kPKHashes, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument("WRONGTYPE, key: " + key.ToString() +
                                     ", expect type: " + DataTypeStrings[static_cast<int>(DataType::kPKHashes)] +
                                     ", get type: " + DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
    }
  }
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      rets->assign(numfields, -2);
      return Status::NotFound("Stale");
    } else if (parsed_hashes_meta_value.Count() == 0) {
      rets->assign(numfields, -2);
      return Status::NotFound();
    } else {
      version = parsed_hashes_meta_value.Version();

      for (const auto& field : fields) {
        HashesDataKey data_key(key, version, field);
        std::string data_value;
        s = db_->Get(default_read_options_, handles_[kPKHashDataCF], data_key.Encode(), &data_value);
        if (s.ok()) {
          ParsedPKHashDataValue parsed_internal_value(&data_value);
          if (parsed_internal_value.IsStale()) {
            rets->push_back(-1);
          } else {
            rets->push_back(1);
            parsed_internal_value.SetEtime(0);
            batch.Put(handles_[kPKHashDataCF], data_key.Encode(), data_value);
          }
        }
      }
      s = db_->Write(default_write_options_, &batch);

      return s;
    }
  } else if (s.IsNotFound()) {
    return Status::NotFound(is_stale ? "Stale" : "NotFound");
  }
  return s;
}

Status Redis::PKHSetex(const Slice& key, const Slice& field, const Slice& value, int32_t ttl, int32_t* ret) {
  if (ttl <= 0) {
    return Status::InvalidArgument("invalid expire time");
  }

  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  int32_t version = 0;
  std::string meta_value;
  uint32_t statistic = 0;

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  char meta_value_buf[4] = {0};
  // 1. 判断类型是否匹配和key是否过期。
  if (s.ok() && !ExpectedMetaValue(DataType::kPKHashes, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument("WRONGTYPE, key: " + key.ToString() +
                                     ", expect type: " + DataTypeStrings[static_cast<int>(DataType::kPKHashes)] +
                                     ", get type: " + DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
    }
  }

  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.Count() == 0 || parsed_hashes_meta_value.IsStale()) {
      version = parsed_hashes_meta_value.InitialMetaValue();
      parsed_hashes_meta_value.SetCount(1);
      batch.Put(handles_[kMetaCF], base_meta_key.Encode(), meta_value);
      HashesDataKey data_key(key, version, field);
      PKHashDataValue ehashes_value(value);
      ehashes_value.SetRelativeTimestamp(ttl);
      batch.Put(handles_[kPKHashDataCF], data_key.Encode(), ehashes_value.Encode());
      *ret = 1;
    } else {
      version = parsed_hashes_meta_value.Version();
      std::string data_value;
      HashesDataKey hashes_data_key(key, version, field);
      s = db_->Get(default_read_options_, handles_[kPKHashDataCF], hashes_data_key.Encode(), &data_value);
      if (s.ok()) {
        *ret = 1;
        if (s.ok()) {
          PKHashDataValue ehashes_value(value);
          ehashes_value.SetRelativeTimestamp(ttl);
          batch.Put(handles_[kPKHashDataCF], hashes_data_key.Encode(), ehashes_value.Encode());
          statistic++;
        } else if (s.IsNotFound()) {
          parsed_hashes_meta_value.ModifyCount(1);
          batch.Put(handles_[kMetaCF], key, meta_value);
          PKHashDataValue ehashes_value(value);
          ehashes_value.SetRelativeTimestamp(ttl);
          batch.Put(handles_[kPKHashDataCF], hashes_data_key.Encode(), ehashes_value.Encode());
          statistic++;
        } else {
          return s;
        }

      } else if (s.IsNotFound()) {
        if (!parsed_hashes_meta_value.CheckModifyCount(1)) {
          return Status::InvalidArgument("hash size overflow");
        }
        parsed_hashes_meta_value.ModifyCount(1);
        batch.Put(handles_[kMetaCF], base_meta_key.Encode(), meta_value);
        PKHashDataValue ehashes_value(value);
        ehashes_value.SetRelativeTimestamp(ttl);
        batch.Put(handles_[kPKHashDataCF], hashes_data_key.Encode(), ehashes_value.Encode());
        *ret = 1;
      } else {
        return s;
      }
    }
  } else if (s.IsNotFound()) {
    EncodeFixed32(meta_value_buf, 1);
    HashesMetaValue hashes_meta_value(DataType::kPKHashes, Slice(meta_value_buf, 4));
    version = hashes_meta_value.UpdateVersion();
    batch.Put(handles_[kMetaCF], base_meta_key.Encode(), hashes_meta_value.Encode());
    HashesDataKey data_key(key, version, field);
    PKHashDataValue ehashes_value(value);
    ehashes_value.SetRelativeTimestamp(ttl);
    batch.Put(handles_[kPKHashDataCF], data_key.Encode(), ehashes_value.Encode());
    *ret = 1;
  } else {
    return s;
  }

  return db_->Write(default_write_options_, &batch);
}

Status Redis::PKHExists(const Slice& key, const Slice& field) {
  std::string value;
  return PKHGet(key, field, &value);
}

Status Redis::PKHDel(const Slice& key, const std::vector<std::string>& fields, int32_t* ret) {
  uint32_t statistic = 0;
  std::vector<std::string> filtered_fields;
  std::unordered_set<std::string> field_set;
  for (const auto& iter : fields) {
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
  if (s.ok() && !ExpectedMetaValue(DataType::kPKHashes, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument("WRONGTYPE, key: " + key.ToString() +
                                     ", expect type: " + DataTypeStrings[static_cast<int>(DataType::kPKHashes)] +
                                     ", get type: " + DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
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
        s = db_->Get(read_options, handles_[kPKHashDataCF], hashes_data_key.Encode(), &data_value);
        if (s.ok()) {
          del_cnt++;
          statistic++;
          batch.Delete(handles_[kPKHashDataCF], hashes_data_key.Encode());
        } else if (s.IsNotFound()) {
          continue;
        } else {
          return s;
        }
      }
      *ret = del_cnt;
      if (!parsed_hashes_meta_value.CheckModifyCount(-del_cnt)) {
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
  UpdateSpecificKeyStatistics(DataType::kPKHashes, key.ToString(), statistic);
  return s;
}

Status Redis::PKHLen(const Slice& key, int32_t* ret, std::string&& prefetch_meta) {
  *ret = 0;
  Status s;
  std::string meta_value(std::move(prefetch_meta));

  // meta_value is empty means no meta value get before,
  // we should get meta first
  if (meta_value.empty()) {
    BaseMetaKey base_meta_key(key);
    s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
    if (s.ok() && !ExpectedMetaValue(DataType::kPKHashes, meta_value)) {
      if (ExpectedStale(meta_value)) {
        s = Status::NotFound();
      } else {
        return Status::InvalidArgument("WRONGTYPE, key: " + key.ToString() + ", expect type: " +
                                       DataTypeStrings[static_cast<int>(DataType::kPKHashes)] + ", get type: " +
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

// Status Redis::PKHLenForce(const Slice& key, int32_t* ret) {}

Status Redis::PKHStrlen(const Slice& key, const Slice& field, int32_t* len) {
  std::string value;
  Status s = PKHGet(key, field, &value);
  if (s.ok()) {
    *len = static_cast<int32_t>(value.size());
  } else {
    *len = 0;
  }
  return s;
}

Status Redis::PKHIncrby(const Slice& key, const Slice& field, int64_t value, int64_t* ret, int32_t ttl) {
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
  if (s.ok() && !ExpectedMetaValue(DataType::kPKHashes, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument("WRONGTYPE, key: " + key.ToString() +
                                     ", expect type: " + DataTypeStrings[static_cast<int>(DataType::kPKHashes)] +
                                     ", get type: " + DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
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
      PKHashDataValue internal_value(value_buf);
      batch.Put(handles_[kPKHashDataCF], hashes_data_key.Encode(), internal_value.Encode());
      *ret = value;
    } else {
      version = parsed_hashes_meta_value.Version();
      HashesDataKey hashes_data_key(key, version, field);
      s = db_->Get(default_read_options_, handles_[kPKHashDataCF], hashes_data_key.Encode(), &old_value);
      if (s.ok()) {
        ParsedPKHashDataValue parsed_internal_value(&old_value);
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
        PKHashDataValue internal_value(value_buf);
        batch.Put(handles_[kPKHashDataCF], hashes_data_key.Encode(), internal_value.Encode());
        statistic++;
      } else if (s.IsNotFound()) {
        Int64ToStr(value_buf, 32, value);
        if (!parsed_hashes_meta_value.CheckModifyCount(1)) {
          return Status::InvalidArgument("hash size overflow");
        }
        PKHashDataValue internal_value(value_buf);
        parsed_hashes_meta_value.ModifyCount(1);
        batch.Put(handles_[kMetaCF], base_meta_key.Encode(), meta_value);
        batch.Put(handles_[kPKHashDataCF], hashes_data_key.Encode(), internal_value.Encode());
        *ret = value;
      } else {
        return s;
      }
    }
  } else if (s.IsNotFound()) {
    EncodeFixed32(meta_value_buf, 1);
    HashesMetaValue hashes_meta_value(DataType::kPKHashes, Slice(meta_value_buf, 4));
    version = hashes_meta_value.UpdateVersion();
    batch.Put(handles_[kMetaCF], base_meta_key.Encode(), hashes_meta_value.Encode());
    HashesDataKey hashes_data_key(key, version, field);

    Int64ToStr(value_buf, 32, value);
    PKHashDataValue internal_value(value_buf);
    batch.Put(handles_[kPKHashDataCF], hashes_data_key.Encode(), internal_value.Encode());
    *ret = value;
  } else {
    return s;
  }
  s = db_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(DataType::kPKHashes, key.ToString(), statistic);
  return s;
}

Status Redis::PKHMSet(const Slice& key, const std::vector<FieldValue>& fvs) {
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
  if (s.ok() && !ExpectedMetaValue(DataType::kPKHashes, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument("WRONGTYPE, key: " + key.ToString() +
                                     ", expect type: " + DataTypeStrings[static_cast<int>(DataType::kPKHashes)] +
                                     ", get type: " + DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
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
        PKHashDataValue inter_value(fv.value);
        batch.Put(handles_[kPKHashDataCF], hashes_data_key.Encode(), inter_value.Encode());
      }
    } else {
      int32_t count = 0;
      std::string data_value;
      version = parsed_hashes_meta_value.Version();
      for (const auto& fv : filtered_fvs) {
        HashesDataKey hashes_data_key(key, version, fv.field);
        PKHashDataValue inter_value(fv.value);
        s = db_->Get(default_read_options_, handles_[kPKHashDataCF], hashes_data_key.Encode(), &data_value);
        if (s.ok()) {
          statistic++;
          batch.Put(handles_[kPKHashDataCF], hashes_data_key.Encode(), inter_value.Encode());
        } else if (s.IsNotFound()) {
          count++;
          batch.Put(handles_[kPKHashDataCF], hashes_data_key.Encode(), inter_value.Encode());
        } else {
          return s;
        }
      }
      if (!parsed_hashes_meta_value.CheckModifyCount(count)) {
        return Status::InvalidArgument("hash size overflow");
      }
      parsed_hashes_meta_value.ModifyCount(count);
      batch.Put(handles_[kMetaCF], base_meta_key.Encode(), meta_value);
    }
  } else if (s.IsNotFound()) {
    EncodeFixed32(meta_value_buf, filtered_fvs.size());
    HashesMetaValue hashes_meta_value(DataType::kPKHashes, Slice(meta_value_buf, 4));
    version = hashes_meta_value.UpdateVersion();
    batch.Put(handles_[kMetaCF], base_meta_key.Encode(), hashes_meta_value.Encode());
    for (const auto& fv : filtered_fvs) {
      HashesDataKey hashes_data_key(key, version, fv.field);
      PKHashDataValue inter_value(fv.value);
      batch.Put(handles_[kPKHashDataCF], hashes_data_key.Encode(), inter_value.Encode());
    }
  }
  s = db_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(DataType::kPKHashes, key.ToString(), statistic);
  return s;
}
Status Redis::PKHMSetex(const Slice& key, const std::vector<FieldValueTTL>& fvts) {
  uint32_t statistic = 0;
  std::unordered_set<std::string> fields;
  std::vector<FieldValueTTL> filtered_fvs;
  for (auto iter = fvts.rbegin(); iter != fvts.rend(); ++iter) {
    std::string field = iter->field;
    if (fields.find(field) == fields.end()) {
      fields.insert(field);
      filtered_fvs.push_back(*iter);
    }
  }

  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  int32_t version = 0;
  std::string meta_value;

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  char meta_value_buf[4] = {0};
  if (s.ok() && !ExpectedMetaValue(DataType::kPKHashes, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument("WRONGTYPE, key: " + key.ToString() +
                                     ", expect type: " + DataTypeStrings[static_cast<int>(DataType::kPKHashes)] +
                                     ", get type: " + DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
    }
  }

  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.Count() == 0 || parsed_hashes_meta_value.IsStale()) {
      version = parsed_hashes_meta_value.InitialMetaValue();
      if (!parsed_hashes_meta_value.check_set_count(static_cast<int32_t>(filtered_fvs.size()))) {
        return Status::InvalidArgument("hash size overflow");
      }
      parsed_hashes_meta_value.SetCount(static_cast<int32_t>(filtered_fvs.size()));
      // parsed_hashes_meta_value.set_timestamp(0);
      batch.Put(handles_[kMetaCF], base_meta_key.Encode(), meta_value);
      for (const auto& fv : filtered_fvs) {
        HashesDataKey hashes_data_key(key, version, fv.field);
        PKHashDataValue ehashes_value(fv.value);
        if (fv.ttl > 0) {
          ehashes_value.SetRelativeTimestamp(fv.ttl);
        }
        batch.Put(handles_[kPKHashDataCF], hashes_data_key.Encode(), ehashes_value.Encode());
      }
    } else {
      int32_t count = 0;
      std::string data_value;
      version = parsed_hashes_meta_value.Version();
      for (const auto& fv : filtered_fvs) {
        HashesDataKey hashes_data_key(key, version, fv.field);
        s = db_->Get(default_read_options_, handles_[kPKHashDataCF], hashes_data_key.Encode(), &data_value);
        if (s.ok()) {
          statistic++;
          PKHashDataValue ehashes_value(fv.value);
          if (fv.ttl > 0) {
            ehashes_value.SetRelativeTimestamp(fv.ttl);
          }
          batch.Put(handles_[kPKHashDataCF], hashes_data_key.Encode(), ehashes_value.Encode());
        } else if (s.IsNotFound()) {
          count++;
          PKHashDataValue ehashes_value(fv.value);
          if (fv.ttl > 0) {
            ehashes_value.SetRelativeTimestamp(fv.ttl);
          }
          batch.Put(handles_[kPKHashDataCF], hashes_data_key.Encode(), ehashes_value.Encode());
        } else {
          return s;
        }
      }

      if (!parsed_hashes_meta_value.CheckModifyCount(count)) {
        return Status::InvalidArgument("hash size overflow");
      }

      parsed_hashes_meta_value.ModifyCount(count);
      batch.Put(handles_[kMetaCF], base_meta_key.Encode(), meta_value);
    }
  } else if (s.IsNotFound()) {
    // char str[4];
    EncodeFixed32(meta_value_buf, filtered_fvs.size());
    HashesMetaValue hashes_meta_value(DataType::kPKHashes, Slice(meta_value_buf, 4));
    version = hashes_meta_value.UpdateVersion();
    batch.Put(handles_[kMetaCF], base_meta_key.Encode(), hashes_meta_value.Encode());
    for (const auto& fv : filtered_fvs) {
      HashesDataKey hashes_data_key(key, version, fv.field);
      PKHashDataValue ehashes_value(fv.value);
      if (fv.ttl > 0) {
        ehashes_value.SetRelativeTimestamp(fv.ttl);
      }
      batch.Put(handles_[kPKHashDataCF], hashes_data_key.Encode(), ehashes_value.Encode());
    }
  }
  s = db_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(DataType::kPKHashes, key.ToString(), statistic);
  return s;
}

Status Redis::PKHMGet(const Slice& key, const std::vector<std::string>& fields, std::vector<ValueStatus>* vss) {
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
  if (s.ok() && !ExpectedMetaValue(DataType::kPKHashes, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument("WRONGTYPE, key: " + key.ToString() +
                                     ", expect type: " + DataTypeStrings[static_cast<int>(DataType::kPKHashes)] +
                                     ", get type: " + DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
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
        s = db_->Get(read_options, handles_[kPKHashDataCF], hashes_data_key.Encode(), &value);
        if (s.ok()) {
          ParsedPKHashDataValue parsed_internal_value(&value);
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

Status Redis::PKHKeys(const Slice& key, std::vector<std::string>* fields) {
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  uint64_t version = 0;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(read_options, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok() && !ExpectedMetaValue(DataType::kPKHashes, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument("WRONGTYPE, key: " + key.ToString() +
                                     ", expect type: " + DataTypeStrings[static_cast<int>(DataType::kPKHashes)] +
                                     ", get type: " + DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
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
      KeyStatisticsDurationGuard guard(this, DataType::kPKHashes, key.ToString());
      auto iter = db_->NewIterator(read_options, handles_[kPKHashDataCF]);
      for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key());
        fields->push_back(parsed_hashes_data_key.field().ToString());
      }
      delete iter;
    }
  }
  return s;
}

Status Redis::PKHVals(const Slice& key, std::vector<std::string>* values) {
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  uint64_t version = 0;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(read_options, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok() && !ExpectedMetaValue(DataType::kPKHashes, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument("WRONGTYPE, key: " + key.ToString() +
                                     ", expect type: " + DataTypeStrings[static_cast<int>(DataType::kPKHashes)] +
                                     ", get type: " + DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
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
      KeyStatisticsDurationGuard guard(this, DataType::kPKHashes, key.ToString());
      auto iter = db_->NewIterator(read_options, handles_[kPKHashDataCF]);
      for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
        ParsedPKHashDataValue parsed_internal_value(iter->value());
        values->push_back(parsed_internal_value.UserValue().ToString());
      }
      delete iter;
    }
  }
  return s;
}

Status Redis::PKHGetall(const Slice& key, std::vector<FieldValueTTL>* fvts) {
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  uint64_t version = 0;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(read_options, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok() && !ExpectedMetaValue(DataType::kPKHashes, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument("WRONGTYPE, key: " + key.ToString() +
                                     ", expect type: " + DataTypeStrings[static_cast<int>(DataType::kPKHashes)] +
                                     ", get type: " + DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
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
      KeyStatisticsDurationGuard guard(this, DataType::kPKHashes, key.ToString());
      auto iter = db_->NewIterator(read_options, handles_[kPKHashDataCF]);
      for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key());
        ParsedPKHashDataValue parsed_internal_value(iter->value());

        if (!parsed_internal_value.IsStale()) {
          int64_t ttl = 0;
          int64_t etime = parsed_internal_value.Etime();
          if (etime == 0) {
            ttl = -1;
          } else {
            int64_t curtime;
            rocksdb::Env::Default()->GetCurrentTime(&curtime);
            ttl = (etime - curtime >= 0) ? etime - curtime : -2;
          }

          fvts->push_back({parsed_hashes_data_key.field().ToString(), parsed_internal_value.UserValue().ToString(),
                           static_cast<int32_t>(ttl)});
        }
      }
      delete iter;
    }
  }
  return s;
}

Status Redis::PKHScan(const Slice& key, int64_t cursor, const std::string& pattern, int64_t count,
                      std::vector<FieldValueTTL>* fvts, int64_t* next_cursor) {
  *next_cursor = 0;
  fvts->clear();
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
  if (s.ok() && !ExpectedMetaValue(DataType::kPKHashes, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument("WRONGTYPE, key: " + key.ToString() +
                                     ", expect type: " + DataTypeStrings[static_cast<int>(DataType::kPKHashes)] +
                                     ", get type: " + DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
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
      s = GetScanStartPoint(DataType::kPKHashes, key, pattern, cursor, &start_point);
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
      KeyStatisticsDurationGuard guard(this, DataType::kPKHashes, key.ToString());
      rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[kPKHashDataCF]);
      for (iter->Seek(hashes_start_data_key.Encode()); iter->Valid() && rest > 0 && iter->key().starts_with(prefix);
           iter->Next()) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key());
        std::string field = parsed_hashes_data_key.field().ToString();
        if (StringMatch(pattern.data(), pattern.size(), field.data(), field.size(), 0) != 0) {
          ParsedPKHashDataValue parsed_internal_value(iter->value());

          if (!parsed_internal_value.IsStale()) {
            int64_t ttl;
            int64_t timestamp = parsed_internal_value.Etime();
            if (timestamp == 0) {
              ttl = -1;
            } else {
              int64_t curtime;
              rocksdb::Env::Default()->GetCurrentTime(&curtime);
              ttl = (timestamp - curtime >= 0) ? timestamp - curtime : -2;
            }
            fvts->push_back({field, parsed_internal_value.UserValue().ToString(), static_cast<int32_t>(ttl)});
          }
        }
        rest--;
      }

      if (iter->Valid() && (iter->key().compare(prefix) <= 0 || iter->key().starts_with(prefix))) {
        *next_cursor = cursor + step_length;
        ParsedHashesDataKey parsed_hashes_data_key(iter->key());
        std::string next_field = parsed_hashes_data_key.field().ToString();
        StoreScanNextPoint(DataType::kPKHashes, key, pattern, *next_cursor, next_field);
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

Status Redis::PKHashesExpire(const Slice& key, int64_t ttl, std::string&& prefetch_meta) {
  std::string meta_value(std::move(prefetch_meta));
  ScopeRecordLock l(lock_mgr_, key);
  BaseMetaKey base_meta_key(key);
  Status s;

  // meta_value is empty means no meta value get before,
  // we should get meta first
  if (meta_value.empty()) {
    s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
    if (s.ok() && !ExpectedMetaValue(DataType::kPKHashes, meta_value)) {
      if (ExpectedStale(meta_value)) {
        s = Status::NotFound();
      } else {
        return Status::InvalidArgument("WRONGTYPE, key: " + key.ToString() + ", expect type: " +
                                       DataTypeStrings[static_cast<int>(DataType::kPKHashes)] + ", get type: " +
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

}  //  namespace storage
