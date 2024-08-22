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
      batch.Put(handles_[kMetaCF], key, meta_value);
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
        *res = 0;
        if (data_value == value.ToString()) {
          return Status::OK();
        } else {
          PKHashDataValue internal_value(value);
          batch.Put(handles_[kPKHashDataCF], hashes_data_key.Encode(), internal_value.Encode());
          statistic++;
        }
      } else if (s.IsNotFound()) {
        if (!parsed_hashes_meta_value.CheckModifyCount(1)) {
          return Status::InvalidArgument("hash size overflow");
        }

        parsed_hashes_meta_value.ModifyCount(1);
        batch.Put(handles_[kMetaCF], key, meta_value);
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

}  //  namespace storage
