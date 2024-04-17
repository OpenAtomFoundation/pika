//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <memory>

#include <fmt/core.h>
#include <glog/logging.h>

#include "pstd/include/pika_codis_slot.h"
#include "src/base_data_value_format.h"
#include "src/lists_filter.h"
#include "src/redis.h"
#include "src/scope_record_lock.h"
#include "src/scope_snapshot.h"
#include "storage/util.h"
#include "src/debug.h"

namespace storage {
Status Redis::ScanListsKeyNum(KeyInfo* key_info) {
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
    auto type = static_cast<enum Type>(static_cast<uint8_t>(iter->value()[0]));
    if (type != Type::kList) {
      continue;
    }
    ParsedListsMetaValue parsed_lists_meta_value(iter->value());
    if (parsed_lists_meta_value.IsStale() || parsed_lists_meta_value.Count() == 0) {
      invaild_keys++;
    } else {
      keys++;
      if (!parsed_lists_meta_value.IsPermanentSurvival()) {
        expires++;
        ttl_sum += parsed_lists_meta_value.Etime() - curtime;
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

Status Redis::ListsPKPatternMatchDel(const std::string& pattern, int32_t* ret) {
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
  rocksdb::Iterator* iter = db_->NewIterator(iterator_options, handles_[kMetaCF]);
  iter->SeekToFirst();
  while (iter->Valid()) {
    auto type = static_cast<enum Type>(static_cast<uint8_t>(meta_value[0]));
    if (type != Type::kList) {
      continue;
    }
    ParsedBaseMetaKey parsed_meta_key(iter->key().ToString());
    meta_value = iter->value().ToString();
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    if (!parsed_lists_meta_value.IsStale() && (parsed_lists_meta_value.Count() != 0U) &&
        (StringMatch(pattern.data(), pattern.size(), parsed_meta_key.Key().data(), parsed_meta_key.Key().size(), 0) != 0)) {
      parsed_lists_meta_value.InitialMetaValue();
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

Status Redis::LIndex(const Slice& key, int64_t index, std::string* element) {
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  std::string meta_value;

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(read_options, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    auto type = static_cast<enum Type>(static_cast<uint8_t>(meta_value[0]));
    if (type != Type::kList) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    uint64_t version = parsed_lists_meta_value.Version();
    if (parsed_lists_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.Count() == 0) {
      return Status::NotFound();
    } else {
      uint64_t target_index =
          index >= 0 ? parsed_lists_meta_value.LeftIndex() + index + 1 : parsed_lists_meta_value.RightIndex() + index;
      if (parsed_lists_meta_value.LeftIndex() < target_index && target_index < parsed_lists_meta_value.RightIndex()) {
        ListsDataKey lists_data_key(key, version, target_index);
        s = db_->Get(read_options, handles_[kListsDataCF], lists_data_key.Encode(), element);
        if (s.ok()) {
          ParsedBaseDataValue parsed_value(element);
          parsed_value.StripSuffix();
        }
      } else {
        return Status::NotFound();
      }
    }
  }
  return s;
}

Status Redis::LInsert(const Slice& key, const BeforeOrAfter& before_or_after, const std::string& pivot,
                      const std::string& value, int64_t* ret) {
  *ret = 0;
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  std::string meta_value;

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    auto type = static_cast<enum Type>(static_cast<uint8_t>(meta_value[0]));
    if (type != Type::kList) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.Count() == 0) {
      return Status::NotFound();
    } else {
      bool find_pivot = false;
      uint64_t pivot_index = 0;
      uint64_t version = parsed_lists_meta_value.Version();
      uint64_t current_index = parsed_lists_meta_value.LeftIndex() + 1;
      rocksdb::Iterator* iter = db_->NewIterator(default_read_options_, handles_[kListsDataCF]);
      ListsDataKey start_data_key(key, version, current_index);
      for (iter->Seek(start_data_key.Encode()); iter->Valid() && current_index < parsed_lists_meta_value.RightIndex();
           iter->Next(), current_index++) {
        ParsedBaseDataValue parsed_value(iter->value());
        if (pivot.compare(parsed_value.UserValue().ToString()) == 0) {
          find_pivot = true;
          pivot_index = current_index;
          break;
        }
      }
      delete iter;
      if (!find_pivot) {
        *ret = -1;
        return Status::NotFound();
      } else {
        uint64_t target_index;
        std::vector<std::string> list_nodes;
        uint64_t mid_index = parsed_lists_meta_value.LeftIndex() +
                             (parsed_lists_meta_value.RightIndex() - parsed_lists_meta_value.LeftIndex()) / 2;
        if (pivot_index <= mid_index) {
          target_index = (before_or_after == Before) ? pivot_index - 1 : pivot_index;
          current_index = parsed_lists_meta_value.LeftIndex() + 1;
          rocksdb::Iterator* first_half_iter = db_->NewIterator(default_read_options_, handles_[kListsDataCF]);
          ListsDataKey start_data_key(key, version, current_index);
          for (first_half_iter->Seek(start_data_key.Encode()); first_half_iter->Valid() && current_index <= pivot_index;
               first_half_iter->Next(), current_index++) {
            ParsedBaseDataValue parsed_value(first_half_iter->value());
            if (current_index == pivot_index) {
              if (before_or_after == After) {
                list_nodes.push_back(parsed_value.UserValue().ToString());
              }
              break;
            }
            list_nodes.push_back(parsed_value.UserValue().ToString());
          }
          delete first_half_iter;

          current_index = parsed_lists_meta_value.LeftIndex();
          for (const auto& node : list_nodes) {
            ListsDataKey lists_data_key(key, version, current_index++);
            BaseDataValue i_val(node);
            batch.Put(handles_[kListsDataCF], lists_data_key.Encode(), i_val.Encode());
          }
          parsed_lists_meta_value.ModifyLeftIndex(1);
        } else {
          target_index = (before_or_after == Before) ? pivot_index : pivot_index + 1;
          current_index = pivot_index;
          rocksdb::Iterator* after_half_iter = db_->NewIterator(default_read_options_, handles_[kListsDataCF]);
          ListsDataKey start_data_key(key, version, current_index);
          for (after_half_iter->Seek(start_data_key.Encode());
               after_half_iter->Valid() && current_index < parsed_lists_meta_value.RightIndex();
               after_half_iter->Next(), current_index++) {
            if (current_index == pivot_index && before_or_after == BeforeOrAfter::After) {
              continue;
            }
            ParsedBaseDataValue parsed_value(after_half_iter->value());
            list_nodes.push_back(parsed_value.UserValue().ToString());
          }
          delete after_half_iter;

          current_index = target_index + 1;
          for (const auto& node : list_nodes) {
            ListsDataKey lists_data_key(key, version, current_index++);
            BaseDataValue i_val(node);
            batch.Put(handles_[kListsDataCF], lists_data_key.Encode(), i_val.Encode());
          }
          parsed_lists_meta_value.ModifyRightIndex(1);
        }
        parsed_lists_meta_value.ModifyCount(1);
        batch.Put(handles_[kMetaCF], base_meta_key.Encode(), meta_value);
        ListsDataKey lists_target_key(key, version, target_index);
        BaseDataValue i_val(value);
        batch.Put(handles_[kListsDataCF], lists_target_key.Encode(), i_val.Encode());
        *ret = static_cast<int32_t>(parsed_lists_meta_value.Count());
        return db_->Write(default_write_options_, &batch);
      }
    }
  } else if (s.IsNotFound()) {
    *ret = 0;
  }
  return s;
}

Status Redis::LLen(const Slice& key, uint64_t* len) {
  *len = 0;
  std::string meta_value;

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    auto type = static_cast<enum Type>(static_cast<uint8_t>(meta_value[0]));
    if (type != Type::kList) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.Count() == 0) {
      return Status::NotFound();
    } else {
      *len = parsed_lists_meta_value.Count();
      return s;
    }
  }
  return s;
}

Status Redis::LPop(const Slice& key, int64_t count, std::vector<std::string>* elements) {
  uint32_t statistic = 0;
  elements->clear();

  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  std::string meta_value;

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    auto type = static_cast<enum Type>(static_cast<uint8_t>(meta_value[0]));
    if (type != Type::kList) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.Count() == 0) {
      return Status::NotFound();
    } else {
      auto size = static_cast<int64_t>(parsed_lists_meta_value.Count());
      uint64_t version = parsed_lists_meta_value.Version();
      int32_t start_index = 0;
      auto stop_index = static_cast<int32_t>(count<=size?count-1:size-1);
      int32_t cur_index = 0;
      ListsDataKey lists_data_key(key, version, parsed_lists_meta_value.LeftIndex()+1);
      rocksdb::Iterator* iter = db_->NewIterator(default_read_options_, handles_[kListsDataCF]);
      for (iter->Seek(lists_data_key.Encode()); iter->Valid() && cur_index <= stop_index; iter->Next(), ++cur_index) {
        statistic++;
        ParsedBaseDataValue parsed_base_data_value(iter->value());
        elements->push_back(parsed_base_data_value.UserValue().ToString());
        batch.Delete(handles_[kListsDataCF],iter->key());

        parsed_lists_meta_value.ModifyCount(-1);
        parsed_lists_meta_value.ModifyLeftIndex(-1);
      }
      batch.Put(handles_[kMetaCF], base_meta_key.Encode(), meta_value);
      delete iter;
    }
  }
  if (batch.Count() != 0U) {
    s = db_->Write(default_write_options_, &batch);
    if (s.ok()) {
      batch.Clear();
    }
    UpdateSpecificKeyStatistics(DataType::kLists, key.ToString(), statistic);
  }
  return s;
}

Status Redis::LPush(const Slice& key, const std::vector<std::string>& values, uint64_t* ret) {
  *ret = 0;
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  uint64_t index = 0;
  uint64_t version = 0;
  std::string meta_value;

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    auto type = static_cast<enum Type>(static_cast<uint8_t>(meta_value[0]));
    if (type != Type::kList) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    if (parsed_lists_meta_value.IsStale() || parsed_lists_meta_value.Count() == 0) {
      version = parsed_lists_meta_value.InitialMetaValue();
    } else {
      version = parsed_lists_meta_value.Version();
    }
    for (const auto& value : values) {
      index = parsed_lists_meta_value.LeftIndex();
      parsed_lists_meta_value.ModifyLeftIndex(1);
      parsed_lists_meta_value.ModifyCount(1);
      ListsDataKey lists_data_key(key, version, index);
      BaseDataValue i_val(value);
      batch.Put(handles_[kListsDataCF], lists_data_key.Encode(), i_val.Encode());
    }
    batch.Put(handles_[kMetaCF], base_meta_key.Encode(), meta_value);
    *ret = parsed_lists_meta_value.Count();
  } else if (s.IsNotFound()) {
    char str[8];
    EncodeFixed64(str, values.size());
    ListsMetaValue lists_meta_value(Slice(str, sizeof(uint64_t)));
    version = lists_meta_value.UpdateVersion();
    for (const auto& value : values) {
      index = lists_meta_value.LeftIndex();
      lists_meta_value.ModifyLeftIndex(1);
      ListsDataKey lists_data_key(key, version, index);
      BaseDataValue i_val(value);
      batch.Put(handles_[kListsDataCF], lists_data_key.Encode(), i_val.Encode());
    }
    batch.Put(handles_[kMetaCF], base_meta_key.Encode(), lists_meta_value.Encode());
    *ret = lists_meta_value.RightIndex() - lists_meta_value.LeftIndex() - 1;
  } else {
    return s;
  }
  return db_->Write(default_write_options_, &batch);
}

Status Redis::LPushx(const Slice& key, const std::vector<std::string>& values, uint64_t* len) {
  *len = 0;
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  std::string meta_value;

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    auto type = static_cast<enum Type>(static_cast<uint8_t>(meta_value[0]));
    if (type != Type::kList) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.Count() == 0) {
      return Status::NotFound();
    } else {
      uint64_t version = parsed_lists_meta_value.Version();
      for (const auto& value : values) {
        uint64_t index = parsed_lists_meta_value.LeftIndex();
        parsed_lists_meta_value.ModifyCount(1);
        parsed_lists_meta_value.ModifyLeftIndex(1);
        ListsDataKey lists_data_key(key, version, index);
        BaseDataValue i_val(value);
        batch.Put(handles_[kListsDataCF], lists_data_key.Encode(), i_val.Encode());
      }
      batch.Put(handles_[kMetaCF], base_meta_key.Encode(), meta_value);
      *len = parsed_lists_meta_value.Count();
      return db_->Write(default_write_options_, &batch);
    }
  }
  return s;
}

Status Redis::LRange(const Slice& key, int64_t start, int64_t stop, std::vector<std::string>* ret) {
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  std::string meta_value;
  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(read_options, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    auto type = static_cast<enum Type>(static_cast<uint8_t>(meta_value[0]));
    if (type != Type::kList) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.Count() == 0) {
      return Status::NotFound();
    } else {
      uint64_t version = parsed_lists_meta_value.Version();
      uint64_t origin_left_index = parsed_lists_meta_value.LeftIndex() + 1;
      uint64_t origin_right_index = parsed_lists_meta_value.RightIndex() - 1;
      uint64_t sublist_left_index = start >= 0 ? origin_left_index + start : origin_right_index + start + 1;
      uint64_t sublist_right_index = stop >= 0 ? origin_left_index + stop : origin_right_index + stop + 1;

      if (sublist_left_index > sublist_right_index || sublist_left_index > origin_right_index ||
          sublist_right_index < origin_left_index) {
        return Status::OK();
      } else {
        if (sublist_left_index < origin_left_index) {
          sublist_left_index = origin_left_index;
        }
        if (sublist_right_index > origin_right_index) {
          sublist_right_index = origin_right_index;
        }
        rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[kListsDataCF]);
        uint64_t current_index = sublist_left_index;
        ListsDataKey start_data_key(key, version, current_index);
        for (iter->Seek(start_data_key.Encode()); iter->Valid() && current_index <= sublist_right_index;
             iter->Next(), current_index++) {
          ParsedBaseDataValue parsed_value(iter->value());
          ret->push_back(parsed_value.UserValue().ToString());
        }
        delete iter;
        return Status::OK();
      }
    }
  } else {
    return s;
  }
}

Status Redis::LRangeWithTTL(const Slice& key, int64_t start, int64_t stop, std::vector<std::string>* ret, int64_t* ttl) {
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  std::string meta_value;
  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(read_options, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    auto type = static_cast<enum Type>(static_cast<uint8_t>(meta_value[0]));
    if (type != Type::kList) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    if (parsed_lists_meta_value.Count() == 0) {
      return Status::NotFound();
    } else if (parsed_lists_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      // ttl
      *ttl = parsed_lists_meta_value.Etime();
      if (*ttl == 0) {
        *ttl = -1;
      } else {
        int64_t curtime;
        rocksdb::Env::Default()->GetCurrentTime(&curtime);
        *ttl = *ttl - curtime >= 0 ? *ttl - curtime : -2;
      }

      uint64_t version = parsed_lists_meta_value.Version();
      uint64_t origin_left_index = parsed_lists_meta_value.LeftIndex() + 1;
      uint64_t origin_right_index = parsed_lists_meta_value.RightIndex() - 1;
      uint64_t sublist_left_index = start >= 0 ? origin_left_index + start : origin_right_index + start + 1;
      uint64_t sublist_right_index = stop >= 0 ? origin_left_index + stop : origin_right_index + stop + 1;

      if (sublist_left_index > sublist_right_index
          || sublist_left_index > origin_right_index
          || sublist_right_index < origin_left_index) {
        return Status::OK();
      } else {
        if (sublist_left_index < origin_left_index) {
          sublist_left_index = origin_left_index;
        }
        if (sublist_right_index > origin_right_index) {
          sublist_right_index = origin_right_index;
        }
        rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[kListsDataCF]);
        uint64_t current_index = sublist_left_index;
        ListsDataKey start_data_key(key, version, current_index);
        for (iter->Seek(start_data_key.Encode());
             iter->Valid() && current_index <= sublist_right_index;
             iter->Next(), current_index++) {
          ParsedBaseDataValue parsed_value(iter->value());
          ret->push_back(parsed_value.UserValue().ToString());
        }
        delete iter;
        return Status::OK();
      }
    }
  } else {
    return s;
  }
}

Status Redis::LRem(const Slice& key, int64_t count, const Slice& value, uint64_t* ret) {
  *ret = 0;
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  std::string meta_value;

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    auto type = static_cast<enum Type>(static_cast<uint8_t>(meta_value[0]));
    if (type != Type::kList) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.Count() == 0) {
      return Status::NotFound();
    } else {
      uint64_t current_index;
      std::vector<uint64_t> target_index;
      std::vector<uint64_t> delete_index;
      uint64_t rest = (count < 0) ? -count : count;
      uint64_t version = parsed_lists_meta_value.Version();
      uint64_t start_index = parsed_lists_meta_value.LeftIndex() + 1;
      uint64_t stop_index = parsed_lists_meta_value.RightIndex() - 1;
      ListsDataKey start_data_key(key, version, start_index);
      ListsDataKey stop_data_key(key, version, stop_index);
      if (count >= 0) {
        current_index = start_index;
        rocksdb::Iterator* iter = db_->NewIterator(default_read_options_, handles_[kListsDataCF]);
        for (iter->Seek(start_data_key.Encode());
             iter->Valid() && current_index <= stop_index && ((count == 0) || rest != 0);
             iter->Next(), current_index++) {
          ParsedBaseDataValue parsed_value(iter->value());
          if (value.compare(parsed_value.UserValue()) == 0) {
            target_index.push_back(current_index);
            if (count != 0) {
              rest--;
            }
          }
        }
        delete iter;
      } else {
        current_index = stop_index;
        rocksdb::Iterator* iter = db_->NewIterator(default_read_options_, handles_[kListsDataCF]);
        for (iter->Seek(stop_data_key.Encode());
             iter->Valid() && current_index >= start_index && ((count == 0) || rest != 0);
             iter->Prev(), current_index--) {
          ParsedBaseDataValue parsed_value(iter->value());
          if (value.compare(parsed_value.UserValue()) == 0) {
            target_index.push_back(current_index);
            if (count != 0) {
              rest--;
            }
          }
        }
        delete iter;
      }
      if (target_index.empty()) {
        *ret = 0;
        return Status::NotFound();
      } else {
        rest = target_index.size();
        uint64_t sublist_left_index = (count >= 0) ? target_index[0] : target_index[target_index.size() - 1];
        uint64_t sublist_right_index = (count >= 0) ? target_index[target_index.size() - 1] : target_index[0];
        uint64_t left_part_len = sublist_right_index - start_index;
        uint64_t right_part_len = stop_index - sublist_left_index;
        if (left_part_len <= right_part_len) {
          uint64_t left = sublist_right_index;
          current_index = sublist_right_index;
          ListsDataKey sublist_right_key(key, version, sublist_right_index);
          rocksdb::Iterator* iter = db_->NewIterator(default_read_options_, handles_[kListsDataCF]);
          for (iter->Seek(sublist_right_key.Encode()); iter->Valid() && current_index >= start_index;
               iter->Prev(), current_index--) {
            ParsedBaseDataValue parsed_value(iter->value());
            if (value.compare(parsed_value.UserValue()) == 0 && rest > 0) {
              rest--;
            } else {
              ListsDataKey lists_data_key(key, version, left--);
              batch.Put(handles_[kListsDataCF], lists_data_key.Encode(), iter->value());
            }
          }
          delete iter;
          uint64_t left_index = parsed_lists_meta_value.LeftIndex();
          for (uint64_t idx = 0; idx < target_index.size(); ++idx) {
            delete_index.push_back(left_index + idx + 1);
          }
          parsed_lists_meta_value.ModifyLeftIndex(-target_index.size());
        } else {
          uint64_t right = sublist_left_index;
          current_index = sublist_left_index;
          ListsDataKey sublist_left_key(key, version, sublist_left_index);
          rocksdb::Iterator* iter = db_->NewIterator(default_read_options_, handles_[kListsDataCF]);
          for (iter->Seek(sublist_left_key.Encode()); iter->Valid() && current_index <= stop_index;
               iter->Next(), current_index++) {
            ParsedBaseDataValue parsed_value(iter->value());
            if ((value.compare(parsed_value.UserValue()) == 0) && rest > 0) {
              rest--;
            } else {
              ListsDataKey lists_data_key(key, version, right++);
              batch.Put(handles_[kListsDataCF], lists_data_key.Encode(), iter->value());
            }
          }
          delete iter;
          uint64_t right_index = parsed_lists_meta_value.RightIndex();
          for (uint64_t idx = 0; idx < target_index.size(); ++idx) {
            delete_index.push_back(right_index - idx - 1);
          }
          parsed_lists_meta_value.ModifyRightIndex(-target_index.size());
        }
        parsed_lists_meta_value.ModifyCount(-target_index.size());
        batch.Put(handles_[kMetaCF], base_meta_key.Encode(), meta_value);
        for (const auto& idx : delete_index) {
          ListsDataKey lists_data_key(key, version, idx);
          batch.Delete(handles_[kListsDataCF], lists_data_key.Encode());
        }
        *ret = target_index.size();
        return db_->Write(default_write_options_, &batch);
      }
    }
  } else if (s.IsNotFound()) {
    *ret = 0;
  }
  return s;
}

Status Redis::LSet(const Slice& key, int64_t index, const Slice& value) {
  uint32_t statistic = 0;
  ScopeRecordLock l(lock_mgr_, key);
  std::string meta_value;

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    auto type = static_cast<enum Type>(static_cast<uint8_t>(meta_value[0]));
    if (type != Type::kList) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.Count() == 0) {
      return Status::NotFound();
    } else {
      uint64_t version = parsed_lists_meta_value.Version();
      uint64_t target_index =
          index >= 0 ? parsed_lists_meta_value.LeftIndex() + index + 1 : parsed_lists_meta_value.RightIndex() + index;
      if (target_index <= parsed_lists_meta_value.LeftIndex() ||
          target_index >= parsed_lists_meta_value.RightIndex()) {
        return Status::Corruption("index out of range");
      }
      ListsDataKey lists_data_key(key, version, target_index);
      BaseDataValue i_val(value);
      s = db_->Put(default_write_options_, handles_[kListsDataCF], lists_data_key.Encode(), i_val.Encode());
      statistic++;
      UpdateSpecificKeyStatistics(DataType::kLists, key.ToString(), statistic);
      return s;
    }
  }
  return s;
}

Status Redis::LTrim(const Slice& key, int64_t start, int64_t stop) {
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  uint32_t statistic = 0;
  std::string meta_value;

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    auto type = static_cast<enum Type>(static_cast<uint8_t>(meta_value[0]));
    if (type != Type::kList) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    uint64_t version = parsed_lists_meta_value.Version();
    if (parsed_lists_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.Count() == 0) {
      return Status::NotFound();
    } else {
      uint64_t origin_left_index = parsed_lists_meta_value.LeftIndex() + 1;
      uint64_t origin_right_index = parsed_lists_meta_value.RightIndex() - 1;
      uint64_t sublist_left_index = start >= 0 ? origin_left_index + start : origin_right_index + start + 1;
      uint64_t sublist_right_index = stop >= 0 ? origin_left_index + stop : origin_right_index + stop + 1;

      if (sublist_left_index > sublist_right_index || sublist_left_index > origin_right_index ||
          sublist_right_index < origin_left_index) {
        parsed_lists_meta_value.InitialMetaValue();
        batch.Put(handles_[kMetaCF], base_meta_key.Encode(), meta_value);
      } else {
        if (sublist_left_index < origin_left_index) {
          sublist_left_index = origin_left_index;
        }

        if (sublist_right_index > origin_right_index) {
          sublist_right_index = origin_right_index;
        }

        uint64_t delete_node_num =
            (sublist_left_index - origin_left_index) + (origin_right_index - sublist_right_index);
        parsed_lists_meta_value.ModifyLeftIndex(-(sublist_left_index - origin_left_index));
        parsed_lists_meta_value.ModifyRightIndex(-(origin_right_index - sublist_right_index));
        parsed_lists_meta_value.ModifyCount(-delete_node_num);
        batch.Put(handles_[kMetaCF], base_meta_key.Encode(), meta_value);
        for (uint64_t idx = origin_left_index; idx < sublist_left_index; ++idx) {
          statistic++;
          ListsDataKey lists_data_key(key, version, idx);
          batch.Delete(handles_[kListsDataCF], lists_data_key.Encode());
        }
        for (uint64_t idx = origin_right_index; idx > sublist_right_index; --idx) {
          statistic++;
          ListsDataKey lists_data_key(key, version, idx);
          batch.Delete(handles_[kListsDataCF], lists_data_key.Encode());
        }
      }
    }
  } else {
    return s;
  }
  s = db_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(DataType::kLists, key.ToString(), statistic);
  return s;
}

Status Redis::RPop(const Slice& key, int64_t count, std::vector<std::string>* elements) {
  uint32_t statistic = 0;
  elements->clear();

  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  std::string meta_value;

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    auto type = static_cast<enum Type>(static_cast<uint8_t>(meta_value[0]));
    if (type != Type::kList) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.Count() == 0) {
      return Status::NotFound();
    } else {
      auto size = static_cast<int64_t>(parsed_lists_meta_value.Count());
      uint64_t version = parsed_lists_meta_value.Version();
      int32_t start_index = 0;
      auto stop_index = static_cast<int32_t>(count<=size?count-1:size-1);
      int32_t cur_index = 0;
      ListsDataKey lists_data_key(key, version, parsed_lists_meta_value.RightIndex()-1);
      rocksdb::Iterator* iter = db_->NewIterator(default_read_options_, handles_[kListsDataCF]);
      for (iter->SeekForPrev(lists_data_key.Encode()); iter->Valid() && cur_index <= stop_index; iter->Prev(), ++cur_index) {
        statistic++;
        ParsedBaseDataValue parsed_value(iter->value());
        elements->push_back(parsed_value.UserValue().ToString());
        batch.Delete(handles_[kListsDataCF],iter->key());

        parsed_lists_meta_value.ModifyCount(-1);
        parsed_lists_meta_value.ModifyRightIndex(-1);
      }
      batch.Put(handles_[kMetaCF], base_meta_key.Encode(), meta_value);
      delete iter;
    }
  }
  if (batch.Count() != 0U) {
    s = db_->Write(default_write_options_, &batch);
    if (s.ok()) {
      batch.Clear();
    }
    UpdateSpecificKeyStatistics(DataType::kLists, key.ToString(), statistic);
  }
  return s;
}

Status Redis::RPoplpush(const Slice& source, const Slice& destination, std::string* element) {
  element->clear();
  uint32_t statistic = 0;
  Status s;
  rocksdb::WriteBatch batch;
  MultiScopeRecordLock l(lock_mgr_, {source.ToString(), destination.ToString()});
  if (source.compare(destination) == 0) {
    std::string meta_value;
    BaseMetaKey base_source(source);
    s = db_->Get(default_read_options_, handles_[kMetaCF], base_source.Encode(), &meta_value);
    if (s.ok()) {
      auto type = static_cast<enum Type>(static_cast<uint8_t>(meta_value[0]));
      if (type != Type::kList) {
        return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
      }
      ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
      if (parsed_lists_meta_value.IsStale()) {
        return Status::NotFound("Stale");
      } else if (parsed_lists_meta_value.Count() == 0) {
        return Status::NotFound();
      } else {
        std::string target;
        uint64_t version = parsed_lists_meta_value.Version();
        uint64_t last_node_index = parsed_lists_meta_value.RightIndex() - 1;
        ListsDataKey lists_data_key(source, version, last_node_index);
        s = db_->Get(default_read_options_, handles_[kListsDataCF], lists_data_key.Encode(), &target);
        if (s.ok()) {
          *element = target;
          ParsedBaseDataValue parsed_value(element);
          parsed_value.StripSuffix();
          if (parsed_lists_meta_value.Count() == 1) {
            return Status::OK();
          } else {
            uint64_t target_index = parsed_lists_meta_value.LeftIndex();
            ListsDataKey lists_target_key(source, version, target_index);
            batch.Delete(handles_[kListsDataCF], lists_data_key.Encode());
            batch.Put(handles_[kListsDataCF], lists_target_key.Encode(), target);
            statistic++;
            parsed_lists_meta_value.ModifyRightIndex(-1);
            parsed_lists_meta_value.ModifyLeftIndex(1);
            batch.Put(handles_[kMetaCF], base_source.Encode(), meta_value);
            s = db_->Write(default_write_options_, &batch);
            UpdateSpecificKeyStatistics(DataType::kLists, source.ToString(), statistic);
            return s;
          }
        } else {
          return s;
        }
      }
    } else {
      return s;
    }
  }

  uint64_t version;
  std::string target;
  std::string source_meta_value;
  BaseMetaKey base_source(source);
  s = db_->Get(default_read_options_, handles_[kMetaCF], base_source.Encode(), &source_meta_value);
  if (s.ok()) {
    auto type = static_cast<enum Type>(static_cast<uint8_t>(source_meta_value[0]));
    if (type != Type::kList) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedListsMetaValue parsed_lists_meta_value(&source_meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.Count() == 0) {
      return Status::NotFound();
    } else {
      version = parsed_lists_meta_value.Version();
      uint64_t last_node_index = parsed_lists_meta_value.RightIndex() - 1;
      ListsDataKey lists_data_key(source, version, last_node_index);
      s = db_->Get(default_read_options_, handles_[kListsDataCF], lists_data_key.Encode(), &target);
      if (s.ok()) {
        batch.Delete(handles_[kListsDataCF], lists_data_key.Encode());
        statistic++;
        parsed_lists_meta_value.ModifyCount(-1);
        parsed_lists_meta_value.ModifyRightIndex(-1);
        batch.Put(handles_[kMetaCF], base_source.Encode(), source_meta_value);
      } else {
        return s;
      }
    }
  } else {
    return s;
  }

  std::string destination_meta_value;
  BaseMetaKey base_destination(destination);
  s = db_->Get(default_read_options_, handles_[kMetaCF], base_destination.Encode(), &destination_meta_value);
  if (s.ok()) {
    auto type = static_cast<enum Type>(static_cast<uint8_t>(destination_meta_value[0]));
    if (type != Type::kList) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedListsMetaValue parsed_lists_meta_value(&destination_meta_value);
    if (parsed_lists_meta_value.IsStale() || parsed_lists_meta_value.Count() == 0) {
      version = parsed_lists_meta_value.InitialMetaValue();
    } else {
      version = parsed_lists_meta_value.Version();
    }
    uint64_t target_index = parsed_lists_meta_value.LeftIndex();
    ListsDataKey lists_data_key(destination, version, target_index);
    batch.Put(handles_[kListsDataCF], lists_data_key.Encode(), target);
    parsed_lists_meta_value.ModifyCount(1);
    parsed_lists_meta_value.ModifyLeftIndex(1);
    batch.Put(handles_[kMetaCF], base_destination.Encode(), destination_meta_value);
  } else if (s.IsNotFound()) {
    char str[8];
    EncodeFixed64(str, 1);
    ListsMetaValue lists_meta_value(Slice(str, sizeof(uint64_t)));
    version = lists_meta_value.UpdateVersion();
    uint64_t target_index = lists_meta_value.LeftIndex();
    ListsDataKey lists_data_key(destination, version, target_index);
    batch.Put(handles_[kListsDataCF], lists_data_key.Encode(), target);
    lists_meta_value.ModifyLeftIndex(1);
    batch.Put(handles_[kMetaCF], base_destination.Encode(), lists_meta_value.Encode());
  } else {
    return s;
  }

  s = db_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(DataType::kLists, source.ToString(), statistic);
  if (s.ok()) {
    ParsedBaseDataValue parsed_value(&target);
    parsed_value.StripSuffix();
    *element = target;
  }
  return s;
}

Status Redis::RPush(const Slice& key, const std::vector<std::string>& values, uint64_t* ret) {
  *ret = 0;
  rocksdb::WriteBatch batch;

  uint64_t index = 0;
  uint64_t version = 0;
  std::string meta_value;

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    auto type = static_cast<enum Type>(static_cast<uint8_t>(meta_value[0]));
    if (type != Type::kList) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    if (parsed_lists_meta_value.IsStale() || parsed_lists_meta_value.Count() == 0) {
      version = parsed_lists_meta_value.InitialMetaValue();
    } else {
      version = parsed_lists_meta_value.Version();
    }
    for (const auto& value : values) {
      index = parsed_lists_meta_value.RightIndex();
      parsed_lists_meta_value.ModifyRightIndex(1);
      parsed_lists_meta_value.ModifyCount(1);
      ListsDataKey lists_data_key(key, version, index);
      BaseDataValue i_val(value);
      batch.Put(handles_[kListsDataCF], lists_data_key.Encode(), i_val.Encode());
    }
    batch.Put(handles_[kMetaCF], base_meta_key.Encode(), meta_value);
    *ret = parsed_lists_meta_value.Count();
  } else if (s.IsNotFound()) {
    char str[8];
    EncodeFixed64(str, values.size());
    ListsMetaValue lists_meta_value(Slice(str, sizeof(uint64_t)));
    version = lists_meta_value.UpdateVersion();
    for (const auto& value : values) {
      index = lists_meta_value.RightIndex();
      lists_meta_value.ModifyRightIndex(1);
      ListsDataKey lists_data_key(key, version, index);
      BaseDataValue i_val(value);
      batch.Put(handles_[kListsDataCF], lists_data_key.Encode(), i_val.Encode());
    }
    batch.Put(handles_[kMetaCF], base_meta_key.Encode(), lists_meta_value.Encode());
    *ret = lists_meta_value.RightIndex() - lists_meta_value.LeftIndex() - 1;
  } else {
    return s;
  }
  return db_->Write(default_write_options_, &batch);
}

Status Redis::RPushx(const Slice& key, const std::vector<std::string>& values, uint64_t* len) {
  *len = 0;
  rocksdb::WriteBatch batch;

  ScopeRecordLock l(lock_mgr_, key);
  std::string meta_value;

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    auto type = static_cast<enum Type>(static_cast<uint8_t>(meta_value[0]));
    if (type != Type::kList) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.Count() == 0) {
      return Status::NotFound();
    } else {
      uint64_t version = parsed_lists_meta_value.Version();
      for (const auto& value : values) {
        uint64_t index = parsed_lists_meta_value.RightIndex();
        parsed_lists_meta_value.ModifyCount(1);
        parsed_lists_meta_value.ModifyRightIndex(1);
        ListsDataKey lists_data_key(key, version, index);
        BaseDataValue i_val(value);
        batch.Put(handles_[kListsDataCF], lists_data_key.Encode(), i_val.Encode());
      }
      batch.Put(handles_[kMetaCF], base_meta_key.Encode(), meta_value);
      *len = parsed_lists_meta_value.Count();
      return db_->Write(default_write_options_, &batch);
    }
  }
  return s;
}

Status Redis::ListsExpire(const Slice& key, int64_t ttl) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    auto type = static_cast<enum Type>(static_cast<uint8_t>(meta_value[0]));
    if (type != Type::kList) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.Count() == 0) {
      return Status::NotFound();
    }

    if (ttl > 0) {
      parsed_lists_meta_value.SetRelativeTimestamp(ttl);
      s = db_->Put(default_write_options_, handles_[kMetaCF], base_meta_key.Encode(), meta_value);
    } else {
      parsed_lists_meta_value.InitialMetaValue();
      s = db_->Put(default_write_options_, handles_[kMetaCF], base_meta_key.Encode(), meta_value);
    }
  }
  return s;
}

Status Redis::ListsDel(const Slice& key) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    auto type = static_cast<enum Type>(static_cast<uint8_t>(meta_value[0]));
    if (type != Type::kList) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.Count() == 0) {
      return Status::NotFound();
    } else {
      uint64_t statistic = parsed_lists_meta_value.Count();
      parsed_lists_meta_value.InitialMetaValue();
      s = db_->Put(default_write_options_, handles_[kMetaCF], base_meta_key.Encode(), meta_value);
      UpdateSpecificKeyStatistics(DataType::kLists, key.ToString(), statistic);
    }
  }
  return s;
}

Status Redis::ListsExpireat(const Slice& key, int64_t timestamp) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    auto type = static_cast<enum Type>(static_cast<uint8_t>(meta_value[0]));
    if (type != Type::kList) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.Count() == 0) {
      return Status::NotFound();
    } else {
      if (timestamp > 0) {
        parsed_lists_meta_value.SetEtime(static_cast<uint64_t>(timestamp));
      } else {
        parsed_lists_meta_value.InitialMetaValue();
      }
      return db_->Put(default_write_options_, handles_[kMetaCF], base_meta_key.Encode(), meta_value);
    }
  }
  return s;
}

Status Redis::ListsPersist(const Slice& key) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    auto type = static_cast<enum Type>(static_cast<uint8_t>(meta_value[0]));
    if (type != Type::kList) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.Count() == 0) {
      return Status::NotFound();
    } else {
      uint64_t timestamp = parsed_lists_meta_value.Etime();
      if (timestamp == 0) {
        return Status::NotFound("Not have an associated timeout");
      } else {
        parsed_lists_meta_value.SetEtime(0);
        return db_->Put(default_write_options_, handles_[kMetaCF], base_meta_key.Encode(), meta_value);
      }
    }
  }
  return s;
}

Status Redis::ListsTTL(const Slice& key, int64_t* timestamp) {
  std::string meta_value;

  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    auto type = static_cast<enum Type>(static_cast<uint8_t>(meta_value[0]));
    if (type != Type::kList) {
      return Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
    if (parsed_lists_meta_value.IsStale()) {
      *timestamp = -2;
      return Status::NotFound("Stale");
    } else if (parsed_lists_meta_value.Count() == 0) {
      *timestamp = -2;
      return Status::NotFound();
    } else {
      *timestamp = parsed_lists_meta_value.Etime();
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

void Redis::ScanLists() {
  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;
  auto current_time = static_cast<int32_t>(time(nullptr));

  LOG(INFO) << "*************** " << "rocksdb instance: " << index_ << " List Meta ***************";
  auto meta_iter = db_->NewIterator(iterator_options, handles_[kMetaCF]);
  for (meta_iter->SeekToFirst(); meta_iter->Valid(); meta_iter->Next()) {
    auto type = static_cast<enum Type>(static_cast<uint8_t>(meta_iter->value()[0]));
    if (type != Type::kList) {
      continue;
    }
    ParsedListsMetaValue parsed_lists_meta_value(meta_iter->value());
    ParsedBaseMetaKey parsed_meta_key(meta_iter->value());
    int32_t survival_time = 0;
    if (parsed_lists_meta_value.Etime() != 0) {
      survival_time = parsed_lists_meta_value.Etime() - current_time > 0
                          ? parsed_lists_meta_value.Etime() - current_time
                          : -1;
    }

    LOG(INFO) << fmt::format(
        "[key : {:<30}] [count : {:<10}] [left index : {:<10}] [right index : {:<10}] [timestamp : {:<10}] [version : "
        "{}] [survival_time : {}]",
        parsed_meta_key.Key().ToString(), parsed_lists_meta_value.Count(), parsed_lists_meta_value.LeftIndex(),
        parsed_lists_meta_value.RightIndex(), parsed_lists_meta_value.Etime(), parsed_lists_meta_value.Version(),
        survival_time);
  }
  delete meta_iter;

  LOG(INFO) << "*************** " << "rocksdb instance: " << index_ << " List Data***************";
  auto data_iter = db_->NewIterator(iterator_options, handles_[kListsDataCF]);
  for (data_iter->SeekToFirst(); data_iter->Valid(); data_iter->Next()) {
    ParsedListsDataKey parsed_lists_data_key(data_iter->key());
    ParsedBaseDataValue parsed_value(data_iter->value());

    LOG(INFO) << fmt::format("[key : {:<30}] [index : {:<10}] [data : {:<20}] [version : {}]",
                             parsed_lists_data_key.key().ToString(), parsed_lists_data_key.index(),
                             parsed_value.UserValue().ToString(), parsed_lists_data_key.Version());
  }
  delete data_iter;
}

}  //  namespace storage
