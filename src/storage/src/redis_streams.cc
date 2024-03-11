//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <sys/types.h>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "rocksdb/slice.h"
#include "rocksdb/status.h"

#include "src/redis.h"
#include "src/base_data_key_format.h"
#include "src/base_filter.h"
#include "src/debug.h"
#include "src/pika_stream_meta_value.h"
#include "src/scope_record_lock.h"
#include "src/scope_snapshot.h"
#include "storage/storage.h"
#include "storage/util.h"

namespace storage {

Status Redis::XAdd(const Slice& key, const std::string& serialized_message, StreamAddTrimArgs& args) {
  // With the lock, we do not need snapshot for read.
  // And it's bugy to use snapshot for read when we try to add message with trim.
  // such as: XADD key 1-0 field value MINID 1-0

  // 1 get stream meta
  rocksdb::Status s;
  StreamMetaValue stream_meta;
  s = GetStreamMeta(stream_meta, key, default_read_options_);
  if (s.IsNotFound() && args.no_mkstream) {
    return Status::NotFound("no_mkstream");
  } else if (s.IsNotFound()) {
    stream_meta.InitMetaValue();
  } else if (!s.ok()) {
    return Status::Corruption("error from XADD, get stream meta failed: " + s.ToString());
  }

  if (stream_meta.length() == 0) {
    if (args.no_mkstream) {
      return Status::NotFound("no_mkstream");
    }
    stream_meta.InitMetaValue();
  }

  if (stream_meta.last_id().ms == UINT64_MAX && stream_meta.last_id().seq == UINT64_MAX) {
    return Status::Corruption("Fatal! Sequence number overflow !");
  }

  // 2 append the message to storage
  s = GenerateStreamID(stream_meta, args);
  if (!s.ok()) {
    return s;
  }

#ifdef DEBUG
  // check the serialized current id is larger than last_id
  std::string serialized_last_id = stream_meta.last_id().Serialize();
  std::string current_id = args.id.Serialize();
  assert(current_id > serialized_last_id);
#endif

  StreamDataKey stream_data_key(key, stream_meta.version(), args.id.Serialize());
  s = db_->Put(default_write_options_, handles_[kStreamsDataCF], stream_data_key.Encode(), serialized_message);
  if (!s.ok()) {
    return Status::Corruption("error from XADD, insert stream message failed 1: " + s.ToString());
  }

  // 3 update stream meta
  if (stream_meta.length() == 0) {
    stream_meta.set_first_id(args.id);
  }
  stream_meta.set_entries_added(stream_meta.entries_added() + 1);
  stream_meta.set_last_id(args.id);
  stream_meta.set_length(stream_meta.length() + 1);

  // 4 trim the stream if needed
  if (args.trim_strategy != StreamTrimStrategy::TRIM_STRATEGY_NONE) {
    int32_t count{0};
    s = TrimStream(count, stream_meta, key, args, default_read_options_);
    if (!s.ok()) {
      return Status::Corruption("error from XADD, trim stream failed: " + s.ToString());
    }
    (void)count;
  }

  // 5 update stream meta
  BaseMetaKey base_meta_key(key);
  s = db_->Put(default_write_options_, handles_[kStreamsMetaCF], base_meta_key.Encode(), stream_meta.value());
  if (!s.ok()) {
    return s;
  }

  return Status::OK();
}

Status Redis::XTrim(const Slice& key, StreamAddTrimArgs& args, int32_t& count) {

  // 1 get stream meta
  rocksdb::Status s;
  StreamMetaValue stream_meta;
  s = GetStreamMeta(stream_meta, key, default_read_options_);
  if (!s.ok()) {
    return s;
  }

  // 2 do the trim
  count = 0;
  s = TrimStream(count, stream_meta, key, args, default_read_options_);
  if (!s.ok()) {
    return s;
  }

  // 3 update stream meta
  BaseMetaKey base_meta_key(key);
  s = db_->Put(default_write_options_, handles_[kStreamsMetaCF], base_meta_key.Encode(), stream_meta.value());
  if (!s.ok()) {
    return s;
  }

  return Status::OK();
}

Status Redis::XDel(const Slice& key, const std::vector<streamID>& ids, int32_t& count) {

  // 1 try to get stream meta
  StreamMetaValue stream_meta;
  auto s = GetStreamMeta(stream_meta, key, default_read_options_);
  if (!s.ok()) {
    return s;
  }

  // 2 do the delete
  if (ids.size() > INT32_MAX) {
    return Status::InvalidArgument("Too many IDs specified");
  }
  count = static_cast<int32_t>(ids.size());
  std::string unused;
  for (auto id : ids) {
    StreamDataKey stream_data_key(key, stream_meta.version(), id.Serialize());
    s = db_->Get(default_read_options_, handles_[kStreamsDataCF], stream_data_key.Encode(), &unused);
    if (s.IsNotFound()) {
      --count;
      continue;
    } else if (!s.ok()) {
      return s;
    }
  }
  s = DeleteStreamMessages(key, stream_meta, ids, default_read_options_);
  if (!s.ok()) {
    return s;
  }

  // 3 update stream meta
  stream_meta.set_length(stream_meta.length() - count);
  for (const auto& id : ids) {
    if (id > stream_meta.max_deleted_entry_id()) {
      stream_meta.set_max_deleted_entry_id(id);
    }
    if (id == stream_meta.first_id()) {
      s = SetFirstID(key, stream_meta, default_read_options_);
    } else if (id == stream_meta.last_id()) {
      s = SetLastID(key, stream_meta, default_read_options_);
    }
    if (!s.ok()) {
      return s;
    }
  }
  
  return db_->Put(default_write_options_, handles_[kStreamsMetaCF], BaseMetaKey(key).Encode(), stream_meta.value());
}

Status Redis::XRange(const Slice& key, const StreamScanArgs& args, std::vector<IdMessage>& field_values) {
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  // 1 get stream meta
  rocksdb::Status s;
  StreamMetaValue stream_meta;
  s = GetStreamMeta(stream_meta, key, read_options);
  if (!s.ok()) {
    return s;
  }

  // 2 do the scan
  std::string next_field;
  ScanStreamOptions options(key, stream_meta.version(), args.start_sid, args.end_sid, args.limit, args.start_ex,
                            args.end_ex, false);
  s = ScanStream(options, field_values, next_field, read_options);
  (void)next_field;

  return s;
}

Status Redis::XRevrange(const Slice& key, const StreamScanArgs& args, std::vector<IdMessage>& field_values) {
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  // 1 get stream meta
  rocksdb::Status s;
  StreamMetaValue stream_meta;
  s = GetStreamMeta(stream_meta, key, read_options);
  if (!s.ok()) {
    return s;
  }

  // 2 do the scan
  std::string next_field;
  ScanStreamOptions options(key, stream_meta.version(), args.start_sid, args.end_sid, args.limit, args.start_ex,
                            args.end_ex, true);
  s = ScanStream(options, field_values, next_field, read_options);
  (void)next_field;

  return s;
}

Status Redis::XLen(const Slice& key, int32_t& len) {
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  // 1 get stream meta
  rocksdb::Status s;
  StreamMetaValue stream_meta;
  s = GetStreamMeta(stream_meta, key, read_options);
  if (!s.ok()) {
    return s;
  }

  len = stream_meta.length();
  return Status::OK();
}

Status Redis::XRead(const StreamReadGroupReadArgs& args, std::vector<std::vector<storage::IdMessage>>& results,
                    std::vector<std::string>& reserved_keys) {
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  // 1 prepare stream_metas
  rocksdb::Status s;
  std::vector<std::pair<StreamMetaValue, int>> streammeta_idx;
  for (int i = 0; i < args.unparsed_ids.size(); i++) {
    const auto& key = args.keys[i];

    StreamMetaValue stream_meta;
    auto s = GetStreamMeta(stream_meta, key, read_options);
    if (s.IsNotFound()) {
      continue;
    } else if (!s.ok()) {
      return s;
    }

    streammeta_idx.emplace_back(std::move(stream_meta), i);
  }

  if (streammeta_idx.empty()) {
    return Status::OK();
  }

  // 2 do the scan
  for (const auto& stream_meta_id : streammeta_idx) {
    const auto& stream_meta = stream_meta_id.first;
    const auto& idx = stream_meta_id.second;
    const auto& unparsed_id = args.unparsed_ids[idx];
    const auto& key = args.keys[idx];

    // 2.1 try to parse id
    storage::streamID id;
    if (unparsed_id == "<") {
      return Status::Corruption(
          "The > ID can be specified only when calling "
          "XREADGROUP using the GROUP <group> "
          "<consumer> option.");
    } else if (unparsed_id == "$") {
      id = stream_meta.last_id();
    } else {
      if (!storage::StreamUtils::StreamParseStrictID(unparsed_id, id, 0, nullptr)) {
        return Status::Corruption("Invalid stream ID specified as stream ");
      }
    }

    // 2.2 scan
    std::vector<storage::IdMessage> field_values;
    std::string next_field;
    ScanStreamOptions options(key, stream_meta.version(), id, storage::kSTREAMID_MAX, args.count, true);
    auto s = ScanStream(options, field_values, next_field, read_options);
    (void)next_field;
    if (!s.ok() && !s.IsNotFound()) {
      return s;
    }
    results.emplace_back(std::move(field_values));
    reserved_keys.emplace_back(args.keys[idx]);
  }

  return Status::OK();
}

Status Redis::XInfo(const Slice& key, StreamInfoResult& result) {
  // 1 get stream meta
  rocksdb::Status s;
  StreamMetaValue stream_meta;
  s = GetStreamMeta(stream_meta, key, default_read_options_);
  if (!s.ok()) {
    return s;
  }

  // 2 fill the result
  result.length = stream_meta.length();
  result.last_id_str = stream_meta.last_id().ToString();
  result.max_deleted_entry_id_str = stream_meta.max_deleted_entry_id().ToString();
  result.entries_added = stream_meta.entries_added();
  result.first_id_str = stream_meta.first_id().ToString();

  return Status::OK();
}

Status Redis::ScanStreamsKeyNum(KeyInfo* key_info) {
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

  rocksdb::Iterator* iter = db_->NewIterator(iterator_options, handles_[kStreamsMetaCF]);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ParsedStreamMetaValue parsed_stream_meta_value(iter->value());
    if (parsed_stream_meta_value.length() == 0) {
      invaild_keys++;
    } else {
      keys++;
    }
  }
  delete iter;

  key_info->keys = keys;
  key_info->invaild_keys = invaild_keys;
  return Status::OK();
}

Status Redis::StreamsPKPatternMatchDel(const std::string& pattern, int32_t* ret) {
  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  std::string encoded_key;
  std::string meta_value;
  int32_t total_delete = 0;
  Status s;
  rocksdb::WriteBatch batch;
  rocksdb::Iterator* iter = db_->NewIterator(iterator_options, handles_[kStreamsMetaCF]);
  iter->SeekToFirst();
  while (iter->Valid()) {
    encoded_key = iter->key().ToString();
    meta_value = iter->value().ToString();
    ParsedBaseMetaKey parsed_meta_key(iter->key());
    StreamMetaValue stream_meta_value;
    stream_meta_value.ParseFrom(meta_value);
    if ((stream_meta_value.length() != 0) &&
        (StringMatch(pattern.data(), pattern.size(), parsed_meta_key.Key().data(), parsed_meta_key.Key().size(), 0) != 0)) {
      stream_meta_value.InitMetaValue();
      batch.Put(handles_[kStreamsMetaCF], encoded_key, stream_meta_value.value());
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

Status Redis::StreamsDel(const Slice& key) {
  std::string meta_value;
  BaseMetaKey base_meta_key(key);
  Status s = db_->Get(default_read_options_, handles_[kStreamsMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    StreamMetaValue stream_meta_value;
    stream_meta_value.ParseFrom(meta_value);
    if (stream_meta_value.length() == 0) {
      return Status::NotFound();
    } else {
      uint32_t statistic = stream_meta_value.length();
      stream_meta_value.InitMetaValue();
      s = db_->Put(default_write_options_, handles_[kStreamsMetaCF], base_meta_key.Encode(), stream_meta_value.value());
      UpdateSpecificKeyStatistics(DataType::kStreams, key.ToString(), statistic);
    }
  }
  return s;
}

Status Redis::GetStreamMeta(StreamMetaValue& stream_meta, const rocksdb::Slice& key,
                            rocksdb::ReadOptions& read_options) {
  std::string value;
  BaseMetaKey base_meta_key(key);
  auto s = db_->Get(read_options, handles_[kStreamsMetaCF], base_meta_key.Encode(), &value);
  if (s.ok()) {
    stream_meta.ParseFrom(value);
    return Status::OK();
  }
  return s;
}

Status Redis::TrimStream(int32_t& count, StreamMetaValue& stream_meta, const rocksdb::Slice& key,
                         StreamAddTrimArgs& args, rocksdb::ReadOptions& read_options) {
  count = 0;
  // 1 do the trim
  TrimRet trim_ret;
  Status s;
  if (args.trim_strategy == StreamTrimStrategy::TRIM_STRATEGY_MAXLEN) {
    s = TrimByMaxlen(trim_ret, stream_meta, key, args, read_options);
  } else {
    assert(args.trim_strategy == StreamTrimStrategy::TRIM_STRATEGY_MINID);
    s = TrimByMinid(trim_ret, stream_meta, key, args, read_options);
  }

  if (!s.ok()) {
    return s;
  }

  if (trim_ret.count == 0) {
    return s;
  }

  // 2 update stream meta
  streamID first_id;
  streamID max_deleted_id;
  if (stream_meta.length() == trim_ret.count) {
    // all the message in the stream were deleted
    first_id = kSTREAMID_MIN;
  } else {
    first_id.DeserializeFrom(trim_ret.next_field);
  }
  assert(!trim_ret.max_deleted_field.empty());
  max_deleted_id.DeserializeFrom(trim_ret.max_deleted_field);

  stream_meta.set_first_id(first_id);
  if (max_deleted_id > stream_meta.max_deleted_entry_id()) {
    stream_meta.set_max_deleted_entry_id(max_deleted_id);
  }
  stream_meta.set_length(stream_meta.length() - trim_ret.count);

  count = trim_ret.count;
  return Status::OK();
}

Status Redis::ScanStream(const ScanStreamOptions& op, std::vector<IdMessage>& field_values,
                         std::string& next_field, rocksdb::ReadOptions& read_options) {
  std::string start_field;
  std::string end_field;
  Slice pattern = "*";  // match all the fields from start_field to end_field
  Status s;

  // 1 do the scan
  if (op.is_reverse) {
    start_field = op.end_sid.Serialize();
    if (op.start_sid == kSTREAMID_MAX) {
      start_field = "";
    } else {
      start_field = op.start_sid.Serialize();
    }
    s = StreamReScanRange(op.key, op.version, start_field, end_field, pattern, op.limit, field_values, next_field,
                          read_options);
  } else {
    start_field = op.start_sid.Serialize();
    if (op.end_sid == kSTREAMID_MAX) {
      end_field = "";
    } else {
      end_field = op.end_sid.Serialize();
    }
    s = StreamScanRange(op.key, op.version, start_field, end_field, pattern, op.limit, field_values, next_field,
                        read_options);
  }

  // 2 exclude the start_sid and end_sid if needed
  if (op.start_ex && !field_values.empty()) {
    streamID sid;
    sid.DeserializeFrom(field_values.front().field);
    if (sid == op.start_sid) {
      field_values.erase(field_values.begin());
    }
  }

  if (op.end_ex && !field_values.empty()) {
    streamID sid;
    sid.DeserializeFrom(field_values.back().field);
    if (sid == op.end_sid) {
      field_values.pop_back();
    }
  }

  return s;
}

Status Redis::GenerateStreamID(const StreamMetaValue& stream_meta, StreamAddTrimArgs& args) {
  auto& id = args.id;
  if (args.id_given && args.seq_given && id.ms == 0 && id.seq == 0) {
    return Status::InvalidArgument("The ID specified in XADD must be greater than 0-0");
  }

  if (!args.id_given || !args.seq_given) {
    // if id not given, generate one
    if (!args.id_given) {
      id.ms = StreamUtils::GetCurrentTimeMs();

      if (id.ms < stream_meta.last_id().ms) {
        id.ms = stream_meta.last_id().ms;
        if (stream_meta.last_id().seq == UINT64_MAX) {
          id.ms++;
          id.seq = 0;
        } else {
          id.seq++;
        }
        return Status::OK();
      }
    }

    // generate seq
    auto last_id = stream_meta.last_id();
    if (id.ms < last_id.ms) {
      return Status::InvalidArgument("The ID specified in XADD is equal or smaller");
    } else if (id.ms == last_id.ms) {
      if (last_id.seq == UINT64_MAX) {
        return Status::InvalidArgument("The ID specified in XADD is equal or smaller");
      }
      id.seq = last_id.seq + 1;
    } else {
      id.seq = 0;
    }

  } else {
    //  Full ID given, check id
    auto last_id = stream_meta.last_id();
    if (id.ms < last_id.ms || (id.ms == last_id.ms && id.seq <= last_id.seq)) {
      return Status::InvalidArgument("INVALID ID given");
    }
  }
  return Status::OK();
}

Status Redis::TrimByMaxlen(TrimRet& trim_ret, StreamMetaValue& stream_meta, const rocksdb::Slice& key,
                           const StreamAddTrimArgs& args, rocksdb::ReadOptions& read_options) {
  Status s;
  // we delete the message in batchs, prevent from using too much memory
  while (stream_meta.length() - trim_ret.count > args.maxlen) {
    auto cur_batch =
        (std::min(static_cast<int32_t>(stream_meta.length() - trim_ret.count - args.maxlen), kDEFAULT_TRIM_BATCH_SIZE));
    std::vector<IdMessage> id_messages;

    ScanStreamOptions options(key, stream_meta.version(), stream_meta.first_id(), kSTREAMID_MAX,
                              cur_batch, false, false, false);
    s = ScanStream(options, id_messages, trim_ret.next_field, read_options);
    if (!s.ok()) {
      assert(!s.IsNotFound());
      return s;
    }

    assert(id_messages.size() == cur_batch);
    trim_ret.count += cur_batch;
    trim_ret.max_deleted_field = id_messages.back().field;

    // delete the message in batchs
    std::vector<std::string> ids_to_del;
    ids_to_del.reserve(id_messages.size());
    for (auto& fv : id_messages) {
      ids_to_del.emplace_back(std::move(fv.field));
    }
    s = DeleteStreamMessages(key, stream_meta, ids_to_del, read_options);
    if (!s.ok()) {
      return s;
    }
  }

  s = Status::OK();
  return s;
}

Status Redis::TrimByMinid(TrimRet& trim_ret, StreamMetaValue& stream_meta, const rocksdb::Slice& key,
                          const StreamAddTrimArgs& args, rocksdb::ReadOptions& read_options) {
  Status s;
  std::string serialized_min_id;
  trim_ret.next_field = stream_meta.first_id().Serialize();
  serialized_min_id = args.minid.Serialize();

  // we delete the message in batchs, prevent from using too much memory
  while (trim_ret.next_field < serialized_min_id && stream_meta.length() - trim_ret.count > 0) {
    auto cur_batch = static_cast<int32_t>(
        std::min(static_cast<int32_t>(stream_meta.length() - trim_ret.count), kDEFAULT_TRIM_BATCH_SIZE));
    std::vector<IdMessage> id_messages;

    ScanStreamOptions options(key, stream_meta.version(), stream_meta.first_id(), args.minid, cur_batch,
                              false, false, false);
    s = ScanStream(options, id_messages, trim_ret.next_field, read_options);
    if (!s.ok()) {
      assert(!s.IsNotFound());
      return s;
    }

    if (!id_messages.empty()) {
      if (id_messages.back().field == serialized_min_id) {
        // we do not need to delete the message that it's id matches the minid
        id_messages.pop_back();
        trim_ret.next_field = serialized_min_id;
      }
      // duble check
      if (!id_messages.empty()) {
        trim_ret.max_deleted_field = id_messages.back().field;
      }
    }

    assert(id_messages.size() <= cur_batch);
    trim_ret.count += static_cast<int32_t>(id_messages.size());

    // do the delete in batch
    std::vector<std::string> fields_to_del;
    fields_to_del.reserve(id_messages.size());
    for (auto& fv : id_messages) {
      fields_to_del.emplace_back(std::move(fv.field));
    }

    s = DeleteStreamMessages(key, stream_meta, fields_to_del, read_options);
    if (!s.ok()) {
      return s;
    }
  }

  s = Status::OK();
  return s;
}

Status Redis::StreamScanRange(const Slice& key, const uint64_t version, const Slice& id_start,
                              const std::string& id_end, const Slice& pattern, int32_t limit,
                              std::vector<IdMessage>& id_messages, std::string& next_id,
                              rocksdb::ReadOptions& read_options) {
  next_id.clear();
  id_messages.clear();

  auto remain = limit;
  std::string meta_value;

  bool start_no_limit = id_start.compare("") == 0;
  bool end_no_limit = id_end.empty();

  if (!start_no_limit && !end_no_limit && (id_start.compare(id_end) > 0)) {
    return Status::InvalidArgument("error in given range");
  }

  StreamDataKey streams_data_prefix(key, version, Slice());
  StreamDataKey streams_start_data_key(key, version, id_start);
  std::string prefix = streams_data_prefix.EncodeSeekKey().ToString();
  rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[kStreamsDataCF]);
  for (iter->Seek(start_no_limit ? prefix : streams_start_data_key.Encode());
       iter->Valid() && remain > 0 && iter->key().starts_with(prefix); iter->Next()) {
    ParsedStreamDataKey parsed_streams_data_key(iter->key());
    std::string id = parsed_streams_data_key.id().ToString();
    if (!end_no_limit && id.compare(id_end) > 0) {
      break;
    }
    if (StringMatch(pattern.data(), pattern.size(), id.data(), id.size(), 0) != 0) {
      id_messages.push_back({id, iter->value().ToString()});
    }
    remain--;
  }

  if (iter->Valid() && iter->key().starts_with(prefix)) {
    ParsedStreamDataKey parsed_streams_data_key(iter->key());
    if (end_no_limit || parsed_streams_data_key.id().compare(id_end) <= 0) {
      next_id = parsed_streams_data_key.id().ToString();
    }
  }
  delete iter;

  return Status::OK();
}

Status Redis::StreamReScanRange(const Slice& key, const uint64_t version, const Slice& id_start,
                                const std::string& id_end, const Slice& pattern, int32_t limit,
                                std::vector<IdMessage>& id_messages, std::string& next_id,
                                rocksdb::ReadOptions& read_options) {
  next_id.clear();
  id_messages.clear();

  auto remain = limit;
  std::string meta_value;

  bool start_no_limit = id_start.compare("") == 0;
  bool end_no_limit = id_end.empty();

  if (!start_no_limit && !end_no_limit && (id_start.compare(id_end) < 0)) {
    return Status::InvalidArgument("error in given range");
  }

  uint64_t start_key_version = start_no_limit ? version + 1 : version;
  std::string start_key_id = start_no_limit ? "" : id_start.ToString();
  StreamDataKey streams_data_prefix(key, version, Slice());
  StreamDataKey streams_start_data_key(key, start_key_version, start_key_id);
  std::string prefix = streams_data_prefix.EncodeSeekKey().ToString();
  rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[kStreamsDataCF]);
  for (iter->SeekForPrev(streams_start_data_key.Encode().ToString());
       iter->Valid() && remain > 0 && iter->key().starts_with(prefix); iter->Prev()) {
    ParsedStreamDataKey parsed_streams_data_key(iter->key());
    std::string id = parsed_streams_data_key.id().ToString();
    if (!end_no_limit && id.compare(id_end) < 0) {
      break;
    }
    if (StringMatch(pattern.data(), pattern.size(), id.data(), id.size(), 0) != 0) {
      id_messages.push_back({id, iter->value().ToString()});
    }
    remain--;
  }

  if (iter->Valid() && iter->key().starts_with(prefix)) {
    ParsedStreamDataKey parsed_streams_data_key(iter->key());
    if (end_no_limit || parsed_streams_data_key.id().compare(id_end) >= 0) {
      next_id = parsed_streams_data_key.id().ToString();
    }
  }
  delete iter;

  return Status::OK();
}

Status Redis::DeleteStreamMessages(const rocksdb::Slice& key, const StreamMetaValue& stream_meta,
                                   const std::vector<streamID>& ids, rocksdb::ReadOptions& read_options) {
  std::vector<std::string> serialized_ids;
  serialized_ids.reserve(ids.size());
  for (const auto& id : ids) {
    serialized_ids.emplace_back(id.Serialize());
  }
  return DeleteStreamMessages(key, stream_meta, serialized_ids, read_options);
}

Status Redis::DeleteStreamMessages(const rocksdb::Slice& key, const StreamMetaValue& stream_meta,
                                   const std::vector<std::string>& serialized_ids,
                                   rocksdb::ReadOptions& read_options) {
  rocksdb::WriteBatch batch;
  for (auto& sid : serialized_ids) {
    StreamDataKey stream_data_key(key, stream_meta.version(), sid);
    batch.Delete(handles_[kStreamsDataCF], stream_data_key.Encode());
  }
  return db_->Write(default_write_options_, &batch);
}

inline Status Redis::SetFirstID(const rocksdb::Slice& key, StreamMetaValue& stream_meta,
                                rocksdb::ReadOptions& read_options) {
  return SetFirstOrLastID(key, stream_meta, true, read_options);
}

inline Status Redis::SetLastID(const rocksdb::Slice& key, StreamMetaValue& stream_meta,
                               rocksdb::ReadOptions& read_options) {
  return SetFirstOrLastID(key, stream_meta, false, read_options);
}

inline Status Redis::SetFirstOrLastID(const rocksdb::Slice& key, StreamMetaValue& stream_meta, bool is_set_first,
                                      rocksdb::ReadOptions& read_options) {
  if (stream_meta.length() == 0) {
    stream_meta.set_first_id(kSTREAMID_MIN);
    return Status::OK();
  }

  std::vector<storage::IdMessage> id_messages;
  std::string next_field;

  storage::Status s;
  if (is_set_first) {
    ScanStreamOptions option(key, stream_meta.version(), kSTREAMID_MIN, kSTREAMID_MAX, 1);
    s = ScanStream(option, id_messages, next_field, read_options);
  } else {
    bool is_reverse = true;
    ScanStreamOptions option(key, stream_meta.version(), kSTREAMID_MAX, kSTREAMID_MIN, 1, false, false, is_reverse);
    s = ScanStream(option, id_messages, next_field, read_options);
  }
  (void)next_field;

  if (!s.ok() && !s.IsNotFound()) {
    LOG(ERROR) << "Internal error: scan stream failed: " << s.ToString();
    return Status::Corruption("Internal error: scan stream failed: " + s.ToString());
  }

  if (id_messages.empty()) {
    LOG(ERROR) << "Internal error: no messages found but stream length is not 0";
    return Status::Corruption("Internal error: no messages found but stream length is not 0");
  }

  streamID id;
  id.DeserializeFrom(id_messages[0].field);
  stream_meta.set_first_id(id);
  return Status::OK();
}

bool StreamUtils::StreamGenericParseID(const std::string& var, streamID& id, uint64_t missing_seq, bool strict,
                                       bool* seq_given) {
  char buf[128];
  if (var.size() > sizeof(buf) - 1) {
    return false;
  }

  memcpy(buf, var.data(), var.size());
  buf[var.size()] = '\0';

  if (strict && (buf[0] == '-' || buf[0] == '+') && buf[1] == '\0') {
    // res.SetRes(CmdRes::kInvalidParameter, "Invalid stream ID specified as stream ");
    return false;
  }

  if (seq_given != nullptr) {
    *seq_given = true;
  }

  if (buf[0] == '-' && buf[1] == '\0') {
    id.ms = 0;
    id.seq = 0;
    return true;
  } else if (buf[0] == '+' && buf[1] == '\0') {
    id.ms = UINT64_MAX;
    id.seq = UINT64_MAX;
    return true;
  }

  uint64_t ms;
  uint64_t seq;
  char* dot = strchr(buf, '-');
  if (dot) {
    *dot = '\0';
  }
  if (!StreamUtils::string2uint64(buf, ms)) {
    return false;
  };
  if (dot) {
    auto seqlen = strlen(dot + 1);
    if (seq_given != nullptr && seqlen == 1 && *(dot + 1) == '*') {
      seq = 0;
      *seq_given = false;
    } else if (!StreamUtils::string2uint64(dot + 1, seq)) {
      return false;
    }
  } else {
    seq = missing_seq;
  }
  id.ms = ms;
  id.seq = seq;
  return true;
}

bool StreamUtils::StreamParseID(const std::string& var, streamID& id, uint64_t missing_seq) {
  return StreamGenericParseID(var, id, missing_seq, false, nullptr);
}

bool StreamUtils::StreamParseStrictID(const std::string& var, streamID& id, uint64_t missing_seq, bool* seq_given) {
  return StreamGenericParseID(var, id, missing_seq, true, seq_given);
}

bool StreamUtils::StreamParseIntervalId(const std::string& var, streamID& id, bool* exclude, uint64_t missing_seq) {
  if (exclude != nullptr) {
    *exclude = (var.size() > 1 && var[0] == '(');
  }
  if (exclude != nullptr && *exclude) {
    return StreamParseStrictID(var.substr(1), id, missing_seq, nullptr);
  } else {
    return StreamParseID(var, id, missing_seq);
  }
}

bool StreamUtils::string2uint64(const char* s, uint64_t& value) {
  if (!s || !*s) {
    return false;
  }

  char* end;
  errno = 0;
  uint64_t tmp = strtoull(s, &end, 10);
  if (*end || errno == ERANGE) {
    // Conversion either didn't consume the entire string, or overflow occurred
    return false;
  }

  value = tmp;
  return true;
}

bool StreamUtils::string2int64(const char* s, int64_t& value) {
  if (!s || !*s) {
    return false;
  }

  char* end;
  errno = 0;
  int64_t tmp = std::strtoll(s, &end, 10);
  if (*end || errno == ERANGE) {
    // Conversion either didn't consume the entire string, or overflow occurred
    return false;
  }

  value = tmp;
  return true;
}

bool StreamUtils::string2int32(const char* s, int32_t& value) {
  if (!s || !*s) {
    return false;
  }

  char* end;
  errno = 0;
  long tmp = strtol(s, &end, 10);
  if (*end || errno == ERANGE || tmp < INT_MIN || tmp > INT_MAX) {
    // Conversion either didn't consume the entire string,
    // or overflow or underflow occurred
    return false;
  }

  value = static_cast<int32_t>(tmp);
  return true;
}

bool StreamUtils::SerializeMessage(const std::vector<std::string>& field_values, std::string& message, int field_pos) {
  assert(field_values.size() - field_pos >= 2 && (field_values.size() - field_pos) % 2 == 0);
  assert(message.empty());
  // count the size of serizlized message
  uint32_t size = 0;
  for (int i = field_pos; i < field_values.size(); i++) {
    size += field_values[i].size() + sizeof(uint32_t);
  }
  message.reserve(size);

  // serialize message
  for (int i = field_pos; i < field_values.size(); i++) {
    uint32_t len = field_values[i].size();
    message.append(reinterpret_cast<const char*>(&len), sizeof(len));
    message.append(field_values[i]);
  }

  return true;
}

bool StreamUtils::DeserializeMessage(const std::string& message, std::vector<std::string>& parsed_message) {
  uint32_t pos = 0;
  while (pos < message.size()) {
    // Read the length of the next field value from the message
    uint32_t len = *reinterpret_cast<const uint32_t*>(&message[pos]);
    pos += sizeof(uint32_t);

    // Check if the calculated end of the string is still within the message bounds
    if (pos + len > message.size()) {
      LOG(ERROR) << "Invalid message format, failed to parse message";
      return false;  // Error: not enough data in the message string
    }

    // Extract the field value and add it to the vector
    parsed_message.push_back(message.substr(pos, len));
    pos += len;
  }

  return true;
}

uint64_t StreamUtils::GetCurrentTimeMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
      .count();
}
};  // namespace storage
