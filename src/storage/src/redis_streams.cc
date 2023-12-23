#include "src/redis_streams.h"
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>
#include "pika_stream_base.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "src/base_data_key_format.h"
#include "src/base_filter.h"
#include "src/debug.h"
#include "src/pika_stream_meta_value.h"
#include "src/scope_record_lock.h"
#include "src/scope_snapshot.h"
#include "storage/storage.h"
#include "storage/util.h"

namespace storage {

Status RedisStreams::XAdd(const Slice& key, const std::string& serialized_message, StreamAddTrimArgs& args) {
  // With the lock, we do not need snapshot for read.
  // And it's bugy to use snapshot for read when we try to add message with trim.
  // such as: XADD key 1-0 field value MINID 1-0
  ScopeRecordLock l(lock_mgr_, key);

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
  std::string serialized_last_id;
  std::string current_id;
  stream_meta.last_id().SerializeTo(serialized_last_id);
  args.id.SerializeTo(current_id);
  assert(current_id > serialized_last_id);
#endif

  StreamDataKey stream_data_key(key, stream_meta.version(), args.id.Serialize());
  s = db_->Put(default_write_options_, handles_[1], stream_data_key.Encode(), serialized_message);
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
    size_t count{0};
    s = TrimStream(count, stream_meta, key, args, default_read_options_);
    if (!s.ok()) {
      return Status::Corruption("error from XADD, trim stream failed: " + s.ToString());
    }
    (void)count;
  }

  // 5 update stream meta
  s = db_->Put(default_write_options_, handles_[0], key, stream_meta.value());
  if (!s.ok()) {
    return s;
  }

  return Status::OK();
}

Status RedisStreams::XTrim(const Slice& key, StreamAddTrimArgs& args, size_t& count) {
  ScopeRecordLock l(lock_mgr_, key);

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
  s = db_->Put(default_write_options_, handles_[0], key, stream_meta.value());
  if (!s.ok()) {
    return s;
  }

  return Status::OK();
}

Status RedisStreams::XDel(const Slice& key, const std::vector<streamID>& ids, size_t& count) {
  ScopeRecordLock l(lock_mgr_, key);

  // 1 try to get stream meta
  StreamMetaValue stream_meta;
  auto s = GetStreamMeta(stream_meta, key, default_read_options_);
  if (!s.ok()) {
    return s;
  }

  // 2 do the delete
  count = ids.size();
  std::string unused;
  for (auto id : ids) {
    StreamDataKey stream_data_key(key, stream_meta.version(), id.Serialize());
    s = db_->Get(default_read_options_, handles_[1], stream_data_key.Encode(), &unused);
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

  return db_->Put(default_write_options_, handles_[0], key, stream_meta.value());
}

Status RedisStreams::XRange(const Slice& key, const StreamScanArgs& args, std::vector<IdMessage>& field_values) {
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

Status RedisStreams::XRevrange(const Slice& key, const StreamScanArgs& args, std::vector<IdMessage>& field_values) {
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

Status RedisStreams::XLen(const Slice& key, uint64_t& len) {
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

Status RedisStreams::XRead(const StreamReadGroupReadArgs& args, std::vector<std::vector<storage::IdMessage>>& results,
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

Status RedisStreams::XInfo(const Slice& key, StreamInfoResult& result) {
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

Status RedisStreams::Open(const StorageOptions& storage_options, const std::string& db_path) {
  statistics_store_->SetCapacity(storage_options.statistics_max_size);
  small_compaction_threshold_ = storage_options.small_compaction_threshold;

  rocksdb::Options ops(storage_options.options);
  Status s = rocksdb::DB::Open(ops, db_path, &db_);
  if (s.ok()) {
    // create column family
    rocksdb::ColumnFamilyHandle* cf;
    // FIXME: Dose stream data need a comparater ?
    s = db_->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "data_cf", &cf);
    if (!s.ok()) {
      return s;
    }
    // close DB
    delete cf;
    delete db_;
  }

  // Open
  rocksdb::DBOptions db_ops(storage_options.options);
  rocksdb::ColumnFamilyOptions meta_cf_ops(storage_options.options);
  rocksdb::ColumnFamilyOptions data_cf_ops(storage_options.options);
  // Notice: Stream's Meta dose not have timestamp and version, so it does not need to be filtered.

  // use the bloom filter policy to reduce disk reads
  rocksdb::BlockBasedTableOptions table_ops(storage_options.table_options);
  table_ops.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, true));
  rocksdb::BlockBasedTableOptions meta_cf_table_ops(table_ops);
  rocksdb::BlockBasedTableOptions data_cf_table_ops(table_ops);
  if (!storage_options.share_block_cache && storage_options.block_cache_size > 0) {
    meta_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
    data_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
  }
  meta_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(meta_cf_table_ops));
  data_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(data_cf_table_ops));

  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  // Meta CF
  column_families.emplace_back(rocksdb::kDefaultColumnFamilyName, meta_cf_ops);
  // Data CF
  column_families.emplace_back("data_cf", data_cf_ops);
  return rocksdb::DB::Open(db_ops, db_path, column_families, &handles_, &db_);
}

Status RedisStreams::CompactRange(const rocksdb::Slice* begin, const rocksdb::Slice* end,
                                  const ColumnFamilyType& type) {
  if (type == kMeta || type == kMetaAndData) {
    db_->CompactRange(default_compact_range_options_, handles_[0], begin, end);
  }
  if (type == kData || type == kMetaAndData) {
    db_->CompactRange(default_compact_range_options_, handles_[1], begin, end);
  }
  return Status::OK();
}

Status RedisStreams::GetProperty(const std::string& property, uint64_t* out) {
  std::string value;
  db_->GetProperty(handles_[0], property, &value);
  *out = std::strtoull(value.c_str(), nullptr, 10);
  db_->GetProperty(handles_[1], property, &value);
  *out += std::strtoull(value.c_str(), nullptr, 10);
  return Status::OK();
}

// Check if the key has prefix of STERAM_TREE_PREFIX.
// That means the key-value is a virtual tree node, not a stream meta.
bool IsVirtualTree(rocksdb::Slice key) {
  if (key.size() < STERAM_TREE_PREFIX.size()) {
    return false;
  }

  if (memcmp(key.data(), STERAM_TREE_PREFIX.data(), STERAM_TREE_PREFIX.size()) == 0) {
    return true;
  }

  return false;
}

Status RedisStreams::ScanKeyNum(KeyInfo* key_info) {
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
    if (IsVirtualTree(iter->key())) {
      continue;
    }
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

Status RedisStreams::ScanKeys(const std::string& pattern, std::vector<std::string>* keys) {
  std::string key;
  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  rocksdb::Iterator* iter = db_->NewIterator(iterator_options, handles_[0]);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    if (IsVirtualTree(iter->key())) {
      continue;
    }
    ParsedStreamMetaValue parsed_stream_meta_value(iter->value());
    if (parsed_stream_meta_value.length() != 0) {
      key = iter->key().ToString();
      if (StringMatch(pattern.data(), pattern.size(), key.data(), key.size(), 0) != 0) {
        keys->push_back(key);
      }
    }
  }
  delete iter;
  return Status::OK();
}

Status RedisStreams::PKPatternMatchDel(const std::string& pattern, int32_t* ret) {
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
    if (IsVirtualTree(iter->key())) {
      iter->Next();
      continue;
    }
    key = iter->key().ToString();
    meta_value = iter->value().ToString();
    StreamMetaValue stream_meta_value;
    stream_meta_value.ParseFrom(meta_value);
    if ((stream_meta_value.length() != 0) &&
        (StringMatch(pattern.data(), pattern.size(), key.data(), key.size(), 0) != 0)) {
      stream_meta_value.InitMetaValue();
      batch.Put(handles_[0], key, stream_meta_value.value());
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

Status RedisStreams::Del(const Slice& key) {
  // FIXME: Check the prefix of key
  // stream TODO: Delete all the cgroup and consumers
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    StreamMetaValue stream_meta_value;
    stream_meta_value.ParseFrom(meta_value);
    if (stream_meta_value.length() == 0) {
      return Status::NotFound();
    } else {
      uint32_t statistic = stream_meta_value.length();
      stream_meta_value.InitMetaValue();
      s = db_->Put(default_write_options_, handles_[0], key, stream_meta_value.value());
      UpdateSpecificKeyStatistics(key.ToString(), statistic);
    }
  }
  return s;
}

bool RedisStreams::Scan(const std::string& start_key, const std::string& pattern, std::vector<std::string>* keys,
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
    if (IsVirtualTree(it->key())) {
      it->Next();
      continue;
    }
    ParsedStreamMetaValue parsed_stream_meta_value(it->value());
    if (parsed_stream_meta_value.length() == 0) {
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

Status RedisStreams::Expire(const Slice& key, int32_t ttl) {
  rocksdb::Status s(rocksdb::Status::NotSupported("RedisStreams::Expire not supported by stream"));
  return Status::Corruption(s.ToString());
}

bool RedisStreams::PKExpireScan(const std::string& start_key, int32_t min_timestamp, int32_t max_timestamp,
                                std::vector<std::string>* keys, int64_t* leftover_visits, std::string* next_key) {
  TRACE("RedisStreams::PKExpireScan not supported by stream");
  return false;
}

Status RedisStreams::Expireat(const Slice& key, int32_t timestamp) {
  rocksdb::Status s(rocksdb::Status::NotSupported("RedisStreams::Expireat not supported by stream"));
  return Status::Corruption(s.ToString());
}

Status RedisStreams::Persist(const Slice& key) {
  rocksdb::Status s(rocksdb::Status::NotSupported("RedisStreams::Persist not supported by stream"));
  return Status::Corruption(s.ToString());
}

Status RedisStreams::TTL(const Slice& key, int64_t* timestamp) {
  rocksdb::Status s(rocksdb::Status::NotSupported("RedisStreams::TTL not supported by stream"));
  return Status::Corruption(s.ToString());
}

Status RedisStreams::GetStreamMeta(StreamMetaValue& stream_meta, const rocksdb::Slice& key,
                                   rocksdb::ReadOptions& read_options) {
  std::string value;
  auto s = db_->Get(read_options, handles_[0], key, &value);
  if (s.ok()) {
    stream_meta.ParseFrom(value);
    return Status::OK();
  }
  return s;
}

Status RedisStreams::TrimStream(size_t& count, StreamMetaValue& stream_meta, const rocksdb::Slice& key,
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

Status RedisStreams::ScanStream(const ScanStreamOptions& op, std::vector<IdMessage>& field_values,
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
    s = ReScanRange(op.key, op.version, start_field, end_field, pattern, op.limit, field_values, next_field,
                    read_options);
  } else {
    start_field = op.start_sid.Serialize();
    if (op.end_sid == kSTREAMID_MAX) {
      end_field = "";
    } else {
      end_field = op.end_sid.Serialize();
    }
    s = ScanRange(op.key, op.version, start_field, end_field, pattern, op.limit, field_values, next_field,
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

Status RedisStreams::GenerateStreamID(const StreamMetaValue& stream_meta, StreamAddTrimArgs& args) {
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

Status RedisStreams::TrimByMaxlen(TrimRet& trim_ret, StreamMetaValue& stream_meta, const rocksdb::Slice& key,
                                  const StreamAddTrimArgs& args, rocksdb::ReadOptions& read_options) {
  Status s;
  // we delete the message in batchs, prevent from using too much memory
  while (stream_meta.length() - trim_ret.count > args.maxlen) {
    auto cur_batch =
        (std::min(static_cast<int32_t>(stream_meta.length() - trim_ret.count - args.maxlen), kDEFAULT_TRIM_BATCH_SIZE));
    std::vector<IdMessage> id_messages;

    RedisStreams::ScanStreamOptions options(key, stream_meta.version(), stream_meta.first_id(), kSTREAMID_MAX,
                                            cur_batch, false, false, false);
    s = RedisStreams::ScanStream(options, id_messages, trim_ret.next_field, read_options);
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

Status RedisStreams::TrimByMinid(TrimRet& trim_ret, StreamMetaValue& stream_meta, const rocksdb::Slice& key,
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

    RedisStreams::ScanStreamOptions options(key, stream_meta.version(), stream_meta.first_id(), args.minid, cur_batch,
                                            false, false, false);
    s = RedisStreams::ScanStream(options, id_messages, trim_ret.next_field, read_options);
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

Status RedisStreams::ScanRange(const Slice& key, const int32_t version, const Slice& id_start,
                               const std::string& id_end, const Slice& pattern, int32_t limit,
                               std::vector<IdMessage>& id_messages, std::string& next_id,
                               rocksdb::ReadOptions& read_options) {
  next_id.clear();
  id_messages.clear();

  int64_t remain = limit;
  std::string meta_value;

  bool start_no_limit = id_start.compare("") == 0;
  bool end_no_limit = id_end.empty();

  if (!start_no_limit && !end_no_limit && (id_start.compare(id_end) > 0)) {
    return Status::InvalidArgument("error in given range");
  }

  StreamDataKey streams_data_prefix(key, version, Slice());
  StreamDataKey streams_start_data_key(key, version, id_start);
  std::string prefix = streams_data_prefix.Encode().ToString();
  rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[1]);
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

Status RedisStreams::ReScanRange(const Slice& key, const int32_t version, const Slice& id_start,
                                 const std::string& id_end, const Slice& pattern, int32_t limit,
                                 std::vector<IdMessage>& id_messages, std::string& next_id,
                                 rocksdb::ReadOptions& read_options) {
  next_id.clear();
  id_messages.clear();

  int64_t remain = limit;
  std::string meta_value;

  bool start_no_limit = id_start.compare("") == 0;
  bool end_no_limit = id_end.empty();

  if (!start_no_limit && !end_no_limit && (id_start.compare(id_end) < 0)) {
    return Status::InvalidArgument("error in given range");
  }

  int32_t start_key_version = start_no_limit ? version + 1 : version;
  std::string start_key_id = start_no_limit ? "" : id_start.ToString();
  StreamDataKey streams_data_prefix(key, version, Slice());
  StreamDataKey streams_start_data_key(key, start_key_version, start_key_id);
  std::string prefix = streams_data_prefix.Encode().ToString();
  rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[1]);
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

Status RedisStreams::DeleteStreamMessages(const rocksdb::Slice& key, const StreamMetaValue& stream_meta,
                                          const std::vector<streamID>& ids, rocksdb::ReadOptions& read_options) {
  std::vector<std::string> serialized_ids;
  serialized_ids.reserve(ids.size());
  for (const auto& id : ids) {
    serialized_ids.emplace_back(id.Serialize());
  }
  return DeleteStreamMessages(key, stream_meta, serialized_ids, read_options);
}

Status RedisStreams::DeleteStreamMessages(const rocksdb::Slice& key, const StreamMetaValue& stream_meta,
                                          const std::vector<std::string>& serialized_ids,
                                          rocksdb::ReadOptions& read_options) {
  rocksdb::WriteBatch batch;
  for (auto& sid : serialized_ids) {
    StreamDataKey stream_data_key(key, stream_meta.version(), sid);
    batch.Delete(handles_[1], stream_data_key.Encode());
  }
  return db_->Write(default_write_options_, &batch);
}
};  // namespace storage