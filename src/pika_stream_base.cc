// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_stream_base.h"

#include "include/pika_command.h"
#include "include/pika_stream_meta_value.h"
#include "include/pika_stream_types.h"
#include "storage/storage.h"

#define GET_NEXT_TREE_ID_AND_CHECK(slot, tid)       \
  do {                                              \
    auto &tid_gen = TreeIDGenerator::GetInstance(); \
    auto s = tid_gen.GetNextTreeID(slot, tid);      \
    if (!s.ok()) {                                  \
      return s;                                     \
    }                                               \
  } while (0)

storage::Status TreeIDGenerator::GetNextTreeID(const Slot *slot, treeID &tid) {
  assert(slot);
  auto expected_id = kINVALID_TREE_ID;

  // if tree_id_ is not initialized (equal to kINVALID_TREE_ID), try to fetch it from storage
  if (tree_id_.compare_exchange_strong(expected_id, START_TREE_ID)) {
    std::string value;
    storage::Status s = slot->db()->HGet(STREAM_META_HASH_KEY, STREAM_LAST_GENERATED_TREE_ID_FIELD, &value);
    if (s.ok()) {
      treeID id;
      if (!StreamUtils::string2int32(value.c_str(), id)) {
        LOG(ERROR) << "Invalid tree id: " << value;
        return storage::Status::Corruption("Invalid tree id");
      }
      tree_id_.store(id);

    } else if (!s.IsNotFound()) {
      return s;
    }
  }

  tree_id_.fetch_add(1);
  tid = tree_id_;

  // insert tree id to storage
  std::string tree_id_str = std::to_string(tree_id_);
  int32_t res;
  return slot->db()->HSet(STREAM_META_HASH_KEY, STREAM_LAST_GENERATED_TREE_ID_FIELD, tree_id_str, &res);
}

storage::Status StreamStorage::GetStreamMeta(StreamMetaValue &stream_meta, const std::string &key, const Slot *slot) {
  assert(slot);
  std::string value;
  auto s = slot->db()->HGet(STREAM_META_HASH_KEY, key, &value);
  if (s.ok()) {
    stream_meta.ParseFrom(value);
    return storage::Status::OK();
  }
  return s;
}

// no need to be thread safe, only xadd will call this function
// and xadd can be locked by the same key using current_key()
storage::Status StreamStorage::SetStreamMeta(const std::string &key, std::string &meta_value, const Slot *slot) {
  assert(slot);
  storage::Status s;
  int32_t temp{0};
  s = slot->db()->HSet(STREAM_META_HASH_KEY, key, meta_value, &temp);
  (void)temp;
  return s;
}

storage::Status StreamStorage::DeleteStreamMeta(const std::string &key, const Slot *slot) {
  assert(slot);
  int32_t ret;
  return slot->db()->HDel({STREAM_META_HASH_KEY}, {key}, &ret);
}

storage::Status StreamStorage::InsertStreamMessage(const std::string &key, const streamID &id,
                                                   const std::string &message, const Slot *slot) {
  assert(slot);
  std::string true_key = STREAM_DATA_HASH_PREFIX + key;
  int32_t temp{0};
  std::string serialized_id;
  id.SerializeTo(serialized_id);
  storage::Status s = slot->db()->HSet(true_key, serialized_id, message, &temp);
  (void)temp;
  return s;
}

storage::Status StreamStorage::GetStreamMessage(const std::string &key, const std::string &sid, std::string &message,
                                                const Slot *slot) {
  assert(slot);
  std::string true_key = STREAM_DATA_HASH_PREFIX + key;
  return slot->db()->HGet(true_key, sid, &message);
}

storage::Status StreamStorage::DeleteStreamMessage(const std::string &key, const std::vector<streamID> &id,
                                                   int32_t &ret, const Slot *slot) {
  assert(slot);
  std::string true_key = STREAM_DATA_HASH_PREFIX + key;
  std::vector<std::string> serialized_ids;
  for (auto &sid : id) {
    std::string serialized_id;
    sid.SerializeTo(serialized_id);
    serialized_ids.emplace_back(std::move(serialized_id));
  }
  return slot->db()->HDel(true_key, {serialized_ids}, &ret);
}

storage::Status StreamStorage::DeleteStreamMessage(const std::string &key,
                                                   const std::vector<std::string> &serialized_ids, int32_t &ret,
                                                   const Slot *slot) {
  assert(slot);
  std::string true_key = STREAM_DATA_HASH_PREFIX + key;
  return slot->db()->HDel(true_key, {serialized_ids}, &ret);
}

storage::Status StreamStorage::ScanStream(const ScanStreamOptions &op, std::vector<storage::FieldValue> &field_values,
                                          std::string &next_field, const Slot *slot) {
  assert(slot);
  std::string start_field;
  std::string end_field;
  storage::Slice pattern = "*";  // match all the fields from start_field to end_field
  storage::Status s;
  std::string true_key = STREAM_DATA_HASH_PREFIX + op.key;

  // 1 do the scan
  if (op.is_reverse) {
    op.end_sid.SerializeTo(start_field);
    if (op.start_sid == kSTREAMID_MAX) {
      start_field = "";
    } else {
      op.start_sid.SerializeTo(start_field);
    }
    s = slot->db()->PKHRScanRange(true_key, start_field, end_field, pattern, op.count, &field_values, &next_field);
  } else {
    op.start_sid.SerializeTo(start_field);
    if (op.end_sid == kSTREAMID_MAX) {
      end_field = "";
    } else {
      op.end_sid.SerializeTo(end_field);
    }
    s = slot->db()->PKHScanRange(true_key, start_field, end_field, pattern, op.count, &field_values, &next_field);
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

storage::Status StreamStorage::DeleteStreamData(const std::string &key, const Slot *slot) {
  assert(slot);
  std::string true_key = STREAM_DATA_HASH_PREFIX + key;
  std::map<storage::DataType, storage::Status> type_status;
  int64_t count = slot->db()->Del({true_key}, &type_status);
  storage::Status s = type_status[storage::DataType::kHashes];
  return s;
}

storage::Status StreamStorage::GetTreeNodeValue(const treeID tid, std::string &field, std::string &value,
                                                const Slot *slot) {
  assert(slot);
  auto key = std::move(StreamUtils::TreeID2Key(tid));
  storage::Status s;
  s = slot->db()->HGet(key, field, &value);
  return s;
}

storage::Status StreamStorage::InsertTreeNodeValue(const treeID tid, const std::string &filed, const std::string &value,
                                                   const Slot *slot) {
  assert(slot);
  auto key = std::move(StreamUtils::TreeID2Key(tid));

  storage::Status s;
  int res;
  s = slot->db()->HSet(key, filed, value, &res);
  (void)res;
  return s;
}

storage::Status StreamStorage::DeleteTreeNode(const treeID tid, const std::string &field, const Slot *slot) {
  assert(slot);
  auto key = std::move(StreamUtils::TreeID2Key(tid));

  storage::Status s;
  int res;
  s = slot->db()->HDel(key, std::vector<std::string>{field}, &res);
  (void)res;
  return s;
}

storage::Status StreamStorage::GetAllTreeNode(const treeID tid, std::vector<storage::FieldValue> &field_values,
                                              const Slot *slot) {
  assert(slot);
  auto key = std::move(StreamUtils::TreeID2Key(tid));
  return slot->db()->PKHScanRange(key, "", "", "*", INT_MAX, &field_values, nullptr);
}

storage::Status StreamStorage::DeleteTree(const treeID tid, const Slot *slot) {
  assert(slot);
  assert(tid != kINVALID_TREE_ID);
  storage::Status s;
  auto key = std::move(StreamUtils::TreeID2Key(tid));
  std::map<storage::DataType, storage::Status> type_status;
  int64_t count = slot->db()->Del({key}, &type_status);
  s = type_status[storage::DataType::kStrings];
  return s;
}

storage::Status StreamStorage::CreateConsumer(treeID consumer_tid, std::string &consumername, const Slot *slot) {
  assert(slot);
  std::string consumer_meta_value;
  auto s = StreamStorage::GetTreeNodeValue(consumer_tid, consumername, consumer_meta_value, slot);
  if (s.IsNotFound()) {
    treeID pel_tid;
    GET_NEXT_TREE_ID_AND_CHECK(slot, pel_tid);

    StreamConsumerMetaValue consumer_meta;
    consumer_meta.Init(pel_tid);
    s = StreamStorage::InsertTreeNodeValue(consumer_tid, consumername, consumer_meta.value(), slot);
    if (!s.ok()) {
      LOG(ERROR) << "Insert consumer meta failed";
      return s;
    }
  }
  // consumer meta already exists or other error
  return s;
}

storage::Status StreamStorage::GetOrCreateConsumer(treeID consumer_tid, std::string &consumername, const Slot *slot,
                                                   StreamConsumerMetaValue &consumer_meta) {
  assert(slot);
  std::string consumer_meta_value;
  auto s = StreamStorage::GetTreeNodeValue(consumer_tid, consumername, consumer_meta_value, slot);
  if (s.ok()) {
    consumer_meta.ParseFrom(consumer_meta_value);

  } else if (s.IsNotFound()) {
    treeID pel_tid;
    GET_NEXT_TREE_ID_AND_CHECK(slot, pel_tid);
    consumer_meta.Init(pel_tid);
    return StreamStorage::InsertTreeNodeValue(consumer_tid, consumername, consumer_meta.value(), slot);
  }

  return s;
}

bool StreamUtils::StreamGenericParseID(const std::string &var, streamID &id, uint64_t missing_seq, bool strict,
                                       bool *seq_given) {
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
  char *dot = strchr(buf, '-');
  if (dot) {
    *dot = '\0';
  }
  if (!StreamUtils::string2uint64(buf, ms)) {
    return false;
  };
  if (dot) {
    size_t seqlen = strlen(dot + 1);
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

bool StreamUtils::StreamParseID(const std::string &var, streamID &id, uint64_t missing_seq) {
  return StreamGenericParseID(var, id, missing_seq, false, nullptr);
}

bool StreamUtils::StreamParseStrictID(const std::string &var, streamID &id, uint64_t missing_seq, bool *seq_given) {
  return StreamGenericParseID(var, id, missing_seq, true, seq_given);
}

bool StreamUtils::StreamParseIntervalId(const std::string &var, streamID &id, bool *exclude, uint64_t missing_seq) {
  if (exclude != nullptr) {
    *exclude = (var.size() > 1 && var[0] == '(');
  }
  if (exclude != nullptr && *exclude) {
    return StreamParseStrictID(var.substr(1), id, missing_seq, nullptr);
  } else {
    return StreamParseID(var, id, missing_seq);
  }
}

storage::Status StreamStorage::TrimByMaxlen(TrimRet &trim_ret, StreamMetaValue &stream_meta, const std::string &key,
                                            const Slot *slot, const StreamAddTrimArgs &args) {
  assert(slot);
  storage::Status s;
  // we delete the message in batchs, prevent from using too much memory
  while (stream_meta.length() - trim_ret.count > args.maxlen) {
    auto cur_batch =
        (std::min(static_cast<int32_t>(stream_meta.length() - trim_ret.count - args.maxlen), kDEFAULT_TRIM_BATCH_SIZE));
    std::vector<storage::FieldValue> filed_values;

    StreamStorage::ScanStreamOptions options(key, stream_meta.first_id(), kSTREAMID_MAX, cur_batch, false, false,
                                             false);
    s = StreamStorage::ScanStream(options, filed_values, trim_ret.next_field, slot);
    if (!s.ok() && !s.IsNotFound()) {
      return s;
    }

    assert(filed_values.size() == cur_batch);
    trim_ret.count += cur_batch;
    trim_ret.max_deleted_field = filed_values.back().field;

    // delete the message in batchs
    std::vector<std::string> fields_to_del;
    fields_to_del.reserve(filed_values.size());
    for (auto &fv : filed_values) {
      fields_to_del.emplace_back(std::move(fv.field));
    }
    int32_t ret;
    s = StreamStorage::DeleteStreamMessage(key, fields_to_del, ret, slot);
    if (!s.ok()) {
      return s;
    }
    assert(ret == fields_to_del.size());
  }

  s = storage::Status::OK();
  return s;
}

storage::Status StreamStorage::TrimByMinid(TrimRet &trim_ret, StreamMetaValue &stream_meta, const std::string &key,
                                           const Slot *slot, const StreamAddTrimArgs &args) {
  assert(slot);
  storage::Status s;
  std::string serialized_min_id;
  stream_meta.first_id().SerializeTo(trim_ret.next_field);
  args.minid.SerializeTo(serialized_min_id);

  // we delete the message in batchs, prevent from using too much memory
  while (trim_ret.next_field < serialized_min_id && stream_meta.length() - trim_ret.count > 0) {
    auto cur_batch = static_cast<int32_t>(std::min(static_cast<int32_t>(stream_meta.length() - trim_ret.count), kDEFAULT_TRIM_BATCH_SIZE));
    std::vector<storage::FieldValue> filed_values;

    StreamStorage::ScanStreamOptions options(key, stream_meta.first_id(), args.minid, cur_batch, false, false, false);
    s = StreamStorage::ScanStream(options, filed_values, trim_ret.next_field, slot);
    if (!s.ok() && !s.IsNotFound()) {
      return s;
    }

    if (!filed_values.empty()) {
      if (filed_values.back().field == serialized_min_id) {
        // we do not need to delete the message that it's id matches the minid
        filed_values.pop_back();
        trim_ret.next_field = serialized_min_id;
      }
      // duble check
      if (!filed_values.empty()) {
        trim_ret.max_deleted_field = filed_values.back().field;
      }
    }

    assert(filed_values.size() <= cur_batch);
    trim_ret.count += static_cast<int32_t>(filed_values.size());

    // do the delete in batch
    std::vector<std::string> fields_to_del;
    fields_to_del.reserve(filed_values.size());
    for (auto &fv : filed_values) {
      fields_to_del.emplace_back(std::move(fv.field));
    }
    int32_t ret;
    s = StreamStorage::DeleteStreamMessage(key, fields_to_del, ret, slot);
    if (!s.ok()) {
      return s;
    }
    assert(ret == fields_to_del.size());
  }

  s = storage::Status::OK();
  return s;
}

storage::Status StreamStorage::TrimStream(int32_t &count, StreamMetaValue &stream_meta, const std::string &key,
                                          StreamAddTrimArgs &args, const Slot *slot) {
  assert(slot);
  count = 0;
  // 1 do the trim
  TrimRet trim_ret;
  storage::Status s;
  if (args.trim_strategy == StreamTrimStrategy::TRIM_STRATEGY_MAXLEN) {
    s = TrimByMaxlen(trim_ret, stream_meta, key, slot, args);
  } else {
    assert(args.trim_strategy == StreamTrimStrategy::TRIM_STRATEGY_MINID);
    s = TrimByMinid(trim_ret, stream_meta, key, slot, args);
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

  s = storage::Status::OK();
  return s;
}

storage::Status StreamStorage::DestoryStreams(std::vector<std::string> &keys, const Slot *slot) {
  assert(slot);
  storage::Status s;
  for (const auto &key : keys) {
    // 1.try to get the stream meta
    StreamMetaValue stream_meta;
    s = StreamStorage::GetStreamMeta(stream_meta, key, slot);
    if (s.IsNotFound()) {
      continue;
    } else if (!s.ok()) {
      LOG(ERROR) << "Get stream meta failed";
      return s;
    }

    // 2 destroy all the cgroup
    // 2.1 find all the cgroups' meta
    auto cgroup_tid = stream_meta.groups_id();
    if (cgroup_tid != kINVALID_TREE_ID) {
      // Korpse TODO: delete the cgroup, now we don't have groups.
    }

    // 3 delete stream meta
    s = StreamStorage::DeleteStreamMeta(key, slot);
    if (!s.ok()) {
      return s;
    }

    // 4 delete stream data
    s = StreamStorage::DeleteStreamData(key, slot);
    if (!s.ok()) {
      return s;
    }
  }

  s = storage::Status::OK();
  return s;
}

bool StreamUtils::string2uint64(const char *s, uint64_t &value) {
  if (!s || !*s) {
    return false;
  }

  char *end;
  errno = 0;
  uint64_t tmp = strtoull(s, &end, 10);
  if (*end || errno == ERANGE) {
    // Conversion either didn't consume the entire string, or overflow occurred
    return false;
  }

  value = tmp;
  return true;
}

bool StreamUtils::string2int64(const char *s, int64_t &value) {
  if (!s || !*s) {
    return false;
  }

  char *end;
  errno = 0;
  int64_t tmp = std::strtoll(s, &end, 10);
  if (*end || errno == ERANGE) {
    // Conversion either didn't consume the entire string, or overflow occurred
    return false;
  }

  value = tmp;
  return true;
}

bool StreamUtils::string2int32(const char *s, int32_t &value) {
  if (!s || !*s) {
    return false;
  }

  char *end;
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

std::string StreamUtils::TreeID2Key(const treeID &tid) {
  std::string key;
  key.reserve(STERAM_TREE_PREFIX.size() + sizeof(tid));
  key.append(STERAM_TREE_PREFIX);
  key.append(reinterpret_cast<const char *>(&tid), sizeof(tid));
  return key;
}

bool StreamUtils::SerializeMessage(const std::vector<std::string> &field_values, std::string &message, int field_pos) {
  assert(field_values.size() - field_pos >= 2 && (field_values.size() - field_pos) % 2 == 0);
  assert(message.empty());
  // count the size of serizlized message
  size_t size = 0;
  for (int i = field_pos; i < field_values.size(); i++) {
    size += field_values[i].size() + sizeof(size_t);
  }
  message.reserve(size);

  // serialize message
  for (int i = field_pos; i < field_values.size(); i++) {
    size_t len = field_values[i].size();
    message.append(reinterpret_cast<const char *>(&len), sizeof(len));
    message.append(field_values[i]);
  }

  return true;
}

bool StreamUtils::DeserializeMessage(const std::string &message, std::vector<std::string> &parsed_message) {
  size_t pos = 0;
  while (pos < message.size()) {
    // Read the length of the next field value from the message
    size_t len = *reinterpret_cast<const size_t *>(&message[pos]);
    pos += sizeof(size_t);

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
