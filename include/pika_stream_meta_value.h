
#ifndef SRC_STREAM_META_VALUE_FORMAT_H_
#define SRC_STREAM_META_VALUE_FORMAT_H_

#include <sys/types.h>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "include/pika_stream_types.h"

static const size_t kDefaultStreamValueLength = sizeof(treeID) + sizeof(uint64_t) + 3 * sizeof(streamID);

// used when create a new stream
class StreamMetaFiledValue {
 public:
  explicit StreamMetaFiledValue(treeID group_id) : groups_id_(group_id) {}

  void Encode() {
    size_t needed = kDefaultStreamValueLength;

    filed_value_.value.resize(needed);
    char* dst = filed_value_.value.data();

    // Encode each member into the string
    memcpy(dst, &groups_id_, sizeof(treeID));
    dst += sizeof(treeID);
    memcpy(dst, &entries_added_, sizeof(uint64_t));
    dst += sizeof(uint64_t);
    memcpy(dst, &first_id_, sizeof(streamID));
    dst += sizeof(streamID);
    memcpy(dst, &last_id_, sizeof(streamID));
    dst += sizeof(streamID);
    memcpy(dst, &max_deleted_entry_id_, sizeof(streamID));

    if (needed <= filed_value_.value.size()) {
      filed_value_.value.resize(needed);
    } else {
      filed_value_.value.resize(needed);
    }
  }

  treeID groups_id() { return groups_id_; }

  void set_groups_id(treeID group_id) { groups_id_ = group_id; }

  uint64_t entries_added() { return entries_added_; }

  void ModifyEntriesAdded(uint64_t delta) { entries_added_ += delta; }

  streamID last_id() { return last_id_; }

  void set_last_id(streamID last_id) { last_id_ = last_id; }

  streamID first_id() { return first_id_; }

  void set_first_id(streamID first_id) { first_id_ = first_id; }

  streamID max_deleted_entry_id() { return max_deleted_entry_id_; }

  void set_max_deleted_entry_id(streamID max_deleted_entry_id) { max_deleted_entry_id_ = max_deleted_entry_id; }

  void set_stream_key(std::string key) { filed_value_.field = std::move(key); }

 private:
  treeID groups_id_ = 0;
  uint64_t entries_added_ = 0;
  streamID first_id_;
  streamID last_id_;
  streamID max_deleted_entry_id_;

  storage::FieldValue filed_value_;
};

// used when reading a stream meta value
class ParsedStreamMetaFiledValue {
 public:
  // Use this constructor after rocksdb::DB::Get();
  explicit ParsedStreamMetaFiledValue(std::string* internal_value_str) : value_(internal_value_str) {
    assert(internal_value_str->size() == kDefaultStreamValueLength);
    if (internal_value_str->size() == kDefaultStreamValueLength) {
      char* pos = const_cast<char*>(internal_value_str->data());
      memcpy(&groups_id_, pos, sizeof(treeID));
      pos += sizeof(treeID);
      memcpy(&entries_added_, pos, sizeof(uint64_t));
      pos += sizeof(uint64_t);
      memcpy(&first_id_, pos, sizeof(streamID));
      pos += sizeof(streamID);
      memcpy(&last_id_, pos, sizeof(streamID));
      pos += sizeof(streamID);
      memcpy(&max_deleted_entry_id_, pos, sizeof(streamID));
    }
  }

  treeID groups_id() { return groups_id_; }

  uint64_t entries_added() { return entries_added_; }

  void ModifyEntriesAdded(uint64_t delta) { set_entries_added(entries_added_ + delta); }

  streamID first_id() { return first_id_; }

  streamID last_id() { return last_id_; }

  void set_groups_id(treeID groups_id) {
    groups_id_ = groups_id;
    if (value_) {
      char* dst = const_cast<char*>(value_->data());
      memcpy(dst, &groups_id_, sizeof(uint32_t));
    }
  }

  void set_entries_added(uint64_t entries_added) {
    entries_added_ += entries_added;
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + sizeof(uint32_t);
      memcpy(dst, &entries_added_, sizeof(uint64_t));
    }
  }

  void set_first_id(streamID first_id) {
    first_id_ = first_id;
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + sizeof(uint32_t) + sizeof(uint64_t);
      memcpy(dst, &first_id_.ms, sizeof(uint64_t));
      dst = const_cast<char*>(value_->data()) + sizeof(uint32_t) + 2 * sizeof(uint64_t);
      memcpy(dst, &first_id_.seq, sizeof(uint64_t));
    }
  }

  void set_last_id(streamID last_id) {
    last_id_ = last_id;
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + sizeof(uint32_t) + 3 * sizeof(uint64_t);
      memcpy(dst, &last_id_.ms, sizeof(uint64_t));
      dst = const_cast<char*>(value_->data()) + sizeof(uint32_t) + 4 * sizeof(uint64_t);
      memcpy(dst, &last_id_.seq, sizeof(uint64_t));
    }
  }

  streamID max_deleted_entry_id() { return max_deleted_entry_id_; }

  void set_max_deleted_entry_id(streamID max_deleted_entry_id) { max_deleted_entry_id_ = max_deleted_entry_id; }

 private:
  treeID groups_id_ = 0;
  uint64_t entries_added_ = 0;
  streamID first_id_;
  streamID last_id_;
  streamID max_deleted_entry_id_;

  std::string* value_;
};

#endif  //  SRC_STREAM_META_VALUE_FORMAT_H_
