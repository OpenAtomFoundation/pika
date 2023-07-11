#ifndef SRC_STREAM_CGROUP_META_VALUE_FORMAT_H_
#define SRC_STREAM_CGROUP_META_VALUE_FORMAT_H_

#include <cstdint>
#include "include/pika_stream_types.h"

static const size_t kDefaultStreamCGroupValueLength = sizeof(streamID) + sizeof(uint64_t) + 2 * sizeof(treeID);

class StreamCGroupMetaFiledValue {
 public:
  // pel and consumers must been set at beginning
  StreamCGroupMetaFiledValue(treeID pel, treeID consumers) : pel_(pel), consumers_(consumers) {}

  void Encode() {
    size_t needed = kDefaultStreamCGroupValueLength;
    filed_value_.value.resize(needed);
    auto dst = filed_value_.value.data();

    memcpy(dst, &last_id_, sizeof(streamID));
    dst += sizeof(uint64_t);
    memcpy(dst, &entries_read_, sizeof(uint64_t));
    dst += sizeof(uint64_t);
    memcpy(dst, &pel_, sizeof(treeID));
    dst += sizeof(treeID);
    memcpy(dst, &consumers_, sizeof(treeID));
  }

  streamID last_id() { return last_id_; }

  void set_last_id(streamID last_id) { last_id_ = last_id; }

  uint64_t entries_read() { return entries_read_; }

  void set_entries_read(uint64_t entries_read) { entries_read_ = entries_read; }

  // pel and consumers were set in constructor,  can't be modified
  treeID pel() { return pel_; }

  treeID consumers() { return consumers_; }

 private:
  storage::FieldValue filed_value_;

  streamID last_id_;
  uint64_t entries_read_ = 0;
  treeID pel_ = 0;
  treeID consumers_ = 0;
};

class ParsedStreamCGroupMetaValue {
 public:
  explicit ParsedStreamCGroupMetaValue(std::string* internal_value_str) : value_(internal_value_str) {
    assert(internal_value_str->size() == kDefaultStreamCGroupValueLength);
    if (internal_value_str->size() == kDefaultStreamCGroupValueLength) {
      auto pos = value_->data();
      memcpy(&last_id_, pos, sizeof(streamID));
      pos += sizeof(streamID);
      memcpy(&entries_read_, pos, sizeof(uint64_t));
      pos += sizeof(uint64_t);
      memcpy(&pel_, pos, sizeof(treeID));
      pos += sizeof(treeID);
      memcpy(&consumers_, pos, sizeof(treeID));
    }
  }

  void SetLastIdToValue() {
    if (value_) {
      char* dst = const_cast<char*>(value_->data());
      memcpy(dst, &last_id_, sizeof(streamID));
    }
  }

  void set_last_id(streamID last_id) {
    last_id_ = last_id;
    SetLastIdToValue();
  }

  void SetEntriesReadToValue() {
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + sizeof(streamID);
      memcpy(dst, &entries_read_, sizeof(uint64_t));
    }
  }

  void ModifyEntriesAdded(int64_t delta) {
    entries_read_ += delta;
    SetEntriesReadToValue();
  }

 private:
  std::string* value_ = nullptr;

  streamID last_id_;
  uint64_t entries_read_ = 0;
  treeID pel_ = 0;
  treeID consumers_ = 0;
};

#endif  //  SRC_STREAM_CGROUP_META_VALUE_FORMAT_H_