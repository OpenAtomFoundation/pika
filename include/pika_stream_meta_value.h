#ifndef SRC_STREAM_META_VALUE_FORMAT_H_
#define SRC_STREAM_META_VALUE_FORMAT_H_

#include <sys/types.h>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "include/pika_stream_types.h"

static const size_t kDefaultStreamValueLength = sizeof(treeID) + sizeof(uint64_t) + 3 * sizeof(streamID);

// used when create a new stream
class StreamMetaValue {
 public:
  // should provie a string
  explicit StreamMetaValue() = default;
  // used only when create a new stream
  void Init() {
    size_t needed = kDefaultStreamValueLength;
    assert(value_.size() == 0);
    if (value_.size() != 0) {
      LOG(ERROR) << "Init on a existed stream meta value!";
      return;
    }
    value_.resize(needed);

    char* dst = value_.data();

    // Encode each member into the string
    memcpy(dst, &groups_id_, sizeof(treeID));
    dst += sizeof(treeID);
    memcpy(dst, &entries_added_, sizeof(size_t));
    dst += sizeof(size_t);
    memcpy(dst, &first_id_, sizeof(streamID));
    dst += sizeof(streamID);
    memcpy(dst, &last_id_, sizeof(streamID));
    dst += sizeof(streamID);
    memcpy(dst, &max_deleted_entry_id_, sizeof(streamID));
    dst += sizeof(length_);
    memcpy(dst, &length_, sizeof(size_t));
  }

  // used only when parse a existed stream meta
  // value_ = std::move(value);
  void ParseFrom(std::string& value) {
    value_ = std::move(value);
    assert(value_.size() == kDefaultStreamValueLength);
    if (value_.size() != kDefaultStreamValueLength) {
      LOG(ERROR) << "Invalid stream meta value length: ";
      return;
    }
    char* pos = value_.data();
    memcpy(&groups_id_, pos, sizeof(treeID));
    pos += sizeof(treeID);
    memcpy(&entries_added_, pos, sizeof(size_t));
    pos += sizeof(size_t);
    memcpy(&first_id_, pos, sizeof(streamID));
    pos += sizeof(streamID);
    memcpy(&last_id_, pos, sizeof(streamID));
    pos += sizeof(streamID);
    memcpy(&max_deleted_entry_id_, pos, sizeof(streamID));
    pos += sizeof(length_);
    memcpy(&length_, pos, sizeof(size_t));
  }

 const treeID groups_id() { return groups_id_; }

  const size_t entries_added() { return entries_added_; }

  void ModifyEntriesAdded(size_t delta) { set_entries_added(entries_added_ + delta); }

  const streamID first_id() { return first_id_; }

  const streamID last_id() const { return last_id_; }

  const streamID max_deleted_entry_id() { return max_deleted_entry_id_; }

  const size_t length() { return length_; }

  std::string& value() { return value_; }

  std::string ToString() {
    return std::string("groups_id: ") + std::to_string(groups_id_) + std::string(", entries_added: ") +
           std::to_string(entries_added_) + std::string(", first_id: ") + first_id_.ToString() +
           std::string(", last_id: ") + last_id_.ToString() + std::string(", max_deleted_entry_id: ") +
           max_deleted_entry_id_.ToString() + std::string(", length: ") + std::to_string(length_);
  }

  void set_groups_id(treeID groups_id) {
    assert(value_.size() == kDefaultStreamValueLength);
    groups_id_ = groups_id;
    char* dst = const_cast<char*>(value_.data());
    memcpy(dst, &groups_id_, sizeof(treeID));
  }

  void set_entries_added(uint64_t entries_added) {
    assert(value_.size() == kDefaultStreamValueLength);
    entries_added_ += entries_added;
    char* dst = const_cast<char*>(value_.data()) + sizeof(treeID);
    memcpy(dst, &entries_added_, sizeof(uint64_t));
  }

  void set_first_id(streamID first_id) {
    assert(value_.size() == kDefaultStreamValueLength);
    first_id_ = first_id;
    char* dst = const_cast<char*>(value_.data()) + sizeof(treeID) + sizeof(uint64_t);
    memcpy(dst, &first_id_, sizeof(uint64_t));
  }

  void set_last_id(streamID last_id) {
    assert(value_.size() == kDefaultStreamValueLength);
    last_id_ = last_id;
    char* dst = const_cast<char*>(value_.data()) + sizeof(treeID) + sizeof(uint64_t) + sizeof(streamID);
    memcpy(dst, &last_id_, sizeof(streamID));
  }

  void set_max_deleted_entry_id(streamID max_deleted_entry_id) {
    assert(value_.size() == kDefaultStreamValueLength);
    max_deleted_entry_id_ = max_deleted_entry_id;
    char* dst = const_cast<char*>(value_.data()) + sizeof(treeID) + sizeof(uint64_t) + 2 * sizeof(streamID);
    memcpy(dst, &max_deleted_entry_id_, sizeof(streamID));
  }

  void set_length(size_t length) {
    assert(value_.size() == kDefaultStreamValueLength);
    length_ = length;
    char* dst = const_cast<char*>(value_.data()) + sizeof(treeID) + sizeof(uint64_t) + 3 * sizeof(streamID);
    memcpy(dst, &length_, sizeof(size_t));
  }

  void add_length(size_t delta) {
    assert(value_.size() == kDefaultStreamValueLength);
    length_ += delta;
    char* dst = const_cast<char*>(value_.data()) + sizeof(treeID) + sizeof(uint64_t) + 3 * sizeof(streamID);
    memcpy(dst, &length_, sizeof(size_t));
  }

  void sub_length(size_t delta) {
    assert(value_.size() == kDefaultStreamValueLength);
    assert(length_ >= delta);
    length_ -= delta;
    char* dst = const_cast<char*>(value_.data()) + sizeof(treeID) + sizeof(uint64_t) + 3 * sizeof(streamID);
    memcpy(dst, &length_, sizeof(size_t));
  }

 private:
  treeID groups_id_ = kINVALID_TREE_ID;
  size_t entries_added_{0};
  streamID first_id_;
  streamID last_id_;
  streamID max_deleted_entry_id_;
  size_t length_{0}; // number of the messages in the stream

  std::string value_{};
};

#endif  //  SRC_STREAM_META_VALUE_FORMAT_H_
