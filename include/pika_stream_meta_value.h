// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_STREAM_META_VALUE_FORMAT_H_
#define SRC_STREAM_META_VALUE_FORMAT_H_

#include "glog/logging.h"
#include "include/pika_stream_types.h"

static const size_t kDefaultStreamValueLength =
    sizeof(treeID) + sizeof(uint64_t) + 3 * sizeof(streamID) + sizeof(uint64_t);
class StreamMetaValue {
 public:
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
    dst += sizeof(streamID);
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
    pos += sizeof(streamID);
    memcpy(&length_, pos, sizeof(size_t));
  }

  treeID groups_id() const { return groups_id_; }

  size_t entries_added() const { return entries_added_; }

  void ModifyEntriesAdded(size_t delta) { set_entries_added(entries_added_ + delta); }

  streamID first_id() const { return first_id_; }

  streamID last_id() const { return last_id_; }

  streamID max_deleted_entry_id() const { return max_deleted_entry_id_; }

  size_t length() const { return length_; }

  std::string& value() { return value_; }

  std::string ToString() {
    return "stream_meta: " + std::string("groups_id: ") + std::to_string(groups_id_) +
           std::string(", entries_added: ") + std::to_string(entries_added_) + std::string(", first_id: ") +
           first_id_.ToString() + std::string(", last_id: ") + last_id_.ToString() +
           std::string(", max_deleted_entry_id: ") + max_deleted_entry_id_.ToString() + std::string(", length: ") +
           std::to_string(length_);
  }

  void set_groups_id(treeID groups_id) {
    assert(value_.size() == kDefaultStreamValueLength);
    groups_id_ = groups_id;
    char* dst = const_cast<char*>(value_.data());
    memcpy(dst, &groups_id_, sizeof(treeID));
  }

  void set_entries_added(uint64_t entries_added) {
    assert(value_.size() == kDefaultStreamValueLength);
    entries_added_ = entries_added;
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

 private:
  treeID groups_id_ = kINVALID_TREE_ID;
  size_t entries_added_{0};
  streamID first_id_;
  streamID last_id_;
  streamID max_deleted_entry_id_;
  size_t length_{0};  // number of the messages in the stream

  std::string value_{};
};

static const size_t kDefaultStreamCGroupValueLength = sizeof(streamID) + sizeof(uint64_t) + 2 * sizeof(treeID);
class StreamCGroupMetaValue {
 public:
  explicit StreamCGroupMetaValue() = default;

  // tid and consumers should be set at beginning
  void Init(treeID tid, treeID consumers) {
    pel_ = tid;
    consumers_ = consumers;
    size_t needed = kDefaultStreamCGroupValueLength;
    assert(value_.size() == 0);
    if (value_.size() != 0) {
      LOG(FATAL) << "Init on a existed stream cgroup meta value!";
      return;
    }
    value_.resize(needed);

    auto dst = value_.data();

    memcpy(dst, &last_id_, sizeof(streamID));
    dst += sizeof(uint64_t);
    memcpy(dst, &entries_read_, sizeof(uint64_t));
    dst += sizeof(uint64_t);
    memcpy(dst, &pel_, sizeof(treeID));
    dst += sizeof(treeID);
    memcpy(dst, &consumers_, sizeof(treeID));
  }

  void ParseFrom(std::string& value) {
    value_ = std::move(value);
    assert(value_.size() == kDefaultStreamCGroupValueLength);
    if (value_.size() != kDefaultStreamCGroupValueLength) {
      LOG(FATAL) << "Invalid stream cgroup meta value length: ";
      return;
    }
    if (value_.size() == kDefaultStreamCGroupValueLength) {
      auto pos = value_.data();
      memcpy(&last_id_, pos, sizeof(streamID));
      pos += sizeof(streamID);
      memcpy(&entries_read_, pos, sizeof(uint64_t));
      pos += sizeof(uint64_t);
      memcpy(&pel_, pos, sizeof(treeID));
      pos += sizeof(treeID);
      memcpy(&consumers_, pos, sizeof(treeID));
    }
  }

  streamID last_id() { return last_id_; }

  void set_last_id(streamID last_id) {
    assert(value_.size() == kDefaultStreamCGroupValueLength);
    last_id_ = last_id;
    char* dst = const_cast<char*>(value_.data());
    memcpy(dst, &last_id_, sizeof(streamID));
  }

  uint64_t entries_read() { return entries_read_; }

  void set_entries_read(uint64_t entries_read) {
    assert(value_.size() == kDefaultStreamCGroupValueLength);
    entries_read_ = entries_read;
    char* dst = const_cast<char*>(value_.data()) + sizeof(streamID);
    memcpy(dst, &entries_read_, sizeof(uint64_t));
  }

  // pel and consumers were set in constructor,  can't be modified
  treeID pel() { return pel_; }

  treeID consumers() { return consumers_; }

  std::string& value() { return value_; }

 private:
  std::string value_;

  streamID last_id_;
  uint64_t entries_read_ = 0;
  treeID pel_ = 0;
  treeID consumers_ = 0;
};

static const size_t kDefaultStreamConsumerValueLength = sizeof(stream_ms_t) * 2 + sizeof(treeID);
class StreamConsumerMetaValue {
 public:
  // pel must been set at beginning
  StreamConsumerMetaValue() = default;

  void ParseFrom(std::string& value) {
    value_ = std::move(value);
    assert(value_.size() == kDefaultStreamConsumerValueLength);
    if (value_.size() != kDefaultStreamConsumerValueLength) {
      LOG(FATAL) << "Invalid stream consumer meta value length: " << value_.size()
                 << " expected: " << kDefaultStreamConsumerValueLength;
      return;
    }
    if (value_.size() == kDefaultStreamConsumerValueLength) {
      auto pos = value_.data();
      memcpy(&seen_time_, pos, sizeof(stream_ms_t));
      pos += sizeof(stream_ms_t);
      memcpy(&active_time_, pos, sizeof(stream_ms_t));
      pos += sizeof(stream_ms_t);
      memcpy(&pel_, pos, sizeof(treeID));
    }
  }

  void Init(treeID pel) {
    pel_ = pel;
    assert(value_.size() == 0);
    if (value_.size() != 0) {
      LOG(FATAL) << "Invalid stream consumer meta value length: " << value_.size() << " expected: 0";
      return;
    }
    size_t needed = kDefaultStreamConsumerValueLength;
    value_.resize(needed);
    auto dst = value_.data();

    memcpy(dst, &seen_time_, sizeof(stream_ms_t));
    dst += sizeof(stream_ms_t);
    memcpy(dst, &active_time_, sizeof(stream_ms_t));
    dst += sizeof(stream_ms_t);
    memcpy(dst, &pel_, sizeof(treeID));
  }

  stream_ms_t seen_time() { return seen_time_; }

  void set_seen_time(stream_ms_t seen_time) {
    seen_time_ = seen_time;
    assert(value_.size() == kDefaultStreamConsumerValueLength);
    char* dst = const_cast<char*>(value_.data());
    memcpy(dst, &seen_time_, sizeof(stream_ms_t));
  }

  stream_ms_t active_time() { return active_time_; }

  void set_active_time(stream_ms_t active_time) {
    active_time_ = active_time;
    assert(value_.size() == kDefaultStreamConsumerValueLength);
    char* dst = const_cast<char*>(value_.data()) + sizeof(stream_ms_t);
    memcpy(dst, &active_time_, sizeof(stream_ms_t));
  }

  // pel was set in constructor,  can't be modified
  treeID pel_tid() { return pel_; }

  std::string& value() { return value_; }

 private:
  std::string value_;

  stream_ms_t seen_time_ = 0;
  stream_ms_t active_time_ = 0;
  treeID pel_ = 0;
};

static const size_t kDefaultStreamPelMetaValueLength = sizeof(stream_ms_t) + sizeof(uint64_t) + sizeof(treeID);
class StreamPelMeta {
 public:
  // consumer must been set at beginning
  StreamPelMeta() = default;

  void Init(std::string consumer, stream_ms_t delivery_time) {
    consumer_ = std::move(consumer);
    delivery_time_ = delivery_time;
    size_t needed = kDefaultStreamPelMetaValueLength;
    assert(value_.size() == 0);
    if (value_.size() != 0) {
      LOG(ERROR) << "Init on a existed stream pel meta value!";
      return;
    }
    value_.resize(needed);
    char* dst = value_.data();

    memcpy(dst, &delivery_time_, sizeof(stream_ms_t));
    dst += sizeof(stream_ms_t);
    memcpy(dst, &delivery_count_, sizeof(uint64_t));
    dst += sizeof(uint64_t);
    memcpy(dst, &cname_len_, sizeof(size_t));
    dst += sizeof(size_t);
    memcpy(dst, consumer_.data(), cname_len_);
  }

  void ParseFrom(std::string& value) {
    value_ = std::move(value);
    assert(value_.size() == kDefaultStreamPelMetaValueLength);
    if (value_.size() != kDefaultStreamPelMetaValueLength) {
      LOG(ERROR) << "Invalid stream pel meta value length: ";
      return;
    }
    auto pos = value_.data();
    memcpy(&delivery_time_, pos, sizeof(stream_ms_t));
    pos += sizeof(stream_ms_t);
    memcpy(&delivery_count_, pos, sizeof(uint64_t));
    pos += sizeof(uint64_t);
    memcpy(&cname_len_, pos, sizeof(size_t));
    pos += sizeof(size_t);
    consumer_.assign(pos, cname_len_);
  }

  stream_ms_t delivery_time() { return delivery_time_; }

  void set_delivery_time(stream_ms_t delivery_time) {
    assert(value_.size() == kDefaultStreamPelMetaValueLength);
    delivery_time_ = delivery_time;
    char* dst = const_cast<char*>(value_.data());
    memcpy(dst, &delivery_time_, sizeof(stream_ms_t));
  }

  uint64_t delivery_count() { return delivery_count_; }

  void set_delivery_count(uint64_t delivery_count) {
    assert(value_.size() == kDefaultStreamPelMetaValueLength);
    delivery_count_ = delivery_count;
    char* dst = const_cast<char*>(value_.data());
    memcpy(dst + sizeof(stream_ms_t), &delivery_count_, sizeof(uint64_t));
  }

  std::string& consumer() { return consumer_; }

  std::string& value() { return value_; }

 private:
  std::string value_;

  stream_ms_t delivery_time_ = 0;
  uint64_t delivery_count_ = 1;
  size_t cname_len_ = 0;
  std::string consumer_;
};

#endif  //  SRC_STREAM_META_VALUE_FORMAT_H_
