//  Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <cstdint>
#include "glog/logging.h"
#include "pika_stream_types.h"
#include "src/coding.h"
#include "storage/storage.h"

namespace storage {

static const uint64_t kDefaultStreamValueLength =
    sizeof(tree_id_t) + sizeof(uint64_t) + 3 * sizeof(streamID) + sizeof(int32_t) + sizeof(uint64_t);
class StreamMetaValue {
 public:
  explicit StreamMetaValue() = default;
  // used only when create a new stream
  void InitMetaValue() {
    groups_id_ = kINVALID_TREE_ID;
    entries_added_ = 0;
    first_id_ = streamID();
    last_id_ = streamID();
    max_deleted_entry_id_ = streamID();
    length_ = 0;

    // We do not reset version_ here, because we want to keep the version of the old stream meta.
    // Each time we delete a stream, we will increase the version of the stream meta, so that the old stream date will
    // not be seen by the new stream with the same key.
    ++version_;

    uint64_t needed = kDefaultStreamValueLength;
    value_.resize(needed);

    char* dst = &value_[0];

    // Encode each member into the string
    EncodeFixed64(dst, groups_id_);
    dst += sizeof(tree_id_t);

    EncodeFixed64(dst, entries_added_);
    dst += sizeof(uint64_t);

    EncodeFixed64(dst, first_id_.ms);
    dst += sizeof(uint64_t);
    EncodeFixed64(dst, first_id_.seq);
    dst += sizeof(uint64_t);

    EncodeFixed64(dst, last_id_.ms);
    dst += sizeof(uint64_t);
    EncodeFixed64(dst, last_id_.seq);
    dst += sizeof(uint64_t);

    EncodeFixed64(dst, max_deleted_entry_id_.ms);
    dst += sizeof(uint64_t);
    EncodeFixed64(dst, max_deleted_entry_id_.seq);
    dst += sizeof(uint64_t);

    EncodeFixed32(dst, length_);
    dst += sizeof(length_);

    EncodeFixed64(dst, version_);
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
    char* pos = &value_[0];
    groups_id_ = DecodeFixed32(pos);
    pos += sizeof(tree_id_t);

    entries_added_ = DecodeFixed64(pos);
    pos += sizeof(uint64_t);

    first_id_.ms = DecodeFixed64(pos);
    pos += sizeof(uint64_t);
    first_id_.seq = DecodeFixed64(pos);
    pos += sizeof(uint64_t);

    last_id_.ms = DecodeFixed64(pos);
    pos += sizeof(uint64_t);
    last_id_.seq = DecodeFixed64(pos);
    pos += sizeof(uint64_t);

    max_deleted_entry_id_.ms = DecodeFixed64(pos);
    pos += sizeof(uint64_t);
    max_deleted_entry_id_.seq = DecodeFixed64(pos);
    pos += sizeof(uint64_t);

    length_ = static_cast<int32_t>(DecodeFixed32(pos));
    pos += sizeof(length_);

    version_ = static_cast<uint64_t>(DecodeFixed64(pos));
  }

  uint64_t version() const { return version_; }

  tree_id_t groups_id() const { return groups_id_; }

  uint64_t entries_added() const { return entries_added_; }

  void ModifyEntriesAdded(uint64_t delta) { set_entries_added(entries_added_ + delta); }

  streamID first_id() const { return first_id_; }

  streamID last_id() const { return last_id_; }

  streamID max_deleted_entry_id() const { return max_deleted_entry_id_; }

  int32_t length() const { return length_; }

  std::string& value() { return value_; }

  std::string ToString() {
    return "stream_meta: " + std::string("groups_id: ") + std::to_string(groups_id_) +
           std::string(", entries_added: ") + std::to_string(entries_added_) + std::string(", first_id: ") +
           first_id_.ToString() + std::string(", last_id: ") + last_id_.ToString() +
           std::string(", max_deleted_entry_id: ") + max_deleted_entry_id_.ToString() + std::string(", length: ") +
           std::to_string(length_) + std::string(", version: ") + std::to_string(version_);
  }

  void set_groups_id(tree_id_t groups_id) {
    assert(value_.size() == kDefaultStreamValueLength);
    groups_id_ = groups_id;
    char* dst = const_cast<char*>(value_.data());
    EncodeFixed32(dst, groups_id_);
  }

  void set_entries_added(uint64_t entries_added) {
    assert(value_.size() == kDefaultStreamValueLength);
    entries_added_ = entries_added;
    char* dst = const_cast<char*>(value_.data()) + sizeof(tree_id_t);
    EncodeFixed64(dst, entries_added_);
  }

  void set_first_id(streamID first_id) {
    assert(value_.size() == kDefaultStreamValueLength);
    first_id_ = first_id;
    char* dst = const_cast<char*>(value_.data()) + sizeof(tree_id_t) + sizeof(uint64_t);
    EncodeFixed64(dst, first_id_.ms);
    dst += sizeof(uint64_t);
    EncodeFixed64(dst, first_id_.seq);
  }

  void set_last_id(streamID last_id) {
    assert(value_.size() == kDefaultStreamValueLength);
    last_id_ = last_id;
    char* dst = const_cast<char*>(value_.data()) + sizeof(tree_id_t) + sizeof(uint64_t) + sizeof(streamID);
    EncodeFixed64(dst, last_id_.ms);
    dst += sizeof(uint64_t);
    EncodeFixed64(dst, last_id_.seq);
  }

  void set_max_deleted_entry_id(streamID max_deleted_entry_id) {
    assert(value_.size() == kDefaultStreamValueLength);
    max_deleted_entry_id_ = max_deleted_entry_id;
    char* dst = const_cast<char*>(value_.data()) + sizeof(tree_id_t) + sizeof(uint64_t) + 2 * sizeof(streamID);
    EncodeFixed64(dst, max_deleted_entry_id_.ms);
    dst += sizeof(uint64_t);
    EncodeFixed64(dst, max_deleted_entry_id_.seq);
  }

  void set_length(int32_t length) {
    assert(value_.size() == kDefaultStreamValueLength);
    length_ = length;
    char* dst = const_cast<char*>(value_.data()) + sizeof(tree_id_t) + sizeof(uint64_t) + 3 * sizeof(streamID);
    EncodeFixed32(dst, length_);
  }

  void set_version(uint64_t version) {
    assert(value_.size() == kDefaultStreamValueLength);
    version_ = version;
    char* dst =
        const_cast<char*>(value_.data()) + sizeof(tree_id_t) + sizeof(uint64_t) + 3 * sizeof(streamID) + sizeof(length_);
    EncodeFixed64(dst, version_);
  }

 private:
  tree_id_t groups_id_ = kINVALID_TREE_ID;
  uint64_t entries_added_{0};
  streamID first_id_;
  streamID last_id_;
  streamID max_deleted_entry_id_;
  int32_t length_{0};  // number of the messages in the stream
  uint64_t version_{0};

  std::string value_{};
};

// Used only for reading !
class ParsedStreamMetaValue {
 public:
  ParsedStreamMetaValue(const Slice& value) {
    assert(value.size() == kDefaultStreamValueLength);
    if (value.size() != kDefaultStreamValueLength) {
      LOG(ERROR) << "Invalid stream meta value length: ";
      return;
    }
    char* pos = const_cast<char*>(value.data());
    groups_id_ = DecodeFixed32(pos);
    pos += sizeof(tree_id_t);

    entries_added_ = DecodeFixed64(pos);
    pos += sizeof(uint64_t);

    first_id_.ms = DecodeFixed64(pos);
    pos += sizeof(uint64_t);
    first_id_.seq = DecodeFixed64(pos);
    pos += sizeof(uint64_t);

    last_id_.ms = DecodeFixed64(pos);
    pos += sizeof(uint64_t);
    last_id_.seq = DecodeFixed64(pos);
    pos += sizeof(uint64_t);

    max_deleted_entry_id_.ms = DecodeFixed64(pos);
    pos += sizeof(uint64_t);
    max_deleted_entry_id_.seq = DecodeFixed64(pos);
    pos += sizeof(uint64_t);

    length_ = static_cast<int32_t>(DecodeFixed32(pos));
    pos += sizeof(length_);

    version_ = static_cast<uint64_t>(DecodeFixed64(pos));
  }

  uint64_t version() const { return version_; }

  tree_id_t groups_id() const { return groups_id_; }

  uint64_t entries_added() const { return entries_added_; }

  streamID first_id() const { return first_id_; }

  streamID last_id() const { return last_id_; }

  streamID max_deleted_entry_id() const { return max_deleted_entry_id_; }

  int32_t length() const { return length_; }

  std::string ToString() {
    return "stream_meta: " + std::string("groups_id: ") + std::to_string(groups_id_) +
           std::string(", entries_added: ") + std::to_string(entries_added_) + std::string(", first_id: ") +
           first_id_.ToString() + std::string(", last_id: ") + last_id_.ToString() +
           std::string(", max_deleted_entry_id: ") + max_deleted_entry_id_.ToString() + std::string(", length: ") +
           std::to_string(length_) + std::string(", version: ") + std::to_string(version_);
  }

 private:
  tree_id_t groups_id_ = kINVALID_TREE_ID;
  uint64_t entries_added_{0};
  streamID first_id_;
  streamID last_id_;
  streamID max_deleted_entry_id_;
  int32_t length_{0};  // number of the messages in the stream
  uint64_t version_{0};
};

static const uint64_t kDefaultStreamCGroupValueLength = sizeof(streamID) + sizeof(uint64_t) + 2 * sizeof(tree_id_t);
class StreamCGroupMetaValue {
 public:
  explicit StreamCGroupMetaValue() = default;

  // tid and consumers should be set at beginning
  void Init(tree_id_t tid, tree_id_t consumers) {
    pel_ = tid;
    consumers_ = consumers;
    uint64_t needed = kDefaultStreamCGroupValueLength;
    assert(value_.size() == 0);
    if (value_.size() != 0) {
      LOG(FATAL) << "Init on a existed stream cgroup meta value!";
      return;
    }
    value_.resize(needed);

    char* dst = &value_[0];

    memcpy(dst, &last_id_, sizeof(streamID));
    dst += sizeof(uint64_t);
    memcpy(dst, &entries_read_, sizeof(uint64_t));
    dst += sizeof(uint64_t);
    memcpy(dst, &pel_, sizeof(tree_id_t));
    dst += sizeof(tree_id_t);
    memcpy(dst, &consumers_, sizeof(tree_id_t));
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
      memcpy(&pel_, pos, sizeof(tree_id_t));
      pos += sizeof(tree_id_t);
      memcpy(&consumers_, pos, sizeof(tree_id_t));
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
  tree_id_t pel() { return pel_; }

  tree_id_t consumers() { return consumers_; }

  std::string& value() { return value_; }

 private:
  std::string value_;

  streamID last_id_;
  uint64_t entries_read_ = 0;
  tree_id_t pel_ = 0;
  tree_id_t consumers_ = 0;
};

static const uint64_t kDefaultStreamConsumerValueLength = sizeof(stream_ms_t) * 2 + sizeof(tree_id_t);
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
      memcpy(&pel_, pos, sizeof(tree_id_t));
    }
  }

  void Init(tree_id_t pel) {
    pel_ = pel;
    assert(value_.size() == 0);
    if (value_.size() != 0) {
      LOG(FATAL) << "Invalid stream consumer meta value length: " << value_.size() << " expected: 0";
      return;
    }
    uint64_t needed = kDefaultStreamConsumerValueLength;
    value_.resize(needed);
    char* dst = &value_[0];

    memcpy(dst, &seen_time_, sizeof(stream_ms_t));
    dst += sizeof(stream_ms_t);
    memcpy(dst, &active_time_, sizeof(stream_ms_t));
    dst += sizeof(stream_ms_t);
    memcpy(dst, &pel_, sizeof(tree_id_t));
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
  tree_id_t pel_tid() { return pel_; }

  std::string& value() { return value_; }

 private:
  std::string value_;

  stream_ms_t seen_time_ = 0;
  stream_ms_t active_time_ = 0;
  tree_id_t pel_ = 0;
};

static const uint64_t kDefaultStreamPelMetaValueLength = sizeof(stream_ms_t) + sizeof(uint64_t) + sizeof(tree_id_t);
class StreamPelMeta {
 public:
  // consumer must been set at beginning
  StreamPelMeta() = default;

  void Init(std::string consumer, stream_ms_t delivery_time) {
    consumer_ = std::move(consumer);
    delivery_time_ = delivery_time;
    uint64_t needed = kDefaultStreamPelMetaValueLength;
    assert(value_.size() == 0);
    if (value_.size() != 0) {
      LOG(ERROR) << "Init on a existed stream pel meta value!";
      return;
    }
    value_.resize(needed);
    char* dst = &value_[0];

    memcpy(dst, &delivery_time_, sizeof(stream_ms_t));
    dst += sizeof(stream_ms_t);
    memcpy(dst, &delivery_count_, sizeof(uint64_t));
    dst += sizeof(uint64_t);
    memcpy(dst, &cname_len_, sizeof(uint64_t));
    dst += sizeof(uint64_t);
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
    memcpy(&cname_len_, pos, sizeof(uint64_t));
    pos += sizeof(uint64_t);
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
  uint64_t cname_len_ = 0;
  std::string consumer_;
};

}  // namespace storage
