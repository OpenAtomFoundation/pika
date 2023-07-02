
#ifndef SRC_STREAM_META_VALUE_FORMAT_H_
#define SRC_STREAM_META_VALUE_FORMAT_H_

#include <cstddef>
#include <string>

#include "src/base_value_format.h"
#include "src/coding.h"

namespace storage {

/* (From redis): Stream item ID: a 128 bit number composed of a milliseconds time and
 * a sequence counter. IDs generated in the same millisecond (or in a past
 * millisecond if the clock jumped backward) will use the millisecond time
 * of the latest generated ID and an incremented sequence. */
using streamID = struct streamID {
  streamID(uint64_t _ms , uint64_t  _seq) : ms(_ms), seq(_seq) {}
  streamID() = default;
    uint64_t ms = 0;        /* Unix time in milliseconds. */
    uint64_t seq = 0;       /* Sequence number. */
};

// FIXME: Where should I put this statement?
using treeID = uint32_t;


class StreamMetaValue : public InternalValue {
 public:

  //  user value is the length of stream
  // FIXME: should I initialize last_id_ and first_id_ here ？
  explicit StreamMetaValue(const rocksdb::Slice& user_value)
      : InternalValue(user_value) {}

  static const size_t kStreamAdditionalMetaValueLength = sizeof(uint32_t) + sizeof(uint64_t) * 7;
  static const size_t kDefaultValueSuffixLength = sizeof(int32_t) * 2 + kStreamAdditionalMetaValueLength;

  size_t AppendTimestampAndVersion() override {
    size_t usize = user_value_.size();
    char* dst = start_;
    memcpy(dst, user_value_.data(), usize);
    dst += usize;
    EncodeFixed32(dst, version_);
    dst += sizeof(int32_t);
    EncodeFixed32(dst, timestamp_);
    return usize + 2 * sizeof(int32_t);
  }

  virtual size_t AppendStreamMetaValue() {
    char* dst = start_;

    // skip timestamp and version
    dst += user_value_.size() + 2 * sizeof(int32_t);

    EncodeFixed32(dst, groups_id_);
    dst += sizeof(uint32_t);
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

    return kStreamAdditionalMetaValueLength;
  }

  rocksdb::Slice Encode() override {
    size_t usize = user_value_.size();
    size_t needed = usize + kDefaultValueSuffixLength;
    char* dst;
    if (needed <= sizeof(space_)) {
      dst = space_;
    } else {
      dst = new char[needed];
    }
    start_ = dst;
    size_t len = AppendTimestampAndVersion() + AppendStreamMetaValue();
    return rocksdb::Slice(start_, len);
  }

  int32_t UpdateVersion() {
    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    if (version_ >= static_cast<int32_t>(unix_time)) {
      version_++;
    } else {
      version_ = static_cast<int32_t>(unix_time);
    }
    return version_;
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

 private:
  //  uint64_t length_ = 0;
  treeID groups_id_ = 0;
  uint64_t entries_added_ = 0;
  streamID first_id_;
  streamID last_id_;
  streamID max_deleted_entry_id_;
};

class ParsedStreamMetaValue : public ParsedInternalValue {
 public:
  static const size_t kStreamMetaValueSuffixLength = sizeof(int32_t) * 2 + sizeof(uint32_t) + sizeof(uint64_t) * 7;

  // Use this constructor after rocksdb::DB::Get();
  explicit ParsedStreamMetaValue(std::string* internal_value_str)
      : ParsedInternalValue(internal_value_str) {
    assert(internal_value_str->size() >= kStreamMetaValueSuffixLength);
    if (internal_value_str->size() >= kStreamMetaValueSuffixLength) {
      user_value_ = rocksdb::Slice(internal_value_str->data(), internal_value_str->size() - kStreamMetaValueSuffixLength);
      version_ = DecodeFixed32(internal_value_str->data() + internal_value_str->size() - kStreamMetaValueSuffixLength);
      timestamp_ = DecodeFixed32(internal_value_str->data() + internal_value_str->size() - sizeof(uint32_t) -
                                 sizeof(uint32_t) - sizeof(uint64_t) * 7);

      groups_id_ = DecodeFixed32(internal_value_str->data() + internal_value_str->size() -
                                 sizeof(uint32_t) - sizeof(uint64_t) * 7);
      entries_added_ = DecodeFixed64(internal_value_str->data() + internal_value_str->size() - sizeof(uint64_t) * 7);
      first_id_.ms = DecodeFixed64(internal_value_str->data() + internal_value_str->size() - sizeof(uint64_t) * 6);
      first_id_.seq = DecodeFixed64(internal_value_str->data() + internal_value_str->size() - sizeof(uint64_t) * 5);
      last_id_.ms = DecodeFixed64(internal_value_str->data() + internal_value_str->size() - sizeof(uint64_t) * 4);
      last_id_.seq = DecodeFixed64(internal_value_str->data() + internal_value_str->size() - sizeof(uint64_t) * 3);
      max_deleted_entry_id_.ms = DecodeFixed64(internal_value_str->data() + internal_value_str->size() - sizeof(uint64_t) * 2);
      max_deleted_entry_id_.seq = DecodeFixed64(internal_value_str->data() + internal_value_str->size() - sizeof(uint64_t));
    }
    stream_size_ = DecodeFixed64(internal_value_str->data());
  }

  // Use this constructor in rocksdb::CompactionFilter::Filter();
  explicit ParsedStreamMetaValue(const rocksdb::Slice& internal_value_slice)
      : ParsedInternalValue(internal_value_slice) {
    assert(internal_value_slice.size() >= kStreamMetaValueSuffixLength);
    if (internal_value_slice.size() >= kStreamMetaValueSuffixLength) {
      user_value_ = rocksdb::Slice(internal_value_slice.data(), internal_value_slice.size() - kStreamMetaValueSuffixLength);
      version_ = DecodeFixed32(internal_value_slice.data() + internal_value_slice.size() - sizeof(uint32_t) * 2 -
                               sizeof(uint32_t) - sizeof(uint64_t) * 7);
      timestamp_ = DecodeFixed32(internal_value_slice.data() + internal_value_slice.size() - sizeof(uint32_t) -
                                 sizeof(uint32_t) - sizeof(uint64_t) * 7);

      groups_id_ = DecodeFixed32(internal_value_slice.data() + internal_value_slice.size() -
                                 sizeof(uint32_t) - sizeof(uint64_t) * 7);
      entries_added_ = DecodeFixed64(internal_value_slice.data() + internal_value_slice.size() - sizeof(uint64_t) * 7);
      first_id_.ms = DecodeFixed64(internal_value_slice.data() + internal_value_slice.size() - sizeof(uint64_t) * 6);
      first_id_.seq = DecodeFixed64(internal_value_slice.data() + internal_value_slice.size() - sizeof(uint64_t) * 5);
      last_id_.ms = DecodeFixed64(internal_value_slice.data() + internal_value_slice.size() - sizeof(uint64_t) * 4);
      last_id_.seq = DecodeFixed64(internal_value_slice.data() + internal_value_slice.size() - sizeof(uint64_t) * 3);
      max_deleted_entry_id_.ms = DecodeFixed64(internal_value_slice.data() + internal_value_slice.size() - sizeof(uint64_t) * 2);
      max_deleted_entry_id_.seq = DecodeFixed64(internal_value_slice.data() + internal_value_slice.size() - sizeof(uint64_t));
    }
    stream_size_ = DecodeFixed64(internal_value_slice.data());
  }

  void StripSuffix() override {
    if (value_) {
      value_->erase(value_->size() - kStreamMetaValueSuffixLength, kStreamMetaValueSuffixLength);
    }
  }

  void SetVersionToValue() override {
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - kStreamMetaValueSuffixLength;
      EncodeFixed32(dst, version_);
    }
  }

  void SetTimestampToValue() override {
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - kStreamMetaValueSuffixLength + sizeof(int32_t);
      EncodeFixed32(dst, timestamp_);
    }
  }

  int32_t InitialMetaValue() {
    this->set_stream_size(0);
    this->set_groups_id(0);
    this->set_entries_added(0);
    this->set_first_id(streamID(0, 0));
    this->set_last_id(streamID(0, 0));
    this->set_max_deleted_entry_id(streamID(0, 0));

    this->set_timestamp(0);
    return this->UpdateVersion();
  }

  uint64_t stream_size() { return stream_size_; }

  void set_stream_size(uint64_t stream_size) {
    stream_size_ = stream_size;
    if (value_) {
      char* dst = const_cast<char*>(value_->data());
      EncodeFixed64(dst, stream_size_);
    }
  }

  void ModifyStreamSize(uint64_t delta) {
    stream_size_ += delta;
    if (value_) {
      char* dst = const_cast<char*>(value_->data());
      EncodeFixed64(dst, stream_size_);
    }
  }

  int32_t UpdateVersion() {
    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    if (version_ >= static_cast<int32_t>(unix_time)) {
      version_++;
    } else {
      version_ = static_cast<int32_t>(unix_time);
    }
    SetVersionToValue();
    return version_;
  }

  treeID  groups_id() { return groups_id_; }

  void set_groups_id(treeID groups_id) {
    groups_id_ = groups_id;
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - sizeof(uint32_t) - sizeof(uint64_t) * 7;
      EncodeFixed32(dst, groups_id_);
    }
  }

  uint64_t entries_added() { return entries_added_; }

  void set_entries_added(uint64_t entries_added) {
    entries_added_ += entries_added;
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - sizeof(uint64_t) * 7;
      EncodeFixed32(dst, entries_added_);
    }
  }

  void ModifyEntriesAdded(uint64_t delta) {
    set_entries_added(entries_added_ + delta);
  }

  streamID first_id() { return first_id_; }

  void set_first_id(streamID first_id) {
    first_id_ = first_id;
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - sizeof(int64_t) * 6;
      EncodeFixed64(dst, first_id_.ms);
      dst = const_cast<char*>(value_->data()) + value_->size() - sizeof(int64_t) * 5;
      EncodeFixed64(dst, first_id_.seq);
    }
  }

  streamID last_id() { return last_id_; }

  void set_last_id(streamID last_id) {
    last_id_ = last_id;
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - sizeof(int64_t) * 4;
      EncodeFixed64(dst, last_id_.ms);
      dst = const_cast<char*>(value_->data()) + value_->size() - sizeof(int64_t) * 3;
      EncodeFixed64(dst, last_id_.seq);
    }
  }

  streamID max_deleted_entry_id() { return max_deleted_entry_id_; }

  void set_max_deleted_entry_id(streamID max_deleted_entry_id) { max_deleted_entry_id_ = max_deleted_entry_id; }

 private:
  uint64_t stream_size_ = 0;
  treeID groups_id_ = 0;
  uint64_t entries_added_ = 0;
  streamID first_id_;
  streamID last_id_;
  streamID max_deleted_entry_id_;
};

}  //  namespace storage
#endif  //  SRC_STREAM_META_VALUE_FORMAT_H_
