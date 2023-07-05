#ifndef SRC_STREAM_CGROUP_META_VALUE_FORMAT_H_
#define SRC_STREAM_CGROUP_META_VALUE_FORMAT_H_

#include <cstdint>
#include "src/stream_meta_value.h"

namespace storage {
// streamID last_id
// long long entries_read
// treeID pel //  pel kv树的前缀
// treeID consumers // 消费者 kv树的前缀
class StreamCGroupMetaValue {
 public:
  // pel and consumers must been set at beginning
  StreamCGroupMetaValue(treeID pel, treeID consumers) : pel_(pel), consumers_(consumers) {}

  rocksdb::Slice Encode() {
    size_t needed = sizeof(streamID) + sizeof(uint64_t) + sizeof(treeID) * 2;
    char* dst;
    if (needed <= sizeof(space_)) {
      dst = space_;
    } else {
      dst = new char[needed];
    }
    start_ = dst;
    EncodeFixed64(dst, last_id_.ms);
    dst += sizeof(uint64_t);
    EncodeFixed64(dst, last_id_.seq);
    dst += sizeof(uint64_t);
    EncodeFixed64(dst, entries_read_);
    dst += sizeof(uint64_t);
    EncodeFixed32(dst, pel_);
    dst += sizeof(treeID);
    EncodeFixed32(dst, consumers_);
    return {start_, needed};
  }

  streamID last_id() { return last_id_; }

  void set_last_id(streamID last_id) { last_id_ = last_id; }

  uint64_t entries_read() { return entries_read_; }

  void set_entries_read(uint64_t entries_read) { entries_read_ = entries_read; }

  // pel and consumers were set in constructor,  can't be modified
  treeID pel() { return pel_; }

  treeID consumers() { return consumers_; }

 private:
  char space_[200];
  char* start_ = nullptr;
  streamID last_id_;
  uint64_t entries_read_ = 0;
  treeID pel_ = 0;
  treeID consumers_ = 0;
};

class ParsedStreamCGroupMetaValue {
 public:
  static const size_t kStreamCGroupMetaValueSuffixLength = sizeof(streamID) + sizeof(uint64_t) + sizeof(treeID) * 2;

  // Use this constructor after rocksdb::DB::Get(), since we use this in
  // the implement of user interfaces and may need to modify the
  // original value suffix, so the value_ must point to the string
  explicit ParsedStreamCGroupMetaValue(std::string* internal_value_str) : value_(internal_value_str) {
    assert(internal_value_str->size() >= kStreamCGroupMetaValueSuffixLength);
    if (internal_value_str->size() >= kStreamCGroupMetaValueSuffixLength) {
      uint64_t pos = 0;
      last_id_.ms = DecodeFixed64(internal_value_str->data() + pos);
      pos += sizeof(uint64_t);
      last_id_.seq = DecodeFixed64(internal_value_str->data() + pos);
      pos += sizeof(uint64_t);
      entries_read_ = DecodeFixed64(internal_value_str->data() + pos);
      pos += sizeof(uint64_t);
      pel_ = DecodeFixed32(internal_value_str->data() + pos);
      pos += sizeof(uint32_t);
      consumers_ = DecodeFixed32(internal_value_str->data() + pos);
    }
  }

  // Use this constructor in rocksdb::CompactionFilter::Filter(),
  // since we use this in Compaction process, all we need to do is parsing
  // the rocksdb::Slice, so don't need to modify the original value, value_ can be
  // set to nullptr
  explicit ParsedStreamCGroupMetaValue(const rocksdb::Slice& internal_value_slice) {
    assert(internal_value_slice.size() >= kStreamCGroupMetaValueSuffixLength);
    if (internal_value_slice.size() >= kStreamCGroupMetaValueSuffixLength) {
      uint64_t pos = 0;
      last_id_.ms = DecodeFixed64(internal_value_slice.data() + pos);
      pos += sizeof(uint64_t);
      last_id_.seq = DecodeFixed64(internal_value_slice.data() + pos);
      pos += sizeof(uint64_t);
      entries_read_ = DecodeFixed64(internal_value_slice.data() + pos);
      pos += sizeof(uint64_t);
      pel_ = DecodeFixed32(internal_value_slice.data() + pos);
      pos += sizeof(uint32_t);
      consumers_ = DecodeFixed32(internal_value_slice.data() + pos);
    }
  }

  void SetLastIdToValue() {
    if (value_) {
      char* dst = const_cast<char*>(value_->data());
      EncodeFixed64(dst, last_id_.ms);
      dst += sizeof(uint64_t);
      EncodeFixed64(dst, last_id_.seq);
    }
  }

  void set_last_id(streamID last_id) {
    last_id_ = last_id;
    SetLastIdToValue();
  }

  void SetEntriesReadToValue() {
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + sizeof(streamID);
      EncodeFixed64(dst, entries_read_);
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

}  // namespace storage

#endif  //  SRC_STREAM_CGROUP_META_VALUE_FORMAT_H_