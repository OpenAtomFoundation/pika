#ifndef SRC_STREAM_CONSUMER_META_VALUE_FORMAT_H_
#define SRC_STREAM_CONSUMER_META_VALUE_FORMAT_H_

#include "src/stream_meta_value.h"

namespace storage {

// mstime_t seen_time
// mstime_t active_time
// treeID pel //  pel kv树的前缀
class StreamConsumerMetaValue {
 public:
  // pel must been set at beginning
  // FIXME: when to set seen_time and active_time?
  StreamConsumerMetaValue(treeID pel) : pel_(pel) {}

  rocksdb::Slice Encode() {
    size_t needed = sizeof(mstime_t) * 2 + sizeof(treeID);
    char* dst;
    if (needed <= sizeof(space_)) {
      dst = space_;
    } else {
      dst = new char[needed];
    }
    start_ = dst;
    EncodeFixed64(dst, seen_time_);
    dst += sizeof(mstime_t);
    EncodeFixed64(dst, active_time_);
    dst += sizeof(mstime_t);
    EncodeFixed32(dst, pel_);
    return {start_, needed};
  }

  mstime_t seen_time() { return seen_time_; }

  void set_seen_time(mstime_t seen_time) { seen_time_ = seen_time; }

  mstime_t active_time() { return active_time_; }

  void set_active_time(mstime_t active_time) { active_time_ = active_time; }

  // pel was set in constructor,  can't be modified
  treeID pel() { return pel_; }

 private:
  char space_[200];
  char* start_ = nullptr;
  mstime_t seen_time_ = 0;
  mstime_t active_time_ = 0;
  treeID pel_ = 0;
};

// mstime_t seen_time
// mstime_t active_time
// treeID pel //  pel kv树的前缀
class ParsedStreamConsumerMetaValue {
 public:
  static const size_t kStreamConsumerMetaValueSuffixLength = sizeof(mstime_t) * 2 + sizeof(treeID);
  
  // Use this constructor after rocksdb::DB::Get(), since we use this in
  // the implement of user interfaces and may need to modify the
  // original value suffix, so the value_ must point to the string
  explicit ParsedStreamConsumerMetaValue(std::string* internal_value_str) : value_(internal_value_str) {
    assert(internal_value_str->size() >= kStreamConsumerMetaValueSuffixLength);
    if (internal_value_str->size() >= kStreamConsumerMetaValueSuffixLength) {
      const char* ptr = internal_value_str->data();
      seen_time_ = DecodeFixed64(ptr);
      ptr += sizeof(mstime_t);
      active_time_ = DecodeFixed64(ptr);
      ptr += sizeof(mstime_t);
      pel_ = DecodeFixed32(ptr);
    }
  }

  // Use this constructor in rocksdb::CompactionFilter::Filter(),
  // since we use this in Compaction process, all we need to do is parsing
  // the rocksdb::Slice, so don't need to modify the original value, value_ can be
  // set to nullptr
  explicit ParsedStreamConsumerMetaValue(const rocksdb::Slice& internal_value_slice) {
    assert(internal_value_slice.size() >= kStreamConsumerMetaValueSuffixLength);
    if (internal_value_slice.size() >= kStreamConsumerMetaValueSuffixLength) {
      const char* ptr = internal_value_slice.data();
      seen_time_ = DecodeFixed64(ptr);
      ptr += sizeof(mstime_t);
      active_time_ = DecodeFixed64(ptr);
      ptr += sizeof(mstime_t);
      pel_ = DecodeFixed32(ptr);
    }
  }

  mstime_t seen_time() { return seen_time_; }

  void SetSeenTimeToValue() {
    if (value_) {
      char* dst = const_cast<char*>(value_->data());
      EncodeFixed64(dst, seen_time_);
    }
  }

  void set_seen_time(mstime_t seen_time) {
    seen_time_ = seen_time;
    SetSeenTimeToValue();
  }

  mstime_t active_time() { return active_time_; }

  void SetActiveTimeToValue() {
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + sizeof(uint64_t);
      EncodeFixed64(dst, active_time_);
    }
  }

  void set_active_time(mstime_t active_time) {
    active_time_ = active_time;
    SetActiveTimeToValue();
  }

  treeID pel() { return pel_; }

 private:
  std::string* value_ = nullptr;
  mstime_t seen_time_ = 0;
  mstime_t active_time_ = 0;
  treeID pel_ = 0;
};

}  //  namespace storage
#endif  //  SRC_STREAM_CONSUMER_META_VALUE_FORMAT_H_