#ifndef SRC_STREAM_PEL_META_VALUE_FORMAT_H_
#define SRC_STREAM_PEL_META_VALUE_FORMAT_H_

#include "src/stream_meta_value.h"

namespace storage {

// mstime_t delivery_time
// uint64_t delivery_count
// treeID consumer
class StreamPelMetaValue {
 public:
  // consumer must been set at beginning
  StreamPelMetaValue(treeID consumer) : consumer_(consumer) {}

  rocksdb::Slice Encode() {
    size_t needed = sizeof(mstime_t) + sizeof(uint64_t) + sizeof(treeID);
    char* dst;
    if (needed <= sizeof(space_)) {
      dst = space_;
    } else {
      dst = new char[needed];
    }
    start_ = dst;
    EncodeFixed64(dst, delivery_time_);
    dst += sizeof(mstime_t);
    EncodeFixed64(dst, delivery_count_);
    dst += sizeof(uint64_t);
    EncodeFixed32(dst, consumer_);
    return {start_, needed};
  }

  mstime_t delivery_time() { return delivery_time_; }

  void set_delivery_time(mstime_t delivery_time) { delivery_time_ = delivery_time; }

  uint64_t delivery_count() { return delivery_count_; }

  void set_delivery_count(uint64_t delivery_count) { delivery_count_ = delivery_count; }

  treeID consumer() { return consumer_; }

 private:
  char space_[200];
  char* start_ = nullptr;
  mstime_t delivery_time_ = 0;
  uint64_t delivery_count_ = 0;
  treeID consumer_ = 0;
};

class ParsedStreamPelMetaValue {
 public:
  static const size_t kStreamPelMetaValueSuffixLength = sizeof(mstime_t) + sizeof(uint64_t) + sizeof(treeID);
  
  // Use this constructor after rocksdb::DB::Get(), since we use this in
  // the implement of user interfaces and may need to modify the
  // original value suffix, so the value_ must point to the string
  explicit ParsedStreamPelMetaValue(std::string* internal_value_str) : value_(internal_value_str) {
    const char* ptr = value_->data();
    delivery_time_ = DecodeFixed64(ptr);
    ptr += sizeof(mstime_t);
    delivery_count_ = DecodeFixed64(ptr);
    ptr += sizeof(uint64_t);
    consumer_ = DecodeFixed32(ptr);
  }

  // Use this constructor in rocksdb::CompactionFilter::Filter(),
  // since we use this in Compaction process, all we need to do is parsing
  // the rocksdb::Slice, so don't need to modify the original value, value_ can be
  // set to nullptr
  explicit ParsedStreamPelMetaValue(const rocksdb::Slice& internal_value_slice) {
    const char* ptr = internal_value_slice.data();
    delivery_time_ = DecodeFixed64(ptr);
    ptr += sizeof(mstime_t);
    delivery_count_ = DecodeFixed64(ptr);
    ptr += sizeof(uint64_t);
    consumer_ = DecodeFixed32(ptr);
  }

  mstime_t delivery_time() { return delivery_time_; }

  uint64_t delivery_count() { return delivery_count_; }

  treeID consumer() { return consumer_; }

 private:
  std::string* value_;
  mstime_t delivery_time_ = 0;
  uint64_t delivery_count_ = 0;
  treeID consumer_ = 0;
};

}  //  namespace storage
#endif  //  SRC_STREAM_PEL_META_VALUE_FORMAT_H_