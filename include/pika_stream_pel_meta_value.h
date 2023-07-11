#ifndef SRC_STREAM_PEL_META_VALUE_FORMAT_H_
#define SRC_STREAM_PEL_META_VALUE_FORMAT_H_

#include "include/pika_stream_types.h"

static const size_t kDefaultStreamPelMetaValueLength = sizeof(mstime_t) + sizeof(uint64_t) + sizeof(treeID);

class StreamPelMetaFiledValue {
 public:
  // consumer must been set at beginning
  StreamPelMetaFiledValue(treeID consumer) : consumer_(consumer) {}

  void Encode() {
    size_t needed = kDefaultStreamPelMetaValueLength;
    filed_value_.value.resize(needed);
    auto dst = filed_value_.value.data();

    memcpy(dst, &delivery_time_, sizeof(mstime_t));
    dst += sizeof(mstime_t);
    memcpy(dst, &delivery_count_, sizeof(uint64_t));
    dst += sizeof(uint64_t);
    memcpy(dst, &consumer_, sizeof(treeID));
  }

  mstime_t delivery_time() { return delivery_time_; }

  void set_delivery_time(mstime_t delivery_time) { delivery_time_ = delivery_time; }

  uint64_t delivery_count() { return delivery_count_; }

  void set_delivery_count(uint64_t delivery_count) { delivery_count_ = delivery_count; }

  treeID consumer() { return consumer_; }

 private:
  storage::FieldValue filed_value_;

  mstime_t delivery_time_ = 0;
  uint64_t delivery_count_ = 0;
  treeID consumer_ = 0;
};

class ParsedStreamPelMetaValue {
 public:
  explicit ParsedStreamPelMetaValue(std::string* internal_value_str) : value_(internal_value_str) {
    assert(internal_value_str->size() == kDefaultStreamPelMetaValueLength);
    if (internal_value_str->size() == kDefaultStreamPelMetaValueLength) {
      auto pos = value_->data();
      memcpy(&delivery_time_, pos, sizeof(mstime_t));
      pos += sizeof(mstime_t);
      memcpy(&delivery_count_, pos, sizeof(uint64_t));
      pos += sizeof(uint64_t);
      memcpy(&consumer_, pos, sizeof(treeID));
    }
  }

  mstime_t delivery_time() { return delivery_time_; }

  uint64_t delivery_count() { return delivery_count_; }

  treeID consumer() { return consumer_; }

 private:
  std::string* value_ = nullptr;

  mstime_t delivery_time_ = 0;
  uint64_t delivery_count_ = 0;
  treeID consumer_ = 0;
};

#endif  //  SRC_STREAM_PEL_META_VALUE_FORMAT_H_