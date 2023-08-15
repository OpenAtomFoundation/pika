#ifndef SRC_STREAM_PEL_META_VALUE_FORMAT_H_
#define SRC_STREAM_PEL_META_VALUE_FORMAT_H_

#include <cassert>
#include <cstddef>
#include <string>

#include "glog/logging.h"
#include "include/pika_stream_types.h"

static const size_t kDefaultStreamPelMetaValueLength = sizeof(mstime_t) + sizeof(uint64_t) + sizeof(treeID);

class StreamPelMeta {
 public:
  // consumer must been set at beginning
  StreamPelMeta() = default;

  void Init(std::string consumer, mstime_t delivery_time) {
    consumer_ = consumer;
    delivery_time_ = delivery_time;
    size_t needed = kDefaultStreamPelMetaValueLength;
    assert(value_.size() == 0);
    if (value_.size() != 0) {
      LOG(ERROR) << "Init on a existed stream pel meta value!";
      return;
    }
    value_.resize(needed);
    char* dst = value_.data();

    memcpy(dst, &delivery_time_, sizeof(mstime_t));
    dst += sizeof(mstime_t);
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
    memcpy(&delivery_time_, pos, sizeof(mstime_t));
    pos += sizeof(mstime_t);
    memcpy(&delivery_count_, pos, sizeof(uint64_t));
    pos += sizeof(uint64_t);
    memcpy(&cname_len_, pos, sizeof(size_t));
    pos += sizeof(size_t);
    consumer_.assign(pos, cname_len_);
  }

  mstime_t delivery_time() { return delivery_time_; }

  void set_delivery_time(mstime_t delivery_time) {
    assert(value_.size() == kDefaultStreamPelMetaValueLength);
    delivery_time_ = delivery_time;
    char* dst = const_cast<char*>(value_.data());
    memcpy(dst, &delivery_time_, sizeof(mstime_t));
  }

  uint64_t delivery_count() { return delivery_count_; }

  void set_delivery_count(uint64_t delivery_count) {
    assert(value_.size() == kDefaultStreamPelMetaValueLength);
    delivery_count_ = delivery_count;
    char* dst = const_cast<char*>(value_.data());
    memcpy(dst + sizeof(mstime_t), &delivery_count_, sizeof(uint64_t));
  }

  std::string& consumer() { return consumer_; }

  std::string& value() { return value_; }

 private:
  std::string value_;

  mstime_t delivery_time_ = 0;
  uint64_t delivery_count_ = 1;
  size_t cname_len_ = 0;
  std::string consumer_;
};

#endif  //  SRC_STREAM_PEL_META_VALUE_FORMAT_H_