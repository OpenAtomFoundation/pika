#ifndef SRC_STREAM_CONSUMER_META_VALUE_FORMAT_H_
#define SRC_STREAM_CONSUMER_META_VALUE_FORMAT_H_

#include <cassert>
#include "glog/logging.h"
#include "include/pika_stream_types.h"

static const size_t kDefaultStreamConsumerValueLength = sizeof(mstime_t) * 2 + sizeof(treeID);

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
      memcpy(&seen_time_, pos, sizeof(mstime_t));
      pos += sizeof(mstime_t);
      memcpy(&active_time_, pos, sizeof(mstime_t));
      pos += sizeof(mstime_t);
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

    memcpy(dst, &seen_time_, sizeof(mstime_t));
    dst += sizeof(mstime_t);
    memcpy(dst, &active_time_, sizeof(mstime_t));
    dst += sizeof(mstime_t);
    memcpy(dst, &pel_, sizeof(treeID));
  }

  mstime_t seen_time() { return seen_time_; }

  void set_seen_time(mstime_t seen_time) {
    seen_time_ = seen_time;
    assert(value.size() == kDefaultStreamConsumerValueLength);
    char* dst = const_cast<char*>(value_.data());
    memcpy(dst, &seen_time_, sizeof(mstime_t));
  }

  mstime_t active_time() { return active_time_; }

  void set_active_time(mstime_t active_time) {
    active_time_ = active_time;
    assert(value.size() == kDefaultStreamConsumerValueLength);
    char* dst = const_cast<char*>(value_.data()) + sizeof(mstime_t);
    memcpy(dst, &active_time_, sizeof(mstime_t));
  }

  // pel was set in constructor,  can't be modified
  treeID pel() { return pel_; }

  std::string& value() { return value_; }

 private:
  std::string value_;

  mstime_t seen_time_ = 0;
  mstime_t active_time_ = 0;
  treeID pel_ = 0;
};

#endif  //  SRC_STREAM_CONSUMER_META_VALUE_FORMAT_H_