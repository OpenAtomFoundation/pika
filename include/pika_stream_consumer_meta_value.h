#ifndef SRC_STREAM_CONSUMER_META_VALUE_FORMAT_H_
#define SRC_STREAM_CONSUMER_META_VALUE_FORMAT_H_

#include "include/pika_stream_types.h"

static const size_t kDefaultStreamConsumerValueLength = sizeof(mstime_t) * 2 + sizeof(treeID);

class StreamConsumerMetaValue {
 public:
  // pel must been set at beginning
  StreamConsumerMetaValue(treeID pel) : pel_(pel) {}

  void Encode() {
    size_t needed = kDefaultStreamConsumerValueLength;
    filed_value_.value.resize(needed);
    auto dst = filed_value_.value.data();

    memcpy(dst, &seen_time_, sizeof(mstime_t));
    dst += sizeof(mstime_t);
    memcpy(dst, &active_time_, sizeof(mstime_t));
    dst += sizeof(mstime_t);
    memcpy(dst, &pel_, sizeof(treeID));
  }

  mstime_t seen_time() { return seen_time_; }

  void set_seen_time(mstime_t seen_time) { seen_time_ = seen_time; }

  mstime_t active_time() { return active_time_; }

  void set_active_time(mstime_t active_time) { active_time_ = active_time; }

  // pel was set in constructor,  can't be modified
  treeID pel() { return pel_; }

 private:
  storage::FieldValue filed_value_;

  mstime_t seen_time_ = 0;
  mstime_t active_time_ = 0;
  treeID pel_ = 0;
};

class ParsedStreamConsumerMetaValue {
 public:
  
  explicit ParsedStreamConsumerMetaValue(std::string* internal_value_str) : value_(internal_value_str) {
    assert(internal_value_str->size() == kDefaultStreamConsumerValueLength);
    if (internal_value_str->size() == kDefaultStreamConsumerValueLength) {
      auto pos = value_->data();
      memcpy(&seen_time_, pos, sizeof(mstime_t));
      pos += sizeof(mstime_t);
      memcpy(&active_time_, pos, sizeof(mstime_t));
      pos += sizeof(mstime_t);
      memcpy(&pel_, pos, sizeof(treeID));
    }
  }

  mstime_t seen_time() { return seen_time_; }

  void SetSeenTimeToValue() {
    if (value_) {
      char* dst = const_cast<char*>(value_->data());
      memcpy(dst, &seen_time_, sizeof(mstime_t));
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
      memcpy(dst, &active_time_, sizeof(mstime_t));
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

#endif  //  SRC_STREAM_CONSUMER_META_VALUE_FORMAT_H_