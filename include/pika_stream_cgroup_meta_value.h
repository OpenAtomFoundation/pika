#ifndef SRC_STREAM_CGROUP_META_VALUE_FORMAT_H_
#define SRC_STREAM_CGROUP_META_VALUE_FORMAT_H_

#include <cassert>
#include <cstdint>
#include "glog/logging.h"
#include "include/pika_stream_types.h"

static const size_t kDefaultStreamCGroupValueLength = sizeof(streamID) + sizeof(uint64_t) + 2 * sizeof(treeID);

class StreamCGroupMetaValue {
 public:
  explicit StreamCGroupMetaValue() = default;

  void Init(treeID pel, treeID consumers) {
    pel_ = pel;
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
    assert(value.size() == kDefaultStreamCGroupValueLength);
    last_id_ = last_id;
    char* dst = const_cast<char*>(value_.data());
    memcpy(dst, &last_id_, sizeof(streamID));
  }

  uint64_t entries_read() { return entries_read_; }

  void set_entries_read(uint64_t entries_read) {
    assert(value.size() == kDefaultStreamCGroupValueLength);
    entries_read_ = entries_read;
      char* dst = const_cast<char*>(value_.data()) + sizeof(streamID);
      memcpy(dst, &entries_read_, sizeof(uint64_t));
  }

  // pel and consumers were set in constructor,  can't be modified
  treeID pel() { return pel_; }

  treeID consumers() { return consumers_; }

 private:

  std::string value_;

  streamID last_id_;
  uint64_t entries_read_ = 0;
  treeID pel_ = 0;
  treeID consumers_ = 0;
};

#endif  //  SRC_STREAM_CGROUP_META_VALUE_FORMAT_H_
