
#ifndef SRC_STREAM_TYPE_H_
#define SRC_STREAM_TYPE_H_

#include <memory.h>
#include <cassert>
#include <cstdint>
#include <string>
#include <vector>

// We must store the streamID in memory in big-endian format. This way, our comparison of the serialized streamID byte
// code will be equivalent to the comparison of the uint64_t numbers.
inline void EncodeUint64(char* buf, uint64_t value) {
  if ((__BYTE_ORDER == __LITTLE_ENDIAN)) {
    // little endian, reverse the bytes
    for (int i = 7; i >= 0; --i) {
      buf[i] = static_cast<char>(value & 0xff);
      value >>= 8;
    }
  } else {
    // big endian, just copy the bytes
    memcpy(buf, &value, sizeof(value));
  }
}

inline uint64_t DecodeUint64(const char* ptr) {
  uint64_t value;
  if ((__BYTE_ORDER == __LITTLE_ENDIAN)) {
    // little endian, reverse the bytes
    value = 0;
    for (int i = 0; i < 8; ++i) {
      value <<= 8;
      value |= static_cast<unsigned char>(ptr[i]);
    }
  } else {
    // big endian, just copy the bytes
    memcpy(&value, ptr, sizeof(value));
  }
  return value;
}

using streamID = struct streamID {
  streamID(uint64_t _ms, uint64_t _seq) : ms(_ms), seq(_seq) {}
  bool operator==(const streamID& other) const { return ms == other.ms && seq == other.seq; }
  bool operator<(const streamID& other) const { return ms < other.ms || (ms == other.ms && seq < other.seq); }
  bool operator>(const streamID& other) const { return ms > other.ms || (ms == other.ms && seq > other.seq); }
  bool operator<=(const streamID& other) const { return ms < other.ms || (ms == other.ms && seq <= other.seq); }
  std::string ToString() const { return std::to_string(ms) + "-" + std::to_string(seq); }

  void SerializeTo(std::string& dst) const {
    dst.resize(sizeof(ms) + sizeof(seq));
    EncodeUint64(&dst[0], ms);
    EncodeUint64(&dst[0] + sizeof(ms), seq);
  }
  void DeserializeFrom(std::string& src) {
    assert(src.size() == sizeof(ms) + sizeof(seq));
    ms = DecodeUint64(&src[0]);
    seq = DecodeUint64(&src[0] + sizeof(ms));
  }

  streamID() = default;
  uint64_t ms = 0;  /* Unix time in milliseconds. */
  uint64_t seq = 0; /* Sequence number. */
};

static const streamID kSTREAMID_MAX = streamID(UINT64_MAX, UINT64_MAX);
static const streamID kSTREAMID_MIN = streamID(0, 0);

enum class StreamTrimStrategy { TRIM_STRATEGY_NONE, TRIM_STRATEGY_MAXLEN, TRIM_STRATEGY_MINID };

using treeID = int32_t;
using mstime_t = uint64_t;

static const char* STERAM_TREE_PREFIX = "STREE";
static const std::string STREAM_META_HASH_KEY = "STREAM";    // key of hash to store stream meta
static const std::string STREAM_TREE_STRING_KEY = "STREAM";  // key of string to store stream tree id

static const treeID kINVALID_TREE_ID = -1;

// the max number of each delete operation in XTRIM command
// eg. if a XTIRM command need to trim 10000 items, the implementation will use rocsDB's delete operation (10000 /
// kDEFAULT_TRIM_BATCH_SIZE) times
const static uint64_t kDEFAULT_TRIM_BATCH_SIZE = 1000;

struct StreamAddTrimArgs {
  // XADD options
  streamID id;
  bool id_given{false};
  bool seq_given{false};
  bool no_mkstream{false};

  // XADD + XTRIM common options
  StreamTrimStrategy trim_strategy{0};
  int trim_strategy_arg_idx{0};

  // TRIM_STRATEGY_MAXLEN options
  uint64_t maxlen{0};
  streamID minid;
};

struct StreamReadGroupReadArgs {
  // XREAD + XREADGROUP common options
  std::vector<std::string> keys;
  std::vector<std::string> unparsed_ids;
  int32_t count{INT32_MAX};  // in redis this is uint64_t, but PKHScanRange only support int32_t
  uint64_t block{0};         // 0 means no block

  // XREADGROUP options
  std::string group_name;
  std::string consumer_name;
  bool noack_{false};
};

#endif  // SRC_STREAM_TYPE_H_