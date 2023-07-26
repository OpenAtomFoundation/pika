
#ifndef SRC_STREAM_TYPE_H_
#define SRC_STREAM_TYPE_H_

#include <cstdint>
#include "storage/storage.h"

// FIXME: will move to pika_command.h in the future
const std::string kCmcNameXAdd = "xadd";

using streamID = struct streamID {
  streamID(uint64_t _ms, uint64_t _seq) : ms(_ms), seq(_seq) {}
  streamID() = default;
  uint64_t ms = 0;  /* Unix time in milliseconds. */
  uint64_t seq = 0; /* Sequence number. */
};

enum class StreamTrimStrategy { TRIM_STRATEGY_NONE, TRIM_STRATEGY_MAXLEN, TRIM_STRATEGY_MINID };

using treeID = int32_t;
using mstime_t = uint64_t;

static const char* STERAM_TREE_PREFIX = "STREE";

static const std::string STREAM_META_HASH_KEY = "STREAM";    // key of hash to store stream meta
static const std::string STREAM_TREE_STRING_KEY = "STREAM";  // key of string to store stream tree id

static const int kSTREAM_MAX_LIMIT = 1000000;
static const int KSTREAM_MIN_LIMIT = 10000;

static const treeID kINVALID_TREE_ID = 0;

struct StreamAddTrimArgs {
  // XADD options
  streamID id;
  bool id_given{false};
  int seq_given{0};
  bool no_mkstream{true};

  // XADD + XTRIM common options
  StreamTrimStrategy trim_strategy{0};
  int trim_strategy_arg_idx{0};
  bool approx_trim{false};
  uint64_t limit{0};

  // TRIM_STRATEGY_MAXLEN options
  uint64_t maxlen{0};
  streamID minid;
};

struct StreamReadGroupReadArgs {
  // XREAD + XREADGROUP common options
  std::vector<std::string> keys;
  std::vector<streamID> ids;
  uint64_t count{0};
  uint64_t block{0};  // 0 means no block

  // XREADGROUP options
  std::string group_name;
  std::string consumer_name;
  bool noack_{false};
};

#endif  // SRC_STREAM_TYPE_H_