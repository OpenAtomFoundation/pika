
#ifndef SRC_STREAM_TYPE_H_
#define SRC_STREAM_TYPE_H_

#include "storage/storage.h"

// FIXME: will move to pika_command.h in the future
const std::string kCmcNameXAdd = "xadd";


/* (From redis): Stream item ID: a 128 bit number composed of a milliseconds time and
 * a sequence counter. IDs generated in the same millisecond (or in a past
 * millisecond if the clock jumped backward) will use the millisecond time
 * of the latest generated ID and an incremented sequence. */
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

static const std::string STREAM_META_HASH_KEY = "STREAM"; // key of hash to store stream meta
static const std::string STREAM_TREE_STRING_KEY = "STREAM"; // key of string to store stream tree id




#endif // SRC_STREAM_TYPE_H_