#include "storage/storage.h"

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

using treeID = uint32_t;
using mstime_t = uint64_t;

static const char* STERAM_TREE_PREFIX = "meta";