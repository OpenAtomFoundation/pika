
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

static const std::string STREAM_META_HASH_KEY = "STREAM"; // key of hash to store stream meta
static const std::string STREAM_TREE_STRING_KEY = "STREAM"; // key of string to store stream tree id

static const int kSTREAM_MAX_LIMIT = 1000000;
static const int KSTREAM_MIN_LIMIT = 10000;

static const treeID kINVALID_TREE_ID = 0;

struct StreamAddTrimArgs{
    /* XADD options */
    streamID id; /* User-provided ID, for XADD only. */
    int id_given{0}; /* Was an ID different than "*" specified? for XADD only. */
    int seq_given{0}; /* Was an ID different than "ms-*" specified? for XADD only. */
    int no_mkstream{0}; /* if set to 1 do not create new stream */

    /* XADD + XTRIM common options */
    StreamTrimStrategy trim_strategy{0}; /* TRIM_STRATEGY_* */
    int trim_strategy_arg_idx{0}; /* Index of the count in MAXLEN/MINID, for rewriting. */
    bool approx_trim{false}; /* If true only delete whole radix tree nodes, so
                      * the trim argument is not applied verbatim. */
    uint64_t limit{0}; /* Maximum amount of entries to trim. If 0, no limitation
                      * on the amount of trimming work is enforced. */
    /* TRIM_STRATEGY_MAXLEN options */
    uint64_t maxlen{0}; /* After trimming, leave stream at this length . */
    /* TRIM_STRATEGY_MINID options */
    streamID minid; /* Trim by ID (No stream entries with ID < 'minid' will remain) */
};

#endif // SRC_STREAM_TYPE_H_