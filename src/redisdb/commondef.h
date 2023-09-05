#ifndef __COMMON_DEF_H__
#define __COMMON_DEF_H__

#include <stdlib.h>

typedef long long mstime_t; /* millisecond time type. */

/* Error codes */
#define C_OK                    0
#define C_ERR                   -1
#define REDIS_INVALID_ARG       -2
#define REDIS_KEY_NOT_EXIST     -3
#define REDIS_INVALID_TYPE      -4
#define REDIS_OVERFLOW          -5
#define REDIS_ITEM_NOT_EXIST    -6
#define REDIS_NO_KEYS    		-7

/* The actual Redis Object */
#define OBJ_STRING 0
#define OBJ_LIST 1
#define OBJ_SET 2
#define OBJ_ZSET 3
#define OBJ_HASH 4

/* Objects encoding. Some kind of objects like Strings and Hashes can be
 * internally represented in multiple ways. The 'encoding' field of the object
 * is set to one of this fields for this object. */
#define OBJ_ENCODING_RAW 0     /* Raw representation */
#define OBJ_ENCODING_INT 1     /* Encoded as integer */
#define OBJ_ENCODING_HT 2      /* Encoded as hash table */
#define OBJ_ENCODING_ZIPMAP 3  /* Encoded as zipmap */
#define OBJ_ENCODING_LINKEDLIST 4 /* No longer used: old list encoding. */
#define OBJ_ENCODING_ZIPLIST 5 /* Encoded as ziplist */
#define OBJ_ENCODING_INTSET 6  /* Encoded as intset */
#define OBJ_ENCODING_SKIPLIST 7  /* Encoded as skiplist */
#define OBJ_ENCODING_EMBSTR 8  /* Embedded sds string encoding */
#define OBJ_ENCODING_QUICKLIST 9 /* Encoded as linked list of ziplists */

/* Redis maxmemory strategies. Instead of using just incremental number
 * for this defines, we use a set of flags so that testing for certain
 * properties common to multiple policies is faster. */
#define MAXMEMORY_FLAG_LRU (1<<0)
#define MAXMEMORY_FLAG_LFU (1<<1)
#define MAXMEMORY_FLAG_ALLKEYS (1<<2)
#define MAXMEMORY_FLAG_NO_SHARED_INTEGERS (MAXMEMORY_FLAG_LRU|MAXMEMORY_FLAG_LFU)

#define MAXMEMORY_VOLATILE_LRU ((0<<8)|MAXMEMORY_FLAG_LRU)
#define MAXMEMORY_VOLATILE_LFU ((1<<8)|MAXMEMORY_FLAG_LFU)
#define MAXMEMORY_VOLATILE_TTL (2<<8)
#define MAXMEMORY_VOLATILE_RANDOM (3<<8)
#define MAXMEMORY_ALLKEYS_LRU ((4<<8)|MAXMEMORY_FLAG_LRU|MAXMEMORY_FLAG_ALLKEYS)
#define MAXMEMORY_ALLKEYS_LFU ((5<<8)|MAXMEMORY_FLAG_LFU|MAXMEMORY_FLAG_ALLKEYS)
#define MAXMEMORY_ALLKEYS_RANDOM ((6<<8)|MAXMEMORY_FLAG_ALLKEYS)
#define MAXMEMORY_NO_EVICTION (7<<8)

/* LRU */
#define LRU_BITS 24
#define LRU_CLOCK_MAX ((1<<LRU_BITS)-1) /* Max value of obj->lru */
#define LRU_CLOCK_RESOLUTION 1000 /* LRU clock resolution in ms */

#define ACTIVE_EXPIRE_CYCLE_LOOKUPS_PER_LOOP 16 /* Loopkups per loop. */
#define ACTIVE_EXPIRE_CYCLE_FAST_DURATION 1000 /* Microseconds */
#define ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC 25 /* CPU max % for keys collection */
#define ACTIVE_EXPIRE_CYCLE_SLOW 0
#define ACTIVE_EXPIRE_CYCLE_FAST 1

#define OBJ_SHARED_REFCOUNT INT_MAX
#define LONG_STR_SIZE      21          /* Bytes needed for long -> str + '\0' */
#define OBJ_SHARED_INTEGERS 10000

/* Units */
#define UNIT_SECONDS 0
#define UNIT_MILLISECONDS 1

/* Static server configuration */
#define CONFIG_DEFAULT_MAXMEMORY (10 * 1024 * 1024 * 1024LL)    // 10G
#define CONFIG_DEFAULT_MAXMEMORY_POLICY MAXMEMORY_ALLKEYS_LRU
#define CONFIG_DEFAULT_MAXMEMORY_SAMPLES 5
#define CONFIG_DEFAULT_LFU_LOG_FACTOR 10
#define CONFIG_DEFAULT_LFU_DECAY_TIME 1

/* Zip structure related defaults */
#define OBJ_HASH_MAX_ZIPLIST_ENTRIES 512
#define OBJ_HASH_MAX_ZIPLIST_VALUE 64
#define OBJ_SET_MAX_INTSET_ENTRIES 512
#define OBJ_ZSET_MAX_ZIPLIST_ENTRIES 128
#define OBJ_ZSET_MAX_ZIPLIST_VALUE 64

/* Hash structure related defaults */
#define OBJ_HASH_KEY 1
#define OBJ_HASH_VALUE 2

/* Hash table parameters */
#define HASHTABLE_MIN_FILL        10      /* Minimal hash table fill 10% */

/* List defaults */
#define OBJ_LIST_MAX_ZIPLIST_SIZE -2
#define OBJ_LIST_COMPRESS_DEPTH 0

/* List related stuff */
#define LIST_HEAD 0
#define LIST_TAIL 1

/* Sorted sets data type */

/* Input flags. */
#define ZADD_NONE 0
#define ZADD_INCR (1<<0)    /* Increment the score instead of setting it. */
#define ZADD_NX (1<<1)      /* Don't touch elements not already existing. */
#define ZADD_XX (1<<2)      /* Only touch elements already exisitng. */

/* Output flags. */
#define ZADD_NOP (1<<3)     /* Operation not performed because of conditionals.*/
#define ZADD_NAN (1<<4)     /* Only touch elements already exisitng. */
#define ZADD_ADDED (1<<5)   /* The element was new and was added. */
#define ZADD_UPDATED (1<<6) /* The element already existed, score updated. */

/* Flags only used by the ZADD command but not by zsetAdd() API: */
#define ZADD_CH (1<<16)      /* Return num of elements added or updated. */

/* Bit pos offset */
#define BIT_POS_NO_OFFSET           0
#define BIT_POS_START_OFFSET        1
#define BIT_POS_START_END_OFFSET    2


// redisdb config
typedef struct _db_config {
    unsigned long long maxmemory;       /* Can used max memory */
    int maxmemory_policy;               /* Policy for key eviction */
    int maxmemory_samples;              /* Pricision of random sampling */
    int lfu_decay_time;                 /* LFU counter decay factor. */
} db_config;

// redisdb status
typedef struct _db_status {
    long long stat_evictedkeys;         /* Number of evicted keys (maxmemory) */
    long long stat_expiredkeys;         /* Number of expired keys */
    long long stat_keyspace_hits;       /* Number of successful lookups of keys */
    long long stat_keyspace_misses;     /* Number of failed lookups of keys */
    size_t stat_peak_memory;            /* Max used memory record */
} db_status;

#endif
