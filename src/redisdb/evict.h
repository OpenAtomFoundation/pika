#ifndef __EVICT_H__
#define __EVICT_H__

#include "object.h"
#include "dict.h"

/* ----------------------------------------------------------------------------
 * Data structures
 * --------------------------------------------------------------------------*/

/* To improve the quality of the LRU approximation we take a set of keys
 * that are good candidate for eviction across freeMemoryIfNeeded() calls.
 *
 * Entries inside the eviciton pool are taken ordered by idle time, putting
 * greater idle times to the right (ascending order).
 *
 * When an LFU policy is used instead, a reverse frequency indication is used
 * instead of the idle time, so that we still evict by larger value (larger
 * inverse frequency means to evict keys with the least frequent accesses).
 *
 * Empty entries have the key pointer set to NULL. */
#define EVPOOL_SIZE 16
#define EVPOOL_CACHED_SDS_SIZE 255
struct evictionPoolEntry {
    unsigned long long idle;    /* Object idle time (inverse frequency for LFU) */
    sds key;                    /* Key name. */
    sds cached;                 /* Cached SDS object for key name. */
};

unsigned int getLRUClock(void);
unsigned int LRU_CLOCK(void);
unsigned long long estimateObjectIdleTime(robj *o);

struct evictionPoolEntry* evictionPoolAlloc(void);
void evictionPoolDestroy(struct evictionPoolEntry *pool);
void evictionPoolPopulate(dict *sampledict, dict *keydict, struct evictionPoolEntry *pool);

#define LFU_INIT_VAL 5
unsigned long LFUGetTimeInMinutes(void);
uint8_t LFULogIncr(uint8_t value);
unsigned long LFUDecrAndReturn(robj *o);


#endif
