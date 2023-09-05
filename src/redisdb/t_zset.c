#include <string.h>
#include <assert.h>
#include <limits.h>

#include "redisdbIF.h"
#include "commondef.h"
#include "commonfunc.h"
#include "object.h"
#include "zmalloc.h"
#include "db.h"
#include "zset.h"
#include "ziplist.h"
#include "util.h"
#include "solarisfixes.h"

#define ZRANGE_RANK 0
#define ZRANGE_SCORE 1
#define ZRANGE_LEX 2


static int zaddGenericCommand(redisDb *redis_db,
                              robj *kobj,
                              robj *items[],
                              unsigned long items_size,
                              int flags) {

    /* Turn options into simple to check vars. */
    int incr = (flags & ZADD_INCR) != 0;
    int nx = (flags & ZADD_NX) != 0;
    int xx = (flags & ZADD_XX) != 0;
    // int ch = (flags & ZADD_CH) != 0;

    /* Check for incompatible options. */
    if (nx && xx) {
        return C_ERR;
    }

    /* After the options, we expect to have an even number of args, since
     * we expect any number of score-element pairs. */
    if (items_size % 2 || !items_size) {
        return C_ERR;
    }

    unsigned long elements = items_size / 2;
    if (incr && elements > 1) {
        return C_ERR;
    }

    /* Start parsing all the scores, we need to emit any syntax error
     * before executing additions to the sorted set, as the command should
     * either execute fully or nothing at all. */
    unsigned long j;
    int scoreidx = 0;
    double *scores = zmalloc(sizeof(double)*elements);
    for (j = 0; j < elements; j++) {
        if (C_OK != getDoubleFromObject(items[scoreidx+j*2],&scores[j])) {
            zfree(scores);
            return C_ERR;
        }
    }

    /* Lookup the key and create the sorted set if does not exist. */
    robj *zobj = lookupKeyWrite(redis_db,kobj);
    if (zobj == NULL) {
        if (xx) {
            zfree(scores);
            return C_ERR; /* No key + XX option: nothing to do. */
        } 
        if (OBJ_ZSET_MAX_ZIPLIST_ENTRIES == 0 ||
            OBJ_ZSET_MAX_ZIPLIST_VALUE < sdslen(items[scoreidx+1]->ptr))
        {
            zobj = createZsetObject();
        } else {
            zobj = createZsetZiplistObject();
        }
        dbAdd(redis_db,kobj,zobj);
    } else {
        if (zobj->type != OBJ_ZSET) {
            zfree(scores);
            return C_ERR;
        }
    }

    /* The following vars are used in order to track what the command actually
     * did during the execution, to reply to the client and to trigger the
     * notification of keyspace change. */
    int added = 0;      /* Number of new elements added. */
    int updated = 0;    /* Number of elements with updated score. */
    int processed = 0;  /* Number of elements processed, may remain zero with
                           options like XX. */
    sds ele;
    double score = 0;
    for (j = 0; j < elements; j++) {
        double newscore;
        score = scores[j];
        int retflags = flags;

        ele = items[scoreidx+1+j*2]->ptr;
        int retval = zsetAdd(zobj, score, ele, &retflags, &newscore);
        if (retval == 0) {
            zfree(scores);
            return C_ERR;
        }
        if (retflags & ZADD_ADDED) added++;
        if (retflags & ZADD_UPDATED) updated++;
        if (!(retflags & ZADD_NOP)) processed++;
        score = newscore;
    }

    zfree(scores);
    return C_OK;
}

static int zrangeGenericCommand(redisDb *redis_db,
                                robj *kobj,
                                long start,
                                long end,
                                zitem **items,
                                unsigned long *items_size,
                                int reverse)
{
    robj *zobj;
    if ((zobj = lookupKeyRead(redis_db,kobj)) == NULL || checkType(zobj,OBJ_ZSET)) {
        return REDIS_KEY_NOT_EXIST;
    }

    /* Sanitize indexes. */
    unsigned int llen;
    llen = zsetLength(zobj);
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */
    if (start > end || start >= llen) {
        *items = NULL;
        *items_size = 0;
        return C_OK;
    }
    if (end >= llen) end = llen-1;
    unsigned long rangelen = (end-start)+1;

    *items_size = rangelen;
    *items = (zitem*)zcalloc(sizeof(zitem) * (*items_size));

    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;
        unsigned long i = 0;

        if (reverse)
            eptr = ziplistIndex(zl,-2-(2*start));
        else
            eptr = ziplistIndex(zl,2*start);

        assert(eptr != NULL);
        sptr = ziplistNext(zl,eptr);

        while (rangelen--) {
            assert(eptr != NULL && sptr != NULL);
            assert(ziplistGet(eptr,&vstr,&vlen,&vlong));
            if (vstr == NULL)
                (*items+i)->member = sdsfromlonglong(vlong);
            else
                (*items+i)->member = sdsnewlen(vstr, vlen);

            (*items+i)->score = zzlGetScore(sptr);


            if (reverse)
                zzlPrev(zl,&eptr,&sptr);
            else
                zzlNext(zl,&eptr,&sptr);

            ++i;
            if (i >= *items_size) break;
        }

    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;
        unsigned long i = 0;

        /* Check if starting point is trivial, before doing log(N) lookup. */
        if (reverse) {
            ln = zsl->tail;
            if (start > 0)
                ln = zslGetElementByRank(zsl,llen-start);
        } else {
            ln = zsl->header->level[0].forward;
            if (start > 0)
                ln = zslGetElementByRank(zsl,start+1);
        }

        while(rangelen--) {
            assert(ln != NULL);
            (*items+i)->member = sdsdup(ln->ele);
            (*items+i)->score = ln->score;
            ln = reverse ? ln->backward : ln->level[0].forward;

            ++i;
            if (i >= *items_size) break;
        }
    } else {
        zfree(*items);
        return C_ERR;
    }

    return C_OK;
}

static int genericZrangebyscoreCommand(redisDb *redis_db,
                                       robj *kobj,
                                       robj *minobj,
                                       robj *maxobj,
                                       zitem **items,
                                       unsigned long *items_size,
                                       int reverse,
                                       long offset, long limit)
{
    /* Parse the range arguments. */
    zrangespec range;
    if (reverse) {
        if (zslParseRange(maxobj,minobj,&range) != C_OK) {
            return C_ERR;
        }
    } else {
        if (zslParseRange(minobj,maxobj,&range) != C_OK) {
            return C_ERR;
        }
    }

    /* Ok, lookup the key and get the range */
    robj *zobj;
    if ((zobj = lookupKeyRead(redis_db,kobj)) == NULL || checkType(zobj,OBJ_ZSET)) {
        return REDIS_KEY_NOT_EXIST;
    }

    unsigned int len = zsetLength(zobj);
    unsigned int zlloc_len = len;
    zlloc_len = (limit > 0 && limit < len) ? limit : len;
    *items = (zitem*)zcalloc(sizeof(zitem) * zlloc_len);

    int withscores = 1;
    unsigned long rangelen = 0;
    unsigned long i = 0;
    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;
        double score;

        /* If reversed, get the last node in range as starting point. */
        if (reverse) {
            eptr = zzlLastInRange(zl,&range);
        } else {
            eptr = zzlFirstInRange(zl,&range);
        }

        /* No "first" element in the specified interval. */
        if (eptr == NULL) {
            *items_size = 0;
            return C_OK;
        }

        /* Get score pointer for the first element. */
        assert(eptr != NULL);
        sptr = ziplistNext(zl,eptr);

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */
        while (eptr && offset--) {
            if (reverse) {
                zzlPrev(zl,&eptr,&sptr);
            } else {
                zzlNext(zl,&eptr,&sptr);
            }
        }

        while (eptr && limit--) {
            score = zzlGetScore(sptr);

            /* Abort when the node is no longer in range. */
            if (reverse) {
                if (!zslValueGteMin(score,&range)) break;
            } else {
                if (!zslValueLteMax(score,&range)) break;
            }

            /* We know the element exists, so ziplistGet should always succeed */
            assert(ziplistGet(eptr,&vstr,&vlen,&vlong));

            rangelen++;
            if (vstr == NULL) {
                (*items+i)->member = sdsfromlonglong(vlong);
            } else {
                (*items+i)->member = sdsnewlen(vstr, vlen);
            }

            if (withscores) {
                (*items+i)->score = score;
            }

            ++i;
            if (i >= len) break;

            /* Move to next node */
            if (reverse) {
                zzlPrev(zl,&eptr,&sptr);
            } else {
                zzlNext(zl,&eptr,&sptr);
            }
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;

        /* If reversed, get the last node in range as starting point. */
        if (reverse) {
            ln = zslLastInRange(zsl,&range);
        } else {
            ln = zslFirstInRange(zsl,&range);
        }

        /* No "first" element in the specified interval. */
        if (ln == NULL) {
            *items_size = 0;
            return C_OK;
        }

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */
        while (ln && offset--) {
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }

        while (ln && limit--) {
            /* Abort when the node is no longer in range. */
            if (reverse) {
                if (!zslValueGteMin(ln->score,&range)) break;
            } else {
                if (!zslValueLteMax(ln->score,&range)) break;
            }

            rangelen++;
            (*items+i)->member = sdsdup(ln->ele);

            if (withscores) {
                (*items+i)->score = ln->score;
            }

            ++i;
            if (i >= len) break;

            /* Move to next node */
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }
    } else {
        zfree(*items);
        return C_ERR;
    }

    *items_size = rangelen;

    return C_OK;
}

static int genericZrangebylexCommand(redisDb *redis_db,
                                     robj *kobj,
                                     robj *minobj,
                                     robj *maxobj,
                                     sds **members,
                                     unsigned long *members_size,
                                     int reverse)
{
    /* Parse the range arguments. */
    zlexrangespec range;
    if (reverse) {
        if (zslParseLexRange(maxobj,minobj,&range) != C_OK) {
            return C_ERR;
        }
    } else {
        if (zslParseLexRange(minobj,maxobj,&range) != C_OK) {
            return C_ERR;
        }
    }

    /* Ok, lookup the key and get the range */
    robj *zobj;
    if ((zobj = lookupKeyRead(redis_db,kobj)) == NULL || checkType(zobj,OBJ_ZSET)) {
        zslFreeLexRange(&range);
        return REDIS_KEY_NOT_EXIST;
    }

    unsigned int len = zsetLength(zobj);
    *members = (sds *)zcalloc(sizeof(sds) * len);
    sds *arrays = *members;

    long offset = 0, limit = -1;
    unsigned long rangelen = 0;
    unsigned long i = 0;
    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        /* If reversed, get the last node in range as starting point. */
        if (reverse) {
            eptr = zzlLastInLexRange(zl,&range);
        } else {
            eptr = zzlFirstInLexRange(zl,&range);
        }

        /* No "first" element in the specified interval. */
        if (eptr == NULL) {
            *members_size = 0;
            zslFreeLexRange(&range);
            return C_OK;
        }

        /* Get score pointer for the first element. */
        assert(eptr != NULL);
        sptr = ziplistNext(zl,eptr);

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */
        while (eptr && offset--) {
            if (reverse) {
                zzlPrev(zl,&eptr,&sptr);
            } else {
                zzlNext(zl,&eptr,&sptr);
            }
        }

        while (eptr && limit--) {
            /* Abort when the node is no longer in range. */
            if (reverse) {
                if (!zzlLexValueGteMin(eptr,&range)) break;
            } else {
                if (!zzlLexValueLteMax(eptr,&range)) break;
            }

            /* We know the element exists, so ziplistGet should always
             * succeed. */
            assert(ziplistGet(eptr,&vstr,&vlen,&vlong));

            rangelen++;
            if (vstr == NULL) {
                arrays[i] = sdsfromlonglong(vlong);
            } else {
                arrays[i] = sdsnewlen(vstr, vlen);
            }

            /* Move to next node */
            if (reverse) {
                zzlPrev(zl,&eptr,&sptr);
            } else {
                zzlNext(zl,&eptr,&sptr);
            }

            ++i;
            if (i >= len) break;
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;

        /* If reversed, get the last node in range as starting point. */
        if (reverse) {
            ln = zslLastInLexRange(zsl,&range);
        } else {
            ln = zslFirstInLexRange(zsl,&range);
        }

        /* No "first" element in the specified interval. */
        if (ln == NULL) {
            *members_size = 0;
            zslFreeLexRange(&range);
            return C_OK;
        }

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */
        while (ln && offset--) {
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }

        while (ln && limit--) {
            /* Abort when the node is no longer in range. */
            if (reverse) {
                if (!zslLexValueGteMin(ln->ele,&range)) break;
            } else {
                if (!zslLexValueLteMax(ln->ele,&range)) break;
            }

            rangelen++;
            arrays[i] = sdsdup(ln->ele);

            /* Move to next node */
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }

            ++i;
            if (i >= len) break;
        }
    } else {
        zfree(*members);
        zslFreeLexRange(&range);
        return C_ERR;
    }

    zslFreeLexRange(&range);
    *members_size = rangelen;
    return C_OK;
}

static int zremrangeGenericCommand(redisDb *redis_db, robj *kobj,
                                   robj *minobj, robj *maxobj,
                                   int rangetype)
{
    robj *zobj;
    unsigned long deleted = 0;
    zrangespec range;
    zlexrangespec lexrange;
    long start, end, llen;

    /* Step 1: Parse the range. */
    if (rangetype == ZRANGE_RANK) {
        if ((getLongFromObject(minobj,&start) != C_OK) ||
            (getLongFromObject(maxobj,&end) != C_OK))
            return C_ERR;
    } else if (rangetype == ZRANGE_SCORE) {
        if (zslParseRange(minobj,maxobj,&range) != C_OK) {
            return C_ERR;
        }
    } else if (rangetype == ZRANGE_LEX) {
        if (zslParseLexRange(minobj,maxobj,&lexrange) != C_OK) {
            return C_ERR;
        }
    }

    /* Step 2: Lookup & range sanity checks if needed. */
    if ((zobj = lookupKeyWrite(redis_db,kobj)) == NULL || checkType(zobj,OBJ_ZSET)) {
        if (rangetype == ZRANGE_LEX) zslFreeLexRange(&lexrange);
        return REDIS_KEY_NOT_EXIST;
    }

    if (rangetype == ZRANGE_RANK) {
        /* Sanitize indexes. */
        llen = zsetLength(zobj);
        if (start < 0) start = llen+start;
        if (end < 0) end = llen+end;
        if (start < 0) start = 0;

        /* Invariant: start >= 0, so this test will be true when end < 0.
         * The range is empty when start > end or start >= length. */
        if (start > end || start >= llen) {
            goto cleanup;
        }
        if (end >= llen) end = llen-1;
    }

    /* Step 3: Perform the range deletion operation. */
    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        switch(rangetype) {
        case ZRANGE_RANK:
            zobj->ptr = zzlDeleteRangeByRank(zobj->ptr,start+1,end+1,&deleted);
            break;
        case ZRANGE_SCORE:
            zobj->ptr = zzlDeleteRangeByScore(zobj->ptr,&range,&deleted);
            break;
        case ZRANGE_LEX:
            zobj->ptr = zzlDeleteRangeByLex(zobj->ptr,&lexrange,&deleted);
            break;
        }
        if (zzlLength(zobj->ptr) == 0) {
            dbDelete(redis_db,kobj);
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        switch(rangetype) {
        case ZRANGE_RANK:
            deleted = zslDeleteRangeByRank(zs->zsl,start+1,end+1,zs->dict);
            break;
        case ZRANGE_SCORE:
            deleted = zslDeleteRangeByScore(zs->zsl,&range,zs->dict);
            break;
        case ZRANGE_LEX:
            deleted = zslDeleteRangeByLex(zs->zsl,&lexrange,zs->dict);
            break;
        }
        if (htNeedsResize(zs->dict)) dictResize(zs->dict);
        if (dictSize(zs->dict) == 0) {
            dbDelete(redis_db,kobj);
        }
    } else {
        goto cleanup;
    }

    if (rangetype == ZRANGE_LEX) zslFreeLexRange(&lexrange);
    return C_OK;

cleanup:
    if (rangetype == ZRANGE_LEX) zslFreeLexRange(&lexrange);
    return C_ERR;
}

static int zrankGenericCommand(redisDb *redis_db, robj *kobj, robj *mobj, long *rank, int reverse)
{
    robj *zobj;
    if ((zobj = lookupKeyRead(redis_db,kobj)) == NULL || checkType(zobj,OBJ_ZSET)) {
        return REDIS_KEY_NOT_EXIST;
    }

    assert(sdsEncodedObject(mobj));
    *rank = zsetRank(zobj,mobj->ptr,reverse);
    return (*rank >= 0) ? C_OK : REDIS_ITEM_NOT_EXIST;
}

int RsZAdd(redisDbIF *db, robj *key, robj *items[], unsigned long items_size)
{
    if (NULL == db || NULL == key || NULL == items) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    return zaddGenericCommand(redis_db, key, items, items_size, ZADD_NONE);
}

int RsZCard(redisDbIF *db, robj *key, unsigned long *len)
{
    if (NULL == db || NULL == key) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    robj *zobj;
    if ((zobj = lookupKeyRead(redis_db,key)) == NULL || checkType(zobj,OBJ_ZSET)) {
        return REDIS_KEY_NOT_EXIST;
    }

    *len = zsetLength(zobj);

    return C_OK;
}

int RsZCount(redisDbIF *db, robj *key, robj *min, robj *max, unsigned long *len)
{
    if (NULL == db || NULL == key || NULL == min || NULL == max) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    robj *zobj;
    if ((zobj = lookupKeyRead(redis_db,key)) == NULL || checkType(zobj,OBJ_ZSET)) {
        return REDIS_KEY_NOT_EXIST;
    }

    /* Parse the range arguments */
    zrangespec range;
    if (zslParseRange(min,max,&range) != C_OK) {
        return C_ERR;
    }

    unsigned long count = 0;
    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        double score;

        /* Use the first element in range as the starting point */
        eptr = zzlFirstInRange(zl,&range);

        /* No "first" element */
        if (eptr == NULL) {
            *len = 0;
            return C_OK;
        }

        /* First element is in range */
        sptr = ziplistNext(zl,eptr);
        score = zzlGetScore(sptr);
        assert(zslValueLteMax(score,&range));

        /* Iterate over elements in range */
        while (eptr) {
            score = zzlGetScore(sptr);

            /* Abort when the node is no longer in range. */
            if (!zslValueLteMax(score,&range)) {
                break;
            } else {
                count++;
                zzlNext(zl,&eptr,&sptr);
            }
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *zn;
        unsigned long rank;

        /* Find first element in range */
        zn = zslFirstInRange(zsl, &range);

        /* Use rank of first element, if any, to determine preliminary count */
        if (zn != NULL) {
            rank = zslGetRank(zsl, zn->score, zn->ele);
            count = (zsl->length - (rank - 1));

            /* Find last element in range */
            zn = zslLastInRange(zsl, &range);

            /* Use rank of last element, if any, to determine the actual count */
            if (zn != NULL) {
                rank = zslGetRank(zsl, zn->score, zn->ele);
                count -= (zsl->length - rank);
            }
        }
    } else {
        return C_ERR;
    }

    *len = count;

    return C_OK;
}

int RsZIncrby(redisDbIF *db, robj *key, robj *items[], unsigned long items_size)
{
    if (NULL == db || NULL == key || NULL == items) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    return zaddGenericCommand(redis_db, key, items, items_size, ZADD_INCR);
}

int RsZrange(redisDbIF *db, robj *key, long start, long end, zitem **items, unsigned long *items_size)
{
    if (NULL == db || NULL == key) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    return zrangeGenericCommand(redis_db, key, start, end, items, items_size, 0);
}

int RsZRangebyscore(redisDbIF *db, robj *key,
                    robj *min, robj *max,
                    zitem **items, unsigned long *items_size,
                    long offset, long count)
{
    if (NULL == db || NULL == key || NULL == min || NULL == max) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    return genericZrangebyscoreCommand(redis_db, key, min, max, items, items_size, 0, offset, count);
}

int RsZRank(redisDbIF *db, robj *key, robj *member, long *rank)
{
    if (NULL == db || NULL == key || NULL == member) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    return zrankGenericCommand(redis_db, key, member, rank, 0);
}

int RsZRem(redisDbIF *db, robj *key, robj *members[], unsigned long members_size)
{
    if (NULL == db || NULL == key || NULL == members) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    robj *zobj;
    if ((zobj = lookupKeyWrite(redis_db,key)) == NULL || checkType(zobj,OBJ_ZSET)) {
        return REDIS_KEY_NOT_EXIST;
    }

    unsigned long i = 0;
    for (i = 0; i < members_size; i++) {
        zsetDel(zobj,members[i]->ptr);
        if (zsetLength(zobj) == 0) {
            dbDelete(redis_db,key);
            break;
        }
    }

    return C_OK;
}

int RsZRemrangebyrank(redisDbIF *db, robj *key, robj *min, robj *max)
{
    if (NULL == db || NULL == key) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    return zremrangeGenericCommand(redis_db, key, min, max, ZRANGE_RANK);
}

int RsZRemrangebyscore(redisDbIF *db, robj *key, robj *min, robj *max)
{
    if (NULL == db || NULL == key) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    return zremrangeGenericCommand(redis_db, key, min, max, ZRANGE_SCORE);
}

int RsZRevrange(redisDbIF *db, robj *key,
                long start, long end,
                zitem **items, unsigned long *items_size)
{
    if (NULL == db || NULL == key) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    return zrangeGenericCommand(redis_db, key, start, end, items, items_size, 1);
}

int RsZRevrangebyscore(redisDbIF *db, robj *key,
                       robj *min, robj *max,
                       zitem **items, unsigned long *items_size,
                       long offset, long count)
{
    if (NULL == db || NULL == key) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    return genericZrangebyscoreCommand(redis_db, key, min, max, items, items_size, 1, offset, count);
}

int RsZRevrangebylex(redisDbIF *db, robj *key,
                     robj *min, robj *max,
                     sds **members, unsigned long *members_size)
{
    if (NULL == db || NULL == key || NULL == min || NULL == max) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    return genericZrangebylexCommand(redis_db, key, min, max, members, members_size, 1);
}

int RsZRevrank(redisDbIF *db, robj *key, robj *member, long *rank)
{
    if (NULL == db || NULL == key || NULL == member) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    return zrankGenericCommand(redis_db, key, member, rank, 1);
}

int RsZScore(redisDbIF *db, robj *key, robj *member, double *score)
{
    if (NULL == db || NULL == key || NULL == member) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    robj *zobj;
    if ((zobj = lookupKeyRead(redis_db,key)) == NULL || checkType(zobj,OBJ_ZSET)) {
        return REDIS_KEY_NOT_EXIST;
    }

    if (zsetScore(zobj,member->ptr,score) == C_ERR) {
        return REDIS_ITEM_NOT_EXIST;
    }

    return C_OK;
}

int RsZRangebylex(redisDbIF *db, robj *key,
                  robj *min, robj *max,
                  sds **members, unsigned long *members_size)
{
    if (NULL == db || NULL == key || NULL == min || NULL == max) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    return genericZrangebylexCommand(redis_db, key, min, max, members, members_size, 0);
}

int RsZLexcount(redisDbIF *db, robj *key, robj *min, robj *max, unsigned long *len)
{
    if (NULL == db || NULL == key || NULL == min || NULL == max) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    robj *zobj;
    if ((zobj = lookupKeyRead(redis_db,key)) == NULL || checkType(zobj,OBJ_ZSET)) {
        return REDIS_KEY_NOT_EXIST;
    }

    /* Parse the range arguments */
    zlexrangespec range;
    if (zslParseLexRange(min,max,&range) != C_OK) {
        return C_ERR;
    }

    int count = 0;
    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;

        /* Use the first element in range as the starting point */
        eptr = zzlFirstInLexRange(zl,&range);

        /* No "first" element */
        if (eptr == NULL) {
            *len = 0;
            zslFreeLexRange(&range);
            return C_OK;
        }

        /* First element is in range */
        sptr = ziplistNext(zl,eptr);
        assert(zzlLexValueLteMax(eptr,&range));

        /* Iterate over elements in range */
        while (eptr) {
            /* Abort when the node is no longer in range. */
            if (!zzlLexValueLteMax(eptr,&range)) {
                break;
            } else {
                count++;
                zzlNext(zl,&eptr,&sptr);
            }
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *zn;
        unsigned long rank;

        /* Find first element in range */
        zn = zslFirstInLexRange(zsl, &range);

        /* Use rank of first element, if any, to determine preliminary count */
        if (zn != NULL) {
            rank = zslGetRank(zsl, zn->score, zn->ele);
            count = (zsl->length - (rank - 1));

            /* Find last element in range */
            zn = zslLastInLexRange(zsl, &range);

            /* Use rank of last element, if any, to determine the actual count */
            if (zn != NULL) {
                rank = zslGetRank(zsl, zn->score, zn->ele);
                count -= (zsl->length - rank);
            }
        }
    } else {
        zslFreeLexRange(&range);
        return C_ERR;
    }

    *len = count;

    zslFreeLexRange(&range);
    return C_OK;
}

int RsZRemrangebylex(redisDbIF *db, robj *key, robj *min, robj *max)
{
    if (NULL == db || NULL == key) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    return zremrangeGenericCommand(redis_db, key, min, max, ZRANGE_LEX);
}
