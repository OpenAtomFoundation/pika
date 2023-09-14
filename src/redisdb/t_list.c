#include <string.h>
#include <assert.h>
#include <limits.h>

#include "redisdbIF.h"
#include "commondef.h"
#include "commonfunc.h"
#include "object.h"
#include "zmalloc.h"
#include "db.h"
#include "util.h"
#include "quicklist.h"

/* Structure to hold list iteration abstraction. */
typedef struct {
    robj *subject;
    unsigned char encoding;
    unsigned char direction; /* Iteration direction */
    quicklistIter *iter;
} listTypeIterator;

/* Structure for an entry while iterating over a list. */
typedef struct {
    listTypeIterator *li;
    quicklistEntry entry; /* Entry in quicklist */
} listTypeEntry;


/* The function pushes an element to the specified list object 'subject',
 * at head or tail position as specified by 'where'.
 *
 * There is no need for the caller to increment the refcount of 'value' as
 * the function takes care of it if needed. */
void listTypePush(robj *subject, robj *value, int where) {
    if (subject->encoding == OBJ_ENCODING_QUICKLIST) {
        int pos = (where == LIST_HEAD) ? QUICKLIST_HEAD : QUICKLIST_TAIL;
        value = getDecodedObject(value);
        size_t len = sdslen(value->ptr);
        quicklistPush(subject->ptr, value->ptr, len, pos);
        decrRefCount(value);
    } else {
        // serverPanic("Unknown list encoding");
    }
}

void *listPopSaver(unsigned char *data, unsigned int sz) {
    return createStringObject((char*)data,sz);
}

robj *listTypePop(robj *subject, int where) {
    long long vlong;
    robj *value = NULL;

    int ql_where = where == LIST_HEAD ? QUICKLIST_HEAD : QUICKLIST_TAIL;
    if (subject->encoding == OBJ_ENCODING_QUICKLIST) {
        if (quicklistPopCustom(subject->ptr, ql_where, (unsigned char **)&value,
                               NULL, &vlong, listPopSaver)) {
            if (!value)
                value = createStringObjectFromLongLong(vlong);
        }
    } else {
        // serverPanic("Unknown list encoding");
    }
    return value;
}

unsigned long listTypeLength(const robj *subject) {
    if (subject->encoding == OBJ_ENCODING_QUICKLIST) {
        return quicklistCount(subject->ptr);
    } else {
        return C_ERR;
        // serverPanic("Unknown list encoding");
    }
}

/* Initialize an iterator at the specified index. */
listTypeIterator *listTypeInitIterator(robj *subject, long index,
                                       unsigned char direction) {
    listTypeIterator *li = zmalloc(sizeof(listTypeIterator));
    li->subject = subject;
    li->encoding = subject->encoding;
    li->direction = direction;
    li->iter = NULL;
    /* LIST_HEAD means start at TAIL and move *towards* head.
     * LIST_TAIL means start at HEAD and move *towards tail. */
    int iter_direction =
        direction == LIST_HEAD ? AL_START_TAIL : AL_START_HEAD;
    if (li->encoding == OBJ_ENCODING_QUICKLIST) {
        li->iter = quicklistGetIteratorAtIdx(li->subject->ptr,
                                             iter_direction, index);
    } else {
        return NULL;
        // serverPanic("Unknown list encoding");
    }
    return li;
}

/* Clean up the iterator. */
void listTypeReleaseIterator(listTypeIterator *li) {
    zfree(li->iter);
    zfree(li);
}

/* Stores pointer to current the entry in the provided entry structure
 * and advances the position of the iterator. Returns 1 when the current
 * entry is in fact an entry, 0 otherwise. */
int listTypeNext(listTypeIterator *li, listTypeEntry *entry) {
    /* Protect from converting when iterating */
    assert(li->subject->encoding == li->encoding);

    entry->li = li;
    if (li->encoding == OBJ_ENCODING_QUICKLIST) {
        return quicklistNext(li->iter, &entry->entry);
    } else {
        return C_ERR;
        // serverPanic("Unknown list encoding");
    }
    return 0;
}

void listTypeInsert(listTypeEntry *entry, robj *value, int where) {
    if (entry->li->encoding == OBJ_ENCODING_QUICKLIST) {
        value = getDecodedObject(value);
        sds str = value->ptr;
        size_t len = sdslen(str);
        if (where == LIST_TAIL) {
            quicklistInsertAfter((quicklist *)entry->entry.quicklist,
                                 &entry->entry, str, len);
        } else if (where == LIST_HEAD) {
            quicklistInsertBefore((quicklist *)entry->entry.quicklist,
                                  &entry->entry, str, len);
        }
        decrRefCount(value);
    } else {
        // serverPanic("Unknown list encoding");
    }
}

/* Compare the given object with the entry at the current position. */
int listTypeEqual(listTypeEntry *entry, robj *o) {
    if (entry->li->encoding == OBJ_ENCODING_QUICKLIST) {
        assert(sdsEncodedObject(o));
        return quicklistCompare(entry->entry.zi,o->ptr,sdslen(o->ptr));
    } else {
        return C_ERR;
        // serverPanic("Unknown list encoding");
    }
}

/* Delete the element pointed to. */
void listTypeDelete(listTypeIterator *iter, listTypeEntry *entry) {
    if (entry->li->encoding == OBJ_ENCODING_QUICKLIST) {
        quicklistDelEntry(iter->iter, &entry->entry);
    } else {
        // serverPanic("Unknown list encoding");
    }
}

static int pushGenericCommand(redisDb *redis_db, robj *kobj, robj *vals[], unsigned long vals_size, int where) {

    robj *lobj = lookupKeyWrite(redis_db,kobj);
    if (lobj && lobj->type != OBJ_LIST) {
        return C_ERR;
    }

    unsigned long i;
    for (i = 0; i < vals_size; i++) {
        if (!lobj) {
            lobj = createQuicklistObject();
            quicklistSetOptions(lobj->ptr, OBJ_LIST_MAX_ZIPLIST_SIZE,
                                OBJ_LIST_COMPRESS_DEPTH);
            dbAdd(redis_db,kobj,lobj);
        }
        listTypePush(lobj,vals[i],where);
    }

    return C_OK;
}

static int pushxGenericCommand(redisDb *redis_db, robj *kobj, robj *vals[], unsigned long vals_size, int where) {

    robj *subject;
    if ((subject = lookupKeyWrite(redis_db,kobj)) == NULL || checkType(subject,OBJ_LIST)) {
        return REDIS_KEY_NOT_EXIST;
    }

    unsigned long i;
    for (i = 0; i < vals_size; i++) {
        listTypePush(subject,vals[i],where);
    }

    return C_OK;
}

static int popGenericCommand(redisDb *redis_db, robj *kobj, sds *element, int where) {
    robj *o = lookupKeyWrite(redis_db,kobj);
    if (o == NULL || checkType(o,OBJ_LIST)) {
        return REDIS_KEY_NOT_EXIST;
    }

    robj *value = listTypePop(o,where);
    if (value == NULL) {
        return C_ERR;
    } else {
        if (listTypeLength(o) == 0) {
            dbDelete(redis_db,kobj);
        }

        convertObjectToSds(value, element);
        decrRefCount(value);
        return C_OK;
    }
}

int RsLIndex(redisDbIF *db, robj *key, long index, sds *element)
{
    if (NULL == db || NULL == key) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    robj *o;
    if ((o = lookupKeyRead(redis_db,key)) == NULL || checkType(o,OBJ_LIST)) {
        return REDIS_KEY_NOT_EXIST;
    }

    if (o->encoding == OBJ_ENCODING_QUICKLIST) {
        quicklistEntry entry;
        if (quicklistIndex(o->ptr, index, &entry)) {
            if (entry.value) {
                *element = sdsnewlen(entry.value, entry.sz);
            } else {
                *element = sdsfromlonglong(entry.longval);
            }
        } else {
            return REDIS_ITEM_NOT_EXIST;
        }
    } else {
        return C_ERR;
    }

    return C_OK;
}

int RsLInsert(redisDbIF *db, robj *key, int where, robj *pivot, robj *val)
{
    if (NULL == db || NULL == key || NULL == pivot || NULL == val) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    robj *subject;
    if ((subject = lookupKeyWrite(redis_db,key)) == NULL || checkType(subject,OBJ_LIST)) {
        return REDIS_KEY_NOT_EXIST;
    }

    /* Seek pivot from head to tail */
    listTypeEntry entry;
    listTypeIterator *iter = listTypeInitIterator(subject,0,LIST_TAIL);
    while (listTypeNext(iter,&entry)) {
        if (listTypeEqual(&entry,pivot)) {
            listTypeInsert(&entry,val,where);
            break;
        }
    }
    listTypeReleaseIterator(iter);

    return C_OK;
}

int RsLLen(redisDbIF *db, robj *key, unsigned long *len)
{
    if (NULL == db || NULL == key) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    robj *o;
    if ((o = lookupKeyRead(redis_db,key)) == NULL || checkType(o,OBJ_LIST)) {
        return REDIS_KEY_NOT_EXIST;
    }

    *len = listTypeLength(o);

    return C_OK;
}

int RsLPop(redisDbIF *db, robj *key, sds *element)
{
    if (NULL == db || NULL == key) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    return popGenericCommand(redis_db, key, element, LIST_HEAD);
}

int RsLPush(redisDbIF *db, robj *key, robj *vals[], unsigned long vals_size)
{
    if (NULL == db || NULL == key || NULL == vals) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    return pushGenericCommand(redis_db, key, vals, vals_size, LIST_HEAD);
}

int RsLPushx(redisDbIF *db, robj *key, robj *vals[], unsigned long vals_size)
{
    if (NULL == db || NULL == key || NULL == vals) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    return pushxGenericCommand(redis_db, key, vals, vals_size, LIST_HEAD);
}

int RsLRange(redisDbIF *db, robj *key, long start, long end, sds **vals, unsigned long *vals_size)
{
    if (NULL == db || NULL == key || NULL == vals) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    robj *o;
    if ((o = lookupKeyRead(redis_db,key)) == NULL || checkType(o,OBJ_LIST)) {
        return REDIS_KEY_NOT_EXIST;
    }

    long llen = listTypeLength(o);

    /* convert negative indexes */
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */
    if (start > end || start >= llen) {
        *vals_size = 0;
        return C_OK;
    }
    if (end >= llen) end = llen-1;
    long rangelen = (end-start)+1;
    *vals_size = rangelen;

    *vals = (sds *)zcalloc(sizeof(sds) * rangelen);
    sds *array = *vals;

    /* Return the result in form of a multi-bulk reply */
    if (o->encoding == OBJ_ENCODING_QUICKLIST) {
        listTypeIterator *iter = listTypeInitIterator(o, start, LIST_TAIL);

        int i = 0;
        while(rangelen--) {
            listTypeEntry entry;
            listTypeNext(iter, &entry);
            quicklistEntry *qe = &entry.entry;
            if (qe->value) {
                array[i] = sdsnewlen(qe->value,qe->sz);
            } else {
                array[i] = sdsfromlonglong(qe->longval);
            }
            ++i;
        }
        listTypeReleaseIterator(iter);
    } else {
        // serverPanic("List encoding is not QUICKLIST!");
    }

    return C_OK;
}

int RsLRem(redisDbIF *db, robj *key, long count, robj *val)
{
    if (NULL == db || NULL == key || NULL == val) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    robj *subject;
    if ((subject = lookupKeyWrite(redis_db,key)) == NULL || checkType(subject,OBJ_LIST)) {
        return REDIS_KEY_NOT_EXIST;
    }

    listTypeIterator *li;
    if (count < 0) {
        count = -count;
        li = listTypeInitIterator(subject,-1,LIST_HEAD);
    } else {
        li = listTypeInitIterator(subject,0,LIST_TAIL);
    }

    long removed = 0;
    listTypeEntry entry;
    while (listTypeNext(li,&entry)) {
        if (listTypeEqual(&entry,val)) {
            listTypeDelete(li, &entry);
            removed++;
            if (count && removed == count) break;
        }
    }
    listTypeReleaseIterator(li);

    if (listTypeLength(subject) == 0) {
        dbDelete(redis_db,key);
    }

    return C_OK;
}

int RsLSet(redisDbIF *db, robj *key, long index, robj *val)
{
    if (NULL == db || NULL == key || NULL == val) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    robj *o;
    if ((o = lookupKeyWrite(redis_db,key)) == NULL || checkType(o,OBJ_LIST)) {
        return REDIS_KEY_NOT_EXIST;
    }

    if (o->encoding == OBJ_ENCODING_QUICKLIST) {
        quicklist *ql = o->ptr;
        int replaced = quicklistReplaceAtIndex(ql, index,
                                               val->ptr, sdslen(val->ptr));
        if (!replaced) {
            return REDIS_ITEM_NOT_EXIST;
        }
    } else {
        return C_ERR;
    }

    return C_OK;
}

int RsLTrim(redisDbIF *db, robj *key, long start, long end)
{
    if (NULL == db || NULL == key) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    robj *o;
    if ((o = lookupKeyWrite(redis_db,key)) == NULL || checkType(o,OBJ_LIST)) {
        return REDIS_KEY_NOT_EXIST;
    }

    /* convert negative indexes */
    long llen = listTypeLength(o);
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */
    long ltrim, rtrim;
    if (start > end || start >= llen) {
        /* Out of range start or start > end result in empty list */
        ltrim = llen;
        rtrim = 0;
    } else {
        if (end >= llen) end = llen-1;
        ltrim = start;
        rtrim = llen-end-1;
    }

    /* Remove list elements to perform the trim */
    if (o->encoding == OBJ_ENCODING_QUICKLIST) {
        quicklistDelRange(o->ptr,0,ltrim);
        quicklistDelRange(o->ptr,-rtrim,rtrim);
    } else {
        return C_ERR;
    }

    if (listTypeLength(o) == 0) {
        dbDelete(redis_db,key);
    }

    return C_OK;
}

int RsRPop(redisDbIF *db, robj *key, sds *element)
{
    if (NULL == db || NULL == key) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    return popGenericCommand(redis_db, key, element, LIST_TAIL);
}

int RsRPush(redisDbIF *db, robj *key, robj *vals[], unsigned long vals_size)
{
    if (NULL == db || NULL == key || NULL == vals) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    return pushGenericCommand(redis_db, key, vals, vals_size, LIST_TAIL);
}

int RsRPushx(redisDbIF *db, robj *key, robj *vals[], unsigned long vals_size)
{
    if (NULL == db || NULL == key || NULL == vals) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    return pushxGenericCommand(redis_db, key, vals, vals_size, LIST_TAIL);
}
