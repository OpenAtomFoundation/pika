#include <string.h>
#include <assert.h>
#include <limits.h>

#include "redisdbIF.h"
#include "commondef.h"
#include "commonfunc.h"
#include "object.h"
#include "zmalloc.h"
#include "db.h"
#include "ziplist.h"
#include "util.h"

extern dictType hashDictType;

/* Structure to hold hash iteration abstraction. Note that iteration over
 * hashes involves both fields and values. Because it is possible that
 * not both are required, store pointers in the iterator to avoid
 * unnecessary memory allocation for fields/values. */
typedef struct {
    robj *subject;
    int encoding;

    unsigned char *fptr, *vptr;

    dictIterator *di;
    dictEntry *de;
} hashTypeIterator;


/* Get the field or value at iterator cursor, for an iterator on a hash value
 * encoded as a hash table. Prototype is similar to
 * `hashTypeGetFromHashTable`. */
sds hashTypeCurrentFromHashTable(hashTypeIterator *hi, int what) {
    assert(hi->encoding == OBJ_ENCODING_HT);

    if (what & OBJ_HASH_KEY) {
        return dictGetKey(hi->de);
    } else {
        return dictGetVal(hi->de);
    }
}

/* Get the field or value at iterator cursor, for an iterator on a hash value
 * encoded as a ziplist. Prototype is similar to `hashTypeGetFromZiplist`. */
void hashTypeCurrentFromZiplist(hashTypeIterator *hi, int what,
                                unsigned char **vstr,
                                unsigned int *vlen,
                                long long *vll)
{
    int ret;

    assert(hi->encoding == OBJ_ENCODING_ZIPLIST);

    if (what & OBJ_HASH_KEY) {
        ret = ziplistGet(hi->fptr, vstr, vlen, vll);
        assert(ret);
    } else {
        ret = ziplistGet(hi->vptr, vstr, vlen, vll);
        assert(ret);
    }
}

/* Higher level function of hashTypeCurrent*() that returns the hash value
 * at current iterator position.
 *
 * The returned element is returned by reference in either *vstr and *vlen if
 * it's returned in string form, or stored in *vll if it's returned as
 * a number.
 *
 * If *vll is populated *vstr is set to NULL, so the caller
 * can always check the function return by checking the return value
 * type checking if vstr == NULL. */
void hashTypeCurrentObject(hashTypeIterator *hi, int what, unsigned char **vstr, unsigned int *vlen, long long *vll) {
    if (hi->encoding == OBJ_ENCODING_ZIPLIST) {
        *vstr = NULL;
        hashTypeCurrentFromZiplist(hi, what, vstr, vlen, vll);
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        sds ele = hashTypeCurrentFromHashTable(hi, what);
        *vstr = (unsigned char*) ele;
        *vlen = sdslen(ele);
    } else {
        // serverPanic("Unknown hash encoding");
    }
}

/* Return the key or value at the current iterator position as a new
 * SDS string. */
sds hashTypeCurrentObjectNewSds(hashTypeIterator *hi, int what) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vll;

    hashTypeCurrentObject(hi,what,&vstr,&vlen,&vll);
    if (vstr) return sdsnewlen(vstr,vlen);
    return sdsfromlonglong(vll);
}

/* Move to the next entry in the hash. Return C_OK when the next entry
 * could be found and C_ERR when the iterator reaches the end. */
int hashTypeNext(hashTypeIterator *hi) {
    if (hi->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl;
        unsigned char *fptr, *vptr;

        zl = hi->subject->ptr;
        fptr = hi->fptr;
        vptr = hi->vptr;

        if (fptr == NULL) {
            /* Initialize cursor */
            assert(vptr == NULL);
            fptr = ziplistIndex(zl, 0);
        } else {
            /* Advance cursor */
            assert(vptr != NULL);
            fptr = ziplistNext(zl, vptr);
        }
        if (fptr == NULL) return C_ERR;

        /* Grab pointer to the value (fptr points to the field) */
        vptr = ziplistNext(zl, fptr);
        assert(vptr != NULL);

        /* fptr, vptr now point to the first or next pair */
        hi->fptr = fptr;
        hi->vptr = vptr;
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        if ((hi->de = dictNext(hi->di)) == NULL) return C_ERR;
    } else {
        // serverPanic("Unknown hash encoding");
    }
    return C_OK;
}

hashTypeIterator *hashTypeInitIterator(robj *subject) {
    hashTypeIterator *hi = zmalloc(sizeof(hashTypeIterator));
    hi->subject = subject;
    hi->encoding = subject->encoding;

    if (hi->encoding == OBJ_ENCODING_ZIPLIST) {
        hi->fptr = NULL;
        hi->vptr = NULL;
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        hi->di = dictGetIterator(subject->ptr);
    } else {
        // serverPanic("Unknown hash encoding");
    }
    return hi;
}

void hashTypeReleaseIterator(hashTypeIterator *hi) {
    if (hi->encoding == OBJ_ENCODING_HT)
        dictReleaseIterator(hi->di);
    zfree(hi);
}

void hashTypeConvertZiplist(robj *o, int enc) {

    assert(o->encoding == OBJ_ENCODING_ZIPLIST);

    if (enc == OBJ_ENCODING_ZIPLIST) {
        /* Nothing to do... */

    } else if (enc == OBJ_ENCODING_HT) {
        hashTypeIterator *hi;
        dict *dict;
        int ret;

        hi = hashTypeInitIterator(o);
        dict = dictCreate(&hashDictType, NULL);

        while (hashTypeNext(hi) != C_ERR) {
            sds key, value;

            key = hashTypeCurrentObjectNewSds(hi,OBJ_HASH_KEY);
            value = hashTypeCurrentObjectNewSds(hi,OBJ_HASH_VALUE);
            ret = dictAdd(dict, key, value);
            if (ret != DICT_OK) {
                // serverLogHexDump(LL_WARNING,"ziplist with dup elements dump",
                //     o->ptr,ziplistBlobLen(o->ptr));
                // serverPanic("Ziplist corruption detected");
            }
        }
        hashTypeReleaseIterator(hi);
        zfree(o->ptr);
        o->encoding = OBJ_ENCODING_HT;
        o->ptr = dict;
    } else {
        // serverPanic("Unknown hash encoding");
    }
}

void hashTypeConvert(robj *o, int enc) {
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        hashTypeConvertZiplist(o, enc);
    } else if (o->encoding == OBJ_ENCODING_HT) {
        // serverPanic("Not implemented");
    } else {
        // serverPanic("Unknown hash encoding");
    }
}

/* Delete an element from a hash.
 * Return 1 on deleted and 0 on not found. */
int hashTypeDelete(robj *o, sds field) {
    int deleted = 0;

    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl, *fptr;

        zl = o->ptr;
        fptr = ziplistIndex(zl, ZIPLIST_HEAD);
        if (fptr != NULL) {
            fptr = ziplistFind(fptr, (unsigned char*)field, sdslen(field), 1);
            if (fptr != NULL) {
                zl = ziplistDelete(zl,&fptr); /* Delete the key. */
                zl = ziplistDelete(zl,&fptr); /* Delete the value. */
                o->ptr = zl;
                deleted = 1;
            }
        }
    } else if (o->encoding == OBJ_ENCODING_HT) {
        if (dictDelete((dict*)o->ptr, field) == C_OK) {
            deleted = 1;

            /* Always check if the dictionary needs a resize after a delete. */
            if (htNeedsResize(o->ptr)) dictResize(o->ptr);
        }

    } else {
        // serverPanic("Unknown hash encoding");
    }
    return deleted;
}

/* Return the number of elements in a hash. */
unsigned long hashTypeLength(const robj *o) {
    unsigned long length = ULONG_MAX;

    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        length = ziplistLen(o->ptr) / 2;
    } else if (o->encoding == OBJ_ENCODING_HT) {
        length = dictSize((const dict*)o->ptr);
    } else {
        // serverPanic("Unknown hash encoding");
    }
    return length;
}

/* Get the value from a ziplist encoded hash, identified by field.
 * Returns -1 when the field cannot be found. */
int hashTypeGetFromZiplist(robj *o, sds field,
                           unsigned char **vstr,
                           unsigned int *vlen,
                           long long *vll)
{
    unsigned char *zl, *fptr = NULL, *vptr = NULL;
    int ret;

    assert(o->encoding == OBJ_ENCODING_ZIPLIST);

    zl = o->ptr;
    fptr = ziplistIndex(zl, ZIPLIST_HEAD);
    if (fptr != NULL) {
        fptr = ziplistFind(fptr, (unsigned char*)field, sdslen(field), 1);
        if (fptr != NULL) {
            /* Grab pointer to the value (fptr points to the field) */
            vptr = ziplistNext(zl, fptr);
            assert(vptr != NULL);
        }
    }

    if (vptr != NULL) {
        ret = ziplistGet(vptr, vstr, vlen, vll);
        assert(ret);
        return 0;
    }

    return -1;
}

/* Get the value from a hash table encoded hash, identified by field.
 * Returns NULL when the field cannot be found, otherwise the SDS value
 * is returned. */
sds hashTypeGetFromHashTable(robj *o, sds field) {
    dictEntry *de;

    assert(o->encoding == OBJ_ENCODING_HT);

    de = dictFind(o->ptr, field);
    if (de == NULL) return NULL;
    return dictGetVal(de);
}

/* Higher level function of hashTypeGet*() that returns the hash value
 * associated with the specified field. If the field is found C_OK
 * is returned, otherwise C_ERR. The returned object is returned by
 * reference in either *vstr and *vlen if it's returned in string form,
 * or stored in *vll if it's returned as a number.
 *
 * If *vll is populated *vstr is set to NULL, so the caller
 * can always check the function return by checking the return value
 * for C_OK and checking if vll (or vstr) is NULL. */
int hashTypeGetValue(robj *o, sds field, unsigned char **vstr, unsigned int *vlen, long long *vll) {
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        *vstr = NULL;
        if (hashTypeGetFromZiplist(o, field, vstr, vlen, vll) == 0)
            return C_OK;
    } else if (o->encoding == OBJ_ENCODING_HT) {
        sds value;
        if ((value = hashTypeGetFromHashTable(o, field)) != NULL) {
            *vstr = (unsigned char*) value;
            *vlen = sdslen(value);
            return C_OK;
        }
    } else {
        // serverPanic("Unknown hash encoding");
    }
    return C_ERR;
}

/* Higher level function using hashTypeGet*() to return the length of the
 * object associated with the requested field, or 0 if the field does not
 * exist. */
size_t hashTypeGetValueLength(robj *o, sds field) {
    size_t len = 0;
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        if (hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll) == 0)
            len = vstr ? vlen : sdigits10(vll);
    } else if (o->encoding == OBJ_ENCODING_HT) {
        sds aux;

        if ((aux = hashTypeGetFromHashTable(o, field)) != NULL)
            len = sdslen(aux);
    } else {
        // serverPanic("Unknown hash encoding");
    }
    return len;
}

/* Test if the specified field exists in the given hash. Returns 1 if the field
 * exists, and 0 when it doesn't. */
int hashTypeExists(robj *o, sds field) {
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        if (hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll) == 0) return 1;
    } else if (o->encoding == OBJ_ENCODING_HT) {
        if (hashTypeGetFromHashTable(o, field) != NULL) return 1;
    } else {
        // serverPanic("Unknown hash encoding");
    }
    return 0;
}

/* Add a new field, overwrite the old with the new value if it already exists.
 * Return 0 on insert and 1 on update.
 *
 * By default, the key and value SDS strings are copied if needed, so the
 * caller retains ownership of the strings passed. However this behavior
 * can be effected by passing appropriate flags (possibly bitwise OR-ed):
 *
 * HASH_SET_TAKE_FIELD -- The SDS field ownership passes to the function.
 * HASH_SET_TAKE_VALUE -- The SDS value ownership passes to the function.
 *
 * When the flags are used the caller does not need to release the passed
 * SDS string(s). It's up to the function to use the string to create a new
 * entry or to free the SDS string before returning to the caller.
 *
 * HASH_SET_COPY corresponds to no flags passed, and means the default
 * semantics of copying the values if needed.
 *
 */
#define HASH_SET_TAKE_FIELD (1<<0)
#define HASH_SET_TAKE_VALUE (1<<1)
#define HASH_SET_COPY 0
int hashTypeSet(robj *o, sds field, sds value, int flags) {
    int update = 0;

    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl, *fptr, *vptr;

        zl = o->ptr;
        fptr = ziplistIndex(zl, ZIPLIST_HEAD);
        if (fptr != NULL) {
            fptr = ziplistFind(fptr, (unsigned char*)field, sdslen(field), 1);
            if (fptr != NULL) {
                /* Grab pointer to the value (fptr points to the field) */
                vptr = ziplistNext(zl, fptr);
                assert(vptr != NULL);
                update = 1;

                /* Delete value */
                zl = ziplistDelete(zl, &vptr);

                /* Insert new value */
                zl = ziplistInsert(zl, vptr, (unsigned char*)value,
                        sdslen(value));
            }
        }

        if (!update) {
            /* Push new field/value pair onto the tail of the ziplist */
            zl = ziplistPush(zl, (unsigned char*)field, sdslen(field),
                    ZIPLIST_TAIL);
            zl = ziplistPush(zl, (unsigned char*)value, sdslen(value),
                    ZIPLIST_TAIL);
        }
        o->ptr = zl;

        /* Check if the ziplist needs to be converted to a hash table */
        if (hashTypeLength(o) > OBJ_ZSET_MAX_ZIPLIST_ENTRIES)
            hashTypeConvert(o, OBJ_ENCODING_HT);
    } else if (o->encoding == OBJ_ENCODING_HT) {
        dictEntry *de = dictFind(o->ptr,field);
        if (de) {
            sdsfree(dictGetVal(de));
            if (flags & HASH_SET_TAKE_VALUE) {
                dictGetVal(de) = value;
                value = NULL;
            } else {
                dictGetVal(de) = sdsdup(value);
            }
            update = 1;
        } else {
            sds f,v;
            if (flags & HASH_SET_TAKE_FIELD) {
                f = field;
                field = NULL;
            } else {
                f = sdsdup(field);
            }
            if (flags & HASH_SET_TAKE_VALUE) {
                v = value;
                value = NULL;
            } else {
                v = sdsdup(value);
            }
            dictAdd(o->ptr,f,v);
        }
    } else {
        // serverPanic("Unknown hash encoding");
    }

    /* Free SDS strings we did not referenced elsewhere if the flags
     * want this function to be responsible. */
    if (flags & HASH_SET_TAKE_FIELD && field) sdsfree(field);
    if (flags & HASH_SET_TAKE_VALUE && value) sdsfree(value);
    return update;
}

/* Check the length of a number of objects to see if we need to convert a
 * ziplist to a real hash. Note that we only check string encoded objects
 * as their string length can be queried in constant time. */
void hashTypeTryConversion(robj *o, robj **argv, int start, int end) {
    int i;

    if (o->encoding != OBJ_ENCODING_ZIPLIST) return;

    for (i = start; i <= end; i++) {
        if (sdsEncodedObject(argv[i]) &&
            sdslen(argv[i]->ptr) > OBJ_HASH_MAX_ZIPLIST_VALUE)
        {
            hashTypeConvert(o, OBJ_ENCODING_HT);
            break;
        }
    }
}

robj *hashTypeLookupWriteOrCreate(redisDb *redis_db, robj *key)
{
    robj *o = lookupKeyWrite(redis_db,key);
    if (o == NULL) {
        o = createHashObject();
        dbAdd(redis_db,key,o);
    } else {
        if (o->type != OBJ_HASH) {
            return NULL;
        }
    }
    return o;
}

static int HSet(redisDb *redis_db, robj *kobj, robj *fobj, robj *vobj)
{
    robj *o;
    if ((o = hashTypeLookupWriteOrCreate(redis_db,kobj)) == NULL) return C_ERR;

    robj *argv[2];
    argv[0] = fobj;
    argv[1] = vobj;
    hashTypeTryConversion(o,argv,0,1);
    hashTypeSet(o, argv[0]->ptr,argv[1]->ptr,HASH_SET_COPY);
    return C_OK;
}

static int HMSet(redisDb *redis_db, robj *kobj, robj *items[], unsigned long items_size)
{
    robj *o;
    if ((o = hashTypeLookupWriteOrCreate(redis_db,kobj)) == NULL) return C_ERR;

    hashTypeTryConversion(o,items,0,items_size-1);

    unsigned long i;
    for (i = 0; i < items_size; i += 2)
        hashTypeSet(o,items[i]->ptr,items[i+1]->ptr,HASH_SET_COPY);

    return C_OK;
}

static int HSetnx(redisDb *redis_db, robj *kobj, robj *fobj, robj *vobj)
{
    robj *o;
    if ((o = hashTypeLookupWriteOrCreate(redis_db,kobj)) == NULL) return C_ERR;

    robj *argv[2];
    argv[0] = fobj;
    argv[1] = vobj;
    hashTypeTryConversion(o,argv,0,1);
    if (!hashTypeExists(o,argv[0]->ptr)) {
        hashTypeSet(o,argv[0]->ptr,argv[1]->ptr,HASH_SET_COPY);
    }

    return C_OK;
}

static int GetHashFieldValue(robj *o, sds field, sds *val)
{
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        if (0 > hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll)) {
            return REDIS_ITEM_NOT_EXIST;
        } else {
            if (vstr) {
                *val = sdsnewlen(vstr, vlen);
            } else {
                *val = sdsfromlonglong(vll);
            }
        }
    } else if (o->encoding == OBJ_ENCODING_HT) {
        sds value = hashTypeGetFromHashTable(o, field);
        if (value == NULL) {
            return REDIS_ITEM_NOT_EXIST;
        } else {
            *val = sdsdup(value);
        }
    } else {
        return C_ERR;
    }

    return C_OK;
}

static void addHashIteratorCursorToReply(hashTypeIterator *hi, int what, sds *out) {
    if (hi->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        hashTypeCurrentFromZiplist(hi, what, &vstr, &vlen, &vll);
        if (vstr) {
            *out = sdsnewlen(vstr, vlen);
        } else {
            *out = sdsfromlonglong(vll);
        }
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        sds value = hashTypeCurrentFromHashTable(hi, what);
        *out = sdsdup(value);
    } else {
        // serverPanic("Unknown hash encoding");
    }
}

static int genericHgetall(redisDb *redis_db, robj *kobj, hitem **items, unsigned long *items_size, int flags)
{
    robj *o;
    hashTypeIterator *hi;

    if ((o = lookupKeyRead(redis_db,kobj)) == NULL
        || checkType(o,OBJ_HASH)) return REDIS_KEY_NOT_EXIST;

    *items_size = hashTypeLength(o);
    *items = (hitem*)zcalloc(sizeof(hitem) * (*items_size));

    hi = hashTypeInitIterator(o);
    unsigned long i = 0;
    while (hashTypeNext(hi) != C_ERR) {
        if (flags & OBJ_HASH_KEY) {
            addHashIteratorCursorToReply(hi, OBJ_HASH_KEY, &((*items+i)->field));
        }
        if (flags & OBJ_HASH_VALUE) {
            addHashIteratorCursorToReply(hi, OBJ_HASH_VALUE, &((*items+i)->value));
        }

        ++i;
        if (i >= *items_size) break;
    }

    hashTypeReleaseIterator(hi);
    return C_OK;
}

int RsHDel(redisDbIF *db, robj *key, robj *fields[], unsigned long fields_size, unsigned long *ret)
{
    if (NULL == db || NULL == key || NULL == fields) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    robj *o;
    if ((o = lookupKeyRead(redis_db,key)) == NULL || checkType(o,OBJ_HASH)) {
        return REDIS_KEY_NOT_EXIST;
    }

    unsigned long i, deleted = 0;
    for (i = 0; i < fields_size; i++) {
        if (hashTypeDelete(o,fields[i]->ptr)) {
            deleted++;
            if (hashTypeLength(o) == 0) {
                dbDelete(redis_db,key);
                break;
            }
        }
    }
    *ret = deleted;

    return C_OK;
}

int RsHSet(redisDbIF *db, robj *key, robj *field, robj *val)
{
    if (NULL == db || NULL == key || NULL == field || NULL == val) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    return HSet(redis_db, key, field, val);
}

int RsHSetnx(redisDbIF *db, robj *key, robj *field, robj *val)
{
    if (NULL == db || NULL == key || NULL == field || NULL == val) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    return HSetnx(redis_db, key, field, val);
}

int RsHMSet(redisDbIF *db, robj *key, robj *items[], unsigned long items_size)
{
    if (NULL == db || NULL == key || NULL == items) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    return HMSet(redis_db, key, items, items_size);
}

int RsHGet(redisDbIF *db, robj *key, robj *field, sds *val)
{
    if (NULL == db || NULL == key || NULL == field) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    robj *o;
    if ((o = lookupKeyRead(redis_db,key)) == NULL || checkType(o,OBJ_HASH)) {
        return REDIS_KEY_NOT_EXIST;
    }

    return GetHashFieldValue(o, field->ptr, val);
}

int RsHMGet(redisDbIF *db, robj *key, hitem *items, unsigned long items_size)
{
    if (NULL == db || NULL == key || NULL == items) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    robj *o;
    if ((o = lookupKeyRead(redis_db,key)) == NULL || checkType(o,OBJ_HASH)) {
        return REDIS_KEY_NOT_EXIST;
    }

    unsigned long i;
    for (i = 0; i < items_size; ++i) {
        items[i].status = GetHashFieldValue(o, items[i].field, &(items[i].value));
    }

    return C_OK;
}

int RsHGetAll(redisDbIF *db, robj *key, hitem **items, unsigned long *items_size)
{
    if (NULL == db || NULL == key) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    return genericHgetall(redis_db, key, items, items_size, OBJ_HASH_KEY|OBJ_HASH_VALUE);
}

int RsHKeys(redisDbIF *db, robj *key, hitem **items, unsigned long *items_size)
{
    if (NULL == db || NULL == key) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    return genericHgetall(redis_db, key, items, items_size, OBJ_HASH_KEY);
}

int RsHVals(redisDbIF *db, robj *key, hitem **items, unsigned long *items_size)
{
    if (NULL == db || NULL == key) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    return genericHgetall(redis_db, key, items, items_size, OBJ_HASH_VALUE);
}

int RsHExists(redisDbIF *db, robj *key, robj *field, int *is_exist)
{
    if (NULL == db || NULL == key || NULL == field) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    robj *o;
    if ((o = lookupKeyRead(redis_db,key)) == NULL || checkType(o,OBJ_HASH)) {
        return REDIS_KEY_NOT_EXIST;
    }

    *is_exist = hashTypeExists(o,field->ptr);

    return C_OK;
}

int RsHIncrby(redisDbIF *db, robj *key, robj *field, long long val, long long *ret)
{
    if (NULL == db || NULL == key || NULL == field) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    long long value, oldvalue;
    robj *o;
    sds new;
    unsigned char *vstr;
    unsigned int vlen;

    if ((o = hashTypeLookupWriteOrCreate(redis_db,key)) == NULL) {
        return C_ERR;
    }

    if (hashTypeGetValue(o,field->ptr,&vstr,&vlen,&value) == C_OK) {
        if (vstr) {
            if (string2ll((char*)vstr,vlen,&value) == 0) {
                return C_ERR;
            }
        } /* Else hashTypeGetValue() already stored it into &value */
    } else {
        value = 0;
    }

    oldvalue = value;
    if ((val < 0 && oldvalue < 0 && val < (LLONG_MIN-oldvalue)) ||
        (val > 0 && oldvalue > 0 && val > (LLONG_MAX-oldvalue))) {
        return REDIS_OVERFLOW;
    }
    value += val;
    *ret = value;

    new = sdsfromlonglong(value);
    hashTypeSet(o,field->ptr,new,HASH_SET_TAKE_VALUE);

    return C_OK;
}

int RsHIncrbyfloat(redisDbIF *db, robj *key, robj *field, long double val, long double *ret)
{
    if (NULL == db || NULL == key || NULL == field) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    long double value;
    long long ll;
    robj *o;
    sds new;
    unsigned char *vstr;
    unsigned int vlen;

    if ((o = hashTypeLookupWriteOrCreate(redis_db,key)) == NULL) {
        return C_ERR;
    }

    if (hashTypeGetValue(o,field->ptr,&vstr,&vlen,&ll) == C_OK) {
        if (vstr) {
            if (string2ld((char*)vstr,vlen,&value) == 0) {
                return C_ERR;
            }
        } else {
            value = (long double)ll;
        }
    } else {
        value = 0;
    }
    value += val;
    *ret = value;

    char buf[MAX_LONG_DOUBLE_CHARS];
    int len = ld2string(buf,sizeof(buf),value,1);
    new = sdsnewlen(buf,len);
    hashTypeSet(o,field->ptr,new,HASH_SET_TAKE_VALUE);

    return C_OK;  
}

int RsHlen(redisDbIF *db, robj *key, unsigned long *len)
{
    if (NULL == db || NULL == key) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    robj *o;
    if ((o = lookupKeyRead(redis_db,key)) == NULL || checkType(o,OBJ_HASH)) {
        return REDIS_KEY_NOT_EXIST;
    }

    *len = hashTypeLength(o);
    
    return C_OK;
}

int RsHStrlen(redisDbIF *db, robj *key, robj *field, unsigned long *len)
{
    if (NULL == db || NULL == key || NULL == field) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    robj *o;
    if ((o = lookupKeyRead(redis_db,key)) == NULL || checkType(o,OBJ_HASH)) {
        return REDIS_KEY_NOT_EXIST;
    }

    *len = hashTypeGetValueLength(o,field->ptr);

    return C_OK;
}
