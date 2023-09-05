#include <string.h>
#include <assert.h>
#include <limits.h>
#include <stdlib.h>

#include "redisdbIF.h"
#include "commondef.h"
#include "commonfunc.h"
#include "object.h"
#include "zmalloc.h"
#include "db.h"
#include "util.h"
#include "intset.h"

#define SRANDMEMBER_SUB_STRATEGY_MUL 3


/* Structure to hold set iteration abstraction. */
typedef struct {
    robj *subject;
    int encoding;
    int ii; /* intset iterator */
    dictIterator *di;
} setTypeIterator;


/* Factory method to return a set that *can* hold "value". When the object has
 * an integer-encodable value, an intset will be returned. Otherwise a regular
 * hash table. */
robj *setTypeCreate(sds value) {
    if (isSdsRepresentableAsLongLong(value,NULL) == C_OK)
        return createIntsetObject();
    return createSetObject();
}

setTypeIterator *setTypeInitIterator(robj *subject) {
    setTypeIterator *si = zmalloc(sizeof(setTypeIterator));
    si->subject = subject;
    si->encoding = subject->encoding;
    if (si->encoding == OBJ_ENCODING_HT) {
        si->di = dictGetIterator(subject->ptr);
    } else if (si->encoding == OBJ_ENCODING_INTSET) {
        si->ii = 0;
    } else {
        // serverPanic("Unknown set encoding");
    }
    return si;
}

void setTypeReleaseIterator(setTypeIterator *si) {
    if (si->encoding == OBJ_ENCODING_HT)
        dictReleaseIterator(si->di);
    zfree(si);
}

/* Move to the next entry in the set. Returns the object at the current
 * position.
 *
 * Since set elements can be internally be stored as SDS strings or
 * simple arrays of integers, setTypeNext returns the encoding of the
 * set object you are iterating, and will populate the appropriate pointer
 * (sdsele) or (llele) accordingly.
 *
 * Note that both the sdsele and llele pointers should be passed and cannot
 * be NULL since the function will try to defensively populate the non
 * used field with values which are easy to trap if misused.
 *
 * When there are no longer elements -1 is returned. */
int setTypeNext(setTypeIterator *si, sds *sdsele, int64_t *llele) {
    if (si->encoding == OBJ_ENCODING_HT) {
        dictEntry *de = dictNext(si->di);
        if (de == NULL) return -1;
        *sdsele = dictGetKey(de);
        *llele = -123456789; /* Not needed. Defensive. */
    } else if (si->encoding == OBJ_ENCODING_INTSET) {
        if (!intsetGet(si->subject->ptr,si->ii++,llele))
            return -1;
        *sdsele = NULL; /* Not needed. Defensive. */
    } else {
        return -1;
    }
    return si->encoding;
}

/* Convert the set to specified encoding. The resulting dict (when converting
 * to a hash table) is presized to hold the number of elements in the original
 * set. */
void setTypeConvert(robj *setobj, int enc) {
    setTypeIterator *si;
    assert(setobj->type == OBJ_SET && setobj->encoding == OBJ_ENCODING_INTSET);

    if (enc == OBJ_ENCODING_HT) {
        int64_t intele;
        dict *d = dictCreate(&setDictType,NULL);
        sds element;

        /* Presize the dict to avoid rehashing */
        dictExpand(d,intsetLen(setobj->ptr));

        /* To add the elements we extract integers and create redis objects */
        si = setTypeInitIterator(setobj);
        while (setTypeNext(si,&element,&intele) != -1) {
            element = sdsfromlonglong(intele);
            assert(dictAdd(d,element,NULL) == DICT_OK);
        }
        setTypeReleaseIterator(si);

        setobj->encoding = OBJ_ENCODING_HT;
        zfree(setobj->ptr);
        setobj->ptr = d;
    } else {
        // serverPanic("Unsupported set conversion");
    }
}

/* Add the specified value into a set.
 *
 * If the value was already member of the set, nothing is done and 0 is
 * returned, otherwise the new element is added and 1 is returned. */
int setTypeAdd(robj *subject, sds value) {
    long long llval;
    if (subject->encoding == OBJ_ENCODING_HT) {
        dict *ht = subject->ptr;
        dictEntry *de = dictAddRaw(ht,value,NULL);
        if (de) {
            dictSetKey(ht,de,sdsdup(value));
            dictSetVal(ht,de,NULL);
            return 1;
        }
    } else if (subject->encoding == OBJ_ENCODING_INTSET) {
        if (isSdsRepresentableAsLongLong(value,&llval) == C_OK) {
            uint8_t success = 0;
            subject->ptr = intsetAdd(subject->ptr,llval,&success);
            if (success) {
                /* Convert to regular set when the intset contains
                 * too many entries. */
                if (intsetLen(subject->ptr) > OBJ_SET_MAX_INTSET_ENTRIES)
                    setTypeConvert(subject,OBJ_ENCODING_HT);
                return 1;
            }
        } else {
            /* Failed to get integer from object, convert to regular set. */
            setTypeConvert(subject,OBJ_ENCODING_HT);

            /* The set *was* an intset and this value is not integer
             * encodable, so dictAdd should always work. */
            assert(dictAdd(subject->ptr,sdsdup(value),NULL) == DICT_OK);
            return 1;
        }
    } else {
        return -1;
    }
    return 0;
}

int setTypeIsMember(robj *subject, sds value) {
    long long llval;
    if (subject->encoding == OBJ_ENCODING_HT) {
        return dictFind((dict*)subject->ptr,value) != NULL;
    } else if (subject->encoding == OBJ_ENCODING_INTSET) {
        if (isSdsRepresentableAsLongLong(value,&llval) == C_OK) {
            return intsetFind((intset*)subject->ptr,llval);
        }
    } else {
        return -1;
    }
    return 0;
}

int setTypeRemove(robj *setobj, sds value) {
    long long llval;
    if (setobj->encoding == OBJ_ENCODING_HT) {
        if (dictDelete(setobj->ptr,value) == DICT_OK) {
            if (htNeedsResize(setobj->ptr)) dictResize(setobj->ptr);
            return 1;
        }
    } else if (setobj->encoding == OBJ_ENCODING_INTSET) {
        if (isSdsRepresentableAsLongLong(value,&llval) == C_OK) {
            int success;
            setobj->ptr = intsetRemove(setobj->ptr,llval,&success);
            if (success) return 1;
        }
    } else {
        return -1;
    }
    return 0;
}

/* Return random element from a non empty set.
 * The returned element can be a int64_t value if the set is encoded
 * as an "intset" blob of integers, or an SDS string if the set
 * is a regular set.
 *
 * The caller provides both pointers to be populated with the right
 * object. The return value of the function is the object->encoding
 * field of the object and is used by the caller to check if the
 * int64_t pointer or the redis object pointer was populated.
 *
 * Note that both the sdsele and llele pointers should be passed and cannot
 * be NULL since the function will try to defensively populate the non
 * used field with values which are easy to trap if misused. */
int setTypeRandomElement(robj *setobj, sds *sdsele, int64_t *llele) {
    if (setobj->encoding == OBJ_ENCODING_HT) {
        dictEntry *de = dictGetRandomKey(setobj->ptr);
        *sdsele = dictGetKey(de);
        *llele = -123456789; /* Not needed. Defensive. */
    } else if (setobj->encoding == OBJ_ENCODING_INTSET) {
        *llele = intsetRandom(setobj->ptr);
        *sdsele = NULL; /* Not needed. Defensive. */
    } else {
        return -1;
    }
    return setobj->encoding;
}

unsigned long setTypeSize(const robj *subject) {
    if (subject->encoding == OBJ_ENCODING_HT) {
        return dictSize((const dict*)subject->ptr);
    } else if (subject->encoding == OBJ_ENCODING_INTSET) {
        return intsetLen((const intset*)subject->ptr);
    } else {
        return -1;
        // serverPanic("Unknown set encoding");
    }
}

static void SMembers(robj *subject,
                    sds **members,
                    unsigned long *members_size) {

    *members_size = setTypeSize(subject);
    *members = (sds *)zcalloc(sizeof(sds) * (*members_size));
    sds *arrays = *members;

    unsigned long i = 0;
    sds elesds;
    int64_t intobj;
    int encoding;
    setTypeIterator *si = setTypeInitIterator(subject);
    while((encoding = setTypeNext(si,&elesds,&intobj)) != -1) {
        if (encoding == OBJ_ENCODING_HT) {
            arrays[i] = sdsdup(elesds);
        } else {
            arrays[i] = sdsfromlonglong(intobj);
        }

        ++i;
        if (i >= *members_size) break;
    }
    setTypeReleaseIterator(si);
}

int RsSAdd(redisDbIF *db, robj *key, robj *members[], unsigned long members_size)
{
    if (NULL == db || NULL == key || NULL == members) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    robj *set = lookupKeyWrite(redis_db,key);
    if (set == NULL) {
        set = setTypeCreate(members[0]->ptr);
        dbAdd(redis_db,key,set);
    } else {
        if (set->type != OBJ_SET) {
            return C_ERR;
        }
    }

    unsigned long j;
    for (j = 0; j < members_size; j++) {
        setTypeAdd(set,members[j]->ptr);
    }

    return C_OK;
}

int RsSCard(redisDbIF *db, robj *key, unsigned long *len)
{
    if (NULL == db || NULL == key) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    robj *o;
    if ((o = lookupKeyRead(redis_db,key)) == NULL || checkType(o,OBJ_SET)) {
        return REDIS_KEY_NOT_EXIST;
    }

    *len = setTypeSize(o);

    return C_OK;
}

int RsSIsmember(redisDbIF *db, robj *key, robj *member, int *is_member)
{
    if (NULL == db || NULL == key || NULL == member) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    robj *set;
    if ((set = lookupKeyRead(redis_db,key)) == NULL || checkType(set,OBJ_SET)) {
        return REDIS_KEY_NOT_EXIST;
    }

    *is_member = setTypeIsMember(set,member->ptr);

    return C_OK;
}

int RsSMembers(redisDbIF *db, robj *key, sds **members, unsigned long *members_size)
{
    if (NULL == db || NULL == key || NULL == members) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    robj *subject;
    if ((subject = lookupKeyRead(redis_db,key)) == NULL || checkType(subject,OBJ_SET)) {
        return REDIS_KEY_NOT_EXIST;
    }

    SMembers(subject, members, members_size);

    return C_OK;
}

int RsSRem(redisDbIF *db, robj *key, robj *members[], unsigned long members_size)
{
    if (NULL == db || NULL == key || NULL == members) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    robj *set;
    if ((set = lookupKeyWrite(redis_db,key)) == NULL || checkType(set,OBJ_SET)) {
        return REDIS_KEY_NOT_EXIST;
    }

    unsigned long j;
    for (j = 0; j < members_size; j++) {
        if (setTypeRemove(set,members[j]->ptr)) {
            if (setTypeSize(set) == 0) {
                dbDelete(redis_db,key);
                break;
            }
        }
    }

    return C_ERR;
}

int RsSRandmember(redisDbIF *db, robj *key, long l, sds **members, unsigned long *members_size)
{
    if (NULL == db || NULL == key || NULL == members) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    robj *subject;
    if ((subject = lookupKeyRead(redis_db,key)) == NULL || checkType(subject,OBJ_SET)) {
        return REDIS_KEY_NOT_EXIST;
    }

    unsigned long count;
    int uniq = 1;
    if (l >= 0) {
        count = (unsigned long) l;
    } else {
        /* A negative count means: return the same elements multiple times
         * (i.e. don't remove the extracted element after every extraction). */
        count = -l;
        uniq = 0;
    }

    /* If count is zero, serve it ASAP to avoid special cases later. */
    if (count == 0) {
        return C_OK;
    }

    /* CASE 1: The count was negative, so the extraction method is just:
     * "return N random elements" sampling the whole set every time.
     * This case is trivial and can be served without auxiliary data
     * structures. */
    sds ele;
    int64_t llele;
    int encoding;
    if (!uniq) {
        *members_size = count;
        *members = (sds *)zcalloc(sizeof(sds) * count);
        sds *arrays = *members;
        int i = 0;
        while(count--) {
            encoding = setTypeRandomElement(subject,&ele,&llele);
            if (encoding == OBJ_ENCODING_INTSET) {
                arrays[i] = sdsfromlonglong(llele);
            } else {
                arrays[i] = sdsdup(ele);
            }
            ++i;
        }
    
        return C_OK;
    }

    /* CASE 2:
     * The number of requested elements is greater than the number of
     * elements inside the set: simply return the whole set. */
    unsigned long size = setTypeSize(subject);
    if (count >= size) {
        SMembers(subject, members, members_size);
        return C_OK;
    }

    /* For CASE 3 and CASE 4 we need an auxiliary dictionary. */
    dict *d = dictCreate(&objectKeyPointerValueDictType,NULL);

    /* CASE 3:
     * The number of elements inside the set is not greater than
     * SRANDMEMBER_SUB_STRATEGY_MUL times the number of requested elements.
     * In this case we create a set from scratch with all the elements, and
     * subtract random elements to reach the requested number of elements.
     *
     * This is done because if the number of requsted elements is just
     * a bit less than the number of elements in the set, the natural approach
     * used into CASE 3 is highly inefficient. */
    if (count*SRANDMEMBER_SUB_STRATEGY_MUL > size) {
        setTypeIterator *si;

        /* Add all the elements into the temporary dictionary. */
        si = setTypeInitIterator(subject);
        while((encoding = setTypeNext(si,&ele,&llele)) != -1) {
            int retval = DICT_ERR;

            if (encoding == OBJ_ENCODING_INTSET) {
                retval = dictAdd(d,createStringObjectFromLongLong(llele),NULL);
            } else {
                retval = dictAdd(d,createStringObject(ele,sdslen(ele)),NULL);
            }
            assert(retval == DICT_OK);
        }
        setTypeReleaseIterator(si);
        assert(dictSize(d) == size);

        /* Remove random elements to reach the right count. */
        while(size > count) {
            dictEntry *de;

            de = dictGetRandomKey(d);
            dictDelete(d,dictGetKey(de));
            size--;
        }
    }

    /* CASE 4: We have a big set compared to the requested number of elements.
     * In this case we can simply get random elements from the set and add
     * to the temporary set, trying to eventually get enough unique elements
     * to reach the specified count. */
    else {
        unsigned long added = 0;
        robj *objele;

        while(added < count) {
            encoding = setTypeRandomElement(subject,&ele,&llele);
            if (encoding == OBJ_ENCODING_INTSET) {
                objele = createStringObjectFromLongLong(llele);
            } else {
                objele = createStringObject(ele,sdslen(ele));
            }
            /* Try to add the object to the dictionary. If it already exists
             * free it, otherwise increment the number of objects we have
             * in the result dictionary. */
            if (dictAdd(d,objele,NULL) == DICT_OK)
                added++;
            else
                decrRefCount(objele);
        }
    }

    /* CASE 3 & 4: send the result to the user. */
    {
        unsigned long i = 0;
        dictEntry *de;
        *members_size = count;
        *members = (sds *)zcalloc(sizeof(sds) * count);
        sds *arrays = *members;

        dictIterator *di = dictGetIterator(d);
        while((de = dictNext(di)) != NULL) {
            robj *o = dictGetKey(de);
            if (sdsEncodedObject(o)) {
                arrays[i] = sdsdup(o->ptr);
            } else if (o->encoding == OBJ_ENCODING_INT) {
                arrays[i] = sdsfromlonglong((long)o->ptr);
            }

            ++i;
            if (i >= count) break;
        }
        dictReleaseIterator(di);
        dictRelease(d);
    }

    return C_OK;
}