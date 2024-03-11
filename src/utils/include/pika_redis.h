#ifndef PIKA_REDIS_H_
#define PIKA_REDIS_H_

#include <stdio.h>
#include <stdlib.h>
//#include <stdint.h>
#include <arpa/inet.h>
#include <inttypes.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include "pika_ziplist.h"
#include "utils/include/pika_lzf.h"
#include "storage/include/storage/storage.h"

/* Error codes */
#define REDIS_OK                0
#define REDIS_ERR               -1

/* The current RDB version. When the format changes in a way that is no longer
 * backward compatible this number gets incremented. */
#define REDIS_RDB_VERSION 6

/* Dup object types to RDB object types. Only reason is readability (are we
 * dealing with RDB types or with in-memory object types?). */
#define REDIS_RDB_TYPE_STRING 0
#define REDIS_RDB_TYPE_LIST   1
#define REDIS_RDB_TYPE_SET    2
#define REDIS_RDB_TYPE_ZSET   3
#define REDIS_RDB_TYPE_HASH   4

/* Object types for encoded objects. */
#define REDIS_RDB_TYPE_HASH_ZIPMAP    9
#define REDIS_RDB_TYPE_LIST_ZIPLIST  10
#define REDIS_RDB_TYPE_SET_INTSET    11
#define REDIS_RDB_TYPE_ZSET_ZIPLIST  12
#define REDIS_RDB_TYPE_HASH_ZIPLIST  13
#define REDIS_RDB_TYPE_LIST_QUICKLIST 14

#define REDIS_RDB_6BITLEN 0
#define REDIS_RDB_14BITLEN 1
#define REDIS_RDB_32BITLEN 2
#define REDIS_RDB_ENCVAL 3
#define REDIS_RDB_LENERR UINT_MAX

#define REDIS_RDB_ENC_INT8 0        /* 8 bit signed integer */
#define REDIS_RDB_ENC_INT16 1       /* 16 bit signed integer */
#define REDIS_RDB_ENC_INT32 2       /* 32 bit signed integer */
#define REDIS_RDB_ENC_LZF 3         /* string compressed with FASTLZ */

/* Test if a type is an object type. */
#define rdbIsObjectType(t) ((t >= 0 && t <= 4) || (t >= 9 && t <= 14))

struct _rio {
  /* Backend functions.
   * Since this functions do not tolerate short writes or reads the return
   * value is simplified to: zero on error, non zero on complete success. */
  size_t (*read)(struct _rio *, void *buf, size_t len);

  /* The update_cksum method if not NULL is used to compute the checksum of
   * all the data that was read or written so far. The method should be
   * designed so that can be called with the current checksum, and the buf
   * and len fields pointing to the new block of data to add to the checksum
   * computation. */
  void (*update_cksum)(struct _rio *, const void *buf, size_t len);

  /* The current checksum */
  uint64_t cksum;

  /* number of bytes read or written */
  size_t processed_bytes;

  /* maximum single read or write chunk size */
  size_t max_processing_chunk;

  /* Backend-specific vars. */
  union {
    /* In-memory buffer target. */
    struct {
      const char *ptr;
      off_t pos;
      size_t len;
    } buffer;
    /* Stdio file pointer target. */
    struct {
      FILE *fp;
      off_t buffered; /* Bytes written since last fsync. */
      off_t autosync; /* fsync after 'autosync' bytes written. */
    } file;
    /* Multiple FDs target (used to write to N sockets). */
    struct {
      int *fds;       /* File descriptors. */
      int *state;     /* Error state of each fd. 0 (if ok) or errno. */
      int numfds;
      off_t pos;
      char *buf;
    } fdset;
  } io;
};
typedef struct _rio rio;

struct _restore_value{
  storage::DataType type;

  std::string kvv;
  std::vector<storage::FieldValue> hashv;
  std::vector<std::string> listv;
  std::vector<storage::ScoreMember> zsetv;
  std::vector<std::string> setv;
};
typedef struct _restore_value restore_value;

size_t rioBufferRead(rio *r, void *buf, size_t len);

void rioInitWithBuffer(rio *r, const char *s, size_t len);
int rdbLoadObjectType(rio *rdb);

uint64_t crc64(uint64_t crc, const unsigned char *s, uint64_t l);

int verifyDumpPayload(unsigned char *p, size_t len);
int rdbLoadObject(int rdbtype, rio *rdb, restore_value *dbvalue);
uint32_t rdbLoadLen(rio *rdb, int *isencoded);

int rdbLoadDoubleValue(rio *rdb, double *val);

int rdbLoadIntegerObject(rio *rdb, int enctype, std::string *str);
int rdbLoadLzfStringObject(rio *rdb, std::string *str);
int rdbLoadStringObject(rio *rdb, std::string *str);

int getZsetFromZiplist(const std::string &aux, restore_value *dbvalue);
int getListFromZiplist(const std::string &aux, restore_value *dbvalue);
int getHashFromZiplist(const std::string &aux, restore_value *dbvalue);
int getSetFromIntset(const std::string &aux, restore_value *dbvalue);

int zzlNext(unsigned char *zl, unsigned char **eptr, unsigned char **sptr);
int zzlGetString(unsigned char *sptr, std::string *value);
int zzlGetScore(unsigned char *sptr, double *score);


#endif