#ifndef _HSEARCHHELPER_H
#define _HSEARCHHELPER_H


#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <search.h>


#ifdef __cplusplus
extern "C" {
#endif


// in glibc misc/search.h
#if 0
typedef enum
  {
    FIND,
    ENTER
  }
ACTION;
#endif

typedef enum {
    FIND2,
    ENTER2,
    REPLACE,
    RELEASE
} ACTION2;

typedef struct _ENTRY
{
  unsigned int used;
  ENTRY entry;
}
_ENTRY;


int hrelease_r(struct hsearch_data *, int manual);
int hsearch2_r(ENTRY, ACTION2, ENTRY **, struct hsearch_data *, int manual);


#ifdef __cplusplus
}
#endif


#endif
