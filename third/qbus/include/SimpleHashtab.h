#ifndef _SIMPLEHASHTAB_H
#define _SIMPLEHASHTAB_H


#include "HSearchHelper.h"


#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    GC_NONE = 0,
    GC_KEY_BY_FREE = 0x01,
    GC_KEY_BY_DELETE = 0x02,
    GC_DATA_BY_FREE = 0x04,
    GC_DATA_BY_DELETE = 0x08 // nonsenses
} GC_MANUAL ;

enum {
    HT_ERR_BAD_ARG = -100,
    HT_ERR_SHORT_SPACE = -101
};

typedef int (*ht_walk_fn)(const char *key, void *data);

#ifdef __cplusplus
}
#endif


class Hashtab {
private:
    struct hsearch_data htab_;
    int errno_;

public:
    Hashtab(unsigned int size);
    ~Hashtab();

    bool insert(const char *key, const void *data);
    bool replace(const char *key, const void *data,
                 GC_MANUAL manual = GC_DATA_BY_FREE);
    ENTRY *find(const char *key);
    void *findData(const char *key);
    bool erase(const char *key, GC_MANUAL manual = GC_KEY_BY_FREE);
    bool traverse(ht_walk_fn fn, int on_error = 0);

    unsigned int getSize() const;
    unsigned int getFilled() const;
    bool getKeys(char *keys[], unsigned int num);
    const char *getEntryKey(const ENTRY *ep);
    void *getEntryData(const ENTRY *ep);

    int getErrno() const;
    int clearErrno();
};


#endif
