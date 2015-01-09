#ifndef _SIMPLECONNPOOL_H
#define _SIMPLECONNPOOL_H


#include <pthread.h>
#include <stdint.h>


#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    int *socks;
    unsigned int size;
    unsigned int num;
    uint64_t avail;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    pthread_cond_t cond2;
} conn_pool_t;

enum {
    CP_ERR_EMPTY = -1,
    CP_ERR_FULL = -2,
    CP_ERR_INADEQUATE = -3,
    CP_ERR_EXHAUSTED = -4,
    CP_ERR_LOCK = -5,
    CP_ERR_ARGS = -6,
};

#ifdef __cplusplus
}
#endif


// sizeof(uint64_t) * 8
#define CONN_MAX 64


conn_pool_t *conn_pool_create(unsigned int size);
bool conn_pool_destroy(conn_pool_t *cp);
int conn_pool_take(conn_pool_t *cp);
int conn_pool_take2(conn_pool_t *cp, unsigned int idx);
int conn_pool_put(conn_pool_t *cp, int sockfd);
int conn_pool_put2(conn_pool_t *cp, int sockfd, unsigned int idx);
bool conn_pool_remove(conn_pool_t *cp, int sockfd);
#if 0
unsigned int conn_pool_size(conn_pool_t *cp);
unsigned int conn_pool_filled(conn_pool_t *cp);
#endif


#endif
