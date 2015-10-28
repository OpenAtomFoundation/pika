#ifndef __PIKA_DEFINE_H__
#define __PIKA_DEFINE_H__
#include "nemo.h"
#define PIKA_VERSION    "0.1"

#define PIKA_MAX_CLIENTS 10240
#define PIKA_MAX_MESSAGE 10240
#define PIKA_THREAD_NUM 24

#define PIKA_IOBUF_LEN (1024 * 64)
#define PIKA_MBULK_BIG_ARG (1024 * 32)
#define PIKA_INLINE_MAX_SIZE (1024 * 64)

#define PIKA_DEFAULT_PID_FILE   "pika.pid"

/*
 * Client Request type
 */
#define PIKA_REQ_INLINE 1
#define PIKA_REQ_MULTIBULK 2

#define PIKA_REP_STRATEGY_ERROR -1
#define PIKA_REP_STRATEGY_ALREADY 0
#define PIKA_REP_STRATEGY_PSYNC 1

#define PIKA_REP_OFFLINE 0
#define PIKA_REP_CONNECT 1
#define PIKA_REP_CONNECTING 2
#define PIKA_REP_CONNECTED 3
#define PIKA_REP_SINGLE 4

#define PIKA_SINGLE 0
#define PIKA_MASTER 1
#define PIKA_SLAVE 2
/*
 * The socket block type
 */
enum BlockType {
    kBlock = 0,
    kNonBlock = 1,
};

enum EventStatus {
    kNone = 0,
    kReadable = 1,
    kWriteable = 2,
};

struct client_info {
    int fd;
    bool is_killed;
    int role;
};

struct dump_args {
    void* p;
    nemo::Snapshots snapshots;
};

/*
 * define the macro in pika_conf
 */

#define PIKA_WORD_SIZE 1024
#define PIKA_LINE_SIZE 1024
#define PIKA_CONF_MAX_NUM 1024


/*
 * define common character
 */
#define SPACE ' '
#define COLON ':'
#define SHARP '#'

#endif
