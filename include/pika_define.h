#ifndef __PIKA_DEFINE_H__
#define __PIKA_DEFINE_H__

#define PIKA_MAX_CLIENTS 10240
#define PIKA_MAX_MESSAGE 10240
#define PIKA_THREAD_NUM 24

#define PIKA_IOBUF_LEN (1024 * 64)
#define PIKA_MBULK_BIG_ARG (1024 * 32)
#define PIKA_INLINE_MAX_SIZE (1024 * 64)
/*
 * Client Request type
 */
#define PIKA_REQ_INLINE 1
#define PIKA_REQ_MULTIBULK 2

#define PIKA_REP_STRATEGY_ERROR -1
#define PIKA_REP_STRATEGY_ALREADY 0
#define PIKA_REP_STRATEGY_PSYNC 1

#define PIKA_REP_OFFLINE 0
#define PIKA_REP_CONNECTED 1

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

#define CLIENT_NORMAL 1
#define CLIENT_MASTER 2
#define CLIENT_SLAVE 4

struct client_info {
    int fd;
    bool is_killed;
    int role;
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
