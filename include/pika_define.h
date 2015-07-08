#ifndef __PIKA_DEFINE_H__
#define __PIKA_DEFINE_H__

#define PIKA_MAX_CLIENTS 10240
#define PIKA_MAX_MESSAGE 10240
#define PIKA_THREAD_NUM 16

/*
 * Client Request type
 */
#define REDIS_REQ_INLINE 1
#define REDIS_REQ_MULTIBULK 2

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
