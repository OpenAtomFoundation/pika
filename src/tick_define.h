#ifndef __TICK_DEFINE_H__
#define __TICK_DEFINE_H__

#define TICK_MAX_CLIENTS 10240
#define TICK_MAX_MESSAGE 10240
#define TICK_THREAD_NUM 16

/*
 * The pb head and code length
 */
#define COMMAND_HEADER_LENGTH 4
#define COMMAND_CODE_LENGTH 4

/*
 * The socket block type
 */
enum BlockType {
    kBlock = 0,
    kNonBlock = 1,
};

enum EventStatus{
    kNone = 0,
    kReadable = 1,
    kWriteable = 2,
};

/*
 * define the macro in tick_conf
 */

#define TICK_WORD_SIZE 1024
#define TICK_LINE_SIZE 1024
#define TICK_CONF_MAX_NUM 1024


/*
 * define common character
 */
#define SPACE ' '
#define COLON ':'
#define SHARP '#'

#endif
