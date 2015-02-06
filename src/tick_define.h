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

enum EventStatus {
    kNone = 0,
    kReadable = 1,
    kWriteable = 2,
};

enum ConnStatus {
    kHeader = 0,
    kCode = 1,
    kPacket = 2,
    kComplete = 3,
    kBuildObuf = 4,
    kWriteObuf = 5,
};

enum CommandCode {
    kSdkInvalidOperation = 512,
    kSdkSet = 513,
    kSdkSetRet = 514,
    kSdkDelete = 515,
    kSdkDeleteRet = 516,
    kSdkGet = 518,
    kSdkGetRet = 519,
    kSdkMGet = 526,
    kSdkMGetRet = 527,


    // for hash 
    kSdkHput = 530,
    kSdkHputret = 531,
    kSdkHget = 532,
    kSdkHgetret = 533,
    kSdkHdelete = 534,
    kSdkHdeleteret = 535,
    
    //for list
    kSdkLSize = 556,
    kSdkLSizeRet = 557,
    kSdkLGet  = 558,
    kSdkLGetRet = 559,
    kSdkLPut = 560,
    kSdkLPutRet  = 561,
    kSdkLPop  = 562,
    kSdkLPopRet =  563,
    kSdkLIndex  = 564,
    kSdkLIndexRet = 565,
    kSdkLRange  = 566,
    kSdkLRangeRet  = 567,
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
