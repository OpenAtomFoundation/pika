// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_INCLUDE_NET_DEFINE_H_
#define NET_INCLUDE_NET_DEFINE_H_

#include <iostream>

namespace net {

#define NET_MAX_CLIENTS 10240
#define NET_MAX_MESSAGE 1024
#define NET_NAME_LEN 1024

const int kProtoMaxMessage = 512 * 1024 * 1024;  // 512MB
#define PB_IOBUF_LEN 67108864                    // 64MB
/*
 * The pb head and code length
 */
#define COMMAND_HEADER_LENGTH 4
#define COMMAND_CODE_LENGTH 4

const int kCommandHeaderLength = 4;

/*
 * The socket block type
 */
enum BlockType {
  kBlock = 0,
  kNonBlock = 1,
};

enum NotifyType {
  kNotiConnect = 0,
  kNotiClose = 1,
  kNotiEpollout = 2,
  kNotiEpollin = 3,
  kNotiEpolloutAndEpollin = 4,
  kNotiWrite = 5,
  kNotiWait = 6,
};

enum EventStatus {
  kNone = 0,
  kReadable = 0x1,
  kWritable = 0x1 << 1,
  kErrorEvent = 0x1 << 2,
};

enum ConnStatus {
  kHeader = 0,
  kPacket = 1,
  kComplete = 2,
  kBuildObuf = 3,
  kWriteObuf = 4,
};

enum ReadStatus {
  kReadHalf = 0,
  kReadAll = 1,
  kReadError = 2,
  kReadClose = 3,
  kFullError = 4,
  kParseError = 5,
  kDealError = 6,
  kOk = 7,
};

enum WriteStatus {
  kWriteHalf = 0,
  kWriteAll = 1,
  kWriteError = 2,
};

enum RetCode {
  kSuccess = 0,
  kBindError = 1,
  kCreateThreadError = 2,
  kListenError = 3,
  kSetSockOptError = 4,
  kCreateThreadPoolError = 5,
};

/*
 * define the redis protocol
 */
#define REDIS_MAX_MESSAGE (1 << 28)      // 256MB
#define REDIS_MBULK_BIG_ARG (1024 * 32)  // 32KB
#define DEFAULT_WBUF_SIZE 262144         // 256KB
#define REDIS_INLINE_MAXLEN (1024 * 64)  // 64KB
#define REDIS_IOBUF_LEN 16384            // 16KB
#define REDIS_REQ_INLINE 1
#define REDIS_REQ_MULTIBULK 2

/*
 * define the net cron interval (ms)
 */
#define NET_CRON_INTERVAL 1000

/*
 * define the macro in NET_conf
 */

#define NET_WORD_SIZE 1024
#define NET_LINE_SIZE 1024
#define NET_CONF_MAX_NUM 1024

/*
 * define common character
 */
#define SPACE ' '
#define COLON ':'
#define SHARP '#'

}  // namespace net
#endif  // NET_INCLUDE_NET_DEFINE_H_
