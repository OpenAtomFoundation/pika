// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_DEFINE_H_
#define PIKA_DEFINE_H_

#include "pink/include/redis_cli.h"

#include "include/pika_conf.h"

#define PIKA_SYNC_BUFFER_SIZE           1000
#define PIKA_MAX_WORKER_THREAD_NUM      24
#define PIKA_META_SYNC_MAX_WAIT_TIME    3

class PikaServer;

/* Port shift */
const int kPortShiftRSync      = 1000;
const int kPortShiftHeatBeat   = 2000;
const int kPortShiftReplServer = 3000;

const std::string kPikaPidFile = "pika.pid";

struct WorkerCronTask {
  int task;
  std::string ip_port;
};
typedef WorkerCronTask MonitorCronTask;
//task define
#define TASK_KILL 0
#define TASK_KILLALL 1

//slave item
struct SlaveItem {
  int64_t sid;
  std::string ip_port;
  std::string ip;
  int port;
  pthread_t sender_tid;
  int hb_fd;
  int stage;
  std::vector<TableStruct> table_structs;
  struct timeval create_time;
};

//dbsync arg
struct DBSyncArg {
  PikaServer* p;
  std::string ip;
  int port;
  std::string table_name;
  uint32_t partition_id;
  DBSyncArg(PikaServer* const _p,
            const std::string& _ip,
            int _port,
            const std::string& _table_name,
            uint32_t _partition_id)
      : p(_p), ip(_ip), port(_port),
        table_name(_table_name), partition_id(_partition_id) {}
};

//slowlog define
#define SLOWLOG_ENTRY_MAX_ARGC 32
#define SLOWLOG_ENTRY_MAX_STRING 128

//slowlog entry
struct SlowlogEntry {
  int64_t id;
  int64_t start_time;
  int64_t duration;
  pink::RedisCmdArgsType argv;
};

#define PIKA_MIN_RESERVED_FDS 5000

const int SLAVE_ITEM_STAGE_ONE    = 1;
const int SLAVE_ITEM_STAGE_TWO    = 2;

//repl_state_
const int PIKA_REPL_NO_CONNECT                = 0;
const int PIKA_REPL_SHOULD_META_SYNC          = 1;
const int PIKA_REPL_WAIT_META_SYNC_RESPONSE   = 2;
const int PIKA_REPL_SHOULD_MARK_TRY_CONNECT   = 3;
const int PIKA_REPL_CONNECTING                = 4;
const int PIKA_REPL_ESTABLISH_SUCCESS         = 5;
const int PIKA_REPL_ERROR                     = 6;

//role
const int PIKA_ROLE_SINGLE        = 0;
const int PIKA_ROLE_SLAVE         = 1;
const int PIKA_ROLE_MASTER        = 2;

/*
 * The size of Binlogfile
 */
//static uint64_t kBinlogSize = 128; 
//static const uint64_t kBinlogSize = 1024 * 1024 * 100;

enum RecordType {
  kZeroType = 0,
  kFullType = 1,
  kFirstType = 2,
  kMiddleType = 3,
  kLastType = 4,
  kEof = 5,
  kBadRecord = 6,
  kOldRecord = 7
};

/*
 * the block size that we read and write from write2file
 * the default size is 64KB
 */
static const size_t kBlockSize = 64 * 1024;

/*
 * Header is Type(1 byte), length (3 bytes), time (4 bytes)
 */
static const size_t kHeaderSize = 1 + 3 + 4;

/*
 * the size of memory when we use memory mode
 * the default memory size is 2GB
 */
const int64_t kPoolSize = 1073741824;

const std::string kBinlogPrefix = "write2file";
const size_t kBinlogPrefixLen = 10;

const std::string kManifest = "manifest";

/*
 * define common character
 *
 */
#define COMMA ','

/*
 * define reply between master and slave
 *
 */
const std::string kInnerReplOk = "ok";
const std::string kInnerReplWait = "wait";

const unsigned int kMaxBitOpInputKey = 12800;
const int kMaxBitOpInputBit = 21;
/*
 * db sync
 */
const uint32_t kDBSyncMaxGap = 50;
const std::string kDBSyncModule = "document";

const std::string kBgsaveInfoFile = "info";
#endif
