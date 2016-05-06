#ifndef PIKA_DEFINE_H_
#define PIKA_DEFINE_H_


#define PIKA_MAX_WORKER_THREAD_NUM 24


const std::string kPikaVersion = "2.0.2";
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
  int port;
  pthread_t sender_tid;
  int hb_fd;
  int stage;
  void* sender;
  struct timeval create_time;
};

#define SLAVE_ITEM_STAGE_ONE 1
#define SLAVE_ITEM_STAGE_TWO 2

//repl_state_
#define PIKA_REPL_NO_CONNECT 0
#define PIKA_REPL_CONNECT 1
#define PIKA_REPL_CONNECTING 2
#define PIKA_REPL_CONNECTED 3
#define PIKA_REPL_WAIT_DBSYNC 4

//role
#define PIKA_ROLE_SINGLE 0
#define PIKA_ROLE_SLAVE 1
#define PIKA_ROLE_MASTER 2


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
 * Header is Type(1 byte), length (2 bytes)
 */
static const size_t kHeaderSize = 1 + 3;

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

/*
 * db sync
 */
const uint32_t kDBSyncMaxGap = 50;
const std::string kDBSyncModule = "document";

const std::string kBgsaveInfoFile = "info";
#endif
