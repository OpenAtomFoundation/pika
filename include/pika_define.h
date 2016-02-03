#ifndef PIKA_DEFINE_H_
#define PIKA_DEFINE_H_


#define PIKA_MAX_WORKER_THREAD_NUM 1


struct WorkerCronTask {
  int task;
  std::string ip_port;
};
//task define
#define TASK_KILL 0
#define TASK_KILLALL 1

//slave item
struct SlaveItem {
  uint64_t sid;
  std::string ip_port;
  int port;
  pthread_t sender_tid;
  int hb_fd;
  int stage;
  struct timeval create_time;
};

#define SLAVE_ITEM_STAGE_ONE 1
#define SLAVE_ITEM_STAGE_TWO 2

//repl_state_
#define PIKA_REPL_NO_CONNECT 0
#define PIKA_REPL_CONNECT 1
#define PIKA_REPL_CONNECTING 2
#define PIKA_REPL_CONNECTED 3

//role
#define PIKA_ROLE_SINGLE 0
#define PIKA_ROLE_SLAVE 1
#define PIKA_ROLE_MASTER 2


/*
 * The size of Binlogfile
 */
static uint64_t kBinlogSize = 128; 
//static uint64_t kBinlogSize = 1024 * 1024 * 100;

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
static const int64_t kPoolSize = 1073741824;

static std::string kBinlog = "/binlog";

static std::string kManifest = "/manifest";

#endif
