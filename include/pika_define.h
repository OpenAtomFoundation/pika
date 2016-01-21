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

#endif
