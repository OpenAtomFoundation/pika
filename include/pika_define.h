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

#define STAGE_ONE 1
#define STAGE_TWO 2

#endif
