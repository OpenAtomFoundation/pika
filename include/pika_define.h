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

#endif
