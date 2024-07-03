// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_SRC_NET_UTIL_H_
#define NET_SRC_NET_UTIL_H_
#include <unistd.h>
#include <cassert>
#include <chrono>
#include <functional>
#include<memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>
#include <glog/logging.h>
#include "net/src/net_multiplexer.h"
#include "net/include/net_thread.h"

namespace net {

int Setnonblocking(int sockfd);

struct TimedTask{
  uint32_t task_id;
  std::string task_name;
  int interval_ms;
  bool repeat_exec;
  std::function<void()> fun;
};

struct ExecTsWithId {
  //the next exec time of the task, unit in ms
  int64_t exec_ts;
  //id of the task to be exec
  uint32_t id;

  bool operator<(const ExecTsWithId& other) const{
    if(exec_ts == other.exec_ts){
      return id < other.id;
    }
    return exec_ts < other.exec_ts;
  }
  bool operator==(const ExecTsWithId& other) const {
    return exec_ts == other.exec_ts && id == other.id;
  }
};

class TimerTaskManager {
 public:
  TimerTaskManager() = default;
  ~TimerTaskManager() = default;

  uint32_t AddTimerTask(const std::string& task_name, int interval_ms, bool repeat_exec, const std::function<void()> &task);
  //return the time gap between now and next task-expired time, which can be used as the timeout value of epoll
  int ExecTimerTask();
  bool DelTimerTaskByTaskId(uint32_t task_id);
  int64_t NowInMs();
  bool Empty() const { return 0 == last_task_id_; }

 private:
  //items stored in std::set are ascending ordered, we regard it as an auto sorted queue
  std::set<ExecTsWithId> exec_queue_;
  std::unordered_map<uint32_t, TimedTask> id_to_task_;
  uint32_t last_task_id_{0};
};


/*
 * For simplicity, current version of TimerTaskThread has no lock inside and all task should be registered before TimerTaskThread started,
 * but if you have the needs of dynamically add/remove timer task after TimerTaskThread started, you can simply add a mutex to protect
 * the timer_task_manager_ and also a pipe to wake up the maybe being endless-wait epoll(if all task consumed, epoll will sink into
 * endless wait) to implement the feature.
 */
class TimerTaskThread : public Thread {
 public:
  TimerTaskThread(){
    net_multiplexer_.reset(CreateNetMultiplexer());
    net_multiplexer_->Initialize();
  }
  ~TimerTaskThread() override;
  int StartThread() override;
  int StopThread() override;
  void set_thread_name(const std::string& name) override { Thread::set_thread_name(name); }

  uint32_t AddTimerTask(const std::string& task_name, int interval_ms, bool repeat_exec, const std::function<void()> &task){
      return timer_task_manager_.AddTimerTask(task_name, interval_ms, repeat_exec, task);
  };

  bool DelTimerTaskByTaskId(uint32_t task_id){
    return timer_task_manager_.DelTimerTaskByTaskId(task_id);
};

 private:
  void* ThreadMain() override;

  TimerTaskManager timer_task_manager_;
  std::unique_ptr<NetMultiplexer> net_multiplexer_;
};

}  // namespace net

#endif  //  NET_SRC_NET_UTIL_H_
