// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "net/src/net_util.h"
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include "net/include/net_define.h"

namespace net {

int Setnonblocking(int sockfd) {
  int flags;
  if ((flags = fcntl(sockfd, F_GETFL, 0)) < 0) {
    close(sockfd);
    return -1;
  }
  flags |= O_NONBLOCK;
  if (fcntl(sockfd, F_SETFL, flags) < 0) {
    close(sockfd);
    return -1;
  }
  return flags;
}

uint32_t TimerTaskManager::AddTimerTask(const std::string& task_name, int interval_ms, bool repeat_exec,
                                        const std::function<void()>& task) {
  TimedTask new_task = {last_task_id_++, task_name, interval_ms, repeat_exec, task};
  id_to_task_[new_task.task_id] = new_task;

  int64_t next_expired_time = NowInMs() + interval_ms;
  exec_queue_.insert({next_expired_time, new_task.task_id});

  // return the id of this task
  return new_task.task_id;
}

int64_t TimerTaskManager::NowInMs() {
  auto now = std::chrono::system_clock::now();
  return std::chrono::time_point_cast<std::chrono::milliseconds>(now).time_since_epoch().count();
}

int32_t TimerTaskManager::ExecTimerTask() {
  std::vector<ExecTsWithId> fired_tasks_;
  int64_t now_in_ms = NowInMs();
  // traverse in ascending order, and exec expired tasks
  for (auto pair : exec_queue_) {
    if (pair.exec_ts <= now_in_ms) {
      auto it = id_to_task_.find(pair.id);
      assert(it != id_to_task_.end());
      it->second.fun();
      fired_tasks_.push_back({pair.exec_ts, pair.id});
      now_in_ms = NowInMs();
    } else {
      break;
    }
  }

  for (auto task : fired_tasks_) {
    exec_queue_.erase(task);
    auto it = id_to_task_.find(task.id);
    assert(it != id_to_task_.end());
    if (it->second.repeat_exec) {
      // this task need to be repeatedly exec, register it again
      exec_queue_.insert({now_in_ms + it->second.interval_ms, task.id});
    } else {
      // this task only need to be exec once, completely remove this task
      id_to_task_.erase(task.id);
    }
  }

  if (exec_queue_.empty()) {
    //no task to exec, epoll will use -1 as timeout value, and sink into endless wait
    return -1;
  }
  int32_t gap_between_now_and_next_task = static_cast<int32_t>(exec_queue_.begin()->exec_ts - NowInMs());
  gap_between_now_and_next_task = gap_between_now_and_next_task < 0 ? 0 : gap_between_now_and_next_task;
  return gap_between_now_and_next_task;
}

bool TimerTaskManager::DelTimerTaskByTaskId(uint32_t task_id) {
  // remove the task
  auto task_to_del = id_to_task_.find(task_id);
  if (task_to_del == id_to_task_.end()) {
    return false;
  }
  int interval_del = task_to_del->second.interval_ms;
  id_to_task_.erase(task_to_del);

  // remove from exec queue
  ExecTsWithId target_key = {-1, 0};
  for (auto pair : exec_queue_) {
    if (pair.id == task_id) {
      target_key = {pair.exec_ts, pair.id};
      break;
    }
  }
  if (target_key.exec_ts != -1) {
    exec_queue_.erase(target_key);
  }
  return true;
}

TimerTaskThread::~TimerTaskThread() {
  if (!timer_task_manager_.Empty()) {
    LOG(INFO) << "TimerTaskThread exit !!!";
  }
}
int TimerTaskThread::StartThread() {
  if (timer_task_manager_.Empty()) {
    LOG(INFO) << "No Timer task registered, TimerTaskThread won't be created.";
    // if there is no timer task registered, no need of start the thread
    return -1;
  }
  set_thread_name("TimerTask");
  LOG(INFO) << "TimerTaskThread Starting...";
  return Thread::StartThread();
}
int TimerTaskThread::StopThread() {
  if (timer_task_manager_.Empty()) {
    LOG(INFO) << "TimerTaskThread::StopThread : TimerTaskThread didn't create, no need to stop it.";
    // if there is no timer task registered, the thread didn't even start
    return -1;
  }
  return Thread::StopThread();
}

void* TimerTaskThread::ThreadMain() {
  int timeout;
  while (!should_stop()) {
    timeout = timer_task_manager_.ExecTimerTask();
    net_multiplexer_->NetPoll(timeout);
  }
  return nullptr;
}
}  // namespace net
