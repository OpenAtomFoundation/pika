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

    if(min_interval_ms_ > interval_ms || min_interval_ms_ == -1){
      min_interval_ms_ = interval_ms;
    }
    //return the id of this task
    return new_task.task_id;
  }
int64_t TimerTaskManager::NowInMs() {
    auto now = std::chrono::system_clock::now();
    return std::chrono::time_point_cast<std::chrono::milliseconds>(now).time_since_epoch().count();
}
void TimerTaskManager::ExecTimerTask() {
    std::vector<ExecTsWithId> fired_tasks_;
    int64_t now_in_ms = NowInMs();
    //traverse in ascending order
    for(auto pair = exec_queue_.begin(); pair != exec_queue_.end(); pair++){
      if(pair->exec_ts <= now_in_ms){
        auto it = id_to_task_.find(pair->id);
        assert(it != id_to_task_.end());
        it->second.fun();
        fired_tasks_.push_back({pair->exec_ts, pair->id});
        now_in_ms = NowInMs();
      }else{
        break;
      }
    }
    for(auto task : fired_tasks_) {
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
}
bool TimerTaskManager::DelTimerTaskByTaskId(uint32_t task_id) {
    //remove the task
    auto task_to_del = id_to_task_.find(task_id);
    if(task_to_del == id_to_task_.end()){
      return false;
    }
    int interval_del = task_to_del->second.interval_ms;
    id_to_task_.erase(task_to_del);

    //renew the min_interval_ms_
    if(interval_del == min_interval_ms_){
      min_interval_ms_ = -1;
      for(auto pair : id_to_task_){
        if(pair.second.interval_ms < min_interval_ms_ || min_interval_ms_ == -1){
          min_interval_ms_ = pair.second.interval_ms;
        }
      }
    }

    //remove from exec queue
    ExecTsWithId target_key = {-1, 0};
    for(auto pair : exec_queue_){
      if(pair.id == task_id){
        target_key = {pair.exec_ts, pair.id};
        break;
      }
    }
    if(target_key.exec_ts != -1){
      exec_queue_.erase(target_key);
    }
    return true;
}

}  // namespace net
