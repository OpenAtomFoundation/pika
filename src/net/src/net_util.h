// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_SRC_NET_UTIL_H_
#define NET_SRC_NET_UTIL_H_
#include <unistd.h>
#include <functional>
#include <memory>
#include <string>
#include <vector>
#include <set>
#include <unordered_map>
#include <chrono>
#include <cassert>


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
  //the exec time of the task, unit in ms
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

  uint32_t AddTimerTask(const std::string& task_name, int interval_ms, bool repeat_exec, const std::function<void()> &task){
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

  int64_t NowInMs(){
    auto now = std::chrono::system_clock::now();
    return std::chrono::time_point_cast<std::chrono::milliseconds>(now).time_since_epoch().count();
  }

  void ExecTimerTask(){
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
    for(auto task : fired_tasks_){
        exec_queue_.erase(task);
        auto it = id_to_task_.find(task.id);
        assert(it != id_to_task_.end());
        if(it->second.repeat_exec){
          //this task need to be repeatedly exec, register it again
          exec_queue_.insert({now_in_ms + it->second.interval_ms, task.id});
        }else{
          //this task only need to be exec once, completely remove this task
          id_to_task_.erase(task.id);
        }
    }
  };

  bool DelTimerTaskByTaskId(uint32_t task_id){
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

  int GetMinIntervalMs() const { return min_interval_ms_; }

 private:
  std::set<ExecTsWithId> exec_queue_;
  std::unordered_map<uint32_t, TimedTask> id_to_task_;
  uint32_t last_task_id_{0};
  int min_interval_ms_{-1};
};


}  // namespace net

#endif  //  NET_SRC_NET_UTIL_H_
