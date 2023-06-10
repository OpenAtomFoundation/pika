// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_SRC_NET_UTIL_H_
#define NET_SRC_NET_UTIL_H_
#include <sys/epoll.h>
#include <sys/timerfd.h>
#include <unistd.h>
#include <functional>
#include <string>
#include <vector>
#include <memory>

namespace net {

int Setnonblocking(int sockfd);

typedef struct {
  std::string task_name;
  uint32_t event_type;
  std::function<void()> fun;
} TimedTask;

class TimedTaskManager {
 public:
  TimedTaskManager(int epoll_fd) : epoll_fd_(epoll_fd) {}
  ~TimedTaskManager() {
    for (auto &pair: tasks_) {
      epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, pair.first, nullptr);
      close(pair.first);
    }
  }
  /**
   * @param task_name name of the timed task
   * @param interval  exec time interval of the timed task, unit in [ms]
   * @param f addr of a function whose exec is the task itself
   * @param args parameters of the function
   * @return fd that related with the task, return -1 if the interval is invalid
   */
  template <class F, class... Args>
  int AddTimedTask(const std::string& task_name, int32_t interval, F&& f, Args&&... args) {
    int task_fd = CreateTimedfd(interval);
    if (task_fd == -1) {
      return -1;
    }

    std::function<void()> new_task = [f = std::forward<F>(f), args = std::make_tuple(std::forward<Args>(args)...), task_fd] {
      std::apply(f, args);
      uint64_t time_now;
      read(task_fd, &time_now, sizeof(time_now));
    };

    tasks_[task_fd] = {task_name, EPOLLIN, new_task};
    epoll_event event;
    event.data.fd = task_fd;
    event.events = EPOLLIN;
    epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, task_fd, &event);
    return task_fd;
  }

  int CreateTimedfd(int32_t interval);

  /**
   * @param fd the fd that fetchd from epoll_wait.
   * @return if this fd is bind to a timed task and which got executed, false if this fd dose not bind to a timed task.
   */
  bool TryToExecTimedTask(int fd, int32_t event_type);

  /**
   *
   * @param task_name
   * @return timed fd of the task, -1 if notfound,
   * only return the first found fd if there are multiple timed fd using same task_name
   */
  int GetTaskfdByTaskName(const std::string& task_name);

  void EraseTask(int task_fd);

  //no copying allowed
  TimedTaskManager(const TimedTaskManager& other) = delete;
  TimedTaskManager& operator=(const TimedTaskManager& other) = delete;
 private:
  int epoll_fd_;
  std::unordered_map<int, TimedTask> tasks_;
};

}  // namespace net

#endif  //  NET_SRC_NET_UTIL_H_
