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

int TimedTaskManager::CreateTimedfd(int32_t interval) {
  if (interval <= 0) {
    return -1;
  }
  int fd = timerfd_create(CLOCK_REALTIME, 0);
  int sec = interval / 1000;
  int ms = interval % 1000;
  timespec current_time;
  clock_gettime(CLOCK_REALTIME, &current_time);
  itimerspec timer_spec;
  timer_spec.it_value = current_time;
  timer_spec.it_interval = {sec, ms * 1000000};
  timerfd_settime(fd, TFD_TIMER_ABSTIME, &timer_spec, nullptr);
  return fd;
}

bool TimedTaskManager::TryToExecTimedTask(int fd, int32_t event_type) {
  auto it = tasks_.find(fd);
  if (it == tasks_.end() || it->second.event_type != event_type) {
    return false;
  }
  it->second.fun();
  return true;
}

int TimedTaskManager::GetTaskfdByTaskName(const std::string& task_name) {
  for (auto& pair : tasks_) {
    if (task_name == pair.second.task_name) {
      return pair.first;
    }
  }
  return -1;
}

void TimedTaskManager::EraseTask(int task_fd) {
  auto it = tasks_.find(task_fd);
  if (it == tasks_.end()) {
    return;
  }
  tasks_.erase(task_fd);
  epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, task_fd, nullptr);
  close(task_fd);
}


}  // namespace net
