// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_monitor_thread.h"

#include <glog/logging.h>

PikaMonitorThread::PikaMonitorThread()  {
  set_thread_name("MonitorThread");
  has_monitor_clients_.store(false);
}

PikaMonitorThread::~PikaMonitorThread() {
  set_should_stop();
  if (is_running()) {
    monitor_cond_.notify_all();
    StopThread();
  }
  for (auto & monitor_client : monitor_clients_) {
    close(monitor_client.fd);
  }
  LOG(INFO) << "PikaMonitorThread " << pthread_self() << " exit!!!";
}

void PikaMonitorThread::AddMonitorClient(const std::shared_ptr<PikaClientConn>& client_ptr) {
  StartThread();
  std::lock_guard lm(monitor_mutex_protector_);
  monitor_clients_.push_back(ClientInfo{client_ptr->fd(), client_ptr->ip_port(), 0, client_ptr});
  has_monitor_clients_.store(true);
}

void PikaMonitorThread::RemoveMonitorClient(const std::string& ip_port) {
  auto iter = monitor_clients_.begin();
  for (; iter != monitor_clients_.end(); ++iter) {
    if (ip_port == "all") {
      close(iter->fd);
      continue;
    }
    if (iter->ip_port == ip_port) {
      close(iter->fd);
      break;
    }
  }
  if (ip_port == "all") {
    monitor_clients_.clear();
  } else if (iter != monitor_clients_.end()) {
    monitor_clients_.erase(iter);
  }
  has_monitor_clients_.store(!monitor_clients_.empty());
}

void PikaMonitorThread::AddMonitorMessage(const std::string& monitor_message) {
  std::lock_guard lm(monitor_mutex_protector_);
  if (monitor_messages_.empty() && cron_tasks_.empty()) {
    monitor_messages_.push_back(monitor_message);
    monitor_cond_.notify_one();
  } else {
    monitor_messages_.push_back(monitor_message);
  }
}

int32_t PikaMonitorThread::ThreadClientList(std::vector<ClientInfo>* clients_ptr) {
  if (clients_ptr) {
    for (auto & monitor_client : monitor_clients_) {
      clients_ptr->push_back(monitor_client);
    }
  }
  return monitor_clients_.size();
}

void PikaMonitorThread::AddCronTask(const MonitorCronTask& task) {
  std::lock_guard lm(monitor_mutex_protector_);
  if (monitor_messages_.empty() && cron_tasks_.empty()) {
    cron_tasks_.push(task);
    monitor_cond_.notify_one();
  } else {
    cron_tasks_.push(task);
  }
}

bool PikaMonitorThread::FindClient(const std::string& ip_port) {
  std::lock_guard lm(monitor_mutex_protector_);
  for (auto & monitor_client : monitor_clients_) {
    if (monitor_client.ip_port == ip_port) {
      return true;
    }
  }
  return false;
}

bool PikaMonitorThread::ThreadClientKill(const std::string& ip_port) {
  if (is_running()) {
    if (ip_port == "all") {
      AddCronTask({TASK_KILLALL, "all"});
    } else if (FindClient(ip_port)) {
      AddCronTask({TASK_KILL, ip_port});
    } else {
      return false;
    }
  }
  return true;
}

bool PikaMonitorThread::HasMonitorClients() { return has_monitor_clients_.load(); }

net::WriteStatus PikaMonitorThread::SendMessage(int32_t fd, std::string& message) {
  size_t retry = 0;
  ssize_t nwritten = 0;
  ssize_t message_len_sended = 0;
  ssize_t message_len_left = message.size();
  while (message_len_left > 0) {
    nwritten = write(fd, message.data() + message_len_sended, message_len_left);
    if (nwritten == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      // If the write buffer is full, but the client no longer consumes, it will
      // get stuck in the loop and cause the entire Pika to block becase of monitor_mutex_protector_.
      // So we put a limit on the number of retries
      if (++retry >= 10) {
        return net::kWriteError;
      } else {
        // Sleep one second wait for client consume message
        sleep(1);
        continue;
      }
    } else if (nwritten == -1) {
      return net::kWriteError;
    }
    if (retry > 0) {
      retry = 0;
    }
    message_len_sended += nwritten;
    message_len_left -= nwritten;
  }
  return net::kWriteAll;
}

void* PikaMonitorThread::ThreadMain() {
  std::deque<std::string> messages_deque;
  std::string messages_transfer;
  MonitorCronTask task;
  net::WriteStatus write_status;
  while (!should_stop()) {
    {
      std::unique_lock lm(monitor_mutex_protector_);
      monitor_cond_.wait(lm, [this]() { return !monitor_messages_.empty() || !cron_tasks_.empty() || should_stop(); });
    }
    if (should_stop()) {
      break;
    }
    {
      std::lock_guard lm(monitor_mutex_protector_);
      while (!cron_tasks_.empty()) {
        task = cron_tasks_.front();
        cron_tasks_.pop();
        RemoveMonitorClient(task.ip_port);
        if (task.task == TASK_KILLALL) {
          std::queue<MonitorCronTask> empty_queue;
          cron_tasks_.swap(empty_queue);
        }
      }
    }

    messages_deque.clear();
    {
      std::lock_guard lm(monitor_mutex_protector_);
      messages_deque.swap(monitor_messages_);
      if (monitor_clients_.empty() || messages_deque.empty()) {
        continue;
      }
    }
    messages_transfer = "+";
    for (const auto& msg : messages_deque) {
      messages_transfer.append(msg.data(), msg.size());
      messages_transfer.append(" ", 1);
    }
    if (messages_transfer == "+") {
      continue;
    }

    messages_transfer.pop_back(); // no space follow last param
    messages_transfer.append("\r\n", 2);

    std::lock_guard lm(monitor_mutex_protector_);
    for (auto & monitor_client : monitor_clients_) {
      write_status = SendMessage(monitor_client.fd, messages_transfer);
      if (write_status == net::kWriteError) {
        cron_tasks_.push({TASK_KILL, monitor_client.ip_port});
      }
    }
  }
  return nullptr;
}
