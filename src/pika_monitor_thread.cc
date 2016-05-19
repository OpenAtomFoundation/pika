#include <string>
#include <utility>
#include <sys/time.h>

#include "pika_monitor_thread.h"
#include "pika_server.h"
#include "pink_define.h"
#include "pika_conf.h"

extern PikaServer* g_pika_server;
extern PikaConf* g_pika_conf;

PikaMonitorThread::PikaMonitorThread() :
  pink::Thread(),
  monitor_cond_(&monitor_mutex_protector_),
  is_running_(false),
  should_exit_(false) {
}

PikaMonitorThread::~PikaMonitorThread() {
  should_exit_ = true;
  if (is_running_) {
    monitor_cond_.SignalAll();
    pthread_join(thread_id(), NULL);
  }
  for (std::list<ClientInfo>::iterator iter = monitor_clients_.begin();
      iter != monitor_clients_.end();
      ++iter) {
    close(iter->fd);
  }
  DLOG(INFO) << " PikaMonitorThread " << pthread_self() << " exit!!!";
}

void PikaMonitorThread::AddMonitorClient(pink::RedisConn* client_ptr) {
  StartThread();
  slash::MutexLock lm(&monitor_mutex_protector_);
  monitor_clients_.push_back(ClientInfo{client_ptr->fd(), client_ptr->ip_port(), 0});
}

void PikaMonitorThread::RemoveMonitorClient(const std::string& ip_port) {
  std::list<ClientInfo>::iterator iter = monitor_clients_.begin();
  for (; iter != monitor_clients_.end(); ++iter) {
    if (ip_port == "all") {
      close(iter->fd);
      continue;
    }
    if (iter->ip_port  == ip_port) {
      close(iter->fd);
      break;
    }
  }
  if (ip_port == "all") {
    monitor_clients_.clear();
  } else if (iter != monitor_clients_.end()) {
    monitor_clients_.erase(iter);
  }
}

void PikaMonitorThread::AddMonitorMessage(std::string monitor_message) {
    slash::MutexLock lm(&monitor_mutex_protector_);
    if (monitor_messages_.empty() && cron_tasks_.empty()) {
      monitor_messages_.push_back(monitor_message);
      monitor_cond_.Signal();
    } else {
      monitor_messages_.push_back(monitor_message);
    }
}

int32_t PikaMonitorThread::ThreadClientList(std::vector<ClientInfo>* clients_ptr) {
  if (clients_ptr != NULL) {
    for (std::list<ClientInfo>::iterator iter = monitor_clients_.begin();
        iter != monitor_clients_.end();
        iter++) {
      clients_ptr->push_back(*iter);
    }
  }
  return monitor_clients_.size();
}

void PikaMonitorThread::AddCronTask(MonitorCronTask task) {
  slash::MutexLock lm(&monitor_mutex_protector_);
  if (monitor_messages_.empty() && cron_tasks_.empty()) {
    cron_tasks_.push(task);
    monitor_cond_.Signal();
  } else {
    cron_tasks_.push(task);
  }
}

bool PikaMonitorThread::FindClient(const std::string &ip_port) {
  slash::MutexLock lm(&monitor_mutex_protector_);
  for (std::list<ClientInfo>::iterator iter = monitor_clients_.begin();
      iter != monitor_clients_.end();
      ++iter) {
    if (iter->ip_port == ip_port) {
      return true;
    }
  }
  return false;
}

bool PikaMonitorThread::ThreadClientKill(const std::string& ip_port) {
  if (ip_port == "all") {
    AddCronTask({TASK_KILLALL, "all"});
  } else if (FindClient(ip_port)) {
    AddCronTask({TASK_KILL, ip_port});
  } else {
    return false;
  }
  return true;
}

bool PikaMonitorThread::HasMonitorClients() {
  slash::MutexLock lm(&monitor_mutex_protector_);
  return !monitor_clients_.empty();
}

void PikaMonitorThread::StartThread() {
  {
    slash::MutexLock lm(&monitor_mutex_protector_);
    if (is_running_) {
      return;
    }
    is_running_ = true;
  }
  pink::Thread::StartThread();
}

pink::WriteStatus PikaMonitorThread::SendMessage(int32_t fd, std::string& message) {
  ssize_t nwritten = 0, message_len_sended = 0, message_len_left = message.size();
  while (message_len_left > 0) {
    nwritten = write(fd, message.data() + message_len_sended, message_len_left);
    if (nwritten == -1 && errno == EAGAIN) {
      continue;
    } else if (nwritten == -1) {
      return pink::kWriteError;
    }
    message_len_sended += nwritten;
    message_len_left -= nwritten;
  }
  return pink::kWriteAll;
}

void* PikaMonitorThread::ThreadMain() {
  std::deque<std::string> messages_deque;
  std::string messages_transfer;
  MonitorCronTask task;
  pink::WriteStatus write_status;
  while (!should_exit_) {
    {
      slash::MutexLock lm(&monitor_mutex_protector_);
      while (monitor_messages_.empty() && cron_tasks_.empty() && !should_exit_) {
        monitor_cond_.Wait();
      }
    }
    if (should_exit_) {
      break;
    } 
    {
      slash::MutexLock lm(&monitor_mutex_protector_);
      while (!cron_tasks_.empty()) {
        task = cron_tasks_.front();
        cron_tasks_.pop();
        RemoveMonitorClient(task.ip_port);
        if (TASK_KILLALL) {
          std::queue<MonitorCronTask> empty_queue;
          cron_tasks_.swap(empty_queue);
        }
      }
    }

    messages_deque.clear();
    {
      slash::MutexLock lm(&monitor_mutex_protector_);
      messages_deque.swap(monitor_messages_);
      if (monitor_clients_.empty() || messages_deque.empty()) {
        continue;
      }
    }
    messages_transfer = "+";
    for (std::deque<std::string>::iterator iter = messages_deque.begin();
        iter != messages_deque.end();
        ++iter) {
      messages_transfer.append(iter->data(), iter->size());
      messages_transfer.append("\n"); 
    }
    if (messages_transfer == "+") {
      continue;
    }
    messages_transfer.replace(messages_transfer.size()-1, 1, "\r\n", 0, 2);
    monitor_mutex_protector_.Lock();
    for (std::list<ClientInfo>::iterator iter = monitor_clients_.begin();
        iter != monitor_clients_.end();
        ++iter) {
      write_status = SendMessage(iter->fd, messages_transfer);
      if (write_status == pink::kWriteError) {
        cron_tasks_.push({TASK_KILL, iter->ip_port});
      }
    }
    monitor_mutex_protector_.Unlock();
  }
  return NULL;
}
