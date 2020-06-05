// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_proxy_cli.h"

#include <glog/logging.h>

#include <memory>

/* ProxyFactory */

ProxyFactory::ProxyFactory(std::shared_ptr<ProxyCli> proxy_cli)
  : proxy_cli_(proxy_cli) {
}

/* ProxyHandle */

void ProxyHandle::FdClosedHandle(int fd, const std::string& ip_port) const {
  proxy_cli_->LostConn(ip_port);
}

/* ProxyCli */

ProxyCli::ProxyCli(int cron_interval, int keepalive_timeout)
  : cron_interval_(cron_interval),
    keepalive_timeout_(keepalive_timeout) {
}

int ProxyCli::Start() {
  ProxyFactory* proxy_factory_ = new ProxyFactory(shared_from_this());
  ProxyHandle* proxy_handle_ = new ProxyHandle(shared_from_this());
  client_ptr_ = std::make_shared<pink::ClientThread>(
      proxy_factory_, cron_interval_,
      keepalive_timeout_, proxy_handle_, nullptr);

  int res = client_ptr_->StartThread();
  if (res != pink::kSuccess) {
    LOG(FATAL) << "Start Proxy ClientThread Error: "
      << res << (res == pink::kCreateThreadError ?
                        ": create thread error " : ": other error");
    return res;
  }
  return pink::kSuccess;
}

int ProxyCli::Stop() {
  client_ptr_->StopThread();
  delete proxy_factory_;
  delete proxy_handle_;
  return pink::kSuccess;
}

Status ProxyCli::WritebackUpdate(
    const std::string& ip_port,
    const std::string& res,
    bool* write_back,
    ProxyTask** res_task) {
  slash::MutexLock l(&input_l_);
  auto iter = backend_task_queue_.find(ip_port);
  if (iter == backend_task_queue_.end()) {
    return Status::NotFound(ip_port);
  }
  std::deque<ProxyCliTask>& queue = iter->second;
  ProxyCliTask cli_task;
  if (!queue.empty()) {
    cli_task = queue.front();
  } else {
    backend_task_queue_.erase(iter);
    return Status::NotFound(ip_port);
  }
  queue.pop_front();
  if (queue.empty()) {
    backend_task_queue_.erase(iter);
  }

  cli_task.resp_ptr->append(res);
  std::shared_ptr<PikaClientConn> conn_ptr = cli_task.conn_ptr;
  conn_ptr->resp_num--;

  if (conn_ptr->resp_num.load() == 0) {
    *write_back = true;
    const auto& iter = task_queue_.find(conn_ptr->ip_port());
    if (iter == task_queue_.end()) {
      LOG(WARNING) << "find ip_port()" << conn_ptr->ip_port() << " not found";
      return Status::Corruption(conn_ptr->ip_port());
    }
    *res_task = iter->second;
    task_queue_.erase(iter);
  }

  return Status::OK();
}

Status ProxyCli::ForwardToBackend(ProxyTask* task) {
  std::shared_ptr<PikaClientConn> conn_ptr = task->conn_ptr;
  conn_ptr->resp_num.store(task->redis_cmds.size());

  slash::MutexLock l(&input_l_);
  size_t loopsize =
    task->redis_cmds.size() == task->redis_cmds_forward_dst.size()
    ? task->redis_cmds.size() : 0;
  if (loopsize == 0) {
    return Status::Corruption("cmd and calculated routing not match");
  }
  for (size_t i = 0; i < loopsize; ++i) {
    std::shared_ptr<std::string> resp_ptr = std::make_shared<std::string>();
    conn_ptr->resp_array.push_back(resp_ptr);
    ProxyCliTask cli_task;
    cli_task.conn_ptr = conn_ptr;
    cli_task.resp_ptr = resp_ptr;
    pink::RedisCmdArgsType& redis_cmd = task->redis_cmds[i];

    std::string redis_cmd_str;
    // TODO(AZ) build more complex redis command
    redis_cmd_str.append("*" + std::to_string(redis_cmd.size()) + "\r\n");
    for (auto& cmd_param : redis_cmd) {
      redis_cmd_str.append(
          "$" + std::to_string(cmd_param.size()) + "\r\n" + cmd_param + "\r\n");
    }

    Node& node = task->redis_cmds_forward_dst[i];
    Status s = client_ptr_->Write(node.Ip(), node.Port(), redis_cmd_str);

    std::string ip_port = node.Ip() + ":" + std::to_string(node.Port());
    backend_task_queue_[ip_port].push_back(cli_task);
  }
  std::string ip_port = conn_ptr->ip_port();
  if (task_queue_.find(ip_port) != task_queue_.end()) {
    ProxyTask* tmp_task = task_queue_[ip_port];
    if (tmp_task) {
      delete tmp_task;
    }
  }
  task_queue_[ip_port] = task;
  return Status::OK();
}

void ProxyCli::LostConn(const std::string& ip_port) {
  slash::MutexLock l(&input_l_);
  auto iter = backend_task_queue_.find(ip_port);
  if (iter == backend_task_queue_.end()) {
    return;
  }
  std::deque<ProxyCliTask>& queue = iter->second;
  // all client whole cmd which sheduled to this ip_port will timeout
  for (auto& cli_task : queue) {
    std::shared_ptr<PikaClientConn> conn_ptr = cli_task.conn_ptr;
    auto iter = task_queue_.find(conn_ptr->ip_port());
    ProxyTask* proxy_task = iter->second;
    task_queue_.erase(iter);
    delete proxy_task;
  }
}
