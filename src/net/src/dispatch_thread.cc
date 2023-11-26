// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <vector>

#include <glog/logging.h>

#include "net/src/dispatch_thread.h"
#include "net/src/worker_thread.h"

namespace net {

DispatchThread::DispatchThread(int port, int work_num, ConnFactory* conn_factory, int cron_interval, int queue_limit,
                               const ServerHandle* handle)
    : ServerThread::ServerThread(port, cron_interval, handle),
      last_thread_(0),
      work_num_(work_num),
      queue_limit_(queue_limit) {
  for (int i = 0; i < work_num_; i++) {
    worker_thread_.emplace_back(std::make_unique<WorkerThread>(conn_factory, this, queue_limit, cron_interval));
  }
}

DispatchThread::DispatchThread(const std::string& ip, int port, int work_num, ConnFactory* conn_factory,
                               int cron_interval, int queue_limit, const ServerHandle* handle)
    : ServerThread::ServerThread(ip, port, cron_interval, handle),
      last_thread_(0),
      work_num_(work_num),
      queue_limit_(queue_limit) {
  for (int i = 0; i < work_num_; i++) {
    worker_thread_.emplace_back(std::make_unique<WorkerThread>(conn_factory, this, queue_limit, cron_interval));
  }
}

DispatchThread::DispatchThread(const std::set<std::string>& ips, int port, int work_num, ConnFactory* conn_factory,
                               int cron_interval, int queue_limit, const ServerHandle* handle)
    : ServerThread::ServerThread(ips, port, cron_interval, handle),
      last_thread_(0),
      work_num_(work_num),
      queue_limit_(queue_limit) {
  for (int i = 0; i < work_num_; i++) {
    worker_thread_.emplace_back(std::make_unique<WorkerThread>(conn_factory, this, queue_limit, cron_interval));
  }
}

DispatchThread::~DispatchThread() = default;

int DispatchThread::StartThread() {
  for (int i = 0; i < work_num_; i++) {
    int ret = handle_->CreateWorkerSpecificData(&(worker_thread_[i]->private_data_));
    if (ret) {
      return ret;
    }

    if (!thread_name().empty()) {
      worker_thread_[i]->set_thread_name("WorkerThread");
    }
    ret = worker_thread_[i]->StartThread();
    if (ret) {
      return ret;
    }
  }

  // Adding timer tasks and run timertaskThread
  timerTaskThread_.AddTimerTask("blrpop_blocking_info_scan", 250, true,
                                [this] { this->ScanExpiredBlockedConnsOfBlrpop(); });

  timerTaskThread_.StartThread();
  return ServerThread::StartThread();
}

int DispatchThread::StopThread() {
  for (int i = 0; i < work_num_; i++) {
    worker_thread_[i]->set_should_stop();
  }
  for (int i = 0; i < work_num_; i++) {
    int ret = worker_thread_[i]->StopThread();
    if (ret) {
      return ret;
    }
    if (worker_thread_[i]->private_data_) {
      ret = handle_->DeleteWorkerSpecificData(worker_thread_[i]->private_data_);
      if (ret) {
        return ret;
      }
      worker_thread_[i]->private_data_ = nullptr;
    }
  }
  timerTaskThread_.StopThread();
  return ServerThread::StopThread();
}

void DispatchThread::set_keepalive_timeout(int timeout) {
  for (int i = 0; i < work_num_; ++i) {
    worker_thread_[i]->set_keepalive_timeout(timeout);
  }
}

int DispatchThread::conn_num() const {
  int conn_num = 0;
  for (int i = 0; i < work_num_; ++i) {
    conn_num += worker_thread_[i]->conn_num();
  }
  return conn_num;
}

std::vector<ServerThread::ConnInfo> DispatchThread::conns_info() const {
  std::vector<ServerThread::ConnInfo> result;
  for (int i = 0; i < work_num_; ++i) {
    const auto worker_conns_info = worker_thread_[i]->conns_info();
    result.insert(result.end(), worker_conns_info.begin(), worker_conns_info.end());
  }
  return result;
}

std::shared_ptr<NetConn> DispatchThread::MoveConnOut(int fd) {
  for (int i = 0; i < work_num_; ++i) {
    std::shared_ptr<NetConn> conn = worker_thread_[i]->MoveConnOut(fd);
    if (conn) {
      return conn;
    }
  }
  return nullptr;
}

void DispatchThread::MoveConnIn(std::shared_ptr<NetConn> conn, const NotifyType& type) {
  std::unique_ptr<WorkerThread>& worker_thread = worker_thread_[last_thread_];
  bool success = worker_thread->MoveConnIn(conn, type, true);
  if (success) {
    last_thread_ = (last_thread_ + 1) % work_num_;
    conn->set_net_multiplexer(worker_thread->net_multiplexer());
  }
}

bool DispatchThread::KillConn(const std::string& ip_port) {
  bool result = false;
  for (int i = 0; i < work_num_; ++i) {
    result = worker_thread_[i]->TryKillConn(ip_port) || result;
  }
  return result;
}

void DispatchThread::KillAllConns() { KillConn(kKillAllConnsTask); }

void DispatchThread::HandleNewConn(const int connfd, const std::string& ip_port) {
  // Slow workers may consume many fds.
  // We simply loop to find next legal worker.
  NetItem ti(connfd, ip_port);
  LOG(INFO) << "accept new conn " << ti.String();
  int next_thread = last_thread_;
  bool find = false;
  for (int cnt = 0; cnt < work_num_; cnt++) {
    std::unique_ptr<WorkerThread>& worker_thread = worker_thread_[next_thread];
    find = worker_thread->MoveConnIn(ti, false);
    if (find) {
      last_thread_ = (next_thread + 1) % work_num_;
      LOG(INFO) << "find worker(" << next_thread << "), refresh the last_thread_ to " << last_thread_;
      break;
    }
    next_thread = (next_thread + 1) % work_num_;
  }

  if (!find) {
    LOG(INFO) << "all workers are full, queue limit is " << queue_limit_;
    // every worker is full
    // TODO(anan) maybe add log
    close(connfd);
  }
}

bool BlockedConnNode::IsExpired() {
  if (expire_time_ == 0) {
    return false;
  }
  auto now = std::chrono::system_clock::now();
  int64_t now_in_ms = std::chrono::time_point_cast<std::chrono::milliseconds>(now).time_since_epoch().count();
  if (expire_time_ <= now_in_ms) {
    return true;
  }
  return false;
}

std::shared_ptr<RedisConn>& BlockedConnNode::GetConnBlocked() { return conn_blocked_; }
BlockKeyType BlockedConnNode::GetBlockType() const { return block_type_; }

void DispatchThread::CleanWaitNodeOfUnBlockedBlrConn(std::shared_ptr<net::RedisConn> conn_unblocked) {
  // removed all the waiting info of this conn/ doing cleaning work
  auto pair = blocked_conn_to_keys_.find(conn_unblocked->fd());
  if (pair == blocked_conn_to_keys_.end()) {
    LOG(WARNING) << "blocking info of blpop/brpop went wrong, blpop/brpop can't working correctly";
    return;
  }
  auto& blpop_keys_list = pair->second;
  for (auto& blpop_key : *blpop_keys_list) {
    auto& wait_list_of_this_key = key_to_blocked_conns_.find(blpop_key)->second;
    for (auto conn = wait_list_of_this_key->begin(); conn != wait_list_of_this_key->end();) {
      if (conn->GetConnBlocked()->fd() == conn_unblocked->fd()) {
        conn = wait_list_of_this_key->erase(conn);
        break;
      }
      conn++;
    }
  }
  blocked_conn_to_keys_.erase(conn_unblocked->fd());
}

void DispatchThread::CleanKeysAfterWaitNodeCleaned() {
  // after wait info of a conn is cleaned, some wait list of keys might be empty, must erase them from the map
  std::vector<BlockKey> keys_to_erase;
  for (auto& pair : key_to_blocked_conns_) {
    if (pair.second->empty()) {
      // wait list of this key is empty, just erase this key
      keys_to_erase.emplace_back(pair.first);
    }
  }
  for (auto& blrpop_key : keys_to_erase) {
    key_to_blocked_conns_.erase(blrpop_key);
  }
}

void DispatchThread::ClosingConnCheckForBlrPop(std::shared_ptr<net::RedisConn> conn_to_close) {
  if (!conn_to_close) {
    // dynamic pointer cast failed, it's not an instance of RedisConn, no need of the process below
    return;
  }
  {
    std::shared_lock l(block_mtx_);
    if (blocked_conn_to_keys_.find(conn_to_close->fd()) == blocked_conn_to_keys_.end()) {
      // this conn_to_close is not disconnected from blocking state cause by "blpop/brpop"
      return;
    }
  }
  std::lock_guard l(block_mtx_);
  CleanWaitNodeOfUnBlockedBlrConn(conn_to_close);
  CleanKeysAfterWaitNodeCleaned();
}

void DispatchThread::ScanExpiredBlockedConnsOfBlrpop() {
  std::unique_lock latch(block_mtx_);
  for (auto& pair : key_to_blocked_conns_) {
    auto& conns_list = pair.second;
    for (auto conn_node = conns_list->begin(); conn_node != conns_list->end();) {
      if (conn_node->IsExpired()) {
        std::shared_ptr conn_ptr = conn_node->GetConnBlocked();
        conn_ptr->WriteResp("$-1\r\n");
        conn_ptr->NotifyEpoll(true);
        conn_node = conns_list->erase(conn_node);
        CleanWaitNodeOfUnBlockedBlrConn(conn_ptr);
      } else {
        conn_node++;
      }
    }
  }
  CleanKeysAfterWaitNodeCleaned();
}

void DispatchThread::SetQueueLimit(int queue_limit) { queue_limit_ = queue_limit; }

void DispatchThread::AllConn(const std::function<void(const std::shared_ptr<NetConn>&)>& func) {
  std::unique_lock l(block_mtx_);
  for (const auto& item : worker_thread_) {
    std::unique_lock wl(item->rwlock_);
    for (const auto& conn : item->conns_) {
      func(conn.second);
    }
  }
}

/**
 * @param keys format: tablename + key,because can watch the key of different db
 */
void DispatchThread::AddWatchKeys(const std::unordered_set<std::string>& keys,
                                  const std::shared_ptr<NetConn>& client_conn) {
  std::lock_guard lg(watch_keys_mu_);
  for (const auto& key : keys) {
    if (key_conns_map_.count(key) == 0) {
      key_conns_map_.emplace();
    }
    key_conns_map_[key].emplace(client_conn);
    conn_keys_map_[client_conn].emplace(key);
  }
}

void DispatchThread::RemoveWatchKeys(const std::shared_ptr<NetConn>& client_conn) {
  std::lock_guard lg(watch_keys_mu_);
  auto& keys = conn_keys_map_[client_conn];
  for (const auto& key : keys) {
    if (key_conns_map_.count(key) == 0 || key_conns_map_[key].count(client_conn) == 0) {
      continue;
    }
    key_conns_map_[key].erase(client_conn);
    if (key_conns_map_[key].empty()) {
      key_conns_map_.erase(key);
    }
  }
  conn_keys_map_.erase(client_conn);
}

std::vector<std::shared_ptr<NetConn>> DispatchThread::GetInvolvedTxn(const std::vector<std::string>& keys) {
  std::lock_guard lg(watch_keys_mu_);
  auto involved_conns = std::vector<std::shared_ptr<NetConn>>{};
  for (const auto& key : keys) {
    if (key_conns_map_.count(key) == 0 || key_conns_map_[key].empty()) {
      continue;
    }
    for (auto& client_conn : key_conns_map_[key]) {
      involved_conns.emplace_back(client_conn);
    }
  }
  return involved_conns;
}

std::vector<std::shared_ptr<NetConn>> DispatchThread::GetAllTxns() {
  std::lock_guard lg(watch_keys_mu_);
  auto involved_conns = std::vector<std::shared_ptr<NetConn>>{};
  for (auto& [client_conn, _] : conn_keys_map_) {
    involved_conns.emplace_back(client_conn);
  }
  return involved_conns;
}

std::vector<std::shared_ptr<NetConn>> DispatchThread::GetDBTxns(std::string db_name) {
  std::lock_guard lg(watch_keys_mu_);
  auto involved_conns = std::vector<std::shared_ptr<NetConn>>{};
  for (auto& [db_key, client_conns] : key_conns_map_) {
    if (db_key.find(db_name) == 0) {
      involved_conns.insert(involved_conns.end(), client_conns.begin(), client_conns.end());
    }
  }
  return involved_conns;
}

extern ServerThread* NewDispatchThread(int port, int work_num, ConnFactory* conn_factory, int cron_interval,
                                       int queue_limit, const ServerHandle* handle) {
  return new DispatchThread(port, work_num, conn_factory, cron_interval, queue_limit, handle);
}
extern ServerThread* NewDispatchThread(const std::string& ip, int port, int work_num, ConnFactory* conn_factory,
                                       int cron_interval, int queue_limit, const ServerHandle* handle) {
  return new DispatchThread(ip, port, work_num, conn_factory, cron_interval, queue_limit, handle);
}
extern ServerThread* NewDispatchThread(const std::set<std::string>& ips, int port, int work_num,
                                       ConnFactory* conn_factory, int cron_interval, int queue_limit,
                                       const ServerHandle* handle) {
  return new DispatchThread(ips, port, work_num, conn_factory, cron_interval, queue_limit, handle);
}

};  // namespace net
