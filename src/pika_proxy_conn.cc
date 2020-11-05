// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_proxy_conn.h"

#include "pink/include/redis_cli.h"

#include "include/pika_proxy.h"

extern PikaProxy* g_pika_proxy;

PikaProxyConn::PikaProxyConn(int fd, std::string ip_port,
                             pink::Thread* thread,
                             pink::PinkEpoll* pink_epoll,
                             std::shared_ptr<ProxyCli> proxy_cli)
      : RedisConn(fd, ip_port, thread, pink_epoll,
        pink::HandleType::kSynchronous, PIKA_MAX_CONN_RBUF_HB),
        proxy_cli_(proxy_cli) {
}


int PikaProxyConn::DealMessage(
    const pink::RedisCmdArgsType& argv, std::string* response) {
  std::string res;
  for (auto& arg : argv) {
    res += arg;
  }
  g_pika_proxy->MayScheduleWritebackToCliConn(
      std::dynamic_pointer_cast<PikaProxyConn>(shared_from_this()),
      proxy_cli_, res);
  return 0;
}

ParallelConn::ParallelConn(const std::string& addr, ConnConfig& config,
    std::shared_ptr<pink::BackendThread> client)
  : addr_(addr), config_(config), client_(client) {
    refCount_ = 1;
}


Status ParallelConn::Connect() {
  int num  = parallelConn_.size() + tmpConns_.size(); 
  if (num > config_.parallel_) {
    return Status::OK();
  }
  for (int i = 0; i < num; i++) {
    std::string ip;
    int port, fd;
    if (!slash::ParseIpPortString(addr_, ip, port)) {
      LOG(INFO) << "parser addr " << addr_ << " error";
      return Status::InvalidArgument("paser addr error, addr: ", addr_);
    }
    Status s = client_->Connect(ip, port, &fd);
    if (!s.ok()) {
      LOG(INFO) << "connect addr: " << addr_ << "error: " << s.ToString();
      return s;
    }
    LOG(INFO) << "connect addr: " << addr_ << " fd: " << std::to_string(fd);
    tmpConns_.insert(fd);
  }
    return Status::OK();
}

std::shared_ptr<pink::PinkConn> ParallelConn::GetConn(int fd) {
  return client_->GetConn(fd);
}

void ParallelConn::VerifyAuth(int fd) {

}

void ParallelConn::SelectConn(int fd) {

}

void ParallelConn::KeepAlive() {

}

void ParallelConn::KeepAliveConn(int fd) {

}

Status ParallelConn::PrepareConn() {
  for(auto item : tmpConns_) {
    auto conn = std::dynamic_pointer_cast<PikaProxyConn>(GetConn(item));
    if (conn->IsAuthed()) {
      SelectConn(item);
    } else {
      VerifyAuth(item);
      SelectConn(item);
    } 
  }
  return Status::OK();
}

Status ParallelConn::Start() {
  Status s = Connect();
  if (!s.ok()) {
    return s;
  }
  s = PrepareConn();
  if (!s.ok()) {
    return s;
  }
  return Status::OK();
}

void ParallelConn::Close() {
  for (auto item : parallelConn_) {
    client_->Close(item.second);
  } 
  parallelConn_.clear(); 
  for (auto item : tmpConns_) {
    client_->Close(item);
  } 
  tmpConns_.clear();
}

void ParallelConn::Retain() {
  int expect = 0;
  if (refCount_.compare_exchange_strong(expect, -1)) {
    LOG(INFO) << "retain parallel conn ref count error";
    return;
  }
  refCount_++;
}

bool ParallelConn::Release() {
  int expect = 0;
  if (refCount_.compare_exchange_strong(expect, -1)) {
    LOG(INFO) << "release parallel conn ref count error";
    return true;
  }
  refCount_--;
  if (refCount_.compare_exchange_strong(expect, -1)) {
    return true;
  }
  return false;
}


void ConnectionPool::Release(std::string addr) {
  if (pool_.find(addr) == pool_.end()) {
    return;
  }
  auto parallel = pool_.find(addr)->second;
  if (parallel->Release()) {
    parallel->Close();
    delete parallel;
    pool_.erase(addr);
    LOG(INFO) << "release parallel conn :" << parallel->Addr() << " table :"
      << std::to_string(parallel->GetTable());
  }
}

void ConnectionPool::AddParallel(const std::string& addr) {
  auto conns = new ParallelConn(addr, config_, client_);
  pool_.insert(make_pair(addr, conns));
  conns->Start();
}

void ConnectionPool::Retain(std::string addr) {
  auto iter = pool_.find(addr);
  if (iter != pool_.end()) {
    iter->second->Retain();
    return;
  }
  AddParallel(addr);
}
