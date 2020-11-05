// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_PROXY_CONN_H_
#define PIKA_PROXY_CONN_H_

#include "pink/include/redis_conn.h"
#include "pink/include/backend_thread.h"
#include "include/pika_client_conn.h"
#include <memory>
class ProxyCli;
class PikaProxyConn;

struct ProxyTask {
  ProxyTask() {
  }
  std::shared_ptr<PikaClientConn> conn_ptr;
  std::vector<pink::RedisCmdArgsType> redis_cmds;
  std::vector<Node> redis_cmds_forward_dst;
};

class PikaProxyConn: public pink::RedisConn {
 public:
  PikaProxyConn(int fd, std::string ip_port,
                 pink::Thread *server_thread,
                 pink::PinkEpoll* pink_epoll,
                 std::shared_ptr<ProxyCli> proxy_cli);
  bool IsAuthed() { return isAuthed_; }
  bool IsSelected() { return isSelected_; }
  bool IsClosed() { return closed_; }
  void SetClose() { closed_ = true;}
  virtual ~PikaProxyConn() {}

 private:
  int DealMessage(
      const pink::RedisCmdArgsType& argv,
      std::string* response) override;
  std::shared_ptr<ProxyCli> proxy_cli_;
  std::string auth_;
  bool isAuthed_;
  int table_;
  bool isSelected_;
  bool closed_;
};

struct ConnConfig {
  ConnConfig( int table, const std::string& auth,  int parallel)
    : table_(table), auth_(auth), parallel_(parallel) {}
    int table_ = 0;
    std::string auth_;
    int parallel_ = 10;
};

class ParallelConn {
  public:
ParallelConn(const std::string& addr, ConnConfig& config,
    std::shared_ptr<pink::BackendThread> client);

    Status Connect();
    Status Start();
    void Close();
    void Retain();
    bool Release();
    std::string Addr() { return addr_; }
    int GetTable() { return config_.table_; }
    Status PrepareConn();
  private:
    std::shared_ptr<pink::PinkConn> GetConn(int fd);
    void VerifyAuth(int fd);
    void SelectConn(int fd);
    void KeepAlive();
    void KeepAliveConn(int fd);


    //std::vector<std::shared_ptr<PikaClientConn>> parallelConn_;
    std::map<int, int> parallelConn_;
    std::set<int> tmpConns_; 
    std::string addr_;
    ConnConfig config_;
    std::atomic<int> refCount_;
    std::shared_ptr<pink::BackendThread> client_;
};

class ConnectionPool {
  public:
    ConnectionPool(const ConnConfig& config,
        std::shared_ptr<pink::BackendThread> client)
      : config_(config), client_(client) { }
    void Retain(std::string addr);
    void Release(std::string addr);
    void AddParallel(const std::string& addr);
  private:
    // addr and ptr
  ConnConfig config_;
  std::unordered_map<std::string, ParallelConn *> pool_; 
  std::shared_ptr<pink::BackendThread> client_;
};



#endif  // PIKA_PROXY_CONN_H_
