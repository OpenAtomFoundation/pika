// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_SRC_DISPATCH_THREAD_H_
#define NET_SRC_DISPATCH_THREAD_H_

#include <list>
#include <map>
#include <queue>
#include <set>
#include <string>
#include <vector>

#include "net/include/net_conn.h"
#include "net/include/redis_conn.h"
#include "net/include/server_thread.h"
#include "pstd/include/env.h"
#include "pstd/include/xdebug.h"

namespace net {

class NetItem;
class NetFiredEvent;
class WorkerThread;

enum BlockPopType { Blpop, Brpop };
typedef struct blrpopKey {  // this data struct is made for the scenario of multi dbs in pika.
  std::string db_name;
  std::string key;
  bool operator==(const blrpopKey& p) const { return p.db_name == db_name && p.key == key; }
} BlrPopKey;
struct BlrPopKeyHash {
  std::size_t operator()(const BlrPopKey& k) const {
    return std::hash<std::string>{}(k.db_name) ^ std::hash<std::string>{}(k.key);
  }
};

class BlockedPopConnNode {
 public:
  virtual ~BlockedPopConnNode() {
    std::cout << "BlockedPopConnNode: fd-" << conn_blocked_->fd() << " expire_time_:" << expire_time_ << std::endl;
  }
  BlockedPopConnNode(int64_t expire_time, std::shared_ptr<RedisConn>& conn_blocked, BlockPopType block_type)
      : expire_time_(expire_time), conn_blocked_(conn_blocked), block_type_(block_type) {}
  bool IsExpired() {
    if (expire_time_ == 0) {
      return false;
    }
    int64_t unix_time;
    pstd::TiemUtil::GetCurrentTime(&unix_time);
    if (expire_time_ <= unix_time) {
      return true;
    }
    return false;
  }
  std::shared_ptr<RedisConn>& GetConnBlocked() { return conn_blocked_; }
  BlockPopType GetBlockType() const { return block_type_; }

  void SelfPrint() {
    std::cout << "fd:" << conn_blocked_->fd() << ", expire_time:" << expire_time_ << ", blockType: " << block_type_
              << std::endl;
  }

 private:
  int64_t expire_time_;
  std::shared_ptr<RedisConn> conn_blocked_;
  BlockPopType block_type_;
};

class DispatchThread : public ServerThread {
 public:
  DispatchThread(int port, int work_num, ConnFactory* conn_factory, int cron_interval, int queue_limit,
                 const ServerHandle* handle);
  DispatchThread(const std::string& ip, int port, int work_num, ConnFactory* conn_factory, int cron_interval,
                 int queue_limit, const ServerHandle* handle);
  DispatchThread(const std::set<std::string>& ips, int port, int work_num, ConnFactory* conn_factory, int cron_interval,
                 int queue_limit, const ServerHandle* handle);

  virtual ~DispatchThread();

  virtual int StartThread() override;

  virtual int StopThread() override;

  virtual void set_keepalive_timeout(int timeout) override;

  virtual int conn_num() const override;

  virtual std::vector<ServerThread::ConnInfo> conns_info() const override;

  virtual std::shared_ptr<NetConn> MoveConnOut(int fd) override;

  virtual void MoveConnIn(std::shared_ptr<NetConn> conn, const NotifyType& type) override;

  virtual void KillAllConns() override;

  virtual bool KillConn(const std::string& ip_port) override;

  void HandleNewConn(const int connfd, const std::string& ip_port) override;

  void SetQueueLimit(int queue_limit) override;

  /**
   * BlPop/BrPop used
   */
  void CleanWaitInfoOfUnBlockedBlrConn(std::shared_ptr<net::RedisConn> conn_unblocked) {
    // removed all the waiting info of this conn/ doing cleaning work
    auto& blpop_keys_list = map_from_conns_to_keys_for_blrpop.find(conn_unblocked->fd())->second;
    for (auto& blpop_key : *blpop_keys_list) {
      auto& wait_list_of_this_key = map_from_keys_to_conns_for_blrpop.find(blpop_key)->second;
      for (auto conn = wait_list_of_this_key->begin(); conn != wait_list_of_this_key->end();) {
        if (conn->GetConnBlocked()->fd() == conn_unblocked->fd()) {
          wait_list_of_this_key->erase(conn);
          break;
        }
        conn++;
      }
    }
    map_from_conns_to_keys_for_blrpop.erase(conn_unblocked->fd());
  }

  void CleanKeysAfterWaitInfoCleaned(std::string table_name) {
    // after wait info of a conn is cleaned, some wait list of keys might be empty, must erase them from the map
    std::vector<BlrPopKey> keys_to_erase;
    for (auto& pair : map_from_keys_to_conns_for_blrpop) {
      if (pair.second->empty()) {
        // wait list of this key is empty, just erase this key
        keys_to_erase.emplace_back(pair.first);
      }
    }
    for (auto& blrpop_key : keys_to_erase) {
      map_from_keys_to_conns_for_blrpop.erase(blrpop_key);
    }
  }

  // if a client closed the conn when waiting for the response of "blpop/brpop", some cleaning work must be done.
  void ClosingConnCheckForBlrPop(std::shared_ptr<net::RedisConn> conn_to_close) {
    if (!conn_to_close) {
      // dynamic pointer cast failed, it's not an instance of RedisConn, no need of the process below
      return;
    }
    std::lock_guard l(bLRPop_blocking_map_latch_);
    if (map_from_conns_to_keys_for_blrpop.find(conn_to_close->fd()) == map_from_conns_to_keys_for_blrpop.end()) {
      // this conn_to_close is not disconnected from blocking state cause by "blpop/brpop"
      return;
    }
    CleanWaitInfoOfUnBlockedBlrConn(conn_to_close);
    CleanKeysAfterWaitInfoCleaned(conn_to_close->GetCurrentTable());
  }

  std::unordered_map<BlrPopKey, std::unique_ptr<std::list<BlockedPopConnNode>>, BlrPopKeyHash>&
  GetMapFromKeysToConnsForBlrpop() {
    return map_from_keys_to_conns_for_blrpop;
  }
  std::unordered_map<int, std::unique_ptr<std::list<BlrPopKey>>>& GetMapFromConnsToKeysForBlrpop() {
    return map_from_conns_to_keys_for_blrpop;
  }
  std::shared_mutex& GetBLRPopBlockingMapLatch() { return bLRPop_blocking_map_latch_; };

 private:
  /*
   * Here we used auto poll to find the next work thread,
   * last_thread_ is the last work thread
   */
  int last_thread_;
  int work_num_;
  /*
   * This is the work threads
   */
  WorkerThread** worker_thread_;
  int queue_limit_;
  std::map<WorkerThread*, void*> localdata_;

  void HandleConnEvent(NetFiredEvent* pfe) override { UNUSED(pfe); }

  // No copying allowed
  DispatchThread(const DispatchThread&);
  void operator=(const DispatchThread&);

  /*
   *  Blpop/BRpop used
   */
  /*  map_from_keys_to_conns_for_blrpop:
   *  mapping from "Blrpopkey"(eg. "<db0, list1>") to a list that stored the blocking info of client-connetions that
   * were blocked by command blpop/brpop with key (eg. "list1").
   */
  std::unordered_map<BlrPopKey, std::unique_ptr<std::list<BlockedPopConnNode>>, BlrPopKeyHash>
      map_from_keys_to_conns_for_blrpop;

  /*
   *  map_from_conns_to_keys_for_blrpop:
   *  mapping from conn(fd) to a list of keys that the client is waiting for.
   */
  std::unordered_map<int, std::unique_ptr<std::list<BlrPopKey>>> map_from_conns_to_keys_for_blrpop;

  /*
   * latch of the two maps above.
   */
  std::shared_mutex bLRPop_blocking_map_latch_;

};  // class DispatchThread

}  // namespace net
#endif  // NET_SRC_DISPATCH_THREAD_H_
