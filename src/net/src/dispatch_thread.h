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
#include <glog/logging.h>

#include "net/src/net_util.h"
#include "net/include/net_conn.h"
#include "net/include/redis_conn.h"
#include "net/include/server_thread.h"
#include "pstd/include/env.h"
#include "pstd/include/xdebug.h"

enum BlockKeyType { Blpop, Brpop };
namespace net {

class NetItem;
class NetFiredEvent;
class WorkerThread;

struct BlockKey {  // this data struct is made for the scenario of multi dbs in pika.
  std::string db_name;
  std::string key;
  bool operator==(const BlockKey& p) const { return p.db_name == db_name && p.key == key; }
};
struct BlockKeyHash {
  std::size_t operator()(const BlockKey& k) const {
    return std::hash<std::string>{}(k.db_name) ^ std::hash<std::string>{}(k.key);
  }
};

class BlockConnNode {
 public:
  virtual ~BlockConnNode() {
    std::cout << "BlockConnNoded-" << conn_blocked_->fd() << " expire_time_:" << expire_time_ << std::endl;
  }
  BlockConnNode(int64_t expire_time, std::shared_ptr<RedisConn>& conn_blocked, BlockKeyType block_type)
      : expire_time_(expire_time), conn_blocked_(conn_blocked), block_type_(block_type) {}
  bool IsExpired();
  std::shared_ptr<RedisConn>& GetConnBlocked();
  BlockKeyType GetBlockType() const;

  // TO DO: delete this fun when testing is done
  void SelfPrint() {
    std::cout << "fd:" << conn_blocked_->fd() << ", expire_time:" << expire_time_ << ", blockType: " << block_type_
              << std::endl;
  }

 private:
  int64_t expire_time_;
  std::shared_ptr<RedisConn> conn_blocked_;
  BlockKeyType block_type_;
};

class DispatchThread : public ServerThread {
 public:
  DispatchThread(int port, int work_num, ConnFactory* conn_factory, int cron_interval, int queue_limit,
                 const ServerHandle* handle);
  DispatchThread(const std::string& ip, int port, int work_num, ConnFactory* conn_factory, int cron_interval,
                 int queue_limit, const ServerHandle* handle);
  DispatchThread(const std::set<std::string>& ips, int port, int work_num, ConnFactory* conn_factory, int cron_interval,
                 int queue_limit, const ServerHandle* handle);

  ~DispatchThread() override;

  int StartThread() override;

  int StopThread() override;

  void set_keepalive_timeout(int timeout) override;

  int conn_num() const override;

  std::vector<ServerThread::ConnInfo> conns_info() const override;

  std::shared_ptr<NetConn> MoveConnOut(int fd) override;

  void MoveConnIn(std::shared_ptr<NetConn> conn, const NotifyType& type) override;

  void KillAllConns() override;

  bool KillConn(const std::string& ip_port) override;

  void HandleNewConn(int connfd, const std::string& ip_port) override;

  void SetQueueLimit(int queue_limit) override;

  /**
   * BlPop/BrPop used start
   */
  void CleanWaitNodeOfUnBlockedBlrConn(std::shared_ptr<net::RedisConn> conn_unblocked);

  void CleanKeysAfterWaitNodeCleaned();

  // if a client closed the conn when waiting for the response of "blpop/brpop", some cleaning work must be done.
  void ClosingConnCheckForBlrPop(std::shared_ptr<net::RedisConn> conn_to_close);

  void ScanExpiredBlockedConnsOfBlrpop();

  std::unordered_map<BlockKey, std::unique_ptr<std::list<BlockConnNode>>, BlockKeyHash>& GetMapFromKeyToConns() {
    return key_to_conns_;
  }
  std::unordered_map<int, std::unique_ptr<std::list<BlockKey>>>& GetMapFromConnToKeys() {
    return conn_to_keys_;
  }
  std::shared_mutex& GetBlockMtx() { return block_mtx_; };

  net::TimedTaskManager* GetTimedTaskManager() { return &timedTaskManager_; }
  // BlPop/BrPop used end


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
  std::vector<std::unique_ptr<WorkerThread>> worker_thread_;
  int queue_limit_;
  std::map<WorkerThread*, void*> localdata_;

  void HandleConnEvent(NetFiredEvent* pfe) override { UNUSED(pfe); }



  /*
   *  Blpop/BRpop used
   */
  /*  key_to_conns_:
   *  mapping from "Blockkey"(eg. "<db0, list1>") to a list that stored the nodes of client connctions that
   *  were blocked by command blpop/brpop with key (eg. "list1").
   */
  std::unordered_map<BlockKey, std::unique_ptr<std::list<BlockConnNode>>, BlockKeyHash> key_to_conns_;

  /*
   *  conn_to_keys_:
   *  mapping from conn(fd) to a list of keys that the client is waiting for.
   */
  std::unordered_map<int, std::unique_ptr<std::list<BlockKey>>> conn_to_keys_;

  /*
   * latch of the two maps above.
   */
  std::shared_mutex block_mtx_;

  net::TimedTaskManager timedTaskManager_;

};  // class DispatchThread

}  // namespace net
#endif  // NET_SRC_DISPATCH_THREAD_H_
