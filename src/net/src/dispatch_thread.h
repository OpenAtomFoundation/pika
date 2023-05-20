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
#include "pstd/include/xdebug.h"
#include "pstd/include/env.h"

namespace net {

class NetItem;
class NetFiredEvent;
class WorkerThread;

enum BlockPopType { Blpop, Brpop };
typedef struct blrpopKey{  // this data struct is made for the scenario of multi dbs in pika.
  std::string db_name;
  std::string key;
  bool operator==(const blrpopKey& p) const{
    return p.db_name == db_name && p.key == key;
  }
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
          conn = wait_list_of_this_key->erase(conn);
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

  void BlockThisClientToWaitLRPush(std::shared_ptr<net::RedisConn> conn_to_block, std::vector<std::string>& keys,
                                   int64_t expire_time, BlockPopType block_pop_type) {
    std::lock_guard latch(bLRPop_blocking_map_latch_);
    std::vector<BlrPopKey> blrpop_keys;
    for (auto& key : keys) {
      BlrPopKey blrpop_key{conn_to_block->GetCurrentTable(), key};
      blrpop_keys.push_back(blrpop_key);
      auto it = map_from_keys_to_conns_for_blrpop.find(blrpop_key);
      if (it == map_from_keys_to_conns_for_blrpop.end()) {
        // no waiting info found, means no other clients are waiting for the list related with this key right now
        map_from_keys_to_conns_for_blrpop.emplace(blrpop_key, std::make_unique<std::list<BlockedPopConnNode>>());
        it = map_from_keys_to_conns_for_blrpop.find(blrpop_key);
      }
      auto& wait_list_of_this_key = it->second;
      // add current client-connection to the tail of waiting list of this key
      wait_list_of_this_key->emplace_back(expire_time, conn_to_block, block_pop_type);
    }

    // construct a list of keys and insert into this map as value(while key of the map is conn_fd)
    map_from_conns_to_keys_for_blrpop.emplace(
        conn_to_block->fd(), std::make_unique<std::list<BlrPopKey>>(blrpop_keys.begin(), blrpop_keys.end()));

    std::cout << "-------------db name:" << conn_to_block->GetCurrentTable() << "-------------" << std::endl;
    std::cout << "from key to conn:" << std::endl;
    for (auto& pair : map_from_keys_to_conns_for_blrpop) {
      std::cout << "key:<" << pair.first.db_name << "," << pair.first.key << ">  list of it:" << std::endl;
      for (auto& it : *pair.second) {
        it.SelfPrint();
      }
    }

    std::cout << "\n\nfrom conn to key:" << std::endl;
    for (auto& pair : map_from_conns_to_keys_for_blrpop) {
      std::cout << "fd:" << pair.first << "  related keys:" << std::endl;
      for (auto& it : *pair.second) {
        std::cout << " <" << it.db_name << "," << it.key << "> " << std::endl;
      }
    }
    std::cout << "-----------end------------------" << std::endl;
  }
/*
  void TryToServeBLrPopWithThisKey(const std::string& key, const std::string& table_name,
                                   std::shared_ptr<Partition> partition) {
    std::lock_guard latch(bLRPop_blocking_map_latch_);
    BlrPopKey blrPop_key{table_name, key};
    auto it = map_from_keys_to_conns_for_blrpop.find(blrPop_key);
    if (it == map_from_keys_to_conns_for_blrpop.end()) {
      // no client is waitting for this key
      return;
    }

    auto& waitting_list_of_this_key = it->second;
    std::string value;
    rocksdb::Status s;
    // traverse this list from head to tail(in the order of adding sequence) which means "first blocked, first get
    // served“
    CmdRes res;
    for (auto conn_blocked = waitting_list_of_this_key->begin(); conn_blocked != waitting_list_of_this_key->end();) {
      auto conn_ptr = conn_blocked->GetConnBlocked();

      if (conn_blocked->GetBlockType() == BlockPopType::Blpop) {
        s = partition->db()->LPop(key, &value);
      } else {  // BlockPopType is Brpop
        s = partition->db()->RPop(key, &value);
      }

      if (s.ok()) {
        res.AppendArrayLen(2);
        res.AppendString(key);
        res.AppendString(value);
      } else if (s.IsNotFound()) {
        // this key has no more elements to serve more blocked conn.
        break;
      } else {
        res.SetRes(CmdRes::kErrOther, s.ToString());
      }
      // send response to this client
      conn_ptr->WriteResp(res.message());
      res.clear();
      conn_ptr->NotifyEpoll(true);
      conn_blocked = waitting_list_of_this_key->erase(conn_blocked);  // remove this conn from current waiting list
      // erase all waiting info of this conn
      CleanWaitInfoOfUnBlockedBlrConn(conn_ptr);
    }
    CleanKeysAfterWaitInfoCleaned(table_name);
  }*/

  // if a client closed the conn when waiting for the response of "blpop/brpop", some cleaning work must be done.
  void ClosingConnCheckForBlrPop(std::shared_ptr<net::RedisConn> conn_to_close) {
    std::shared_ptr<net::RedisConn> conn = conn_to_close;
    if (!conn) {
      // it's not an instance of PikaClientConn, no need of the process below
      return;
    }
    std::lock_guard l(bLRPop_blocking_map_latch_);
    auto keys_list = map_from_conns_to_keys_for_blrpop.find(conn->fd());
    if (keys_list == map_from_conns_to_keys_for_blrpop.end()) {
      // this conn is not disconnected from with blocking state cause by "blpop/brpop"
      return;
    }
    CleanWaitInfoOfUnBlockedBlrConn(conn);
    CleanKeysAfterWaitInfoCleaned(conn->GetCurrentTable());
  }


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

  std::unordered_map<BlrPopKey, std::unique_ptr<std::list<BlockedPopConnNode>>, BlrPopKeyHash> map_from_keys_to_conns_for_blrpop;

  /*
   *  map_from_conns_to_keys_for_blrpop:
   *  mapping from conn(fd) to a list of keys that the client is waiting for.
   */
  std::unordered_map<int, std::unique_ptr<std::list<BlrPopKey>>> map_from_conns_to_keys_for_blrpop;

  /*
   * latch of the two maps above.
   */
  std::mutex bLRPop_blocking_map_latch_;


};  // class DispatchThread

}  // namespace net
#endif  // NET_SRC_DISPATCH_THREAD_H_
