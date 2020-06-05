// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_PROXY_CONN_H_
#define PIKA_PROXY_CONN_H_

#include "pink/include/redis_conn.h"

#include "include/pika_client_conn.h"

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
  virtual ~PikaProxyConn() {}

 private:
  int DealMessage(
      const pink::RedisCmdArgsType& argv,
      std::string* response) override;

  std::shared_ptr<ProxyCli> proxy_cli_;
};

#endif  // PIKA_PROXY_CONN_H_
