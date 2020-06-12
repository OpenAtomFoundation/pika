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


