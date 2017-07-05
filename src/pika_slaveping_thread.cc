// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <glog/logging.h>
#include <poll.h>

#include "pink/include/redis_cli.h"
#include "pika_slaveping_thread.h"
#include "pika_server.h"

extern PikaServer* g_pika_server;

Status PikaSlavepingThread::Send() {
  std::string wbuf_str;
  if (!is_first_send_) {
    pink::SerializeRedisCommand(&wbuf_str, "ping");
  } else {
    pink::RedisCmdArgsType argv;
    argv.push_back("spci");
    argv.push_back(std::to_string(sid_));
    pink::SerializeRedisCommand(argv, &wbuf_str);
    is_first_send_ = false;
    LOG(INFO) << wbuf_str;
  }
  return cli_->Send(&wbuf_str);
}

Status PikaSlavepingThread::RecvProc() {
  pink::RedisCmdArgsType argv;
  Status s = cli_->Recv(&argv);
  if (s.ok()) {
    slash::StringToLower(argv[0]);
    DLOG(INFO) << "Reply from master after ping: " << argv[0];
    if (argv[0] == "pong" || argv[0] == "ok") {
    } else {
      s = Status::Corruption("Reply is not pong or ok");
    }
  } else {
    LOG(WARNING) << "RecvProc, recv error: " << s.ToString();
  }
  return s;
}

void* PikaSlavepingThread::ThreadMain() {
  struct timeval last_interaction;
  struct timeval now;
  gettimeofday(&now, NULL);
  last_interaction = now;
  Status s;
  int connect_retry_times = 0;
  while (!should_stop() && g_pika_server->ShouldStartPingMaster()) {
    if (!should_stop() && (cli_->Connect(g_pika_server->master_ip(),
                                         g_pika_server->master_port() + 2000,
                                         g_pika_server->host())).ok()) {
      cli_->set_send_timeout(1000);
      cli_->set_recv_timeout(1000);
      connect_retry_times = 0;
      g_pika_server->PlusMasterConnection();
      while (true) {
        if (should_stop()) {
          LOG(INFO) << "Close Slaveping Thread now";
          cli_->Close();
          g_pika_server->KillBinlogSenderConn();
          break;
        }

        s = Send();
        if (s.ok()) {
          s = RecvProc();
        }
        if (s.ok()) {
          DLOG(INFO) << "Ping master success";
          gettimeofday(&last_interaction, NULL);
        } else if (s.IsTimeout()) {
          LOG(WARNING) << "Slaveping timeout once";
          gettimeofday(&now, NULL);
          if (now.tv_sec - last_interaction.tv_sec > 30) {
            //timeout;
            LOG(WARNING) << "Ping master timeout";
            cli_->Close();
            g_pika_server->KillBinlogSenderConn();
            break;
          }
        } else {
          LOG(WARNING) << "Ping master error";
          cli_->Close();
          g_pika_server->KillBinlogSenderConn();
          break;
        }
        sleep(1);
      }
      sleep(2);
      g_pika_server->MinusMasterConnection();
    } else if (!should_stop()) {
      LOG(WARNING) << "Slaveping, Connect timeout";
      if ((++connect_retry_times) >= 30) {
        LOG(WARNING) << "Slaveping, Connect timeout 10 times, disconnect with master";
        cli_->Close();
        g_pika_server->KillBinlogSenderConn();
        connect_retry_times = 0;
      }
    }
  }
  return NULL;
}
