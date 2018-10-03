// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <glog/logging.h>
#include <poll.h>
#include "slaveping_thread.h"
#include "pika_port.h"

extern PikaPort* g_pika_port;

Status SlavepingThread::Send() {
  std::string wbuf_str;
  if (!is_first_send_) {
    pink::SerializeRedisCommand(&wbuf_str, "ping"); // reply == pong
  } else {
    pink::RedisCmdArgsType argv;
    argv.push_back("spci"); // reply == pong
    argv.push_back(std::to_string(sid_));
    pink::SerializeRedisCommand(argv, &wbuf_str);
    is_first_send_ = false;
    LOG(INFO) << "spci " << sid_;
  }

  return cli_->Send(&wbuf_str);
}

Status SlavepingThread::RecvProc() {
  pink::RedisCmdArgsType argv;
  Status s = cli_->Recv(&argv);
  if (s.ok()) {
    slash::StringToLower(argv[0]);
    // LOG(INFO) << "Reply from master after ping: " << argv[0];
    if (argv[0] == "pong" || argv[0] == "ok") {
    } else {
      s = Status::Corruption("");
    }
  } else {
    LOG(INFO) << "RecvProc, recv error: " << s.ToString();
  }
  return s;
}

void* SlavepingThread::ThreadMain() {
  struct timeval last_interaction;
  struct timeval now;
  gettimeofday(&now, NULL);
  last_interaction = now;
  Status s;
  int connect_retry_times = 0;
  while (!should_stop() && g_pika_port->ShouldStartPingMaster()) {
    if (!should_stop() && (cli_->Connect(g_pika_port->master_ip(), g_pika_port->master_port() + 2000)).ok()) {
      cli_->set_send_timeout(1000);
      cli_->set_recv_timeout(1000);
      connect_retry_times = 0;
      g_pika_port->PlusMasterConnection();
      while (true) {
        if (should_stop()) {
          LOG(INFO) << "Close Slaveping Thread now";
          close(cli_->fd());
          g_pika_port->binlog_receiver_thread()->KillBinlogSender();
          break;
        }

        s = Send();
        if (s.ok()) {
          s = RecvProc();
        }
        if (s.ok()) {
          // LOG(INFO) << "Ping master success";
          gettimeofday(&last_interaction, NULL);
        } else if (s.IsTimeout()) {
          LOG(INFO) << "Slaveping timeout once";
          gettimeofday(&now, NULL);
          if (now.tv_sec - last_interaction.tv_sec > 30) {
            //timeout;
            LOG(INFO) << "Ping master timeout";
            close(cli_->fd());
            g_pika_port->binlog_receiver_thread()->KillBinlogSender();
            break;
          }
        } else {
          LOG(INFO) << "Ping master error";
          close(cli_->fd());
          g_pika_port->binlog_receiver_thread()->KillBinlogSender();
          break;
        }
        sleep(1);
      }
      g_pika_port->MinusMasterConnection();
    } else if (!should_stop()) {
      LOG(INFO) << "Slaveping, Connect timeout";
      if ((++connect_retry_times) >= 30) {
        LOG(INFO) << "Slaveping, Connect timeout 10 times, disconnect with master";
        close(cli_->fd());
        g_pika_port->binlog_receiver_thread()->KillBinlogSender();
        connect_retry_times = 0;
      }
    }
  }
  return NULL;
}
