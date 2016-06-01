#include <glog/logging.h>
#include <poll.h>
#include "slaveping_thread.h"
#include "binlog_sync.h"

extern BinlogSync* g_binlog_sync;

pink::Status SlavepingThread::Send() {
  std::string wbuf_str;
  if (!is_first_send_) {
    pink::RedisCli::SerializeCommand(&wbuf_str, "ping");
  } else {
    pink::RedisCmdArgsType argv;
    argv.push_back("spci");
    argv.push_back(std::to_string(sid_));
    pink::RedisCli::SerializeCommand(argv, &wbuf_str);
    is_first_send_ = false;
  }
  DLOG(INFO) << wbuf_str;
  return cli_->Send(&wbuf_str);
}

pink::Status SlavepingThread::RecvProc() {
  pink::Status s = cli_->Recv(NULL);
  if (s.ok()) {
    slash::StringToLower(cli_->argv_[0]);
    DLOG(INFO) << "Reply from master after ping: " << cli_->argv_[0];
    if (cli_->argv_[0] == "pong" || cli_->argv_[0] == "ok") {
    } else {
      s = pink::Status::Corruption("");
    }
  } else {
    DLOG(INFO) << "RecvProc, recv error: " << s.ToString();
  }
  return s;
}

void* SlavepingThread::ThreadMain() {
  struct timeval last_interaction;
  struct timeval now;
  gettimeofday(&now, NULL);
  last_interaction = now;
  pink::Status s;
  int connect_retry_times = 0;
  while (!should_exit_ && g_binlog_sync->ShouldStartPingMaster()) {
    if (!should_exit_ && (cli_->Connect(g_binlog_sync->master_ip(), g_binlog_sync->master_port() + 200)).ok()) {
      cli_->set_send_timeout(1000);
      cli_->set_recv_timeout(1000);
      connect_retry_times = 0;
      g_binlog_sync->PlusMasterConnection();
      while (true) {
        if (should_exit_) {
          DLOG(INFO) << "Close Slaveping Thread now";
          close(cli_->fd());
          g_binlog_sync->binlog_receiver_thread()->KillBinlogSender();
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
          DLOG(INFO) << "Slaveping timeout once";
          gettimeofday(&now, NULL);
          if (now.tv_sec - last_interaction.tv_sec > 30) {
            //timeout;
            DLOG(INFO) << "Ping master timeout";
            close(cli_->fd());
            g_binlog_sync->binlog_receiver_thread()->KillBinlogSender();
            break;
          }
        } else {
          DLOG(INFO) << "Ping master error";
          close(cli_->fd());
          g_binlog_sync->binlog_receiver_thread()->KillBinlogSender();
          break;
        }
        sleep(1);
      }
      g_binlog_sync->MinusMasterConnection();
    } else if (!should_exit_) {
      DLOG(INFO) << "Slaveping, Connect timeout";
      if ((++connect_retry_times) >= 30) {
        DLOG(INFO) << "Slaveping, Connect timeout 10 times, disconnect with master";
        close(cli_->fd());
        g_binlog_sync->binlog_receiver_thread()->KillBinlogSender();
        connect_retry_times = 0;
      }
    }
  }
  return NULL;
}
