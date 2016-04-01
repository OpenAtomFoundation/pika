#include <glog/logging.h>
#include <poll.h>
#include "pika_slaveping_thread.h"
#include "pika_trysync_thread.h"
#include "pika_server.h"
#include "pika_conf.h"

extern PikaServer* g_pika_server;
extern PikaConf* g_pika_conf;

bool PikaTrysyncThread::Send() {
  pink::RedisCmdArgsType argv;
  std::string wbuf_str;
  std::string requirepass = g_pika_conf->requirepass();
  if (requirepass != "") {
    argv.push_back("auth");
    argv.push_back(requirepass);
    pink::RedisCli::SerializeCommand(argv, &wbuf_str);
  }

  argv.clear();
  std::string tbuf_str;
  argv.push_back("trysync");
  argv.push_back(g_pika_server->host());
  argv.push_back(std::to_string(g_pika_server->port()));
  uint32_t filenum;
  uint64_t pro_offset;
  g_pika_server->logger_->GetProducerStatus(&filenum, &pro_offset);
  argv.push_back(std::to_string(filenum));
  argv.push_back(std::to_string(pro_offset));
  pink::RedisCli::SerializeCommand(argv, &tbuf_str);

  wbuf_str.append(tbuf_str);
  DLOG(INFO) << wbuf_str;

  pink::Status s;
  s = cli_->Send(&wbuf_str);
  if (!s.ok()) {
    LOG(WARNING) << "Connect master, Send, error: " <<strerror(errno);
    return false;
  }
  return true;
}

bool PikaTrysyncThread::RecvProc() {
  bool should_auth = g_pika_conf->requirepass() == "" ? false : true;
  bool is_authed = false;
  pink::Status s;

  while (1) {
    s = cli_->Recv(NULL);
    if (!s.ok()) {
      LOG(WARNING) << "Connect master, Recv, error: " <<strerror(errno);
      return false;
    }

    DLOG(INFO) << "Reply from master after trysync: " << cli_->argv_[0];
    if (!is_authed && should_auth) {
      slash::StringToLower(cli_->argv_[0]);
      if (cli_->argv_[0] != "ok") {
        g_pika_server->RemoveMaster();
        return false;
      }
      is_authed = true;
    } else {
      if (cli_->argv_.size() == 1 && slash::string2l(cli_->argv_[0].data(), cli_->argv_[0].size(), &sid_)) {
        DLOG(INFO) << "Recv sid from master: " << sid_;
        break;
      } else {
        g_pika_server->RemoveMaster();
        return false;
      }
    }
  }
  return true;
}

// TODO maybe use RedisCli
void* PikaTrysyncThread::ThreadMain() {
  while (!should_exit_) {
    if (g_pika_server->ShouldConnectMaster()) { //g_pika_server->repl_state_ == PIKA_REPL_CONNECT
      sleep(2);
      DLOG(INFO) << "Should connect master";
      if ((cli_->Connect(g_pika_server->master_ip(), g_pika_server->master_port())).ok()) {
        cli_->set_send_timeout(1000);
        cli_->set_recv_timeout(1000);
        if (Send() && RecvProc()) {
          g_pika_server->ConnectMasterDone();
          delete g_pika_server->ping_thread_;
          g_pika_server->ping_thread_ = new PikaSlavepingThread(sid_);
          g_pika_server->ping_thread_->StartThread();
          close(cli_->fd());
          DLOG(INFO) << "Trysync success";
        } else {
          close(cli_->fd());
        }
      }
    }
    sleep(1);
  }
  return NULL;
}
