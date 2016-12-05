// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <fstream>
#include <glog/logging.h>
#include <poll.h>
#include <sys/types.h>
#include <unistd.h>
#include <signal.h>
#include "slaveping_thread.h"
#include "trysync_thread.h"
#include "binlog_sync.h"

extern BinlogSync* g_binlog_sync;

TrysyncThread::~TrysyncThread() {
  should_exit_ = true;
  pthread_join(thread_id(), NULL);
  delete cli_;
  DLOG(INFO) << " Trysync thread " << pthread_self() << " exit!!!";
}

bool TrysyncThread::Send() {
  pink::RedisCmdArgsType argv;
  std::string wbuf_str;
  std::string requirepass = g_binlog_sync->requirepass();
  if (requirepass != "") {
    argv.push_back("auth");
    argv.push_back(requirepass);
    pink::RedisCli::SerializeCommand(argv, &wbuf_str);
  }

  argv.clear();
  std::string tbuf_str;
  argv.push_back("trysync");
  argv.push_back(g_binlog_sync->host());
  argv.push_back(std::to_string(g_binlog_sync->port()));
  uint32_t filenum;
  uint64_t pro_offset;
  g_binlog_sync->logger()->GetProducerStatus(&filenum, &pro_offset);
  
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

bool TrysyncThread::RecvProc() {
  bool should_auth = g_binlog_sync->requirepass() == "" ? false : true;
  bool is_authed = false;
  pink::Status s;
  std::string reply;

  while (1) {
    s = cli_->Recv(NULL);
    if (!s.ok()) {
      LOG(WARNING) << "Connect master, Recv, error: " <<strerror(errno);
      return false;
    }

    reply = cli_->argv_[0];
    DLOG(INFO) << "Reply from master after trysync: " << reply;
    if (!is_authed && should_auth) {
      if (kInnerReplOk != slash::StringToLower(reply)) {
        g_binlog_sync->RemoveMaster();
        return false;
      }
      is_authed = true;
    } else {
      if (cli_->argv_.size() == 1 &&
          slash::string2l(reply.data(), reply.size(), &sid_)) {
        // Luckly, I got your point, the sync is comming
        DLOG(INFO) << "Recv sid from master: " << sid_;
        break;
      }
      // Failed

      LOG(INFO) << "Sync Error, Quit";
      kill(getpid(), SIGQUIT);
      g_binlog_sync->RemoveMaster();
      
      return false;
    }
  }
  return true;
}

void* TrysyncThread::ThreadMain() {
  while (!should_exit_) {
    sleep(1);
    if (!g_binlog_sync->ShouldConnectMaster()) {
      continue;
    }
    sleep(2);
    DLOG(INFO) << "Should connect master";
    
    std::string master_ip = g_binlog_sync->master_ip();
    int master_port = g_binlog_sync->master_port();
    

    if ((cli_->Connect(master_ip, master_port)).ok()) {
      cli_->set_send_timeout(5000);
      cli_->set_recv_timeout(5000);
      if (Send() && RecvProc()) {
        g_binlog_sync->ConnectMasterDone();
        delete g_binlog_sync->ping_thread_;
        g_binlog_sync->ping_thread_ = new SlavepingThread(sid_);
        g_binlog_sync->ping_thread_->StartThread();
        DLOG(INFO) << "Trysync success";
      }
      cli_->Close();
    } else {
      LOG(WARNING) << "Failed to connect to master, " << master_ip << ":" << master_port;
    }
  }
  return NULL;
}
