// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <fstream>
#include <glog/logging.h>
#include <poll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "slash/include/env.h"
#include "slash/include/rsync.h"
#include "slash/include/slash_status.h"
#include "include/pika_slaveping_thread.h"
#include "include/pika_trysync_thread.h"
#include "include/pika_server.h"
#include "include/pika_conf.h"

extern PikaServer* g_pika_server;
extern PikaConf* g_pika_conf;

PikaTrysyncThread::~PikaTrysyncThread() {
  StopThread();
  slash::StopRsync(g_pika_conf->db_sync_path());
  delete cli_;
  LOG(INFO) << " Trysync thread " << thread_id() << " exit!!!";
}

bool PikaTrysyncThread::Send(std::string lip) {
  pink::RedisCmdArgsType argv;
  std::string wbuf_str;
  std::string masterauth = g_pika_conf->masterauth();
  if (masterauth != "") {
    argv.push_back("auth");
    argv.push_back(masterauth);
    pink::SerializeRedisCommand(argv, &wbuf_str);
  }

  argv.clear();
  std::string tbuf_str;
  argv.push_back("trysync");
  argv.push_back(lip);
  argv.push_back(std::to_string(g_pika_server->port()));
  uint32_t filenum;
  uint64_t pro_offset;
  g_pika_server->logger_->GetProducerStatus(&filenum, &pro_offset);
  
  if (g_pika_server->force_full_sync()) {
    argv.push_back(std::to_string(UINT32_MAX));
    argv.push_back(std::to_string(0));
  } else if (g_pika_server->DoubleMasterMode()) {
    uint64_t double_recv_offset;
    uint32_t double_recv_num;
    g_pika_server->logger_->GetDoubleRecvInfo(&double_recv_num, &double_recv_offset);
    argv.push_back(std::to_string(double_recv_num));
    argv.push_back(std::to_string(double_recv_offset));
  } else {
    argv.push_back(std::to_string(filenum));
    argv.push_back(std::to_string(pro_offset));
  }
  pink::SerializeRedisCommand(argv, &tbuf_str);

  wbuf_str.append(tbuf_str);
  LOG(INFO) << wbuf_str;

  slash::Status s = cli_->Send(&wbuf_str);
  if (!s.ok()) {
    LOG(WARNING) << "Connect master, Send, error: " <<strerror(errno);
    return false;
  }
  return true;
}

bool PikaTrysyncThread::RecvProc() {
  bool should_auth = g_pika_conf->masterauth() == "" ? false : true;
  bool is_authed = false;
  slash::Status s;
  std::string reply;

  pink::RedisCmdArgsType argv;
  while (1) {
    s = cli_->Recv(&argv);
    if (!s.ok()) {
      LOG(WARNING) << "Connect master, Recv, error: " <<strerror(errno);
      return false;
    }

    reply = argv[0];
    LOG(WARNING) << "Reply from master after trysync: " << reply;
    if (!is_authed && should_auth) {
      if (kInnerReplOk != slash::StringToLower(reply)) {
        LOG(WARNING) << "Auth with master error: " << reply;
        return false;
      }
      is_authed = true;
    } else {
      if (argv.size() == 1 &&
          slash::string2l(reply.data(), reply.size(), &sid_)) {
        // Luckly, I got your point, the sync is comming
        LOG(INFO) << "Recv sid from master: " << sid_;
        g_pika_server->SetSid(sid_);
        break;
      }

      // Failed
      if (kInnerReplWait == reply) {
        // You can't sync this time, but may be different next time,
        // This may happened when 
        // 1, Master do bgsave first.
        // 2, Master waiting for an existing bgsaving process
        // 3, Master do dbsyncing
        LOG(INFO) << "Need wait to sync";
        g_pika_server->NeedWaitDBSync();
      } else {
        LOG(WARNING) << "Connect to master error: " << reply;
        // In double master mode
        if (g_pika_server->IsDoubleMaster(g_pika_server->master_ip(), g_pika_server->master_port())) {
          g_pika_server->RemoveMaster();
          LOG(INFO) << "Because the invalid filenum and offset, close the connection between the peer-masters";
        } else {  // In master slave mode
          LOG(WARNING) << "something wrong with sync, come in SyncError stage";
          g_pika_server->SyncError();
        }
      }
      return false;
    }
  }
  return true;
}

// Try to update master offset
// This may happend when dbsync from master finished
// Here we do:
// 1, Check dbsync finished, got the new binlog offset
// 2, Replace the old db
// 3, Update master offset, and the PikaTrysyncThread cron will connect and do slaveof task with master
bool PikaTrysyncThread::TryUpdateMasterOffset() {
  // Check dbsync finished
  std::string info_path = g_pika_conf->db_sync_path() + kBgsaveInfoFile;
  if (!slash::FileExists(info_path)) {
    return false;
  }

  // Got new binlog offset
  std::ifstream is(info_path);
  if (!is) {
    LOG(WARNING) << "Failed to open info file after db sync";
    return false;
  }
  std::string line, master_ip;
  int lineno = 0;
  int64_t filenum = 0, offset = 0, tmp = 0, master_port = 0;
  while (std::getline(is, line)) {
    lineno++;
    if (lineno == 2) {
      master_ip = line;
    } else if (lineno > 2 && lineno < 6) {
      if (!slash::string2l(line.data(), line.size(), &tmp) || tmp < 0) {
        LOG(WARNING) << "Format of info file after db sync error, line : " << line;
        is.close();
        return false;
      }
      if (lineno == 3) { master_port = tmp; }
      else if (lineno == 4) { filenum = tmp; }
      else { offset = tmp; }

    } else if (lineno > 5) {
      LOG(WARNING) << "Format of info file after db sync error, line : " << line;
      is.close();
      return false;
    }
  }
  is.close();
  LOG(INFO) << "Information from dbsync info. master_ip: " << master_ip
    << ", master_port: " << master_port
    << ", filenum: " << filenum
    << ", offset: " << offset;

  // Sanity check
  if (master_ip != g_pika_server->master_ip() ||
      master_port != g_pika_server->master_port()) {
    LOG(WARNING) << "Error master ip port: " << master_ip << ":" << master_port;
    return false;
  }

  // Replace the old db
  slash::StopRsync(g_pika_conf->db_sync_path());
  slash::DeleteFile(info_path);
  if (!g_pika_server->ChangeDb(g_pika_conf->db_sync_path())) {
    LOG(WARNING) << "Failed to change db";
    return false;
  }

  // Update master offset
  g_pika_server->logger_->SetProducerStatus(filenum, offset);

  // If sender is the peer-master
  // need to update receive binlog info after rsync finished.
  if (g_pika_server->DoubleMasterMode() && g_pika_server->IsDoubleMaster(master_ip, master_port)) {
    g_pika_server->logger_->SetDoubleRecvInfo(filenum, offset);
    LOG(INFO) << "Update receive infomation after rsync finished. filenum: " << filenum << " offset: " << offset;
  }
  g_pika_server->WaitDBSyncFinish();
  g_pika_server->SetForceFullSync(false);
  return true;
}

void PikaTrysyncThread::PrepareRsync() {
  std::string db_sync_path = g_pika_conf->db_sync_path();
  slash::StopRsync(db_sync_path);
  slash::CreatePath(db_sync_path + "strings");
  slash::CreatePath(db_sync_path + "hashes");
  slash::CreatePath(db_sync_path + "lists");
  slash::CreatePath(db_sync_path + "sets");
  slash::CreatePath(db_sync_path + "zsets");
}

// TODO maybe use RedisCli
void* PikaTrysyncThread::ThreadMain() {
  while (!should_stop()) {
    sleep(1);
    if (g_pika_server->WaitingDBSync()) {
      //Try to update offset by db sync
      if (TryUpdateMasterOffset()) {
        LOG(INFO) << "Success Update Master Offset";
      }
    }

    if (!g_pika_server->ShouldConnectMaster()) {
      continue;
    }
    sleep(2);
    LOG(INFO) << "Should connect master";
    
    std::string master_ip = g_pika_server->master_ip();
    int master_port = g_pika_server->master_port();
    std::string dbsync_path = g_pika_conf->db_sync_path();

    // Start rsync service
    PrepareRsync();

    if ((cli_->Connect(master_ip, master_port, "")).ok()) {
      LOG(INFO) << "Connect to master ip:" << master_ip << " port: " << master_port;
      cli_->set_send_timeout(30000);
      cli_->set_recv_timeout(30000);
      std::string ip_port = slash::IpPortString(master_ip, master_port);
      struct sockaddr_in laddr;
      socklen_t llen = sizeof(laddr);
      getsockname(cli_->fd(), (struct sockaddr*) &laddr, &llen);
      std::string lip(inet_ntoa(laddr.sin_addr));
      // We append the master ip port after module name
      // To make sure only data from current master is received
      int ret = slash::StartRsync(dbsync_path, kDBSyncModule + "_" + ip_port, lip, g_pika_conf->port() + 3000);
      if (0 != ret) {
        LOG(WARNING) << "Failed to start rsync, path:" << dbsync_path << " error : " << ret;
      }
      LOG(INFO) << "Finish to start rsync, path:" << dbsync_path;

      // Make sure the listening addr of rsyncd is accessible, avoid the corner case
      // that rsync --daemon process is started but not finished listening on the socket
      pink::PinkCli *rsync = pink::NewRedisCli();
      int retry_times;
      for (retry_times = 0; retry_times < 5; retry_times++) {
        if (rsync->Connect(lip, g_pika_conf->port() + 3000, "").ok()) {
          LOG(INFO) << "rsync successfully started, address:" << lip << ":" << g_pika_conf->port() + 3000;
          rsync->Close();
          delete rsync;
          break;
        } else {
          sleep(1);
        }
      }
      if (retry_times >= 5) {
        LOG(WARNING) << "connecting to rsync failed, address:" << lip << ":" << g_pika_conf->port() + 3000;
      }

      if (Send(lip) && RecvProc()) {
        LOG(INFO) << "Open write-binlog mode";
        g_pika_conf->SetWriteBinlog("yes");
        g_pika_server->ConnectMasterDone();
        // Stop rsync, binlog sync with master is begin
        slash::StopRsync(dbsync_path);

        delete g_pika_server->ping_thread_;
        g_pika_server->ping_thread_ = new PikaSlavepingThread(sid_);
        g_pika_server->ping_thread_->StartThread();
        LOG(INFO) << "Trysync success";
      }
      cli_->Close();
    } else {
      LOG(WARNING) << "Failed to connect to master, " << master_ip << ":" << master_port;
    }
  }
  return NULL;
}
