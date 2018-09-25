// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "trysync_thread.h"
#include "pika_port.h"
#include "conf.h"
#include "const.h"

#include <arpa/inet.h>
#include <sys/socket.h>

#include <fstream>
#include <glog/logging.h>
#include <poll.h>
#include <sys/types.h>
#include <unistd.h>
#include <signal.h>

#include "slash/include/slash_status.h"
#include "slash/include/rsync.h"
#include "slaveping_thread.h"

#include "include/pika_define.h"
#include "blackwidow/blackwidow.h"

extern Conf g_conf;
extern PikaPort* g_pika_port;

TrysyncThread::~TrysyncThread() {
  StopThread();
  slash::StopRsync(g_conf.dump_path);
  delete cli_;
  LOG(INFO) << " Trysync thread " << pthread_self() << " exit!!!";
}

void TrysyncThread::PrepareRsync() {
  std::string db_sync_path = g_conf.dump_path;
  slash::StopRsync(db_sync_path);
  slash::CreatePath(db_sync_path);

  slash::CreatePath(db_sync_path + "strings");
  slash::CreatePath(db_sync_path + "hashes");
  slash::CreatePath(db_sync_path + "lists");
  slash::CreatePath(db_sync_path + "sets");
  slash::CreatePath(db_sync_path + "zsets");
}

bool TrysyncThread::Send(std::string lip) {
  pink::RedisCmdArgsType argv;
  std::string wbuf_str;
  std::string requirepass = g_pika_port->requirepass();
  if (requirepass != "") {
    argv.push_back("auth");
    argv.push_back(requirepass);
    pink::SerializeRedisCommand(argv, &wbuf_str);
  }

  argv.clear();
  std::string tbuf_str;
  argv.push_back("trysync");
  argv.push_back(lip);
  argv.push_back(std::to_string(g_conf.local_port));
  uint32_t filenum;
  uint64_t pro_offset;
  g_pika_port->logger()->GetProducerStatus(&filenum, &pro_offset);
  LOG(INFO) << "producer filenum: " << filenum << ", producer offset:" << pro_offset;

  argv.push_back(std::to_string(filenum));
  argv.push_back(std::to_string(pro_offset));
  pink::SerializeRedisCommand(argv, &tbuf_str);

  wbuf_str.append(tbuf_str);
  LOG(INFO) << "redis command: trysync " << g_conf.local_ip << " "
             << g_conf.local_port << " " << filenum << " " << pro_offset;

  slash::Status s;
  s = cli_->Send(&wbuf_str);
  if (!s.ok()) {
    LOG(WARNING) << "Connect master, Send " << wbuf_str << ", error: " << strerror(errno);
    return false;
  }

  return true;
}

// if send command {trysync slaveip slaveport 0 0}, the reply = wait.
// if send command {trysync slaveip slaveport 11 38709514}, the reply = "sid:.
// it means that slave sid is allocated by master.
bool TrysyncThread::RecvProc() {
  bool should_auth = g_pika_port->requirepass() == "" ? false : true;
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
    LOG(INFO) << "Reply from master after trysync: " << reply;
    if (!is_authed && should_auth) {
      if (kInnerReplOk != slash::StringToLower(reply)) {
        g_pika_port->RemoveMaster();
        return false;
      }
      is_authed = true;
    } else {
      if (argv.size() == 1 &&
          slash::string2l(reply.data(), reply.size(), &sid_)) {
        // Luckly, I got your point, the sync is comming
        LOG(INFO) << "Recv sid from master: " << sid_;
        g_pika_port->SetSid(sid_);
        break;
      }

      // Failed
      if (reply == kInnerReplWait) {
        // You can't sync this time, but may be different next time,
        // This may happened when
        // 1, Master do bgsave first.
        // 2, Master waiting for an existing bgsaving process
        // 3, Master do dbsyncing
        LOG(INFO) << "Need wait to sync";
        g_pika_port->NeedWaitDBSync();
        // break;
      } else {
        LOG(INFO) << "Sync Error, Quit";
        kill(getpid(), SIGQUIT);
        g_pika_port->RemoveMaster();
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
bool TrysyncThread::TryUpdateMasterOffset() {
  // Check dbsync finished
  std::string db_sync_path = g_conf.dump_path;
  std::string info_path = db_sync_path + kBgsaveInfoFile;
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
  if (master_ip != g_conf.master_ip ||
      master_port != g_conf.master_port) {
    LOG(WARNING) << "Error master ip port: " << master_ip << ":" << master_port;
    return false;
  }

  // Replace the old db
  slash::StopRsync(db_sync_path);
  slash::DeleteFile(info_path);

  // Update master offset
  g_pika_port->logger()->SetProducerStatus(filenum, offset);
  Retransmit();
  g_pika_port->WaitDBSyncFinish();

  return true;
}

#include "pika_sender.h"
#include "migrator_thread.h"

#include <iostream>
#include <sstream>
#include <vector>
#include <memory>

#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "rocksdb/slice.h"
#include "src/redis_strings.h"

using std::chrono::high_resolution_clock;
using std::chrono::milliseconds;

int TrysyncThread::Retransmit() {
  std::string db_path = g_conf.dump_path;
  std::string ip = g_conf.forward_ip;
  int port = g_conf.forward_port;
  size_t thread_num = g_conf.forward_thread_num;
  std::string password = g_conf.forward_passwd;
  
  std::vector<PikaSender*> senders;
  std::vector<std::unique_ptr<MigratorThread>> migrators;
  rocksdb::Status s;

  high_resolution_clock::time_point start = high_resolution_clock::now();
  if (db_path[db_path.length() - 1] != '/') {
    db_path.append("/");
  }

  // Init db
  rocksdb::Options options;
  options.create_if_missing = true;
  options.keep_log_file_num = 10;
  options.max_manifest_file_size = 64 * 1024 * 1024;
  options.max_log_file_size = 512 * 1024 * 1024;
  options.write_buffer_size = 512 * 1024 * 1024; // 512M
  options.target_file_size_base = 40 * 1024 * 1024; // 40M
  blackwidow::RedisStrings stringsDB;
  s = stringsDB.Open(options, db_path + "strings");
  assert(s.ok());
  LOG(INFO) << "Open DB " << db_path << " Success";

  // Init SenderThread
  for (size_t i = 0; i < thread_num; i++) {
    senders.emplace_back(new PikaSender((void*)(&stringsDB), ip, port, password));
  }

  migrators.emplace_back(new MigratorThread((void*)(&stringsDB), &senders, blackwidow::kStrings, thread_num));
  // migrators.emplace_back(new MigratorThread(db.get(), &senders, blackwidow::kHashes, thread_num));
  // migrators.emplace_back(new MigratorThread(db.get(), &senders, blackwidow::kSets, thread_num));
  // migrators.emplace_back(new MigratorThread(db.get(), &senders, blackwidow::kLists, thread_num));
  // migrators.emplace_back(new MigratorThread(db.get(), &senders, blackwidow::kZSets, thread_num));

  // start threads
  // for (size_t i = 0; i < kDataSetNum; i++) {
  for (size_t i = 0; i < 1; i++) {
    migrators[i]->StartThread();
  }
  for (size_t i = 0; i < thread_num; i++) {
    senders[i]->StartThread();
  }

  // for(size_t i = 0; i < kDataSetNum; i++) {
  for(size_t i = 0; i < 1; i++) {
    migrators[i]->JoinThread();
  }
  for(size_t i = 0; i < thread_num; i++) {
    senders[i]->Stop();
  }
  for (size_t i = 0; i < thread_num; i++) {
    senders[i]->JoinThread();
  }

  int64_t replies = 0, records = 0;
  // for (size_t i = 0; i < kDataSetNum; i++) {
  //   records += migrators[i]->num();
  //   delete migrators[i];
  // }
  for (size_t i = 0; i < thread_num; i++) {
    replies += senders[i]->elements();
    delete senders[i];
  }

  high_resolution_clock::time_point end = high_resolution_clock::now();
  std::chrono::hours  hours = std::chrono::duration_cast<std::chrono::hours>(end - start);
  std::chrono::minutes  minutes = std::chrono::duration_cast<std::chrono::minutes>(end - start);
  std::chrono::seconds  seconds = std::chrono::duration_cast<std::chrono::seconds>(end - start);

  LOG(INFO) << "=============== Retransmitting =====================" << std::endl;
  LOG(INFO) << "Running time  :";
  LOG(INFO) << hours.count() << " hours " << minutes.count() - hours.count() * 60 << " min " << seconds.count() - hours.count() * 60 * 60 << " seconds";
  LOG(INFO) << "Total records : " << records << " have been Scaned";
  LOG(INFO) << "Total replies : " << replies << " received from redis server";
  // delete db

  return 0;
}

void* TrysyncThread::ThreadMain() {
  while (!should_stop()) {
    sleep(1);

    if (g_pika_port->IsWaitingDBSync()) {
      LOG(INFO) << "Waiting db sync";
      //Try to update offset by db sync
      if (TryUpdateMasterOffset()) {
        LOG(INFO) << "Success Update Master Offset";
      }
    }

    if (!g_pika_port->ShouldConnectMaster()) {
      continue;
    }
    sleep(2);
    LOG(INFO) << "Should connect master";
    
    std::string master_ip = g_conf.master_ip;
    int master_port = g_conf.master_port;
    std::string dbsync_path = g_conf.dump_path;

    // Start rsync service
    PrepareRsync();
    std::string ip_port = slash::IpPortString(g_conf.master_ip, g_conf.master_port);
    int ret = slash::StartRsync(dbsync_path, kDBSyncModule + "_" + ip_port,
	                g_conf.local_ip, g_conf.local_port + 3000);
    if (0 != ret) {
      LOG(WARNING) << "Failed to start rsync, path:" << dbsync_path << " error : " << ret;
      return false;
    }
    LOG(INFO) << "Finish to start rsync, path:" << dbsync_path;

    if ((cli_->Connect(master_ip, master_port, g_conf.local_ip)).ok()) {
      LOG(INFO) << "Connect to master{ip:" << master_ip << ", port: " << master_port << "}";
      cli_->set_send_timeout(30000);
      cli_->set_recv_timeout(30000);

      std::string ip_port = slash::IpPortString(master_ip, master_port);
      struct sockaddr_in laddr = {0};
      socklen_t llen = sizeof(laddr);
      ::getsockname(cli_->fd(), (struct sockaddr*) &laddr, &llen);
      std::string lip(::inet_ntoa(laddr.sin_addr));
      // We append the master ip port after module name
      // To make sure only data from current master is received
      int ret = slash::StartRsync(dbsync_path, kDBSyncModule + "_" + ip_port, lip, g_conf.local_port + 3000);
      if (0 != ret) {
        LOG(WARNING) << "Failed to start rsync, path:" << dbsync_path << " error : " << ret;
      }
      LOG(INFO) << "Finish to start rsync, path:" << dbsync_path;

      // Make sure the listenning addr of rsyncd is accessible, to avoid the corner case
      // that "rsync --daemon" process has started but can not bind its port which is
	  // used by other process.
	  int rsync_port = g_conf.local_port + 3000;
      pink::PinkCli *rsync = pink::NewRedisCli();
      int retry_times;
      for (retry_times = 0; retry_times < 5; retry_times++) {
        if (rsync->Connect(lip, rsync_port, "").ok()) {
          LOG(INFO) << "rsync successfully started, address:" << lip << ":" << rsync_port;
          rsync->Close();
          break;
        }

        sleep(1);
      }
      if (retry_times >= 5) {
        LOG(WARNING) << "connecting to rsync failed, address:" << lip << ":" << rsync_port;
      }

      if (Send(lip) && RecvProc()) {
        g_pika_port->ConnectMasterDone();
        // Stop rsync, binlog sync with master is begin
        slash::StopRsync(dbsync_path);
        
        delete g_pika_port->ping_thread_;
        g_pika_port->ping_thread_ = new SlavepingThread(sid_);
        g_pika_port->ping_thread_->StartThread();
        LOG(INFO) << "Trysync success";
      }
      cli_->Close();
    } else {
      LOG(WARNING) << "Failed to connect to master, " << master_ip << ":" << master_port;
    }
  }

  return NULL;
}
