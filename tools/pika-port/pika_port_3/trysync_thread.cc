// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "trysync_thread.h"
#include "conf.h"
#include "const.h"
#include "pika_port.h"

#include <arpa/inet.h>
#include <sys/socket.h>

#include <glog/logging.h>
#include <poll.h>
#include <sys/types.h>
#include <unistd.h>
#include <csignal>
#include <fstream>

#include "pstd/include/rsync.h"
#include "pstd/include/pstd_status.h"
#include "slaveping_thread.h"

#include "storage/storage.h"
#include "pika_define.h"


extern PikaPort* g_pika_port;

TrysyncThread::~TrysyncThread() {
  StopThread();
  pstd::StopRsync(g_conf.dump_path);
  delete cli_;
  LOG(INFO) << " Trysync thread " << pthread_self() << " exit!!!";
}

void TrysyncThread::Stop() {
  retransmit_mutex_.lock();
  if (retransmit_flag_) {
    size_t size = senders_.size();
    for (size_t i = 0; i < size; i++) {
      senders_[i]->Stop();
    }

    size = migrators_.size();
    for (size_t i = 0; i < size; i++) {
      migrators_[i]->Stop();
    }
  }
  retransmit_mutex_.unlock();
  StopThread();
}

void TrysyncThread::PrepareRsync() {
  std::string db_sync_path = g_conf.dump_path;
  pstd::StopRsync(db_sync_path);
  pstd::CreatePath(db_sync_path);

  pstd::CreatePath(db_sync_path + "strings");
  pstd::CreatePath(db_sync_path + "hashes");
  pstd::CreatePath(db_sync_path + "lists");
  pstd::CreatePath(db_sync_path + "sets");
  pstd::CreatePath(db_sync_path + "zsets");
}

bool TrysyncThread::Send(const std::string& lip) {
  net::RedisCmdArgsType argv;
  std::string wbuf_str;
  std::string requirepass = g_pika_port->requirepass();
  if (!requirepass.empty()) {
    argv.push_back("auth");
    argv.push_back(requirepass);
    net::SerializeRedisCommand(argv, &wbuf_str);
  }

  argv.clear();
  std::string tbuf_str;
  argv.push_back("trysync");
  argv.push_back(lip);
  argv.push_back(std::to_string(g_conf.local_port));
  uint32_t filenum;
  uint64_t pro_offset;
  g_pika_port->logger()->GetProducerStatus(&filenum, &pro_offset);
  LOG(INFO) << "producer filenum: " << filenum << ", producer offset: " << pro_offset;

  argv.push_back(std::to_string(filenum));
  argv.push_back(std::to_string(pro_offset));
  net::SerializeRedisCommand(argv, &tbuf_str);

  wbuf_str.append(tbuf_str);
  LOG(INFO) << "redis command: trysync " << g_conf.local_ip.c_str() << " " << g_conf.local_port << " " << filenum << " "
            << pro_offset;

  pstd::Status s;
  s = cli_->Send(&wbuf_str);
  if (!s.ok()) {
    LOG(WARNING) << "Connect master, Send: " << wbuf_str << ", status: " << s.ToString();
    return false;
  }

  return true;
}

// if send command {trysync slaveip slaveport 0 0}, the reply = wait.
// if send command {trysync slaveip slaveport 11 38709514}, the reply = "sid:.
// it means that slave sid is allocated by master.
bool TrysyncThread::RecvProc() {
  bool should_auth = !g_pika_port->requirepass().empty();
  bool is_authed = false;
  pstd::Status s;
  std::string reply;

  net::RedisCmdArgsType argv;
  while (true) {
    s = cli_->Recv(&argv);
    if (!s.ok()) {
      LOG(WARNING) << "Connect master, status: " << s.ToString() << ", Recv error";
      return false;
    }

    reply = argv[0];
    LOG(INFO) << "Reply from master after trysync: " << reply.c_str();
    if (!is_authed && should_auth) {
      if (kInnerReplOk != pstd::StringToLower(reply)) {
        g_pika_port->RemoveMaster();
        return false;
      }
      is_authed = true;
    } else {
      if (argv.size() == 1 && (pstd::string2int(reply.data(), reply.size(), &sid_) != 0)) {
        // Luckily, I got your point, the sync is comming
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
        if (g_conf.exit_if_dbsync) {
          LOG(WARNING) << "Exit, due to -e option configured";
          exit(-1);
        }
        LOG(INFO) << "Need to wait for pika master sync";
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
  if (!pstd::FileExists(info_path)) {
    return false;
  }

  // Got new binlog offset
  std::ifstream is(info_path);
  if (!is) {
    LOG(WARNING) << "Failed to open info file after db sync";
    return false;
  }
  std::string line;
  std::string master_ip;
  int lineno = 0;
  int64_t filenum = 0;
  int64_t offset = 0;
  int64_t tmp = 0;
  int64_t master_port = 0;
  while (std::getline(is, line)) {
    lineno++;
    if (lineno == 2) {
      master_ip = line;
    } else if (lineno > 2 && lineno < 6) {
      if ((pstd::string2int(line.data(), line.size(), &tmp) == 0) || tmp < 0) {
        LOG(WARNING) << "Format of info file after db sync error, line: " << line;
        is.close();
        return false;
      }
      if (lineno == 3) {
        master_port = tmp;
      } else if (lineno == 4) {
        filenum = tmp;
      } else {
        offset = tmp;
      }
    } else if (lineno > 5) {
      LOG(WARNING) << "Format of info file after db sync error, line: " << line;
      is.close();
      return false;
    }
  }
  is.close();
  LOG(INFO) << "Information from dbsync info. master_ip: " << master_ip << ", master_port: " << master_port
            << ", filenum: " << filenum << ", offset: " << offset;

  // Sanity check
  if (  // master_ip != g_conf.master_ip ||
      master_port != g_conf.master_port) {
    LOG(WARNING) << "Error master{" << master_ip << ":" << master_port << "} != g_config.master{"
                 << "{" << g_conf.master_ip.c_str() << ":" << g_conf.master_port << "}";
    return false;
  }

  // Replace the old db
  pstd::StopRsync(db_sync_path);
  pstd::DeleteFile(info_path);

  // Update master offset
  g_pika_port->logger()->SetProducerStatus(filenum, offset);
  Retransmit();
  g_pika_port->WaitDBSyncFinish();

  return true;
}

#include <iostream>
#include <sstream>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

#include "src/redis_hashes.h"
#include "src/redis_lists.h"
#include "src/redis_sets.h"
#include "src/redis_strings.h"
#include "src/redis_zsets.h"

using std::chrono::high_resolution_clock;
using std::chrono::milliseconds;

int TrysyncThread::Retransmit() {
  std::string db_path = g_conf.dump_path;
  std::string ip = g_conf.forward_ip;
  int port = g_conf.forward_port;
  size_t thread_num = g_conf.forward_thread_num;
  std::string password = g_conf.forward_passwd;

  rocksdb::Status s;

  high_resolution_clock::time_point start = high_resolution_clock::now();
  if (db_path[db_path.length() - 1] != '/') {
    db_path.append("/");
  }

  // Init SenderThread
  for (size_t i = 0; i < thread_num; i++) {
    senders_.emplace_back(new PikaSender(ip, port, password));
  }

  // Init db
  rocksdb::Options options;
  options.create_if_missing = true;
  options.keep_log_file_num = 10;
  options.max_manifest_file_size = 64 * 1024 * 1024;
  options.max_log_file_size = 512 * 1024 * 1024;
  options.write_buffer_size = 512 * 1024 * 1024;     // 512M
  options.target_file_size_base = 40 * 1024 * 1024;  // 40M

  storage::StorageOptions bwOptions;
  bwOptions.options = options;

  storage::Storage bw;

  storage::RedisStrings stringsDB(&bw, storage::kStrings);
  std::string path = db_path + "strings";
  s = stringsDB.Open(bwOptions, path);
  LOG(INFO) << "Open strings DB " << path << " result " << s.ToString();
  if (s.ok()) {
    migrators_.emplace_back(new MigratorThread(&stringsDB, &senders_, storage::kStrings, thread_num));
  }

  storage::RedisLists listsDB(&bw, storage::kLists);
  path = db_path + "lists";
  s = listsDB.Open(bwOptions, path);
  LOG(INFO) << "Open lists DB " << path << " result " << s.ToString();
  if (s.ok()) {
    migrators_.emplace_back(new MigratorThread(&listsDB, &senders_, storage::kLists, thread_num));
  }

  storage::RedisHashes hashesDB(&bw, storage::kHashes);
  path = db_path + "hashes";
  s = hashesDB.Open(bwOptions, path);
  LOG(INFO) << "Open hashes DB " << path << " result " << s.ToString();
  if (s.ok()) {
    migrators_.emplace_back(new MigratorThread(&hashesDB, &senders_, storage::kHashes, thread_num));
  }

  storage::RedisSets setsDB(&bw, storage::kSets);
  path = db_path + "sets";
  s = setsDB.Open(bwOptions, path);
  LOG(INFO) << "Open sets DB " << path << " result " << s.ToString();
  if (s.ok()) {
    migrators_.emplace_back(new MigratorThread(&setsDB, &senders_, storage::kSets, thread_num));
  }

  storage::RedisZSets zsetsDB(&bw, storage::kZSets);
  path = db_path + "zsets";
  s = zsetsDB.Open(bwOptions, path);
  LOG(INFO) << "Open zsets DB " << path << " result " << s.ToString();
  if (s.ok()) {
    migrators_.emplace_back(new MigratorThread(&zsetsDB, &senders_, storage::kZSets, thread_num));
  }

  retransmit_mutex_.lock();
  retransmit_flag_ = true;
  retransmit_mutex_.unlock();

  // start threads
  size_t size = senders_.size();
  for (size_t i = 0; i < size; i++) {
    senders_[i]->StartThread();
  }

  size = migrators_.size();
  for (size_t i = 0; i < size; i++) {
    migrators_[i]->StartThread();
  }
  size = migrators_.size();
  for (size_t i = 0; i < size; i++) {
    migrators_[i]->JoinThread();
  }

  size = senders_.size();
  for (size_t i = 0; i < size; i++) {
    senders_[i]->Stop();
  }
  size = senders_.size();
  for (size_t i = 0; i < size; i++) {
    senders_[i]->JoinThread();
  }

  retransmit_mutex_.lock();
  retransmit_flag_ = false;
  retransmit_mutex_.unlock();

  int64_t replies = 0;
  int64_t records = 0;
  size = migrators_.size();
  for (size_t i = 0; i < size; i++) {
    records += migrators_[i]->num();
    delete migrators_[i];
  }
  size = senders_.size();
  for (size_t i = 0; i < thread_num; i++) {
    replies += senders_[i]->elements();
    delete senders_[i];
  }

  high_resolution_clock::time_point end = high_resolution_clock::now();
  std::chrono::hours hours = std::chrono::duration_cast<std::chrono::hours>(end - start);
  std::chrono::minutes minutes = std::chrono::duration_cast<std::chrono::minutes>(end - start);
  std::chrono::seconds seconds = std::chrono::duration_cast<std::chrono::seconds>(end - start);

  LOG(INFO) << "=============== Retransmitting =====================";
  LOG(INFO) << "Running time  :";
  LOG(INFO) << hours.count() << " hour " << minutes.count() - hours.count() * 60 << " min "
            << seconds.count() - hours.count() * 60 * 60 << " s";
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
      static time_t wait_start = 0;
      time_t cur_time = time(nullptr);
      if (wait_start == 0) {
        wait_start = cur_time;
      }
      // Try to update offset by db sync
      if (TryUpdateMasterOffset()) {
        LOG(INFO) << "Success Update Master Offset";
      } else {
        time_t diff = cur_time - wait_start;
        if (g_conf.wait_bgsave_timeout < diff) {
          LOG(WARNING) << "Pika-port has waited about " << diff << " seconds for pika master bgsave data";
        }
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
    Status connectStatus = cli_->Connect(master_ip, master_port, g_conf.local_ip);
    if (!connectStatus.ok()) {
      LOG(WARNING) << "Failed to connect to master " << master_ip << ":" << master_port
                   << ", status: " << connectStatus.ToString();
      continue;
    }
    LOG(INFO) << "Connect to master " << master_ip << ":" << master_port;
    cli_->set_send_timeout(30000);
    cli_->set_recv_timeout(30000);

    std::string lip(g_conf.local_ip);
    // Bug Fix by AS on 20190414  22:22 pm:
    // the pika master module name rule is: document_${slave_ip}:master_port
    //
    // document_${slave_ip}:master_port
    // std::string ip_port = pstd::IpPortString(lip, master_port);

    // pika 3.0.0-3.0.15 uses document_${master_ip}:${master_port}
    // document_${master_ip}:${master_port}
    std::string ip_port = pstd::IpPortString(master_ip, master_port);
    // We append the master ip port after module name
    // To make sure only data from current master is received
    int rsync_port = g_conf.local_port + 3000;
    auto s = kDBSyncModule;
    s.append("_" + ip_port);
    int ret = pstd::StartRsync(dbsync_path, s, lip, rsync_port, g_conf.passwd);
    if (0 != ret) {
      LOG(WARNING) << "Failed to start rsync, path: " << dbsync_path << ", error: " << ret;
    }
    LOG(INFO) << "Finish to start rsync, path: " << dbsync_path << ", local address: " << lip << ":" << rsync_port;

    // Make sure the listening addr of rsyncd is accessible, to avoid the corner case
    // that "rsync --daemon" process has started but can not bind its port which is
    // used by other process.
    net::NetCli* rsync = net::NewRedisCli();
    int retry_times;
    for (retry_times = 0; retry_times < 5; retry_times++) {
      if (rsync->Connect(lip, rsync_port, "").ok()) {
        LOG(INFO) << "rsync successfully started, address: " << lip << ":" << rsync_port;
        rsync->Close();
        break;
      }

      sleep(1);
    }
    if (retry_times >= 5) {
      LOG(FATAL) << "connecting to rsync failed, address " << lip << ":" << rsync_port;
    }

    if (Send(lip) && RecvProc()) {
      g_pika_port->ConnectMasterDone();
      // Stop rsync, binlog sync with master is begin
      pstd::StopRsync(dbsync_path);

      delete g_pika_port->ping_thread_;
      g_pika_port->ping_thread_ = new SlavepingThread(sid_);
      g_pika_port->ping_thread_->StartThread();
      LOG(INFO) << "Trysync success";
    }
    cli_->Close();
  }

  return nullptr;
}
