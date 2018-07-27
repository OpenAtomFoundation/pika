// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <fstream>
#include <glog/logging.h>
#include <assert.h>
#include <sys/types.h>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <string.h>
#include <arpa/inet.h>
#include <sstream>
#include <iostream>
#include <iterator>
#include <ctime>
#include <algorithm>
#include <sys/resource.h>

#include "slash/include/env.h"
#include "slash/include/rsync.h"
#include "slash/include/slash_string.h"
#include "pink/include/bg_thread.h"
#include "include/pika_server.h"
#include "include/pika_conf.h"
#include "include/pika_dispatch_thread.h"

extern PikaConf *g_pika_conf;

PikaServer::PikaServer() :
  ping_thread_(NULL),
  exit_(false),
  binlog_io_error_(false),
  have_scheduled_crontask_(false),
  last_check_compact_time_({0, 0}),
  sid_(0),
  master_ip_(""),
  master_connection_(0),
  master_port_(0),
  repl_state_(PIKA_REPL_NO_CONNECT),
  role_(PIKA_ROLE_SINGLE),
  force_full_sync_(false),
  double_master_sid_(0),
  double_master_mode_(false),
  bgsave_engine_(NULL),
  purging_(false),
  binlogbg_exit_(false),
  binlogbg_cond_(&binlogbg_mutex_),
  binlogbg_serial_(0) {

  pthread_rwlockattr_t attr;
  pthread_rwlockattr_init(&attr);
  pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
  pthread_rwlock_init(&rwlock_, &attr);

  //Init server ip host
  if (!ServerInit()) {
    LOG(FATAL) << "ServerInit iotcl error";
  }

  //Create blackwidow handle
  rocksdb::Options rocksdb_option;
  RocksdbOptionInit(&rocksdb_option);

  std::string db_path = g_pika_conf->db_path();
  LOG(INFO) << "Prepare Blackwidow DB...";
  db_ = std::shared_ptr<blackwidow::BlackWidow>(new blackwidow::BlackWidow());
  rocksdb::Status s = db_->Open(rocksdb_option, db_path);
  assert(db_);
  assert(s.ok());
  LOG(INFO) << "DB Success";


  // Create thread
  worker_num_ = std::min(g_pika_conf->thread_num(),
                         PIKA_MAX_WORKER_THREAD_NUM);

  std::set<std::string> ips;
  if (g_pika_conf->network_interface().empty()) {
    ips.insert("0.0.0.0");
  } else {
    ips.insert("127.0.0.1");
    ips.insert(host_);
  }
  // We estimate the queue size
  int worker_queue_limit = g_pika_conf->maxclients() / worker_num_ + 100;
  LOG(INFO) << "Worker queue limit is " << worker_queue_limit;
  pika_dispatch_thread_ = new PikaDispatchThread(ips, port_, worker_num_, 3000,
                                                 worker_queue_limit);
  pika_binlog_receiver_thread_ = new PikaBinlogReceiverThread(ips, port_ + 1000, 1000);
  pika_heartbeat_thread_ = new PikaHeartbeatThread(ips, port_ + 2000, 1000);
  pika_trysync_thread_ = new PikaTrysyncThread();
  monitor_thread_ = new PikaMonitorThread();
  pika_pubsub_thread_ = new pink::PubSubThread();

  //for (int j = 0; j < g_pika_conf->binlogbg_thread_num; j++) {
  for (int j = 0; j < g_pika_conf->sync_thread_num(); j++) {
    binlogbg_workers_.push_back(new BinlogBGWorker(g_pika_conf->sync_buffer_size()));
  }

  pthread_rwlock_init(&state_protector_, NULL);
  logger_ = new Binlog(g_pika_conf->log_path(), g_pika_conf->binlog_file_size());

  uint64_t double_recv_offset;
  uint32_t double_recv_num;
  logger_->GetDoubleRecvInfo(&double_recv_num, &double_recv_offset);
  LOG(INFO) << "double recv info: filenum " << double_recv_num << " offset " << double_recv_offset;
}

PikaServer::~PikaServer() {
  delete bgsave_engine_;

  // DispatchThread will use queue of worker thread,
  // so we need to delete dispatch before worker.
  delete pika_dispatch_thread_;

  {
  slash::MutexLock l(&slave_mutex_);
  std::vector<SlaveItem>::iterator iter = slaves_.begin();
  while (iter != slaves_.end()) {
    if (iter->sender != NULL) {
      delete static_cast<PikaBinlogSenderThread*>(iter->sender);
    }
    iter =  slaves_.erase(iter);
    LOG(INFO) << "Delete slave success";
  }
  }

  delete pika_trysync_thread_;
  {
    slash::MutexLock l(&slaveping_protector_);
    delete ping_thread_;
    ping_thread_ = NULL;
  }
  delete pika_binlog_receiver_thread_;
  delete pika_pubsub_thread_;

  binlogbg_exit_ = true;
  std::vector<BinlogBGWorker*>::iterator binlogbg_iter = binlogbg_workers_.begin();
  while (binlogbg_iter != binlogbg_workers_.end()) {
    binlogbg_cond_.SignalAll();
    delete (*binlogbg_iter);
    binlogbg_iter++;
  }
  delete pika_heartbeat_thread_;
  delete monitor_thread_;

  StopKeyScan();
  key_scan_thread_.StopThread();

  delete logger_;
  db_.reset();
  pthread_rwlock_destroy(&state_protector_);
  pthread_rwlock_destroy(&rwlock_);

  LOG(INFO) << "PikaServer " << pthread_self() << " exit!!!";
}

bool PikaServer::ServerInit() {
  std::string network_interface = g_pika_conf->network_interface();

  if (network_interface == "") {

    std::ifstream routeFile("/proc/net/route", std::ios_base::in);
    if (!routeFile.good())
    {
      return false;
    }

    std::string line;
    std::vector<std::string> tokens;
    while(std::getline(routeFile, line))
    {
      std::istringstream stream(line);
      std::copy(std::istream_iterator<std::string>(stream),
          std::istream_iterator<std::string>(),
          std::back_inserter<std::vector<std::string> >(tokens));

      // the default interface is the one having the second
      // field, Destination, set to "00000000"
      if ((tokens.size() >= 2) && (tokens[1] == std::string("00000000")))
      {
        network_interface = tokens[0];
        break;
      }

      tokens.clear();
    }
    routeFile.close();
  }
  LOG(INFO) << "Using Networker Interface: " << network_interface;

  struct ifaddrs * ifAddrStruct = NULL;
  struct ifaddrs * ifa = NULL;
  void * tmpAddrPtr = NULL;

  if (getifaddrs(&ifAddrStruct) == -1) {
    LOG(FATAL) << "getifaddrs failed: " << strerror(errno);
  }

  for (ifa = ifAddrStruct; ifa != NULL; ifa = ifa->ifa_next) {
    if (ifa->ifa_addr == NULL) {
      continue;
    }
    if (ifa ->ifa_addr->sa_family==AF_INET) { // Check it is
      // a valid IPv4 address
      tmpAddrPtr = &((struct sockaddr_in *)ifa->ifa_addr)->sin_addr;
      char addressBuffer[INET_ADDRSTRLEN];
      inet_ntop(AF_INET, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN);
      if (std::string(ifa->ifa_name) == network_interface) {
        host_ = addressBuffer;
        break;
      }
    } else if (ifa->ifa_addr->sa_family==AF_INET6) { // Check it is
      // a valid IPv6 address
      tmpAddrPtr = &((struct sockaddr_in6 *)ifa->ifa_addr)->sin6_addr;
      char addressBuffer[INET6_ADDRSTRLEN];
      inet_ntop(AF_INET6, tmpAddrPtr, addressBuffer, INET6_ADDRSTRLEN);
      if (std::string(ifa->ifa_name) == network_interface) {
        host_ = addressBuffer;
        break;
      }
    }
  }

  if (ifAddrStruct != NULL) {
    freeifaddrs(ifAddrStruct);
  }
  if (ifa == NULL) {
    LOG(FATAL) << "error network interface: " << network_interface << ", please check!";
  }

  port_ = g_pika_conf->port();
  LOG(INFO) << "host: " << host_ << " port: " << port_;
  return true;

}

void PikaServer::RocksdbOptionInit(rocksdb::Options* option) {
  option->create_if_missing = true;
  option->keep_log_file_num = 10;
  option->max_manifest_file_size = 64 * 1024 * 1024;
  option->max_log_file_size = 512 * 1024 * 1024;

  option->write_buffer_size = g_pika_conf->write_buffer_size();
  option->target_file_size_base = g_pika_conf->target_file_size_base();
  option->max_background_flushes = g_pika_conf->max_background_flushes();
  option->max_background_compactions = g_pika_conf->max_background_compactions();
  option->max_open_files = g_pika_conf->max_cache_files();
  option->max_bytes_for_level_multiplier = g_pika_conf->max_bytes_for_level_multiplier();

  if (g_pika_conf->compression() == "none") {
    option->compression = rocksdb::CompressionType::kNoCompression;
  } else if (g_pika_conf->compression() == "snappy") {
    option->compression = rocksdb::CompressionType::kSnappyCompression;
  } else if (g_pika_conf->compression() == "zlib") {
    option->compression = rocksdb::CompressionType::kZlibCompression;
  }
}

void PikaServer::Start() {
  int ret = 0;
  ret = pika_dispatch_thread_->StartThread();
  if (ret != pink::kSuccess) {
    delete logger_;
    db_.reset();
    LOG(FATAL) << "Start Dispatch Error: " << ret << (ret == pink::kBindError ? ": bind port " + std::to_string(port_) + " conflict"
            : ": other error") << ", Listen on this port to handle the connected redis client";
  }
  ret = pika_binlog_receiver_thread_->StartThread();
  if (ret != pink::kSuccess) {
    delete logger_;
    db_.reset();
    LOG(FATAL) << "Start BinlogReceiver Error: " << ret << (ret == pink::kBindError ? ": bind port " + std::to_string(port_ + 1000) + " conflict"
            : ": other error") << ", Listen on this port to handle the data sent by the Binlog Sender";
  }
  ret = pika_heartbeat_thread_->StartThread();
  if (ret != pink::kSuccess) {
    delete logger_;
    db_.reset();
    LOG(FATAL) << "Start Heartbeat Error: " << ret << (ret == pink::kBindError ? ": bind port " + std::to_string(port_ + 2000) + " conflict"
            : ": other error") << ", Listen on this port to receive the heartbeat packets sent by the master";
  }
  ret = pika_trysync_thread_->StartThread();
  if (ret != pink::kSuccess) {
    delete logger_;
    db_.reset();
    LOG(FATAL) << "Start Trysync Error: " << ret << (ret == pink::kBindError ? ": bind port conflict" : ": other error");
  }
  ret = pika_pubsub_thread_->StartThread();
  if (ret != pink::kSuccess) {
    delete logger_;
    db_.reset();
    LOG(FATAL) << "Start Pubsub Error: " << ret << (ret == pink::kBindError ? ": bind port conflict" : ": other error");
  }


  time(&start_time_s_);

  std::string slaveof = g_pika_conf->slaveof();
  if (!slaveof.empty()) {
    int32_t sep = slaveof.find(":");
    std::string master_ip = slaveof.substr(0, sep);
    int32_t master_port = std::stoi(slaveof.substr(sep+1));
    if ((master_ip == "127.0.0.1" || master_ip == host_) && master_port == port_) {
      LOG(FATAL) << "you will slaveof yourself as the config file, please check";
    } else {
      SetMaster(master_ip, master_port);
    }
  }


  // Double master mode
  if (!g_pika_conf->double_master_ip().empty()) {
    std::string double_master_ip = g_pika_conf->double_master_ip();
    int32_t double_master_port = g_pika_conf->double_master_port();
    double_master_sid_ = std::stoi(g_pika_conf->double_master_sid());
    if ((double_master_ip == "127.0.0.1" || double_master_ip == host_) && double_master_port == port_) {
    LOG(FATAL) << "set yourself as the peer-master, please check";
    } else {
      double_master_mode_ = true;
      SetMaster(double_master_ip, double_master_port);
    }
  }

  LOG(INFO) << "Pika Server going to start";
  while (!exit_) {
    DoTimingTask();
    // wake up every 10 second
    int try_num = 0;
    while (!exit_ && try_num++ < 10) {
      sleep(1);
    }
  }
  LOG(INFO) << "Goodbye...";
}

void PikaServer::DeleteSlave(const std::string& ip, int64_t port) {
  std::string ip_port = slash::IpPortString(ip, port);
  int slave_num = 0;
  {
  slash::MutexLock l(&slave_mutex_);
  std::vector<SlaveItem>::iterator iter = slaves_.begin();
  while (iter != slaves_.end()) {
    if (iter->ip_port == ip_port) {
      break;
    }
    iter++;
  }
  if (iter == slaves_.end()) {
    return;
  }
  if (iter->sender != NULL) {
    delete static_cast<PikaBinlogSenderThread*>(iter->sender);
  }
  slaves_.erase(iter);
  slave_num = slaves_.size();
  }

  slash::RWLock l(&state_protector_, true);
  if (slave_num == 0) {
    role_ &= ~PIKA_ROLE_MASTER;
    if (DoubleMasterMode()) {
      role_ |= PIKA_ROLE_DOUBLE_MASTER;
    }
  }
}

void PikaServer::DeleteSlave(int fd) {
  int slave_num = 0;
  {
  slash::MutexLock l(&slave_mutex_);
  std::vector<SlaveItem>::iterator iter = slaves_.begin();

  while (iter != slaves_.end()) {
    if (iter->hb_fd == fd) {
      if (iter->sender != NULL) {
        delete static_cast<PikaBinlogSenderThread*>(iter->sender);
      }
      slaves_.erase(iter);
      LOG(INFO) << "Delete slave success";
      break;
    }
    iter++;
  }
  slave_num = slaves_.size();
  }
  slash::RWLock l(&state_protector_, true);
  if (slave_num == 0) {
    role_ &= ~PIKA_ROLE_MASTER;
    if (DoubleMasterMode()) {
      role_ |= PIKA_ROLE_DOUBLE_MASTER; 
    }
  }
}

/*
 * Change a new db locate in new_path
 * return true when change success
 * db remain the old one if return false
 */
bool PikaServer::ChangeDb(const std::string& new_path) {

  rocksdb::Options option;
  option.create_if_missing = true;
  option.keep_log_file_num = 10;
  option.max_manifest_file_size = 64 * 1024 * 1024;
  option.max_log_file_size = 512 * 1024 * 1024;

  option.write_buffer_size = g_pika_conf->write_buffer_size();
  option.target_file_size_base = g_pika_conf->target_file_size_base();
  option.max_background_flushes = g_pika_conf->max_background_flushes();
  option.max_background_compactions = g_pika_conf->max_background_compactions();
  option.max_open_files = g_pika_conf->max_cache_files();
  option.max_bytes_for_level_multiplier = g_pika_conf->max_bytes_for_level_multiplier();

  if (g_pika_conf->compression() == "none") {
    option.compression = rocksdb::CompressionType::kNoCompression;
  } else if (g_pika_conf->compression() == "snappy") {
    option.compression = rocksdb::CompressionType::kSnappyCompression;
  } else if (g_pika_conf->compression() == "zlib") {
    option.compression = rocksdb::CompressionType::kZlibCompression;
  }

  std::string db_path = g_pika_conf->db_path();
  std::string tmp_path(db_path);
  if (tmp_path.back() == '/') {
    tmp_path.resize(tmp_path.size() - 1);
  }
  tmp_path += "_bak";
  slash::DeleteDirIfExist(tmp_path);

  RWLock l(&rwlock_, true);
  LOG(INFO) << "Prepare change db from: " << tmp_path;
  db_.reset();
  if (0 != slash::RenameFile(db_path.c_str(), tmp_path)) {
    LOG(WARNING) << "Failed to rename db path when change db, error: " << strerror(errno);
    return false;
  }

  if (0 != slash::RenameFile(new_path.c_str(), db_path.c_str())) {
    LOG(WARNING) << "Failed to rename new db path when change db, error: " << strerror(errno);
    return false;
  }

  db_.reset(new blackwidow::BlackWidow());
  rocksdb::Status s = db_->Open(option, db_path);
  assert(db_);
  assert(s.ok());
  slash::DeleteDirIfExist(tmp_path);
  LOG(INFO) << "Change db success";
  return true;
}

bool PikaServer::IsDoubleMaster(const std::string master_ip, int master_port) {
  if ((g_pika_conf->double_master_ip() == master_ip || host() == master_ip)
    && g_pika_conf->double_master_port() == master_port) {
    return true;
  } else {
    return false;
  }
}


void PikaServer::MayUpdateSlavesMap(int64_t sid, int32_t hb_fd) {
  slash::MutexLock l(&slave_mutex_);
  std::vector<SlaveItem>::iterator iter = slaves_.begin();
  LOG(INFO) << "MayUpdateSlavesMap, sid: " << sid << " hb_fd: " << hb_fd;
  while (iter != slaves_.end()) {
    if (iter->sid == sid) {
      iter->hb_fd = hb_fd;
      iter->stage = SLAVE_ITEM_STAGE_TWO;
      LOG(INFO) << "New Master-Slave connection established successfully, Slave host: " << iter->ip_port;
      
      // If receive 'spci' from the peer-master
      if (DoubleMasterMode() && repl_state_ == PIKA_REPL_NO_CONNECT && iter->sid == double_master_sid_) {
        std::string double_master_ip = g_pika_conf->double_master_ip();
        int32_t double_master_port = g_pika_conf->double_master_port();
        SetMaster(double_master_ip, double_master_port);
      }
      break;
    }
    iter++;
  }
}

// Try add Slave, return slave sid if success,
// return -1 when slave already exist
int64_t PikaServer::TryAddSlave(const std::string& ip, int64_t port) {
  std::string ip_port = slash::IpPortString(ip, port);

  slash::MutexLock l(&slave_mutex_);
  std::vector<SlaveItem>::iterator iter = slaves_.begin();
  while (iter != slaves_.end()) {
    if (iter->ip_port == ip_port) {
      return -1;
    }
    iter++;
  }

  // Not exist, so add new
  LOG(INFO) << "Add new slave, " << ip << ":" << port;
  SlaveItem s;
  if (DoubleMasterMode() && IsDoubleMaster(ip, port)) {  // Double master mode
    s.sid = double_master_sid_;
  } else {
    s.sid = GenSid();
  }
  s.ip_port = ip_port;
  s.port = port;
  s.hb_fd = -1;
  s.stage = SLAVE_ITEM_STAGE_ONE;
  gettimeofday(&s.create_time, NULL);
  s.sender = NULL;
  slaves_.push_back(s);
  return s.sid;
}

// Set binlog sender of SlaveItem
bool PikaServer::SetSlaveSender(const std::string& ip, int64_t port,
    PikaBinlogSenderThread* s){
  std::string ip_port = slash::IpPortString(ip, port);

  slash::MutexLock l(&slave_mutex_);
  std::vector<SlaveItem>::iterator iter = slaves_.begin();
  while (iter != slaves_.end()) {
    if (iter->ip_port == ip_port) {
      break;
    }
    iter++;
  }
  if (iter == slaves_.end()) {
    // Not exist
    return false;
  }

  iter->sender = s;
  iter->sender_tid = s->thread_id();
  LOG(INFO) << "SetSlaveSender ok, tid is " << iter->sender_tid
    << " hd_fd: " << iter->hb_fd << " stage: " << iter->stage;
  return true;
}

int32_t PikaServer::GetSlaveListString(std::string& slave_list_str) {
  size_t index = 0;
  std::string slave_ip_port;
  std::stringstream tmp_stream;

  slash::MutexLock l(&slave_mutex_);
  std::vector<SlaveItem>::iterator iter = slaves_.begin();
  for (; iter != slaves_.end(); ++iter) {
    if ((*iter).sender == NULL) {
      // Binlog Sender has not yet created
      continue;
    }

    uint32_t master_filenum, slave_filenum;
    uint64_t master_offset, slave_offset;
    logger_->GetProducerStatus(&master_filenum, &master_offset);
    PikaBinlogSenderThread* ptr_sender = static_cast<PikaBinlogSenderThread*>(iter->sender);
    slave_filenum = ptr_sender->filenum();
    slave_offset = ptr_sender->con_offset();

    uint64_t lag = (master_filenum - slave_filenum) * logger_->file_size()
      + (master_offset - slave_offset);

    slave_ip_port =(*iter).ip_port;
    tmp_stream << "slave" << index++
      << ":ip=" << slave_ip_port.substr(0, slave_ip_port.find(":"))
      << ",port=" << slave_ip_port.substr(slave_ip_port.find(":") + 1)
      << ",state=" << ((*iter).stage == SLAVE_ITEM_STAGE_TWO ? "online" : "offline")
      << ",sid=" << (*iter).sid
      << ",lag=" << lag
      << "\r\n";
  }
  slave_list_str.assign(tmp_stream.str());
  return index;
}

void PikaServer::BecomeMaster() {
  slash::RWLock l(&state_protector_, true);
  if (double_master_mode_) {
    role_ |= PIKA_ROLE_DOUBLE_MASTER;
  } else {
    role_ |= PIKA_ROLE_MASTER;
  }
}

bool PikaServer::SetMaster(std::string& master_ip, int master_port) {
  if (master_ip == "127.0.0.1") {
    master_ip = host_;
  }
  slash::RWLock l(&state_protector_, true);
  if ((role_ ^ PIKA_ROLE_SLAVE) && repl_state_ == PIKA_REPL_NO_CONNECT) {
    master_ip_ = master_ip;
    master_port_ = master_port;
    if (!double_master_mode_) {
      role_ |= PIKA_ROLE_SLAVE;
      repl_state_ = PIKA_REPL_CONNECT;
      LOG(INFO) << "Open read-only mode";
      g_pika_conf->SetReadonly(true);
      return true;
    } else {
      role_ |= PIKA_ROLE_DOUBLE_MASTER;
      repl_state_ = PIKA_REPL_CONNECT;
      LOG(INFO) << "In double-master mode, do not open read-only mode";
      return true;
    }
  }
  return false;
}

bool PikaServer::WaitingDBSync() {
  slash::RWLock l(&state_protector_, false);
  DLOG(INFO) << "repl_state: " << repl_state_ << " role: " << role_ << " master_connection: " << master_connection_;
  if (repl_state_ == PIKA_REPL_WAIT_DBSYNC) {
    return true;
  }
  return false;
}

void PikaServer::NeedWaitDBSync() {
  slash::RWLock l(&state_protector_, true);
  repl_state_ = PIKA_REPL_WAIT_DBSYNC;
}

void PikaServer::WaitDBSyncFinish() {
  slash::RWLock l(&state_protector_, true);
  if (repl_state_ == PIKA_REPL_WAIT_DBSYNC) {
    repl_state_ = PIKA_REPL_CONNECT;
  }
}

void PikaServer::KillBinlogSenderConn() {
  pika_binlog_receiver_thread_->KillBinlogSender();
}

bool PikaServer::ShouldConnectMaster() {
  slash::RWLock l(&state_protector_, false);
  DLOG(INFO) << "repl_state: " << repl_state_ << " role: " << role_ << " master_connection: " << master_connection_;
  if (repl_state_ == PIKA_REPL_CONNECT) {
    return true;
  }
  return false;
}

void PikaServer::ConnectMasterDone() {
  slash::RWLock l(&state_protector_, true);
  if (repl_state_ == PIKA_REPL_CONNECT) {
    repl_state_ = PIKA_REPL_CONNECTING;
  }
}

bool PikaServer::ShouldStartPingMaster() {
  slash::RWLock l(&state_protector_, false);
  DLOG(INFO) << "ShouldStartPingMaster: master_connection " << master_connection_ << " repl_state " << repl_state_;
  if (repl_state_ == PIKA_REPL_CONNECTING && master_connection_ < 2) {
    return true;
  }
  return false;
}

void PikaServer::MinusMasterConnection() {
  slash::RWLock l(&state_protector_, true);
  if (master_connection_ > 0) {
    if ((--master_connection_) <= 0) {
      // two connection with master has been deleted
      if (role_ & PIKA_ROLE_SLAVE) {
        repl_state_ = PIKA_REPL_CONNECT; // not change by slaveof no one, so set repl_state = PIKA_REPL_CONNECT, continue to connect master
      } else {
        repl_state_ = PIKA_REPL_NO_CONNECT; // change by slaveof no one, so set repl_state = PIKA_REPL_NO_CONNECT, reset to SINGLE state
      }
      master_connection_ = 0;
    }
  }
}

void PikaServer::PlusMasterConnection() {
  slash::RWLock l(&state_protector_, true);
  if (master_connection_ < 2) {
    if ((++master_connection_) >= 2) {
      // two connection with master has been established
      repl_state_ = PIKA_REPL_CONNECTED;
      master_connection_ = 2;
      LOG(INFO) << "Master-Slave connection established successfully";
    }
  }
}

bool PikaServer::ShouldAccessConnAsMaster(const std::string& ip) {
  slash::RWLock l(&state_protector_, false);
  DLOG(INFO) << "ShouldAccessConnAsMaster, repl_state_: " << repl_state_ << " ip: " << ip << " master_ip: " << master_ip_;
  if ((repl_state_ == PIKA_REPL_CONNECTING || repl_state_ == PIKA_REPL_CONNECTED) &&
      ip == master_ip_) {
    return true;
  }
  return false;
}

void PikaServer::SyncError() {

  {
  slash::RWLock l(&state_protector_, true);
  repl_state_ = PIKA_REPL_ERROR;
  }
  {
    slash::MutexLock l(&slaveping_protector_);
    if (ping_thread_ != NULL) {
      int err = ping_thread_->StopThread();
      if (err != 0) {
        std::string msg = "can't join thread " + std::string(strerror(err));
        LOG(WARNING) << msg;
      }
      delete ping_thread_;
      ping_thread_ = NULL;
    }
  }
  LOG(WARNING) << "Sync error, set repl_state to PIKA_REPL_ERROR";
}

void PikaServer::RemoveMaster() {

  {
  slash::RWLock l(&state_protector_, true);
  repl_state_ = PIKA_REPL_NO_CONNECT;
  if (DoubleMasterMode()) {
    role_ &= ~PIKA_ROLE_DOUBLE_MASTER;
  } else {
    role_ &= ~PIKA_ROLE_SLAVE;
  }

  master_ip_ = "";
  master_port_ = -1;
  }
  {
    slash::MutexLock l(&slaveping_protector_);
    if (ping_thread_ != NULL) {
      int err = ping_thread_->StopThread();
      if (err != 0) {
        std::string msg = "can't join thread " + std::string(strerror(err));
        LOG(WARNING) << msg;
      }
      delete ping_thread_;
      ping_thread_ = NULL;
    }
  }
  {
  slash::RWLock l(&state_protector_, true);
  master_connection_ = 0;
  }
  LOG(INFO) << "close read-only mode";
  g_pika_conf->SetReadonly(false);
}

void PikaServer::TryDBSync(const std::string& ip, int port, int32_t top) {
  std::string bg_path;
  uint32_t bg_filenum = 0;
  {
    slash::MutexLock l(&bgsave_protector_);
    bg_path = bgsave_info_.path;
    bg_filenum = bgsave_info_.filenum;
  }

  if (0 != slash::IsDir(bg_path) ||                               //Bgsaving dir exist
      !slash::FileExists(NewFileName(logger_->filename, bg_filenum)) ||  //filenum can be found in binglog
      top - bg_filenum > kDBSyncMaxGap) {      //The file is not too old
    // Need Bgsave first
    Bgsave();
  }
  DBSync(ip, port);
}

void PikaServer::DBSync(const std::string& ip, int port) {
  // Only one DBSync task for every ip_port
  std::string ip_port = slash::IpPortString(ip, port);
  {
    slash::MutexLock ldb(&db_sync_protector_);
    if (db_sync_slaves_.find(ip_port) != db_sync_slaves_.end()) {
      return;
    }
    db_sync_slaves_.insert(ip_port);
  }
  // Reuse the bgsave_thread_
  // Since we expect Bgsave and DBSync execute serially
  bgsave_thread_.StartThread();
  DBSyncArg *arg = new DBSyncArg(this, ip, port);
  bgsave_thread_.Schedule(&DoDBSync, static_cast<void*>(arg));
}

void PikaServer::DoDBSync(void* arg) {
  DBSyncArg *ppurge = static_cast<DBSyncArg*>(arg);
  PikaServer* ps = ppurge->p;

  ps->DBSyncSendFile(ppurge->ip, ppurge->port);

  delete (PurgeArg*)arg;
}

void PikaServer::DBSyncSendFile(const std::string& ip, int port) {
  std::string bg_path;
  uint32_t binlog_filenum;
  uint64_t binlog_offset;
  {
    slash::MutexLock l(&bgsave_protector_);
    bg_path = bgsave_info_.path;
    binlog_filenum = bgsave_info_.filenum;
    binlog_offset = bgsave_info_.offset;
  }
  // Get all files need to send
  std::vector<std::string> descendant;
  int ret = 0;
  LOG(INFO) << "Start Send files in " << bg_path << " to " << ip;
  ret = slash::GetChildren(bg_path, descendant);
  if (ret != 0) {
    LOG(WARNING) << "Get child directory when try to do sync failed, error: " << strerror(ret);
    return;
  }

  // Iterate to send files
  ret = 0;
  std::string local_path, target_path;
  std::string module = kDBSyncModule + "_" + slash::IpPortString(host_, port_);
  std::vector<std::string>::iterator it = descendant.begin();
  slash::RsyncRemote remote(ip, port, module, g_pika_conf->db_sync_speed() * 1024);
  for (; it != descendant.end(); ++it) {
    local_path = bg_path + "/" + *it;
    target_path = *it;

    if (target_path == kBgsaveInfoFile) {
      continue;
    }

    if (slash::IsDir(local_path) == 0 &&
        local_path.back() != '/') {
      local_path.push_back('/');
      target_path.push_back('/');
    }

    // We need specify the speed limit for every single file
    ret = slash::RsyncSendFile(local_path, target_path, remote);

    if (0 != ret) {
      LOG(WARNING) << "rsync send file failed! From: " << *it
        << ", To: " << target_path
        << ", At: " << ip << ":" << port
        << ", Error: " << ret;
      break;
    }
  }

  // Clear target path
  slash::RsyncSendClearTarget(bg_path + "/strings", "strings", remote);
  slash::RsyncSendClearTarget(bg_path + "/hashes", "hashes", remote);
  slash::RsyncSendClearTarget(bg_path + "/lists", "lists", remote);
  slash::RsyncSendClearTarget(bg_path + "/sets", "sets", remote);
  slash::RsyncSendClearTarget(bg_path + "/zsets", "zsets", remote);

  // Send info file at last
  if (0 == ret) {
    if (0 != (ret = slash::RsyncSendFile(bg_path + "/" + kBgsaveInfoFile, kBgsaveInfoFile, remote))) {
      LOG(WARNING) << "send info file failed";
    }
  }

  // remove slave
  std::string ip_port = slash::IpPortString(ip, port);
  {
    slash::MutexLock ldb(&db_sync_protector_);
    db_sync_slaves_.erase(ip_port);
  }
  if (0 == ret) {
    LOG(INFO) << "rsync send files success";
    // If receiver is the peer-master, 
    // need to update receive binlog info
    if ((g_pika_conf->double_master_ip() == ip || host() == ip)
        && (g_pika_conf->double_master_port() + 3000) == port) {
      // Update Recv Info
      logger_->SetDoubleRecvInfo(binlog_filenum, binlog_offset);
      LOG(INFO) << "Update recv info filenum: " << binlog_filenum << " offset: " << binlog_offset;
    }
  }
}

/*
 * BinlogSender
 */
Status PikaServer::AddBinlogSender(const std::string& ip, int64_t port,
                                   int64_t sid,
                                   uint32_t filenum, uint64_t con_offset) {
  // Sanity check
  if (con_offset > logger_->file_size()) {
    return Status::InvalidArgument("AddBinlogSender invalid binlog offset");
  }
  uint32_t cur_filenum = 0;
  uint64_t cur_offset = 0;
  logger_->GetProducerStatus(&cur_filenum, &cur_offset);
  if (filenum != UINT32_MAX &&
      (cur_filenum < filenum || (cur_filenum == filenum && cur_offset < con_offset))) {
    return Status::InvalidArgument("AddBinlogSender invalid binlog offset");
  }

  if (filenum == UINT32_MAX) {
    LOG(INFO) << "Maybe force full sync";
  }

  // Create and set sender
  slash::SequentialFile *readfile;
  std::string confile = NewFileName(logger_->filename, filenum);
  if (!slash::FileExists(confile)) {
    // Not found binlog specified by filenum
    // If in double-master mode, return error status
    if (DoubleMasterMode() && IsDoubleMaster(ip, port) && filenum != UINT32_MAX) {
      return Status::InvalidArgument("AddBinlogSender invalid binlog offset");
    }

    TryDBSync(ip, port + 3000, cur_filenum);
    return Status::Incomplete("Bgsaving and DBSync first");
  }
  if (!slash::NewSequentialFile(confile, &readfile).ok()) {
    return Status::IOError("AddBinlogSender new sequtialfile");
  }

  PikaBinlogSenderThread* sender = new PikaBinlogSenderThread(ip,
      port + 1000, sid, readfile, filenum, con_offset);

  if (sender->trim() == 0 // Error binlog
      && SetSlaveSender(ip, port, sender)) { // SlaveItem not exist
    sender->StartThread();
    return Status::OK();
  } else {
    delete sender;
    LOG(WARNING) << "AddBinlogSender failed";
    return Status::NotFound("AddBinlogSender bad sender");
  }
}

// Prepare engine, need bgsave_protector protect
bool PikaServer::InitBgsaveEnv() {
  {
    slash::MutexLock l(&bgsave_protector_);
    // Prepare for bgsave dir
    bgsave_info_.start_time = time(NULL);
    char s_time[32];
    int len = strftime(s_time, sizeof(s_time), "%Y%m%d%H%M%S", localtime(&bgsave_info_.start_time));
    bgsave_info_.s_start_time.assign(s_time, len);
    std::string bgsave_path(g_pika_conf->bgsave_path());
    bgsave_info_.path = bgsave_path + g_pika_conf->bgsave_prefix() + std::string(s_time, 8);
    if (!slash::DeleteDirIfExist(bgsave_info_.path)) {
      LOG(WARNING) << "remove exist bgsave dir failed";
      return false;
    }
    slash::CreatePath(bgsave_info_.path, 0755);
    // Prepare for failed dir
    if (!slash::DeleteDirIfExist(bgsave_info_.path + "_FAILED")) {
      LOG(WARNING) << "remove exist fail bgsave dir failed :";
      return false;
    }
  }
  return true;
}

// Prepare bgsave env, need bgsave_protector protect
bool PikaServer::InitBgsaveEngine() {
  delete bgsave_engine_;
  rocksdb::Status s = blackwidow::BackupEngine::Open(db().get(), &bgsave_engine_);
  if (!s.ok()) {
    LOG(WARNING) << "open backup engine failed " << s.ToString();
    return false;
  }

  {
    RWLock l(&rwlock_, true);
    {
      slash::MutexLock l(&bgsave_protector_);
      logger_->GetProducerStatus(&bgsave_info_.filenum, &bgsave_info_.offset);
    }
    s = bgsave_engine_->SetBackupContent();
    if (!s.ok()){
      LOG(WARNING) << "set backup content failed " << s.ToString();
      return false;
    }
  }
  return true;
}

bool PikaServer::RunBgsaveEngine() {
  // Prepare for Bgsaving
  if (!InitBgsaveEnv() || !InitBgsaveEngine()) {
    ClearBgsave();
    return false;
  }
  LOG(INFO) << "after prepare bgsave";
  
  BGSaveInfo info = bgsave_info();
  LOG(INFO) << "   bgsave_info: path=" << info.path
    << ",  filenum=" << info.filenum
    << ", offset=" << info.offset;

  // Backup to tmp dir
  rocksdb::Status s = bgsave_engine_->CreateNewBackup(info.path);
  LOG(INFO) << "Create new backup finished.";

  if (!s.ok()) {
    LOG(WARNING) << "backup failed :" << s.ToString();
    return false;
  }
  return true;
}

void PikaServer::Bgsave() {
  // Only one thread can go through
  {
    slash::MutexLock l(&bgsave_protector_);
    if (bgsave_info_.bgsaving) {
      return;
    }
    bgsave_info_.bgsaving = true;
  }

  // Start new thread if needed
  bgsave_thread_.StartThread();
  bgsave_thread_.Schedule(&DoBgsave, static_cast<void*>(this));
}

void PikaServer::DoBgsave(void* arg) {
  PikaServer* p = static_cast<PikaServer*>(arg);

  // Do bgsave
  bool ok = p->RunBgsaveEngine();

  // Some output
  BGSaveInfo info = p->bgsave_info();
  std::ofstream out;
  out.open(info.path + "/info", std::ios::in | std::ios::trunc);
  if (out.is_open()) {
    out << (time(NULL) - info.start_time) << "s\n"
      << p->host() << "\n"
      << p->port() << "\n"
      << info.filenum << "\n"
      << info.offset << "\n";
    out.close();
  }
  if (!ok) {
    std::string fail_path = info.path + "_FAILED";
    slash::RenameFile(info.path.c_str(), fail_path.c_str());
  }
  p->FinishBgsave();
}

bool PikaServer::Bgsaveoff() {
  {
    slash::MutexLock l(&bgsave_protector_);
    if (!bgsave_info_.bgsaving) {
      return false;
    }
  }
  if (bgsave_engine_ != NULL) {
    bgsave_engine_->StopBackup();
  }
  return true;
}

bool PikaServer::PurgeLogs(uint32_t to, bool manual, bool force) {
  // Only one thread can go through
  bool expect = false;
  if (!purging_.compare_exchange_strong(expect, true)) {
    LOG(WARNING) << "purge process already exist";
    return false;
  }
  PurgeArg *arg = new PurgeArg();
  arg->p = this;
  arg->to = to;
  arg->manual = manual;
  arg->force = force;
  // Start new thread if needed
  purge_thread_.StartThread();
  purge_thread_.Schedule(&DoPurgeLogs, static_cast<void*>(arg));
  return true;
}

void PikaServer::DoPurgeLogs(void* arg) {
  PurgeArg *ppurge = static_cast<PurgeArg*>(arg);
  PikaServer* ps = ppurge->p;

  ps->PurgeFiles(ppurge->to, ppurge->manual, ppurge->force);

  ps->ClearPurge();
  delete (PurgeArg*)arg;
}

bool PikaServer::GetPurgeWindow(uint32_t &max) {
  uint64_t tmp;
  logger_->GetProducerStatus(&max, &tmp);
  slash::MutexLock l(&slave_mutex_);
  std::vector<SlaveItem>::iterator it;
  for (it = slaves_.begin(); it != slaves_.end(); ++it) {
    if ((*it).sender == NULL) {
      // One Binlog Sender has not yet created, no purge
      return false;
    }
    PikaBinlogSenderThread *pb = static_cast<PikaBinlogSenderThread*>((*it).sender);
    uint32_t filenum = pb->filenum();
    max = filenum < max ? filenum : max;
  }
  // remain some more
  if (max >= 10) {
    max -= 10;
    return true;
  }
  return false;
}

bool PikaServer::CouldPurge(uint32_t index) {
  uint32_t pro_num;
  uint64_t tmp;
  logger_->GetProducerStatus(&pro_num, &tmp);

  index += 10; //remain some more
  if (index > pro_num) {
    return false;
  }
  slash::MutexLock l(&slave_mutex_);
  std::vector<SlaveItem>::iterator it;
  for (it = slaves_.begin(); it != slaves_.end(); ++it) {
    if ((*it).sender == NULL) {
      // One Binlog Sender has not yet created, no purge
      return false;
    }
    PikaBinlogSenderThread *pb = static_cast<PikaBinlogSenderThread*>((*it).sender);
    uint32_t filenum = pb->filenum();
    if (index > filenum) {   // slaves
      return false;
    }
  }
  return true;
}

bool PikaServer::PurgeFiles(uint32_t to, bool manual, bool force)
{
  std::map<uint32_t, std::string> binlogs;
  if (!GetBinlogFiles(binlogs)) {
    LOG(WARNING) << "Could not get binlog files!";
    return false;
  }

  int delete_num = 0;
  struct stat file_stat;
  int remain_expire_num = binlogs.size() - g_pika_conf->expire_logs_nums();
  std::map<uint32_t, std::string>::iterator it;
  for (it = binlogs.begin(); it != binlogs.end(); ++it) {
    if ((manual && it->first <= to) ||           // Argument bound
        remain_expire_num > 0 ||                 // Expire num trigger
        (binlogs.size() > 10 /* at lease remain 10 files */
         && stat(((g_pika_conf->log_path() + it->second)).c_str(), &file_stat) == 0 &&
         file_stat.st_mtime < time(NULL) - g_pika_conf->expire_logs_days()*24*3600)) // Expire time trigger
    {
      // We check this every time to avoid lock when we do file deletion
      if (!CouldPurge(it->first) && !force) {
        LOG(WARNING) << "Could not purge "<< (it->first) << ", since it is already be used";
        return false;
      }

      // Do delete
      slash::Status s = slash::DeleteFile(g_pika_conf->log_path() + it->second);
      if (s.ok()) {
        ++delete_num;
        --remain_expire_num;
      } else {
        LOG(WARNING) << "Purge log file : " << (it->second) <<  " failed! error:" << s.ToString();
      }
    } else {
      // Break when face the first one not satisfied
      // Since the binlogs is order by the file index
      break;
    }
  }
  if (delete_num) {
    LOG(INFO) << "Success purge "<< delete_num;
  }

  return true;
}

bool PikaServer::GetBinlogFiles(std::map<uint32_t, std::string>& binlogs) {
  std::vector<std::string> children;
  int ret = slash::GetChildren(g_pika_conf->log_path(), children);
  if (ret != 0){
    LOG(WARNING) << "Get all files in log path failed! error:" << ret;
    return false;
  }

  int64_t index = 0;
  std::string sindex;
  std::vector<std::string>::iterator it;
  for (it = children.begin(); it != children.end(); ++it) {
    if ((*it).compare(0, kBinlogPrefixLen, kBinlogPrefix) != 0) {
      continue;
    }
    sindex = (*it).substr(kBinlogPrefixLen);
    if (slash::string2l(sindex.c_str(), sindex.size(), &index) == 1) {
      binlogs.insert(std::pair<uint32_t, std::string>(static_cast<uint32_t>(index), *it));
    }
  }
  return true;
}

void PikaServer::AutoCompactRange() {
  struct statfs disk_info;
  int ret = statfs(g_pika_conf->db_path().c_str(), &disk_info);
  if (ret == -1) {
    LOG(WARNING) << "statfs error: " << strerror(errno);
    return;
  }

  uint64_t total_size = disk_info.f_bsize * disk_info.f_blocks;
  uint64_t free_size = disk_info.f_bsize * disk_info.f_bfree;
//      LOG(INFO) << "free_size: " << free_size << " disk_size: " << total_size <<
//        " cal: " << ((double)free_size / total_size) * 100;
  std::string ci = g_pika_conf->compact_interval();
  std::string cc = g_pika_conf->compact_cron();

  if (ci != "") {
    std::string::size_type slash = ci.find("/");
    int interval = std::atoi(ci.substr(0, slash).c_str());
    int usage = std::atoi(ci.substr(slash+1).c_str());
    struct timeval now;
    gettimeofday(&now, NULL);
    if (last_check_compact_time_.tv_sec == 0 ||
          now.tv_sec - last_check_compact_time_.tv_sec
          >= interval * 3600) {
      gettimeofday(&last_check_compact_time_, NULL);
      if (((double)free_size / total_size) * 100 >= usage) {
        rocksdb::Status s = db_->Compact(blackwidow::kAll);
        if (s.ok()) {
          LOG(INFO) << "[Interval]schedule compactRange, freesize: " << free_size/1048576 << "MB, disksize: " << total_size/1048576 << "MB";
        } else {
          LOG(INFO) << "[Interval]schedule compactRange Failed, freesize: " << free_size/1048576 << "MB, disksize: " << total_size/1048576
            << "MB, error: " << s.ToString();
        }
      }
    }
    return;
  }

  if (cc != "") {
    std::string::size_type colon = cc.find("-");
    std::string::size_type underline = cc.find("/");
    int start = std::atoi(cc.substr(0, colon).c_str());
    int end = std::atoi(cc.substr(colon+1, underline).c_str());
    int usage = std::atoi(cc.substr(underline+1).c_str());
    std::time_t t = std::time(nullptr);
    std::tm* t_m = std::localtime(&t);
    bool in_window = false;
    if (start < end && (t_m->tm_hour >= start && t_m->tm_hour < end)) {
      in_window = true;
    } else if (start > end && ((t_m->tm_hour >= start && t_m->tm_hour < 24) ||
          (t_m->tm_hour >= 0 && t_m->tm_hour < end))) {
      in_window = true;
    } else {
      have_scheduled_crontask_ = false;
    }
    if (!have_scheduled_crontask_ && in_window) {
      if (((double)free_size / total_size) * 100 >= usage) {
        rocksdb::Status s = db_->Compact(blackwidow::kAll);
        if (s.ok()) {
          LOG(INFO) << "[Cron]schedule compactRange, freesize: " << free_size/1048576 << "MB, disksize: " << total_size/1048576 << "MB";
        } else {
          LOG(INFO) << "[Cron]schedule compactRange Failed, freesize: " << free_size/1048576 << "MB, disksize: " << total_size/1048576
            << "MB, error: " << s.ToString();
        }
        have_scheduled_crontask_ = true;
      }
    }
  }
}

void PikaServer::AutoPurge() {
  if (!PurgeLogs(0, false, false)) {
    DLOG(WARNING) << "Auto purge failed";
    return;
  }
}

void PikaServer::AutoDeleteExpiredDump() {
  std::string db_sync_prefix = g_pika_conf->bgsave_prefix();
  std::string db_sync_path = g_pika_conf->bgsave_path();
  int expiry_days = g_pika_conf->expire_dump_days();
  std::vector<std::string> dump_dir;

  // Never expire
  if (expiry_days <= 0) {
    return;
  }

  // Dump is not exist
  if (!slash::FileExists(db_sync_path)) {
    return;
  }

  // Directory traversal
  if (slash::GetChildren(db_sync_path, dump_dir) != 0) {
    return;
  }
  // Handle dump directory
  for (size_t i = 0; i < dump_dir.size(); i++) {
    if (dump_dir[i].substr(0, db_sync_prefix.size()) != db_sync_prefix || dump_dir[i].size() != (db_sync_prefix.size() + 8)) {
      continue;
    }

    std::string str_date = dump_dir[i].substr(db_sync_prefix.size(), (dump_dir[i].size() - db_sync_prefix.size()));
    char *end = NULL;
    std::strtol(str_date.c_str(), &end, 10);
    if (*end != 0) {
      continue;
    }

    // Parse filename
    int dump_year = std::atoi(str_date.substr(0, 4).c_str());
    int dump_month = std::atoi(str_date.substr(4, 2).c_str());
    int dump_day = std::atoi(str_date.substr(6, 2).c_str());

    time_t t = time(NULL);
    struct tm *now = localtime(&t);
    int now_year = now->tm_year + 1900;
    int now_month = now->tm_mon + 1;
    int now_day = now->tm_mday;

    struct tm dump_time, now_time;

    dump_time.tm_year = dump_year;
    dump_time.tm_mon = dump_month;
    dump_time.tm_mday = dump_day;
    dump_time.tm_hour = 0;
    dump_time.tm_min = 0;
    dump_time.tm_sec = 0;

    now_time.tm_year = now_year;
    now_time.tm_mon = now_month;
    now_time.tm_mday = now_day;
    now_time.tm_hour = 0;
    now_time.tm_min = 0;
    now_time.tm_sec = 0;

    long dump_timestamp = mktime(&dump_time);
    long now_timestamp = mktime(&now_time);
    // How many days, 1 day = 86400s
    int interval_days = (now_timestamp - dump_timestamp) / 86400;

    if (interval_days >= expiry_days) {
      std::string dump_file = db_sync_path + dump_dir[i];
      if (CountSyncSlaves() == 0) {
        LOG(INFO) << "Not syncing, delete dump file: " << dump_file;
        slash::DeleteDirIfExist(dump_file);
      } else if (bgsave_info().path != dump_file) {
        LOG(INFO) << "Syncing, delete expired dump file: " << dump_file;
        slash::DeleteDirIfExist(dump_file);
      } else {
        LOG(INFO) << "Syncing, can not delete " << dump_file << " dump file";
      }
    }
  }
}

bool PikaServer::FlushAll() {
  {
    slash::MutexLock l(&bgsave_protector_);
    if (bgsave_info_.bgsaving) {
      return false;
    }
  }
  {
    slash::MutexLock l(&key_scan_protector_);
    if (key_scan_info_.key_scaning_) {
      return false;
    }
  }

  LOG(INFO) << "Delete old db...";
  db_.reset();

  std::string dbpath = g_pika_conf->db_path();
  if (dbpath[dbpath.length() - 1] == '/') {
    dbpath.erase(dbpath.length() - 1);
  }
  int pos = dbpath.find_last_of('/');
  dbpath = dbpath.substr(0, pos);
  dbpath.append("/deleting");
  slash::RenameFile(g_pika_conf->db_path(), dbpath.c_str());

  //Create blackwidow handle
  rocksdb::Options rocksdb_option;
  RocksdbOptionInit(&rocksdb_option);

  LOG(INFO) << "Prepare open new db...";
  db_ = std::shared_ptr<blackwidow::BlackWidow>(new blackwidow::BlackWidow());
  rocksdb::Status s = db_->Open(rocksdb_option, g_pika_conf->db_path());
  assert(db_);
  assert(s.ok());
  LOG(INFO) << "open new db success";
  PurgeDir(dbpath);
  return true;
}

bool PikaServer::FlushDb(const std::string& db_name) {
  {
    slash::MutexLock l(&bgsave_protector_);
    if (bgsave_info_.bgsaving) {
      return false;
    }
  }
  {
    slash::MutexLock l(&key_scan_protector_);
    if (key_scan_info_.key_scaning_) {
      return false;
    }
  }

  LOG(INFO) << "Delete old " + db_name + " db...";
  db_.reset();

  std::string dbpath = g_pika_conf->db_path();
  if (dbpath[dbpath.length() - 1] != '/') {
     dbpath.append("/");
  }
  std::string sub_dbpath = dbpath + db_name;
  std::string del_dbpath = dbpath + db_name + "_deleting";
  slash::RenameFile(sub_dbpath, del_dbpath);

  rocksdb::Options rocksdb_option;
  RocksdbOptionInit(&rocksdb_option);

  LOG(INFO) << "Prepare open new " + db_name + " db...";
  db_ = std::shared_ptr<blackwidow::BlackWidow>(new blackwidow::BlackWidow());
  rocksdb::Status s = db_->Open(rocksdb_option, g_pika_conf->db_path());
  assert(db_);
  assert(s.ok());
  LOG(INFO) << "open new " + db_name + " db success";
  PurgeDir(del_dbpath);
  return true;
}

void PikaServer::PurgeDir(std::string& path) {
  std::string *dir_path = new std::string(path);
  // Start new thread if needed
  purge_thread_.StartThread();
  purge_thread_.Schedule(&DoPurgeDir, static_cast<void*>(dir_path));
}

void PikaServer::DoPurgeDir(void* arg) {
  std::string path = *(static_cast<std::string*>(arg));
  LOG(INFO) << "Delete dir: " << path << " start";
  slash::DeleteDir(path);
  LOG(INFO) << "Delete dir: " << path << " done";
  delete static_cast<std::string*>(arg);
}

// PubSub

// Publish
int PikaServer::Publish(const std::string& channel, const std::string& msg) {
  int receivers = pika_pubsub_thread_->Publish(channel, msg);
  return receivers;
}

// Subscribe/PSubscribe
void PikaServer::Subscribe(pink::PinkConn* conn, const std::vector<std::string>& channels, bool pattern, std::vector<std::pair<std::string, int>>* result) {
  pika_pubsub_thread_->Subscribe(conn, channels, pattern, result);
}

// UnSubscribe/PUnSubscribe
int PikaServer::UnSubscribe(pink::PinkConn* conn, const std::vector<std::string>& channels, bool pattern, std::vector<std::pair<std::string, int>>* result) {
  int subscribed = pika_pubsub_thread_->UnSubscribe(conn, channels, pattern, result);
  return subscribed;
}

/*
 * PubSub
 */
void PikaServer::PubSubChannels(const std::string& pattern,
                      std::vector<std::string >* result) {
  pika_pubsub_thread_->PubSubChannels(pattern, result);
}

void PikaServer::PubSubNumSub(const std::vector<std::string>& channels,
                    std::vector<std::pair<std::string, int>>* result) {
  pika_pubsub_thread_->PubSubNumSub(channels, result);
}
int PikaServer::PubSubNumPat() {
  return pika_pubsub_thread_->PubSubNumPat();
}

void PikaServer::AddMonitorClient(PikaClientConn* client_ptr) {
  monitor_thread_->AddMonitorClient(client_ptr);
}

void PikaServer::AddMonitorMessage(const std::string& monitor_message) {
  monitor_thread_->AddMonitorMessage(monitor_message);
}

bool PikaServer::HasMonitorClients() {
  return monitor_thread_->HasMonitorClients();
}

void PikaServer::DispatchBinlogBG(const std::string &key,
    PikaCmdArgsType* argv, BinlogItem* binlog_item, uint64_t cur_serial, bool readonly) {
  size_t index = str_hash(key) % binlogbg_workers_.size();
  binlogbg_workers_[index]->Schedule(argv, binlog_item, cur_serial, readonly);
}

bool PikaServer::WaitTillBinlogBGSerial(uint64_t my_serial) {
  binlogbg_mutex_.Lock();
  //DLOG(INFO) << "Binlog serial wait: " << my_serial << ", current: " << binlogbg_serial_;
  while (binlogbg_serial_ != my_serial && !binlogbg_exit_) {
    binlogbg_cond_.Wait();
  }
  binlogbg_mutex_.Unlock();
  return (binlogbg_serial_ == my_serial);
}

void PikaServer::SignalNextBinlogBGSerial() {
  binlogbg_mutex_.Lock();
  //DLOG(INFO) << "Binlog serial signal: " << binlogbg_serial_;
  ++binlogbg_serial_;
  binlogbg_cond_.SignalAll();
  binlogbg_mutex_.Unlock();
}

void PikaServer::RunKeyScan() {
  std::vector<uint64_t> new_key_nums_v;

  rocksdb::Status s = db_->GetKeyNum(&new_key_nums_v);

  slash::MutexLock lm(&key_scan_protector_);
  if (s.ok()) {
    key_scan_info_.key_nums_v = new_key_nums_v;
  }
  key_scan_info_.key_scaning_ = false;
}

void PikaServer::DoKeyScan(void *arg) {
  PikaServer *p = reinterpret_cast<PikaServer*>(arg);
  p->RunKeyScan();
}

void PikaServer::StopKeyScan() {
  slash::MutexLock l(&key_scan_protector_);
  if (key_scan_info_.key_scaning_) {
    db_->StopScanKeyNum();
    key_scan_info_.key_scaning_ = false;
  }
}

void PikaServer::KeyScan() {
  key_scan_protector_.Lock();
  if (key_scan_info_.key_scaning_) {
    key_scan_protector_.Unlock();
    return;
  }
  key_scan_info_.key_scaning_ = true;
  key_scan_protector_.Unlock();

  key_scan_thread_.StartThread();
  InitKeyScan();
  key_scan_thread_.Schedule(&DoKeyScan, reinterpret_cast<void*>(this));
}

void PikaServer::InitKeyScan() {
  key_scan_info_.start_time = time(NULL);
  char s_time[32];
  int len = strftime(s_time, sizeof(s_time), "%Y-%m-%d %H:%M:%S", localtime(&key_scan_info_.start_time));
  key_scan_info_.s_start_time.assign(s_time, len);
}

void PikaServer::ClientKillAll() {
  pika_dispatch_thread_->ClientKillAll();
  monitor_thread_->ThreadClientKill();
}

int PikaServer::ClientKill(const std::string &ip_port) {
  if (pika_dispatch_thread_->ClientKill(ip_port) ||
      monitor_thread_->ThreadClientKill(ip_port)) {
    return 1;
  }
  return 0;
}

int64_t PikaServer::ClientList(std::vector<ClientInfo> *clients) {
  int64_t clients_num = 0;
  clients_num += pika_dispatch_thread_->ThreadClientList(clients);
  clients_num += monitor_thread_->ThreadClientList(clients);
  return clients_num;
}

void PikaServer::RWLockWriter() {
  pthread_rwlock_wrlock(&rwlock_);
}

void PikaServer::RWLockReader() {
  pthread_rwlock_rdlock(&rwlock_);
}

void PikaServer::RWUnlock() {
  pthread_rwlock_unlock(&rwlock_);
}

void PikaServer::PlusThreadQuerynum() {
  slash::WriteLock l(&statistic_data_.statistic_lock);
  statistic_data_.thread_querynum++;
}

uint64_t PikaServer::ServerQueryNum() {
  slash::ReadLock l(&statistic_data_.statistic_lock);
  return statistic_data_.thread_querynum;
}

void PikaServer::ResetStat() {
  statistic_data_.accumulative_connections.store(0);
  slash::WriteLock l(&statistic_data_.statistic_lock);
  statistic_data_.thread_querynum = 0;
  statistic_data_.last_thread_querynum = 0;
}

void PikaServer::SetDispatchQueueLimit(int queue_limit) {
  rlimit limit;
  rlim_t maxfiles = g_pika_conf->maxclients() + PIKA_MIN_RESERVED_FDS;
  if (getrlimit(RLIMIT_NOFILE, &limit) == -1) {
    LOG(WARNING) << "getrlimit error: " << strerror(errno);
  } else if (limit.rlim_cur < maxfiles) {
    rlim_t old_limit = limit.rlim_cur;
    limit.rlim_cur = maxfiles;
    limit.rlim_max = maxfiles;
    if (setrlimit(RLIMIT_NOFILE, &limit) != -1) {
      LOG(WARNING) << "your 'limit -n ' of " << old_limit << " is not enough for Redis to start. pika have successfully reconfig it to " << limit.rlim_cur;
    } else {
      LOG(FATAL) << "your 'limit -n ' of " << old_limit << " is not enough for Redis to start. pika can not reconfig it(" << strerror(errno) << "), do it by yourself";
    }
  }

  pika_dispatch_thread_->SetQueueLimit(queue_limit);
}

uint64_t PikaServer::ServerCurrentQps() {
  slash::ReadLock l(&statistic_data_.statistic_lock);
  return statistic_data_.last_sec_thread_querynum;
}

void PikaServer::ResetLastSecQuerynum() {
 slash::WriteLock l(&statistic_data_.statistic_lock);
 uint64_t cur_time_us = slash::NowMicros();
 statistic_data_.last_sec_thread_querynum = (
      (statistic_data_.thread_querynum - statistic_data_.last_thread_querynum)
      * 1000000 / (cur_time_us - statistic_data_.last_time_us + 1));
 statistic_data_.last_thread_querynum = statistic_data_.thread_querynum;
 statistic_data_.last_time_us = cur_time_us;
}

void PikaServer::DoTimingTask() {
  // Maybe schedule compactrange
  AutoCompactRange();
  // Purge log
  AutoPurge();
  // Delete expired dump
  AutoDeleteExpiredDump();

  // Check rsync deamon
 if (((role_ & PIKA_ROLE_SLAVE) ^ PIKA_ROLE_SLAVE) || // Not a slave
   repl_state_ == PIKA_REPL_NO_CONNECT ||
   repl_state_ == PIKA_REPL_CONNECTED ||
   repl_state_ == PIKA_REPL_ERROR) {
   slash::StopRsync(g_pika_conf->db_sync_path());
 }
}
