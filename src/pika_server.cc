// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_server.h"

#include <ctime>
#include <fstream>
#include <iterator>
#include <algorithm>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/resource.h>

#include "slash/include/env.h"
#include "slash/include/rsync.h"
#include "slash/include/slash_string.h"
#include "pink/include/bg_thread.h"

#include "include/pika_dispatch_thread.h"

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
  bgsave_engine_(NULL),
  purging_(false),
  binlogbg_exit_(false),
  binlogbg_cond_(&binlogbg_mutex_),
  binlogbg_serial_(0),
  slowlog_entry_id_(0) {

  pthread_rwlockattr_t attr;
  pthread_rwlockattr_init(&attr);
  pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
  pthread_rwlock_init(&rwlock_, &attr);

  //Init server ip host
  if (!ServerInit()) {
    LOG(FATAL) << "ServerInit iotcl error";
  }

  //Create blackwidow handle
  blackwidow::BlackwidowOptions bw_option;
  RocksdbOptionInit(&bw_option);

  std::string db_path = g_pika_conf->db_path();
  std::string log_path = g_pika_conf->log_path();
  LOG(INFO) << "Prepare Blackwidow DB...";
  db_ = std::shared_ptr<blackwidow::BlackWidow>(new blackwidow::BlackWidow());
  rocksdb::Status s = db_->Open(bw_option, db_path);
  assert(db_);
  assert(s.ok());
  LOG(INFO) << "DB Success";


  pthread_rwlockattr_t tables_rw_attr;
  pthread_rwlockattr_init(&tables_rw_attr);
  pthread_rwlockattr_setkind_np(&tables_rw_attr,
          PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
  pthread_rwlock_init(&tables_rw_, &tables_rw_attr);

  InitTableStruct();

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
  pika_heartbeat_thread_ = new PikaHeartbeatThread(ips, port_ + 2000, 1000);
  monitor_thread_ = new PikaMonitorThread();
  pika_pubsub_thread_ = new pink::PubSubThread();
  pika_repl_client_ = new PikaReplClient(3000, 60);
  pika_repl_server_ = new PikaReplServer(ips, port_ + 3000, 3000);
  pika_auxiliary_thread_ = new PikaAuxiliaryThread();
  pika_thread_pool_ = new pink::ThreadPool(g_pika_conf->thread_pool_size(), 100000);

  for (int j = 0; j < g_pika_conf->sync_thread_num(); j++) {
    binlogbg_workers_.push_back(new BinlogBGWorker(g_pika_conf->sync_buffer_size()));
  }

  pthread_rwlock_init(&state_protector_, NULL);
  logger_ = new Binlog(g_pika_conf->log_path(), g_pika_conf->binlog_file_size());

  pthread_rwlock_init(&slowlog_protector_, NULL);
}

PikaServer::~PikaServer() {
  slash::StopRsync(g_pika_conf->db_sync_path());
  delete bgsave_engine_;
  delete pika_thread_pool_;

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

  delete ping_thread_;
  delete pika_pubsub_thread_;
  delete pika_auxiliary_thread_;
  delete pika_repl_client_;
  delete pika_repl_server_;

  binlogbg_exit_ = true;
  std::vector<BinlogBGWorker*>::iterator binlogbg_iter = binlogbg_workers_.begin();
  while (binlogbg_iter != binlogbg_workers_.end()) {
    binlogbg_cond_.SignalAll();
    delete (*binlogbg_iter);
    binlogbg_iter++;
  }
  delete pika_heartbeat_thread_;
  delete monitor_thread_;

  delete logger_;
  db_.reset();
  tables_.clear();

  bgsave_thread_.StopThread();
  key_scan_thread_.StopThread();

  pthread_rwlock_destroy(&rwlock_);
  pthread_rwlock_destroy(&tables_rw_);
  pthread_rwlock_destroy(&state_protector_);
  pthread_rwlock_destroy(&slowlog_protector_);

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

void PikaServer::Schedule(pink::TaskFunc func, void* arg) {
  pika_thread_pool_->Schedule(func, arg);
}

void PikaServer::ScheduleReplCliTask(pink::TaskFunc func, void* arg) {
  pika_repl_client_->Schedule(func, arg);
}

void PikaServer::RocksdbOptionInit(blackwidow::BlackwidowOptions* bw_option) {
  bw_option->options.create_if_missing = true;
  bw_option->options.keep_log_file_num = 10;
  bw_option->options.max_manifest_file_size = 64 * 1024 * 1024;
  bw_option->options.max_log_file_size = 512 * 1024 * 1024;

  bw_option->options.write_buffer_size = g_pika_conf->write_buffer_size();
  bw_option->options.target_file_size_base = g_pika_conf->target_file_size_base();
  bw_option->options.max_background_flushes = g_pika_conf->max_background_flushes();
  bw_option->options.max_background_compactions = g_pika_conf->max_background_compactions();
  bw_option->options.max_open_files = g_pika_conf->max_cache_files();
  bw_option->options.max_bytes_for_level_multiplier = g_pika_conf->max_bytes_for_level_multiplier();
  bw_option->options.optimize_filters_for_hits = g_pika_conf->optimize_filters_for_hits();
  bw_option->options.level_compaction_dynamic_level_bytes = g_pika_conf->level_compaction_dynamic_level_bytes();

  if (g_pika_conf->compression() == "none") {
    bw_option->options.compression = rocksdb::CompressionType::kNoCompression;
  } else if (g_pika_conf->compression() == "snappy") {
    bw_option->options.compression = rocksdb::CompressionType::kSnappyCompression;
  } else if (g_pika_conf->compression() == "zlib") {
    bw_option->options.compression = rocksdb::CompressionType::kZlibCompression;
  }

  bw_option->table_options.block_size = g_pika_conf->block_size();
  bw_option->table_options.cache_index_and_filter_blocks = g_pika_conf->cache_index_and_filter_blocks();
  bw_option->block_cache_size = g_pika_conf->block_cache();
  bw_option->share_block_cache = g_pika_conf->share_block_cache();
  bw_option->statistics_max_size = g_pika_conf->max_cache_statistic_keys();
  bw_option->small_compaction_threshold = g_pika_conf->small_compaction_threshold();
}

void PikaServer::Start() {
  int ret = 0;
  ret = pika_thread_pool_->start_thread_pool();
  if (ret != pink::kSuccess) {
    delete logger_;
    db_.reset();
    LOG(FATAL) << "Start ThreadPool Error: " << ret << (ret == pink::kCreateThreadError ? ": create thread error " : ": other error");
  }
  ret = pika_dispatch_thread_->StartThread();
  if (ret != pink::kSuccess) {
    delete logger_;
    db_.reset();
    LOG(FATAL) << "Start Dispatch Error: " << ret << (ret == pink::kBindError ? ": bind port " + std::to_string(port_) + " conflict"
            : ": other error") << ", Listen on this port to handle the connected redis client";
  }
  ret = pika_heartbeat_thread_->StartThread();
  if (ret != pink::kSuccess) {
    delete logger_;
    db_.reset();
    LOG(FATAL) << "Start Heartbeat Error: " << ret << (ret == pink::kBindError ? ": bind port " + std::to_string(port_ + 2000) + " conflict"
            : ": other error") << ", Listen on this port to receive the heartbeat packets sent by the master";
  }
  ret = pika_pubsub_thread_->StartThread();
  if (ret != pink::kSuccess) {
    delete logger_;
    db_.reset();
    LOG(FATAL) << "Start Pubsub Error: " << ret << (ret == pink::kBindError ? ": bind port conflict" : ": other error");
  }

  ret = pika_repl_client_->Start();
  if (ret != pink::kSuccess) {
    delete logger_;
    db_.reset();
    LOG(FATAL) << "Start Repl Client Error: " << ret << (ret == pink::kCreateThreadError ? ": create thread error " : ": other error");
  }

  ret = pika_repl_server_->Start();
  if (ret != pink::kSuccess) {
    delete logger_;
    db_.reset();
    LOG(FATAL) << "Start Repl Server Error: " << ret << (ret == pink::kCreateThreadError ? ": create thread error " : ": other error");
  }

  ret = pika_auxiliary_thread_->StartThread();
  if (ret != pink::kSuccess) {
    delete logger_;
    db_.reset();
    LOG(FATAL) << "Start Auxiliary Thread Error: " << ret << (ret == pink::kCreateThreadError ? ": create thread error " : ": other error");
  }

  ret = slash::StartRsync(g_pika_conf->db_sync_path(), kDBSyncModule, host_, g_pika_conf->port() + 4000);
  if (0 != ret) {
    delete logger_;
    db_.reset();
    LOG(FATAL) << "Start Rsync Error Path:" << g_pika_conf->db_sync_path() << " error : " << ret;
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

void PikaServer::InitTableStruct() {
  std::string db_path = g_pika_conf->db_path();
  std::string log_path = g_pika_conf->log_path();
  std::string trash_path = g_pika_conf->trash_path();
  std::vector<TableStruct> table_structs = g_pika_conf->table_structs();
  for (const auto& table : table_structs) {
    std::string name = table.table_name;
    uint32_t num = table.partition_num;
    tables_.emplace(name, std::shared_ptr<Table>(
                new Table(name, num, db_path, log_path, trash_path)));
  }
}

bool PikaServer::RebuildTableStruct(const std::vector<TableStruct>& table_structs) {
  if (IsKeyScaning()) {
    LOG(WARNING) << "Some table in key scaning, rebuild table struct failed";
    return false;
  }

  if (IsBgSaving()) {
    LOG(WARNING) << "Some table in bgsaving, rebuild table struct failed";
    return false;
  }

  {
    slash::RWLock rwl(&tables_rw_, true);
    for (const auto& table_item : tables_) {
      table_item.second->LeaveAllPartition();
    }
    tables_.clear();
  }

  // TODO(Axlgrep): The new table structure needs to be written back
  // to the pika.conf file
  g_pika_conf->SetTableStructs(table_structs);
  InitTableStruct();
  return true;
}

std::shared_ptr<Table> PikaServer::GetTable(const std::string &table_name) {
  slash::RWLock l(&tables_rw_, false);
  auto iter = tables_.find(table_name);
  return (iter == tables_.end()) ? NULL : iter->second;
}

bool PikaServer::IsCommandCurrentSupport(const std::string& command) {
  std::string cmd = command;
  slash::StringToLower(cmd);
  return CurrentNotSupportCommands.find(cmd) == CurrentNotSupportCommands.end();
}

bool PikaServer::IsTableBinlogIoError(const std::string& table_name) {
  std::shared_ptr<Table> table = GetTable(table_name);
  return table ? table->IsBinlogIoError() : true;
}

bool PikaServer::IsBgSaving() {
  slash::RWLock table_rwl(&tables_rw_, false);
  for (const auto& table_item : tables_) {
    slash::RWLock partition_rwl(&table_item.second->partitions_rw_, false);
    for (const auto& patition_item : table_item.second->partitions_) {
      if (patition_item.second->IsBgSaving()) {
        return true;
      }
    }
  }
  return false;
}

bool PikaServer::IsKeyScaning() {
  slash::RWLock table_rwl(&tables_rw_, false);
  for (const auto& table_item : tables_) {
    if (table_item.second->IsKeyScaning()) {
      return true;
    }
  }
  return false;
}

bool PikaServer::IsCompacting() {
  slash::RWLock table_rwl(&tables_rw_, false);
  for (const auto& table_item : tables_) {
    slash::RWLock partition_rwl(&table_item.second->partitions_rw_, false);
    for (const auto& patition_item : table_item.second->partitions_) {
      if (strcasecmp(patition_item.second->db()->GetCurrentTaskType().data(), "no")) {
        return true;
      }
    }
  }
  return false;
}

bool PikaServer::IsTableExist(const std::string& table_name) {
  return GetTable(table_name) ? true : false;
}

bool PikaServer::IsCommandSupport(const std::string& command) {
  if (g_pika_conf->classic_mode()) {
    return true;
  } else {
    std::string cmd = command;
    slash::StringToLower(cmd);
    return !ShardingModeNotSupportCommands.count(cmd);
  }
}

uint32_t PikaServer::GetPartitionNumByTable(const std::string& table_name) {
  std::shared_ptr<Table> table = GetTable(table_name);
  return table ? table->PartitionNum() : 0;
}

bool PikaServer::GetTablePartitionBinlogOffset(const std::string& table_name,
                                               uint32_t partition_id,
                                               BinlogOffset* const boffset) {
  std::shared_ptr<Partition> partition = GetTablePartitionById(table_name, partition_id);
  if (!partition) {
    return false;
  } else {
    return partition->GetBinlogOffset(boffset);
  }
}

void PikaServer::PreparePartitionTrySync() {
  slash::RWLock rwl(&tables_rw_, false);
  for (const auto& table_item : tables_) {
    for (const auto& partition_item : table_item.second->partitions_) {
      partition_item.second->SetFullSync(force_full_sync_);
      partition_item.second->MarkTryConnectState();
    }
  }
  MarkTryConnectDone();
  force_full_sync_ = false;
}

std::shared_ptr<Partition> PikaServer::GetTablePartitionById(
                                    const std::string& table_name,
                                    uint32_t partition_id) {
  std::shared_ptr<Table> table = GetTable(table_name);
  return table ? table->GetPartitionById(partition_id) : NULL;
}

std::shared_ptr<Partition> PikaServer::GetTablePartitionByKey(
                                    const std::string& table_name,
                                    const std::string& key) {
  std::shared_ptr<Table> table = GetTable(table_name);
  return table ? table->GetPartitionByKey(key) : NULL;
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

  slave_num = slaves_.size();
  {
  slash::RWLock l(&state_protector_, true);
  if (slave_num == 0) {
    role_ &= ~PIKA_ROLE_MASTER;
  }
  }

  if (iter == slaves_.end()) {
    return;
  }
  if (iter->sender != NULL) {
    delete static_cast<PikaBinlogSenderThread*>(iter->sender);
  }
  RmNode slave(iter->table, iter->partition_id, ip, port);
  pika_repl_client_->RemoveBinlogReader(slave);
  slaves_.erase(iter);
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
      std::string ip;
      int port;
      bool res =  slash::ParseIpPortString(iter->ip_port, ip, port);
      if (!res) {
        LOG(WARNING) << "Parse ip port error " << iter->ip_port;
        slaves_.erase(iter);
        LOG(INFO) << "Delete slave success";
        break;
      }
      RmNode slave(iter->table, iter->partition_id, ip, port);
      pika_repl_client_->RemoveBinlogReader(slave);
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
  }
}

/*
 * Change a new db locate in new_path
 * return true when change success
 * db remain the old one if return false
 */
bool PikaServer::ChangeDb(const std::string& new_path) {

  blackwidow::BlackwidowOptions bw_option;
  RocksdbOptionInit(&bw_option);

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
  rocksdb::Status s = db_->Open(bw_option, db_path);
  assert(db_);
  assert(s.ok());
  slash::DeleteDirIfExist(tmp_path);
  LOG(INFO) << "Change db success";
  return true;
}

void PikaServer::TryDBSync(const std::string& ip, int port,
                           const std::string& table_name,
                           uint32_t partition_id, int32_t top) {
  std::shared_ptr<Partition> partition =
    GetTablePartitionById(table_name, partition_id);
  if (!partition) {
    LOG(WARNING) << "Partition: " << partition->GetPartitionName()
      << " Not Found, TryDBSync Failed";
  } else {
    BgSaveInfo bgsave_info = partition->bgsave_info();
    std::string logger_filename = partition->logger()->filename;
    if (slash::IsDir(bgsave_info.path) != 0
      || !slash::FileExists(NewFileName(logger_filename, bgsave_info.filenum))
      || top - bgsave_info.filenum > kDBSyncMaxGap) {
      // Need Bgsave first
      partition->BgSavePartition();
    }
    DBSync(ip, port, table_name, partition_id);
  }
}

std::string PikaServer::DbSyncTaskIndex(const std::string& ip,
                                        int port,
                                        const std::string& table_name,
                                        uint32_t partition_id) {
  char buf[256];
  snprintf(buf, sizeof(buf), "%s:%d_%s:%d",
      ip.data(), port, table_name.data(), partition_id);
  return buf;
}

void PikaServer::DBSync(const std::string& ip, int port,
                        const std::string& table_name,
                        uint32_t partition_id) {
  {
    std::string task_index =
      DbSyncTaskIndex(ip, port, table_name, partition_id);
    slash::MutexLock ml(&db_sync_protector_);
    if (db_sync_slaves_.find(task_index) != db_sync_slaves_.end()) {
      return;
    }
    db_sync_slaves_.insert(task_index);
  }
  // Reuse the bgsave_thread_
  // Since we expect BgSave and DBSync execute serially
  bgsave_thread_.StartThread();
  NewDBSyncArg* arg = new NewDBSyncArg(this, ip, port, table_name, partition_id);
  bgsave_thread_.Schedule(&NewDoDbSync, reinterpret_cast<void*>(arg));
}

void PikaServer::NewDoDbSync(void* arg) {
  NewDBSyncArg* dbsa = reinterpret_cast<NewDBSyncArg*>(arg);
  PikaServer* const ps = dbsa->p;
  ps->NewDbSyncSendFile(dbsa->ip, dbsa->port,
      dbsa->table_name, dbsa->partition_id);
  delete dbsa;
}

void PikaServer::NewDbSyncSendFile(const std::string& ip, int port,
                                   const std::string& table_name,
                                   uint32_t partition_id) {
  std::shared_ptr<Partition> partition = GetTablePartitionById(table_name, partition_id);
  if (!partition) {
    LOG(WARNING) << "Partition: " << partition->GetPartitionName()
      << " Not Found, DbSync send file Failed";
    return;
  }

  std::string bg_path;
  BgSaveInfo bgsave_info = partition->bgsave_info();
  bg_path = bgsave_info.path;

  // Get all files need to send
  std::vector<std::string> descendant;
  int ret = 0;
  LOG(INFO) << "Start Send files in " << bg_path << " to " << ip;
  ret = slash::GetChildren(bg_path, descendant);
  if (ret != 0) {
    std::string ip_port = slash::IpPortString(ip, port);
    slash::MutexLock ldb(&db_sync_protector_);
    db_sync_slaves_.erase(ip_port);
    LOG(WARNING) << "Get child directory when try to do sync failed, error: " << strerror(ret);
    return;
  }

  std::string local_path, target_path;
  std::string remote_path = g_pika_conf->classic_mode() ? table_name : table_name + "/" + std::to_string(partition_id);
  std::vector<std::string>::const_iterator iter = descendant.begin();
  slash::RsyncRemote remote(ip, port, kDBSyncModule, g_pika_conf->db_sync_speed() * 1024);
  for (; iter != descendant.end(); ++iter) {
    local_path = bg_path + "/" + *iter;
    target_path = remote_path + "/" + *iter;

    if (*iter == kBgsaveInfoFile) {
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
      LOG(WARNING) << "rsync send file failed! From: " << *iter
        << ", To: " << target_path
        << ", At: " << ip << ":" << port
        << ", Error: " << ret;
      break;
    }
  }

  // Clear target path
  slash::RsyncSendClearTarget(bg_path + "/strings", remote_path + "/strings", remote);
  slash::RsyncSendClearTarget(bg_path + "/hashes", remote_path + "/hashes", remote);
  slash::RsyncSendClearTarget(bg_path + "/lists", remote_path + "/lists", remote);
  slash::RsyncSendClearTarget(bg_path + "/sets", remote_path + "/sets", remote);
  slash::RsyncSendClearTarget(bg_path + "/zsets", remote_path + "/zsets", remote);

  // Send info file at last
  if (0 == ret) {
    if (0 != (ret = slash::RsyncSendFile(bg_path + "/" + kBgsaveInfoFile, remote_path + "/" + kBgsaveInfoFile, remote))) {
      LOG(WARNING) << "send info file failed";
    }
  }

  // remove slave
  {
    std::string task_index =
      DbSyncTaskIndex(ip, port, table_name, partition_id);
    slash::MutexLock ml(&db_sync_protector_);
    db_sync_slaves_.erase(task_index);
  }

  if (0 == ret) {
    LOG(INFO) << "rsync send files success";
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
      break;
    }
    iter++;
  }
}

// Try add Slave, return slave sid if success,
// return -1 when slave already exist
int64_t PikaServer::TryAddSlave(const std::string& ip, int64_t port, const std::string& table, uint32_t partition_id) {
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
  s.table = table;
  s.partition_id = partition_id;
  s.sid = GenSid();
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
  role_ |= PIKA_ROLE_MASTER;
}

bool PikaServer::SetMaster(std::string& master_ip, int master_port) {
  if (master_ip == "127.0.0.1") {
    master_ip = host_;
  }
  slash::RWLock l(&state_protector_, true);
  if ((role_ ^ PIKA_ROLE_SLAVE) && repl_state_ == PIKA_REPL_NO_CONNECT) {
    master_ip_ = master_ip;
    master_port_ = master_port;
    role_ |= PIKA_ROLE_SLAVE;
    repl_state_ = PIKA_REPL_SHOULD_META_SYNC;
    return true;
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

bool PikaServer::ShouldMetaSync() {
  slash::RWLock l(&state_protector_, false);
  return repl_state_ == PIKA_REPL_SHOULD_META_SYNC;
}

void PikaServer::MetaSyncDone() {
  slash::RWLock l(&state_protector_, true);
  assert(repl_state_ == PIKA_REPL_WAIT_META_SYNC_RESPONSE);
  repl_state_ = PIKA_REPL_SHOULD_MARK_TRY_CONNECT;
}

bool PikaServer::ShouldMarkTryConnect() {
  slash::RWLock l(&state_protector_, false);
  return repl_state_ == PIKA_REPL_SHOULD_MARK_TRY_CONNECT;
}

void PikaServer::MarkTryConnectDone() {
  slash::RWLock l(&state_protector_, true);
  assert(repl_state_ == PIKA_REPL_SHOULD_MARK_TRY_CONNECT);
  repl_state_ = PIKA_REPL_CONNECTING;
}

bool PikaServer::ShouldTrySyncPartition() {
  slash::RWLock l(&state_protector_, false);
  return repl_state_ == PIKA_REPL_CONNECTING;
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
  slash::RWLock l(&state_protector_, true);
  repl_state_ = PIKA_REPL_ERROR;
  LOG(WARNING) << "Sync error, set repl_state to PIKA_REPL_ERROR";
}

void PikaServer::RemoveMaster() {
  {
  slash::RWLock l(&state_protector_, true);
  repl_state_ = PIKA_REPL_NO_CONNECT;
  role_ &= ~PIKA_ROLE_SLAVE;

  master_ip_ = "";
  master_port_ = -1;
  }
  if (ping_thread_ != NULL) {
    int err = ping_thread_->StopThread();
    if (err != 0) {
      std::string msg = "Can't join thread " + std::string(strerror(err));
      LOG(WARNING) << msg;
    }
    delete ping_thread_;
    ping_thread_ = NULL;
  }
  {
  slash::RWLock l(&state_protector_, true);
  master_connection_ = 0;
  }
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
  {
    slash::MutexLock ldb(&db_sync_protector_);
    std::string ip_port = slash::IpPortString(ip, port);
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
    std::string ip_port = slash::IpPortString(ip, port);
    slash::MutexLock ldb(&db_sync_protector_);
    db_sync_slaves_.erase(ip_port);
    LOG(WARNING) << "Get child directory when try to do sync failed, error: " << strerror(ret);
    return;
  }

  // Iterate to send files
  ret = 0;
  std::string local_path, target_path;
  pink::PinkCli *cli = pink::NewRedisCli();
  std::string lip(host_);
  if (cli->Connect(ip, port, "").ok()) {
    struct sockaddr_in laddr;
    socklen_t llen = sizeof(laddr);
    getsockname(cli->fd(), (struct sockaddr*) &laddr, &llen);
    lip = inet_ntoa(laddr.sin_addr);
    cli->Close();
  }
  std::string module = kDBSyncModule + "_" + slash::IpPortString(lip, port_);
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
    // need to modify the IP addr in the info file
    if (lip.compare(host_) != 0) {
      std::ofstream fix;
      std::string fn = bg_path + "/" + kBgsaveInfoFile + "." + std::to_string(time(NULL));
      fix.open(fn, std::ios::in | std::ios::trunc);
      if (fix.is_open()) {
        fix << "0s\n" << lip << "\n" << port_ << "\n" << binlog_filenum << "\n" << binlog_offset << "\n";
        fix.close();
      }
      ret = slash::RsyncSendFile(fn, kBgsaveInfoFile, remote);
      slash::DeleteFile(fn);
      if (ret != 0) {
        LOG(WARNING) << "send modified info file failed";
      }
    } else if (0 != (ret = slash::RsyncSendFile(bg_path + "/" + kBgsaveInfoFile, kBgsaveInfoFile, remote))) {
      LOG(WARNING) << "send info file failed";
    }
  }

  // remove slave
  {
    std::string ip_port = slash::IpPortString(ip, port);
    slash::MutexLock ldb(&db_sync_protector_);
    db_sync_slaves_.erase(ip_port);
  }
  if (0 == ret) {
    LOG(INFO) << "rsync send files success";
  }
}

Status PikaServer::AddBinlogSender(const std::string& table_name,
                                   uint32_t partition_id,
                                   const std::string& ip,
                                   int64_t port,
                                   int64_t sid,
                                   uint32_t filenum, uint64_t con_offset) {
  // shift 3000 to connect repl sserver
  RmNode slave(table_name, partition_id, ip, port + 3000);
  std::shared_ptr<Partition> partition = GetTablePartitionById(table_name, partition_id);
  std::shared_ptr<Binlog>logger = partition->logger();
  Status res = pika_repl_client_->AddBinlogReader(slave, logger, filenum, con_offset);
  if (!res.ok()) {
    return res;
  }
  return pika_repl_client_->SendBinlogSync(slave);
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
  out.open(info.path + "/" + kBgsaveInfoFile, std::ios::in | std::ios::trunc);
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

void PikaServer::BGSaveTaskSchedule(void (*function)(void*), void* arg) {
  slash::MutexLock l(&bgsave_thread_protector_);
  bgsave_thread_.StartThread();
  bgsave_thread_.Schedule(function, arg);
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

void PikaServer::PurgelogsTaskSchedule(void (*function)(void*), void* arg) {
  purge_thread_.StartThread();
  purge_thread_.Schedule(function, arg);
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
  std::string ci = g_pika_conf->compact_interval();
  std::string cc = g_pika_conf->compact_cron();

  if (ci != "") {
    std::string::size_type slash = ci.find("/");
    int interval = std::atoi(ci.substr(0, slash).c_str());
    int usage = std::atoi(ci.substr(slash+1).c_str());
    struct timeval now;
    gettimeofday(&now, NULL);
    if (last_check_compact_time_.tv_sec == 0 ||
      now.tv_sec - last_check_compact_time_.tv_sec >= interval * 3600) {
      gettimeofday(&last_check_compact_time_, NULL);
      if (((double)free_size / total_size) * 100 >= usage) {
        Status s = DoSameThingEveryPartition(TaskType::kCompactAll);
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
    bool have_week = false;
    std::string compact_cron, week_str;
    int slash_num = count(cc.begin(), cc.end(), '/');
    if (slash_num == 2) {
      have_week = true;
      std::string::size_type first_slash = cc.find("/");
      week_str = cc.substr(0, first_slash);
      compact_cron = cc.substr(first_slash + 1);
    } else {
      compact_cron = cc;
    }

    std::string::size_type colon = compact_cron.find("-");
    std::string::size_type underline = compact_cron.find("/");
    int week = have_week ? (std::atoi(week_str.c_str()) % 7) : 0;
    int start = std::atoi(compact_cron.substr(0, colon).c_str());
    int end = std::atoi(compact_cron.substr(colon+1, underline).c_str());
    int usage = std::atoi(compact_cron.substr(underline+1).c_str());
    std::time_t t = std::time(nullptr);
    std::tm* t_m = std::localtime(&t);

    bool in_window = false;
    if (start < end && (t_m->tm_hour >= start && t_m->tm_hour < end)) {
      in_window = have_week ? (week == t_m->tm_wday) : true;
    } else if (start > end && ((t_m->tm_hour >= start && t_m->tm_hour < 24) ||
          (t_m->tm_hour >= 0 && t_m->tm_hour < end))) {
      in_window = have_week ? false : true;
    } else {
      have_scheduled_crontask_ = false;
    }

    if (!have_scheduled_crontask_ && in_window) {
      if (((double)free_size / total_size) * 100 >= usage) {
        Status s = DoSameThingEveryPartition(TaskType::kCompactAll);
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
  DoSameThingEveryPartition(TaskType::kPurgeLog);
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

void PikaServer::PurgeDir(const std::string& path) {
  std::string* dir_path = new std::string(path);
  PurgeDirTaskSchedule(&DoPurgeDir, static_cast<void*>(dir_path));
}

void PikaServer::PurgeDirTaskSchedule(void (*function)(void*), void* arg) {
  purge_thread_.StartThread();
  purge_thread_.Schedule(function, arg);
}

// Publish
int PikaServer::Publish(const std::string& channel, const std::string& msg) {
  int receivers = pika_pubsub_thread_->Publish(channel, msg);
  return receivers;
}

// Subscribe/PSubscribe
void PikaServer::Subscribe(std::shared_ptr<pink::PinkConn> conn,
                           const std::vector<std::string>& channels,
                           bool pattern,
                           std::vector<std::pair<std::string, int>>* result) {
  pika_pubsub_thread_->Subscribe(conn, channels, pattern, result);
}

// UnSubscribe/PUnSubscribe
int PikaServer::UnSubscribe(std::shared_ptr<pink::PinkConn> conn,
                            const std::vector<std::string>& channels,
                            bool pattern,
                            std::vector<std::pair<std::string, int>>* result) {
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

Status PikaServer::SendMetaSyncRequest() {
  Status status = pika_repl_client_->SendMetaSync();
  slash::RWLock l(&state_protector_, true);
  repl_state_ = PIKA_REPL_WAIT_META_SYNC_RESPONSE;
  return status;
}

Status PikaServer::SendPartitionTrySyncRequest(std::shared_ptr<Partition> partition) {
  BinlogOffset boffset;
  if (!partition->GetBinlogOffset(&boffset)) {
    LOG(WARNING) << "Get partition binlog offset failed";
    return Status::Corruption("Partition get binlog offset error");
  }
  std::string table_name = partition->GetTableName();
  uint32_t partition_id = partition->GetPartitionId();
  bool force = partition->FullSync();
  partition->PrepareRsync();
  Status status = pika_repl_client_->SendPartitionTrySync(table_name, partition_id, boffset, force);
  partition->MarkWaitReplyState();
  return status;
}

Status PikaServer::SendBinlogSyncRequest(const std::string& table, uint32_t partition, const std::string& ip, int port) {
  RmNode slave = RmNode(table, partition, ip, port);
  return pika_repl_client_->SendBinlogSync(slave);
}

void PikaServer::AddMonitorClient(std::shared_ptr<PikaClientConn> client_ptr) {
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

void PikaServer::ScheduleReplBinlogSyncTask(std::string table_partition,
    const std::shared_ptr<InnerMessage::InnerRequest> req,
    std::shared_ptr<pink::PbConn> conn,
    void* req_private_data) {
  pika_repl_server_->ScheduleBinlogSyncTask(table_partition, req, conn, req_private_data);
}

void PikaServer::ScheduleReplMetaSyncTask(const std::shared_ptr<InnerMessage::InnerRequest> req,
    std::shared_ptr<pink::PbConn> conn,
    void* req_private_data) {
  pika_repl_server_->ScheduleMetaSyncTask(req, conn, req_private_data);
}

void PikaServer::ScheduleReplTrySyncTask(const std::shared_ptr<InnerMessage::InnerRequest> req,
    std::shared_ptr<pink::PbConn> conn,
    void* req_private_data) {
  pika_repl_server_->ScheduleTrySyncTask(req, conn, req_private_data);
}

void PikaServer::ScheduleReplDbTask(const std::string &key,
    PikaCmdArgsType* argv, BinlogItem* binlog_item,
    const std::string& table_name, uint32_t partition_id) {
  pika_repl_server_->ScheduleDbTask(key, argv, binlog_item, table_name, partition_id);
}

bool PikaServer::SetBinlogAckInfo(const std::string& table, uint32_t partition, const std::string& ip, int port,
    uint32_t ack_file_num, uint64_t ack_offset, uint64_t active_time) {
  RmNode slave = RmNode(table, partition, ip, port);
  return pika_repl_client_->SetAckInfo(slave, ack_file_num, ack_offset, active_time);
}

bool PikaServer::GetBinlogAckInfo(const std::string& table, uint32_t partition, const std::string& ip, int port,
    uint32_t* ack_file_num, uint64_t* ack_offset, uint64_t* active_time) {
  RmNode slave = RmNode(table, partition, ip, port);
  return pika_repl_client_->GetAckInfo(slave, ack_file_num, ack_offset, active_time);
}

int PikaServer::SendToPeer() {
  return pika_repl_client_->ConsumeWriteQueue();
}

Status PikaServer::TriggerSendBinlogSync() {
  return pika_repl_client_->TriggerSendBinlogSync();
}

void PikaServer::SignalAuxiliary() {
  pika_auxiliary_thread_->mu_.Lock();
  pika_auxiliary_thread_->cv_.Signal();
  pika_auxiliary_thread_->mu_.Unlock();
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

void PikaServer::SlowlogTrim() {
  pthread_rwlock_wrlock(&slowlog_protector_);
  while (slowlog_list_.size() > static_cast<uint32_t>(g_pika_conf->slowlog_max_len())) {
    slowlog_list_.pop_back();
  }
  pthread_rwlock_unlock(&slowlog_protector_);
}

void PikaServer::SlowlogReset() {
  pthread_rwlock_wrlock(&slowlog_protector_);
  slowlog_list_.clear();
  pthread_rwlock_unlock(&slowlog_protector_);
}

uint32_t PikaServer::SlowlogLen() {
  RWLock l(&slowlog_protector_, false);
  return slowlog_list_.size();
}

void PikaServer::SlowlogObtain(int64_t number, std::vector<SlowlogEntry>* slowlogs) {
  pthread_rwlock_rdlock(&slowlog_protector_);
  slowlogs->clear();
  std::list<SlowlogEntry>::const_iterator iter = slowlog_list_.begin();
  while (number-- && iter != slowlog_list_.end()) {
    slowlogs->push_back(*iter);
    iter++;
  }
  pthread_rwlock_unlock(&slowlog_protector_);
}

void PikaServer::SlowlogPushEntry(const PikaCmdArgsType& argv, int32_t time, int64_t duration) {
  SlowlogEntry entry;
  uint32_t slargc = (argv.size() < SLOWLOG_ENTRY_MAX_ARGC)
      ? argv.size() : SLOWLOG_ENTRY_MAX_ARGC;

  for (uint32_t idx = 0; idx < slargc; ++idx) {
    if (slargc != argv.size() && idx == slargc - 1) {
      char buffer[32];
      sprintf(buffer, "... (%lu more arguments)", argv.size() - slargc + 1);
      entry.argv.push_back(std::string(buffer));
    } else {
      if (argv[idx].size() > SLOWLOG_ENTRY_MAX_STRING) {
        char buffer[32];
        sprintf(buffer, "... (%lu more bytes)", argv[idx].size() - SLOWLOG_ENTRY_MAX_STRING);
        std::string suffix(buffer);
        std::string brief = argv[idx].substr(0, SLOWLOG_ENTRY_MAX_STRING);
        entry.argv.push_back(brief + suffix);
      } else {
        entry.argv.push_back(argv[idx]);
      }
    }
  }

  pthread_rwlock_wrlock(&slowlog_protector_);
  entry.id = slowlog_entry_id_++;
  entry.start_time = time;
  entry.duration = duration;
  slowlog_list_.push_front(entry);
  pthread_rwlock_unlock(&slowlog_protector_);

  SlowlogTrim();
}

Status PikaServer::DoSameThingEveryTable(const TaskType& type) {
  slash::RWLock rwl(&tables_rw_, false);
  for (const auto& table_item : tables_) {
    switch (type) {
      case TaskType::kStartKeyScan:
        table_item.second->KeyScan();
        break;
      case TaskType::kStopKeyScan:
        table_item.second->StopKeyScan();
        break;
      case TaskType::kBgSave:
        table_item.second->BgSaveTable();
        break;
      default:
        break;
    }
  }
  return Status::OK();
}

Status PikaServer::DoSameThingEveryPartition(const TaskType& type) {
  slash::RWLock rwl(&tables_rw_, false);
  for (const auto& table_item : tables_) {
    for (const auto& partition_item : table_item.second->partitions_) {
      switch (type) {
        case TaskType::kCompactAll:
          partition_item.second->Compact(blackwidow::DataType::kAll);
          break;
        case TaskType::kCompactStrings:
          partition_item.second->Compact(blackwidow::DataType::kStrings);
          break;
        case TaskType::kCompactHashes:
          partition_item.second->Compact(blackwidow::DataType::kHashes);
          break;
        case TaskType::kCompactSets:
          partition_item.second->Compact(blackwidow::DataType::kSets);
          break;
        case TaskType::kCompactZSets:
          partition_item.second->Compact(blackwidow::DataType::kZSets);
          break;
        case TaskType::kCompactList:
          partition_item.second->Compact(blackwidow::DataType::kLists);
          break;
        case TaskType::kPurgeLog:
          partition_item.second->PurgeLogs(0, false, false);
          break;
        default:
          break;
      }
    }
  }
  return Status::OK();
}

void PikaServer::KeyScanWholeTable(const std::string& table_name) {
  std::shared_ptr<Table> table = GetTable(table_name);
  if (!table) {
    LOG(WARNING) << "table : " << table_name << " not exist, key scan failed!";
    return;
  }
  table->KeyScan();
}

void PikaServer::StopKeyScanWholeTable(const std::string& table_name) {
  std::shared_ptr<Table> table = GetTable(table_name);
  if (!table) {
    LOG(WARNING) << "table : " << table_name << " not exist, stop key scan failed!";
    return;
  }
  table->StopKeyScan();
}

void PikaServer::KeyScanTaskSchedule(void (*function)(void*), void* arg) {
  key_scan_thread_.StartThread();
  key_scan_thread_.Schedule(function, arg);
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

void PikaServer::UpdateQueryNumAndExecCountTable(const std::string& command) {
  std::string cmd(command);
  statistic_data_.statistic_lock.WriteLock();
  statistic_data_.thread_querynum++;
  statistic_data_.exec_count_table[slash::StringToUpper(cmd)]++;
  statistic_data_.statistic_lock.WriteUnlock();
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

std::unordered_map<std::string, uint64_t> PikaServer::ServerExecCountTable() {
  slash::ReadLock l(&statistic_data_.statistic_lock);
  return statistic_data_.exec_count_table;
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
}
