// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/resource.h>
#include <algorithm>
#include <ctime>
#include <fstream>
#include <memory>
#include <utility>
#include "net/include/net_cli.h"
#include "net/include/net_interfaces.h"
#include "net/include/net_stats.h"
#include "net/include/redis_cli.h"
#include "pstd/include/env.h"
#include "pstd/include/rsync.h"

#include "include/pika_cmd_table_manager.h"
#include "include/pika_dispatch_thread.h"
#include "include/pika_instant.h"
#include "include/pika_monotonic_time.h"
#include "include/pika_rm.h"
#include "include/pika_server.h"

using pstd::Status;
extern PikaServer* g_pika_server;
extern std::unique_ptr<PikaReplicaManager> g_pika_rm;
extern std::unique_ptr<PikaCmdTableManager> g_pika_cmd_table_manager;
extern std::unique_ptr<net::NetworkStatistic> g_network_statistic;
// QUEUE_SIZE_THRESHOLD_PERCENTAGE is used to represent a percentage value and should be within the range of 0 to 100.
const size_t QUEUE_SIZE_THRESHOLD_PERCENTAGE = 75;

void DoPurgeDir(void* arg) {
  std::unique_ptr<std::string> path(static_cast<std::string*>(arg));
  LOG(INFO) << "Delete dir: " << *path << " start";
  pstd::DeleteDir(*path);
  LOG(INFO) << "Delete dir: " << *path << " done";
}

void DoDBSync(void* arg) {
  std::unique_ptr<DBSyncArg> dbsa(static_cast<DBSyncArg*>(arg));
  PikaServer* const ps = dbsa->p;
  ps->DbSyncSendFile(dbsa->ip, dbsa->port, dbsa->db_name);
}

PikaServer::PikaServer()
    : exit_(false),
      last_check_compact_time_({0, 0}),
      last_check_resume_time_({0, 0}),
      repl_state_(PIKA_REPL_NO_CONNECT),
      role_(PIKA_ROLE_SINGLE) {
  // Init server ip host
  if (!ServerInit()) {
    LOG(FATAL) << "ServerInit iotcl error";
  }

  InitStorageOptions();

  // Create thread
  worker_num_ = std::min(g_pika_conf->thread_num(), PIKA_MAX_WORKER_THREAD_NUM);

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
  for_each(ips.begin(), ips.end(), [](auto& ip) { LOG(WARNING) << ip; });
  pika_dispatch_thread_ = std::make_unique<PikaDispatchThread>(ips, port_, worker_num_, 3000, worker_queue_limit,
                                                               g_pika_conf->max_conn_rbuf_size());
  pika_rsync_service_ =
      std::make_unique<PikaRsyncService>(g_pika_conf->db_sync_path(), g_pika_conf->port() + kPortShiftRSync);
  // TODO: remove pika_rsync_service_，reuse pika_rsync_service_ port
  rsync_server_ = std::make_unique<rsync::RsyncServer>(ips, port_ + kPortShiftRsync2);
  pika_pubsub_thread_ = std::make_unique<net::PubSubThread>();
  pika_auxiliary_thread_ = std::make_unique<PikaAuxiliaryThread>();
  pika_migrate_ = std::make_unique<PikaMigrate>();
  pika_migrate_thread_ = std::make_unique<PikaMigrateThread>();

  pika_client_processor_ = std::make_unique<PikaClientProcessor>(g_pika_conf->thread_pool_size(), 100000);
  pika_slow_cmd_thread_pool_ = std::make_unique<net::ThreadPool>(g_pika_conf->slow_cmd_thread_pool_size(), 100000);
  instant_ = std::make_unique<Instant>();
  exit_mutex_.lock();
  int64_t lastsave = GetLastSaveTime(g_pika_conf->bgsave_path());
  UpdateLastSave(lastsave);
  
  // init role
  std::string slaveof = g_pika_conf->slaveof();
  if (!slaveof.empty()) {
    auto sep = static_cast<int32_t>(slaveof.find(':'));
    std::string master_ip = slaveof.substr(0, sep);
    int32_t master_port = std::stoi(slaveof.substr(sep + 1));
    if ((master_ip == "127.0.0.1" || master_ip == host_) && master_port == port_) {
      LOG(FATAL) << "you will slaveof yourself as the config file, please check";
    } else {
      SetMaster(master_ip, master_port);
    }
  }
        
  acl_ = std::make_unique<::Acl>();
}

PikaServer::~PikaServer() {
  rsync_server_->Stop();
  // DispatchThread will use queue of worker thread,
  // so we need to delete dispatch before worker.
  pika_client_processor_->Stop();
  pika_slow_cmd_thread_pool_->stop_thread_pool();
  {
    std::lock_guard l(slave_mutex_);
    auto iter = slaves_.begin();
    while (iter != slaves_.end()) {
      iter = slaves_.erase(iter);
      LOG(INFO) << "Delete slave success";
    }
  }
  bgsave_thread_.StopThread();
  key_scan_thread_.StopThread();
  pika_migrate_thread_->StopThread();

  dbs_.clear();

  LOG(INFO) << "PikaServer " << pthread_self() << " exit!!!";
}

bool PikaServer::ServerInit() {
  std::string network_interface = g_pika_conf->network_interface();
  if (network_interface.empty()) {
    network_interface = GetDefaultInterface();
  }

  if (network_interface.empty()) {
    LOG(FATAL) << "Can't get Networker Interface";
    return false;
  }

  host_ = GetIpByInterface(network_interface);
  if (host_.empty()) {
    LOG(FATAL) << "can't get host ip for " << network_interface;
    return false;
  }

  port_ = g_pika_conf->port();
  LOG(INFO) << "host: " << host_ << " port: " << port_;
  return true;
}

void PikaServer::Start() {
  int ret = 0;
  // start rsync first, rocksdb opened fd will not appear in this fork
  // TODO: temporarily disable rsync server
  /*
  ret = pika_rsync_service_->StartRsync();
  if (0 != ret) {
    dbs_.clear();
    LOG(FATAL) << "Start Rsync Error: bind port " + std::to_string(pika_rsync_service_->ListenPort()) + " failed"
               << ", Listen on this port to receive Master FullSync Data";
  }
  */

  ret = pika_client_processor_->Start();
  if (ret != net::kSuccess) {
    dbs_.clear();
    LOG(FATAL) << "Start PikaClientProcessor Error: " << ret
               << (ret == net::kCreateThreadError ? ": create thread error " : ": other error");
  }
  ret = pika_slow_cmd_thread_pool_->start_thread_pool();
  if (ret != net::kSuccess) {
    dbs_.clear();
    LOG(FATAL) << "Start PikaLowLevelThreadPool Error: " << ret
               << (ret == net::kCreateThreadError ? ": create thread error " : ": other error");
  }
  ret = pika_dispatch_thread_->StartThread();
  if (ret != net::kSuccess) {
    dbs_.clear();
    LOG(FATAL) << "Start Dispatch Error: " << ret
               << (ret == net::kBindError ? ": bind port " + std::to_string(port_) + " conflict" : ": other error")
               << ", Listen on this port to handle the connected redis client";
  }
  ret = pika_pubsub_thread_->StartThread();
  if (ret != net::kSuccess) {
    dbs_.clear();
    LOG(FATAL) << "Start Pubsub Error: " << ret << (ret == net::kBindError ? ": bind port conflict" : ": other error");
  }

  ret = pika_auxiliary_thread_->StartThread();
  if (ret != net::kSuccess) {
    dbs_.clear();
    LOG(FATAL) << "Start Auxiliary Thread Error: " << ret
               << (ret == net::kCreateThreadError ? ": create thread error " : ": other error");
  }

  time(&start_time_s_);
  LOG(INFO) << "Pika Server going to start";
  rsync_server_->Start();
  while (!exit_) {
    DoTimingTask();
    // wake up every 5 seconds
    if (!exit_ && exit_mutex_.try_lock_for(std::chrono::seconds(5))) {
      exit_mutex_.unlock();
    }
  }
  LOG(INFO) << "Goodbye...";
}

void PikaServer::Exit() {
  exit_mutex_.unlock();
  exit_ = true;
}

std::string PikaServer::host() { return host_; }

int PikaServer::port() { return port_; }

time_t PikaServer::start_time_s() { return start_time_s_; }

std::string PikaServer::master_ip() {
  std::shared_lock l(state_protector_);
  return master_ip_;
}

int PikaServer::master_port() {
  std::shared_lock l(state_protector_);
  return master_port_;
}

int PikaServer::role() {
  std::shared_lock l(state_protector_);
  return role_;
}

bool PikaServer::leader_protected_mode() {
  std::shared_lock l(state_protector_);
  return leader_protected_mode_;
}

void PikaServer::CheckLeaderProtectedMode() {
  if (!leader_protected_mode()) {
    return;
  }
  if (g_pika_rm->CheckMasterSyncFinished()) {
    LOG(INFO) << "Master finish sync and commit binlog";

    std::lock_guard l(state_protector_);
    leader_protected_mode_ = false;
  }
}

bool PikaServer::readonly(const std::string& db_name) {
  std::shared_lock l(state_protector_);
  return ((role_ & PIKA_ROLE_SLAVE) != 0) && g_pika_conf->slave_read_only();
}

int PikaServer::repl_state() {
  std::shared_lock l(state_protector_);
  return repl_state_;
}

std::string PikaServer::repl_state_str() {
  std::shared_lock l(state_protector_);
  switch (repl_state_) {
    case PIKA_REPL_NO_CONNECT:
      return "no connect";
    case PIKA_REPL_SHOULD_META_SYNC:
      return "should meta sync";
    case PIKA_REPL_META_SYNC_DONE:
      return "meta sync done";
    case PIKA_REPL_ERROR:
      return "error";
    default:
      return "";
  }
}

bool PikaServer::force_full_sync() { return force_full_sync_; }

void PikaServer::SetForceFullSync(bool v) { force_full_sync_ = v; }

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
      LOG(WARNING) << "your 'limit -n ' of " << old_limit
                   << " is not enough for Redis to start. pika have successfully reconfig it to " << limit.rlim_cur;
    } else {
      LOG(FATAL) << "your 'limit -n ' of " << old_limit
                 << " is not enough for Redis to start. pika can not reconfig it(" << strerror(errno)
                 << "), do it by yourself";
    }
  }
  pika_dispatch_thread_->SetQueueLimit(queue_limit);
}

storage::StorageOptions PikaServer::storage_options() {
  std::shared_lock rwl(storage_options_rw_);
  return storage_options_;
}

void PikaServer::InitDBStruct() {
  std::string db_path = g_pika_conf->db_path();
  std::string log_path = g_pika_conf->log_path();
  std::vector<DBStruct> db_structs = g_pika_conf->db_structs();
  std::lock_guard rwl(dbs_rw_);
  for (const auto& db : db_structs) {
    std::string name = db.db_name;
    std::shared_ptr<DB> db_ptr = std::make_shared<DB>(name, db_path, log_path);
    db_ptr->Init();
    dbs_.emplace(name, db_ptr);
  }
}

std::shared_ptr<DB> PikaServer::GetDB(const std::string& db_name) {
  std::shared_lock l(dbs_rw_);
  auto iter = dbs_.find(db_name);
  return (iter == dbs_.end()) ? nullptr : iter->second;
}

bool PikaServer::IsBgSaving() {
  std::shared_lock l(dbs_rw_);
  for (const auto& db_item : dbs_) {
    if (db_item.second->IsBgSaving()) {
      return true;
    }
  }
  return false;
}

bool PikaServer::IsKeyScaning() {
  std::shared_lock l(dbs_rw_);
  for (const auto& db_item : dbs_) {
    if (db_item.second->IsKeyScaning()) {
      return true;
    }
  }
  return false;
}

bool PikaServer::IsCompacting() {
  std::shared_lock db_rwl(dbs_rw_);
  for (const auto& db_item : dbs_) {
    db_item.second->DbRWLockReader();
    std::string task_type = db_item.second->storage()->GetCurrentTaskType();
    db_item.second->DbRWUnLock();
    if (strcasecmp(task_type.data(), "no") != 0) {
      return true;
    }
  }
  return false;
}

bool PikaServer::IsDBExist(const std::string& db_name) { return static_cast<bool>(GetDB(db_name)); }

bool PikaServer::IsDBBinlogIoError(const std::string& db_name) {
  std::shared_ptr<DB> db = GetDB(db_name);
  return db ? db->IsBinlogIoError() : true;
}

std::set<std::string> PikaServer::GetAllDBName() {
  std::set<std::string> dbs;
  std::shared_lock l(dbs_rw_);
  for (const auto& db_item : dbs_) {
    dbs.insert(db_item.first);
  }
  return dbs;
}

Status PikaServer::DoSameThingSpecificDB(const std::set<std::string>& dbs, const TaskArg& arg) {
  std::shared_lock rwl(dbs_rw_);
  for (const auto& db_item : dbs_) {
    if (dbs.find(db_item.first) == dbs.end()) {
      continue;
    }
    switch (arg.type) {
      case TaskType::kCompactAll:
        db_item.second->Compact(storage::DataType::kAll);
        break;
      case TaskType::kCompactStrings:
        db_item.second->Compact(storage::DataType::kStrings);
        break;
      case TaskType::kCompactHashes:
        db_item.second->Compact(storage::DataType::kHashes);
        break;
      case TaskType::kCompactSets:
        db_item.second->Compact(storage::DataType::kSets);
        break;
      case TaskType::kCompactZSets:
        db_item.second->Compact(storage::DataType::kZSets);
        break;
      case TaskType::kCompactList:
        db_item.second->Compact(storage::DataType::kLists);
        break;
      case TaskType::kStartKeyScan:
        db_item.second->KeyScan();
        break;
      case TaskType::kStopKeyScan:
        db_item.second->StopKeyScan();
        break;
      case TaskType::kBgSave:
        db_item.second->BgSaveDB();
        break;
      case TaskType::kCompactRangeStrings:
        db_item.second->CompactRange(storage::DataType::kStrings, arg.argv[0], arg.argv[1]);
        break;
      case TaskType::kCompactRangeHashes:
        db_item.second->CompactRange(storage::DataType::kHashes, arg.argv[0], arg.argv[1]);
        break;
      case TaskType::kCompactRangeSets:
        db_item.second->CompactRange(storage::DataType::kSets, arg.argv[0], arg.argv[1]);
        break;
      case TaskType::kCompactRangeZSets:
        db_item.second->CompactRange(storage::DataType::kZSets, arg.argv[0], arg.argv[1]);
        break;
      case TaskType::kCompactRangeList:
        db_item.second->CompactRange(storage::DataType::kLists, arg.argv[0], arg.argv[1]);
        break;
      default:
        break;
    }
  }
  return Status::OK();
}

void PikaServer::PrepareDBTrySync() {
  std::shared_lock rwl(dbs_rw_);
  ReplState state = force_full_sync_ ? ReplState::kTryDBSync : ReplState::kTryConnect;
  for (const auto& db_item : dbs_) {
    Status s = g_pika_rm->ActivateSyncSlaveDB(
        RmNode(g_pika_server->master_ip(), g_pika_server->master_port(), db_item.second->GetDBName()), state);
    if (!s.ok()) {
      LOG(WARNING) << s.ToString();
    }
  }
  force_full_sync_ = false;
  LOG(INFO) << "Mark try connect finish";
}

void PikaServer::DBSetMaxCacheStatisticKeys(uint32_t max_cache_statistic_keys) {
  std::shared_lock rwl(dbs_rw_);
  for (const auto& db_item : dbs_) {
    db_item.second->DbRWLockReader();
    db_item.second->storage()->SetMaxCacheStatisticKeys(max_cache_statistic_keys);
    db_item.second->DbRWUnLock();
  }
}

void PikaServer::DBSetSmallCompactionThreshold(uint32_t small_compaction_threshold) {
  std::shared_lock rwl(dbs_rw_);
  for (const auto& db_item : dbs_) {
    db_item.second->DbRWLockReader();
    db_item.second->storage()->SetSmallCompactionThreshold(small_compaction_threshold);
    db_item.second->DbRWUnLock();
  }
}

void PikaServer::DBSetSmallCompactionDurationThreshold(uint32_t small_compaction_duration_threshold) {
  std::shared_lock rwl(dbs_rw_);
  for (const auto& db_item : dbs_) {
    db_item.second->DbRWLockReader();
    db_item.second->storage()->SetSmallCompactionDurationThreshold(small_compaction_duration_threshold);
    db_item.second->DbRWUnLock();
  }
}

bool PikaServer::GetDBBinlogOffset(const std::string& db_name, BinlogOffset* const boffset) {
  std::shared_ptr<SyncMasterDB> db = g_pika_rm->GetSyncMasterDBByName(DBInfo(db_name));
  if (!db) {
    return false;
  }
  Status s = db->Logger()->GetProducerStatus(&(boffset->filenum), &(boffset->offset));
  return s.ok();
}

Status PikaServer::DoSameThingEveryDB(const TaskType& type) {
  std::shared_lock rwl(dbs_rw_);
  std::shared_ptr<SyncSlaveDB> slave_db = nullptr;
  for (const auto& db_item : dbs_) {
    switch (type) {
      case TaskType::kResetReplState: {
        slave_db = g_pika_rm->GetSyncSlaveDBByName(DBInfo(db_item.second->GetDBName()));
        if (!slave_db) {
          LOG(WARNING) << "Slave DB: " << db_item.second->GetDBName() << ":"
                       << " Not Found";
        }
        slave_db->SetReplState(ReplState::kNoConnect);
        break;
      }
      case TaskType::kPurgeLog: {
        std::shared_ptr<SyncMasterDB> db = g_pika_rm->GetSyncMasterDBByName(
            DBInfo(db_item.second->GetDBName()));
        if (!db) {
          LOG(WARNING) << "DB: " << db_item.second->GetDBName() << ":"
                       << " Not Found.";
          break;
        }
        db->StableLogger()->PurgeStableLogs();
        break;
      }
      case TaskType::kCompactAll:
        db_item.second->Compact(storage::kAll);
        break;
      default:
        break;
    }
  }
  return Status::OK();
}

void PikaServer::BecomeMaster() {
  std::lock_guard l(state_protector_);
  role_ |= PIKA_ROLE_MASTER;
}

void PikaServer::DeleteSlave(int fd) {
  std::string ip;
  int port = -1;
  bool is_find = false;
  int slave_num = -1;
  {
    std::lock_guard l(slave_mutex_);
    auto iter = slaves_.begin();
    while (iter != slaves_.end()) {
      if (iter->conn_fd == fd) {
        ip = iter->ip;
        port = iter->port;
        is_find = true;
        LOG(INFO) << "Delete Slave Success, ip_port: " << iter->ip << ":" << iter->port;
        slaves_.erase(iter);
        break;
      }
      iter++;
    }
    slave_num = static_cast<int32_t>(slaves_.size());
  }

  if (is_find) {
    g_pika_rm->LostConnection(ip, port);
    g_pika_rm->DropItemInWriteQueue(ip, port);
  }

  if (slave_num == 0) {
    std::lock_guard l(state_protector_);
    role_ &= ~PIKA_ROLE_MASTER;
    leader_protected_mode_ = false;  // explicitly cancel protected mode
  }
}

int32_t PikaServer::CountSyncSlaves() {
  std::lock_guard ldb(db_sync_protector_);
  return static_cast<int32_t>(db_sync_slaves_.size());
}

int32_t PikaServer::GetSlaveListString(std::string& slave_list_str) {
  size_t index = 0;
  SlaveState slave_state;
  BinlogOffset master_boffset;
  BinlogOffset sent_slave_boffset;
  BinlogOffset acked_slave_boffset;
  std::stringstream tmp_stream;
  std::lock_guard l(slave_mutex_);
  std::shared_ptr<SyncMasterDB> master_db = nullptr;
  for (const auto& slave : slaves_) {
    tmp_stream << "slave" << index++ << ":ip=" << slave.ip << ",port=" << slave.port << ",conn_fd=" << slave.conn_fd
               << ",lag=";
    for (const auto& ts : slave.db_structs) {
      std::shared_ptr<SyncMasterDB> db = g_pika_rm->GetSyncMasterDBByName(DBInfo(ts.db_name));
      if (!db) {
        LOG(WARNING) << "Sync Master DB: " << ts.db_name << ", NotFound";
        continue;
      }
      Status s = db->GetSlaveState(slave.ip, slave.port, &slave_state);
      if (s.ok() && slave_state == SlaveState::kSlaveBinlogSync &&
          db->GetSlaveSyncBinlogInfo(slave.ip, slave.port, &sent_slave_boffset, &acked_slave_boffset).ok()) {
        Status s = db->Logger()->GetProducerStatus(&(master_boffset.filenum), &(master_boffset.offset));
        if (!s.ok()) {
          continue;
        } else {
          uint64_t lag =
              static_cast<uint64_t>((master_boffset.filenum - sent_slave_boffset.filenum)) * g_pika_conf->binlog_file_size() +
              master_boffset.offset - sent_slave_boffset.offset;
          tmp_stream << "(" << db->DBName() << ":" << lag << ")";
        }
      } else {
        tmp_stream << "(" << db->DBName() << ":not syncing)";
      }
    }
    tmp_stream << "\r\n";
  }
  slave_list_str.assign(tmp_stream.str());
  return static_cast<int32_t>(index);
}

// Try add Slave, return true if success,
// return false when slave already exist
bool PikaServer::TryAddSlave(const std::string& ip, int64_t port, int fd, const std::vector<DBStruct>& db_structs) {
  std::string ip_port = pstd::IpPortString(ip, static_cast<int32_t>(port));

  std::lock_guard l(slave_mutex_);
  auto iter = slaves_.begin();
  while (iter != slaves_.end()) {
    if (iter->ip_port == ip_port) {
      LOG(WARNING) << "Slave Already Exist, ip_port: " << ip << ":" << port;
      return false;
    }
    iter++;
  }

  // Not exist, so add new
  LOG(INFO) << "Add New Slave, " << ip << ":" << port;
  SlaveItem s;
  s.ip_port = ip_port;
  s.ip = ip;
  s.port = static_cast<int32_t>(port);
  s.conn_fd = fd;
  s.stage = SLAVE_ITEM_STAGE_ONE;
  s.db_structs = db_structs;
  gettimeofday(&s.create_time, nullptr);
  slaves_.push_back(s);
  return true;
}

void PikaServer::SyncError() {
  std::lock_guard l(state_protector_);
  repl_state_ = PIKA_REPL_ERROR;
  LOG(WARNING) << "Sync error, set repl_state to PIKA_REPL_ERROR";
}

void PikaServer::RemoveMaster() {
  {
    std::lock_guard l(state_protector_);
    repl_state_ = PIKA_REPL_NO_CONNECT;
    role_ &= ~PIKA_ROLE_SLAVE;

    if (!master_ip_.empty() && master_port_ != -1) {
      g_pika_rm->CloseReplClientConn(master_ip_, master_port_ + kPortShiftReplServer);
      g_pika_rm->LostConnection(master_ip_, master_port_);
      UpdateMetaSyncTimestampWithoutLock();
      LOG(INFO) << "Remove Master Success, ip_port: " << master_ip_ << ":" << master_port_;
    }

    master_ip_ = "";
    master_port_ = -1;
    DoSameThingEveryDB(TaskType::kResetReplState);
  }
}

bool PikaServer::SetMaster(std::string& master_ip, int master_port) {
  if (master_ip == "127.0.0.1") {
    master_ip = host_;
  }
  std::lock_guard l(state_protector_);
  if (((role_ ^ PIKA_ROLE_SLAVE) != 0) && repl_state_ == PIKA_REPL_NO_CONNECT) {
    master_ip_ = master_ip;
    master_port_ = master_port;
    role_ |= PIKA_ROLE_SLAVE;
    repl_state_ = PIKA_REPL_SHOULD_META_SYNC;
    return true;
  }
  return false;
}

bool PikaServer::ShouldMetaSync() {
  std::shared_lock l(state_protector_);
  return repl_state_ == PIKA_REPL_SHOULD_META_SYNC;
}

void PikaServer::FinishMetaSync() {
  std::lock_guard l(state_protector_);
  assert(repl_state_ == PIKA_REPL_SHOULD_META_SYNC);
  repl_state_ = PIKA_REPL_META_SYNC_DONE;
}

bool PikaServer::MetaSyncDone() {
  std::shared_lock l(state_protector_);
  return repl_state_ == PIKA_REPL_META_SYNC_DONE;
}

void PikaServer::ResetMetaSyncStatus() {
  std::lock_guard sp_l(state_protector_);
  if ((role_ & PIKA_ROLE_SLAVE) != 0) {
    // not change by slaveof no one, so set repl_state = PIKA_REPL_SHOULD_META_SYNC,
    // continue to connect master
    repl_state_ = PIKA_REPL_SHOULD_META_SYNC;
    DoSameThingEveryDB(TaskType::kResetReplState);
  }
}

int PikaServer::GetMetaSyncTimestamp() {
  std::shared_lock sp_l(state_protector_);
  return last_meta_sync_timestamp_;
}

void PikaServer::UpdateMetaSyncTimestamp() {
  std::lock_guard sp_l(state_protector_);
  last_meta_sync_timestamp_ = static_cast<int32_t>(time(nullptr));
}

void PikaServer::UpdateMetaSyncTimestampWithoutLock() {
  last_meta_sync_timestamp_ = static_cast<int32_t>(time(nullptr));
}

bool PikaServer::IsFirstMetaSync() {
  std::shared_lock sp_l(state_protector_);
  return first_meta_sync_;
}

void PikaServer::SetFirstMetaSync(bool v) {
  std::lock_guard sp_l(state_protector_);
  first_meta_sync_ = v;
}

void PikaServer::ScheduleClientPool(net::TaskFunc func, void* arg, bool is_slow_cmd) {
  if (is_slow_cmd) {
    pika_slow_cmd_thread_pool_->Schedule(func, arg);
    return;
  }
  pika_client_processor_->SchedulePool(func, arg);
}

void PikaServer::ScheduleClientBgThreads(net::TaskFunc func, void* arg, const std::string& hash_str) {
  pika_client_processor_->ScheduleBgThreads(func, arg, hash_str);
}

size_t PikaServer::ClientProcessorThreadPoolCurQueueSize() {
  if (!pika_client_processor_) {
    return 0;
  }
  return pika_client_processor_->ThreadPoolCurQueueSize();
}

size_t PikaServer::ClientProcessorThreadPoolMaxQueueSize() {
  if (!pika_client_processor_) {
    return 0;
  }
  return pika_client_processor_->ThreadPoolMaxQueueSize();
}

size_t PikaServer::SlowCmdThreadPoolCurQueueSize() {
  if (!pika_slow_cmd_thread_pool_) {
    return 0;
  }
  size_t cur_size = 0;
  pika_slow_cmd_thread_pool_->cur_queue_size(&cur_size);
  return cur_size;
}

size_t PikaServer::SlowCmdThreadPoolMaxQueueSize() {
  if (!pika_slow_cmd_thread_pool_) {
    return 0;
  }
  return pika_slow_cmd_thread_pool_->max_queue_size();
}

void PikaServer::BGSaveTaskSchedule(net::TaskFunc func, void* arg) {
  bgsave_thread_.StartThread();
  bgsave_thread_.Schedule(func, arg);
}

void PikaServer::PurgelogsTaskSchedule(net::TaskFunc func, void* arg) {
  purge_thread_.StartThread();
  purge_thread_.Schedule(func, arg);
}

void PikaServer::PurgeDir(const std::string& path) {
  auto dir_path = new std::string(path);
  PurgeDirTaskSchedule(&DoPurgeDir, static_cast<void*>(dir_path));
}

void PikaServer::PurgeDirTaskSchedule(void (*function)(void*), void* arg) {
  purge_thread_.StartThread();
  purge_thread_.Schedule(function, arg);
}

void PikaServer::DBSync(const std::string& ip, int port, const std::string& db_name) {
  {
    std::string task_index = DbSyncTaskIndex(ip, port, db_name);
    std::lock_guard ml(db_sync_protector_);
    if (db_sync_slaves_.find(task_index) != db_sync_slaves_.end()) {
      return;
    }
    db_sync_slaves_.insert(task_index);
  }
  // Reuse the bgsave_thread_
  // Since we expect BgSave and DBSync execute serially
  bgsave_thread_.StartThread();
  auto arg = new DBSyncArg(this, ip, port, db_name);
  bgsave_thread_.Schedule(&DoDBSync, reinterpret_cast<void*>(arg));
}

pstd::Status PikaServer::GetDumpUUID(const std::string& db_name, std::string* snapshot_uuid) {
  std::shared_ptr<DB> db = GetDB(db_name);
  if (!db) {
    LOG(WARNING) << "cannot find db for db_name " << db_name;
    return pstd::Status::NotFound("db no found");
  }
  db->GetBgSaveUUID(snapshot_uuid);
  return pstd::Status::OK();
}

pstd::Status PikaServer::GetDumpMeta(const std::string& db_name, std::vector<std::string>* fileNames, std::string* snapshot_uuid) {
  std::shared_ptr<DB> db = GetDB(db_name);
  if (!db) {
    LOG(WARNING) << "cannot find db for db_name " << db_name;
    return pstd::Status::NotFound("db no found");
  }
  db->GetBgSaveMetaData(fileNames, snapshot_uuid);
  return pstd::Status::OK();
}

void PikaServer::TryDBSync(const std::string& ip, int port, const std::string& db_name,
                           int32_t top) {
  std::shared_ptr<DB> db = GetDB(db_name);
  if (!db) {
    LOG(WARNING) << "can not find DB : " << db_name
                 << ", TryDBSync Failed";
    return;
  }
  std::shared_ptr<SyncMasterDB> sync_db =
      g_pika_rm->GetSyncMasterDBByName(DBInfo(db_name));
  if (!sync_db) {
    LOG(WARNING) << "can not find DB: " << db_name
                 << ", TryDBSync Failed";
    return;
  }
  BgSaveInfo bgsave_info = db->bgsave_info();
  std::string logger_filename = sync_db->Logger()->filename();
  if (pstd::IsDir(bgsave_info.path) != 0 ||
      !pstd::FileExists(NewFileName(logger_filename, bgsave_info.offset.b_offset.filenum)) ||
      top - bgsave_info.offset.b_offset.filenum > kDBSyncMaxGap) {
    // Need Bgsave first
    db->BgSaveDB();
  }
}

void PikaServer::DbSyncSendFile(const std::string& ip, int port, const std::string& db_name) {
  std::shared_ptr<DB> db = GetDB(db_name);
  if (!db) {
    LOG(WARNING) << "can not find DB: " << db_name
                 << ", DbSync send file Failed";
    return;
  }

  BgSaveInfo bgsave_info = db->bgsave_info();
  std::string bg_path = bgsave_info.path;
  uint32_t binlog_filenum = bgsave_info.offset.b_offset.filenum;
  uint64_t binlog_offset = bgsave_info.offset.b_offset.offset;
  uint32_t term = bgsave_info.offset.l_offset.term;
  uint64_t index = bgsave_info.offset.l_offset.index;

  // Get all files need to send
  std::vector<std::string> descendant;
  int ret = 0;
  LOG(INFO) << "DB: " << db->GetDBName() << " Start Send files in " << bg_path << " to " << ip;
  ret = pstd::GetChildren(bg_path, descendant);
  if (ret) {
    std::string ip_port = pstd::IpPortString(ip, port);
    std::lock_guard ldb(db_sync_protector_);
    db_sync_slaves_.erase(ip_port);
    LOG(WARNING) << "DB: " << db->GetDBName()
                 << " Get child directory when try to do sync failed, error: " << strerror(ret);
    return;
  }

  std::string local_path;
  std::string target_path;
  const std::string& remote_path = db_name;
  auto iter = descendant.begin();
  pstd::RsyncRemote remote(ip, port, kDBSyncModule, g_pika_conf->db_sync_speed() * 1024);
  std::string secret_file_path = g_pika_conf->db_sync_path();
  if (g_pika_conf->db_sync_path().back() != '/') {
    secret_file_path += "/";
  }
  secret_file_path += pstd::kRsyncSubDir + "/" + kPikaSecretFile;

  for (; iter != descendant.end(); ++iter) {
    local_path = bg_path + "/" + *iter;
    target_path = remote_path + "/" + *iter;

    if (*iter == kBgsaveInfoFile) {
      continue;
    }

    if (pstd::IsDir(local_path) == 0 && local_path.back() != '/') {
      local_path.push_back('/');
      target_path.push_back('/');
    }

    // We need specify the speed limit for every single file
    ret = pstd::RsyncSendFile(local_path, target_path, secret_file_path, remote);
    if (0 != ret) {
      LOG(WARNING) << "DB: " << db->GetDBName() << " RSync send file failed! From: " << *iter
                   << ", To: " << target_path << ", At: " << ip << ":" << port << ", Error: " << ret;
      break;
    }
  }
  // Clear target path
  pstd::RsyncSendClearTarget(bg_path + "/strings", remote_path + "/strings", secret_file_path, remote);
  pstd::RsyncSendClearTarget(bg_path + "/hashes", remote_path + "/hashes", secret_file_path, remote);
  pstd::RsyncSendClearTarget(bg_path + "/lists", remote_path + "/lists", secret_file_path, remote);
  pstd::RsyncSendClearTarget(bg_path + "/sets", remote_path + "/sets", secret_file_path, remote);
  pstd::RsyncSendClearTarget(bg_path + "/zsets", remote_path + "/zsets", secret_file_path, remote);

  std::unique_ptr<net::NetCli> cli(net::NewRedisCli());
  std::string lip(host_);
  if (cli->Connect(ip, port, "").ok()) {
    struct sockaddr_in laddr;
    socklen_t llen = sizeof(laddr);
    getsockname(cli->fd(), reinterpret_cast<struct sockaddr*>(&laddr), &llen);
    lip = inet_ntoa(laddr.sin_addr);
    cli->Close();
  } else {
    LOG(WARNING) << "Rsync try connect slave rsync service error"
                 << ", slave rsync service(" << ip << ":" << port << ")";
  }

  // Send info file at last
  if (0 == ret) {
    // need to modify the IP addr in the info file
    if (lip != host_) {
      std::ofstream fix;
      std::string fn = bg_path + "/" + kBgsaveInfoFile + "." + std::to_string(time(nullptr));
      fix.open(fn, std::ios::in | std::ios::trunc);
      if (fix.is_open()) {
        fix << "0s\n" << lip << "\n" << port_ << "\n" << binlog_filenum << "\n" << binlog_offset << "\n";
        fix.close();
      }
      ret = pstd::RsyncSendFile(fn, remote_path + "/" + kBgsaveInfoFile, secret_file_path, remote);
      pstd::DeleteFile(fn);
      if (ret) {
        LOG(WARNING) << "DB: " << db->GetDBName() << " Send Modified Info File Failed";
      }
    } else if (0 != (ret = pstd::RsyncSendFile(bg_path + "/" + kBgsaveInfoFile, remote_path + "/" + kBgsaveInfoFile,
                                               secret_file_path, remote))) {
      LOG(WARNING) << "DB: " << db->GetDBName() << " Send Info File Failed";
    }
  }
  // remove slave
  {
    std::string task_index = DbSyncTaskIndex(ip, port, db_name);
    std::lock_guard ml(db_sync_protector_);
    db_sync_slaves_.erase(task_index);
  }

  if (0 == ret) {
    LOG(INFO) << "DB: " << db->GetDBName() << " RSync Send Files Success";
  }
}

std::string PikaServer::DbSyncTaskIndex(const std::string& ip, int port, const std::string& db_name) {
  char buf[256];
  snprintf(buf, sizeof(buf), "%s:%d_%s:%d", ip.data(), port, db_name.data());
  return buf;
}

void PikaServer::KeyScanTaskSchedule(net::TaskFunc func, void* arg) {
  key_scan_thread_.StartThread();
  key_scan_thread_.Schedule(func, arg);
}

void PikaServer::ClientKillAll() { pika_dispatch_thread_->ClientKillAll(); }

int PikaServer::ClientKill(const std::string& ip_port) {
  if (pika_dispatch_thread_->ClientKill(ip_port)) {
    return 1;
  }
  return 0;
}

int64_t PikaServer::ClientList(std::vector<ClientInfo>* clients) {
  int64_t clients_num = 0;
  clients_num += static_cast<int64_t>(pika_dispatch_thread_->ThreadClientList(clients));
  return clients_num;
}

bool PikaServer::HasMonitorClients() const {
  std::unique_lock lock(monitor_mutex_protector_);
  return !pika_monitor_clients_.empty();
}
bool PikaServer::ClientIsMonitor(const std::shared_ptr<PikaClientConn>& client_ptr) const {
  std::unique_lock lock(monitor_mutex_protector_);
  return pika_monitor_clients_.count(client_ptr) != 0;
}

void PikaServer::AddMonitorMessage(const std::string& monitor_message) {
  const std::string msg = "+" + monitor_message + "\r\n";

  std::vector<std::shared_ptr<PikaClientConn>> clients;

  std::unique_lock lock(monitor_mutex_protector_);
  clients.reserve(pika_monitor_clients_.size());
  for (auto it = pika_monitor_clients_.begin(); it != pika_monitor_clients_.end();) {
    auto cli = (*it).lock();
    if (cli) {
      clients.push_back(std::move(cli));
      ++it;
    } else {
      it = pika_monitor_clients_.erase(it);
    }
  }
  for (const auto& cli : clients) {
    cli->WriteResp(msg);
    cli->SendReply();
  }
  lock.unlock();  // SendReply without lock
}

void PikaServer::AddMonitorClient(const std::shared_ptr<PikaClientConn>& client_ptr) {
  if (client_ptr) {
    std::unique_lock lock(monitor_mutex_protector_);
    pika_monitor_clients_.insert(client_ptr);
  }
}

void PikaServer::SlowlogTrim() {
  std::lock_guard l(slowlog_protector_);
  while (slowlog_list_.size() > static_cast<uint32_t>(g_pika_conf->slowlog_max_len())) {
    slowlog_list_.pop_back();
  }
}

void PikaServer::SlowlogReset() {
  std::lock_guard l(slowlog_protector_);
  slowlog_list_.clear();
}

uint32_t PikaServer::SlowlogLen() {
  std::shared_lock l(slowlog_protector_);
  return slowlog_list_.size();
}

void PikaServer::SlowlogObtain(int64_t number, std::vector<SlowlogEntry>* slowlogs) {
  std::shared_lock l(slowlog_protector_);
  slowlogs->clear();
  auto iter = slowlog_list_.begin();
  while (((number--) != 0) && iter != slowlog_list_.end()) {
    slowlogs->push_back(*iter);
    iter++;
  }
}

void PikaServer::SlowlogPushEntry(const PikaCmdArgsType& argv, int64_t time, int64_t duration) {
  SlowlogEntry entry;
  uint32_t slargc = (argv.size() < SLOWLOG_ENTRY_MAX_ARGC) ? argv.size() : SLOWLOG_ENTRY_MAX_ARGC;

  for (uint32_t idx = 0; idx < slargc; ++idx) {
    if (slargc != argv.size() && idx == slargc - 1) {
      char buffer[32];
      snprintf(buffer, sizeof(buffer), "... (%lu more arguments)", argv.size() - slargc + 1);
      entry.argv.push_back(std::string(buffer));
    } else {
      if (argv[idx].size() > SLOWLOG_ENTRY_MAX_STRING) {
        char buffer[32];
        snprintf(buffer, sizeof(buffer), "... (%lu more bytes)", argv[idx].size() - SLOWLOG_ENTRY_MAX_STRING);
        std::string suffix(buffer);
        std::string brief = argv[idx].substr(0, SLOWLOG_ENTRY_MAX_STRING);
        entry.argv.push_back(brief + suffix);
      } else {
        entry.argv.push_back(argv[idx]);
      }
    }
  }

  {
    std::lock_guard lock(slowlog_protector_);
    entry.id = static_cast<int64_t>(slowlog_entry_id_++);
    entry.start_time = time;
    entry.duration = duration;
    slowlog_list_.push_front(entry);
    slowlog_counter_++;
  }

  SlowlogTrim();
}

uint64_t PikaServer::SlowlogCount() {
  std::shared_lock l(slowlog_protector_);
  return slowlog_counter_;
}

void PikaServer::ResetStat() {
  statistic_.server_stat.accumulative_connections.store(0);
  statistic_.server_stat.qps.querynum.store(0);
  statistic_.server_stat.qps.last_querynum.store(0);
}

uint64_t PikaServer::ServerQueryNum() { return statistic_.server_stat.qps.querynum.load(); }

uint64_t PikaServer::ServerCurrentQps() { return statistic_.server_stat.qps.last_sec_querynum.load(); }

uint64_t PikaServer::accumulative_connections() { return statistic_.server_stat.accumulative_connections.load(); }

void PikaServer::incr_accumulative_connections() { ++(statistic_.server_stat.accumulative_connections); }

// only one thread invoke this right now
void PikaServer::ResetLastSecQuerynum() {
  statistic_.server_stat.qps.ResetLastSecQuerynum();
  statistic_.ResetDBLastSecQuerynum();
}

void PikaServer::UpdateQueryNumAndExecCountDB(const std::string& db_name, const std::string& command, bool is_write) {
  std::string cmd(command);
  statistic_.server_stat.qps.querynum++;
  statistic_.server_stat.exec_count_db[pstd::StringToUpper(cmd)]++;
  statistic_.UpdateDBQps(db_name, command, is_write);
}

size_t PikaServer::NetInputBytes() { return g_network_statistic->NetInputBytes(); }

size_t PikaServer::NetOutputBytes() { return g_network_statistic->NetOutputBytes(); }

size_t PikaServer::NetReplInputBytes() { return g_network_statistic->NetReplInputBytes(); }

size_t PikaServer::NetReplOutputBytes() { return g_network_statistic->NetReplOutputBytes(); }

float PikaServer::InstantaneousInputKbps() {
  return static_cast<float>(g_pika_server->instant_->getInstantaneousMetric(STATS_METRIC_NET_INPUT)) / 1024.0f;
}

float PikaServer::InstantaneousOutputKbps() {
  return static_cast<float>(g_pika_server->instant_->getInstantaneousMetric(STATS_METRIC_NET_OUTPUT)) / 1024.0f;
}

float PikaServer::InstantaneousInputReplKbps() {
  return static_cast<float>(g_pika_server->instant_->getInstantaneousMetric(STATS_METRIC_NET_INPUT_REPLICATION)) /
         1024.0f;
}

float PikaServer::InstantaneousOutputReplKbps() {
  return static_cast<float>(g_pika_server->instant_->getInstantaneousMetric(STATS_METRIC_NET_OUTPUT_REPLICATION)) /
         1024.0f;
}

std::unordered_map<std::string, uint64_t> PikaServer::ServerExecCountDB() {
  std::unordered_map<std::string, uint64_t> res;
  for (auto& cmd : statistic_.server_stat.exec_count_db) {
    res[cmd.first] = cmd.second.load();
  }
  return res;
}

std::unordered_map<std::string, QpsStatistic> PikaServer::ServerAllDBStat() { return statistic_.AllDBStat(); }

int PikaServer::SendToPeer() { return g_pika_rm->ConsumeWriteQueue(); }

void PikaServer::SignalAuxiliary() { pika_auxiliary_thread_->cv_.notify_one(); }

Status PikaServer::TriggerSendBinlogSync() { return g_pika_rm->WakeUpBinlogSync(); }

int PikaServer::PubSubNumPat() { return pika_pubsub_thread_->PubSubNumPat(); }

int PikaServer::Publish(const std::string& channel, const std::string& msg) {
  int receivers = pika_pubsub_thread_->Publish(channel, msg);
  return receivers;
}

void PikaServer::EnablePublish(int fd) {
  pika_pubsub_thread_->UpdateConnReadyState(fd, net::PubSubThread::ReadyState::kReady);
}

int PikaServer::UnSubscribe(const std::shared_ptr<net::NetConn>& conn, const std::vector<std::string>& channels,
                            bool pattern, std::vector<std::pair<std::string, int>>* result) {
  int subscribed = pika_pubsub_thread_->UnSubscribe(conn, channels, pattern, result);
  return subscribed;
}

void PikaServer::Subscribe(const std::shared_ptr<net::NetConn>& conn, const std::vector<std::string>& channels,
                           bool pattern, std::vector<std::pair<std::string, int>>* result) {
  pika_pubsub_thread_->Subscribe(conn, channels, pattern, result);
}

void PikaServer::PubSubChannels(const std::string& pattern, std::vector<std::string>* result) {
  pika_pubsub_thread_->PubSubChannels(pattern, result);
}

void PikaServer::PubSubNumSub(const std::vector<std::string>& channels,
                              std::vector<std::pair<std::string, int>>* result) {
  pika_pubsub_thread_->PubSubNumSub(channels, result);
}

int PikaServer::ClientPubSubChannelSize(const std::shared_ptr<NetConn>& conn) {
  return pika_pubsub_thread_->ClientPubSubChannelSize(conn);
}

int PikaServer::ClientPubSubChannelPatternSize(const std::shared_ptr<NetConn>& conn) {
  return pika_pubsub_thread_->ClientPubSubChannelPatternSize(conn);
}

/******************************* PRIVATE *******************************/

void PikaServer::DoTimingTask() {
  // Maybe schedule compactrange
  AutoCompactRange();
  // Purge log
  AutoPurge();
  // Delete expired dump
  AutoDeleteExpiredDump();
  // Cheek Rsync Status
  // TODO: temporarily disable rsync
  // AutoKeepAliveRSync();
  // Reset server qps
  ResetLastSecQuerynum();
  // Auto update network instantaneous metric
  AutoUpdateNetworkMetric();
  ProcessCronTask();
  UpdateCacheInfo();
  // Print the queue status periodically
  PrintThreadPoolQueueStatus();

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

  if (!ci.empty()) {
    std::string::size_type slash = ci.find('/');
    int interval = std::atoi(ci.substr(0, slash).c_str());
    int usage = std::atoi(ci.substr(slash + 1).c_str());
    struct timeval now;
    gettimeofday(&now, nullptr);
    if (last_check_compact_time_.tv_sec == 0 || now.tv_sec - last_check_compact_time_.tv_sec >= interval * 3600) {
      gettimeofday(&last_check_compact_time_, nullptr);
      if ((static_cast<double>(free_size) / static_cast<double>(total_size)) * 100 >= usage) {
        std::set<std::string> dbs = g_pika_server->GetAllDBName();
        Status s = DoSameThingSpecificDB(dbs, {TaskType::kCompactAll});
        if (s.ok()) {
          LOG(INFO) << "[Interval]schedule compactRange, freesize: " << free_size / 1048576
                    << "MB, disksize: " << total_size / 1048576 << "MB";
        } else {
          LOG(INFO) << "[Interval]schedule compactRange Failed, freesize: " << free_size / 1048576
                    << "MB, disksize: " << total_size / 1048576 << "MB, error: " << s.ToString();
        }
      } else {
        LOG(WARNING) << "compact-interval failed, because there is not enough disk space left, freesize"
                     << free_size / 1048576 << "MB, disksize: " << total_size / 1048576 << "MB";
      }
    }
    return;
  }

  if (!cc.empty()) {
    bool have_week = false;
    std::string compact_cron;
    std::string week_str;
    int64_t slash_num = count(cc.begin(), cc.end(), '/');
    if (slash_num == 2) {
      have_week = true;
      std::string::size_type first_slash = cc.find('/');
      week_str = cc.substr(0, first_slash);
      compact_cron = cc.substr(first_slash + 1);
    } else {
      compact_cron = cc;
    }

    std::string::size_type colon = compact_cron.find('-');
    std::string::size_type underline = compact_cron.find('/');
    int week = have_week ? (std::atoi(week_str.c_str()) % 7) : 0;
    int start = std::atoi(compact_cron.substr(0, colon).c_str());
    int end = std::atoi(compact_cron.substr(colon + 1, underline).c_str());
    int usage = std::atoi(compact_cron.substr(underline + 1).c_str());
    std::time_t t = std::time(nullptr);
    std::tm* t_m = std::localtime(&t);

    bool in_window = false;
    if (start < end && (t_m->tm_hour >= start && t_m->tm_hour < end)) {
      in_window = have_week ? (week == t_m->tm_wday) : true;
    } else if (start > end &&
               ((t_m->tm_hour >= start && t_m->tm_hour < 24) || (t_m->tm_hour >= 0 && t_m->tm_hour < end))) {
      in_window = !have_week;
    } else {
      have_scheduled_crontask_ = false;
    }

    if (!have_scheduled_crontask_ && in_window) {
      if ((static_cast<double>(free_size) / static_cast<double>(total_size)) * 100 >= usage) {
        Status s = DoSameThingEveryDB(TaskType::kCompactAll);
        if (s.ok()) {
          LOG(INFO) << "[Cron]schedule compactRange, freesize: " << free_size / 1048576
                    << "MB, disksize: " << total_size / 1048576 << "MB";
        } else {
          LOG(INFO) << "[Cron]schedule compactRange Failed, freesize: " << free_size / 1048576
                    << "MB, disksize: " << total_size / 1048576 << "MB, error: " << s.ToString();
        }
        have_scheduled_crontask_ = true;
      } else {
        LOG(WARNING) << "compact-cron failed, because there is not enough disk space left, freesize"
                     << free_size / 1048576 << "MB, disksize: " << total_size / 1048576 << "MB";
      }
    }
  }
}

void PikaServer::AutoPurge() { DoSameThingEveryDB(TaskType::kPurgeLog); }

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
  if (!pstd::FileExists(db_sync_path)) {
    return;
  }

  // Directory traversal
  if (pstd::GetChildren(db_sync_path, dump_dir) != 0) {
    return;
  }
  // Handle dump directory
  for (auto& i : dump_dir) {
    if (i.substr(0, db_sync_prefix.size()) != db_sync_prefix || i.size() != (db_sync_prefix.size() + 8)) {
      continue;
    }

    std::string str_date = i.substr(db_sync_prefix.size(), (i.size() - db_sync_prefix.size()));
    char* end = nullptr;
    std::strtol(str_date.c_str(), &end, 10);
    if (*end != 0) {
      continue;
    }

    // Parse filename
    int dump_year = std::atoi(str_date.substr(0, 4).c_str());
    int dump_month = std::atoi(str_date.substr(4, 2).c_str());
    int dump_day = std::atoi(str_date.substr(6, 2).c_str());

    time_t t = time(nullptr);
    struct tm* now = localtime(&t);
    int now_year = now->tm_year + 1900;
    int now_month = now->tm_mon + 1;
    int now_day = now->tm_mday;

    struct tm dump_time;
    struct tm now_time;

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

    int64_t dump_timestamp = mktime(&dump_time);
    int64_t now_timestamp = mktime(&now_time);
    // How many days, 1 day = 86400s
    int64_t interval_days = (now_timestamp - dump_timestamp) / 86400;

    if (interval_days >= expiry_days) {
      std::string dump_file = db_sync_path + i;
      if (CountSyncSlaves() == 0) {
        LOG(INFO) << "Not syncing, delete dump file: " << dump_file;
        pstd::DeleteDirIfExist(dump_file);
      } else {
        LOG(INFO) << "Syncing, can not delete " << dump_file << " dump file";
      }
    }
  }
}

void PikaServer::AutoUpdateNetworkMetric() {
  monotime current_time = getMonotonicUs();
  size_t factor = 5e6;  // us, 5s
  instant_->trackInstantaneousMetric(STATS_METRIC_NET_INPUT,
                                     g_pika_server->NetInputBytes() + g_pika_server->NetReplInputBytes(), current_time,
                                     factor);
  instant_->trackInstantaneousMetric(STATS_METRIC_NET_OUTPUT,
                                     g_pika_server->NetOutputBytes() + g_pika_server->NetReplOutputBytes(),
                                     current_time, factor);
  instant_->trackInstantaneousMetric(STATS_METRIC_NET_INPUT_REPLICATION, g_pika_server->NetReplInputBytes(),
                                     current_time, factor);
  instant_->trackInstantaneousMetric(STATS_METRIC_NET_OUTPUT_REPLICATION, g_pika_server->NetReplOutputBytes(),
                                     current_time, factor);
}

void PikaServer::PrintThreadPoolQueueStatus() {
  // Print the current queue size if it exceeds QUEUE_SIZE_THRESHOLD_PERCENTAGE/100 of the maximum queue size.
  size_t cur_size = ClientProcessorThreadPoolCurQueueSize();
  size_t max_size = ClientProcessorThreadPoolMaxQueueSize();
  size_t thread_hold = (max_size / 100) * QUEUE_SIZE_THRESHOLD_PERCENTAGE;
  if (cur_size > thread_hold) {
    LOG(INFO) << "The current queue size of the Pika Server's client thread processor thread pool: " << cur_size;
  }
}

void PikaServer::InitStorageOptions() {
  std::lock_guard rwl(storage_options_rw_);

  // For rocksdb::Options
  storage_options_.options.create_if_missing = true;
  storage_options_.options.keep_log_file_num = 10;
  storage_options_.options.max_manifest_file_size = 64 * 1024 * 1024;
  storage_options_.options.max_log_file_size = 512 * 1024 * 1024;

  storage_options_.options.write_buffer_size = g_pika_conf->write_buffer_size();
  storage_options_.options.arena_block_size = g_pika_conf->arena_block_size();
  storage_options_.options.write_buffer_manager =
      std::make_shared<rocksdb::WriteBufferManager>(g_pika_conf->max_write_buffer_size());
  storage_options_.options.max_write_buffer_number = g_pika_conf->max_write_buffer_number();
  storage_options_.options.target_file_size_base = g_pika_conf->target_file_size_base();
  storage_options_.options.max_background_flushes = g_pika_conf->max_background_flushes();
  storage_options_.options.max_background_compactions = g_pika_conf->max_background_compactions();
  storage_options_.options.disable_auto_compactions = g_pika_conf->disable_auto_compactions();
  storage_options_.options.max_background_jobs = g_pika_conf->max_background_jobs();
  storage_options_.options.max_open_files = g_pika_conf->max_cache_files();
  storage_options_.options.max_bytes_for_level_multiplier = g_pika_conf->max_bytes_for_level_multiplier();
  storage_options_.options.optimize_filters_for_hits = g_pika_conf->optimize_filters_for_hits();
  storage_options_.options.level_compaction_dynamic_level_bytes = g_pika_conf->level_compaction_dynamic_level_bytes();

  storage_options_.options.compression = PikaConf::GetCompression(g_pika_conf->compression());
  storage_options_.options.compression_per_level = g_pika_conf->compression_per_level();
  // avoid blocking io on scan
  // see https://github.com/facebook/rocksdb/wiki/IO#avoid-blocking-io
  storage_options_.options.avoid_unnecessary_blocking_io = true;

  // default l0 l1 noCompression l2 and more use `compression` option
  if (storage_options_.options.compression_per_level.empty() &&
      storage_options_.options.compression != rocksdb::kNoCompression) {
    storage_options_.options.compression_per_level.push_back(rocksdb::kNoCompression);
    storage_options_.options.compression_per_level.push_back(rocksdb::kNoCompression);
    storage_options_.options.compression_per_level.push_back(storage_options_.options.compression);
  }

  // For rocksdb::BlockBasedDBOptions
  storage_options_.table_options.block_size = g_pika_conf->block_size();
  storage_options_.table_options.cache_index_and_filter_blocks = g_pika_conf->cache_index_and_filter_blocks();
  storage_options_.block_cache_size = g_pika_conf->block_cache();
  storage_options_.share_block_cache = g_pika_conf->share_block_cache();

  storage_options_.table_options.pin_l0_filter_and_index_blocks_in_cache =
      g_pika_conf->pin_l0_filter_and_index_blocks_in_cache();

  if (storage_options_.block_cache_size == 0) {
    storage_options_.table_options.no_block_cache = true;
  } else if (storage_options_.share_block_cache) {
    storage_options_.table_options.block_cache =
        rocksdb::NewLRUCache(storage_options_.block_cache_size, static_cast<int>(g_pika_conf->num_shard_bits()));
  }

  storage_options_.options.rate_limiter =
      std::shared_ptr<rocksdb::RateLimiter>(
          rocksdb::NewGenericRateLimiter(
              g_pika_conf->rate_limiter_bandwidth(),
              g_pika_conf->rate_limiter_refill_period_us(),
              static_cast<int32_t>(g_pika_conf->rate_limiter_fairness()),
              rocksdb::RateLimiter::Mode::kWritesOnly,
              g_pika_conf->rate_limiter_auto_tuned()
                  ));

  // For Storage small compaction
  storage_options_.statistics_max_size = g_pika_conf->max_cache_statistic_keys();
  storage_options_.small_compaction_threshold = g_pika_conf->small_compaction_threshold();

  // rocksdb blob
  if (g_pika_conf->enable_blob_files()) {
    storage_options_.options.enable_blob_files = g_pika_conf->enable_blob_files();
    storage_options_.options.min_blob_size = g_pika_conf->min_blob_size();
    storage_options_.options.blob_file_size = g_pika_conf->blob_file_size();
    storage_options_.options.blob_compression_type = PikaConf::GetCompression(g_pika_conf->blob_compression_type());
    storage_options_.options.enable_blob_garbage_collection = g_pika_conf->enable_blob_garbage_collection();
    storage_options_.options.blob_garbage_collection_age_cutoff = g_pika_conf->blob_garbage_collection_age_cutoff();
    storage_options_.options.blob_garbage_collection_force_threshold =
        g_pika_conf->blob_garbage_collection_force_threshold();
    if (g_pika_conf->blob_cache() > 0) {  // blob cache less than 0，not open cache
      storage_options_.options.blob_cache =
          rocksdb::NewLRUCache(g_pika_conf->blob_cache(), static_cast<int>(g_pika_conf->blob_num_shard_bits()));
    }
  }
}

storage::Status PikaServer::RewriteStorageOptions(const storage::OptionType& option_type,
                                                  const std::unordered_map<std::string, std::string>& options_map) {
  storage::Status s;
  std::shared_lock db_rwl(dbs_rw_);
  for (const auto& db_item : dbs_) {
    db_item.second->DbRWLockWriter();
    s = db_item.second->storage()->SetOptions(option_type, storage::ALL_DB, options_map);
    db_item.second->DbRWUnLock();
    if (!s.ok()) {
      return s;
    }
  }
  std::lock_guard rwl(storage_options_rw_);
  s = storage_options_.ResetOptions(option_type, options_map);
  return s;
}

Status PikaServer::GetCmdRouting(std::vector<net::RedisCmdArgsType>& redis_cmds, std::vector<Node>* dst,
                                 bool* all_local) {
  UNUSED(redis_cmds);
  UNUSED(dst);
  *all_local = true;
  return Status::OK();
}

void PikaServer::ServerStatus(std::string* info) {
  std::stringstream tmp_stream;
  size_t q_size = ClientProcessorThreadPoolCurQueueSize();
  tmp_stream << "Client Processor thread-pool queue size: " << q_size << "\r\n";
  info->append(tmp_stream.str());
}

bool PikaServer::SlotsMigrateBatch(const std::string &ip, int64_t port, int64_t time_out, int64_t slot_num,int64_t keys_num, const std::shared_ptr<DB>& db) {
  return pika_migrate_thread_->ReqMigrateBatch(ip, port, time_out, slot_num, keys_num, db);
}

void PikaServer::GetSlotsMgrtSenderStatus(std::string *ip, int64_t* port, int64_t *slot, bool *migrating, int64_t *moved, int64_t *remained) {
  return pika_migrate_thread_->GetMigrateStatus(ip, port, slot, migrating, moved, remained);
}

int PikaServer::SlotsMigrateOne(const std::string& key, const std::shared_ptr<DB>& db) {
  return pika_migrate_thread_->ReqMigrateOne(key, db);
}

bool PikaServer::SlotsMigrateAsyncCancel() {
  pika_migrate_thread_->CancelMigrate();
  return true;
}

void PikaServer::Bgslotsreload(const std::shared_ptr<DB>& db) {
  // Only one thread can go through
  {
    std::lock_guard ml(bgsave_protector_);
    if (bgslots_reload_.reloading || bgsave_info_.bgsaving) {
      return;
    }
    bgslots_reload_.reloading = true;
  }

  bgslots_reload_.start_time = time(nullptr);
  char s_time[32];
  size_t len = strftime(s_time, sizeof(s_time), "%Y%m%d%H%M%S", localtime(&bgslots_reload_.start_time));
  bgslots_reload_.s_start_time.assign(s_time, len);
  bgslots_reload_.cursor = 0;
  bgslots_reload_.pattern = "*";
  bgslots_reload_.count = 100;
  bgslots_reload_.db = db;

  LOG(INFO) << "Start slot reloading";

  // Start new thread if needed
  bgsave_thread_.StartThread();
  bgsave_thread_.Schedule(&DoBgslotsreload, static_cast<void*>(this));
}

void DoBgslotsreload(void* arg) {
  auto p = static_cast<PikaServer*>(arg);
  PikaServer::BGSlotsReload reload = p->bgslots_reload();

  // Do slotsreload
  rocksdb::Status s;
  std::vector<std::string> keys;
  int64_t cursor_ret = -1;
  while(cursor_ret != 0 && p->GetSlotsreloading()){
    cursor_ret = reload.db->storage()->Scan(storage::DataType::kAll, reload.cursor, reload.pattern, reload.count, &keys);

    std::vector<std::string>::const_iterator iter;
    for (iter = keys.begin(); iter != keys.end(); iter++) {
      std::string key_type;
      int s = GetKeyType(*iter, key_type, reload.db);
      //if key is slotkey, can't add to SlotKey
      if (s > 0){
        if (key_type == "s" && ((*iter).find(SlotKeyPrefix) != std::string::npos || (*iter).find(SlotTagPrefix) != std::string::npos)){
          continue;
        }

        AddSlotKey(key_type, *iter, reload.db);
      }
    }

    reload.cursor = cursor_ret;
    p->SetSlotsreloadingCursor(cursor_ret);
    keys.clear();
  }
  p->SetSlotsreloading(false);

  if (cursor_ret == 0) {
    LOG(INFO) << "Finish slot reloading";
  } else {
    LOG(INFO) << "Stop slot reloading";
  }
}

void PikaServer::Bgslotscleanup(std::vector<int> cleanupSlots, const std::shared_ptr<DB>& db) {
  // Only one thread can go through
  {
    std::lock_guard ml(bgsave_protector_);
    if (bgslots_cleanup_.cleaningup || bgslots_reload_.reloading || bgsave_info_.bgsaving) {
      return;
    }
    bgslots_cleanup_.cleaningup = true;
  }

  bgslots_cleanup_.start_time = time(nullptr);
  char s_time[32];
  size_t len = strftime(s_time, sizeof(s_time), "%Y%m%d%H%M%S", localtime(&bgslots_cleanup_.start_time));
  bgslots_cleanup_.s_start_time.assign(s_time, len);
  bgslots_cleanup_.cursor = 0;
  bgslots_cleanup_.pattern = "*";
  bgslots_cleanup_.count = 100;
  bgslots_cleanup_.db = db;
  bgslots_cleanup_.cleanup_slots.swap(cleanupSlots);

  std::string slotsStr;
  slotsStr.assign(cleanupSlots.begin(), cleanupSlots.end());
  LOG(INFO) << "Start slot cleanup, slots: " << slotsStr << std::endl;

  // Start new thread if needed
  bgslots_cleanup_thread_.StartThread();
  bgslots_cleanup_thread_.Schedule(&DoBgslotscleanup, static_cast<void*>(this));
}
int64_t PikaServer::GetLastSaveTime(const std::string& dir_path) {
  std::vector<std::string> dump_dir;
  // Dump file is not exist
  if (!pstd::FileExists(dir_path)) {
    LOG(INFO) << "Dump file is not exist,path: " << dir_path;
    return 0;
  }
  if (pstd::GetChildren(dir_path, dump_dir) != 0) {
    return 0;
  }
  std::string dump_file = dir_path + dump_dir[0];
  struct stat fileStat;
  if (stat(dump_file.c_str(), &fileStat) == 0) {
    return static_cast<int64_t>(fileStat.st_mtime);
  }
  return 0;
}

void PikaServer::AllClientUnAuth(const std::set<std::string>& users) {
  pika_dispatch_thread_->UnAuthUserAndKillClient(users, acl_->GetUserLock(Acl::DefaultUser));
}

void PikaServer::CheckPubsubClientKill(const std::string& userName, const std::vector<std::string>& allChannel) {
  pika_pubsub_thread_->ConnCanSubscribe(allChannel, [&](const std::shared_ptr<net::NetConn>& conn) -> bool {
    auto pikaConn = std::dynamic_pointer_cast<PikaClientConn>(conn);
    if (pikaConn && pikaConn->UserName() == userName) {
      return true;
    }
    return false;
  });
}

void DoBgslotscleanup(void* arg) {
  auto p = static_cast<PikaServer*>(arg);
  PikaServer::BGSlotsCleanup cleanup = p->bgslots_cleanup();

  // Do slotscleanup
  std::vector<std::string> keys;
  int64_t cursor_ret = -1;
  std::vector<int> cleanupSlots(cleanup.cleanup_slots);
  while (cursor_ret != 0 && p->GetSlotscleaningup()){
    cursor_ret = g_pika_server->bgslots_cleanup_.db->storage()->Scan(storage::DataType::kAll, cleanup.cursor, cleanup.pattern, cleanup.count, &keys);

    std::string key_type;
    std::vector<std::string>::const_iterator iter;
    for (iter = keys.begin(); iter != keys.end(); iter++) {
      if ((*iter).find(SlotKeyPrefix) != std::string::npos || (*iter).find(SlotTagPrefix) != std::string::npos) {
        continue;
      }
      if (std::find(cleanupSlots.begin(), cleanupSlots.end(), GetSlotID(*iter)) != cleanupSlots.end()){
        if (GetKeyType(*iter, key_type, g_pika_server->bgslots_cleanup_.db) <= 0) {
          LOG(WARNING) << "slots clean get key type for slot " << GetSlotID(*iter) << " key " << *iter << " error";
          continue;
        }
        if (DeleteKey(*iter, key_type[0], g_pika_server->bgslots_cleanup_.db) <= 0){
          LOG(WARNING) << "slots clean del for slot " << GetSlotID(*iter) << " key "<< *iter << " error";
        }
      }
    }

    cleanup.cursor = cursor_ret;
    p->SetSlotscleaningupCursor(cursor_ret);
    keys.clear();
  }

  for (int cleanupSlot : cleanupSlots){
    WriteDelKeyToBinlog(GetSlotKey(cleanupSlot), g_pika_server->bgslots_cleanup_.db);
    WriteDelKeyToBinlog(GetSlotsTagKey(cleanupSlot), g_pika_server->bgslots_cleanup_.db);
  }

  p->SetSlotscleaningup(false);
  std::vector<int> empty;
  p->SetCleanupSlots(empty);

  std::string slotsStr;
  slotsStr.assign(cleanup.cleanup_slots.begin(), cleanup.cleanup_slots.end());
  LOG(INFO) << "Finish slots cleanup, slots " << slotsStr;
}

void PikaServer::ResetCacheAsync(uint32_t cache_num, std::shared_ptr<DB> db, cache::CacheConfig *cache_cfg) {
  if (PIKA_CACHE_STATUS_OK == db->cache()->CacheStatus()
      || PIKA_CACHE_STATUS_NONE == db->cache()->CacheStatus()) {

    common_bg_thread_.StartThread();
    BGCacheTaskArg *arg = new BGCacheTaskArg();
    arg->db = db;
    arg->cache_num = cache_num;
    if (cache_cfg == nullptr) {
      arg->task_type = CACHE_BGTASK_RESET_NUM;
    } else {
      arg->task_type = CACHE_BGTASK_RESET_CFG;
      arg->cache_cfg = *cache_cfg;
    }
    common_bg_thread_.Schedule(&DoCacheBGTask, static_cast<void*>(arg));
  } else {
    LOG(WARNING) << "can not reset cache in status: " << db->cache()->CacheStatus();
  }
}

void PikaServer::ClearCacheDbAsync(std::shared_ptr<DB> db) {
  // disable cache temporarily, and restore it after cache cleared
  g_pika_conf->SetCacheDisableFlag();
  if (PIKA_CACHE_STATUS_OK != db->cache()->CacheStatus()) {
    LOG(WARNING) << "can not clear cache in status: " << db->cache()->CacheStatus();
    return;
  }

  common_bg_thread_.StartThread();
  BGCacheTaskArg *arg = new BGCacheTaskArg();
  arg->db = db;
  arg->task_type = CACHE_BGTASK_CLEAR;
  common_bg_thread_.Schedule(&DoCacheBGTask, static_cast<void*>(arg));
}

void PikaServer::DoCacheBGTask(void* arg) {
  std::unique_ptr<BGCacheTaskArg> pCacheTaskArg(static_cast<BGCacheTaskArg*>(arg));
  std::shared_ptr<DB> db = pCacheTaskArg->db;

  switch (pCacheTaskArg->task_type) {
    case CACHE_BGTASK_CLEAR:
      LOG(INFO) << "clear cache start...";
      db->cache()->SetCacheStatus(PIKA_CACHE_STATUS_CLEAR);
      g_pika_server->ResetDisplayCacheInfo(PIKA_CACHE_STATUS_CLEAR, db);
      db->cache()->FlushCache();
      LOG(INFO) << "clear cache finish";
      break;
    case CACHE_BGTASK_RESET_NUM:
      LOG(INFO) << "reset cache num start...";
      db->cache()->SetCacheStatus(PIKA_CACHE_STATUS_RESET);
      g_pika_server->ResetDisplayCacheInfo(PIKA_CACHE_STATUS_RESET, db);
      db->cache()->Reset(pCacheTaskArg->cache_num);
      LOG(INFO) << "reset cache num finish";
      break;
    case CACHE_BGTASK_RESET_CFG:
      LOG(INFO) << "reset cache config start...";
      db->cache()->SetCacheStatus(PIKA_CACHE_STATUS_RESET);
      g_pika_server->ResetDisplayCacheInfo(PIKA_CACHE_STATUS_RESET, db);
      db->cache()->Reset(pCacheTaskArg->cache_num);
      LOG(INFO) << "reset cache config finish";
      break;
    default:
      LOG(WARNING) << "invalid cache task type: " << pCacheTaskArg->task_type;
      break;
  }

  db->cache()->SetCacheStatus(PIKA_CACHE_STATUS_OK);
  if (pCacheTaskArg->reenable_cache) {
    pCacheTaskArg->conf->UnsetCacheDisableFlag();
  }
}

void PikaServer::ResetCacheConfig(std::shared_ptr<DB> db) {
  cache::CacheConfig cache_cfg;
  cache_cfg.maxmemory = g_pika_conf->cache_maxmemory();
  cache_cfg.maxmemory_policy = g_pika_conf->cache_maxmemory_policy();
  cache_cfg.maxmemory_samples = g_pika_conf->cache_maxmemory_samples();
  cache_cfg.lfu_decay_time = g_pika_conf->cache_lfu_decay_time();
  cache_cfg.zset_cache_start_direction = g_pika_conf->zset_cache_start_direction();
  cache_cfg.zset_cache_field_num_per_key = g_pika_conf->zset_cache_field_num_per_key();
  db->cache()->ResetConfig(&cache_cfg);
}

void PikaServer::ClearHitRatio(std::shared_ptr<DB> db) {
  db->cache()->ClearHitRatio();
}

void PikaServer::OnCacheStartPosChanged(int zset_cache_start_direction, std::shared_ptr<DB> db) {
  ResetCacheConfig(db);
  ClearCacheDbAsync(db);
}

void PikaServer::ClearCacheDbAsyncV2(std::shared_ptr<DB> db) {
  if (PIKA_CACHE_STATUS_OK != db->cache()->CacheStatus()) {
    LOG(WARNING) << "can not clear cache in status: " << db->cache()->CacheStatus();
    return;
  }

  common_bg_thread_.StartThread();
  BGCacheTaskArg *arg = new BGCacheTaskArg();
  arg->db = db;
  arg->task_type = CACHE_BGTASK_CLEAR;
  arg->conf = std::move(g_pika_conf);
  arg->reenable_cache = true;
  common_bg_thread_.Schedule(&DoCacheBGTask, static_cast<void*>(arg));
}

void PikaServer::ProcessCronTask() {
  for (auto& dbs : dbs_) {
    auto cache = dbs.second->cache();
    cache->ProcessCronTask();
  }
  LOG(INFO) << "hit rate:" << HitRatio() << std::endl;
}

double PikaServer::HitRatio(void) {
  std::unique_lock l(mu_);
  int64_t hits = 0;
  int64_t misses = 0;
  cache::RedisCache::GetHitAndMissNum(&hits, &misses);
  int64_t all_cmds = hits + misses;
  if (0 >= all_cmds) {
    return 0;
  }
  return hits / (all_cmds * 1.0);
}

void PikaServer::UpdateCacheInfo(void) {
  for (auto& dbs : dbs_) {
    if (PIKA_CACHE_STATUS_OK != dbs.second->cache()->CacheStatus()) {
      return;
    }
    // get cache info from redis cache
    CacheInfo cache_info;
    dbs.second->cache()->Info(cache_info);
    dbs.second->UpdateCacheInfo(cache_info);
  }
}

void PikaServer::ResetDisplayCacheInfo(int status, std::shared_ptr<DB> db) {
  db->ResetDisplayCacheInfo(status);
}

void PikaServer::CacheConfigInit(cache::CacheConfig& cache_cfg) {
  cache_cfg.maxmemory = g_pika_conf->cache_maxmemory();
  cache_cfg.maxmemory_policy = g_pika_conf->cache_maxmemory_policy();
  cache_cfg.maxmemory_samples = g_pika_conf->cache_maxmemory_samples();
  cache_cfg.lfu_decay_time = g_pika_conf->cache_lfu_decay_time();
}