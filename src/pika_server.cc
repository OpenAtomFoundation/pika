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
#include "env.h"
#include "rsync.h"
#include "pika_server.h"
#include "slash_string.h"
#include "bg_thread.h"
#include "pika_conf.h"
#include "crc32.h"
#include <json/json.h>

extern PikaConf *g_pika_conf;

PikaServer::PikaServer() :
  ping_thread_(NULL),
  slot_logger_(NULL),
  exit_(false),
  sid_(0),
  master_ip_(""),
  master_connection_(0),
  master_port_(0),
  repl_state_(PIKA_REPL_NO_CONNECT),
  role_(PIKA_ROLE_SINGLE),
  slot_sync_(-1),
  bgsave_engine_(NULL),
  purging_(false),
  binlogbg_exit_(false),
  binlogbg_cond_(&binlogbg_mutex_),
  binlogbg_serial_(0),
  accumulative_connections_(0) {

  pthread_rwlockattr_t attr;
  pthread_rwlockattr_init(&attr);
  pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
  pthread_rwlock_init(&rwlock_, &attr);
  
  //Init server ip host
  if (!ServerInit()) {
    LOG(FATAL) << "ServerInit iotcl error";
  }
  // Create nemo handle
  nemo::Options option;

  option.write_buffer_size = g_pika_conf->write_buffer_size();
  option.target_file_size_base = g_pika_conf->target_file_size_base();
  option.max_background_flushes = g_pika_conf->max_background_flushes();
  option.max_background_compactions = g_pika_conf->max_background_compactions();
  option.max_open_files = g_pika_conf->max_cache_files();
	if (g_pika_conf->compression() == "none") {
		 option.compression = false;
  }
  std::string db_path = g_pika_conf->db_path();
  LOG(INFO) << "Prepare DB...";
  db_ = std::shared_ptr<nemo::Nemo>(new nemo::Nemo(db_path, option));
  assert(db_);
  LOG(INFO) << "DB Success";

  // Create thread
  worker_num_ = g_pika_conf->thread_num();
  for (int i = 0; i < worker_num_; i++) {
    pika_worker_thread_[i] = new PikaWorkerThread(1000);
  }

  std::set<std::string> ips;
  if (g_pika_conf->network_interface().empty()) {
    ips.insert("0.0.0.0");
  } else {
    ips.insert("127.0.0.1");
    ips.insert(host_);
  }
  pika_dispatch_thread_ = new PikaDispatchThread(ips, port_, worker_num_, pika_worker_thread_, 3000);
  pika_binlog_receiver_thread_ = new PikaBinlogReceiverThread(ips, port_ + 1000, 1000);
  pika_heartbeat_thread_ = new PikaHeartbeatThread(ips, port_ + 2000, 1000);
  pika_trysync_thread_ = new PikaTrysyncThread();
  pika_trysyncslot_thread_ = new PikaTrysyncSlotThread();
  monitor_thread_ = new PikaMonitorThread();
  
  //for (int j = 0; j < g_pika_conf->binlogbg_thread_num; j++) {
  for (int j = 0; j < g_pika_conf->sync_thread_num(); j++) {
    binlogbg_workers_.push_back(new BinlogBGWorker(g_pika_conf->sync_buffer_size()));
  }

  pthread_rwlock_init(&state_protector_, NULL);
  logger_ = new Binlog(g_pika_conf->log_path(), g_pika_conf->binlog_file_size());
}

PikaServer::~PikaServer() {
  delete bgsave_engine_;

  // DispatchThread will use queue of worker thread,
  // so we need to delete dispatch before worker.
  delete pika_dispatch_thread_;

  for (int i = 0; i < worker_num_; i++) {
    delete pika_worker_thread_[i];
  }

  {
  slash::MutexLock l(&slave_mutex_);
  std::vector<SlaveItem>::iterator iter = slaves_.begin();

  while (iter != slaves_.end()) {
    delete static_cast<PikaBinlogSenderThread*>(iter->sender);
    iter =  slaves_.erase(iter);
    LOG(INFO) << "Delete slave success";
  }
  }
  {
	  slash::MutexLock l(&slot_slave_mutex_);
	  std::vector<SlaveItem>::iterator iter = slot_slaves_.begin();
	  while (iter != slot_slaves_.end()) {
		  delete static_cast<PikaSlotBinlogSenderThread*>(iter->sender);
		  iter =  slot_slaves_.erase(iter);
		  LOG(INFO) << "Delete slot slave success";
	  }
  }
  delete pika_trysync_thread_;
  delete pika_trysyncslot_thread_;
  delete ping_thread_;
  delete pika_binlog_receiver_thread_;

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
  key_scan_thread_.Stop();

  DestoryCmdInfoTable();
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

  getifaddrs(&ifAddrStruct);

  for (ifa = ifAddrStruct; ifa != NULL; ifa = ifa->ifa_next) {
      if (ifa ->ifa_addr->sa_family==AF_INET) { // Check it is
          // a valid IPv4 address
          tmpAddrPtr = &((struct sockaddr_in *)ifa->ifa_addr)->sin_addr;
          char addressBuffer[INET_ADDRSTRLEN];
          inet_ntop(AF_INET, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN);
					if (std::string(ifa->ifa_name) == network_interface) {
						host_ = addressBuffer;
						break;
					}
      }
      else if (ifa->ifa_addr->sa_family==AF_INET6) { // Check it is
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

void PikaServer::Cleanup() {
  // shutdown server
  if (g_pika_conf->daemonize()) {
    unlink(g_pika_conf->pidfile().c_str());
  }

//  DestoryCmdInfoTable();

  //g_pika_server->shutdown = true;
  //sleep(1);

  delete this;
  delete g_pika_conf;
  ::google::ShutdownGoogleLogging();
}

void PikaServer::Start() {
  pika_dispatch_thread_->StartThread();
  pika_binlog_receiver_thread_->StartThread();
  pika_heartbeat_thread_->StartThread();
  pika_trysync_thread_->StartThread();
  pika_trysyncslot_thread_->StartThread();

  time(&start_time_s_);

  //SetMaster("127.0.0.1", 9221);
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
    // wake up every half hour
    int try_num = 0;
    while (!exit_ && try_num++ < 10) {
      sleep(1);
    }
  }
  LOG(INFO) << "Goodbye...";
  Cleanup();
}


void PikaServer::DeleteSlave(int fd) {
  slash::MutexLock l(&slave_mutex_);
  std::vector<SlaveItem>::iterator iter = slaves_.begin();

  while (iter != slaves_.end()) {
    if (iter->hb_fd == fd) {
      //pthread_kill(iter->tid);

      // Remove BinlogSender first
   //   static_cast<PikaBinlogSenderThread*>(iter->sender)->SetExit();
   //   
   //   DLOG(INFO) << "DeleteSlave: start join";
   //   int err = pthread_join(iter->sender_tid, NULL);
   //   DLOG(INFO) << "DeleteSlave: after join";
   //   if (err != 0) {
   //     std::string msg = "can't join thread " + std::string(strerror(err));
   //     LOG(WARNING) << msg;
   //     //return Status::Corruption(msg);
   //   }

      delete static_cast<PikaBinlogSenderThread*>(iter->sender);
      
      slaves_.erase(iter);
      LOG(INFO) << "Delete slave success";
      break;
    }
    iter++;
  }
}

/*
 * Change a new db locate in new_path
 * return true when change success
 * db remain the old one if return false
 */
bool PikaServer::ChangeDb(const std::string& new_path) {
  nemo::Options option;
  option.write_buffer_size = g_pika_conf->write_buffer_size();
  option.target_file_size_base = g_pika_conf->target_file_size_base();
  if (g_pika_conf->compression() == "none") {
    option.compression = false;
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
  db_.reset(new nemo::Nemo(db_path, option));
  assert(db_);
  slash::DeleteDirIfExist(tmp_path);
  LOG(INFO) << "Change db success";
  return true;
}

bool PikaServer::LoadSlotDb(const std::string& new_path) {
  nemo::Options option;
  option.write_buffer_size = g_pika_conf->write_buffer_size();
  option.target_file_size_base = g_pika_conf->target_file_size_base();
  if (g_pika_conf->compression() == "none") {
    option.compression = false;
  }
  std::string db_path = g_pika_conf->db_path();
  std::string tmp_path(db_path);
  if (tmp_path.back() == '/') {
    tmp_path.resize(tmp_path.size() - 1);
  }
  RWLock l(&rwlock_, true);
  LOG(INFO) << "Prepare load slot db from: " << tmp_path;
  nemo::Nemo* new_db = new nemo::Nemo(db_path, option);
  assert(new_db);
  //slash::DeleteDirIfExist(tmp_path);
  LOG(INFO) << "Change db success";
  return true;
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

bool PikaServer::FindSlave(std::string& ip_port) {
//  slash::MutexLock l(&slave_mutex_);
  std::vector<SlaveItem>::iterator iter = slaves_.begin();

  while (iter != slaves_.end()) {
    if (iter->ip_port == ip_port) {
      return true;
    }
    iter++;
  }
  return false;
}

int32_t PikaServer::GetSlaveListString(std::string& slave_list_str) {
  slash::MutexLock l(&slave_mutex_);
  size_t index = 0, slaves_num = slaves_.size();

  std::stringstream tmp_stream;
  std::string slave_ip_port;
  while (index < slaves_num) {
    slave_ip_port = slaves_[index].ip_port;
    tmp_stream << "slave" << index << ":ip=" << slave_ip_port.substr(0, slave_ip_port.find(":"))
                                   << ",port=" << slave_ip_port.substr(slave_ip_port.find(":")+1)
                           << ",state=" << (slaves_[index].stage == SLAVE_ITEM_STAGE_TWO ? "online" : "offline") << "\r\n";
    index++;
  }
  slave_list_str.assign(tmp_stream.str());
  return slaves_num;
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
    repl_state_ = PIKA_REPL_CONNECT;
    LOG(INFO) << "open read-only mode";
    g_pika_conf->SetReadonly(true);
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

//////////////////////////////migrate//////////////////////////////////////
bool PikaServer::SetMigrateSlotMaster(std::string& master_ip, int master_port, int slot) {
  if (master_ip == "127.0.0.1") {
    master_ip = host_;
  }
  slash::RWLock l(&state_protector_, true);
  if (repl_state_ == PIKA_REPL_NO_CONNECT) {
    master_ip_ = master_ip;
    master_port_ = master_port;
    repl_state_ = PIKA_REPL_SLOT_CONNECT;
	slot_sync_ = slot;
    //g_pika_conf->SetReadonly(true);
    LOG(INFO) << "open read-only mode";
    return true;
  }
  return false;
}

bool PikaServer::SlotWaitingDBSync() {
  slash::RWLock l(&state_protector_, false);
  //DLOG(INFO) << "repl_state: " << repl_state_ << " role: " << role_ << " master_connection: " << master_connection_;
  if (repl_state_ == PIKA_REPL_SLOT_WAIT_DBSYNC) {
    return true;
  }
  return false;
}

void PikaServer::SlotNeedWaitDBSync() {
  slash::RWLock l(&state_protector_, true);
  repl_state_ = PIKA_REPL_SLOT_WAIT_DBSYNC;
}

void PikaServer::SlotWaitDBSyncFinish() {
  slash::RWLock l(&state_protector_, true);
  if (repl_state_ == PIKA_REPL_SLOT_WAIT_DBSYNC) {
    repl_state_ = PIKA_REPL_SLOT_CONNECT;
  }
}

void PikaServer::SlotMigrateFinished() {
  slash::RWLock l(&state_protector_, true);
  repl_state_ = PIKA_REPL_NO_CONNECT;
}

bool PikaServer::SlotShouldConnectMaster() {
  slash::RWLock l(&state_protector_, false);
  //DLOG(INFO) << "repl_state: " << repl_state_ << " role: " << role_ << " master_connection: " << master_connection_;
  if (repl_state_ == PIKA_REPL_SLOT_CONNECT) {
    return true;
  }
  return false;
}

void PikaServer::SlotConnectMasterDone() {
  slash::RWLock l(&state_protector_, true);
  if (repl_state_ == PIKA_REPL_SLOT_CONNECT) {
	slot_sync_ = -1;
    repl_state_ = PIKA_REPL_SLOT_CONNECTING;
  }
}
///////////////////////////////////////migrate///////////////////////////////////

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
//  if (repl_state_ != PIKA_REPL_NO_CONNECT && repl_state_ != PIKA_REPL_WAIT_DBSYNC && ip == master_ip_) {
  if (repl_state_ != PIKA_REPL_NO_CONNECT && repl_state_ != PIKA_REPL_WAIT_DBSYNC && ip == master_ip_) {
    return true;
  }
  return false;
}

void PikaServer::SyncError() {

  {
  slash::RWLock l(&state_protector_, true);
  repl_state_ = PIKA_REPL_ERROR;
  }
  if (ping_thread_ != NULL) {
    ping_thread_->should_exit_ = true;
    int err = pthread_join(ping_thread_->thread_id(), NULL);
    if (err != 0) {
      std::string msg = "can't join thread " + std::string(strerror(err));
      LOG(WARNING) << msg;
    }
    delete ping_thread_;
    ping_thread_ = NULL;
  }
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
    ping_thread_->should_exit_ = true;
    int err = pthread_join(ping_thread_->thread_id(), NULL);
    if (err != 0) {
      std::string msg = "can't join thread " + std::string(strerror(err));
      LOG(WARNING) << msg;
    }
    delete ping_thread_;
    ping_thread_ = NULL;
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
  bgsave_thread_.StartIfNeed();
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
  {
    slash::MutexLock l(&bgsave_protector_);
    bg_path = bgsave_info_.path;
  }
  // Get all files need to send
  std::vector<std::string> descendant;
  if (!slash::GetDescendant(bg_path, descendant)) {
    LOG(WARNING) << "Get Descendant when try to do db sync failed";
  }

  // Iterate to send files
  int ret = 0;
  std::string target_path;
  std::string module = kDBSyncModule + "_" + slash::IpPortString(host_, port_);
  std::vector<std::string>::iterator it = descendant.begin();
  slash::RsyncRemote remote(ip, port, module, g_pika_conf->db_sync_speed() * 1024);
  for (; it != descendant.end(); ++it) {
    target_path = (*it).substr(bg_path.size() + 1);
    if (target_path == kBgsaveInfoFile) {
      continue;
    }
    // We need specify the speed limit for every single file
    ret = slash::RsyncSendFile(*it, target_path, remote);
    if (0 != ret) {
      LOG(WARNING) << "rsync send file failed! From: " << *it
        << ", To: " << target_path
        << ", At: " << ip << ":" << port
        << ", Error: " << ret;
      break;
    }
  }
 
  // Clear target path
  slash::RsyncSendClearTarget(bg_path + "/kv", "kv", remote);
  slash::RsyncSendClearTarget(bg_path + "/hash", "hash", remote);
  slash::RsyncSendClearTarget(bg_path + "/list", "list", remote);
  slash::RsyncSendClearTarget(bg_path + "/set", "set", remote);
  slash::RsyncSendClearTarget(bg_path + "/zset", "zset", remote);

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
  }
}
//////////////////////////////////////////////////////////////////////////Slot BgSave///////////////////////////////////////////////////////////////////////////////////////////////////////
std::string PikaServer::MasterMigrateInfo(){
	struct json_object *info_object = json_object_new_object();  
	slash::MutexLock ldb(&slot_bgsave_protector_);
	json_object_object_add(info_object, "bgsaving", json_object_new_int(slot_bgsave_info_.bgsaving));
	json_object_object_add(info_object, "sync_db_file", json_object_new_int(slot_bgsave_info_.sync_db_file));
	json_object_object_add(info_object, "slot", json_object_new_int(slot_bgsave_info_.slot));
	json_object_object_add(info_object, "slave_ip_port", json_object_new_string(slot_bgsave_info_.slave_ip_port.c_str()));

	if (slot_logger_ != NULL){
		uint32_t filenum;
		uint64_t con_offset;
		slot_logger_->GetProducerStatus(&filenum, &con_offset);
		json_object_object_add(info_object, "master_slot_binlog_filenum", json_object_new_int(filenum));
		json_object_object_add(info_object, "master_slot_binlog_con_offset", json_object_new_int(con_offset));
	}else{
		json_object_object_add(info_object, "master_slot_binlog_filenum", json_object_new_int(-1));
		json_object_object_add(info_object, "master_slot_binlog_con_offset", json_object_new_int(-1));
	}

	std::vector<SlaveItem>::iterator iter = slot_slaves_.begin();
	if (iter != slot_slaves_.end()) {
		PikaSlotBinlogSenderThread* sender = static_cast<PikaSlotBinlogSenderThread*>(iter->sender);
		uint32_t filenum = sender->filenum();
		uint64_t con_offset = sender->con_offset();
		json_object_object_add(info_object, "slave_slot_binlog_filenum", json_object_new_int(filenum));
		json_object_object_add(info_object, "slave_slot_binlog_con_offset", json_object_new_int(con_offset));
	}else{
		json_object_object_add(info_object, "slave_slot_binlog_filenum", json_object_new_int(-1));
		json_object_object_add(info_object, "slave_slot_binlog_con_offset", json_object_new_int(-1));
	}

	std::string res = json_object_to_json_string(info_object);
	json_object_put(info_object);
	return res;
}

bool PikaServer::FinishMigrateSlot(uint32_t slot){
	//1.bgsave
	slash::MutexLock ldb(&slot_bgsave_protector_);
	if (slot_bgsave_info_.bgsaving == false){
		return true;
	}
    //2.dump
	if (slot_bgsave_info_.sync_db_file == true){
		return false;
	}
	//3.
	if (slot_bgsave_info_.slot != slot){
		return false;
	}
	//4.slot_logger_
	uint32_t slot_filenum;
	uint64_t slot_con_offset;
	if (slot_logger_ == NULL){
		return false;
	}
	slot_logger_->GetProducerStatus(&slot_filenum, &slot_con_offset);
	std::vector<SlaveItem>::iterator iter = slot_slaves_.begin();
	if (iter != slot_slaves_.end()) {
		PikaSlotBinlogSenderThread* sender = static_cast<PikaSlotBinlogSenderThread*>(iter->sender);
		uint32_t filenum = sender->filenum();
		uint64_t con_offset = sender->con_offset();
		if (filenum == slot_filenum && con_offset == slot_con_offset){
			slot_slaves_.erase(iter);
			delete sender;
			slot_bgsave_info_.bgsaving = false;
			slot_bgsave_info_.slot  = -1;
			delete slot_logger_;
			slot_logger_ = NULL;
			if (!slash::DeleteDirIfExist(slot_bgsave_info_.path)) {
				LOG(WARNING) << "remove exist slotbgsave dir failed";
				return false;
			}
			return true;
		}
	}
	return true;
}

Status PikaServer::AddSlotBinlogSender(SlaveItem &slave, uint32_t slot){
	if (slot >= nemo::MAX_SLOT_NUM){
    	return Status::InvalidArgument("AddSlotBinlogSender invalid slot num");
	}
    std::string slave_ip = slave.ip_port.substr(0, slave.ip_port.find(':'));
	std::string bgsave_path(g_pika_conf->bgsave_path());
	std::string slot_str;
	std::stringstream ss;
	ss << slot;
	ss >> slot_str;
	{
		std::string slot_db_path  = bgsave_path + g_pika_conf->bgsave_prefix() + slave.ip_port + "/" + slot_str + "/";
		slash::MutexLock ldb(&slot_bgsave_protector_);
		if (!slash::FileExists(slot_db_path + "info.done")) {
			if (slot_bgsave_info_.bgsaving == false){
				slot_bgsave_info_.start_time = time(NULL);
				char s_time[32];
				int len = strftime(s_time, sizeof(s_time), "%Y%m%d%H%M%S", localtime(&slot_bgsave_info_.start_time));
				slot_bgsave_info_.s_start_time.assign(s_time, len);
				slot_bgsave_info_.bgsaving = true;
				slot_bgsave_info_.sync_db_file = true;
				slot_bgsave_info_.path = slot_db_path;
				slot_bgsave_info_.slot = slot;
				slot_bgsave_info_.slave_ip_port = slave.ip_port;
				slot_sync_ = slot;	
				TryDBSlotSync(slave_ip, slave.port + 3000, slot);

				if (!slash::DeleteDirIfExist(slot_bgsave_info_.path)) {
					LOG(WARNING) << "remove exist slotbgsave dir failed";
					return Status::Incomplete("Internal Error");
				}

				//binlog
				slash::CreatePath(slot_bgsave_info_.path, 0755);
				slash::CreatePath(slot_bgsave_info_.path + "log", 0755);
				slot_logger_ = new Binlog(slot_bgsave_info_.path + "log/", 1024 * 1024 * 1024);
				if (!slash::DeleteDirIfExist(slot_bgsave_info_.path + "_FAILED")) {
					LOG(WARNING) << "remove exist fail slotbgsave dir failed :";
					return Status::IOError("AddSlotBinlogSender delete DIR");
				}
			}
			return Status::Incomplete("SlotBgsaving and DBSync first");
		}

		//add new slot binglog meta file
		slash::SequentialFile *readfile;
		std::string confile = NewFileName(slot_logger_->filename, 0);
		if (!slash::NewSequentialFile(confile, &readfile).ok()) {
			return Status::IOError("AddSlotBinlogSender new sequtialfile");
		}

		//slot binlog start with filenum=0 and offset=0
		PikaSlotBinlogSenderThread* sender = new PikaSlotBinlogSenderThread(slave_ip, slave.port+1000, readfile, 0, 0);
		sender->StartThread();
		slave.sender = sender;
		slot_slaves_.push_back(slave);
	}
	return Status::OK();
}


void PikaServer::TryDBSlotSync(const std::string& ip, int port, uint32_t slot) {
  SlotBgsave();
  DBSlotSync(ip, port);
}

void PikaServer::DBSlotSync(const std::string& ip, int port) {
  // Only one DBSync task for every ip_port
  // Reuse the slot_bgsave_thread_
  // Since we expect Bgsave and DBSync execute serially
  slot_bgsave_thread_.StartIfNeed();
  DBSyncArg *arg = new DBSyncArg(this, ip, port);
  slot_bgsave_thread_.Schedule(&DoDBSlotSync, static_cast<void*>(arg));
}

void PikaServer::DoDBSlotSync(void* arg) {
  DBSyncArg *ppurge = static_cast<DBSyncArg*>(arg);
  PikaServer* ps = ppurge->p;

  ps->DBSyncSlotSendFile(ppurge->ip, ppurge->port);
  delete (PurgeArg*)arg;
}

void PikaServer::DBSyncSlotSendFile(const std::string& ip, int port) {
  std::string bg_path = slot_bgsave_info_.path;
  // Get all files need to send
  std::vector<std::string> descendant;
  //if (!slash::GetDescendant(bg_path, descendant)) {
  //  LOG(WARNING) << "Get Descendant when try to do db sync failed";
  //}

  // Iterate to send files
  int ret = 0;
  std::string target_path;
  std::string module = kDBSyncModule + "_" + slash::IpPortString(host_, port_);
  printf("module:%s\n",module.c_str());
  std::vector<std::string>::iterator it = descendant.begin();
  slash::RsyncRemote remote(ip, port, module, g_pika_conf->db_sync_speed() * 1024);
  for (; it != descendant.end(); ++it) {
    target_path = (*it).substr(bg_path.size() + 1);
    if (target_path == kBgsaveInfoFile) {
      continue;
    }
    // We need specify the speed limit for every single file
    ret = slash::RsyncSendFile(*it, target_path, remote);
    if (0 != ret) {
      LOG(WARNING) << "rsync send file failed! From: " << *it
        << ", To: " << target_path
        << ", At: " << ip << ":" << port
        << ", Error: " << ret;
      break;
    }else{
      LOG(INFO) << "rsync send file success! From: " << *it
        << ", To: " << target_path
        << ", At: " << ip << ":" << port
        << ", Error: " << ret;
	}
  }
 
  // Clear target path
  slash::RsyncSendClearTarget(bg_path + "/kv", "kv", remote);
  slash::RsyncSendClearTarget(bg_path + "/hash", "hash", remote);
  slash::RsyncSendClearTarget(bg_path + "/list", "list", remote);
  slash::RsyncSendClearTarget(bg_path + "/set", "set", remote);
  slash::RsyncSendClearTarget(bg_path + "/zset", "zset", remote);

  // Send info file at last
  if (0 == ret) {
    if (0 != (ret = slash::RsyncSendFile(bg_path + "/" + kBgsaveInfoFile, kBgsaveInfoFile, remote))) {
      LOG(WARNING) << "send info file failed";
    }
  }
  if (0 == ret) {
  	this->FinishSlotBgsave();
    LOG(INFO) << "rsync send files success";
  }
}

// Prepare engine, need bgsave_protector protect
bool PikaServer::InitSlotBgsaveEnv() {
  return true;
}

// Prepare bgsave env, need bgsave_protector protect
bool PikaServer::InitSlotBgsaveEngine() {
	slot_bgsave_info_.kv_keys.clear();
	slot_bgsave_info_.list_keys.clear();
	slot_bgsave_info_.hash_keys.clear();
	slot_bgsave_info_.set_keys.clear();
	slot_bgsave_info_.zset_keys.clear();

	nemo::Status nemo_s = db_->SlotKeys(slot_bgsave_info_.slot, 
			slot_bgsave_info_.kv_keys, 
			slot_bgsave_info_.list_keys,
			slot_bgsave_info_.hash_keys,
			slot_bgsave_info_.set_keys,
			slot_bgsave_info_.zset_keys);
    if (!nemo_s.ok()){
      LOG(WARNING) << "set backup content failed " << nemo_s.ToString();
      return false;
    }
  return true;
}

bool PikaServer::RunSlotBgsaveEngine(const std::string path) {
  // Backup to tmp dir
  nemo::Options option;
  option.write_buffer_size = g_pika_conf->write_buffer_size();
  option.target_file_size_base = g_pika_conf->target_file_size_base();
  option.max_background_flushes = g_pika_conf->max_background_flushes();
  option.max_background_compactions = g_pika_conf->max_background_compactions();
  option.max_open_files = g_pika_conf->max_cache_files();
	if (g_pika_conf->compression() == "none") {
		 option.compression = false;
  }
  std::string db_path = slot_bgsave_info_.path;
  LOG(INFO) << "Prepare DB... DB Path:" << db_path;
  std::shared_ptr<nemo::Nemo> slot_db = std::shared_ptr<nemo::Nemo>(new nemo::Nemo(db_path, option));
  assert(slot_db);
  LOG(INFO) << "DB Success";

  //TODO:ttl
  //backup kv
  for (size_t i = 0; i < slot_bgsave_info_.kv_keys.size(); ++i){
	  std::string key =  slot_bgsave_info_.kv_keys[i];
	  std::string value;

	  int64_t ttl = 0;
	  nemo::Status ttl_s = db_->TTL(key, &ttl);
	  if (!(ttl_s.ok() || ttl_s.IsNotFound())) {
		  return false;
	  }

	  nemo::Status s = db_->Get(key, &value);
	  if (s.ok()){
		  if (ttl > 0){
		 	slot_db->Set(key, value, ttl);
		  }else{
		 	slot_db->Set(key, value, 0);
		  }
         LOG(INFO) << "==============kv key:[" << key << "] value:[" << value << "] ttl:[" << ttl << "]";
	  }else if (s.IsNotFound()) {

	  }else{
		  return false;
	  }
  }
  LOG(INFO) << "back kv key finished!";
  //backup list
  for (size_t i = 0; i < slot_bgsave_info_.list_keys.size(); ++i){
	  std::string key =  slot_bgsave_info_.list_keys[i];

	  int64_t ttl = 0;
	  nemo::Status ttl_s = db_->TTL(key, &ttl);
	  if (!(ttl_s.ok() || ttl_s.IsNotFound())) {
		  return false;
	  }

	  std::vector<nemo::IV> ivs;
	  nemo::Status s = db_->LRange(key, 0, -1, ivs);
	  std::string val_list;
	  int64_t len;
	  if (s.ok()){
		  std::vector<nemo::IV>::iterator iter;
		  for (iter = ivs.begin(); iter != ivs.end(); iter++) {
			  val_list.append(iter->val);
			  val_list.append(",");
			  slot_db->RPush(key, iter->val, &len);
		  }    
		  if (ttl > 0){
		  	int64_t res;
		  	slot_db->Expire(key, ttl, &res);
		  }
	      LOG(INFO) << "==============list key:[" << key << "] val_list:[" << val_list << "] ttl:[" << ttl << "]";
	  }else if (s.IsNotFound()) {

	  }else{
		return false;
	  }
  }
  LOG(INFO) << "back list key finished!";
  //backup hash
  for (size_t i = 0; i < slot_bgsave_info_.hash_keys.size(); ++i){
	  std::string key =  slot_bgsave_info_.hash_keys[i];

	  int64_t ttl = 0;
	  nemo::Status ttl_s = db_->TTL(key, &ttl);
	  if (!(ttl_s.ok() || ttl_s.IsNotFound())) {
		  return false;
	  }

	  std::vector<nemo::FV> fvs;
	  nemo::Status s = db_->HGetall(key, fvs);
	  std::string fv;
	  if (s.ok()){
		  std::vector<nemo::FV>::const_iterator iter;
		  for (iter = fvs.begin(); iter != fvs.end(); iter++) {
			  fv.append(iter->field);
			  fv.append("=");
			  fv.append(iter->val);
			  fv.append(",");
			  slot_db->HSet(key, iter->field, iter->val);
		  }
		  if (ttl > 0){
		  	int64_t res;
		  	slot_db->Expire(key, ttl, &res);
		  }
          LOG(INFO) << "===========hash key:[" << key << "] fvs:[" << fv << "] ttl:[" << ttl << "]";
	  }else if (s.IsNotFound()) {

	  }else{
		return false;
	  }
  }
  LOG(INFO) << "back hash key finished!";

  //backup set
  for (size_t i = 0; i < slot_bgsave_info_.set_keys.size(); ++i){
	  std::string key =  slot_bgsave_info_.set_keys[i];
	  int64_t ttl = 0;
	  nemo::Status ttl_s = db_->TTL(key, &ttl);
	  if (!(ttl_s.ok() || ttl_s.IsNotFound())) {
		  return false;
	  }

	  std::vector<std::string> members;
	  nemo::Status s = db_->SMembers(key, members);
	  std::string vals;
	  int64_t len;
	  if (s.ok()){
		  for (size_t i = 0; i < members.size(); ++i){
			  vals.append(members[i]);
			  vals.append(",");
			  slot_db->SAdd(key, members[i], &len);
		  }
		  if (ttl > 0){
		  	int64_t res;
		  	slot_db->Expire(key, ttl, &res);
		  }
          LOG(INFO) << "===============set key:[" << key << "] members:[" << vals << "] ttl:[" << ttl << "]";
	  }else if (s.IsNotFound()) {

	  }else{
		return false;
	  }
  }
  LOG(INFO) << "back set key finished!";
  //backup zset
  for (size_t i = 0; i < slot_bgsave_info_.zset_keys.size(); ++i){
	  std::string key =  slot_bgsave_info_.zset_keys[i];
	  int64_t ttl = 0;
	  nemo::Status ttl_s = db_->TTL(key, &ttl);
	  if (!(ttl_s.ok() || ttl_s.IsNotFound())) {
		  return false;
	  }

	  std::vector<nemo::SM> sms_v;
	  nemo::Status s = db_->ZRange(key, 0, -1, sms_v);
	  std::string vals;
      char buf[32];
	  if (s.ok()) {
		  std::vector<nemo::SM>::const_iterator iter = sms_v.begin();
		  int64_t res;
		  for (; iter != sms_v.end(); iter++) {
			  vals.append(iter->member);
			  vals.append("=");
			  slash::d2string(buf, sizeof(buf), iter->score);
			  vals.append(buf);
			  slot_db->ZAdd(key, iter->score, iter->member, &res);
		  }
		  if (ttl > 0){
		  	int64_t res_ttl;
		  	slot_db->Expire(key, ttl, &res_ttl);
		  }
          LOG(INFO) << "==============zset key:[" << key << "] vals:[" << vals << "] ttl:[" << ttl << "]";
	  }else if (s.IsNotFound()) {

	  }else{
		return false;
	  }
  }
  LOG(INFO) << "back zset key finished!";

  std::vector<std::string> keys;
  slot_db->Keys("*", keys);
  std::cout << "=============key size:" << keys.size() << std::endl;

  return true;
}

void PikaServer::SlotBgsave() {
		// Prepare for Bgsaving
	if (!InitSlotBgsaveEnv() || !InitSlotBgsaveEngine()) {
    return;
  }
  LOG(INFO) << "after prepare slotbgsave";
  // Start new thread if needed
  slot_bgsave_thread_.StartIfNeed();
  slot_bgsave_thread_.Schedule(&DoSlotBgsave, static_cast<void*>(this));
}

void PikaServer::DoSlotBgsave(void* arg) {
  PikaServer* p = static_cast<PikaServer*>(arg);
  SlotBGSaveInfo info = p->slot_bgsave_info();
  // Do bgsave
  bool ok = p->RunSlotBgsaveEngine(info.path);
  // Some output
  std::ofstream out;
  out.open(info.path + "/info", std::ios::in | std::ios::trunc);
  if (out.is_open()) {
    out << (time(NULL) - info.start_time) << "s\n"
      << p->host() << "\n" 
      << p->port() << "\n"
      << info.slot << "\n";
    out.close();
  }

  if (!ok) {
	  std::string fail_path = info.path + "_FAILED";
	  slash::RenameFile(info.path.c_str(), fail_path.c_str());
  }else{
	  //verify
	  out.open(info.path + "/info.done", std::ios::in | std::ios::trunc);
	  out.close();
  }
}
//////////////////////////////////////////////////////////////Slot bgsave end///////////////////////////////////////////////////////////////////////////

/*
 * BinlogSender
 */
Status PikaServer::AddBinlogSender(SlaveItem &slave, uint32_t filenum, uint64_t con_offset) {
  // Sanity check
  if (con_offset > logger_->file_size()) {
    return Status::InvalidArgument("AddBinlogSender invalid offset");
  }
  uint32_t cur_filenum = 0;
  uint64_t cur_offset = 0;
  logger_->GetProducerStatus(&cur_filenum, &cur_offset);
  if (cur_filenum < filenum || (cur_filenum == filenum && cur_offset < con_offset)) {
    return Status::InvalidArgument("AddBinlogSender invalid binlog offset");
  }

  std::string slave_ip = slave.ip_port.substr(0, slave.ip_port.find(':'));

  slash::SequentialFile *readfile;
  std::string confile = NewFileName(logger_->filename, filenum);
  if (!slash::FileExists(confile)) {
    // Not found binlog specified by filenum
    TryDBSync(slave_ip, slave.port + 3000, cur_filenum);
    return Status::Incomplete("Bgsaving and DBSync first");
  }
  if (!slash::NewSequentialFile(confile, &readfile).ok()) {
    return Status::IOError("AddBinlogSender new sequtialfile");
  }

  PikaBinlogSenderThread* sender = new PikaBinlogSenderThread(slave_ip, slave.port+1000, readfile, filenum, con_offset);

  slave.sender = sender;

  if (sender->trim() == 0) {
    sender->StartThread();
    pthread_t tid = sender->thread_id();
    slave.sender_tid = tid;

    LOG(INFO) << "AddBinlogSender ok, tid is " << slave.sender_tid << " hd_fd: " << slave.hb_fd << " stage: " << slave.stage;
    // Add sender
//    slash::MutexLock l(&slave_mutex_);
    slaves_.push_back(slave);

    return Status::OK();
  } else {
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
  nemo::Status nemo_s = nemo::BackupEngine::Open(db().get(), &bgsave_engine_);
  if (!nemo_s.ok()) {
    LOG(WARNING) << "open backup engine failed " << nemo_s.ToString();
    return false;
  }

  {
    RWLock l(&rwlock_, true);
    {
      slash::MutexLock l(&bgsave_protector_);
      logger_->GetProducerStatus(&bgsave_info_.filenum, &bgsave_info_.offset);
    }
    nemo_s = bgsave_engine_->SetBackupContent();
    if (!nemo_s.ok()){
      LOG(WARNING) << "set backup content failed " << nemo_s.ToString();
      return false;
    }
  }
  return true;
}

bool PikaServer::RunBgsaveEngine(const std::string path) {
  // Backup to tmp dir
  nemo::Status nemo_s = bgsave_engine_->CreateNewBackup(path);
  LOG(INFO) << "Create new backup finished.";
  
  if (!nemo_s.ok()) {
    LOG(WARNING) << "backup failed :" << nemo_s.ToString();
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
  
  // Prepare for Bgsaving
  if (!InitBgsaveEnv() || !InitBgsaveEngine()) {
    ClearBgsave();
    return;
  }
  LOG(INFO) << "after prepare bgsave";

  // Start new thread if needed
  bgsave_thread_.StartIfNeed();
  bgsave_thread_.Schedule(&DoBgsave, static_cast<void*>(this));
}

void PikaServer::DoBgsave(void* arg) {
  PikaServer* p = static_cast<PikaServer*>(arg);
  BGSaveInfo info = p->bgsave_info();

  // Do bgsave
  bool ok = p->RunBgsaveEngine(info.path);

  // Some output
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

void PikaServer::FinishSlotBgsave(){
    slot_bgsave_info_.sync_db_file = false;
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
  purge_thread_.StartIfNeed();
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
    PikaBinlogSenderThread *pb = static_cast<PikaBinlogSenderThread*>((*it).sender);
    uint32_t filenum = pb->filenum();
    if (index > filenum) { 
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
        (stat(((g_pika_conf->log_path() + it->second)).c_str(), &file_stat) == 0 &&     
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

void PikaServer::AutoPurge() {
  if (!PurgeLogs(0, false, false)) {
    DLOG(WARNING) << "Auto purge failed";
    return;
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
  std::string dbpath = g_pika_conf->db_path();
  if (dbpath[dbpath.length() - 1] == '/') {
    dbpath.erase(dbpath.length() - 1);
  }
  int pos = dbpath.find_last_of('/');
  dbpath = dbpath.substr(0, pos);
  dbpath.append("/deleting");
  slash::RenameFile(g_pika_conf->db_path(), dbpath.c_str());

  LOG(INFO) << "Delete old db...";
  db_.reset();

  nemo::Options option;
  option.write_buffer_size = g_pika_conf->write_buffer_size();
  option.target_file_size_base = g_pika_conf->target_file_size_base();
  if (g_pika_conf->compression() == "none") {
    option.compression = false;
  }
  LOG(INFO) << "Prepare open new db...";
  db_ = std::shared_ptr<nemo::Nemo>(new nemo::Nemo(g_pika_conf->db_path(), option));
  LOG(INFO) << "open new db success";
  PurgeDir(dbpath);
  return true; 
}

void PikaServer::PurgeDir(std::string& path) {
  std::string *dir_path = new std::string(path);
  // Start new thread if needed
  purge_thread_.StartIfNeed();
  purge_thread_.Schedule(&DoPurgeDir, static_cast<void*>(dir_path));
}

void PikaServer::DoPurgeDir(void* arg) {
  std::string path = *(static_cast<std::string*>(arg));
  LOG(INFO) << "Delete dir: " << path << " start";
  slash::DeleteDir(path);
  LOG(INFO) << "Delete dir: " << path << " done";
  delete static_cast<std::string*>(arg);
}

void PikaServer::DispatchBinlogBG(const std::string &key,
    PikaCmdArgsType* argv, const std::string& raw_args,
    uint64_t cur_serial, bool readonly) {
  size_t index = str_hash(key) % binlogbg_workers_.size();
  binlogbg_workers_[index]->Schedule(argv, raw_args, cur_serial, readonly);
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

  nemo::Status s = db_->GetKeyNum(new_key_nums_v);

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

  key_scan_thread_.StartIfNeed();
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
  for (int idx = 0; idx != worker_num_; idx++) {
    pika_worker_thread_[idx]->ThreadClientKill();
  }
  monitor_thread_->ThreadClientKill();
}

int PikaServer::ClientKill(const std::string &ip_port) {
  for (int idx = 0; idx != worker_num_; ++idx) {
    if (pika_worker_thread_[idx]->ThreadClientKill(ip_port)) {
      return 1;
    }
  }
  if (monitor_thread_->ThreadClientKill(ip_port)) {
    return 1;
  }
  return 0;
}

int64_t PikaServer::ClientList(std::vector<ClientInfo> *clients) {
  int64_t clients_num = 0;
  for (int idx = 0; idx != worker_num_; ++idx) {
    clients_num += pika_worker_thread_[idx]->ThreadClientList(clients);
  }
  clients_num += monitor_thread_->ThreadClientList(clients);
  return clients_num;
}

uint64_t PikaServer::ServerQueryNum() {
  uint64_t server_query_num = 0;
  for (int idx = 0; idx != worker_num_; ++idx) {
    server_query_num += pika_worker_thread_[idx]->thread_querynum();
  }
  server_query_num += pika_binlog_receiver_thread_->thread_querynum();
  return server_query_num;
}

void PikaServer::ResetStat() {
  for (int idx = 0; idx != worker_num_; ++idx) {
    pika_worker_thread_[idx]->ResetThreadQuerynum();
  }
  pika_binlog_receiver_thread_->ResetThreadQuerynum();
  accumulative_connections_ = 0;
}

uint64_t PikaServer::ServerCurrentQps() {
  uint64_t server_current_qps = 0;
  for (int idx = 0; idx != worker_num_; ++idx) {
    server_current_qps += pika_worker_thread_[idx]->last_sec_thread_querynum();
  }
  server_current_qps += pika_binlog_receiver_thread_->last_sec_thread_querynum();
  return server_current_qps;
}

