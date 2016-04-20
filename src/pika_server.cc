#include <fstream>
#include <glog/logging.h>
#include <assert.h>
#include <sys/ioctl.h>
#include <net/if.h>
#include "env.h"
#include "pika_server.h"
#include "slash_string.h"
#include "bg_thread.h"
#include "pika_conf.h"

extern PikaConf *g_pika_conf;

PikaServer::PikaServer() :
  exit_(false),
  ping_thread_(NULL),
  sid_(0),
  master_ip_(""),
  master_connection_(0),
  master_port_(0),
  repl_state_(PIKA_REPL_NO_CONNECT),
  role_(PIKA_ROLE_SINGLE),
  purging_(false),
  accumulative_connections_(0) {

  pthread_rwlock_init(&rwlock_, NULL);
  
  //Init server ip host
  if (!ServerInit()) {
    LOG(FATAL) << "ServerInit iotcl error";
  }
  // Create nemo handle
  nemo::Options option;

  option.write_buffer_size = g_pika_conf->write_buffer_size();
  option.target_file_size_base = g_pika_conf->target_file_size_base();
	if (g_pika_conf->compression() == "none") {
		 option.compression = false;
  }
  std::string db_path = g_pika_conf->db_path();
  LOG(WARNING) << "Prepare DB...";
  db_ = std::shared_ptr<nemo::Nemo>(new nemo::Nemo(db_path, option));
  assert(db_);
  LOG(WARNING) << "DB Success";

  // Create thread
  worker_num_ = g_pika_conf->thread_num();
  for (int i = 0; i < worker_num_; i++) {
    pika_worker_thread_[i] = new PikaWorkerThread(1000);
  }

  pika_dispatch_thread_ = new PikaDispatchThread(port_, worker_num_, pika_worker_thread_, 3000);
  pika_binlog_receiver_thread_ = new PikaBinlogReceiverThread(port_ + 100, 1000);
  pika_heartbeat_thread_ = new PikaHeartbeatThread(port_ + 200, 1000);
  pika_trysync_thread_ = new PikaTrysyncThread();
  monitor_thread_ = new PikaMonitorThread();

  pthread_rwlock_init(&state_protector_, NULL);
  logger_ = new Binlog(g_pika_conf->log_path(), g_pika_conf->binlog_file_size());
}

PikaServer::~PikaServer() {
  if (bgsave_engine_ != NULL) {
    delete bgsave_engine_;
  }

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
    DLOG(INFO) << "Delete slave success";
  }
  }
  delete ping_thread_;
  delete pika_binlog_receiver_thread_;
  delete pika_trysync_thread_;
  delete pika_heartbeat_thread_;
  delete monitor_thread_;

  DestoryCmdInfoTable();
  delete logger_;
  db_.reset();
  pthread_rwlock_destroy(&state_protector_);
  pthread_rwlock_destroy(&rwlock_);

  DLOG(INFO) << "PikaServer " << pthread_self() << " exit!!!";
}

bool PikaServer::ServerInit() {

  int fd;
  struct ifreq ifr;

  fd = socket(AF_INET, SOCK_DGRAM, 0);

  /* I want to get an IPv4 IP address */
  ifr.ifr_addr.sa_family = AF_INET;

  /* I want IP address attached to "eth0" */
  strncpy(ifr.ifr_name, "eth0", IFNAMSIZ-1);

  ioctl(fd, SIOCGIFADDR, &ifr);

  close(fd);
  host_ = inet_ntoa(((struct sockaddr_in *)&ifr.ifr_addr)->sin_addr);

	port_ = g_pika_conf->port();	
  DLOG(INFO) << "host: " << host_ << " port: " << port_;
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

  time(&start_time_s_);

  //SetMaster("127.0.0.1", 9221);

  DLOG(WARNING) << "Pika Server going to start";
  while (!exit_) {
    DoTimingTask();
    // wake up every half hour
    int try_num = 0;
    while (!exit_ && try_num++ < 10) {
      sleep(1);
    }
  }
  DLOG(INFO) << "Goodbye...";
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
      DLOG(INFO) << "Delete slave success";
      break;
    }
    iter++;
  }
}

void PikaServer::MayUpdateSlavesMap(int64_t sid, int32_t hb_fd) {
  slash::MutexLock l(&slave_mutex_);
  std::vector<SlaveItem>::iterator iter = slaves_.begin();
  DLOG(INFO) << "MayUpdateSlavesMap, sid: " << sid << " hb_fd: " << hb_fd;
  while (iter != slaves_.end()) {
    if (iter->sid == sid) {
      iter->hb_fd = hb_fd;
      iter->stage = SLAVE_ITEM_STAGE_TWO;
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
  while (index < slaves_num) {
    tmp_stream << "slave" << index << ": host_port=" << slaves_[index].ip_port
                           << " state=" << (slaves_[index].stage == SLAVE_ITEM_STAGE_TWO ? "online" : "offline") << "\r\n";
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
    DLOG(INFO) << "open read-only mode";
    g_pika_conf->SetReadonly(true);
    return true;
  }
  return false;
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
    }
  }
}

bool PikaServer::ShouldAccessConnAsMaster(const std::string& ip) {
  slash::RWLock l(&state_protector_, false);
  DLOG(INFO) << "ShouldAccessConnAsMaster, repl_state_: " << repl_state_ << " ip: " << ip << " master_ip: " << master_ip_;
  if (repl_state_ != PIKA_REPL_NO_CONNECT && ip == master_ip_) {
    return true;
  }
  return false;
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
  DLOG(INFO) << "close read-only mode";
  g_pika_conf->SetReadonly(false);
}

/*
 * BinlogSender
 */
Status PikaServer::AddBinlogSender(SlaveItem &slave, uint32_t filenum, uint64_t con_offset) {
  if (con_offset > logger_->file_size()) {
    return Status::InvalidArgument("AddBinlogSender invalid offset");
  }

  slash::SequentialFile *readfile;
  std::string confile = NewFileName(logger_->filename, filenum);
  if (!slash::NewSequentialFile(confile, &readfile).ok()) {
    return Status::IOError("AddBinlogSender new sequtialfile");
  }

  std::string slave_ip = slave.ip_port.substr(0, slave.ip_port.find(':'));
  PikaBinlogSenderThread* sender = new PikaBinlogSenderThread(slave_ip, slave.port+100, readfile, filenum, con_offset);

  slave.sender = sender;

  if (sender->trim() == 0) {
    sender->StartThread();
    pthread_t tid = sender->thread_id();
    slave.sender_tid = tid;

    DLOG(INFO) << "AddBinlogSender ok, tid is " << slave.sender_tid << " hd_fd: " << slave.hb_fd << " stage: " << slave.stage;
    // Add sender
//    slash::MutexLock l(&slave_mutex_);
    slaves_.push_back(slave);

    return Status::OK();
  } else {
    DLOG(INFO) << "AddBinlogSender failed";
    return Status::NotFound("AddBinlogSender bad sender");
  }
}

// Prepare engine, need bgsave_protector protect
bool PikaServer::InitBgsaveEnv(const std::string& bgsave_path) {
  // Prepare for bgsave dir
  bgsave_info_.start_time = time(NULL);
  char s_time[32];
  int len = strftime(s_time, sizeof(s_time), "%Y%m%d%H%M%S", localtime(&bgsave_info_.start_time));
  bgsave_info_.s_start_time.assign(s_time, len);
  bgsave_info_.path = bgsave_path + std::string(s_time, 8);
  bgsave_info_.tmp_path = bgsave_path + "tmp";
  if (!slash::DeleteDirIfExist(bgsave_info_.path)) {
    LOG(ERROR) << "remove exist bgsave dir failed";
    return false;
  }
  slash::CreateDir(bgsave_info_.path);
  // Prepare for tmp dir
  if (!slash::DeleteDirIfExist(bgsave_info_.tmp_path)) {
    LOG(ERROR) << "remove exist tmp bgsave dir failed";
    return false;
  }
  // Prepare for failed dir
  if (!slash::DeleteDirIfExist(bgsave_info_.path + "_FAILED")) {
    LOG(ERROR) << "remove exist fail bgsave dir failed :";
    return false;
  }
  return true;
}

// Prepare bgsave env, need bgsave_protector protect
bool PikaServer::InitBgsaveEngine() {
  if (bgsave_engine_ != NULL) {
    delete bgsave_engine_;
  }
  nemo::Status nemo_s = nemo::BackupEngine::Open(
      nemo::BackupableOptions(bgsave_info_.tmp_path, true, false), 
      &bgsave_engine_);
  if (!nemo_s.ok()) {
    LOG(ERROR) << "open backup engine failed " << nemo_s.ToString();
    return false;
  }

  {
    RWLock l(&rwlock_, true);
    logger_->GetProducerStatus(&bgsave_info_.filenum, &bgsave_info_.offset);
    nemo_s = bgsave_engine_->SetBackupContent(db_.get());
    if (!nemo_s.ok()){
      LOG(ERROR) << "set backup content failed " << nemo_s.ToString();
      return false;
    }
  }
  return true;
}

bool PikaServer::RunBgsaveEngine(const std::string path) {
  // Backup to tmp dir
  nemo::Status nemo_s = bgsave_engine_->CreateNewBackup(db().get());
  LOG(INFO) << "create new backup finished.";
  // Restore to bgsave dir
  if (nemo_s.ok()) {
    nemo_s = bgsave_engine_->RestoreDBFromBackup(
        bgsave_engine_->GetLatestBackupID() + 1, path);
  }
  LOG(INFO) << "backup finished.";
  
  if (!nemo_s.ok()) {
    LOG(ERROR) << "backup failed :" << nemo_s.ToString();
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

    // Prepare for Bgsaving
    if (!InitBgsaveEnv(g_pika_conf->bgsave_path())
        || !InitBgsaveEngine()) {
      ClearBgsave();
      return;
    }
  }

  // Start new thread if needed
  if (!bgsave_thread_.is_running()) {
    bgsave_thread_.StartThread();
  }
  bgsave_thread_.Schedule(&DoBgsave, static_cast<void*>(this));
}

void PikaServer::DoBgsave(void* arg) {
  PikaServer* p = static_cast<PikaServer*>(arg);
  BGSaveInfo info = p->bgsave_info();

  // Do bgsave
  bool ok = p->RunBgsaveEngine(info.path);

  // Delete tmp
  if (!slash::DeleteDirIfExist(info.tmp_path)) {
    LOG(ERROR) << "remove tmp bgsave dir failed";
  }
  // Some output
  std::ofstream out;
  out.open(info.path + "/info", std::ios::in | std::ios::trunc);
  if (out.is_open()) {
    out << (time(NULL) - info.start_time) << "s\r\n"
      << p->host() << "\r\n" 
      << p->port() << "\r\n"
      << info.filenum << "\r\n"
      << info.offset << "\r\n";
    out.close();
  }
  if (!ok) {
    std::string fail_path = info.path + "_FAILED";
    slash::RenameFile(info.path.c_str(), fail_path.c_str());
  }
  {
    slash::MutexLock l(p->bgsave_protector());
    p->ClearBgsave();
  }
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

bool PikaServer::PurgeLogs(uint32_t to, bool manual) {
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
  // Start new thread if needed
  if (!purge_thread_.is_running()) {
    purge_thread_.StartThread();
  }
  purge_thread_.Schedule(&DoPurgeLogs, static_cast<void*>(arg));
  return true;
}

void PikaServer::DoPurgeLogs(void* arg) {
  PurgeArg *ppurge = static_cast<PurgeArg*>(arg);
  PikaServer* ps = ppurge->p;

  ps->PurgeFiles(ppurge->to, ppurge->manual);

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

bool PikaServer::PurgeFiles(uint32_t to, bool manual)
{
  std::map<uint32_t, std::string> binlogs;
  if (!GetBinlogFiles(binlogs)) {
    LOG(ERROR) << "Could not get binlog files!";
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
      if (!CouldPurge(it->first)) {
        LOG(INFO) << "Could not purge "<< (it->first) << ", since it is already be used";
        return false;
      }

      // Do delete
      slash::Status s = slash::DeleteFile(g_pika_conf->log_path() + it->second);
      if (s.ok()) {
        ++delete_num;
        --remain_expire_num;
      } else {
        LOG(ERROR) << "Purge log file : " << (it->second) <<  " failed! error:" << s.ToString();
      }
    } else {
      // Break when face the first one not satisfied
      // Since the binlogs is order by the file index
      break;
    }
  }
  LOG(INFO) << "Success purge "<< delete_num << " files to index : " << to;

  return true;
}

bool PikaServer::GetBinlogFiles(std::map<uint32_t, std::string>& binlogs) {
  std::vector<std::string> children;
  int ret = slash::GetChildren(g_pika_conf->log_path(), children);
  if (ret != 0){
    LOG(ERROR) << "Get all files in log path failed! error:" << ret; 
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
  if (!PurgeLogs(0, false)) {
    LOG(WARNING) << "Auto purge failed";
    return;
  }
  LOG(INFO) << "Auto Purge sucess";
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

  LOG(WARNING) << "Delete old db...";
  db_.reset();

  nemo::Options option;
  option.write_buffer_size = g_pika_conf->write_buffer_size();
  option.target_file_size_base = g_pika_conf->target_file_size_base();
  if (g_pika_conf->compression() == "none") {
    option.compression = false;
  }
  LOG(WARNING) << "Prepare open new db...";
  db_ = std::shared_ptr<nemo::Nemo>(new nemo::Nemo(g_pika_conf->db_path(), option));
  LOG(WARNING) << "open new db success";
  PurgeDir(dbpath);
  return true; 
}

void PikaServer::PurgeDir(std::string& path) {
  std::string *dir_path = new std::string(path);
  // Start new thread if needed
  if (!purge_thread_.is_running()) {
    purge_thread_.StartThread();
  }
  purge_thread_.Schedule(&DoPurgeDir, static_cast<void*>(dir_path));
}

void PikaServer::DoPurgeDir(void* arg) {
  std::string path = *(static_cast<std::string*>(arg));
  DLOG(INFO) << "Delete dir: " << path << " start";
  slash::DeleteDir(path);
  DLOG(INFO) << "Delete dir: " << path << " done";
  delete static_cast<std::string*>(arg);
}

void PikaServer::RunKeyScan() {
  std::vector<uint64_t> new_key_nums_v;
  db_->GetKeyNum(new_key_nums_v);
  slash::MutexLock lm(&key_scan_protector_);
  key_scan_info_.key_nums_v = new_key_nums_v;
  key_scan_info_.key_scaning_ = false;
}

void PikaServer::DoKeyScan(void *arg) {
  PikaServer *p = reinterpret_cast<PikaServer*>(arg);
  p->RunKeyScan();
}

void PikaServer::KeyScan() {
  key_scan_protector_.Lock();
  if (key_scan_info_.key_scaning_) {
    key_scan_protector_.Unlock();
    return;
  }
  key_scan_info_.key_scaning_ = true; 
  key_scan_protector_.Unlock();

  if (!key_scan_thread_.is_running()) {
    key_scan_thread_.StartThread(); 
  }
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
  for (size_t idx = 0; idx != worker_num_; idx++) {
    pika_worker_thread_[idx]->ThreadClientKill();
  }
  monitor_thread_->ThreadClientKill();
}

int PikaServer::ClientKill(const std::string &ip_port) {
  for (size_t idx = 0; idx != worker_num_; ++idx) {
    if (pika_worker_thread_[idx]->ThreadClientKill(ip_port)) {
      return 1;
    }
  }
  if (monitor_thread_->ThreadClientKill(ip_port)) {
    return 1;
  }
  return 0;
}

int64_t PikaServer::ClientList(std::vector< std::pair<int, std::string> > *clients) {
  int64_t clients_num = 0;
  for (size_t idx = 0; idx != worker_num_; ++idx) {
    clients_num += pika_worker_thread_[idx]->ThreadClientList(clients);
  }
  clients_num += monitor_thread_->ThreadClientList(clients);
  return clients_num;
}

uint64_t PikaServer::ServerQueryNum() {
  uint64_t server_query_num = 0;
  for (size_t idx = 0; idx != worker_num_; ++idx) {
    server_query_num += pika_worker_thread_[idx]->thread_querynum();
  }
  server_query_num += pika_binlog_receiver_thread_->thread_querynum();
  return server_query_num;
}

uint64_t PikaServer::ServerCurrentQps() {
  uint64_t server_current_qps = 0;
  for (size_t idx = 0; idx != worker_num_; ++idx) {
    server_current_qps += pika_worker_thread_[idx]->last_sec_thread_querynum();
  }
  server_current_qps += pika_binlog_receiver_thread_->last_sec_thread_querynum();
  return server_current_qps;
}
