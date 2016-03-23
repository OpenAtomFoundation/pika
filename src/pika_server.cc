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
  bgsaving_(false),
  purging_(false) {

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
  for (int i = 0; i < PIKA_MAX_WORKER_THREAD_NUM; i++) {
    pika_worker_thread_[i] = new PikaWorkerThread(1000);
  }

  pika_dispatch_thread_ = new PikaDispatchThread(port_, PIKA_MAX_WORKER_THREAD_NUM, pika_worker_thread_, 3000);
  pika_binlog_receiver_thread_ = new PikaBinlogReceiverThread(port_ + 100, 1000);
  pika_heartbeat_thread_ = new PikaHeartbeatThread(port_ + 200, 1000);
  pika_trysync_thread_ = new PikaTrysyncThread();

  pthread_rwlock_init(&state_protector_, NULL);
  logger_ = new Binlog(g_pika_conf->log_path(), g_pika_conf->binlog_file_size());
}

PikaServer::~PikaServer() {
  pthread_rwlock_destroy(&state_protector_);
  pthread_rwlock_destroy(&rwlock_);
  if (bgsave_engine_ != NULL) {
    delete bgsave_engine_;
  }

  delete logger_;
  db_.reset();

  for (int i = 0; i < PIKA_MAX_WORKER_THREAD_NUM; i++) {
    delete pika_worker_thread_[i];
  }

  delete pika_dispatch_thread_;
  delete pika_binlog_receiver_thread_;
  delete pika_trysync_thread_;
  delete pika_heartbeat_thread_;

  //TODO
  //delete pika_slaveping_thread_;

  DLOG(INFO) << "PikaServer " << pthread_self() << " exit!!!";
}

bool PikaServer::ServerInit() {
	char hname[128];
	struct hostent *hent;

	gethostname(hname, sizeof(hname));
	hent = gethostbyname(hname);

	host_ = inet_ntoa(*(struct in_addr*)(hent->h_addr_list[0]));

	port_ = g_pika_conf->port();	
  DLOG(INFO) << "host: " << host_ << " port: " << port_;
	return true;
}

void PikaServer::Cleanup() {
  // shutdown server
  if (g_pika_conf->daemonize()) {
    unlink(g_pika_conf->pidfile().c_str());
  }

  DestoryCmdInfoTable();

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
  slash::MutexLock l(&slave_mutex_);
  std::vector<SlaveItem>::iterator iter = slaves_.begin();

  while (iter != slaves_.end()) {
    if (iter->ip_port == ip_port) {
      return true;
    }
    iter++;
  }
  return false;
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
  DLOG(INFO) << "ShouldStartPingMaster: master_connection " << master_connection_;
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
    ping_thread_->SetExit();
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
  if (con_offset > kBinlogSize) {
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
    slash::MutexLock l(&slave_mutex_);
    slaves_.push_back(slave);

    return Status::OK();
  } else {
    DLOG(INFO) << "AddBinlogSender failed";
    return Status::NotFound("AddBinlogSender bad sender");
  }
}

bool PikaServer::InitBgsaveEnv(const std::string& bgsave_path) {
  // Prepare for bgsave dir
  bgsave_info_.start_time = time(NULL);
  char s_time[32];
  int len = strftime(s_time, sizeof(s_time), "%Y%m%d%H%M%S", localtime(&bgsave_info_.start_time));
  bgsave_info_.s_start_time.assign(s_time, len);
  bgsave_info_.path = bgsave_path + std::string(s_time, 8);
  if (!slash::DeleteDirIfExist(bgsave_info_.path)) {
    LOG(ERROR) << "remove exist bgsave dir failed";
    return false;
  }
  slash::CreateDir(bgsave_info_.path);
  // Prepare for tmp dir
  bgsave_info_.tmp_path = bgsave_path + "tmp";
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

bool PikaServer::RunBgsaveEngine() {
  // Backup to tmp dir
  nemo::Status nemo_s = bgsave_engine_->CreateNewBackup(db().get());
  LOG(INFO) << "create new backup finished.";
  // Restore to bgsave dir
  if (nemo_s.ok()) {
    nemo_s = bgsave_engine_->RestoreDBFromBackup(
        bgsave_engine_->GetLatestBackupID() + 1, bgsave_info_.path);
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
  bool expect = false;
  if (!bgsaving_.compare_exchange_strong(expect, true)) {
    return;
  }

  // Prepare for Bgsaving
  if (!InitBgsaveEnv(g_pika_conf->bgsave_path())
      || !InitBgsaveEngine()) {
    ClearBgsave();
    return;
  }

  // Start new thread if needed
  if (!bgsave_thread_.is_running()) {
    bgsave_thread_.StartThread();
  }
  bgsave_thread_.Schedule(&DoBgsave, static_cast<void*>(this));
}

void PikaServer::DoBgsave(void* arg) {
  PikaServer* p = static_cast<PikaServer*>(arg);
  const BGSaveInfo& info = p->bgsave_info();
  
  // Do bgsave
  bool ok = p->RunBgsaveEngine();

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

  p->ClearBgsave();
}

bool PikaServer::Bgsaveoff() {
  if (!bgsaving_) {
    return false;
  }
  if (bgsave_engine_ != NULL) {
    bgsave_engine_->StopBackup();
  }
  return true;
}

bool PikaServer::PurgeLogs(uint32_t to) {
  // Only one thread can go through
  bool expect = false;
  if (!purging_.compare_exchange_strong(expect, true)) {
    LOG(WARNING) << "purge process already exist";
    return false;
  }
  uint32_t max = 0;
  if (!GetPurgeWindow(max)){
    LOG(WARNING) << "no need to purge";
    return false;
  }
  LOG(WARNING) << "max seqnum could be deleted: " << max;
  if (to > max) {
    LOG(WARNING) << "seqnum:" << to << " larger than max could be deleted: " << max;
    ClearPurge();
    return false;
  }
  PurgeArg *arg = new PurgeArg();
  arg->p = this;
  arg->to = to;
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
  
  ps->PurgeFiles(ppurge->to);

  ps->ClearPurge();
  delete (PurgeArg*)arg;
}

bool PikaServer::GetPurgeWindow(uint32_t &max) {
  max = logger_->version_->pro_num();
  slash::MutexLock l(&slave_mutex_);
  std::vector<SlaveItem>::iterator it;
  for (it = slaves_.begin(); it != slaves_.end(); ++it) {
    PikaBinlogSenderThread *pb = static_cast<PikaBinlogSenderThread*>((*it).sender);
    uint32_t filenum = pb->filenum();
    max = filenum < max ? filenum : max;
  }
  // remain some more
  if (max > 10) {
    max -= 10;
    return true;
  }
  return false;
}

bool PikaServer::PurgeFiles(uint32_t to)
{
  std::vector<std::string> binlogs;
  if (!GetBinlogFiles(binlogs)) {
    return false;
  }

  int delete_num = 0;
  std::vector<std::string>::iterator it;
  for (it = binlogs.begin(); it != binlogs.end(); ++it) {
    if (stoul((*it).substr(kBinlogPrefixLen)) > to) {
      continue;
    }
    slash::Status s = slash::DeleteFile(g_pika_conf->log_path() + *it);
    if (!s.ok()) {
      LOG(ERROR) << "Purge log file : " << *it <<  " failed! error:" << s.ToString();
    } else {
      ++delete_num;
    }
  }
  LOG(INFO) << "Success purge "<< delete_num << " files to index : " << to;

  return true;
}

bool PikaServer::GetBinlogFiles(std::vector<std::string>& binlogs) {
  std::vector<std::string> children;
  int ret = slash::GetChildren(g_pika_conf->log_path(), children);
  if (ret != 0){
    LOG(ERROR) << "Get all files in log path failed! error:" << ret; 
    return false;
  }
  std::vector<std::string>::iterator it;
  for (it = children.begin(); it != children.end(); ++it) {
    if ((*it).compare(0, kBinlogPrefixLen, kBinlogPrefix) == 0) {
      binlogs.push_back(*it);
    }
  }
  return true;
}

// Return auto purge up index, return -1 if no need to purge
int PikaServer::GetAutoPurgeUpIndex()
{
  int expire_logs_nums = g_pika_conf->expire_logs_nums();
  int expire_logs_days = g_pika_conf->expire_logs_days();
  time_t deadline = time(NULL) - expire_logs_days * 24 * 1024;
  
  // Retrive all binlog files
  std::vector<std::string> binlogs;
  if (!GetBinlogFiles(binlogs)) {
    return -1;
  }

  // Get max index and max expire index
  int up = -1, t_up = -1;
  struct stat file_stat;
  std::vector<std::string>::iterator it;
  for (it = binlogs.begin(); it != binlogs.end(); ++it) {
      int cur = stol((*it).substr(kBinlogPrefixLen));
      up = up > cur ? up : cur;
      if (0 != stat((*it).c_str(), &file_stat)) continue;
      if (cur > t_up && file_stat.st_mtime <= deadline) {
        t_up = cur;
      }
  }
  
  // Calc auto purge index
  int index = -1;
  if (binlogs.size() >= expire_logs_nums) {
    index = up - expire_logs_nums;
  }
  index = index < t_up ? t_up : index; //choose the large one
  return index;
}

void PikaServer::AutoPurge() {
  int index = GetAutoPurgeUpIndex();
  if (index > 0) {
    LOG(INFO) << "Do Auto Purge"; 
    if (!PurgeLogs(index)) {
      LOG(ERROR) << "Auto purge failed"; 
    }
  }
}

bool PikaServer::FlushAll() {
  if (bgsaving_ /*|| bgscaning_*/) {
    return false;
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

void PikaServer::ClientKillAll() {
  for (size_t idx = 0; idx != PIKA_MAX_WORKER_THREAD_NUM; idx++) {
    pika_worker_thread_[idx]->ThreadClientKill();
  }  
}

int PikaServer::ClientKill(const std::string &ip_port) {
  for (size_t idx = 0; idx != PIKA_MAX_WORKER_THREAD_NUM; idx++) {
    if (pika_worker_thread_[idx]->ThreadClientKill(ip_port)) {
      return 1;
    }
  }
  return 0;
}

void PikaServer::ClientList(std::vector< std::pair<int, std::string> > &clients) {
  for (size_t idx = 0; idx != PIKA_MAX_WORKER_THREAD_NUM; idx++) {
    pika_worker_thread_[idx]->ThreadClientList(clients); 
  }
}
