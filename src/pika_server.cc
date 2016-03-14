#include <glog/logging.h>
#include <assert.h>
#include <sys/ioctl.h>
#include <net/if.h>
#include "env.h"
#include "pika_server.h"
#include "slash_string.h"
#include "pika_conf.h"

extern PikaConf *g_pika_conf;

PikaServer::PikaServer() :
  ping_thread_(NULL),
  sid_(0),
  master_ip_(""),
  master_connection_(0),
  master_port_(0),
  repl_state_(PIKA_REPL_NO_CONNECT),
  role_(PIKA_ROLE_SINGLE) {

  pthread_rwlock_init(&rwlock_, NULL);
  
  //Init server ip host
  if (!ServerInit()) {
    LOG(FATAL) << "ServerInit iotcl error";
  }
  // Create nemo handle
  nemo::Options option;

  option.write_buffer_size = g_pika_conf->write_buffer_size();
  option.target_file_size_base = g_pika_conf->target_file_size_base();
  std::string db_path = g_pika_conf->db_path();
  LOG(WARNING) << "Prepare DB...";
  db_ = std::unique_ptr<nemo::Nemo>(new nemo::Nemo(db_path, option));
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
  logger_ = new Binlog("./log");
}

PikaServer::~PikaServer() {
  pthread_rwlock_destroy(&state_protector_);
  pthread_rwlock_destroy(&rwlock_);
  //delete logger_;
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

void PikaServer::Start() {
  pika_dispatch_thread_->StartThread();
  pika_binlog_receiver_thread_->StartThread();
  pika_heartbeat_thread_->StartThread();
  pika_trysync_thread_->StartThread();


  //SetMaster("127.0.0.1", 9221);

  mutex_.Lock();
  mutex_.Lock();
  DLOG(INFO) << "Goodbye...";
  mutex_.Unlock();
}

void PikaServer::DeleteSlave(int fd) {
  slash::MutexLock l(&slave_mutex_);
  std::vector<SlaveItem>::iterator iter = slaves_.begin();

  while (iter != slaves_.end()) {
    if (iter->hb_fd == fd) {
      //pthread_kill(iter->tid);

      // Remove BinlogSender first
      static_cast<PikaBinlogSenderThread*>(iter->sender)->SetExit();
      
      DLOG(INFO) << "DeleteSlave: start join";
      int err = pthread_join(iter->sender_tid, NULL);
      DLOG(INFO) << "DeleteSlave: after join";
      if (err != 0) {
        std::string msg = "can't join thread " + std::string(strerror(err));
        LOG(WARNING) << msg;
        //return Status::Corruption(msg);
      }

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

Status PikaServer::GetSmallestValidLog(uint32_t* max) {
  slash::MutexLock l(&slave_mutex_);
  std::vector<SlaveItem>::iterator iter;

  *max = logger_->version_->pro_num();
  for (iter = slaves_.begin(); iter != slaves_.end(); iter++) {
    int tmp = static_cast<PikaBinlogSenderThread*>(iter->sender)->filenum();
    if (tmp < *max) {
      *max = tmp;
    }
  }

  return Status::OK();
}
