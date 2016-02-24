#include <glog/logging.h>
#include <assert.h>
#include "env.h"
#include "pika_server.h"
#include "pika_conf.h"

extern PikaConf *g_pika_conf;

PikaServer::PikaServer(int port) :
  port_(port),
  master_ip_(""),
  master_connection_(0),
  master_port_(0),
  repl_state_(PIKA_REPL_NO_CONNECT),
  role_(PIKA_ROLE_SINGLE) {

  pthread_rwlock_init(&rwlock_, NULL);
  
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
  pika_binlog_receiver_thread_ = new PikaBinlogReceiverThread(port_ + 100);
  pika_heartbeat_thread_ = new PikaHeartbeatThread(port_ + 200, 1000);
  pika_trysync_thread_ = new PikaTrysyncThread();

  pthread_rwlock_init(&state_protector_, NULL);
  logger = new Binlog("./log");
}

PikaServer::~PikaServer() {
  pthread_rwlock_destroy(&state_protector_);
  pthread_rwlock_destroy(&rwlock_);
  //delete logger;
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
      std::vector<PikaBinlogSenderThread *>::iterator sender = binlog_sender_threads_.begin() + (iter - slaves_.begin());
      (*sender)->SetExit();
      
      int err = pthread_join(iter->sender_tid, NULL);
      if (err != 0) {
        std::string msg = "can't join thread " + std::string(strerror(err));
        LOG(WARNING) << msg;
        //return Status::Corruption(msg);
      }

      delete (*sender);
      binlog_sender_threads_.erase(sender);
      
      slaves_.erase(iter);
      break;
    }
    iter++;
  }
}

bool PikaServer::SetMaster(const std::string& master_ip, int master_port) {
  slash::RWLock l(&state_protector_, true);
  if (role_ == PIKA_ROLE_SINGLE && repl_state_ == PIKA_REPL_NO_CONNECT) {
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
  slash::RWLock l(&state_protector_, true);
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
      repl_state_ = PIKA_REPL_CONNECT;
    }
  }
}

void PikaServer::PlusMasterConnection() {
  slash::RWLock l(&state_protector_, true);
  if (master_connection_ < 2) {
    if ((++master_connection_) >= 2) {
      // two connection with master has been established
      repl_state_ = PIKA_REPL_CONNECTED;
    }
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
  std::string confile = NewFileName(logger->filename, filenum);
  if (!slash::NewSequentialFile(confile, &readfile).ok()) {
    return Status::IOError("AddBinlogSender new sequtialfile");
  }

  std::string slave_ip = slave.ip_port.substr(0, slave.ip_port.find(':'));
  PikaBinlogSenderThread* sender = new PikaBinlogSenderThread(slave_ip, slave.port, readfile, con_offset, filenum);

  if (sender->trim() == 0) {
    sender->StartThread();
    pthread_t tid = sender->thread_id();

    DLOG(INFO) << "AddBinlogSender ok, tid is " << tid;
    // Add sender
    slash::MutexLock l(&slave_mutex_);
    binlog_sender_threads_.push_back(sender);

    return Status::OK();
  } else {
    DLOG(INFO) << "AddBinlogSender failed";
    return Status::NotFound("AddBinlogSender bad sender");
  }
}

Status PikaServer::GetSmallestValidLog(uint32_t* max) {
  slash::MutexLock l(&slave_mutex_);
  std::vector<PikaBinlogSenderThread *>::iterator iter;

  *max = logger->version_->pronum();
  for (iter = binlog_sender_threads_.begin(); iter != binlog_sender_threads_.end(); iter++) {
    int tmp = (*iter)->filenum();
    if (tmp < *max) {
      *max = tmp;
    }
  }

  return Status::OK();
}
