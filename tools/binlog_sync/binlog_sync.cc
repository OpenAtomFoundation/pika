#include <glog/logging.h>
#include <assert.h>
#include <sys/ioctl.h>
#include <net/if.h>
#include "env.h"
#include "binlog_sync.h"
#include "slash_string.h"

BinlogSync::BinlogSync(int64_t filenum, int64_t offset, int port, std::string& master_ip, int master_port, std::string& passwd, std::string& log_path) :
  filenum_(filenum),
  offset_(offset) ,
  ping_thread_(NULL), 
  port_(port),
  master_ip_(master_ip),
  master_port_(master_port),
  master_connection_(0),
  role_(PIKA_ROLE_SINGLE),
  repl_state_(PIKA_REPL_NO_CONNECT),
  requirepass_(passwd),
  log_path_(log_path) {

  pthread_rwlockattr_t attr;
  pthread_rwlockattr_init(&attr);
  pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
  pthread_rwlock_init(&rwlock_, &attr);
  
  //Init ip host
  if (!Init()) {
    LOG(FATAL) << "Init iotcl error";
  }

  // Create thread

  binlog_receiver_thread_ = new BinlogReceiverThread(port_ + 100, 1000);
  trysync_thread_ = new TrysyncThread();
  
  pthread_rwlock_init(&state_protector_, NULL);
  logger_ = new Binlog(log_path, 104857600);
}

BinlogSync::~BinlogSync() {

  delete ping_thread_;
  sleep(1);
  delete binlog_receiver_thread_;
  delete trysync_thread_;

  delete logger_;

  pthread_rwlock_destroy(&state_protector_);
  pthread_rwlock_destroy(&rwlock_);

  DLOG(INFO) << "BinlogSync " << pthread_self() << " exit!!!";
}

bool BinlogSync::Init() {

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

  DLOG(INFO) << "host: " << host_ << " port: " << port_;
	return true;

}

void BinlogSync::Cleanup() {
  // shutdown server
//  if (g_pika_conf->daemonize()) {
//    unlink(g_pika_conf->pidfile().c_str());
//  }

  delete this;
  ::google::ShutdownGoogleLogging();
}

void BinlogSync::Start() {
  trysync_thread_->StartThread();
  binlog_receiver_thread_->StartThread();

  if (filenum_ >= 0 && offset_ >= 0) {
    logger_->SetProducerStatus(filenum_, offset_);
  }
  SetMaster(master_ip_, master_port_);

  mutex_.Lock();
  mutex_.Lock();
  mutex_.Unlock();
  DLOG(INFO) << "Goodbye...";
  Cleanup();
}


bool BinlogSync::SetMaster(std::string& master_ip, int master_port) {
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

bool BinlogSync::ShouldConnectMaster() {
  slash::RWLock l(&state_protector_, false);
  DLOG(INFO) << "repl_state: " << repl_state_ << " role: " << role_ << " master_connection: " << master_connection_;
  if (repl_state_ == PIKA_REPL_CONNECT) {
    return true;
  }
  return false;
}

void BinlogSync::ConnectMasterDone() {
  slash::RWLock l(&state_protector_, true);
  if (repl_state_ == PIKA_REPL_CONNECT) {
    repl_state_ = PIKA_REPL_CONNECTING;
  }
}

bool BinlogSync::ShouldStartPingMaster() {
  slash::RWLock l(&state_protector_, false);
  DLOG(INFO) << "ShouldStartPingMaster: master_connection " << master_connection_ << " repl_state " << repl_state_;
  if (repl_state_ == PIKA_REPL_CONNECTING && master_connection_ < 2) {
    return true;
  }
  return false;
}

void BinlogSync::MinusMasterConnection() {
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

void BinlogSync::PlusMasterConnection() {
  slash::RWLock l(&state_protector_, true);
  if (master_connection_ < 2) {
    if ((++master_connection_) >= 2) {
      // two connection with master has been established
      repl_state_ = PIKA_REPL_CONNECTED;
      master_connection_ = 2;
    }
  }
}

bool BinlogSync::ShouldAccessConnAsMaster(const std::string& ip) {
  slash::RWLock l(&state_protector_, false);
  DLOG(INFO) << "ShouldAccessConnAsMaster, repl_state_: " << repl_state_ << " ip: " << ip << " master_ip: " << master_ip_;
  if (repl_state_ != PIKA_REPL_NO_CONNECT && ip == master_ip_) {
    return true;
  }
  return false;
}

void BinlogSync::RemoveMaster() {

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
}
