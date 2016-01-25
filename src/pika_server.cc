#include <glog/logging.h>
#include "pika_server.h"

PikaServer::PikaServer(int port) :
  port_(port),
  master_ip_(""),
  master_connection_(0),
  master_port_(0),
  repl_state_(PIKA_REPL_NO_CONNECT),
  role_(PIKA_ROLE_SINGLE) {

  for (int i = 0; i < PIKA_MAX_WORKER_THREAD_NUM; i++) {
    pika_worker_thread_[i] = new PikaWorkerThread(1000);
  }

  pika_dispatch_thread_ = new PikaDispatchThread(port_, PIKA_MAX_WORKER_THREAD_NUM, pika_worker_thread_, 3000);
  pika_binlog_receiver_thread_ = new PikaBinlogReceiverThread(port_ + 100);
  pika_heartbeat_thread_ = new PikaHeartbeatThread(port_ + 200, 1000);
  pika_trysync_thread_ = new PikaTrysyncThread();
}

void PikaServer::Start() {
  pika_dispatch_thread_->StartThread();
  pika_binlog_receiver_thread_->StartThread();
  pika_heartbeat_thread_->StartThread();
  pika_trysync_thread_->StartThread();


  SetMaster("127.0.0.1", 9211);

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
