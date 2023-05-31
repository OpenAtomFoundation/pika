// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <arpa/inet.h>
#include <net/if.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <cassert>

#include <glog/logging.h>
#include <functional>

#include "conf.h"
#include "const.h"
#include "pika_port.h"
#include "pstd/include/env.h"
#include "pstd/include/rsync.h"
#include "pstd/include/pstd_string.h"

PikaPort::PikaPort(std::string& master_ip, int master_port, std::string& passwd)
    : ping_thread_(nullptr),
      master_ip_(master_ip),
      master_port_(master_port),
      master_connection_(0),
      role_(PIKA_ROLE_PORT),
      repl_state_(PIKA_REPL_NO_CONNECT),
      requirepass_(passwd),
      should_exit_(false),
      sid_(0) {
  // Init ip host
  if (!Init()) {
    LOG(FATAL) << "Init iotcl error";
  }

  // Create redis sender
  size_t thread_num = g_conf.forward_thread_num;
  for (size_t i = 0; i < thread_num; i++) {
    senders_.emplace_back(new RedisSender(static_cast<int>(i), g_conf.forward_ip, g_conf.forward_port, g_conf.forward_passwd));
  }

  // Create thread
  binlog_receiver_thread_ = new BinlogReceiverThread(g_conf.local_ip, g_conf.local_port + 1000, 1000);
  trysync_thread_ = new TrysyncThread();

  logger_ = new Binlog(g_conf.log_path, 104857600);
}

PikaPort::~PikaPort() {
  LOG(INFO) << "Ending...";
  delete trysync_thread_;
  delete ping_thread_;
  sleep(1);
  delete binlog_receiver_thread_;

  delete logger_;

  LOG(INFO) << "PikaPort " << pthread_self() << " exit!!!";
}

bool PikaPort::Init() {
  // LOG(INFO) << "host: " << g_conf.local_ip << " port: " << g_conf.local_port;
  return true;
}

void PikaPort::Cleanup() {
  // shutdown server
  if (ping_thread_) {
    ping_thread_->StopThread();
  }
  trysync_thread_->Stop();
  size_t thread_num = g_conf.forward_thread_num;
  for (size_t i = 0; i < thread_num; i++) {
    senders_[i]->Stop();
  }
  for (size_t i = 0; i < thread_num; i++) {
    // senders_[i]->set_should_stop();
    senders_[i]->JoinThread();
  }
  int64_t replies = 0;
  for (size_t i = 0; i < thread_num; i++) {
    replies += senders_[i]->elements();
    delete senders_[i];
  }
  LOG(INFO) << "=============== Syncing =====================";
  LOG(INFO) << "Total replies : " << replies << " received from redis server";

  delete this;  // PikaPort is a global object
  // ::google::ShutdownGoogleLogging();
}

void PikaPort::Start() {
  // start redis sender threads
  size_t thread_num = g_conf.forward_thread_num;
  for (size_t i = 0; i < thread_num; i++) {
    senders_[i]->StartThread();
  }

  // sender_->StartThread();
  trysync_thread_->StartThread();
  binlog_receiver_thread_->StartThread();

  // if (g_conf.filenum >= 0 && g_conf.filenum != UINT32_MAX && g_conf.offset >= 0) {
  if (g_conf.filenum != UINT32_MAX) {
    logger_->SetProducerStatus(g_conf.filenum, g_conf.offset);
  }
  SetMaster(master_ip_, master_port_);

  mutex_.lock();
  mutex_.lock();
  mutex_.unlock();
  LOG(INFO) << "Goodbye...";
  Cleanup();
}

void PikaPort::Stop() { mutex_.unlock(); }

int PikaPort::SendRedisCommand(const std::string& command, const std::string& key) {
  // Send command
  size_t idx = std::hash<std::string>()(key) % g_conf.forward_thread_num;
  senders_[idx]->SendRedisCommand(command);

  return 0;
}

bool PikaPort::SetMaster(std::string& master_ip, int master_port) {
  std::lock_guard l(state_protector_);
  if (((role_ ^ PIKA_ROLE_SLAVE) != 0) && repl_state_ == PIKA_REPL_NO_CONNECT) {
    master_ip_ = master_ip;
    master_port_ = master_port;
    // role_ |= PIKA_ROLE_SLAVE;
    role_ = PIKA_ROLE_PORT;
    repl_state_ = PIKA_REPL_CONNECT;
    LOG(INFO) << "set role_ = PIKA_ROLE_PORT, repl_state_ = PIKA_REPL_CONNECT";
    return true;
  }

  return false;
}

bool PikaPort::ShouldConnectMaster() {
  std::shared_lock l(state_protector_);
  // LOG(INFO) << "repl_state: " << PikaState(repl_state_)
  //            << " role: " << PikaRole(role_)
  //   		 << " master_connection: " << master_connection_;
  return repl_state_ == PIKA_REPL_CONNECT;
}

void PikaPort::ConnectMasterDone() {
  std::lock_guard l(state_protector_);
  if (repl_state_ == PIKA_REPL_CONNECT) {
    repl_state_ = PIKA_REPL_CONNECTING;
  }
}

bool PikaPort::ShouldStartPingMaster() {
  std::shared_lock l(state_protector_);
  LOG(INFO) << "ShouldStartPingMaster: master_connection " << master_connection_ << ", repl_state "
            << PikaState(repl_state_);
  return repl_state_ == PIKA_REPL_CONNECTING && master_connection_ < 2;
}

void PikaPort::MinusMasterConnection() {
  std::lock_guard l(state_protector_);
  if (master_connection_ > 0) {
    if ((--master_connection_) <= 0) {
      // two connection with master has been deleted
      if (((role_ & PIKA_ROLE_SLAVE) != 0) || ((role_ & PIKA_ROLE_PORT) != 0)) {
        // not change by slaveof no one, so set repl_state = PIKA_REPL_CONNECT, continue to connect master
        repl_state_ = PIKA_REPL_CONNECT;
      } else {
        // change by slaveof no one, so set repl_state = PIKA_REPL_NO_CONNECT, reset to SINGLE state
        repl_state_ = PIKA_REPL_NO_CONNECT;
      }
      master_connection_ = 0;
    }
  }
}

void PikaPort::PlusMasterConnection() {
  std::lock_guard l(state_protector_);
  if (master_connection_ < 2) {
    if ((++master_connection_) >= 2) {
      // two connection with master has been established
      repl_state_ = PIKA_REPL_CONNECTED;
      LOG(INFO) << "Start Sync...";
      master_connection_ = 2;
    }
  }
}

bool PikaPort::ShouldAccessConnAsMaster(const std::string& ip) {
  std::shared_lock l(state_protector_);
  LOG(INFO) << "ShouldAccessConnAsMaster, repl_state_: " << PikaState(repl_state_) << ", ip: " << ip
            << ", master_ip: " << master_ip_;
  return repl_state_ != PIKA_REPL_NO_CONNECT && ip == master_ip_;
}

void PikaPort::RemoveMaster() {
  {
    std::lock_guard l(state_protector_);
    repl_state_ = PIKA_REPL_NO_CONNECT;
    role_ &= ~PIKA_ROLE_SLAVE;
    master_ip_ = "";
    master_port_ = -1;
  }
  if (ping_thread_) {
    int err = ping_thread_->StopThread();
    if (err != 0) {
      LOG(WARNING) << "can't join thread " << strerror(err);
    }
    delete ping_thread_;
    ping_thread_ = nullptr;
  }
}

bool PikaPort::IsWaitingDBSync() {
  std::shared_lock l(state_protector_);
  return repl_state_ == PIKA_REPL_WAIT_DBSYNC;
}

void PikaPort::NeedWaitDBSync() {
  std::lock_guard l(state_protector_);
  repl_state_ = PIKA_REPL_WAIT_DBSYNC;
}

void PikaPort::WaitDBSyncFinish() {
  std::lock_guard l(state_protector_);
  if (repl_state_ == PIKA_REPL_WAIT_DBSYNC) {
    repl_state_ = PIKA_REPL_CONNECT;
  }
}
