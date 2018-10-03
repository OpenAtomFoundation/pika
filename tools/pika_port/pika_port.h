// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef BINLOG_SYNC_H_
#define BINLOG_SYNC_H_

#include "slash/include/slash_status.h"
#include "slash/include/slash_mutex.h"
#include "include/pika_binlog.h"
#include "include/pika_define.h"
#include "binlog_receiver_thread.h"
#include "slaveping_thread.h"
#include "trysync_thread.h"
#include "redis_sender.h"

#include <vector>

using slash::Status;
using slash::Slice;

class PikaPort
{
public:
  PikaPort(std::string& master_ip, int master_port, std::string& passwd);
  ~PikaPort();

  /*
   * Get & Set 
   */
  std::string& master_ip() {
    return master_ip_;
  }
  int master_port() {
    return master_port_;
  }

  int64_t sid() {
    return sid_;
  }

  void SetSid(int64_t sid) {
    sid_ = sid;
  }

  int role() {
    slash::RWLock(&state_protector_, false);
    return role_;
  }
  int repl_state() {
    slash::RWLock(&state_protector_, false);
    return repl_state_;
  }
  std::string requirepass() {
    return requirepass_;
  }
  pthread_rwlock_t* rwlock() {
      return &rwlock_;
  }
  BinlogReceiverThread* binlog_receiver_thread() {
    return binlog_receiver_thread_;
  }
  TrysyncThread* trysync_thread() {
    return trysync_thread_;
  }
  Binlog* logger() {
    return logger_;
  }

  int SendRedisCommand(const std::string &command, const std::string &key);

  bool SetMaster(std::string& master_ip, int master_port);
  bool ShouldConnectMaster();
  void ConnectMasterDone();
  bool ShouldStartPingMaster();
  void MinusMasterConnection();
  void PlusMasterConnection();
  bool ShouldAccessConnAsMaster(const std::string& ip);
  void RemoveMaster();
  bool IsWaitingDBSync();
  void NeedWaitDBSync();
  void WaitDBSyncFinish();

  void Start();
  void Stop();
  void Cleanup();

  bool Init();
  SlavepingThread* ping_thread_;

private:
  std::string master_ip_;
  int master_port_;
  int master_connection_;
  int role_;
  int repl_state_;
  std::string requirepass_;
  std::string log_path_;
  std::string dump_path_;
  pthread_rwlock_t rwlock_;

  slash::Mutex mutex_; // double lock to block main thread

  // redis client
  // pink::PinkCli *cli_;
  // RedisSender *sender_;
  std::vector<RedisSender*> senders_;

  bool should_exit_;

  // Master use
  int64_t sid_;

  BinlogReceiverThread* binlog_receiver_thread_;
  TrysyncThread* trysync_thread_;

  Binlog *logger_;

  pthread_rwlock_t state_protector_; //protect below, use for master-slave mode

  PikaPort(PikaPort &bs);
  void operator =(const PikaPort &bs);
  void ConnectRedis();
};

#endif
