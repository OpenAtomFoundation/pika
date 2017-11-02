// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_SERVER_H_
#define PIKA_SERVER_H_

#include <vector>
#include <functional>
#include <map>
#include <unordered_set>
#include <sys/statfs.h>
#include <nemo.h>
#include <time.h>

#include "pika_binlog.h"
#include "pika_binlog_receiver_thread.h"
#include "pika_binlog_sender_thread.h"
#include "pika_heartbeat_thread.h"
#include "pika_slaveping_thread.h"
#include "pika_trysync_thread.h"
#include "pika_monitor_thread.h"
#include "pika_define.h"
#include "pika_binlog_bgworker.h"

#include "slash/include/slash_status.h"
#include "slash/include/slash_mutex.h"
#include "pink/include/bg_thread.h"
#include "pink/include/pink_pubsub.h"
#include "nemo_backupable.h"

using slash::Status;
using slash::Slice;

class PikaDispatchThread;

class PikaServer {
 public:
  PikaServer();
  ~PikaServer();

  static uint64_t DiskSize(std::string path) {
    struct statfs diskInfo;
    int ret = statfs(path.c_str(), &diskInfo);
    if (ret == -1) {
      LOG(WARNING) << "Get DiskSize error: " << strerror(errno);
      return 0;
    }
    return diskInfo.f_bsize * diskInfo.f_blocks;
  }

  /*
   * Get & Set
   */
  std::string host() {
    return host_;
  }
  int port() {
    return port_;
  }
  time_t start_time_s() {
    return start_time_s_;
  }
  std::string master_ip() {
    return master_ip_;
  }
  int master_port() {
    return master_port_;
  }
  const std::shared_ptr<nemo::Nemo> db() {
    return db_;
  }


  int role() {
    slash::RWLock(&state_protector_, false);
    return role_;
  }
  int repl_state() {
    slash::RWLock(&state_protector_, false);
    return repl_state_;
  }
  bool force_full_sync() {
    return force_full_sync_;
  }
  void SetForceFullSync(bool v) {
    force_full_sync_ = v;
  }
  /*
   * Master use
   */
  int64_t GenSid() {
    // slave_mutex has been locked from exterior
    int64_t sid = sid_;
    sid_++;
    return sid;
  }

  void DeleteSlave(int fd); // hb_fd
  void DeleteSlave(const std::string& ip, int64_t port);
  int64_t TryAddSlave(const std::string& ip, int64_t port);
  bool SetSlaveSender(const std::string& ip, int64_t port,
      PikaBinlogSenderThread* s);
  int32_t GetSlaveListString(std::string& slave_list_str);
  Status GetSmallestValidLog(uint32_t* max);
  void MayUpdateSlavesMap(int64_t sid, int32_t hb_fd);
  void BecomeMaster();

  slash::Mutex slave_mutex_; // protect slaves_;
  std::vector<SlaveItem> slaves_;

  /*
   * Slave use
   */
  bool SetMaster(std::string& master_ip, int master_port);
  bool ShouldConnectMaster();
  void ConnectMasterDone();
  bool ShouldStartPingMaster();
  void MinusMasterConnection();
  void PlusMasterConnection();
  bool ShouldAccessConnAsMaster(const std::string& ip);
  void SyncError();
  void RemoveMaster();
  bool WaitingDBSync();
  void NeedWaitDBSync();
  void WaitDBSyncFinish();
  void KillBinlogSenderConn();

  void Start();
  void Exit() {
    exit_ = true;
  }

  void SetBinlogIoError(bool error) {
    binlog_io_error_ = error;
  }

  bool BinlogIoError() {
    return binlog_io_error_;
  }

  void DoTimingTask();

  PikaSlavepingThread* ping_thread_;

  /*
   * Server init info
   */
  bool ServerInit();

  /*
   * Binlog
   */
  Binlog *logger_;
  Status AddBinlogSender(const std::string& ip, int64_t port,
      uint32_t filenum, uint64_t con_offset);

  /*
   * BGSave used
   */
  struct BGSaveInfo {
    bool bgsaving;
    time_t start_time;
    std::string s_start_time;
    std::string path;
    uint32_t filenum;
    uint64_t offset;
    BGSaveInfo() : bgsaving(false), filenum(0), offset(0){}
    void Clear() {
      bgsaving = false;
      path.clear();
      filenum = 0;
      offset = 0;
    }
  };
  BGSaveInfo bgsave_info() {
    slash::MutexLock l(&bgsave_protector_);
    return bgsave_info_;
  }
  bool bgsaving() {
    slash::MutexLock l(&bgsave_protector_);
    return bgsave_info_.bgsaving;
  }
  void Bgsave();
  bool Bgsaveoff();
  bool RunBgsaveEngine(const std::string path);
  void FinishBgsave() {
    slash::MutexLock l(&bgsave_protector_);
    bgsave_info_.bgsaving = false;
  }


  /*
   * BGSlotsReload used
   */
  struct BGSlotsReload {
    bool reloading;
    time_t start_time;
    std::string s_start_time;
    int64_t cursor;
    std::string pattern;
    int64_t count;
    BGSlotsReload() : reloading(false), cursor(0), pattern("*"), count(100){}
    void Clear() {
      reloading = false;
      pattern = "*";
      count = 100;
      cursor = 0;
    }
  };
  BGSlotsReload bgslots_reload() {
    slash::MutexLock l(&bgsave_protector_);
    return bgslots_reload_;
  }
  bool GetSlotsreloading() {
    slash::MutexLock l(&bgsave_protector_);
    return bgslots_reload_.reloading;
  }
  void SetSlotsreloading(bool reloading) {
    slash::MutexLock l(&bgsave_protector_);
    bgslots_reload_.reloading = reloading;
  }
  void SetSlotsreloadingCursor(int64_t cursor) {
    slash::MutexLock l(&bgsave_protector_);
    bgslots_reload_.cursor = cursor;
  }
  int64_t GetSlotsreloadingCursor() {
    slash::MutexLock l(&bgsave_protector_);
    return bgslots_reload_.cursor;
  }
  void Bgslotsreload();
  void StopBgslotsreload() {
    slash::MutexLock l(&bgsave_protector_);
    bgslots_reload_.reloading = false;
  }

  /*
   * PurgeLog used
   */
  struct PurgeArg {
    PikaServer *p;
    uint32_t to;
    bool manual;
    bool force; // Ignore the delete window
  };
  bool PurgeLogs(uint32_t to, bool manual, bool force);
  bool PurgeFiles(uint32_t to, bool manual, bool force);
  bool GetPurgeWindow(uint32_t &max);
  void ClearPurge() {
    purging_ = false;
  }

  /*
   * DBSync used
   */
  struct DBSyncArg {
    PikaServer *p;
    std::string ip;
    int port;
    DBSyncArg(PikaServer *_p, const std::string& _ip, int &_port)
      : p(_p), ip(_ip), port(_port) {}
  };
  void DBSyncSendFile(const std::string& ip, int port);
  bool ChangeDb(const std::string& new_path);
  int CountSyncSlaves() {
    slash::MutexLock ldb(&db_sync_protector_);
    return db_sync_slaves_.size();
  }
  slash::Mutex & GetSlavesMutex() { return db_sync_protector_; }

  //flushall
  bool FlushAll();
  void PurgeDir(std::string& path);

  /*
   *Keyscan used
   */
  struct KeyScanInfo {
    time_t start_time;
    std::string s_start_time;
    std::vector<uint64_t> key_nums_v; //the order is kv, hash, list, zset, set
    bool key_scaning_;
    KeyScanInfo() : start_time(0), s_start_time("1970-01-01 08:00:00"), key_nums_v({0, 0, 0, 0, 0}), key_scaning_(false) {
    }
  };
  bool key_scaning() {
    slash::MutexLock lm(&key_scan_protector_);
    return key_scan_info_.key_scaning_;
  }
  KeyScanInfo key_scan_info() {
    slash::MutexLock lm(&key_scan_protector_);
    return key_scan_info_;
  }
  void KeyScan();
  void RunKeyScan();
  void StopKeyScan();


  /*
   * client related
   */
  void ClientKillAll();
  int ClientKill(const std::string &ip_port);
  int64_t ClientList(std::vector<ClientInfo> *clients = nullptr);

  // rwlock_
  void RWLockWriter();
  void RWLockReader();
  void RWUnlock();

  /*
   * PubSub used
   */
  int Publish(int fd, const std::string& channel, const std::string& msg);
  void Subscribe(pink::PinkConn* conn, const std::vector<std::string> channels, std::map<std::string, int>& result);
  void UnSubscribe(pink::PinkConn* conn, std::vector<std::string> channels, std::map<std::string, int>& result);

  /*
   * Monitor used
   */
  void AddMonitorClient(PikaClientConn* client_ptr);
  void AddMonitorMessage(const std::string &monitor_message);
  bool HasMonitorClients();

  /*
   * Binlog Receiver use
   */
  void DispatchBinlogBG(const std::string &key,
      PikaCmdArgsType* argv, const std::string& raw_args,
      uint64_t cur_serial, bool readonly);
  bool WaitTillBinlogBGSerial(uint64_t my_serial);
  void SignalNextBinlogBGSerial();

  /*
   *for statistic
   */
  void PlusThreadQuerynum();
  uint64_t ServerQueryNum();
  uint64_t ServerCurrentQps();
  void ResetLastSecQuerynum(); /* Invoked in PikaDispatchThread's CronHandle */
  uint64_t accumulative_connections() {
    return statistic_data_.accumulative_connections;
  }
  void incr_accumulative_connections() {
    ++statistic_data_.accumulative_connections;  
  }
  void ResetStat();
  slash::RecordMutex mutex_record_;

 private:
  std::atomic<bool> exit_;
  std::atomic<bool> binlog_io_error_;
  std::string host_;
  int port_;
  pthread_rwlock_t rwlock_;
  std::shared_ptr<nemo::Nemo> db_;

  time_t start_time_s_;
  bool have_scheduled_crontask_;
  struct timeval last_check_compact_time_;

  int worker_num_;
  PikaDispatchThread* pika_dispatch_thread_;

  PikaBinlogReceiverThread* pika_binlog_receiver_thread_;
  PikaHeartbeatThread* pika_heartbeat_thread_;
  PikaTrysyncThread* pika_trysync_thread_;

  /*
   * Master use
   */
  int64_t sid_;

  /*
   * Slave use
   */
  pthread_rwlock_t state_protector_; //protect below, use for master-slave mode
  std::string master_ip_;
  int master_connection_;
  int master_port_;
  int repl_state_;
  int role_;
  bool force_full_sync_;

  /*
   * Bgsave use
   */
  slash::Mutex bgsave_protector_;
  pink::BGThread bgsave_thread_;
  nemo::BackupEngine *bgsave_engine_;
  BGSaveInfo bgsave_info_;

  static void DoBgsave(void* arg);
  bool InitBgsaveEnv();
  bool InitBgsaveEngine();
  void ClearBgsave() {
    slash::MutexLock l(&bgsave_protector_);
    bgsave_info_.Clear();
  }

  /*
   * BGSlotsReload use
   */
  BGSlotsReload bgslots_reload_;
  static void DoBgslotsreload(void* arg);

  /*
   * Purgelogs use
   */
  std::atomic<bool> purging_;
  pink::BGThread purge_thread_;

  static void DoPurgeLogs(void* arg);
  bool GetBinlogFiles(std::map<uint32_t, std::string>& binlogs);
  void AutoCompactRange();
  void AutoPurge();
  void AutoDeleteExpiredDump();
  bool CouldPurge(uint32_t index);

  /*
   * DBSync use
   */
  slash::Mutex db_sync_protector_;
  std::unordered_set<std::string> db_sync_slaves_;
  void TryDBSync(const std::string& ip, int port, int32_t top);
  void DBSync(const std::string& ip, int port);
  static void DoDBSync(void* arg);

  /*
   * Flushall use
   */
  static void DoPurgeDir(void* arg);
  /*
   * Keyscan use
   */
  slash::Mutex key_scan_protector_;
  pink::BGThread key_scan_thread_;
  KeyScanInfo key_scan_info_;

  /*
   * Monitor use
   */
  PikaMonitorThread* monitor_thread_;

  /*
   *  Pubsub use
   */
  pink::PubSubThread * pika_pubsub_thread_;

  /*
   * Binlog Receiver use
   */
  bool binlogbg_exit_;
  slash::Mutex binlogbg_mutex_;
  slash::CondVar binlogbg_cond_;
  uint64_t binlogbg_serial_;
  std::vector<BinlogBGWorker*> binlogbg_workers_;
  std::hash<std::string> str_hash;


  /*
   * for statistic
   */
  struct StatisticData {
    StatisticData()
        : accumulative_connections(0),
          thread_querynum(0),
          last_thread_querynum(0),
          last_sec_thread_querynum(0),
          last_time_us(0) {
    }

    slash::RWMutex statistic_lock;
    std::atomic<uint64_t> accumulative_connections;
    uint64_t thread_querynum;
    uint64_t last_thread_querynum;
    uint64_t last_sec_thread_querynum;
    uint64_t last_time_us;
  };
  StatisticData statistic_data_;

  static void DoKeyScan(void *arg);
  void InitKeyScan();

  PikaServer(PikaServer &ps);
  void operator =(const PikaServer &ps);
};

#endif
