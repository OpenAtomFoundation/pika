#ifndef PIKA_SERVER_H_
#define PIKA_SERVER_H_

#include <vector>
#include <functional>
#include <map>
#include <unordered_set>
#include <nemo.h>
#include <time.h>
#include "pika_binlog.h"
#include "pika_binlog_receiver_thread.h"
#include "pika_binlog_sender_thread.h"
#include "pika_heartbeat_thread.h"
#include "pika_slaveping_thread.h"
#include "pika_dispatch_thread.h"
#include "pika_trysync_thread.h"
#include "pika_worker_thread.h"
#include "pika_monitor_thread.h"
#include "pika_define.h"
#include "pika_binlog_bgworker.h"

#include "slash_status.h"
#include "slash_mutex.h"
#include "bg_thread.h"
#include "nemo_backupable.h"

using slash::Status;
using slash::Slice;

class PikaServer
{
public:
  PikaServer();
  ~PikaServer();

  /*
   * Get & Set 
   */
  std::string host() {
    return host_;
  }
  int port() {
    return port_;
  };
  time_t start_time_s() {
    return start_time_s_;
  }
  PikaWorkerThread** pika_worker_thread() {
    return pika_worker_thread_;
  };
  PikaDispatchThread* pika_dispatch_thread() {
    return pika_dispatch_thread_;
  };
  PikaBinlogReceiverThread* pika_binlog_receiver_thread() {
    return pika_binlog_receiver_thread_;
  }
  PikaHeartbeatThread* pika_heartbeat_thread() {
    return pika_heartbeat_thread_;
  }
  PikaTrysyncThread* pika_trysync_thread() {
    return pika_trysync_thread_;
  }
  std::string& master_ip() {
    return master_ip_;
  }
  int master_port() {
    return master_port_;
  }
  pthread_rwlock_t* rwlock() {
      return &rwlock_;
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
/*
 * Master use
 */
  int64_t GenSid() {
//    slash::MutexLock l(&slave_mutex_);
    int64_t sid = sid_;
    sid_++;
    return sid;
  }

  void DeleteSlave(int fd); // hb_fd
  bool FindSlave(std::string& ip_port);
  int32_t GetSlaveListString(std::string& slave_list_str);
  Status GetSmallestValidLog(uint32_t* max);
  void MayUpdateSlavesMap(int64_t sid, int32_t hb_fd);
  void BecomeMaster(); 

  slash::Mutex slave_mutex_; // protect slaves_;
  std::vector<SlaveItem> slaves_;
//  pthread_mutex_t binlog_sender_mutex_;
  std::vector<PikaBinlogSenderThread *> binlog_sender_threads_;

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

  void Start();
  void Exit() {
    exit_ = true;
  }
  void DoTimingTask() {
    AutoPurge();
  }
  void Cleanup();

  PikaSlavepingThread* ping_thread_;
  //slash::Mutex mutex_; // double lock to block main thread

/*
 * Server init info
 */
  bool ServerInit();

/*
 * Binlog
 */
  Binlog *logger_;
  Status AddBinlogSender(SlaveItem &slave, uint32_t filenum, uint64_t con_offset);

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
  int64_t ClientList(std::vector<ClientInfo> *clients = NULL);

/*
 * Monitor used
 */
  PikaMonitorThread* monitor_thread() {
    return monitor_thread_;
  }

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
  uint64_t ServerQueryNum();
  uint64_t ServerCurrentQps();
  uint64_t accumulative_connections() {
    return accumulative_connections_;
  }
  void incr_accumulative_connections() {
    ++accumulative_connections_;  
  }
  void ResetStat();
  slash::RecordMutex mutex_record_;

private:
  std::atomic<bool> exit_;
  std::string host_;
  int port_;
  pthread_rwlock_t rwlock_;
  std::shared_ptr<nemo::Nemo> db_;

  time_t start_time_s_;

  int worker_num_;
  PikaWorkerThread* pika_worker_thread_[PIKA_MAX_WORKER_THREAD_NUM];
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
   * Purgelogs use
   */
  std::atomic<bool> purging_;
  pink::BGThread purge_thread_;
  
  static void DoPurgeLogs(void* arg);
  bool GetBinlogFiles(std::map<uint32_t, std::string>& binlogs);
  void AutoPurge();
  bool CouldPurge(uint32_t index);

  /*
   * DBSync use
   */
  std::unordered_set<std::string> db_sync_slaves;
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
  std::atomic<uint64_t> accumulative_connections_;

  static void DoKeyScan(void *arg);
  void InitKeyScan();

  PikaServer(PikaServer &ps);
  void operator =(const PikaServer &ps);
};




#endif
