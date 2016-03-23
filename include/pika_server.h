#ifndef PIKA_SERVER_H_
#define PIKA_SERVER_H_

#include <vector>
#include <list>
#include <nemo.h>

#include "pika_binlog.h"
#include "pika_binlog_receiver_thread.h"
#include "pika_binlog_sender_thread.h"
#include "pika_heartbeat_thread.h"
#include "pika_slaveping_thread.h"
#include "pika_dispatch_thread.h"
#include "pika_trysync_thread.h"
#include "pika_worker_thread.h"
#include "pika_define.h"

#include "slash_status.h"
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
  

/*
 * Master use
 */
  int64_t GenSid() {
    slash::MutexLock l(&slave_mutex_);
    int64_t sid = sid_;
    sid_++;
    return sid;
  }

  void DeleteSlave(int fd); // hb_fd
  bool FindSlave(std::string& ip_port);
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
  void RemoveMaster();

  void Start();
  void Cleanup();

  PikaSlavepingThread* ping_thread_;
  slash::Mutex mutex_; // double lock to block main thread

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
    time_t start_time;
    std::string s_start_time;
    std::string path;
    std::string tmp_path;
    uint32_t filenum;
    uint64_t offset;
    BGSaveInfo() : filenum(0), offset(0){}
    void Clear() {
      path.clear();
      tmp_path.clear();
      filenum = 0;
      offset = 0;
    }
  };
  const BGSaveInfo& bgsave_info() const {
    return bgsave_info_;
  }
  void Bgsave();
  bool Bgsaveoff();
  bool RunBgsaveEngine();
  void ClearBgsave() {
    bgsave_info_.Clear();
    bgsaving_ = false;
  }

/*
 * PurgeLog used
 */
  struct PurgeArg {
    PikaServer *p;
    uint32_t to;  
  };
  bool PurgeLogs(uint32_t to);
  bool PurgeFiles(uint32_t to);
  void ClearPurge() {
    purging_ = false;
  }
  //flushall
  bool FlushAll();
  void PurgeDir(std::string& path);

/*
 * client related
 */
  void ClientKillAll();
  int ClientKill(const std::string &ip_port);
  void ClientList(std::vector< std::pair<int, std::string> > &clients);


private:
  std::string host_;
  int port_;
  pthread_rwlock_t rwlock_;
  std::shared_ptr<nemo::Nemo> db_;

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
  std::atomic<bool> bgsaving_;
  pink::BGThread bgsave_thread_;
  nemo::BackupEngine *bgsave_engine_;
  BGSaveInfo bgsave_info_;
  
  static void DoBgsave(void* arg);
  bool InitBgsaveEnv(const std::string& bgsave_path);
  bool InitBgsaveEngine();

  /*
   * Purgelogs use
   */
  std::atomic<bool> purging_;
  pink::BGThread purge_thread_;
  
  static void DoPurgeLogs(void* arg);
  bool GetPurgeWindow(uint32_t &max);

  /*
   * Flushall use 
   */
  static void DoPurgeDir(void* arg);

  PikaServer(PikaServer &ps);
  void operator =(const PikaServer &ps);
};
#endif
