#ifndef BINLOG_SYNC_H_
#define BINLOG_SYNC_H_

#include "pika_binlog.h"
#include "binlog_receiver_thread.h"
#include "slaveping_thread.h"
#include "trysync_thread.h"
#include "pika_define.h"

#include "slash_status.h"
#include "slash_mutex.h"

using slash::Status;
using slash::Slice;

class BinlogSync
{
public:
  BinlogSync(int64_t filenum, int64_t offset, int port, std::string& master_ip, int master_port, std::string& passwd, std::string& log_path);
  ~BinlogSync();

  /*
   * Get & Set 
   */
  std::string host() {
    return host_;
  }
  int port() {
    return port_;
  };
  std::string& master_ip() {
    return master_ip_;
  }
  int master_port() {
    return master_port_;
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
  void UnLock() {
    mutex_.Unlock();
  }

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

  slash::Mutex mutex_; // double lock to block main thread

  bool Init();

  int64_t filenum_;
  int64_t offset_;
  SlavepingThread* ping_thread_;

private:
  std::string host_;
  int port_;
  std::string master_ip_;
  int master_port_;
  int master_connection_;
  int role_;
  int repl_state_;
  std::string requirepass_;
  std::string log_path_;
  pthread_rwlock_t rwlock_;

  BinlogReceiverThread* binlog_receiver_thread_;
  TrysyncThread* trysync_thread_;

  Binlog *logger_;

  pthread_rwlock_t state_protector_; //protect below, use for master-slave mode

  BinlogSync(BinlogSync &bs);
  void operator =(const BinlogSync &bs);
};

#endif
