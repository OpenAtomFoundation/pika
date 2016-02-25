#ifndef PIKA_SERVER_H_
#define PIKA_SERVER_H_

#include <vector>
#include <list>
#include <nemo.h>

#include "pika_binlog.h"
#include "pika_binlog_receiver_thread.h"
#include "pika_binlog_sender_thread.h"
#include "pika_heartbeat_thread.h"
#include "pika_dispatch_thread.h"
#include "pika_trysync_thread.h"
#include "pika_worker_thread.h"
#include "pika_define.h"

#include "slash_status.h"

using slash::Status;
using slash::Slice;

class PikaServer
{
public:
  PikaServer(int port);
  ~PikaServer();

  /*
   * Get & Set 
   */
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
  void DeleteSlave(int fd); // hb_fd
  Status GetSmallestValidLog(uint32_t* max);

  slash::Mutex slave_mutex_; // protect slaves_;
  std::vector<SlaveItem> slaves_;
//  pthread_mutex_t binlog_sender_mutex_;
  std::vector<PikaBinlogSenderThread *> binlog_sender_threads_;

/*
 * Slave use
 */
  bool SetMaster(const std::string& master_ip, int master_port);
  bool ShouldConnectMaster();
  void ConnectMasterDone();
  bool ShouldStartPingMaster();
  void MinusMasterConnection();
  void PlusMasterConnection();

  void Start();
  slash::Mutex mutex_; // double lock to block main thread

  /*
   * Binlog
   */
  Binlog *logger_;
  Status AddBinlogSender(SlaveItem &slave, uint32_t filenum, uint64_t con_offset);

private:
  int port_;
  pthread_rwlock_t rwlock_;
  std::shared_ptr<nemo::Nemo> db_;

  PikaWorkerThread* pika_worker_thread_[PIKA_MAX_WORKER_THREAD_NUM];
  PikaDispatchThread* pika_dispatch_thread_;

  PikaBinlogReceiverThread* pika_binlog_receiver_thread_;
  PikaHeartbeatThread* pika_heartbeat_thread_;
  PikaTrysyncThread* pika_trysync_thread_;


  /*
   * Slave use
   */
  pthread_rwlock_t state_protector_; //protect below, use for master-slave mode
  std::string master_ip_;
  int master_connection_;
  int master_port_;
  int repl_state_;
  int role_;

  PikaServer(PikaServer &ps);
  void operator =(const PikaServer &ps);
};
#endif
