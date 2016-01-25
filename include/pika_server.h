#ifndef PIKA_SERVER_H_
#define PIKA_SERVER_H_

#include "pika_binlog_receiver_thread.h"
#include "pika_heartbeat_thread.h"
#include "pika_dispatch_thread.h"
#include "pika_trysync_thread.h"
#include "pika_worker_thread.h"
#include "pika_define.h"

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
/*
 * Master use
 */
  void DeleteSlave(int fd); // hb_fd
  slash::Mutex slave_mutex_; // protect slaves_;
  std::vector<SlaveItem> slaves_;

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


private:
  int port_;
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
