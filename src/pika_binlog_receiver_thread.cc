#include <glog/logging.h>
#include "pika_binlog_receiver_thread.h"
#include "pika_master_conn.h"
#include "pika_server.h"
#include "pika_command.h"

extern PikaServer* g_pika_server;

PikaBinlogReceiverThread::PikaBinlogReceiverThread(int port) :
  HolyThread::HolyThread(port) {
    InitCmdTable(&cmds_);
}

PikaBinlogReceiverThread::~PikaBinlogReceiverThread() {

}

bool PikaBinlogReceiverThread::AccessHandle(const std::string& ip_port) {
  if (conns_.size() != 0 /* ip_port != master_host */) {
    return false;
  }
  g_pika_server->PlusMasterConnection();
  return true;
}

void PikaBinlogReceiverThread::KillAll() {
  {
  slash::RWLock l(&rwlock_, true);
  std::map<int, void*>::iterator iter = conns_.begin();
  while (iter != conns_.end()) {
    DLOG(INFO) << "==========Kill Master==============";
    close(iter->first);
    delete(static_cast<PikaMasterConn*>(iter->second));
    iter = conns_.erase(iter);
  }
  }
  sleep(1); // wait for master connect truly be killed by cron;
}
