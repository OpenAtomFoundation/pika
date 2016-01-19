#include <glog/logging.h>
#include "pika_binlog_receiver_thread.h"
#include "pika_master_conn.h"

PikaBinlogReceiverThread::PikaBinlogReceiverThread(int port) :
  HolyThread::HolyThread(port) {
}

PikaBinlogReceiverThread::~PikaBinlogReceiverThread() {

}

bool PikaBinlogReceiverThread::AccessHandle(const std::string& ip_port) {
  if (conns_.size() != 0 /* ip_port != master_host */) {
    return false;
  }
  return true;
}

