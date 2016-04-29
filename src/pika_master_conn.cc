#include <glog/logging.h>
#include "pika_master_conn.h"
#include "pika_server.h"
#include "pika_conf.h"
#include "pika_binlog_receiver_thread.h"

extern PikaServer* g_pika_server;
extern PikaConf* g_pika_conf;

static const int RAW_ARGS_LEN = 1024 * 1024; 
PikaMasterConn::PikaMasterConn(int fd, std::string ip_port, pink::Thread* thread) :
  RedisConn(fd, ip_port) {
//  self_thread_ = reinterpret_cast<PikaBinlogReceiverThread*>(thread);
  raw_args_.reserve(RAW_ARGS_LEN);
  self_thread_ = dynamic_cast<PikaBinlogReceiverThread*>(thread);
}

PikaMasterConn::~PikaMasterConn() {
}

void PikaMasterConn::RestoreArgs() {
  raw_args_.clear();
  RedisAppendLen(raw_args_, argv_.size(), "*");
  PikaCmdArgsType::const_iterator it = argv_.begin();
  for ( ; it != argv_.end(); ++it) {
    RedisAppendLen(raw_args_, (*it).size(), "$");
    RedisAppendContent(raw_args_, *it);
  }
}

int PikaMasterConn::DealMessage() {
  //no reply
  //eq set_is_reply(false);
  self_thread_ -> PlusThreadQuerynum();
  if (argv_.empty()) {
    return -2;
  }

  // Monitor related
  std::string monitor_message;
  bool is_monitoring = g_pika_server->monitor_thread()->HasMonitorClients();
  if (is_monitoring) {
    monitor_message = std::to_string(1.0*slash::NowMicros()/1000000) + " [" + this->ip_port() + "]";
    for (PikaCmdArgsType::iterator iter = argv_.begin(); iter != argv_.end(); iter++) {
      monitor_message += " \"" + *iter + "\"";
    }
    g_pika_server->monitor_thread()->AddMonitorMessage(monitor_message);
  }
  RestoreArgs();

  PikaCmdArgsType *argv = new PikaCmdArgsType(argv_);
  g_pika_server->DispatchBinlogBG(argv_[1], argv, raw_args_, self_thread_->GetnPlusSerial());
//  memcpy(wbuf_ + wbuf_len_, res.data(), res.size());
//  wbuf_len_ += res.size();
  return 0;
}
