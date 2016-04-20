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
  self_thread_ = dynamic_cast<PikaBinlogReceiverThread*>(thread);
}

PikaMasterConn::~PikaMasterConn() {
}

std::string PikaMasterConn::RestoreArgs() {
  std::string res;
  res.reserve(RAW_ARGS_LEN);
  RedisAppendLen(res, argv_.size(), "*");
  PikaCmdArgsType::const_iterator it = argv_.begin();
  for ( ; it != argv_.end(); ++it) {
    RedisAppendLen(res, (*it).size(), "$");
    RedisAppendContent(res, *it);
  }
  return res;
}

std::string PikaMasterConn::DoCmd(const std::string& opt) {
  // Get command info
  const CmdInfo* const cinfo_ptr = GetCmdInfo(opt);
  Cmd* c_ptr = self_thread_->GetCmd(opt);
  if (!cinfo_ptr || !c_ptr) {
      return "-Err unknown or unsupported command \'" + opt + "\r\n";
  }
  c_ptr->res().clear();

  uint64_t start_us;
  if (g_pika_conf->slowlog_slower_than() >= 0) {
    start_us = slash::NowMicros();
  }
  
  std::string monitor_message;
  bool is_monitoring = g_pika_server->monitor_thread()->HasMonitorClients();
  if (is_monitoring) {
    monitor_message = std::to_string(1.0*slash::NowMicros()/1000000) + " [" + this->ip_port() + "]";
    for (PikaCmdArgsType::iterator iter = argv_.begin(); iter != argv_.end(); iter++) {
      monitor_message += " \"" + *iter + "\"";
    }
    g_pika_server->monitor_thread()->AddMonitorMessage(monitor_message);
  }

  // Initial
  c_ptr->Initial(argv_, cinfo_ptr);
  if (!c_ptr->res().ok()) {
    return c_ptr->res().message();
  }

  // TODO Check authed
  // Add read lock for no suspend command

  g_pika_server->mutex_record_.Lock(argv_[1]);
  c_ptr->Do();

  if (c_ptr->res().ok()) {
    g_pika_server->logger_->Lock();
    g_pika_server->logger_->Put(RestoreArgs());
    g_pika_server->logger_->Unlock();
  }
  g_pika_server->mutex_record_.Unlock(argv_[1]);

  if (g_pika_conf->slowlog_slower_than() >= 0) {
    int64_t duration = slash::NowMicros() - start_us;
    if (duration > g_pika_conf->slowlog_slower_than()) {
      LOG(ERROR) << "command:" << opt << ", start_time(s): " << start_us / 1000000 << ", duration(us): " << duration;
    }
  }


  return c_ptr->res().message();
}

int PikaMasterConn::DealMessage() {
  //no reply
  //eq set_is_reply(false);
  self_thread_ -> PlusThreadQuerynum();
  if (argv_.empty()) {
    return -2;
  }
  std::string opt = argv_[0];
  slash::StringToLower(opt);
  DoCmd(opt);
//  memcpy(wbuf_ + wbuf_len_, res.data(), res.size());
//  wbuf_len_ += res.size();
  return 0;
}
