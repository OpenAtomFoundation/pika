#include <sstream>
#include <glog/logging.h>
#include "pika_server.h"
#include "pika_client_conn.h"

extern PikaServer* g_pika_server;
static const int RAW_ARGS_LEN = 1024 * 1024; 

PikaClientConn::PikaClientConn(int fd, std::string ip_port, pink::Thread* thread) :
  RedisConn(fd, ip_port) {
  self_thread_ = dynamic_cast<PikaWorkerThread*>(thread);
}

PikaClientConn::~PikaClientConn() {
}

std::string PikaClientConn::RestoreArgs() {
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

void PikaClientConn::DoCmd(const std::string& opt, std::string &ret) {
  // Get command info
  const CmdInfo* const cinfo_ptr = GetCmdInfo(opt);
  Cmd* c_ptr = self_thread_->GetCmd(opt);
  if (!cinfo_ptr || !c_ptr) {
      ret.append("-Err unknown or unsupported command \'" + opt + "\r\n");
      return;
  }
  c_ptr->res().clear();
  
  // Initial
  c_ptr->Initial(argv_, cinfo_ptr);
  if (!c_ptr->res().ok()) {
    return;
  }

  // TODO Check authed
  // Add read lock for no suspend command
  if (!cinfo_ptr->is_suspend()) {
    pthread_rwlock_rdlock(g_pika_server->rwlock());
  }

  if (cinfo_ptr->is_write()) {
      g_pika_server->logger_->Lock();
  }

  c_ptr->Do();

  if (cinfo_ptr->is_write()) {
      if (c_ptr->res().ok()) {
          g_pika_server->logger_->Put(RestoreArgs());
      }
      g_pika_server->logger_->Unlock();
  }

  if (!cinfo_ptr->is_suspend()) {
      pthread_rwlock_unlock(g_pika_server->rwlock());
  }
  ret = c_ptr->res().message();
}

int PikaClientConn::DealMessage() {
  
  self_thread_->PlusThreadQuerynum();
    /* Sync cmd
     * 1 logger_->Lock()
     * 2 cmd->Do
     * 3 AppendLog
     * 4 logger_->Unlock
     */

    //logger_->Lock();
    //TODO return value
    if (argv_.empty()) return -2;
    std::string res, opt = argv_[0];
    slash::StringToLower(opt);
    //TODO add logger lock
    DoCmd(opt, res);
    // if (opt == "pikasync") {
    //     AppendReply(ret);
    //     mutex_.Unlock();
    // } else if ((role_ != PIKA_MASTER  && !(role_ == PIKA_SLAVE && opt == "ping")) || (role_ == PIKA_MASTER && opt == "syncdb")) {
    //     if (role_ == PIKA_SLAVE) {
    //         MutexLock l(&mutex_);
    //         AppendReply(ret);
    //     } else {
    //         AppendReply(ret);
    //     }
    // }
    //  // TEST trysync
    //  if (argv_[0] == "trysync") {
    //    DLOG(INFO) << "recieve \"trysync\"";
    //
    //    // Test BinlogSender
    //    SlaveItem slave;
    //    slave.ip_port = "127.0.0.1:9922";
    //    slave.port = 9922;
    //
    //    Status s = g_pika_server->AddBinlogSender(slave, 0, 0);
    //    if (s.ok()) {
    //      DLOG(INFO) << "AddBinlogSender ok";
    //    } else {
    //      DLOG(INFO) << "AddBinlogSender failed, " << s.ToString();
    //    }
    //
    //  } else if (argv_[0] == "slaveof") {
    //    DLOG(INFO) << "recieve \"slaveof\"";
    //    DLOG(INFO) << "SetMaster " << g_pika_server->SetMaster("127.0.0.1", 9821);
    //  } else {
    //    // TEST Put
    //    for (int i = 0; i < 10; i++) {
    //      DLOG(INFO) << "Logger Put a msg:" << i << ";";
    //      g_pika_server->logger_->Put(std::string("*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$1\r\n1\r\n"));
    //    }
    //  }
    //
    memcpy(wbuf_ + wbuf_len_, res.data(), res.size());
    wbuf_len_ += res.size();
    set_is_reply(true);
    return 0;
}
