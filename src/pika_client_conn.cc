#include <sstream>
#include <glog/logging.h>
#include "pika_server.h"
#include "pika_client_conn.h"

extern PikaServer* g_pika_server;

PikaClientConn::PikaClientConn(int fd, std::string ip_port, pink::Thread* thread) :
  RedisConn(fd, ip_port) {
  pika_thread_ = dynamic_cast<PikaWorkerThread*>(thread);
}

PikaClientConn::~PikaClientConn() {
}

std::string PikaClientConn::RestoreArgs() {
  CmdRes res;
  res.AppendArrayLen(argv_.size());
  PikaCmdArgsType::const_iterator it = argv_.begin();
  for ( ; it != argv_.end(); ++it) {
    res.AppendStringLen((*it).size());
    res.AppendContent(*it);
  }
  return res.message();
}

void PikaClientConn::DoCmd(const std::string& opt, CmdRes& ret) {
  // Get command info
  const CmdInfo* cinfo_ptr = GetCmdInfo(opt);
  Cmd* c_ptr = pika_thread_->GetCmd(opt);
  if (!cinfo_ptr || !c_ptr) {
      ret.SetErr("unknown or unsupported command \'" + opt);
      return;
  }

  // TODO Check authed
  // Add read lock for no suspend command
  if (!cinfo_ptr->is_suspend()) {
    pthread_rwlock_rdlock(g_pika_server->rwlock());
  }

  if (cinfo_ptr->is_write()) {
      g_pika_server->logger->Lock();
  }

  c_ptr->Do(argv_, ret);

  if (cinfo_ptr->is_write()) {
      if (ret.ok()) {
          g_pika_server->logger->Put(RestoreArgs());
      }
      g_pika_server->logger->Unlock();
  }

  if (!cinfo_ptr->is_suspend()) {
      pthread_rwlock_unlock(g_pika_server->rwlock());
  }
}

int PikaClientConn::DealMessage() {
    PlusConnQuerynum();

    /* Sync cmd
     * 1 logger->Lock()
     * 2 cmd->Do
     * 3 AppendLog
     * 4 logger->Unlock
     */

    //logger->Lock();
    //TODO return value
    if (argv_.empty()) return -2;
    CmdRes res;
    std::string opt = argv_[0];
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
    //      g_pika_server->logger->Put(std::string("*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$1\r\n1\r\n"));
    //    }
    //  }
    //
    memcpy(wbuf_ + wbuf_len_, res.message().data(), res.message().size());
    wbuf_len_ += res.message().size();
    return 0;
}
