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
  //TODO check performance
  const static std::string new_line = "\r\n";
  std::stringstream ss;
  ss << "*" << argv_.size() << new_line;
  PikaCmdArgsType::iterator it = argv_.begin();
  for ( ; it != argv_.end(); ++it) {
    ss << "$" << (*it).size() << new_line;
    ss << *it << new_line;
  }
  return ss.str();
}

int PikaClientConn::DoCmd(const std::string& opt, const std::string& raw_args, std::string& ret) {
  // Get command info
  CmdInfoPtr cinfo_ptr = GetCmdInfo(opt);
  CmdPtr c_ptr = pika_thread_->GetCmd(opt);
  if (!cinfo_ptr || !c_ptr) {
      ret.append("-ERR unknown or unsupported command \'" + opt + "\'\r\n");
      return 0;
  }

  // TODO Check authed
  // Add read lock for no suspend command
  if (!cinfo_ptr->is_suspend()) {
    pthread_rwlock_rdlock(g_pika_server->rwlock());
  }

  if (cinfo_ptr->is_write()) {
      g_pika_server->logger->Lock();
  }

  int exe_ret = c_ptr->Do(argv_, ret);

  if (cinfo_ptr->is_write()) {
      if (exe_ret == 0 && ret.find("-ERR ") != 0) {
          g_pika_server->logger->Put(raw_args);
      }
      g_pika_server->logger->Unlock();
  }

  if (!cinfo_ptr->is_suspend()) {
      pthread_rwlock_unlock(g_pika_server->rwlock());
  }
  return exe_ret;
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
    std::string raw_args = RestoreArgs();
    std::string ret, opt = PopArg(argv_, true);
    //TODO add logger lock
    int cmd_ret = DoCmd(opt, raw_args, ret);
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
    memcpy(wbuf_ + wbuf_len_, ret.data(), ret.size());
    wbuf_len_ += ret.size();
    return cmd_ret;
}
