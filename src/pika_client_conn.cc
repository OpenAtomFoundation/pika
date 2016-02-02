#include <glog/logging.h>

#include "pika_server.h"
#include "pika_client_conn.h"

extern PikaServer* g_pika_server;

PikaClientConn::PikaClientConn(int fd, std::string ip_port, pink::Thread* thread) :
  RedisConn(fd, ip_port) {
  pika_thread_ = reinterpret_cast<PikaWorkerThread*>(thread);
}

PikaClientConn::~PikaClientConn() {
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
  
  // TEST trysync
  if (argv_[0] == "trysync") {
    DLOG(INFO) << "recieve \"trysync\"";

    // Test BinlogSender
    SlaveItem slave;
    slave.ip_port = "127.0.0.1:9922";
    slave.port = 9922;

    Status s = g_pika_server->AddBinlogSender(slave, 0, 0);
    if (s.ok()) {
      DLOG(INFO) << "AddBinlogSender ok";
    } else {
      DLOG(INFO) << "AddBinlogSender failed, " << s.ToString();
    }

  } else if (argv_[0] == "slaveof") {
    DLOG(INFO) << "recieve \"slaveof\"";
    DLOG(INFO) << "SetMaster " << g_pika_server->SetMaster("127.0.0.1", 9821);
  } else {
    // TEST Put
    for (int i = 0; i < 10; i++) {
      DLOG(INFO) << "Logger Put a msg:" << i << ";";
      g_pika_server->logger->Put(std::string("*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$1\r\n1\r\n"));
    }
  }

  memcpy(wbuf_ + wbuf_len_, "+OK\r\n", 5);
  wbuf_len_ += 5;
  return 0;
}
