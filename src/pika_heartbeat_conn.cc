#include <glog/logging.h>
#include "pika_heartbeat_conn.h"

PikaHeartbeatConn::PikaHeartbeatConn(int fd, std::string ip_port, pink::Thread* thread) :
  RedisConn(fd, ip_port) {
  pika_thread_ = reinterpret_cast<PikaHeartbeatThread*>(thread);
}

PikaHeartbeatConn::~PikaHeartbeatConn() {
}

int PikaHeartbeatConn::DealMessage() {
  PlusConnQuerynum();
  memcpy(wbuf_ + wbuf_len_, "+OK\r\n", 5);
  wbuf_len_ += 5;
  return 0;
}
