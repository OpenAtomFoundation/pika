#include <glog/logging.h>
#include "pika_heartbeat_conn.h"
#include "pika_server.h"
#include "slash_string.h"

extern PikaServer *g_pika_server;

PikaHeartbeatConn::PikaHeartbeatConn(int fd, std::string ip_port, pink::Thread* thread) :
  RedisConn(fd, ip_port) {
  pika_thread_ = reinterpret_cast<PikaHeartbeatThread*>(thread);
}

PikaHeartbeatConn::~PikaHeartbeatConn() {
}

int PikaHeartbeatConn::DealMessage() {
  set_is_reply(true);
  if (argv_[0] == "ping") {
    memcpy(wbuf_ + wbuf_len_, "+PONG\r\n", 7);
    wbuf_len_ += 7;
  } else if (argv_[0] == "spci") {
    int64_t sid = -1;
    slash::string2l(argv_[1].data(), argv_[1].size(), &sid);
    g_pika_server->MayUpdateSlavesMap(sid, fd());
    memcpy(wbuf_ + wbuf_len_, "+OK\r\n", 5);
    wbuf_len_ += 5;
  } else {
    memcpy(wbuf_ + wbuf_len_, "-ERR What the fuck are u sending\r\n", 34);
    wbuf_len_ += 34;
  }
  return 0;
}
