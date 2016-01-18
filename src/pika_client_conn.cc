#include "pika_client_conn.h"
namespace pika {
PikaClientConn::PikaClientConn(int fd, std::string ip_port, Thread* thread) :
  RedisConn(fd, ip_port) {
  pika_thread_ = reinterpret_cast<PikaWorkerThread*>(thread);
}

PikaClientConn::~PikaClientConn() {
}

int PikaClientConn::DealMessage() {
  PlusConnQuerynum();
  memcpy(wbuf_ + wbuf_len_, "+OK\r\n", 5);
  wbuf_len_ += 5;
  return 0;
}
};
