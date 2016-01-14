#include "pika_conn.h"
namespace pika {
PikaConn::PikaConn(int fd, std::string ip_port, Thread* thread) :
  RedisConn(fd, ip_port) {
  pika_thread_ = reinterpret_cast<PikaWorkerThread*>(thread);
}

PikaConn::~PikaConn() {
}

int PikaConn::DealMessage() {
  memcpy(wbuf_ + wbuf_len_, "+OK\r\n", 5);
  wbuf_len_ += 5;
  return 0;
}
};
