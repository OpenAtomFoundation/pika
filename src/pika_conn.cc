#include "pika_conn.h"
namespace pika {
PikaConn::PikaConn(int fd, Thread *thread) :
  RedisConn(fd) {
  pika_thread_ = reinterpret_cast<PikaWorkerThread *>(thread);
}

int PikaConn::DealMessage() {
  memcpy(wbuf_ + wbuf_len_, "+OK\r\n", 5);
  wbuf_len_ += 5;
  return 0;
}
};
