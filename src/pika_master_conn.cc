#include <glog/logging.h>
#include "pika_master_conn.h"
#include "pika_binlog_receiver_thread.h"

PikaMasterConn::PikaMasterConn(int fd, std::string ip_port, pink::Thread* thread) :
  RedisConn(fd, ip_port) {
//  self_thread_ = reinterpret_cast<PikaBinlogReceiverThread*>(thread);
  self_thread_ = dynamic_cast<PikaBinlogReceiverThread*>(thread);
}

PikaMasterConn::~PikaMasterConn() {
}

int PikaMasterConn::DealMessage() {
  //no reply
  //eq set_is_reply(false);
  self_thread_ -> PlusThreadQuerynum();
  memcpy(wbuf_ + wbuf_len_, "+OK\r\n", 5);
  wbuf_len_ += 5;
  return 0;
}
