// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_repl_client_conn.h"

#include "include/pika_server.h"

#include "src/pika_inner_message.pb.h"

extern PikaServer* g_pika_server;

PikaReplClientConn::PikaReplClientConn(int fd,
                               const std::string& ip_port,
                               pink::Thread* thread,
                               void* worker_specific_data)
      : PbConn(fd, ip_port, thread) {
}

void PikaReplClientConn::DoReplClientTask(void* arg) {
  InnerMessage::InnerResponse* response = reinterpret_cast<InnerMessage::InnerResponse*>(arg);
  //  std::string& argv = info->argv;
  //  int port = info->port;
  delete response;
}

int PikaReplClientConn::DealMessage() {
  InnerMessage::InnerResponse* res = new InnerMessage::InnerResponse();
  res->ParseFromArray(rbuf_ + cur_pos_ - header_len_, header_len_);
  g_pika_server->Schedule(&DoReplClientTask, res);
  return 0;
}
