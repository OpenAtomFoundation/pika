// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_repl_server_conn.h"

#include "include/pika_server.h"

#include "src/pika_inner_message.pb.h"

extern PikaServer* g_pika_server;

PikaReplServerConn::PikaReplServerConn(int fd,
                                       const std::string& ip_port,
                                       pink::Thread* thread,
                                       void* worker_specific_data)
    : pink::PbConn(fd, ip_port, thread) {
}

void PikaReplServerConn::DoReplServerTask(void* arg) {
  InnerMessage::InnerRequest* request = reinterpret_cast<InnerMessage::InnerRequest*>(arg);
  //  std::string& argv = info->argv;
  //  int port = info->port;
  delete request;
}

int PikaReplServerConn::DealMessage() {
  InnerMessage::InnerRequest* request = new InnerMessage::InnerRequest();
  request->ParseFromArray(rbuf_ + cur_pos_ - header_len_, header_len_);
  g_pika_server->Schedule(&DoReplServerTask, request);
  return 0;
}
