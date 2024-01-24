// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/rsync_client_thread.h"
#include "include/rsync_client.h"
#include "include/pika_define.h"

using namespace pstd;
using namespace net;
using namespace RsyncService;

namespace rsync {
class RsyncClient;
RsyncClientConn::RsyncClientConn(int fd, const std::string& ip_port,
    net::Thread* thread, void* worker_specific_data, NetMultiplexer* mpx)
    : PbConn(fd, ip_port, thread, mpx), cb_handler_(worker_specific_data) {}

RsyncClientConn::~RsyncClientConn() {}

int RsyncClientConn::DealMessage() {
  RsyncResponse* response = new RsyncResponse();
  ::google::protobuf::io::ArrayInputStream input(rbuf_ + cur_pos_ - header_len_, header_len_);
  ::google::protobuf::io::CodedInputStream decoder(&input);
  decoder.SetTotalBytesLimit(PIKA_MAX_CONN_RBUF);
  bool success = response->ParseFromCodedStream(&decoder) && decoder.ConsumedEntireMessage();
  if (!success) {
    delete response;
    LOG(WARNING) << "ParseFromArray FAILED! "
                 << " msg_len: " << header_len_;
    return -1;
  }
  WaitObjectManager* handler = (WaitObjectManager*)cb_handler_;
  handler->WakeUp(response);
  return 0;
}

RsyncClientThread::RsyncClientThread(int cron_interval, int keepalive_timeout, void* scheduler)
    : ClientThread(&conn_factory_, cron_interval, keepalive_timeout, &handle_, nullptr),
      conn_factory_(scheduler) {}

RsyncClientThread::~RsyncClientThread() {}
} //end namespace rsync

