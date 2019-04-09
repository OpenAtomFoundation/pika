// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_repl_server_conn.h"

#include <glog/logging.h>

#include "include/pika_server.h"

extern PikaServer* g_pika_server;

PikaReplServerConn::PikaReplServerConn(int fd,
                                       std::string ip_port,
                                       pink::Thread* thread,
                                       void* worker_specific_data, pink::PinkEpoll* epoll)
    : PbConn(fd, ip_port, thread, epoll) {
}

PikaReplServerConn::~PikaReplServerConn() {
}

int PikaReplServerConn::DealMessage() {
  std::shared_ptr<InnerMessage::InnerRequest> req = std::make_shared<InnerMessage::InnerRequest>();
  bool parse_res = req->ParseFromArray(rbuf_ + cur_pos_ - header_len_, header_len_);
  if (!parse_res) {
    LOG(WARNING) << "Pika repl server connection pb parse error.";
    return -1;
  }
  int res = 0;
  switch (req->type()) {
    case InnerMessage::kMetaSync:
    {
      ReplServerTaskArg* task_arg = new ReplServerTaskArg(req, std::dynamic_pointer_cast<PikaReplServerConn>(shared_from_this()));
      g_pika_server->ScheduleReplServerBGTask(&PikaReplServer::HandleMetaSyncRequest, task_arg);
      break;
    }
    case InnerMessage::kTrySync:
    {
      ReplServerTaskArg* task_arg = new ReplServerTaskArg(req, std::dynamic_pointer_cast<PikaReplServerConn>(shared_from_this()));
      g_pika_server->ScheduleReplServerBGTask(&PikaReplServer::HandleTrySyncRequest, task_arg);
      break;
    }
    case InnerMessage::kDBSync:
    {
      ReplServerTaskArg* task_arg = new ReplServerTaskArg(req, std::dynamic_pointer_cast<PikaReplServerConn>(shared_from_this()));
      g_pika_server->ScheduleReplServerBGTask(&PikaReplServer::HandleDBSyncRequest, task_arg);
      break;
    }
    case InnerMessage::kBinlogSync:
    {
      ReplServerTaskArg* task_arg = new ReplServerTaskArg(req, std::dynamic_pointer_cast<PikaReplServerConn>(shared_from_this()));
      g_pika_server->ScheduleReplServerBGTask(&PikaReplServer::HandleBinlogSyncRequest, task_arg);
      break;
    }
    default:
      break;
  }
  return res;
}
