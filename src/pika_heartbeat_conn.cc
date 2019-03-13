// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_heartbeat_conn.h"

#include "include/pika_server.h"

extern PikaServer* g_pika_server;

PikaHeartbeatConn::PikaHeartbeatConn(int fd,
                                     std::string ip_port,
                                     pink::Thread* thread,
                                     void* worker_specific_data, pink::PinkEpoll* epoll)
    : PbConn(fd, ip_port, thread, epoll) {
}

PikaHeartbeatConn::~PikaHeartbeatConn() {
}

int PikaHeartbeatConn::DealMessage() {
  InnerMessage::InnerRequest request;
  InnerMessage::InnerResponse response;
  bool res = request.ParseFromArray(rbuf_ + cur_pos_ - header_len_, header_len_);
  if (!res) {
    LOG(WARNING) << "Pika heart beat connection pb parse error.";
    return -1;
  }
  if (request.type() != InnerMessage::kHeatBeat) {
    LOG(WARNING) << "Request Type error.";
    return -1;
  } else {
    InnerMessage::InnerRequest::HeatBeat heat_beat_req = request.heat_beat();
    if (heat_beat_req.has_sid()) {
      int64_t sid = heat_beat_req.sid();
      g_pika_server->MayUpdateSlavesMap(sid, fd());
    }

    response.set_code(InnerMessage::kOk);
    response.set_type(InnerMessage::kHeatBeat);
    InnerMessage::InnerResponse::HeatBeat* heat_beat_resp = response.mutable_heat_beat();
    heat_beat_resp->set_pong("PONG");
  }

  std::string reply;
  response.SerializeToString(&reply);
  return WriteResp(reply);
}
