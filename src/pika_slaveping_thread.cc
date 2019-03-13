// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_slaveping_thread.h"

#include "include/pika_server.h"

extern PikaServer* g_pika_server;

Status PikaSlavepingThread::Send() {
  InnerMessage::InnerRequest request;
  request.set_type(InnerMessage::kHeatBeat);
  InnerMessage::InnerRequest::HeatBeat* heat_beat = request.mutable_heat_beat();

  InnerMessage::Node* node = heat_beat->mutable_node();
  node->set_ip(g_pika_server->host());
  node->set_port(g_pika_server->port());

  if (!is_first_send_) {
    heat_beat->set_ping("PING");
  } else {
    heat_beat->set_sid(sid_);
    heat_beat->set_ping("PING");
    is_first_send_ = false;
  }

  std::string to_send;
  if (!request.SerializeToString(&to_send)) {
    return Status::Corruption("Serialize Failed");
  }
  return cli_->Send(reinterpret_cast<void*>(&request));
}

Status PikaSlavepingThread::RecvProc() {
  InnerMessage::InnerResponse response;
  Status s = cli_->Recv(&response);
  if (s.ok()) {
    if (response.type() != InnerMessage::kHeatBeat) {
      LOG(WARNING) << "Response Type Error";
      return Status::Corruption("Type Error");
    }
    InnerMessage::InnerResponse::HeatBeat heat_beat_resp = response.heat_beat();
  }
  return s;
}


void* PikaSlavepingThread::ThreadMain() {
  Status s;
  struct timeval now;
  gettimeofday(&now, NULL);
  struct timeval last_interaction;
  last_interaction = now;
  int connect_retry_times = 0;

  while (!should_stop()) {
    if (!should_stop() && (cli_->Connect(g_pika_server->master_ip(),
                           g_pika_server->master_port() + kPortShiftHeatBeat,
                           "")).ok()) {
      cli_->set_send_timeout(1000);
      cli_->set_recv_timeout(1000);
      connect_retry_times = 0;

      // g_pika_server ping success status
      while (true) {
        if (should_stop()) {
          LOG(INFO) << "Close Slaveping Thread now";
          cli_->Close();
          // Kill sync binlog conn
          break;
        }

        s = Send();
        if (s.ok()) {
          s = RecvProc();
        }
        if (s.ok()) {
          gettimeofday(&last_interaction, NULL);
        } else if (s.IsTimeout()) {
          LOG(WARNING) << "Slaveping timeout once";
          gettimeofday(&now, NULL);
          if (now.tv_sec - last_interaction.tv_sec > 30) {
            //timeout;
            LOG(WARNING) << "Ping master timeout";
            cli_->Close();
            // kill sync binlog conn
            break;
          }
        } else {
          LOG(WARNING) << "Ping master error";
          cli_->Close();
          // kill sync binlog conn
          break;
        }
        sleep(1);
      }
      // g_pika_server ping error status
      sleep(2);
    } else if (!should_stop()) {
      LOG(WARNING) << "Slaveping, Connect timeout";
      if ((++connect_retry_times) >= 30) {
        LOG(WARNING) << "Slaveping, Connect timeout 10 times, disconnect with master";
        cli_->Close();
          // kill sync binlog conn
        connect_retry_times = 0;
      }
    }
  }
  return NULL;
}
