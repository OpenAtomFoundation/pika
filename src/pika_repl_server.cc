// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_repl_server.h"

#include <glog/logging.h>

#include "include/pika_conf.h"

extern PikaConf* g_pika_conf;

PikaReplServer::PikaReplServer(const std::set<std::string>& ips, int port, int cron_interval) : next_avail_(0) {
  pika_repl_server_thread_ = new PikaReplServerThread(ips, port, cron_interval);
  pika_repl_server_thread_->set_thread_name("PikaReplServer");
  for (int i = 0; i < 2 * g_pika_conf->sync_thread_num(); ++i) {
    bg_workers_.push_back(new PikaReplBgWorker(PIKA_SYNC_BUFFER_SIZE));
  }
}

PikaReplServer::~PikaReplServer() {
  pika_repl_server_thread_->StopThread();
  delete pika_repl_server_thread_;
  for (size_t i = 0; i < bg_workers_.size(); ++i) {
    delete bg_workers_[i];
  }
  LOG(INFO) << "PikaReplServer exit!!!";
}

int PikaReplServer::Start() {
  int res = pika_repl_server_thread_->StartThread();
  if (res != pink::kSuccess) {
    LOG(FATAL) << "Start Pika Repl Server Thread Error: " << res << (res == pink::kCreateThreadError ? ": create thread error " : ": other error");
  }
  for (size_t i = 0; i < bg_workers_.size(); ++i) {
    res = bg_workers_[i]->StartThread();
    if (res != pink::kSuccess) {
      LOG(FATAL) << "Start Pika Repl Worker Thread Error: " << res << (res == pink::kCreateThreadError ? ": create thread error " : ": other error");
    }
  }
  return res;
}

void PikaReplServer::ScheduleBinlogSyncTask(std::string table_partition, const std::shared_ptr<InnerMessage::InnerRequest> req, std::shared_ptr<pink::PbConn> conn, void* req_private_data) {
  size_t index = GetHashIndex(table_partition, true);
  bg_workers_[index]->ScheduleRequest(req, conn, req_private_data);
}

void PikaReplServer::ScheduleMetaSyncTask(const std::shared_ptr<InnerMessage::InnerRequest> req, std::shared_ptr<pink::PbConn> conn, void* req_private_data) {
  bg_workers_[next_avail_]->ScheduleRequest(req, conn, req_private_data);
  UpdateNextAvail();
}

void PikaReplServer::ScheduleTrySyncTask(const std::shared_ptr<InnerMessage::InnerRequest> req, std::shared_ptr<pink::PbConn> conn, void* req_private_data) {
  bg_workers_[next_avail_]->ScheduleRequest(req, conn, NULL);
  UpdateNextAvail();
}

void PikaReplServer::ScheduleDbTask(const std::string& key, PikaCmdArgsType* argv, BinlogItem* binlog_item, const std::string& table_name, uint32_t partition_id) {
  size_t index = GetHashIndex(key, false);
  bg_workers_[index]->ScheduleWriteDb(argv, binlog_item, table_name, partition_id);
}

size_t PikaReplServer::GetHashIndex(std::string key, bool upper_half) {
  size_t hash_base = bg_workers_.size() / 2;
  return (str_hash(key) % hash_base) + (upper_half ? 0 : hash_base);
}
