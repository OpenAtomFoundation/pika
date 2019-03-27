// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_auxiliary_thread.h"

#include "include/pika_server.h"
#include "include/pika_define.h"

extern PikaServer* g_pika_server;

PikaAuxiliaryThread::~PikaAuxiliaryThread() {
  StopThread();
  LOG(INFO) << "PikaAuxiliary thread " << thread_id() << " exit!!!";
}

void* PikaAuxiliaryThread::ThreadMain() {
  while (!should_stop()) {
    if (g_pika_server->ShouldMetaSync()) {
      g_pika_server->SendMetaSyncRequest();
    } else if (g_pika_server->ShouldWaitMetaSyncResponse()) {
      g_pika_server->CheckWaitMetaSyncTimeout();
    } else if (g_pika_server->ShouldMarkTryConnect()) {
      g_pika_server->PreparePartitionTrySync();
    } else if (g_pika_server->ShouldTrySyncPartition()) {
      RunEveryPartitionStateMachine();
    }

    // TODO(whoiami) timeout
    Status s = g_pika_server->TriggerSendBinlogSync();
    if (!s.ok()) {
      LOG(WARNING) << s.ToString();
    }
    // send to peer
    int res = g_pika_server->SendToPeer();
    if (!res) {
      // sleep 100 ms
      mu_.Lock();
      cv_.TimedWait(100);
      mu_.Unlock();
    } else {
      //LOG_EVERY_N(INFO, 1000) << "Consume binlog number " << res;
    }
  }
  return NULL;
}

void PikaAuxiliaryThread::RunEveryPartitionStateMachine() {
  int total = 0, connected = 0;
  std::vector<TableStruct> table_structs = g_pika_conf->table_structs();
  for (const auto& table : table_structs) {
    for (size_t idx = 0; idx < table.partition_num; ++idx) {
      total++;
      std::shared_ptr<Partition> partition =
        g_pika_server->GetTablePartitionById(table.table_name, idx);
      if (!partition) {
        LOG(WARNING) << "Partition not found, Table Name: "
          << table.table_name << " Partition Id: " << idx;
        continue;
      }
      if (partition->State() == ReplState::kTryConnect) {
        // If partition need to FullSync, we Send DBSync Request
        // directly, instead of TrySync first
        if (partition->FullSync()) {
          g_pika_server->SendPartitionDBSyncRequest(partition);
        } else {
          g_pika_server->SendPartitionTrySyncRequest(partition);
        }
      } else if (partition->State() == ReplState::kTryDBSync) {
        g_pika_server->SendPartitionDBSyncRequest(partition);
      } else if (partition->State() == ReplState::kWaitReply) {
        continue;
      } else if (partition->State() == ReplState::kWaitDBSync) {
        partition->TryUpdateMasterOffset();
      } else if (partition->State() == ReplState::kConnected) {
        connected++;
      }
    }
  }

  if (total == connected) {
    g_pika_server->MarkEstablishSuccess();
  }
}
