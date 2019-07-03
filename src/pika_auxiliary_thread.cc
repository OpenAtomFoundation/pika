// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_auxiliary_thread.h"

#include "include/pika_server.h"
#include "include/pika_define.h"
#include "include/pika_rm.h"

extern PikaServer* g_pika_server;
extern PikaReplicaManager* g_pika_rm;

PikaAuxiliaryThread::~PikaAuxiliaryThread() {
  StopThread();
  LOG(INFO) << "PikaAuxiliary thread " << thread_id() << " exit!!!";
}

void* PikaAuxiliaryThread::ThreadMain() {
  while (!should_stop()) {
    if (g_pika_conf->classic_mode()) {
      if (g_pika_server->ShouldMetaSync()) {
        g_pika_rm->SendMetaSyncRequest();
      } else if (g_pika_server->MetaSyncDone()) {
        g_pika_rm->RunSyncSlavePartitionStateMachine();
      }
    } else {
      g_pika_rm->RunSyncSlavePartitionStateMachine();
    }

    Status s = g_pika_rm->CheckSyncTimeout(slash::NowMicros());
    if (!s.ok()) {
      LOG(WARNING) << s.ToString();
    }

    // TODO(whoiami) timeout
    s = g_pika_server->TriggerSendBinlogSync();
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

