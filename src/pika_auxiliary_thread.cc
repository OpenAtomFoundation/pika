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
    sleep(1);
    if (g_pika_server->ShouldMetaSync()) {
      g_pika_server->SendMetaSyncRequest();
      continue;
    }
  }
  return NULL;
}
