// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "pika_binlog_bgworker.h"
#include "pika_server.h"
#include "pika_conf.h"
#include "slash_string.h"

extern PikaServer* g_pika_server;
extern PikaConf* g_pika_conf;

void BinlogBGWorker::DoBinlogBG(void* arg) {
  BinlogBGArg *bgarg = static_cast<BinlogBGArg*>(arg);
  PikaCmdArgsType argv = *(bgarg->argv);
  uint64_t my_serial = bgarg->serial;
  bool is_readonly = bgarg->readonly;
  BinlogBGWorker *self = bgarg->myself;
  std::string opt = argv[0];
  slash::StringToLower(opt);

  // Get command info
  const CmdInfo* const cinfo_ptr = GetCmdInfo(opt);
  Cmd* c_ptr = self->GetCmd(opt);
  if (!cinfo_ptr || !c_ptr) {
    LOG(WARNING) << "Error operation from binlog: " << opt;
  }
  c_ptr->res().clear();

  // Initial
  c_ptr->Initial(argv, cinfo_ptr);
  if (!c_ptr->res().ok()) {
    LOG(WARNING) << "Fail to initial command from binlog: " << opt;
  }

  uint64_t start_us = 0;
  if (g_pika_conf->slowlog_slower_than() >= 0) {
    start_us = slash::NowMicros();
  }

  // No need lock on readonly mode
  // Since the binlog task is dispatched by hash code of key
  // That is to say binlog with same key will be dispatched to same thread and execute sequencly
  if (!is_readonly && argv.size() >= 2) {
    g_pika_server->mutex_record_.Lock(argv[1]);
  }

  // Add read lock for no suspend command
  if (!cinfo_ptr->is_suspend()) {
    g_pika_server->RWLockReader();
  }

  // Force the binlog write option to serialize
  // Unlock, clean env, and exit when error happend
  bool error_happend = false;
  if (!is_readonly) {
    error_happend = !g_pika_server->WaitTillBinlogBGSerial(my_serial);
    if (!error_happend) {
      g_pika_server->logger_->Lock();
      g_pika_server->logger_->Put(bgarg->raw_args);
      g_pika_server->logger_->Unlock();
      g_pika_server->SignalNextBinlogBGSerial();
    }
  }

  if (!error_happend) {
    c_ptr->Do();
  }

  if (!cinfo_ptr->is_suspend()) {
    g_pika_server->RWUnlock();
  }

  if (!is_readonly && argv.size() >= 2) {
    g_pika_server->mutex_record_.Unlock(argv[1]);
  }
  if (g_pika_conf->slowlog_slower_than() >= 0) {
    int64_t duration = slash::NowMicros() - start_us;
    if (duration > g_pika_conf->slowlog_slower_than()) {
      LOG(WARNING) << "command:" << opt << ", start_time(s): " << start_us / 1000000 << ", duration(us): " << duration;
    }
  }

  delete bgarg->argv;
  delete bgarg;
}
