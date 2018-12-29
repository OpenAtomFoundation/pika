// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_binlog_bgworker.h"
#include "include/pika_server.h"
#include "include/pika_conf.h"
#include "slash/include/slash_string.h"
#include "slash/include/slash_coding.h"

extern PikaServer* g_pika_server;
extern PikaConf* g_pika_conf;

void BinlogBGWorker::DoBinlogBG(void* arg) {
  BinlogBGArg *bgarg = static_cast<BinlogBGArg*>(arg);
  PikaCmdArgsType argv = *(bgarg->argv);
  BinlogItem binlog_item = *(bgarg->binlog_item);
  uint64_t my_serial = bgarg->serial;
  bool is_readonly = bgarg->readonly;
  BinlogBGWorker *self = bgarg->myself;
  std::string opt = argv[0];
  slash::StringToLower(opt);

  std::string server_id = std::to_string(binlog_item.server_id());

  // Get command
  Cmd* c_ptr = self->GetCmd(opt);
  if (!c_ptr) {
    LOG(WARNING) << "Error operation from binlog: " << opt;
    delete bgarg;
    return;
  }
  c_ptr->res().clear();

  // Initial
  c_ptr->Initial(argv, g_pika_conf->default_table());
  if (!c_ptr->res().ok()) {
    LOG(WARNING) << "Fail to initial command from binlog: " << opt;
    delete bgarg;
    return;
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

  // Force the binlog write option to serialize
  // Unlock, clean env, and exit when error happend
  bool error_happend = false;
  if (!is_readonly) {
    error_happend = !g_pika_server->WaitTillBinlogBGSerial(my_serial);
    if (!error_happend) {
      server_id = g_pika_conf->server_id();
      g_pika_server->logger_->Lock();
      std::string binlog = c_ptr->ToBinlog(binlog_item.exec_time(),
                                           server_id,
                                           binlog_item.logic_id(),
                                           binlog_item.filenum(),
                                           binlog_item.offset());
      if (!binlog.empty()) {
        g_pika_server->logger_->Put(binlog);
      }
      g_pika_server->logger_->Unlock();
      g_pika_server->SignalNextBinlogBGSerial();
    }
  }

  // Add read lock for no suspend command
  if (!c_ptr->is_suspend()) {
    g_pika_server->RWLockReader();
  }

  if (!error_happend) {
    c_ptr->Do();
  }

  if (!c_ptr->is_suspend()) {
    g_pika_server->RWUnlock();
  }

  if (!is_readonly && argv.size() >= 2) {
    g_pika_server->mutex_record_.Unlock(argv[1]);
  }
  if (g_pika_conf->slowlog_slower_than() >= 0) {
    int64_t duration = slash::NowMicros() - start_us;
    if (duration > g_pika_conf->slowlog_slower_than()) {
      LOG(ERROR) << "command: " << opt << ", start_time(s): " << start_us / 1000000 << ", duration(us): " << duration;
    }
  }

  delete bgarg;
}
