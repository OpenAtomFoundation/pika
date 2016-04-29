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
  BinlogBGWorker *self = bgarg->myself;
  std::string opt = argv[0];
  slash::StringToLower(opt);

  // Get command info
  const CmdInfo* const cinfo_ptr = GetCmdInfo(opt);
  Cmd* c_ptr = self->GetCmd(opt);
  if (!cinfo_ptr || !c_ptr) {
    LOG(ERROR) << "Error operation from binlog: " << opt;
  }
  c_ptr->res().clear();

  // Initial
  c_ptr->Initial(argv, cinfo_ptr);
  if (!c_ptr->res().ok()) {
    LOG(ERROR) << "Fail to initial command from binlog: " << opt;
  }

  uint64_t start_us;
  if (g_pika_conf->slowlog_slower_than() >= 0) {
    start_us = slash::NowMicros();
  }

  g_pika_server->mutex_record_.Lock(argv[1]);

  // Add read lock for no suspend command
  if (!cinfo_ptr->is_suspend()) {
    pthread_rwlock_rdlock(g_pika_server->rwlock());
  }

  // Force the binlog write option to serialize
  if (g_pika_server->WaitTillBinlogBGSerial(my_serial)) {
    g_pika_server->logger_->Lock();
    g_pika_server->logger_->Put(bgarg->raw_args);
    g_pika_server->logger_->Unlock();
    g_pika_server->SignalNextBinlogBGSerial();

    c_ptr->Do();
  }

  if (!cinfo_ptr->is_suspend()) {
    pthread_rwlock_unlock(g_pika_server->rwlock());
  }

  g_pika_server->mutex_record_.Unlock(argv[1]);

  if (g_pika_conf->slowlog_slower_than() >= 0) {
    int64_t duration = slash::NowMicros() - start_us;
    if (duration > g_pika_conf->slowlog_slower_than()) {
      LOG(ERROR) << "command:" << opt << ", start_time(s): " << start_us / 1000000 << ", duration(us): " << duration;
    }
  }

  DLOG(INFO) << "delete argv ptr";
  delete bgarg->argv;
  delete bgarg;
}
