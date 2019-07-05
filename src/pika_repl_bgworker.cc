// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_repl_bgworker.h"

#include <glog/logging.h>

#include "include/pika_rm.h"
#include "include/pika_conf.h"
#include "include/pika_server.h"
#include "include/pika_cmd_table_manager.h"

extern PikaConf* g_pika_conf;
extern PikaServer* g_pika_server;
extern PikaReplicaManager* g_pika_rm;
extern PikaCmdTableManager* g_pika_cmd_table_manager;

PikaReplBgWorker::PikaReplBgWorker(int queue_size)
    : bg_thread_(queue_size) {
  bg_thread_.set_thread_name("ReplBgWorker");
  pink::RedisParserSettings settings;
  settings.DealMessage = &(PikaReplBgWorker::HandleWriteBinlog);
  redis_parser_.RedisParserInit(REDIS_PARSER_REQUEST, settings);
  redis_parser_.data = this;
  table_name_ = g_pika_conf->default_table();
  partition_id_ = 0;
}

int PikaReplBgWorker::StartThread() {
  return bg_thread_.StartThread();
}

int PikaReplBgWorker::StopThread() {
  return bg_thread_.StopThread();
}

void PikaReplBgWorker::Schedule(pink::TaskFunc func, void* arg) {
  bg_thread_.Schedule(func, arg);
}

void PikaReplBgWorker::QueueClear() {
  bg_thread_.QueueClear();
}

void PikaReplBgWorker::HandleBGWorkerWriteBinlog(void* arg) {
  ReplClientWriteBinlogTaskArg* task_arg = static_cast<ReplClientWriteBinlogTaskArg*>(arg);
  const std::shared_ptr<InnerMessage::InnerResponse> res = task_arg->res;
  std::shared_ptr<pink::PbConn> conn = task_arg->conn;
  std::vector<int>* index = static_cast<std::vector<int>* >(task_arg->res_private_data);
  PikaReplBgWorker* worker = task_arg->worker;
  worker->ip_port_ = conn->ip_port();

  std::string table_name;
  uint32_t partition_id = 0;
  BinlogOffset ack_start, ack_end;
  // find the first not keepalive binlogsync
  for (size_t i = 0; i < index->size(); ++i) {
    const InnerMessage::InnerResponse::BinlogSync& binlog_res = res->binlog_sync((*index)[i]);
    if (i == 0) {
      table_name = binlog_res.partition().table_name();
      partition_id = binlog_res.partition().partition_id();
    }
    if (!binlog_res.binlog().empty()) {
      ack_start.filenum = binlog_res.binlog_offset().filenum();
      ack_start.offset = binlog_res.binlog_offset().offset();
      break;
    }
  }
  worker->table_name_ = table_name;
  worker->partition_id_ = partition_id;

  std::shared_ptr<Partition> partition = g_pika_server->GetTablePartitionById(table_name, partition_id);
  if (!partition) {
    LOG(WARNING) << "Partition " << table_name << "_" << partition_id << " Not Found";
    delete index;
    delete task_arg;
    return;
  }

  std::shared_ptr<SyncSlavePartition> slave_partition =
    g_pika_rm->GetSyncSlavePartitionByName(
        PartitionInfo(table_name, partition_id));
  if (!slave_partition) {
    LOG(WARNING) << "Slave Partition " << table_name << "_" << partition_id << " Not Found";
    delete index;
    delete task_arg;
    return;
  }

  for (size_t i = 0; i < index->size(); ++i) {
    const InnerMessage::InnerResponse::BinlogSync& binlog_res = res->binlog_sync((*index)[i]);
    // if pika are not current a slave or partition not in
    // BinlogSync state, we drop remain write binlog task
    if ((g_pika_conf->classic_mode() && !(g_pika_server->role() & PIKA_ROLE_SLAVE))
      || ((slave_partition->State() != ReplState::kConnected)
         && (slave_partition->State() != ReplState::kWaitDBSync))) {
      delete index;
      delete task_arg;
      return;
    }

    if (!g_pika_rm->CheckSlavePartitionSessionId(
                binlog_res.partition().table_name(),
                binlog_res.partition().partition_id(),
                binlog_res.session_id())) {
      LOG(WARNING) << "Check Session failed "
          << binlog_res.partition().table_name()
          << "_" << binlog_res.partition().partition_id();
      slave_partition->SetReplState(ReplState::kTryConnect);
      delete index;
      delete task_arg;
      return;
    }

    // empty binlog treated as keepalive packet
    if (binlog_res.binlog().empty()) {
      continue;
    }
    if (!PikaBinlogTransverter::BinlogItemWithoutContentDecode(TypeFirst, binlog_res.binlog(), &worker->binlog_item_)) {
      LOG(WARNING) << "Binlog item decode failed";
      slave_partition->SetReplState(ReplState::kTryConnect);
      delete index;
      delete task_arg;
      return;
    }
    const char* redis_parser_start = binlog_res.binlog().data() + BINLOG_ENCODE_LEN;
    int redis_parser_len = static_cast<int>(binlog_res.binlog().size()) - BINLOG_ENCODE_LEN;
    int processed_len = 0;
    pink::RedisParserStatus ret = worker->redis_parser_.ProcessInputBuffer(
      redis_parser_start, redis_parser_len, &processed_len);
    if (ret != pink::kRedisParserDone) {
      LOG(WARNING) << "Redis parser failed";
      slave_partition->SetReplState(ReplState::kTryConnect);
      delete index;
      delete task_arg;
      return;
    }
  }
  delete index;
  delete task_arg;

  // Reply Ack to master immediately
  std::shared_ptr<Binlog> logger = partition->logger();
  logger->GetProducerStatus(&ack_end.filenum, &ack_end.offset);
  // keepalive case
  if (ack_start == BinlogOffset()) {
    // set ack_end as 0
    ack_end = ack_start;
  }
  g_pika_rm->SendPartitionBinlogSyncAckRequest(table_name, partition_id, ack_start, ack_end);
}

int PikaReplBgWorker::HandleWriteBinlog(pink::RedisParser* parser, const pink::RedisCmdArgsType& argv) {
  PikaReplBgWorker* worker = static_cast<PikaReplBgWorker*>(parser->data);
  const BinlogItem& binlog_item = worker->binlog_item_;
  g_pika_server->UpdateQueryNumAndExecCountTable(argv[0]);

  // Monitor related
  std::string monitor_message;
  if (g_pika_server->HasMonitorClients()) {
    std::string table_name = g_pika_conf->classic_mode()
      ? worker->table_name_.substr(2) : worker->table_name_;
    std::string monitor_message = std::to_string(1.0 * slash::NowMicros() / 1000000)
      + " [" + table_name + " " + worker->ip_port_ + "]";
    for (const auto& item : argv) {
      monitor_message += " " + slash::ToRead(item);
    }
    g_pika_server->AddMonitorMessage(monitor_message);
  }

  std::string opt = argv[0];
  Cmd* c_ptr = g_pika_cmd_table_manager->GetCmd(slash::StringToLower(opt));
  if (!c_ptr) {
    LOG(WARNING) << "Command " << opt << " not in the command table";
    return -1;
  }
  // Initial
  c_ptr->Initial(argv, worker->table_name_);
  if (!c_ptr->res().ok()) {
    LOG(WARNING) << "Fail to initial command from binlog: " << opt;
    return -1;
  }

  std::shared_ptr<Partition> partition = g_pika_server->GetTablePartitionById(worker->table_name_, worker->partition_id_);
  std::shared_ptr<Binlog> logger = partition->logger();

  logger->Lock();
  logger->Put(c_ptr->ToBinlog(binlog_item.exec_time(),
                              std::to_string(binlog_item.server_id()),
                              binlog_item.logic_id(),
                              binlog_item.filenum(),
                              binlog_item.offset()));
  uint32_t filenum;
  uint64_t offset;
  logger->GetProducerStatus(&filenum, &offset);
  logger->Unlock();

  PikaCmdArgsType *v = new PikaCmdArgsType(argv);
  BinlogItem *b = new BinlogItem(binlog_item);
  std::string dispatch_key = argv.size() >= 2 ? argv[1] : argv[0];
  g_pika_rm->ScheduleWriteDBTask(dispatch_key, v, b, worker->table_name_, worker->partition_id_);
  return 0;
}

void PikaReplBgWorker::HandleBGWorkerWriteDB(void* arg) {
  ReplClientWriteDBTaskArg* task_arg = static_cast<ReplClientWriteDBTaskArg*>(arg);
  PikaCmdArgsType* argv = task_arg->argv;
  BinlogItem binlog_item = *(task_arg->binlog_item);
  std::string table_name = task_arg->table_name;
  uint32_t partition_id = task_arg->partition_id;
  std::string opt = (*argv)[0];
  slash::StringToLower(opt);

  // Get command
  Cmd* c_ptr = g_pika_cmd_table_manager->GetCmd(slash::StringToLower(opt));
  if (!c_ptr) {
    LOG(WARNING) << "Error operation from binlog: " << opt;
    delete task_arg;
    return;
  }

  // Initial
  c_ptr->Initial(*argv, table_name);
  if (!c_ptr->res().ok()) {
    LOG(WARNING) << "Fail to initial command from binlog: " << opt;
    delete task_arg;
    return;
  }

  uint64_t start_us = 0;
  if (g_pika_conf->slowlog_slower_than() >= 0) {
    start_us = slash::NowMicros();
  }
  std::shared_ptr<Partition> partition = g_pika_server->GetTablePartitionById(table_name, partition_id);
  // Add read lock for no suspend command
  if (!c_ptr->is_suspend()) {
    partition->DbRWLockReader();
  }

  c_ptr->Do(partition);

  if (!c_ptr->is_suspend()) {
    partition->DbRWUnLock();
  }

  if (g_pika_conf->slowlog_slower_than() >= 0) {
    int32_t start_time = start_us / 1000000;
    int64_t duration = slash::NowMicros() - start_us;
    if (duration > g_pika_conf->slowlog_slower_than()) {
      g_pika_server->SlowlogPushEntry(*argv, start_time, duration);
      if (g_pika_conf->slowlog_write_errorlog()) {
        LOG(ERROR) << "command: " << opt << ", start_time(s): " << start_time << ", duration(us): " << duration;
      }
    }
  }
  delete task_arg;
}

