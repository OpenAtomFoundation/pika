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
    : offset_(), bg_thread_(queue_size) {
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

  if (res->has_consensus_meta()) {
    worker->offset_.b_offset.filenum = res->consensus_meta().commit().filenum();
    worker->offset_.b_offset.offset = res->consensus_meta().commit().offset();
    worker->offset_.l_offset.term = res->consensus_meta().commit().term();
    worker->offset_.l_offset.index = res->consensus_meta().commit().index();
  } else {
    worker->offset_ = LogOffset();
  }

  std::string table_name;
  uint32_t partition_id = 0;
  LogOffset ack_start, ack_end;
  // find the first not keepalive binlogsync
  for (size_t i = 0; i < index->size(); ++i) {
    const InnerMessage::InnerResponse::BinlogSync& binlog_res = res->binlog_sync((*index)[i]);
    if (i == 0) {
      table_name = binlog_res.partition().table_name();
      partition_id = binlog_res.partition().partition_id();
    }
    if (!binlog_res.binlog().empty()) {
      ack_start.b_offset.filenum = binlog_res.binlog_offset().filenum();
      ack_start.b_offset.offset = binlog_res.binlog_offset().offset();
      ack_start.l_offset.term = binlog_res.binlog_offset().term();
      ack_start.l_offset.index = binlog_res.binlog_offset().index();
      break;
    }
  }
  // table_name and partition_id in the vector are same in the bgworker,
  // because DispatchBinlogRes() have been order them. 
  worker->table_name_ = table_name;
  worker->partition_id_ = partition_id;

  std::shared_ptr<SyncMasterPartition> partition = g_pika_rm->GetSyncMasterPartitionByName(PartitionInfo(table_name, partition_id));
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

    if (slave_partition->MasterSessionId() != binlog_res.session_id()) {
      LOG(WARNING)<< "Check SessionId Mismatch: " << slave_partition->MasterIp()
        << ":" << slave_partition->MasterPort() << ", "
        << slave_partition->SyncPartitionInfo().ToString()
        << " expected_session: " << binlog_res.session_id() << ", actual_session:"
        << slave_partition->MasterSessionId();
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
  std::shared_ptr<Binlog> logger = partition->Logger();
  logger->GetProducerStatus(&ack_end.b_offset.filenum, &ack_end.b_offset.offset,
      &ack_end.l_offset.index, &ack_end.l_offset.term);
  // keepalive case
  if (ack_start.b_offset == BinlogOffset()) {
    // set ack_end as 0
    ack_end = ack_start;
  }
  g_pika_rm->SendPartitionBinlogSyncAckRequest(table_name, partition_id, ack_start, ack_end);
}

int PikaReplBgWorker::HandleWriteBinlog(pink::RedisParser* parser, const pink::RedisCmdArgsType& argv) {
  PikaReplBgWorker* worker = static_cast<PikaReplBgWorker*>(parser->data);
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
  std::shared_ptr<Cmd> c_ptr = g_pika_cmd_table_manager->GetCmd(slash::StringToLower(opt));
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

  std::shared_ptr<SyncMasterPartition> partition
    = g_pika_rm->GetSyncMasterPartitionByName(PartitionInfo(worker->table_name_, worker->partition_id_));
  if (!partition) {
    LOG(WARNING) << worker->table_name_ << worker->partition_id_ << "Not found.";
  }

  partition->ConsensusProcessLeaderLog(c_ptr, worker->binlog_item_, worker->offset_);
  return 0;
}

void PikaReplBgWorker::HandleBGWorkerWriteDB(void* arg) {
  ReplClientWriteDBTaskArg* task_arg = static_cast<ReplClientWriteDBTaskArg*>(arg);
  const std::shared_ptr<Cmd> c_ptr = task_arg->cmd_ptr;
  const PikaCmdArgsType& argv = c_ptr->argv();
  std::string table_name = task_arg->table_name;
  uint32_t partition_id = task_arg->partition_id;

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
      g_pika_server->SlowlogPushEntry(argv, start_time, duration);
      if (g_pika_conf->slowlog_write_errorlog()) {
        LOG(ERROR) << "command: " << argv[0] << ", start_time(s): " << start_time << ", duration(us): " << duration;
      }
    }
  }
  delete task_arg;
}

