// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_admin.h"

#include <algorithm>
#include <sys/time.h>
#include <sys/utsname.h>

#include "slash/include/rsync.h"

#include "include/pika_conf.h"
#include "include/pika_server.h"
#include "include/pika_rm.h"
#include "include/pika_version.h"
#include "include/build_version.h"

#ifdef TCMALLOC_EXTENSION
#include <gperftools/malloc_extension.h>
#endif

extern PikaServer *g_pika_server;
extern PikaConf *g_pika_conf;
extern PikaReplicaManager *g_pika_rm;

/*
 * slaveof no one
 * slaveof ip port
 * slaveof ip port force
 */
void SlaveofCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlaveof);
    return;
  }

  if (argv_.size() == 3
    && !strcasecmp(argv_[1].data(), "no")
    && !strcasecmp(argv_[2].data(), "one")) {
    is_noone_ = true;
    return;
  }

  // self is master of A , want to slavof B
  if (g_pika_server->role() & PIKA_ROLE_MASTER) {
    res_.SetRes(CmdRes::kErrOther, "already master of others, invalid usage");
    return;
  }

  master_ip_ = argv_[1];
  std::string str_master_port = argv_[2];
  if (!slash::string2l(str_master_port.data(), str_master_port.size(), &master_port_) || master_port_ <= 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }

  if ((master_ip_ == "127.0.0.1" || master_ip_ == g_pika_server->host())
    && master_port_ == g_pika_server->port()) {
    res_.SetRes(CmdRes::kErrOther, "you fucked up");
    return;
  }

  if (argv_.size() == 4) {
    if (!strcasecmp(argv_[3].data(), "force")) {
      g_pika_server->SetForceFullSync(true);
    } else {
      res_.SetRes(CmdRes::kWrongNum, kCmdNameSlaveof);
    }
  }
}

void SlaveofCmd::Do(std::shared_ptr<Partition> partition) {
  // Check if we are already connected to the specified master
  if ((master_ip_ == "127.0.0.1" || g_pika_server->master_ip() == master_ip_)
    && g_pika_server->master_port() == master_port_) {
    res_.SetRes(CmdRes::kOk);
    return;
  }

  g_pika_server->RemoveMaster();

  if (is_noone_) {
    res_.SetRes(CmdRes::kOk);
    return;
  }

  bool sm_ret = g_pika_server->SetMaster(master_ip_, master_port_);
  
  if (sm_ret) {
    res_.SetRes(CmdRes::kOk);
    g_pika_conf->SetSlaveof(master_ip_ + ":" + std::to_string(master_port_));
  } else {
    res_.SetRes(CmdRes::kErrOther, "Server is not in correct state for slaveof");
  }
}

/*
 * dbslaveof db[0 ~ 7] no one
 * dbslaveof db[0 ~ 7] filenum offset
 */
void DbSlaveofCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameDbSlaveof);
    return;
  }
  if (!g_pika_conf->classic_mode()) {
    res_.SetRes(CmdRes::kErrOther, "DbSlaveof only support on classic mode");
    return;
  }
  if (g_pika_server->role() ^ PIKA_ROLE_SLAVE
    || !g_pika_server->MetaSyncDone()) {
    res_.SetRes(CmdRes::kErrOther, "Not currently a slave");
    return;
  }

  db_name_ = argv_[1];
  if (!g_pika_server->IsTableExist(db_name_)) {
    res_.SetRes(CmdRes::kErrOther, "Invaild db name");
    return;
  }

  if (!strcasecmp(argv_[2].data(), "no")
    && !strcasecmp(argv_[3].data(), "one")) {
    is_noone_ = true;
    return;
  }

  if (!slash::string2l(argv_[2].data(), argv_[2].size(), &filenum_) || filenum_ < 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  if (!slash::string2l(argv_[3].data(), argv_[3].size(), &offset_) || offset_ < 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
}

void DbSlaveofCmd::Do(std::shared_ptr<Partition> partition) {
  std::shared_ptr<Partition> db_partition = g_pika_server->GetPartitionByDbName(db_name_);

  if (!db_partition) {
    res_.SetRes(CmdRes::kErrOther, "Db not found");
    return;
  }

  std::shared_ptr<SyncSlavePartition> slave_partition =
    g_pika_rm->GetSyncSlavePartitionByName(RmNode(db_partition->GetTableName(), db_partition->GetPartitionId()));
  if (!slave_partition) {
    res_.SetRes(CmdRes::kErrOther, "Db not found");
    return;
  }

  Status s;
  if (is_noone_) {
    if (slave_partition->State() == ReplState::kConnected) {
      slave_partition->SetReplState(ReplState::kNoConnect);
      s = g_pika_server->SendRemoveSlaveNodeRequest(
              db_partition->GetTableName(), db_partition->GetPartitionId());
    }
  } else {
    if (slave_partition->State() == ReplState::kNoConnect
        || slave_partition->State() == ReplState::kError) {
      db_partition->logger()->SetProducerStatus(filenum_, offset_);
      slave_partition->SetReplState(ReplState::kTryConnect);
      g_pika_server->SetLoopPartitionStateMachine(true);
    }
  }

  if (s.ok()) {
    res_.SetRes(CmdRes::kOk);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void AuthCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameAuth);
    return;
  }
  pwd_ = argv_[1];
}

void AuthCmd::Do(std::shared_ptr<Partition> partition) {
  std::string root_password(g_pika_conf->requirepass());
  std::string user_password(g_pika_conf->userpass());
  if (user_password.empty() && root_password.empty()) {
    res_.SetRes(CmdRes::kErrOther, "Client sent AUTH, but no password is set");
    return;
  }

  if (pwd_ == user_password) {
    res_.SetRes(CmdRes::kOk, "USER");
  }
  if (pwd_ == root_password) {
    res_.SetRes(CmdRes::kOk, "ROOT");
  }
  if (res_.none()) {
    res_.SetRes(CmdRes::kInvalidPwd);
  }
}

void BgsaveCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameBgsave);
    return;
  }
  if (argv_.size() == 2) {
    std::vector<std::string> tables;
    slash::StringSplit(argv_[1], COMMA, tables);
    for (const auto& table : tables) {
      if (!g_pika_server->IsTableExist(table)) {
        res_.SetRes(CmdRes::kInvalidTable, table);
        return;
      } else {
        bgsave_tables_.insert(table);
      }
    }
  }
}

void BgsaveCmd::Do(std::shared_ptr<Partition> partition) {
  g_pika_server->DoSameThingSpecificTable(TaskType::kBgSave, bgsave_tables_);
  LogCommand();
  res_.AppendContent("+Background saving started");
}

void CompactCmd::DoInitial() {
  if (!CheckArg(argv_.size())
    || argv_.size() > 3) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameCompact);
    return;
  }

  if (g_pika_server->IsKeyScaning()) {
    res_.SetRes(CmdRes::kErrOther, "The info keyspace operation is executing, Try again later");
    return;
  }

  if (argv_.size() == 1) {
    struct_type_ = "all";
  } else if (argv_.size() == 2) {
    struct_type_ = argv_[1];
  } else if (argv_.size() == 3) {
    std::vector<std::string> tables;
    slash::StringSplit(argv_[1], COMMA, tables);
    for (const auto& table : tables) {
      if (!g_pika_server->IsTableExist(table)) {
        res_.SetRes(CmdRes::kInvalidTable, table);
        return;
      } else {
        compact_tables_.insert(table);
      }
    }
    struct_type_ = argv_[2];
  }
}

void CompactCmd::Do(std::shared_ptr<Partition> partition) {
  if (!strcasecmp(struct_type_.data(), "all")) {
    g_pika_server->DoSameThingSpecificTable(TaskType::kCompactAll, compact_tables_);
  } else if (!strcasecmp(struct_type_.data(), "string")) {
    g_pika_server->DoSameThingSpecificTable(TaskType::kCompactStrings, compact_tables_);
  } else if (!strcasecmp(struct_type_.data(), "hash")) {
    g_pika_server->DoSameThingSpecificTable(TaskType::kCompactHashes, compact_tables_);
  } else if (!strcasecmp(struct_type_.data(), "set")) {
    g_pika_server->DoSameThingSpecificTable(TaskType::kCompactSets, compact_tables_);
  } else if (!strcasecmp(struct_type_.data(), "zset")) {
    g_pika_server->DoSameThingSpecificTable(TaskType::kCompactZSets, compact_tables_);
  } else if (!strcasecmp(struct_type_.data(), "list")) {
    g_pika_server->DoSameThingSpecificTable(TaskType::kCompactList, compact_tables_);
  } else {
    res_.SetRes(CmdRes::kInvalidDbType, struct_type_);
    return;
  }
  LogCommand();
  res_.SetRes(CmdRes::kOk);
}

void PurgelogstoCmd::DoInitial() {
  if (!CheckArg(argv_.size())
    || argv_.size() > 3) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePurgelogsto);
    return;
  }
  std::string filename = argv_[1];
  if (filename.size() <= kBinlogPrefixLen ||
    kBinlogPrefix != filename.substr(0, kBinlogPrefixLen)) {
    res_.SetRes(CmdRes::kInvalidParameter);
    return;
  }
  std::string str_num = filename.substr(kBinlogPrefixLen);
  int64_t num = 0;
  if (!slash::string2l(str_num.data(), str_num.size(), &num) || num < 0) {
    res_.SetRes(CmdRes::kInvalidParameter);
    return;
  }
  num_ = num;

  table_ = (argv_.size() == 3) ? argv_[2] :g_pika_conf->default_table();
  if (!g_pika_server->IsTableExist(table_)) {
    res_.SetRes(CmdRes::kInvalidTable, table_);
    return;
  }
}

void PurgelogstoCmd::Do(std::shared_ptr<Partition> partition) {
  std::shared_ptr<Partition> table_partition = g_pika_server->GetTablePartitionById(table_, 0);
  if (!table_partition) {
    res_.SetRes(CmdRes::kErrOther, "Partition not found");
  } else {
    table_partition->PurgeLogs(num_, true);
    res_.SetRes(CmdRes::kOk);
  }
}

void PingCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePing);
    return;
  }
}

void PingCmd::Do(std::shared_ptr<Partition> partition) {
  res_.SetRes(CmdRes::kPong);
}

void SelectCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSelect);
    return;
  }
}

void SelectCmd::Do(std::shared_ptr<Partition> partition) {
}

void FlushallCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameFlushall);
    return;
  }
}
void FlushallCmd::Do(std::shared_ptr<Partition> partition) {
  if (!partition) {
    LOG(INFO) << "Flushall, but partition not found";
  } else {
    partition->FlushDB();
  }
}

// flushall convert flushdb writes to every partition binlog
std::string FlushallCmd::ToBinlog(
      uint32_t exec_time,
      const std::string& server_id,
      uint64_t logic_id,
      uint32_t filenum,
      uint64_t offset) {
  std::string content;
  content.reserve(RAW_ARGS_LEN);
  RedisAppendLen(content, 1, "*");

  // to flushdb cmd
  std::string flushdb_cmd("flushdb");
  RedisAppendLen(content, flushdb_cmd.size(), "$");
  RedisAppendContent(content, flushdb_cmd);
  return PikaBinlogTransverter::BinlogEncode(BinlogType::TypeFirst,
                                             exec_time,
                                             std::stoi(server_id),
                                             logic_id,
                                             filenum,
                                             offset,
                                             content,
                                             {});
}

void FlushdbCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameFlushdb);
    return;
  }
  if (argv_.size() == 1) {
    db_name_ = "all";
  } else {
    std::string struct_type = argv_[1];
    if (!strcasecmp(struct_type.data(), "string")) {
      db_name_ = "strings";
    } else if (!strcasecmp(struct_type.data(), "hash")) {
      db_name_ = "hashes";
    } else if (!strcasecmp(struct_type.data(), "set")) {
      db_name_ = "sets";
    } else if (!strcasecmp(struct_type.data(), "zset")) {
      db_name_ = "zsets";
    } else if (!strcasecmp(struct_type.data(), "list")) {
      db_name_ = "lists";
    } else {
      res_.SetRes(CmdRes::kInvalidDbType);
    }
  }
}

void FlushdbCmd::Do(std::shared_ptr<Partition> partition) {
  if (!partition) {
    LOG(INFO) << "Flushdb, but partition not found";
  } else {
    if (db_name_ == "all") {
      partition->FlushDB();
    } else {
      partition->FlushSubDB(db_name_);
    }
  }
}

void ClientCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameClient);
    return;
  }
  if (!strcasecmp(argv_[1].data(), "list") && argv_.size() == 2) {
    //nothing
  } else if (!strcasecmp(argv_[1].data(), "kill") && argv_.size() == 3) {
    ip_port_ = argv_[2];
  } else {
    res_.SetRes(CmdRes::kErrOther, "Syntax error, try CLIENT (LIST | KILL ip:port)");
    return;
  }
  operation_ = argv_[1];
  return;
}

void ClientCmd::Do(std::shared_ptr<Partition> partition) {
  if (!strcasecmp(operation_.data(), "list")) {
    struct timeval now;
    gettimeofday(&now, NULL);
    std::vector<ClientInfo> clients;
    g_pika_server->ClientList(&clients);
    std::vector<ClientInfo>::iterator iter= clients.begin();
    std::string reply = "";
    char buf[128];
    while (iter != clients.end()) {
      snprintf(buf, sizeof(buf), "addr=%s fd=%d idle=%ld\n", iter->ip_port.c_str(), iter->fd, iter->last_interaction == 0 ? 0 : now.tv_sec - iter->last_interaction);
      reply.append(buf);
      iter++;
    }
    res_.AppendString(reply);
  } else if (!strcasecmp(operation_.data(), "kill") && !strcasecmp(ip_port_.data(), "all")) {
    g_pika_server->ClientKillAll();
    res_.SetRes(CmdRes::kOk);
  } else if (g_pika_server->ClientKill(ip_port_) == 1) {
    res_.SetRes(CmdRes::kOk);
  } else {
    res_.SetRes(CmdRes::kErrOther, "No such client");
  }
  return;
}

void ShutdownCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameShutdown);
    return;
  }
}
// no return
void ShutdownCmd::Do(std::shared_ptr<Partition> partition) {
  DLOG(WARNING) << "handle \'shutdown\'";
  g_pika_server->Exit();
  res_.SetRes(CmdRes::kNone);
}

const std::string InfoCmd::kInfoSection = "info";
const std::string InfoCmd::kAllSection = "all";
const std::string InfoCmd::kServerSection = "server";
const std::string InfoCmd::kClientsSection = "clients";
const std::string InfoCmd::kStatsSection = "stats";
const std::string InfoCmd::kExecCountSection= "command_exec_count";
const std::string InfoCmd::kCPUSection = "cpu";
const std::string InfoCmd::kReplicationSection = "replication";
const std::string InfoCmd::kKeyspaceSection = "keyspace";
const std::string InfoCmd::kLogSection = "log";
const std::string InfoCmd::kDataSection = "data";
const std::string InfoCmd::kDebugSection = "debug";

void InfoCmd::DoInitial() {
  size_t argc = argv_.size();
  if (argc > 4) {
    res_.SetRes(CmdRes::kSyntaxErr);
    return;
  }
  if (argc == 1) {
    info_section_ = kInfo;
    return;
  } //then the agc is 2 or 3

  if (!strcasecmp(argv_[1].data(), kAllSection.data())) {
    info_section_ = kInfoAll;
  } else if (!strcasecmp(argv_[1].data(), kServerSection.data())) {
    info_section_ = kInfoServer;
  } else if (!strcasecmp(argv_[1].data(), kClientsSection.data())) {
    info_section_ = kInfoClients;
  } else if (!strcasecmp(argv_[1].data(), kStatsSection.data())) {
    info_section_ = kInfoStats;
  } else if (!strcasecmp(argv_[1].data(), kExecCountSection.data())) {
    info_section_ = kInfoExecCount;
  } else if (!strcasecmp(argv_[1].data(), kCPUSection.data())) {
    info_section_ = kInfoCPU;
  } else if (!strcasecmp(argv_[1].data(), kReplicationSection.data())) {
    info_section_ = kInfoReplication;
  } else if (!strcasecmp(argv_[1].data(), kKeyspaceSection.data())) {
    info_section_ = kInfoKeyspace;
    if (argc == 2) {
      LogCommand();
      return;
    }
    // info keyspace [ 0 | 1 | off ]
    // info keyspace 1 db0,db1
    // info keyspace 0 db0,db1
    // info keyspace off db0,db1
    if (argv_[2] == "1") {
      if (g_pika_server->IsCompacting()) {
        res_.SetRes(CmdRes::kErrOther, "The compact operation is executing, Try again later");
      } else {
        rescan_ = true;
      }
    } else if (argv_[2] == "off") {
      off_ = true;
    } else if (argv_[2] != "0") {
      res_.SetRes(CmdRes::kSyntaxErr);
    }

    if (argc == 4) {
      std::vector<std::string> tables;
      slash::StringSplit(argv_[3], COMMA, tables);
      for (const auto& table : tables) {
        if (!g_pika_server->IsTableExist(table)) {
          res_.SetRes(CmdRes::kInvalidTable, table);
          return;
        } else {
          keyspace_scan_tables_.insert(table);
        }
      }
    }
    LogCommand();
    return;
  } else if (!strcasecmp(argv_[1].data(), kLogSection.data())) {
    info_section_ = kInfoLog;
  } else if (!strcasecmp(argv_[1].data(), kDataSection.data())) {
    info_section_ = kInfoData;
  } else if (!strcasecmp(argv_[1].data(), kDebugSection.data())) {
    info_section_ = kInfoDebug;
  } else {
    info_section_ = kInfoErr;
  }
  if (argc != 2) {
    res_.SetRes(CmdRes::kSyntaxErr);
  }
}

void InfoCmd::Do(std::shared_ptr<Partition> partition) {
  std::string info;
  switch (info_section_) {
    case kInfo:
      InfoServer(info);
      info.append("\r\n");
      InfoData(info);
      info.append("\r\n");
      InfoLog(info);
      info.append("\r\n");
      InfoClients(info);
      info.append("\r\n");
      InfoStats(info);
      info.append("\r\n");
      InfoCPU(info);
      info.append("\r\n");
      InfoReplication(info);
      info.append("\r\n");
      InfoKeyspace(info);
      break;
    case kInfoAll:
      InfoServer(info);
      info.append("\r\n");
      InfoData(info);
      info.append("\r\n");
      InfoLog(info);
      info.append("\r\n");
      InfoClients(info);
      info.append("\r\n");
      InfoStats(info);
      info.append("\r\n");
      InfoExecCount(info);
      info.append("\r\n");
      InfoCPU(info);
      info.append("\r\n");
      InfoReplication(info);
      info.append("\r\n");
      InfoKeyspace(info);
      break;
    case kInfoServer:
      InfoServer(info);
      break;
    case kInfoClients:
      InfoClients(info);
      break;
    case kInfoStats:
      InfoStats(info);
      break;
    case kInfoExecCount:
      InfoExecCount(info);
      break;
    case kInfoCPU:
      InfoCPU(info);
      break;
    case kInfoReplication:
      InfoReplication(info);
      break;
    case kInfoKeyspace:
      InfoKeyspace(info);
      break;
    case kInfoLog:
      InfoLog(info);
      break;
    case kInfoData:
      InfoData(info);
      break;
    case kInfoDebug:
      InfoDebug(info);
      break;
    default:
      //kInfoErr is nothing
      break;
  }


  res_.AppendStringLen(info.size());
  res_.AppendContent(info);
  return;
}

void InfoCmd::InfoServer(std::string& info) {
  static struct utsname host_info;
  static bool host_info_valid = false;
  if (!host_info_valid) {
    uname(&host_info);
    host_info_valid = true;
  }

  time_t current_time_s = time(NULL);
  std::stringstream tmp_stream;
  char version[32];
  snprintf(version, sizeof(version), "%d.%d.%d", PIKA_MAJOR,
      PIKA_MINOR, PIKA_PATCH);
  tmp_stream << "# Server\r\n";
  tmp_stream << "pika_version:" << version << "\r\n";
  tmp_stream << pika_build_git_sha << "\r\n";
  tmp_stream << "pika_build_compile_date: " <<
    pika_build_compile_date << "\r\n";
  tmp_stream << "os:" << host_info.sysname << " " << host_info.release << " " << host_info.machine << "\r\n";
  tmp_stream << "arch_bits:" << (reinterpret_cast<char*>(&host_info.machine) + strlen(host_info.machine) - 2) << "\r\n";
  tmp_stream << "process_id:" << getpid() << "\r\n";
  tmp_stream << "tcp_port:" << g_pika_conf->port() << "\r\n";
  tmp_stream << "thread_num:" << g_pika_conf->thread_num() << "\r\n";
  tmp_stream << "sync_thread_num:" << g_pika_conf->sync_thread_num() << "\r\n";
  tmp_stream << "uptime_in_seconds:" << (current_time_s - g_pika_server->start_time_s()) << "\r\n";
  tmp_stream << "uptime_in_days:" << (current_time_s / (24*3600) - g_pika_server->start_time_s() / (24*3600) + 1) << "\r\n";
  tmp_stream << "config_file:" << g_pika_conf->conf_path() << "\r\n";
  tmp_stream << "server_id:" << g_pika_conf->server_id() << "\r\n";

  info.append(tmp_stream.str());
}

void InfoCmd::InfoClients(std::string& info) {
  std::stringstream tmp_stream;
  tmp_stream << "# Clients\r\n";
  tmp_stream << "connected_clients:" << g_pika_server->ClientList() << "\r\n";

  info.append(tmp_stream.str());
}

void InfoCmd::InfoStats(std::string& info) {
  std::stringstream tmp_stream;
  tmp_stream << "# Stats\r\n";
  tmp_stream << "total_connections_received:" << g_pika_server->accumulative_connections() << "\r\n";
  tmp_stream << "instantaneous_ops_per_sec:" << g_pika_server->ServerCurrentQps() << "\r\n";
  tmp_stream << "total_commands_processed:" << g_pika_server->ServerQueryNum() << "\r\n";
  tmp_stream << "is_bgsaving:" << (g_pika_server->IsBgSaving() ? "Yes" : "No") << "\r\n";
  tmp_stream << "is_scaning_keyspace:" << (g_pika_server->IsKeyScaning() ? "Yes" : "No") << "\r\n";
  tmp_stream << "is_compact:" << (g_pika_server->IsCompacting() ? "Yes" : "No") << "\r\n";
  tmp_stream << "compact_cron:" << g_pika_conf->compact_cron() << "\r\n";
  tmp_stream << "compact_interval:" << g_pika_conf->compact_interval() << "\r\n";

  info.append(tmp_stream.str());
}

void InfoCmd::InfoExecCount(std::string& info) {
  std::stringstream tmp_stream;
  tmp_stream << "# Command_Exec_Count\r\n";

  std::unordered_map<std::string, uint64_t> command_exec_count_table = g_pika_server->ServerExecCountTable();
  for (const auto& item : command_exec_count_table) {
    tmp_stream << item.first << ":" << item.second << "\r\n";
  }
  info.append(tmp_stream.str());
}

void InfoCmd::InfoCPU(std::string& info) {
  struct rusage self_ru, c_ru;
  getrusage(RUSAGE_SELF, &self_ru);
  getrusage(RUSAGE_CHILDREN, &c_ru);
  std::stringstream tmp_stream;
  tmp_stream << "# CPU\r\n";
  tmp_stream << "used_cpu_sys:" <<
    setiosflags(std::ios::fixed) << std::setprecision(2) <<
    (float)self_ru.ru_stime.tv_sec+(float)self_ru.ru_stime.tv_usec/1000000 <<
    "\r\n";
  tmp_stream << "used_cpu_user:" <<
    setiosflags(std::ios::fixed) << std::setprecision(2) <<
    (float)self_ru.ru_utime.tv_sec+(float)self_ru.ru_utime.tv_usec/1000000 <<
    "\r\n";
  tmp_stream << "used_cpu_sys_children:" <<
    setiosflags(std::ios::fixed) << std::setprecision(2) <<
    (float)c_ru.ru_stime.tv_sec+(float)c_ru.ru_stime.tv_usec/1000000 <<
    "\r\n";
  tmp_stream << "used_cpu_user_children:" <<
    setiosflags(std::ios::fixed) << std::setprecision(2) <<
    (float)c_ru.ru_utime.tv_sec+(float)c_ru.ru_utime.tv_usec/1000000 <<
    "\r\n";
  info.append(tmp_stream.str());
}

void InfoCmd::InfoReplication(std::string& info) {
  int host_role = g_pika_server->role();
  std::stringstream tmp_stream;
  std::stringstream out_of_sync;

  bool all_partition_sync = true;
  slash::RWLock table_rwl(&g_pika_server->tables_rw_, false);
  for (const auto& table_item : g_pika_server->tables_) {
    slash::RWLock partition_rwl(&table_item.second->partitions_rw_, false);
    for (const auto& partition_item : table_item.second->partitions_) {
      std::shared_ptr<SyncSlavePartition> slave_partition
        = g_pika_rm->GetSyncSlavePartitionByName(
          RmNode(table_item.second->GetTableName(),
            partition_item.second->GetPartitionId()));
      if (!slave_partition) {
        out_of_sync << "(" << partition_item.second->GetPartitionName() << ": InternalError)";
        continue;
      }
      if (slave_partition->State() != ReplState::kConnected) {
        all_partition_sync = false;
        out_of_sync << "(" << partition_item.second->GetPartitionName() << ":";
        if (slave_partition->State() == ReplState::kNoConnect) {
          out_of_sync << "NoConnect)";
        } else if (slave_partition->State() == ReplState::kWaitDBSync) {
          out_of_sync << "WaitDBSync)";
        } else if (slave_partition->State() == ReplState::kError) {
          out_of_sync << "Error)";
        } else {
          out_of_sync << "Other)";
        }
      }
    }
  }

  tmp_stream << "# Replication(";
  switch (host_role) {
    case PIKA_ROLE_SINGLE :
    case PIKA_ROLE_MASTER : tmp_stream << "MASTER)\r\nrole:master\r\n"; break;
    case PIKA_ROLE_SLAVE : tmp_stream << "SLAVE)\r\nrole:slave\r\n"; break;
    case PIKA_ROLE_MASTER | PIKA_ROLE_SLAVE : tmp_stream << "Master && SLAVE)\r\nrole:master&&slave\r\n"; break;
    default: info.append("ERR: server role is error\r\n"); return;
  }

  std::string slaves_list_str;
  switch (host_role) {
    case PIKA_ROLE_SLAVE :
      tmp_stream << "master_host:" << g_pika_server->master_ip() << "\r\n";
      tmp_stream << "master_port:" << g_pika_server->master_port() << "\r\n";
      tmp_stream << "master_link_status:" << (((g_pika_server->repl_state() == PIKA_REPL_META_SYNC_DONE)
              && all_partition_sync) ? "up" : "down") << "\r\n";
      tmp_stream << "slave_priority:" << g_pika_conf->slave_priority() << "\r\n";
      tmp_stream << "slave_read_only:" << g_pika_conf->slave_read_only() << "\r\n";
      if (!all_partition_sync) {
        tmp_stream <<"db_repl_error_state:" << out_of_sync.str() << "\r\n";
      }
      break;
    case PIKA_ROLE_MASTER | PIKA_ROLE_SLAVE :
      tmp_stream << "master_host:" << g_pika_server->master_ip() << "\r\n";
      tmp_stream << "master_port:" << g_pika_server->master_port() << "\r\n";
      tmp_stream << "master_link_status:" << (((g_pika_server->repl_state() == PIKA_REPL_META_SYNC_DONE)
              && all_partition_sync) ? "up" : "down") << "\r\n";
      tmp_stream << "slave_read_only:" << g_pika_conf->slave_read_only() << "\r\n";
      if (!all_partition_sync) {
        tmp_stream <<"db_repl_error_state:" << out_of_sync.str() << "\r\n";
      }
    case PIKA_ROLE_SINGLE :
    case PIKA_ROLE_MASTER :
      tmp_stream << "connected_slaves:" << g_pika_server->GetSlaveListString(slaves_list_str) << "\r\n" << slaves_list_str;
  }

  info.append(tmp_stream.str());
}

void InfoCmd::InfoKeyspace(std::string& info) {
  if (off_) {
    g_pika_server->DoSameThingSpecificTable(TaskType::kStopKeyScan, keyspace_scan_tables_);
    info.append("OK\r\n");
    return;
  }

  std::string table_name;
  KeyScanInfo key_scan_info;
  int32_t duration;
  std::vector<blackwidow::KeyInfo> key_infos;
  std::stringstream tmp_stream;
  tmp_stream << "# Keyspace\r\n";
  slash::RWLock rwl(&g_pika_server->tables_rw_, false);
  for (const auto& table_item : g_pika_server->tables_) {
    if (keyspace_scan_tables_.empty()
      || keyspace_scan_tables_.find(table_item.first) != keyspace_scan_tables_.end()) {
      table_name = table_item.second->GetTableName();
      key_scan_info = table_item.second->GetKeyScanInfo();
      key_infos = key_scan_info.key_infos;
      duration = key_scan_info.duration;
      if (key_infos.size() != 5) {
        info.append("info keyspace error\r\n");
        return;
      }
      tmp_stream << "# Time:" << key_scan_info.s_start_time << "\r\n";
      if (duration == -2) {
        tmp_stream << "# Duration: " << "In Waiting\r\n";
      } else if (duration == -1) {
        tmp_stream << "# Duration: " << "In Processing\r\n";
      } else if (duration >= 0) {
        tmp_stream << "# Duration: " << std::to_string(duration) + "s" << "\r\n";
      }

      tmp_stream << table_name << "_Strings: keys=" << key_infos[0].keys << ", expires=" << key_infos[0].expires << ", invaild_keys=" << key_infos[0].invaild_keys << "\r\n";
      tmp_stream << table_name << "_Hashes: keys=" << key_infos[1].keys << ", expires=" << key_infos[1].expires << ", invaild_keys=" << key_infos[1].invaild_keys << "\r\n";
      tmp_stream << table_name << "_Lists: keys=" << key_infos[2].keys << ", expires=" << key_infos[2].expires << ", invaild_keys=" << key_infos[2].invaild_keys << "\r\n";
      tmp_stream << table_name << "_Zsets: keys=" << key_infos[3].keys << ", expires=" << key_infos[3].expires << ", invaild_keys=" << key_infos[3].invaild_keys << "\r\n";
      tmp_stream << table_name << "_Sets: keys=" << key_infos[4].keys << ", expires=" << key_infos[4].expires << ", invaild_keys=" << key_infos[4].invaild_keys << "\r\n\r\n";
    }
  }
  info.append(tmp_stream.str());

  if (rescan_) {
    g_pika_server->DoSameThingSpecificTable(TaskType::kStartKeyScan, keyspace_scan_tables_);
  }
  return;
}

void InfoCmd::InfoLog(std::string& info) {
  std::stringstream  tmp_stream;
  tmp_stream << "# Log" << "\r\n";
  int64_t log_size = slash::Du(g_pika_conf->log_path());
  tmp_stream << "log_size:" << log_size << "\r\n";
  tmp_stream << "log_size_human:" << (log_size >> 20) << "M\r\n";
  tmp_stream << "expire_logs_days:" << g_pika_conf->expire_logs_days() << "\r\n";
  tmp_stream << "expire_logs_nums:" << g_pika_conf->expire_logs_nums() << "\r\n";

  uint32_t filenum;
  uint64_t offset;
  SlaveState slave_state;
  BinlogOffset sent_slave_boffset;
  BinlogOffset acked_slave_boffset;
  slash::RWLock table_rwl(&g_pika_server->tables_rw_, false);
  for (const auto& table_item : g_pika_server->tables_) {
    slash::RWLock partition_rwl(&table_item.second->partitions_rw_, false);
    for (const auto& patition_item : table_item.second->partitions_) {
      patition_item.second->logger()->GetProducerStatus(&filenum, &offset);
      tmp_stream << patition_item.second->GetPartitionName() << ":binlog_offset=" << filenum << " " << offset;

      bool success = true;
      uint32_t purge_max = filenum;
      if (purge_max >= 10) {
        purge_max -= 10;   //remain some more
        slash::MutexLock l(&g_pika_server->slave_mutex_);
        for (const auto& slave : g_pika_server->slaves_) {
          RmNode rm_node(slave.ip, slave.port,
                  patition_item.second->GetTableName(),
                  patition_item.second->GetPartitionId());
          Status s = g_pika_rm->GetSyncMasterPartitionSlaveState(rm_node, &slave_state);
          if (s.ok()
            && slave_state == SlaveState::kSlaveBinlogSync
            && g_pika_rm->GetSyncBinlogStatus(rm_node, &sent_slave_boffset, &acked_slave_boffset).ok()
            && sent_slave_boffset.filenum > 0) {
            purge_max = (sent_slave_boffset.filenum - 1 < purge_max)
                ? sent_slave_boffset.filenum - 1 : purge_max;
          }
        }
      } else {
        success = false;
      }
      tmp_stream << ",safety_purge=" << (success ? kBinlogPrefix + std::to_string(static_cast<int32_t>(purge_max)) : "none") << "\r\n";
    }
  }
  info.append(tmp_stream.str());
  return;
}

void InfoCmd::InfoData(std::string& info) {
  std::stringstream tmp_stream;

  int64_t db_size = slash::Du(g_pika_conf->db_path());
  tmp_stream << "# Data" << "\r\n";
  tmp_stream << "db_size:" << db_size << "\r\n";
  tmp_stream << "db_size_human:" << (db_size >> 20) << "M\r\n";
  tmp_stream << "compression:" << g_pika_conf->compression() << "\r\n";

  // rocksdb related memory usage
  uint64_t total_memtable_usage = 0, memtable_usage = 0;
  uint64_t total_table_reader_usage = 0, table_reader_usage = 0;
  slash::RWLock table_rwl(&g_pika_server->tables_rw_, false);
  for (const auto& table_item : g_pika_server->tables_) {
    slash::RWLock partition_rwl(&table_item.second->partitions_rw_, false);
    for (const auto& patition_item : table_item.second->partitions_) {
      memtable_usage = table_reader_usage = 0;
      patition_item.second->DbRWLockReader();
      patition_item.second->db()->GetUsage(blackwidow::USAGE_TYPE_ROCKSDB_MEMTABLE, &memtable_usage);
      patition_item.second->db()->GetUsage(blackwidow::USAGE_TYPE_ROCKSDB_TABLE_READER, &table_reader_usage);
      patition_item.second->DbRWUnLock();
      total_memtable_usage += memtable_usage;
      total_table_reader_usage += table_reader_usage;
    }
  }

  tmp_stream << "used_memory:" << (total_memtable_usage + total_table_reader_usage) << "\r\n";
  tmp_stream << "used_memory_human:" << ((total_memtable_usage + total_table_reader_usage) >> 20) << "M\r\n";
  tmp_stream << "db_memtable_usage:" << total_memtable_usage << "\r\n";
  tmp_stream << "db_tablereader_usage:" << total_table_reader_usage << "\r\n";

  info.append(tmp_stream.str());
  return;
}

void InfoCmd::InfoDebug(std::string& info) {
  std::stringstream tmp_stream;
  tmp_stream << "# Synchronization Status" << "\r\n";
  info.append(tmp_stream.str());
  g_pika_rm->RmStatus(&info);
  return;
}

void ConfigCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameConfig);
    return;
  }
  size_t argc = argv_.size();
  if (!strcasecmp(argv_[1].data(), "get")) {
    if (argc != 3) {
      res_.SetRes(CmdRes::kErrOther, "Wrong number of arguments for CONFIG get");
      return;
    }
  } else if (!strcasecmp(argv_[1].data(), "set")) {
    if (argc == 3 && argv_[2] != "*") {
      res_.SetRes(CmdRes::kErrOther, "Wrong number of arguments for CONFIG set");
      return;
    } else if (argc != 4 && argc != 3) {
      res_.SetRes(CmdRes::kErrOther, "Wrong number of arguments for CONFIG set");
      return;
    }
  } else if (!strcasecmp(argv_[1].data(), "rewrite")) {
    if (argc != 2) {
      res_.SetRes(CmdRes::kErrOther, "Wrong number of arguments for CONFIG rewrite");
      return;
    }
  } else if (!strcasecmp(argv_[1].data(), "resetstat")) {
    if (argc != 2) {
      res_.SetRes(CmdRes::kErrOther, "Wrong number of arguments for CONFIG resetstat");
      return;
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, "CONFIG subcommand must be one of GET, SET, RESETSTAT, REWRITE");
    return;
  }
  config_args_v_.assign(argv_.begin() + 1, argv_.end());
  return;
}

void ConfigCmd::Do(std::shared_ptr<Partition> partition) {
  std::string config_ret;
  if (!strcasecmp(config_args_v_[0].data(), "get")) {
    ConfigGet(config_ret);
  } else if (!strcasecmp(config_args_v_[0].data(), "set")) {
    ConfigSet(config_ret);
  } else if (!strcasecmp(config_args_v_[0].data(), "rewrite")) {
    ConfigRewrite(config_ret);
  } else if (!strcasecmp(config_args_v_[0].data(), "resetstat")) {
    ConfigResetstat(config_ret);
  }
  res_.AppendStringRaw(config_ret);
  return;
}

static void EncodeString(std::string *dst, const std::string &value) {
  dst->append("$");
  dst->append(std::to_string(value.size()));
  dst->append("\r\n");
  dst->append(value.data(), value.size());
  dst->append("\r\n");
}

static void EncodeInt32(std::string *dst, const int32_t v) {
  std::string vstr = std::to_string(v);
  dst->append("$");
  dst->append(std::to_string(vstr.length()));
  dst->append("\r\n");
  dst->append(vstr);
  dst->append("\r\n");
}

static void EncodeInt64(std::string *dst, const int64_t v) {
  std::string vstr = std::to_string(v);
  dst->append("$");
  dst->append(std::to_string(vstr.length()));
  dst->append("\r\n");
  dst->append(vstr);
  dst->append("\r\n");
}

void ConfigCmd::ConfigGet(std::string &ret) {
  size_t elements = 0;
  std::string config_body;
  std::string pattern = config_args_v_[1];

  if (slash::stringmatch(pattern.data(), "port", 1)) {
    elements += 2;
    EncodeString(&config_body, "port");
    EncodeInt32(&config_body, g_pika_conf->port());
  }

  if (slash::stringmatch(pattern.data(), "thread-num", 1)) {
    elements += 2;
    EncodeString(&config_body, "thread-num");
    EncodeInt32(&config_body, g_pika_conf->thread_num());
  }

  if (slash::stringmatch(pattern.data(), "thread-pool-size", 1)) {
    elements += 2;
    EncodeString(&config_body, "thread-pool-size");
    EncodeInt32(&config_body, g_pika_conf->thread_pool_size());
  }

  if (slash::stringmatch(pattern.data(), "sync-thread-num", 1)) {
    elements += 2;
    EncodeString(&config_body, "sync-thread-num");
    EncodeInt32(&config_body, g_pika_conf->sync_thread_num());
  }

  if (slash::stringmatch(pattern.data(), "log-path", 1)) {
    elements += 2;
    EncodeString(&config_body, "log-path");
    EncodeString(&config_body, g_pika_conf->log_path());
  }

  if (slash::stringmatch(pattern.data(), "db-path", 1)) {
    elements += 2;
    EncodeString(&config_body, "db-path");
    EncodeString(&config_body, g_pika_conf->db_path());
  }

  if (slash::stringmatch(pattern.data(), "maxmemory", 1)) {
    elements += 2;
    EncodeString(&config_body, "maxmemory");
    EncodeInt64(&config_body, g_pika_conf->write_buffer_size());
  }

  if (slash::stringmatch(pattern.data(), "write-buffer-size", 1)) {
    elements += 2;
    EncodeString(&config_body, "write-buffer-size");
    EncodeInt64(&config_body, g_pika_conf->write_buffer_size());
  }

  if (slash::stringmatch(pattern.data(), "timeout", 1)) {
    elements += 2;
    EncodeString(&config_body, "timeout");
    EncodeInt32(&config_body, g_pika_conf->timeout());
  }

  if (slash::stringmatch(pattern.data(), "requirepass", 1)) {
    elements += 2;
    EncodeString(&config_body, "requirepass");
    EncodeString(&config_body, g_pika_conf->requirepass());
  }

  if (slash::stringmatch(pattern.data(), "masterauth", 1)) {
    elements += 2;
    EncodeString(&config_body, "masterauth");
    EncodeString(&config_body, g_pika_conf->masterauth());
  }

  if (slash::stringmatch(pattern.data(), "userpass", 1)) {
    elements += 2;
    EncodeString(&config_body, "userpass");
    EncodeString(&config_body, g_pika_conf->userpass());
  }

  if (slash::stringmatch(pattern.data(), "userblacklist", 1)) {
    elements += 2;
    EncodeString(&config_body, "userblacklist");
    EncodeString(&config_body, (g_pika_conf->suser_blacklist()).c_str());
  }

  if (slash::stringmatch(pattern.data(), "instance-mode", 1)) {
    elements += 2;
    EncodeString(&config_body, "instance-mode");
    EncodeString(&config_body, (g_pika_conf->classic_mode() ? "classic" : "sharding"));
  }

  if (g_pika_conf->classic_mode()
    && slash::stringmatch(pattern.data(), "databases", 1)) {
    elements += 2;
    EncodeString(&config_body, "databases");
    EncodeInt32(&config_body, g_pika_conf->databases());
  }

  if (!g_pika_conf->classic_mode()
    && slash::stringmatch(pattern.data(), "default-partition-num", 1)) {
    elements += 2;
    EncodeString(&config_body, "default-partition-num");
    EncodeInt32(&config_body, g_pika_conf->default_partition_num());
  }

  if (slash::stringmatch(pattern.data(), "daemonize", 1)) {
    elements += 2;
    EncodeString(&config_body, "daemonize");
    EncodeString(&config_body, g_pika_conf->daemonize() ? "yes" : "no");
  }

  if (slash::stringmatch(pattern.data(), "dump-path", 1)) {
    elements += 2;
    EncodeString(&config_body, "dump-path");
    EncodeString(&config_body, g_pika_conf->bgsave_path());
  }

  if (slash::stringmatch(pattern.data(), "dump-expire", 1)) {
    elements += 2;
    EncodeString(&config_body, "dump-expire");
    EncodeInt32(&config_body, g_pika_conf->expire_dump_days());
  }

  if (slash::stringmatch(pattern.data(), "dump-prefix", 1)) {
    elements += 2;
    EncodeString(&config_body, "dump-prefix");
    EncodeString(&config_body, g_pika_conf->bgsave_prefix());
  }

  if (slash::stringmatch(pattern.data(), "pidfile", 1)) {
    elements += 2;
    EncodeString(&config_body, "pidfile");
    EncodeString(&config_body, g_pika_conf->pidfile());
  }

  if (slash::stringmatch(pattern.data(), "maxclients", 1)) {
    elements += 2;
    EncodeString(&config_body, "maxclients");
    EncodeInt32(&config_body, g_pika_conf->maxclients());
  }

  if (slash::stringmatch(pattern.data(), "target-file-size-base", 1)) {
    elements += 2;
    EncodeString(&config_body, "target-file-size-base");
    EncodeInt32(&config_body, g_pika_conf->target_file_size_base());
  }

  if (slash::stringmatch(pattern.data(), "max-cache-statistic-keys", 1)) {
    elements += 2;
    EncodeString(&config_body, "max-cache-statistic-keys");
    EncodeInt32(&config_body, g_pika_conf->max_cache_statistic_keys());
  }

  if (slash::stringmatch(pattern.data(), "small-compaction-threshold", 1)) {
    elements += 2;
    EncodeString(&config_body, "small-compaction-threshold");
    EncodeInt32(&config_body, g_pika_conf->small_compaction_threshold());
  }

  if (slash::stringmatch(pattern.data(), "max-background-flushes", 1)) {
    elements += 2;
    EncodeString(&config_body, "max-background-flushes");
    EncodeInt32(&config_body, g_pika_conf->max_background_flushes());
  }

  if (slash::stringmatch(pattern.data(), "max-background-compactions", 1)) {
    elements += 2;
    EncodeString(&config_body, "max-background-compactions");
    EncodeInt32(&config_body, g_pika_conf->max_background_compactions());
  }

  if (slash::stringmatch(pattern.data(), "max-cache-files", 1)) {
    elements += 2;
    EncodeString(&config_body, "max-cache-files");
    EncodeInt32(&config_body, g_pika_conf->max_cache_files());
  }

  if (slash::stringmatch(pattern.data(), "max-bytes-for-level-multiplier", 1)) {
    elements += 2;
    EncodeString(&config_body, "max-bytes-for-level-multiplier");
    EncodeInt32(&config_body, g_pika_conf->max_bytes_for_level_multiplier());
  }

  if (slash::stringmatch(pattern.data(), "block-size", 1)) {
    elements += 2;
    EncodeString(&config_body, "block-size");
    EncodeInt64(&config_body, g_pika_conf->block_size());
  }

  if (slash::stringmatch(pattern.data(), "block-cache", 1)) {
    elements += 2;
    EncodeString(&config_body, "block-cache");
    EncodeInt64(&config_body, g_pika_conf->block_cache());
  }

  if (slash::stringmatch(pattern.data(), "share-block-cache", 1)) {
    elements += 2;
    EncodeString(&config_body, "share-block-cache");
    EncodeString(&config_body, g_pika_conf->share_block_cache() ? "yes" : "no");
  }

  if (slash::stringmatch(pattern.data(), "cache-index-and-filter-blocks", 1)) {
    elements += 2;
    EncodeString(&config_body, "cache-index-and-filter-blocks");
    EncodeString(&config_body, g_pika_conf->cache_index_and_filter_blocks() ? "yes" : "no");
  }

  if (slash::stringmatch(pattern.data(), "optimize-filters-for-hits", 1)) {
    elements += 2;
    EncodeString(&config_body, "optimize-filters-for-hits");
    EncodeString(&config_body, g_pika_conf->optimize_filters_for_hits() ? "yes" : "no");
  }

  if (slash::stringmatch(pattern.data(), "level-compaction-dynamic-level-bytes", 1)) {
    elements += 2;
    EncodeString(&config_body, "level-compaction-dynamic-level-bytes");
    EncodeString(&config_body, g_pika_conf->level_compaction_dynamic_level_bytes() ? "yes" : "no");
  }

  if (slash::stringmatch(pattern.data(), "expire-logs-days", 1)) {
    elements += 2;
    EncodeString(&config_body, "expire-logs-days");
    EncodeInt32(&config_body, g_pika_conf->expire_logs_days());
  }

  if (slash::stringmatch(pattern.data(), "expire-logs-nums", 1)) {
    elements += 2;
    EncodeString(&config_body, "expire-logs-nums");
    EncodeInt32(&config_body, g_pika_conf->expire_logs_nums());
  }

  if (slash::stringmatch(pattern.data(), "root-connection-num", 1)) {
    elements += 2;
    EncodeString(&config_body, "root-connection-num");
    EncodeInt32(&config_body, g_pika_conf->root_connection_num());
  }

  if (slash::stringmatch(pattern.data(), "slowlog-write-errorlog", 1)) {
    elements += 2;
    EncodeString(&config_body, "slowlog-write-errorlog");
    EncodeString(&config_body, g_pika_conf->slowlog_write_errorlog() ? "yes" : "no");
  }

  if (slash::stringmatch(pattern.data(), "slowlog-log-slower-than", 1)) {
    elements += 2;
    EncodeString(&config_body, "slowlog-log-slower-than");
    EncodeInt32(&config_body, g_pika_conf->slowlog_slower_than());
  }

  if (slash::stringmatch(pattern.data(), "slowlog-max-len", 1)) {
    elements += 2;
    EncodeString(&config_body, "slowlog-max-len");
    EncodeInt32(&config_body, g_pika_conf->slowlog_max_len());
  }

  if (slash::stringmatch(pattern.data(), "slave-read-only", 1)) {
    elements += 2;
    EncodeString(&config_body, "slave-read-only");
    EncodeString(&config_body, g_pika_conf->slave_read_only() ? "yes" : "no");
  }

  if (slash::stringmatch(pattern.data(), "write-binlog", 1)) {
    elements += 2;
    EncodeString(&config_body, "write-binlog");
    EncodeString(&config_body, g_pika_conf->write_binlog() ? "yes" : "no");
  }

  if (slash::stringmatch(pattern.data(), "binlog-file-size", 1)) {
    elements += 2;
    EncodeString(&config_body, "binlog-file-size");
    EncodeInt32(&config_body, g_pika_conf->binlog_file_size());
  }

  if (slash::stringmatch(pattern.data(), "max-cache-statistic-keys", 1)) {
    elements += 2;
    EncodeString(&config_body, "max-cache-statistic-keys");
    EncodeInt32(&config_body, g_pika_conf->max_cache_statistic_keys());
  }

  if (slash::stringmatch(pattern.data(), "small-compaction-threshold", 1)) {
    elements += 2;
    EncodeString(&config_body, "small-compaction-threshold");
    EncodeInt32(&config_body, g_pika_conf->small_compaction_threshold());
  }

  if (slash::stringmatch(pattern.data(), "max-write-buffer-size", 1)) {
    elements += 2;
    EncodeString(&config_body, "max-write-buffer-size");
    EncodeInt64(&config_body, g_pika_conf->max_write_buffer_size());
  }

  if (slash::stringmatch(pattern.data(), "compression", 1)) {
    elements += 2;
    EncodeString(&config_body, "compression");
    EncodeString(&config_body, g_pika_conf->compression());
  }

  if (slash::stringmatch(pattern.data(), "db-sync-path", 1)) {
    elements += 2;
    EncodeString(&config_body, "db-sync-path");
    EncodeString(&config_body, g_pika_conf->db_sync_path());
  }

  if (slash::stringmatch(pattern.data(), "db-sync-speed", 1)) {
    elements += 2;
    EncodeString(&config_body, "db-sync-speed");
    EncodeInt32(&config_body, g_pika_conf->db_sync_speed());
  }

  if (slash::stringmatch(pattern.data(), "compact-cron", 1)) {
    elements += 2;
    EncodeString(&config_body, "compact-cron");
    EncodeString(&config_body, g_pika_conf->compact_cron());
  }

  if (slash::stringmatch(pattern.data(), "compact-interval", 1)) {
    elements += 2;
    EncodeString(&config_body, "compact-interval");
    EncodeString(&config_body, g_pika_conf->compact_interval());
  }

  if (slash::stringmatch(pattern.data(), "network-interface", 1)) {
    elements += 2;
    EncodeString(&config_body, "network-interface");
    EncodeString(&config_body, g_pika_conf->network_interface());
  }

  if (slash::stringmatch(pattern.data(), "slaveof", 1)) {
    elements += 2;
    EncodeString(&config_body, "slaveof");
    EncodeString(&config_body, g_pika_conf->slaveof());
  }

  if (slash::stringmatch(pattern.data(), "slave-priority", 1)) {
    elements += 2;
    EncodeString(&config_body, "slave-priority");
    EncodeInt32(&config_body, g_pika_conf->slave_priority());
  }

  std::stringstream resp;
  resp << "*" << std::to_string(elements) << "\r\n" << config_body;
  ret = resp.str();
}

void ConfigCmd::ConfigSet(std::string& ret) {
  std::string set_item = config_args_v_[1];
  if (set_item == "*") {
    ret = "*22\r\n";
    EncodeString(&ret, "timeout");
    EncodeString(&ret, "requirepass");
    EncodeString(&ret, "masterauth");
    EncodeString(&ret, "userpass");
    EncodeString(&ret, "userblacklist");
    EncodeString(&ret, "dump-prefix");
    EncodeString(&ret, "maxclients");
    EncodeString(&ret, "dump-expire");
    EncodeString(&ret, "expire-logs-days");
    EncodeString(&ret, "expire-logs-nums");
    EncodeString(&ret, "root-connection-num");
    EncodeString(&ret, "slowlog-write-errorlog");
    EncodeString(&ret, "slowlog-log-slower-than");
    EncodeString(&ret, "slowlog-max-len");
    EncodeString(&ret, "slave-read-only");
    EncodeString(&ret, "write-binlog");
    EncodeString(&ret, "max-cache-statistic-keys");
    EncodeString(&ret, "small-compaction-threshold");
    EncodeString(&ret, "db-sync-speed");
    EncodeString(&ret, "compact-cron");
    EncodeString(&ret, "compact-interval");
    EncodeString(&ret, "slave-priority");
    return;
  }
  long int ival;
  std::string value = config_args_v_[2];
  if (set_item == "timeout") {
    if (!slash::string2l(value.data(), value.size(), &ival)) {
      ret = "-ERR Invalid argument " + value + " for CONFIG SET 'timeout'\r\n";
      return;
    }
    g_pika_conf->SetTimeout(ival);
    ret = "+OK\r\n";
  } else if (set_item == "requirepass") {
    g_pika_conf->SetRequirePass(value);
    ret = "+OK\r\n";
  } else if (set_item == "masterauth") {
    g_pika_conf->SetMasterAuth(value);
    ret = "+OK\r\n";
  } else if (set_item == "userpass") {
    g_pika_conf->SetUserPass(value);
    ret = "+OK\r\n";
  } else if (set_item == "userblacklist") {
    g_pika_conf->SetUserBlackList(value);
    ret = "+OK\r\n";
  } else if (set_item == "dump-prefix") {
    g_pika_conf->SetBgsavePrefix(value);
    ret = "+OK\r\n";
  } else if (set_item == "maxclients") {
    if (!slash::string2l(value.data(), value.size(), &ival) || ival <= 0) {
      ret = "-ERR Invalid argument \'" + value + "\' for CONFIG SET 'maxclients'\r\n";
      return;
    }
    g_pika_conf->SetMaxConnection(ival);
    g_pika_server->SetDispatchQueueLimit(ival);
    ret = "+OK\r\n";
  } else if (set_item == "dump-expire") {
    if (!slash::string2l(value.data(), value.size(), &ival)) {
      ret = "-ERR Invalid argument \'" + value + "\' for CONFIG SET 'dump-expire'\r\n";
      return;
    }
    g_pika_conf->SetExpireDumpDays(ival);
    ret = "+OK\r\n";
  } else if (set_item == "slave-priority") {
     if (!slash::string2l(value.data(), value.size(), &ival)) {
      ret = "-ERR Invalid argument \'" + value + "\' for CONFIG SET 'slave-priority'\r\n";
      return;
    }
    g_pika_conf->SetSlavePriority(ival);
    ret = "+OK\r\n";
  } else if (set_item == "expire-logs-days") {
    if (!slash::string2l(value.data(), value.size(), &ival) || ival <= 0) {
      ret = "-ERR Invalid argument \'" + value + "\' for CONFIG SET 'expire-logs-days'\r\n";
      return;
    }
    g_pika_conf->SetExpireLogsDays(ival);
    ret = "+OK\r\n";
  } else if (set_item == "expire-logs-nums") {
    if (!slash::string2l(value.data(), value.size(), &ival) || ival <= 0) {
      ret = "-ERR Invalid argument \'" + value + "\' for CONFIG SET 'expire-logs-nums'\r\n";
      return;
    }
    g_pika_conf->SetExpireLogsNums(ival);
    ret = "+OK\r\n";
  } else if (set_item == "root-connection-num") {
    if (!slash::string2l(value.data(), value.size(), &ival) || ival <= 0) {
      ret = "-ERR Invalid argument \'" + value + "\' for CONFIG SET 'root-connection-num'\r\n";
      return;
    }
    g_pika_conf->SetRootConnectionNum(ival);
    ret = "+OK\r\n";
  } else if (set_item == "slowlog-write-errorlog") {
    bool is_write_errorlog;
    if (value == "yes") {
      is_write_errorlog = true;
    } else if (value == "no") {
      is_write_errorlog = false;
    } else {
      ret = "-ERR Invalid argument \'" + value + "\' for CONFIG SET 'slowlog-write-errorlog'\r\n";
      return;
    }
    g_pika_conf->SetSlowlogWriteErrorlog(is_write_errorlog);
    ret = "+OK\r\n";
  } else if (set_item == "slowlog-log-slower-than") {
    if (!slash::string2l(value.data(), value.size(), &ival) || ival < 0) {
      ret = "-ERR Invalid argument \'" + value + "\' for CONFIG SET 'slowlog-log-slower-than'\r\n";
      return;
    }
    g_pika_conf->SetSlowlogSlowerThan(ival);
    ret = "+OK\r\n";
  } else if (set_item == "slowlog-max-len") {
    if (!slash::string2l(value.data(), value.size(), &ival) || ival < 0) {
      ret = "-ERR Invalid argument \'" + value + "\' for CONFIG SET 'slowlog-max-len'\r\n";
      return;
    }
    g_pika_conf->SetSlowlogMaxLen(ival);
    g_pika_server->SlowlogTrim();
    ret = "+OK\r\n";
  } else if (set_item == "slave-read-only") {
    slash::StringToLower(value);
    bool is_readonly;
    if (value == "1" || value == "yes") {
      is_readonly = true;
    } else if (value == "0" || value == "no") {
      is_readonly = false;
    } else {
      ret = "-ERR Invalid argument \'" + value + "\' for CONFIG SET 'slave-read-only'\r\n";
      return;
    }
    g_pika_conf->SetSlaveReadOnly(is_readonly);
    ret = "+OK\r\n";
  } else if (set_item == "max-cache-statistic-keys") {
    if (!slash::string2l(value.data(), value.size(), &ival) || ival < 0) {
      ret = "-ERR Invalid argument \'" + value + "\' for CONFIG SET 'max-cache-statistic-keys'\r\n";
      return;
    }
    g_pika_conf->SetMaxCacheStatisticKeys(ival);
    g_pika_server->PartitionSetMaxCacheStatisticKeys(ival);
    ret = "+OK\r\n";
  } else if (set_item == "small-compaction-threshold") {
    if (!slash::string2l(value.data(), value.size(), &ival) || ival < 0) {
      ret = "-ERR Invalid argument \'" + value + "\' for CONFIG SET 'small-compaction-threshold'\r\n";
      return;
    }
    g_pika_conf->SetSmallCompactionThreshold(ival);
    g_pika_server->PartitionSetSmallCompactionThreshold(ival);
    ret = "+OK\r\n";
  } else if (set_item == "write-binlog") {
    int role = g_pika_server->role();
    if (role == PIKA_ROLE_SLAVE) {
      ret = "-ERR need to close master-slave mode first\r\n";
      return;
    } else if (value != "yes" && value != "no") {
      ret = "-ERR invalid write-binlog (yes or no)\r\n";
      return;
    } else {
      g_pika_conf->SetWriteBinlog(value);
      ret = "+OK\r\n";
    }
  } else if (set_item == "db-sync-speed") {
    if (!slash::string2l(value.data(), value.size(), &ival)) {
      ret = "-ERR Invalid argument \'" + value + "\' for CONFIG SET 'db-sync-speed(MB)'\r\n";
      return;
    }
    if (ival < 0 || ival > 1024) {
      ival = 1024;
    }
    g_pika_conf->SetDbSyncSpeed(ival);
    ret = "+OK\r\n";
  } else if (set_item == "compact-cron") {
    bool invalid = false;
    if (value != "") {
      bool have_week = false;
      std::string compact_cron, week_str;
      int slash_num = count(value.begin(), value.end(), '/');
      if (slash_num == 2) {
        have_week = true;
        std::string::size_type first_slash = value.find("/");
        week_str = value.substr(0, first_slash);
        compact_cron = value.substr(first_slash + 1);
      } else {
        compact_cron = value;
      }

      std::string::size_type len = compact_cron.length();
      std::string::size_type colon = compact_cron.find("-");
      std::string::size_type underline = compact_cron.find("/");
      if (colon == std::string::npos || underline == std::string::npos ||
          colon >= underline || colon + 1 >= len ||
          colon + 1 == underline || underline + 1 >= len) {
          invalid = true;
      } else {
        int week = std::atoi(week_str.c_str());
        int start = std::atoi(compact_cron.substr(0, colon).c_str());
        int end = std::atoi(compact_cron.substr(colon + 1, underline).c_str());
        int usage = std::atoi(compact_cron.substr(underline + 1).c_str());
        if ((have_week && (week < 1 || week > 7)) || start < 0 || start > 23 || end < 0 || end > 23 || usage < 0 || usage > 100) {
          invalid = true;
        }
      }
    }
    if (invalid) {
      ret = "-ERR invalid compact-cron\r\n";
      return;
    } else {
      g_pika_conf->SetCompactCron(value);
      ret = "+OK\r\n";
    }
  } else if (set_item == "compact-interval") {
    bool invalid = false;
    if (value != "") {
      std::string::size_type len = value.length();
      std::string::size_type slash = value.find("/");
      if (slash == std::string::npos || slash + 1 >= len) {
        invalid = true;
      } else {
        int interval = std::atoi(value.substr(0, slash).c_str());
        int usage = std::atoi(value.substr(slash+1).c_str());
        if (interval <= 0 || usage < 0 || usage > 100) {
          invalid = true;
        }
      }
    }
    if (invalid) {
      ret = "-ERR invalid compact-interval\r\n";
      return;
    } else {
      g_pika_conf->SetCompactInterval(value);
      ret = "+OK\r\n";
    }
  } else {
    ret = "-ERR Unsupported CONFIG parameter: " + set_item + "\r\n";
  }
}

void ConfigCmd::ConfigRewrite(std::string &ret) {
  g_pika_conf->ConfigRewrite();
  ret = "+OK\r\n";
}

void ConfigCmd::ConfigResetstat(std::string &ret) {
  g_pika_server->ResetStat();
  ret = "+OK\r\n";
}

void MonitorCmd::DoInitial() {
  if (argv_.size() != 1) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameMonitor);
    return;
  }
}

void MonitorCmd::Do(std::shared_ptr<Partition> partition) {
}

void DbsizeCmd::DoInitial() {
  if (argv_.size() != 1) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameDbsize);
    return;
  }
}

void DbsizeCmd::Do(std::shared_ptr<Partition> partition) {
  std::shared_ptr<Table> table = g_pika_server->GetTable(table_name_);
  if (!table) {
    res_.SetRes(CmdRes::kInvalidTable);
  } else {
    KeyScanInfo key_scan_info = table->GetKeyScanInfo();
    std::vector<blackwidow::KeyInfo> key_infos = key_scan_info.key_infos;
    if (key_infos.size() != 5) {
      res_.SetRes(CmdRes::kErrOther, "keyspace error");
      return;
    }
    int64_t dbsize = key_infos[0].keys
        + key_infos[1].keys
        + key_infos[2].keys
        + key_infos[3].keys
        + key_infos[4].keys;
    res_.AppendInteger(dbsize);
  }
}

void TimeCmd::DoInitial() {
  if (argv_.size() != 1) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameTime);
    return;
  }
}

void TimeCmd::Do(std::shared_ptr<Partition> partition) {
  struct timeval tv;
  if (gettimeofday(&tv, NULL) == 0) {
    res_.AppendArrayLen(2);
    char buf[32];
    int32_t len = slash::ll2string(buf, sizeof(buf), tv.tv_sec);
    res_.AppendStringLen(len);
    res_.AppendContent(buf);

    len = slash::ll2string(buf, sizeof(buf), tv.tv_usec);
    res_.AppendStringLen(len);
    res_.AppendContent(buf);
  } else {
    res_.SetRes(CmdRes::kErrOther, strerror(errno));
  }
}

void DelbackupCmd::DoInitial() {
  if (argv_.size() != 1) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameDelbackup);
    return;
  }
}

void DelbackupCmd::Do(std::shared_ptr<Partition> partition) {
  std::string db_sync_prefix = g_pika_conf->bgsave_prefix();
  std::string db_sync_path = g_pika_conf->bgsave_path();
  std::vector<std::string> dump_dir;

  // Dump file is not exist
  if (!slash::FileExists(db_sync_path)) {
    res_.SetRes(CmdRes::kOk);
    return;
  }
  // Directory traversal
  if (slash::GetChildren(db_sync_path, dump_dir) != 0) {
    res_.SetRes(CmdRes::kOk);
    return;
  }

  int len = dump_dir.size();
  for (size_t i = 0; i < dump_dir.size(); i++) {
    if (dump_dir[i].substr(0, db_sync_prefix.size()) != db_sync_prefix || dump_dir[i].size() != (db_sync_prefix.size() + 8)) {
      continue;
    }

    std::string str_date = dump_dir[i].substr(db_sync_prefix.size(), (dump_dir[i].size() - db_sync_prefix.size()));
    char *end = NULL;
    std::strtol(str_date.c_str(), &end, 10);
    if (*end != 0) {
      continue;
    }

    std::string dump_dir_name = db_sync_path + dump_dir[i] + "/" + table_name_;
    if (g_pika_server->CountSyncSlaves() == 0) {
      LOG(INFO) << "Not syncing, delete dump file: " << dump_dir_name;
      slash::DeleteDirIfExist(dump_dir_name);
      len--;
    } else {
      LOG(INFO) << "Syncing, can not delete " << dump_dir_name << " dump file" << std::endl;
    }
  }
  res_.SetRes(CmdRes::kOk);
  return;
}

void EchoCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameEcho);
    return;
  }
  body_ = argv_[1];
  return;
}

void EchoCmd::Do(std::shared_ptr<Partition> partition) {
  res_.AppendString(body_);
  return;
}

void ScandbCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameEcho);
    return;
  }
  if (argv_.size() == 1) {
    type_ = blackwidow::kAll;
  } else {
    if (!strcasecmp(argv_[1].data(),"string")) {
      type_ = blackwidow::kStrings;
    } else if (!strcasecmp(argv_[1].data(), "hash")) {
      type_ = blackwidow::kHashes;
    } else if (!strcasecmp(argv_[1].data(), "set")) {
      type_ = blackwidow::kSets;
    } else if (!strcasecmp(argv_[1].data(), "zset")) {
      type_ = blackwidow::kZSets;
    } else if (!strcasecmp(argv_[1].data(), "list")) {
      type_ = blackwidow::kLists;
    } else {
      res_.SetRes(CmdRes::kInvalidDbType);
    }
  }
  return;
}

void ScandbCmd::Do(std::shared_ptr<Partition> partition) {
  std::shared_ptr<Table> table = g_pika_server->GetTable(table_name_);
  if (!table) {
    res_.SetRes(CmdRes::kInvalidTable);
  } else {
    table->ScanDatabase(type_);
    res_.SetRes(CmdRes::kOk);
  }
  return;
}

void SlowlogCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlowlog);
    return;
  }
  if (argv_.size() == 2 && !strcasecmp(argv_[1].data(), "reset")) {
    condition_ = SlowlogCmd::kRESET;
  } else if (argv_.size() == 2 && !strcasecmp(argv_[1].data(), "len")) {
    condition_ = SlowlogCmd::kLEN;
  } else if ((argv_.size() == 2 || argv_.size() == 3) && !strcasecmp(argv_[1].data(), "get")) {
    condition_ = SlowlogCmd::kGET;
    if (argv_.size() == 3 && !slash::string2l(argv_[2].data(), argv_[2].size(), &number_)) {
      res_.SetRes(CmdRes::kInvalidInt);
      return;
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, "Unknown SLOWLOG subcommand or wrong # of args. Try GET, RESET, LEN.");
    return;
  }
}

void SlowlogCmd::Do(std::shared_ptr<Partition> partition) {
  if (condition_ == SlowlogCmd::kRESET) {
    g_pika_server->SlowlogReset();
    res_.SetRes(CmdRes::kOk);
  } else if (condition_ ==  SlowlogCmd::kLEN) {
    res_.AppendInteger(g_pika_server->SlowlogLen());
  } else {
    std::vector<SlowlogEntry> slowlogs;
    g_pika_server->SlowlogObtain(number_, &slowlogs);
    res_.AppendArrayLen(slowlogs.size());
    for (const auto& slowlog : slowlogs) {
      res_.AppendArrayLen(4);
      res_.AppendInteger(slowlog.id);
      res_.AppendInteger(slowlog.start_time);
      res_.AppendInteger(slowlog.duration);
      res_.AppendArrayLen(slowlog.argv.size());
      for (const auto& arg : slowlog.argv) {
        res_.AppendString(arg);
      }
    }
  }
  return;
}

#ifdef TCMALLOC_EXTENSION
void TcmallocCmd::DoInitial() {
  if (argv_.size() != 2 && argv_.size() != 3) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameTcmalloc);
    return;
  }
  rate_ = 0;
  std::string type = argv_[1];
  if (!strcasecmp(type.data(), "stats")) {
    type_ = 0;
  } else if (!strcasecmp(type.data(), "rate")) {
    type_ = 1;
    if (argv_.size() == 3) {
      if (!slash::string2l(argv_[2].data(), argv_[2].size(), &rate_)) {
        res_.SetRes(CmdRes::kSyntaxErr, kCmdNameTcmalloc);
      }
    }
  } else if (!strcasecmp(type.data(), "list")) {
    type_ = 2;
  } else if (!strcasecmp(type.data(), "free")) {
    type_ = 3;
  } else {
    res_.SetRes(CmdRes::kInvalidParameter, kCmdNameTcmalloc);
    return;
  }
}

void TcmallocCmd::Do(std::shared_ptr<Partition> partition) {
  std::vector<MallocExtension::FreeListInfo> fli;
  std::vector<std::string> elems;
  switch(type_) {
    case 0:
      char stats[1024];
      MallocExtension::instance()->GetStats(stats, 1024);
      slash::StringSplit(stats, '\n', elems);
      res_.AppendArrayLen(elems.size());
      for (auto& i : elems) {
        res_.AppendString(i);
      }
      break;
    case 1:
      if (rate_) {
        MallocExtension::instance()->SetMemoryReleaseRate(rate_);
      }
      res_.AppendInteger(MallocExtension::instance()->GetMemoryReleaseRate());
      break;
    case 2:
      MallocExtension::instance()->GetFreeListSizes(&fli);
      res_.AppendArrayLen(fli.size());
      for (auto& i : fli) {
        res_.AppendString("type: " + std::string(i.type) + ", min: " + std::to_string(i.min_object_size) +
          ", max: " + std::to_string(i.max_object_size) + ", total: " + std::to_string(i.total_bytes_free));
      }
      break;
    case 3:
      MallocExtension::instance()->ReleaseFreeMemory();
      res_.SetRes(CmdRes::kOk);
  }
}
#endif
