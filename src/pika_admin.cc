// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "slash_string.h"
#include "rsync.h"
#include "pika_conf.h"
#include "pika_admin.h"
#include "pika_server.h"

#include <sys/utsname.h>

extern PikaServer *g_pika_server;
extern PikaConf *g_pika_conf;

void SlaveofCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlaveof);
    return;
  }
  PikaCmdArgsType::iterator it = argv.begin() + 1; //Remember the first args is the opt name

  master_ip_ = slash::StringToLower(*it++);

  is_noone_ = false;
  if (master_ip_ == "no" && slash::StringToLower(*it) == "one") {
    if (argv.end() - it == 1) {
      is_noone_ = true;
    } else {
      res_.SetRes(CmdRes::kWrongNum, kCmdNameSlaveof);
    }
    return;
  }

  std::string str_master_port = *it++;
  if (!slash::string2l(str_master_port.data(), str_master_port.size(), &master_port_) || master_port_ <= 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }

  if ((master_ip_ == "127.0.0.1" || master_ip_ == g_pika_server->host()) && master_port_ == g_pika_server->port()) {
    res_.SetRes(CmdRes::kErrOther, "you fucked up");
    return;
  }

  have_offset_ = false;
  int cur_size = argv.end() - it;
  if (cur_size == 0) {

  } else if (cur_size == 2) {
    have_offset_ = true;
    std::string str_filenum = *it++;
    if (!slash::string2l(str_filenum.data(), str_filenum.size(), &filenum_) || filenum_ < 0) {
      res_.SetRes(CmdRes::kInvalidInt);
      return;
    }
    std::string str_pro_offset = *it++;
    if (!slash::string2l(str_pro_offset.data(), str_pro_offset.size(), &pro_offset_) || pro_offset_ < 0) {
      res_.SetRes(CmdRes::kInvalidInt);
      return;
    }
  } else {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlaveof);
  }
}

void SlaveofCmd::Do() {
  if (is_noone_) {
    // Stop rsync
    LOG(INFO) << "start slaveof, stop rsync first";
    slash::StopRsync(g_pika_conf->db_sync_path());
    
    g_pika_server->RemoveMaster();
    res_.SetRes(CmdRes::kOk);
    return;
  }
  if (have_offset_) {
    // Before we send the trysync command, we need purge current logs older than the sync point
    if (filenum_ > 0) {
      g_pika_server->PurgeLogs(filenum_ - 1, true, true);
    }
    g_pika_server->logger_->SetProducerStatus(filenum_, pro_offset_);
  }
  bool sm_ret = g_pika_server->SetMaster(master_ip_, master_port_);
  if (sm_ret) {
    res_.SetRes(CmdRes::kOk);
  } else {
    res_.SetRes(CmdRes::kErrOther, "Server is not in correct state for slaveof");
  }
}

void TrysyncCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameTrysync);
    return;
  }
  PikaCmdArgsType::iterator it = argv.begin() + 1; //Remember the first args is the opt name
  slave_ip_ = *it++;

  std::string str_slave_port = *it++;
  if (!slash::string2l(str_slave_port.data(), str_slave_port.size(), &slave_port_) || slave_port_ <= 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }

  std::string str_filenum = *it++;
  if (!slash::string2l(str_filenum.data(), str_filenum.size(), &filenum_) || filenum_ < 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }

  std::string str_pro_offset = *it++;
  if (!slash::string2l(str_pro_offset.data(), str_pro_offset.size(), &pro_offset_) || pro_offset_ < 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }

}

void TrysyncCmd::Do() {
  std::string ip_port = slash::IpPortString(slave_ip_, slave_port_);
  LOG(INFO) << "Trysync, Slave ip_port: " << ip_port << " filenum: " << filenum_ << " pro_offset: " << pro_offset_;
  slash::MutexLock l(&(g_pika_server->slave_mutex_));
  if (!g_pika_server->FindSlave(ip_port)) {
    SlaveItem s;
    s.sid = g_pika_server->GenSid();
    s.ip_port = ip_port;
    s.port = slave_port_;
    s.hb_fd = -1;
    s.stage = SLAVE_ITEM_STAGE_ONE;
    gettimeofday(&s.create_time, NULL);
    s.sender = NULL;
    
    LOG(INFO) << "Trysync, dont FindSlave, so AddBinlogSender";
    Status status = g_pika_server->AddBinlogSender(s, filenum_, pro_offset_);
    if (status.ok()) {
      res_.AppendInteger(s.sid);
      LOG(INFO) << "Send Sid to Slave: " << s.sid;
      g_pika_server->BecomeMaster();
    } else if (status.IsIncomplete()) {
      res_.AppendString(kInnerReplWait);
    } else {
      LOG(WARNING) << "slave offset is larger than mine, slave ip: " << ip_port << " filenum: " << filenum_ << " pro_offset_: " << pro_offset_;
      res_.SetRes(CmdRes::kErrOther, "InvalidOffset");
    }
  } else {
    LOG(WARNING) << "slave already exist, slave ip: " << ip_port;
    res_.SetRes(CmdRes::kErrOther, "AlreadyExist");
  }
}

void AuthCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameAuth);
    return;
  }
  pwd_ = argv[1];
}

void AuthCmd::Do() {
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

void BgsaveCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameBgsave);
    return;
  }
}
void BgsaveCmd::Do() {
  g_pika_server->Bgsave();
  const PikaServer::BGSaveInfo& info = g_pika_server->bgsave_info();
  char buf[256];
  snprintf(buf, sizeof(buf), "+%s : %u: %lu", 
      info.s_start_time.c_str(), info.filenum, info.offset);
  res_.AppendContent(buf);
}

void BgsaveoffCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameBgsaveoff);
    return;
  }
}
void BgsaveoffCmd::Do() {
  CmdRes::CmdRet ret;
  if (g_pika_server->Bgsaveoff()) {
   ret = CmdRes::kOk;
  } else {
   ret = CmdRes::kNoneBgsave;
  }
  res_.SetRes(ret);
}

void CompactCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameCompact);
    return;
  }
}

void CompactCmd::Do() {
  nemo::Status s = g_pika_server->db()->Compact(nemo::kALL);
  if (s.ok()) {
    res_.SetRes(CmdRes::kOk);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void PurgelogstoCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePurgelogsto);
    return;
  }
  std::string filename = slash::StringToLower(argv[1]);
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
}
void PurgelogstoCmd::Do() {
  if (g_pika_server->PurgeLogs(num_, true, false)) {
    res_.SetRes(CmdRes::kOk);
  } else {
    res_.SetRes(CmdRes::kPurgeExist);
  }
}

void PingCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePing);
    return;
  }
}
void PingCmd::Do() {
  res_.SetRes(CmdRes::kPong);
}

void SelectCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSelect);
    return;
  }

  int64_t db_id;
  if (!slash::string2l(argv[1].data(), argv[1].size(), &db_id)) {
    res_.SetRes(CmdRes::kInvalidInt);
  }
}
void SelectCmd::Do() {
  res_.SetRes(CmdRes::kOk);
}

void FlushallCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameFlushall);
    return;
  }
}
void FlushallCmd::Do() {
  g_pika_server->RWLockWriter();
  if (g_pika_server->FlushAll()) {
    res_.SetRes(CmdRes::kOk);
  } else {
    res_.SetRes(CmdRes::kErrOther, "There are some bgthread using db now, can not flushall");
  }
  g_pika_server->RWUnlock();
}

void ReadonlyCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameReadonly);
    return;
  }
  std::string opt = slash::StringToLower(argv[1]);
  if (opt == "on" || opt == "1") {
    is_open_ = true;
  } else if (opt == "off" || opt == "0") {
    is_open_ = false;
  } else {
    res_.SetRes(CmdRes::kSyntaxErr, kCmdNameReadonly);
    return;
  }
}
void ReadonlyCmd::Do() {
  g_pika_server->RWLockWriter();
  if (is_open_) {
    g_pika_conf->SetReadonly(true);
  } else {
    g_pika_conf->SetReadonly(false);
  }
  res_.SetRes(CmdRes::kOk);
  g_pika_server->RWUnlock();
}

const std::string ClientCmd::CLIENT_LIST_S = "list";
const std::string ClientCmd::CLIENT_KILL_S = "kill";
void ClientCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameClient);
    return;
  }
  slash::StringToLower(argv[1]);
  if (argv[1] == CLIENT_LIST_S && argv.size() == 2) {
    //nothing
  } else if (argv[1] == CLIENT_KILL_S && argv.size() == 3) {
    ip_port_ = slash::StringToLower(argv[2]);
  } else {
    res_.SetRes(CmdRes::kErrOther, "Syntax error, try CLIENT (LIST | KILL ip:port)");
    return;
  }
  operation_ = argv[1];
  return;
}

void ClientCmd::Do() {
  if (operation_ == CLIENT_LIST_S) {
    struct timeval now;
    gettimeofday(&now, NULL);
    std::vector<ClientInfo> clients;
    g_pika_server->ClientList(&clients);
    std::vector<ClientInfo>::iterator iter= clients.begin();
    std::string reply = "+";
    char buf[128];
    while (iter != clients.end()) {
      snprintf(buf, sizeof(buf), "addr=%s fd=%d idle=%ld\n", iter->ip_port.c_str(), iter->fd, iter->last_interaction == 0 ? 0 : now.tv_sec - iter->last_interaction);
      reply.append(buf);
      iter++;
    }
    res_.AppendContent(reply);
  } else if (operation_ == CLIENT_KILL_S && ip_port_ == "all") {
    g_pika_server->ClientKillAll();
    res_.SetRes(CmdRes::kOk);
  } else if (g_pika_server->ClientKill(ip_port_) == 1) {
    res_.SetRes(CmdRes::kOk);
  } else {
    res_.SetRes(CmdRes::kErrOther, "No such client");
  }
  return;
}

void ShutdownCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameShutdown);
    return;
  }
}
// no return
void ShutdownCmd::Do() {
  DLOG(WARNING) << "handle \'shutdown\'";
  g_pika_server->Exit();
  res_.SetRes(CmdRes::kNone);
}

const std::string InfoCmd::kAllSection = "all";
const std::string InfoCmd::kServerSection = "server";
const std::string InfoCmd::kClientsSection = "clients";
const std::string InfoCmd::kStatsSection = "stats";
const std::string InfoCmd::kReplicationSection = "replication";
const std::string InfoCmd::kKeyspaceSection = "keyspace";
const std::string InfoCmd::kLogSection = "log";
const std::string InfoCmd::kDataSection = "data";

void InfoCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  (void)ptr_info;
  size_t argc = argv.size();
  if (argc > 3) {
    res_.SetRes(CmdRes::kSyntaxErr);
    return;
  }
  if (argc == 1) {
    info_section_ = kInfoAll;
    return;
  } //then the agc is 2 or 3
  slash::StringToLower(argv[1]);
  if (argv[1] == kAllSection) {
    info_section_ = kInfoAll;
  } else if (argv[1] == kServerSection) {
    info_section_ = kInfoServer;
  } else if (argv[1] == kClientsSection) {
    info_section_ = kInfoClients;
  } else if (argv[1] == kStatsSection) {
    info_section_ = kInfoStats;
  } else if (argv[1] == kReplicationSection) {
    info_section_ = kInfoReplication;
  } else if (argv[1] == kKeyspaceSection) {
    info_section_ = kInfoKeyspace;
    if (argc == 2) {
      return;
    }
    if (argv[2] == "1") { //info keyspace [ 0 | 1 | off ]
      rescan_ = true;
    } else if (argv[2] == "off") {
      off_ = true;
    } else if (argv[2] != "0") {
      res_.SetRes(CmdRes::kSyntaxErr);
    }
    return;
  } else if (argv[1] == kLogSection) {
    info_section_ = kInfoLog;
  } else if (argv[1] == kDataSection) {
    info_section_ = kInfoData;
  }else {
    info_section_ = kInfoErr;
  }
  if (argc != 2) {
    res_.SetRes(CmdRes::kSyntaxErr);
  }
}

void InfoCmd::Do() {
  std::string info;
  switch (info_section_) {
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
    case kInfoReplication:
      InfoReplication(info);
      break;
    case kInfoKeyspace:
      InfoKeyspace(info);
      // off_ should return +OK
      if (off_) {
        res_.SetRes(CmdRes::kOk);
      }
      break;
    case kInfoLog:
      InfoLog(info);
      break;
    case kInfoData:
      InfoData(info);
      break;
    default:
      //kInfoErr is nothing 
      break;
  }


  res_.AppendStringLen(info.size());
  res_.AppendContent(info);
  return;
}

void InfoCmd::InfoServer(std::string &info) {
  static struct utsname host_info;
  static bool host_info_valid = false;
  if (!host_info_valid) {
    uname(&host_info);
    host_info_valid = true;
  }

  time_t current_time_s = time(NULL);
  std::stringstream tmp_stream;
  tmp_stream << "# Server\r\n";
  tmp_stream << "pika_version:" << kPikaVersion << "\r\n";
  tmp_stream << "os:" << host_info.sysname << " " << host_info.release << " " << host_info.machine << "\r\n";
  tmp_stream << "arch_bits:" << (reinterpret_cast<char*>(&host_info.machine) + strlen(host_info.machine) - 2) << "\r\n";
  tmp_stream << "process_id:" << getpid() << "\r\n";
  tmp_stream << "tcp_port:" << g_pika_conf->port() << "\r\n";
  tmp_stream << "thread_num:" << g_pika_conf->thread_num() << "\r\n";
  tmp_stream << "sync_thread_num:" << g_pika_conf->sync_thread_num() << "\r\n";
  tmp_stream << "uptime_in_seconds:" << (current_time_s - g_pika_server->start_time_s()) << "\r\n";
  tmp_stream << "uptime_in_days:" << (current_time_s / (24*3600) - g_pika_server->start_time_s() / (24*3600) + 1) << "\r\n";
  tmp_stream << "config_file:" << g_pika_conf->conf_path() << "\r\n";
  
  info.append(tmp_stream.str());
}

void InfoCmd::InfoClients(std::string &info) {
  std::stringstream tmp_stream;
  tmp_stream << "# Clients\r\n";
  tmp_stream << "connected_clients:" << g_pika_server->ClientList() << "\r\n";

  info.append(tmp_stream.str());
}

void InfoCmd::InfoStats(std::string &info) {
  std::stringstream tmp_stream;
  tmp_stream << "# Stats\r\n";

  tmp_stream << "total_connections_received:" << g_pika_server->accumulative_connections() << "\r\n";
  tmp_stream << "instantaneous_ops_per_sec:" << g_pika_server->ServerCurrentQps() << "\r\n";
  tmp_stream << "total_commands_processed:" << g_pika_server->ServerQueryNum() << "\r\n";
  PikaServer::BGSaveInfo bgsave_info = g_pika_server->bgsave_info();
  bool is_bgsaving = g_pika_server->bgsaving();
  time_t current_time_s = time(NULL);
  tmp_stream << "is_bgsaving:" << (is_bgsaving ? "Yes, " : "No, ") << bgsave_info.s_start_time << ", "
                                << (is_bgsaving ? (current_time_s - bgsave_info.start_time) : 0) << "\r\n";
  PikaServer::KeyScanInfo key_scan_info = g_pika_server->key_scan_info();
  bool is_scaning = g_pika_server->key_scaning();
  tmp_stream << "is_scaning_keyspace:" << (is_scaning ? ("Yes, " + key_scan_info.s_start_time) + "," : "No");
  if (is_scaning) {
    tmp_stream << current_time_s - key_scan_info.start_time;
  }
  tmp_stream << "\r\n"; 
  tmp_stream << "is_compact:" << g_pika_server->db()->GetCurrentTaskType() << "\r\n";

  info.append(tmp_stream.str());
}

void InfoCmd::InfoReplication(std::string &info) {
  int host_role = g_pika_server->role();
  std::stringstream tmp_stream;
  tmp_stream << "# Replication(";
  switch (host_role) {
    case PIKA_ROLE_SINGLE :
    case PIKA_ROLE_MASTER : tmp_stream << "MASTER)\r\nrole:master\r\n"; break;
    case PIKA_ROLE_SLAVE : tmp_stream << "SLAVE)\r\nrole:slave\r\n"; break;
    case PIKA_ROLE_MASTER | PIKA_ROLE_SLAVE : tmp_stream << "MASTER/SLAVE)\r\nrole:slave\r\n"; break;
    default: info.append("ERR: server role is error\r\n"); return;
  }
  
  std::string slaves_list_str;
  //int32_t slaves_num = g_pika_server->GetSlaveListString(slaves_list_str);
  switch (host_role) {
    case PIKA_ROLE_SLAVE :
      tmp_stream << "master_host:" << g_pika_server->master_ip() << "\r\n";
      tmp_stream << "master_port:" << g_pika_server->master_port() << "\r\n";
      tmp_stream << "master_link_status:" << (g_pika_server->repl_state() == PIKA_REPL_CONNECTED ? "up" : "down") << "\r\n";
      tmp_stream << "slave_read_only:" << g_pika_conf->readonly() << "\r\n";
      tmp_stream << "repl_state: " << (g_pika_server->repl_state()) << "\r\n";
      break;
    case PIKA_ROLE_MASTER | PIKA_ROLE_SLAVE :
      tmp_stream << "master_host:" << g_pika_server->master_ip() << "\r\n";
      tmp_stream << "master_port:" << g_pika_server->master_port() << "\r\n";
      tmp_stream << "master_link_status:" << (g_pika_server->repl_state() == PIKA_REPL_CONNECTED ? "up" : "down") << "\r\n";
      tmp_stream << "slave_read_only:" << g_pika_conf->readonly() << "\r\n";
      tmp_stream << "repl_state: " << (g_pika_server->repl_state()) << "\r\n";
    case PIKA_ROLE_SINGLE :
    case PIKA_ROLE_MASTER :
      tmp_stream << "connected_slaves:" << g_pika_server->GetSlaveListString(slaves_list_str) << "\r\n" << slaves_list_str;
  }
  
  info.append(tmp_stream.str());
}

void InfoCmd::InfoKeyspace(std::string &info) {
  if (off_) {
    g_pika_server->StopKeyScan();
    off_ = false;
    return;
  }

  PikaServer::KeyScanInfo key_scan_info = g_pika_server->key_scan_info();
  std::vector<uint64_t> &key_nums_v = key_scan_info.key_nums_v;
  if (key_scan_info.key_nums_v.size() != 5) {
    info.append("info keyspace error\r\n");
    return;
  }
  std::stringstream tmp_stream;
  tmp_stream << "# Keyspace\r\n";
  tmp_stream << "# Time:" << key_scan_info.s_start_time << "\r\n";
  tmp_stream << "kv keys:" << key_nums_v[0] << "\r\n";
  tmp_stream << "hash keys:" << key_nums_v[1] << "\r\n";
  tmp_stream << "list keys:" << key_nums_v[2] << "\r\n";
  tmp_stream << "zset keys:" << key_nums_v[3] << "\r\n";
  tmp_stream << "set keys:" << key_nums_v[4] << "\r\n";
  info.append(tmp_stream.str());

  if (rescan_) {
    g_pika_server->KeyScan();
  }
  return;
}

void InfoCmd::InfoLog(std::string &info) {
  std::stringstream  tmp_stream;
  tmp_stream << "# Log" << "\r\n";
  uint32_t purge_max;
  int64_t log_size = slash::Du(g_pika_conf->log_path());
  tmp_stream << "log_size:" << log_size << "\r\n";
  tmp_stream << "log_size_human:" << (log_size >> 20) << "M\r\n";
  tmp_stream << "safety_purge:" << (g_pika_server->GetPurgeWindow(purge_max) ?
      kBinlogPrefix + std::to_string(static_cast<int32_t>(purge_max)) : "none") << "\r\n"; 
  tmp_stream << "expire_logs_days:" << g_pika_conf->expire_logs_days() << "\r\n";
  tmp_stream << "expire_logs_nums:" << g_pika_conf->expire_logs_nums() << "\r\n";
  uint32_t filenum;
  uint64_t offset;
  g_pika_server->logger_->GetProducerStatus(&filenum, &offset);
  tmp_stream << "binlog_offset:" << filenum << " " << offset << "\r\n"; 

  info.append(tmp_stream.str());
  return;
}

void InfoCmd::InfoData(std::string &info) {
  std::stringstream tmp_stream;
  
  int64_t db_size = slash::Du(g_pika_conf->db_path());
  tmp_stream << "# Data" << "\r\n"; 
  tmp_stream << "db_size:" << db_size << "\r\n";
  tmp_stream << "db_size_human:" << (db_size >> 20) << "M\r\n";
  tmp_stream << "compression:" << g_pika_conf->compression() << "\r\n";

  // rocksdb related memory usage
  uint64_t memtable_usage, table_reader_usage;
  g_pika_server->db()->GetUsage(nemo::USAGE_TYPE_ROCKSDB_MEMTABLE, &memtable_usage);
  g_pika_server->db()->GetUsage(nemo::USAGE_TYPE_ROCKSDB_TABLE_READER, &table_reader_usage);

  tmp_stream << "used_memory:" << (memtable_usage + table_reader_usage) << "\r\n";
  tmp_stream << "used_memory_human:" << ((memtable_usage + table_reader_usage) >> 20) << "M\r\n";
  tmp_stream << "db_memtable_usage:" << memtable_usage << "\r\n";
  tmp_stream << "db_tablereader_usage:" << table_reader_usage << "\r\n";

  info.append(tmp_stream.str());
  return;
}

void ConfigCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameConfig);
    return;
  }
  size_t argc = argv.size();
  slash::StringToLower(argv[1]);
  if (argv[1] == "get") {
    if (argc != 3) {
      res_.SetRes(CmdRes::kErrOther, "Wrong number of arguments for CONFIG get");
      return;
    }
  } else if (argv[1] == "set") {
    if (argc == 3 && argv[2] != "*") {
      res_.SetRes(CmdRes::kErrOther, "Wrong number of arguments for CONFIG set");
      return;
    } else if (argc != 4 && argc != 3) {
      res_.SetRes(CmdRes::kErrOther, "Wrong number of arguments for CONFIG set");
      return;
    }
  } else if (argv[1] == "rewrite") {
    if (argc != 2) {
      res_.SetRes(CmdRes::kErrOther, "Wrong number of arguments for CONFIG rewrite");
      return;
    }
  } else if (argv[1] == "resetstat") {
    if (argc != 2) {
      res_.SetRes(CmdRes::kErrOther, "Wrong number of arguments for CONFIG resetstat");
      return;
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, "CONFIG subcommand must be one of GET, SET, RESETSTAT, REWRITE");
    return;
  }
  config_args_v_.assign(argv.begin()+1, argv.end()); 
  return;
}

void ConfigCmd::Do() {
  std::string config_ret;
  if (config_args_v_[0] == "get") {
    ConfigGet(config_ret);
  } else if (config_args_v_[0] == "set") {
    ConfigSet(config_ret);
  } else if (config_args_v_[0] == "rewrite") {
    ConfigRewrite(config_ret);
  } else if (config_args_v_[0] == "resetstat") {
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

void ConfigCmd::ConfigGet(std::string &ret) {
  std::string get_item = config_args_v_[1];
  if (get_item == "port") {
      ret = "*2\r\n";
      EncodeString(&ret, "port");
      EncodeInt32(&ret, g_pika_conf->port());
  } else if (get_item == "thread-num") {
      ret = "*2\r\n";
      EncodeString(&ret, "thread-num");
      EncodeInt32(&ret, g_pika_conf->thread_num());
  } else if (get_item == "sync-thread-num") {
      ret = "*2\r\n";
      EncodeString(&ret, "sync-thread-num");
      EncodeInt32(&ret, g_pika_conf->sync_thread_num());
  } else if (get_item == "sync-buffer-size") {
      ret = "*2\r\n";
      EncodeString(&ret, "sync-buffer-size");
      EncodeInt32(&ret, g_pika_conf->sync_buffer_size());
  } else if (get_item == "log-path") {
      ret = "*2\r\n";
      EncodeString(&ret, "log-path");
      EncodeString(&ret, g_pika_conf->log_path());
  } else if (get_item == "loglevel") {
      ret = "*2\r\n";
      EncodeString(&ret, "loglevel");
      EncodeString(&ret, g_pika_conf->log_level() ? "ERROR" : "INFO");
  } else if (get_item == "db-path") {
      ret = "*2\r\n";
      EncodeString(&ret, "db-path");
      EncodeString(&ret, g_pika_conf->db_path());
  } else if (get_item == "db-sync-path") {
      ret = "*2\r\n";
      EncodeString(&ret, "db-sync-path");
      EncodeString(&ret, g_pika_conf->db_sync_path());
  } else if (get_item == "db-sync-speed") {
      ret = "*2\r\n";
      EncodeString(&ret, "db-sync-speed");
      EncodeInt32(&ret, g_pika_conf->db_sync_speed());
  } else if (get_item == "maxmemory") {
      ret = "*2\r\n";
      EncodeString(&ret, "maxmemory");
      EncodeInt32(&ret, g_pika_conf->write_buffer_size());
  } else if (get_item == "write-buffer-size") {
      ret = "*2\r\n";
      EncodeString(&ret, "write-buffer-size");
      EncodeInt32(&ret, g_pika_conf->write_buffer_size());
  } else if (get_item == "timeout") {
      ret = "*2\r\n";
      EncodeString(&ret, "timeout");
      EncodeInt32(&ret, g_pika_conf->timeout());
  } else if (get_item == "requirepass") {
      ret = "*2\r\n";
      EncodeString(&ret, "requirepass");
      EncodeString(&ret, g_pika_conf->requirepass());
  } else if (get_item == "userpass") {
      ret = "*2\r\n";
      EncodeString(&ret, "userpass");
      EncodeString(&ret, g_pika_conf->userpass());
  } else if (get_item == "userblacklist") {
      ret = "*2\r\n";
      EncodeString(&ret, "userblacklist");
      EncodeString(&ret, (g_pika_conf->suser_blacklist()).c_str());
  } else if (get_item == "dump-prefix") {
      ret = "*2\r\n";
      EncodeString(&ret, "dump-prefix");
      EncodeString(&ret, g_pika_conf->bgsave_prefix());
  } else if (get_item == "daemonize") {
      ret = "*2\r\n";
      EncodeString(&ret, "daemonize");
      EncodeString(&ret, g_pika_conf->daemonize() ? "yes" : "no");
  } else if (get_item == "slotmigrate") {
      ret = "*2\r\n";
      EncodeString(&ret, "slotmigrate");
      EncodeString(&ret, g_pika_conf->slotmigrate() ? "yes" : "no");
  } else if (get_item == "dump-path") {
      ret = "*2\r\n";
      EncodeString(&ret, "dump-path");
      EncodeString(&ret, g_pika_conf->bgsave_path());
  } else if (get_item == "pidfile") {
      ret = "*2\r\n";
      EncodeString(&ret, "pidfile");
      EncodeString(&ret, g_pika_conf->pidfile());
  } else if (get_item == "maxclients") {
      ret = "*2\r\n";
      EncodeString(&ret, "maxclients");
      EncodeInt32(&ret, g_pika_conf->maxclients());
  } else if (get_item == "target-file-size-base") {
      ret = "*2\r\n";
      EncodeString(&ret, "target-file-size-base");
      EncodeInt32(&ret, g_pika_conf->target_file_size_base());
  } else if (get_item == "max-background-flushes") {
      ret = "*2\r\n";
      EncodeString(&ret, "max-background-flushes");
      EncodeInt32(&ret, g_pika_conf->max_background_flushes());
  } else if (get_item == "max-background-compactions") {
      ret = "*2\r\n";
      EncodeString(&ret, "max-background-compactions");
      EncodeInt32(&ret, g_pika_conf->max_background_compactions());
  } else if (get_item == "max-cache-files") {
      ret = "*2\r\n";
      EncodeString(&ret, "max-cache-files");
      EncodeInt32(&ret, g_pika_conf->max_cache_files());
  } else if (get_item == "expire-logs-days") {
      ret = "*2\r\n";
      EncodeString(&ret, "expire-logs-days");
      EncodeInt32(&ret, g_pika_conf->expire_logs_days());
  } else if (get_item == "expire-logs-nums") {
      ret = "*2\r\n";
      EncodeString(&ret, "expire-logs-nums");
      EncodeInt32(&ret, g_pika_conf->expire_logs_nums());
  } else if (get_item == "root-connection-num" ) {
      ret = "*2\r\n";
      EncodeString(&ret, "root-connection-num");
      EncodeInt32(&ret, g_pika_conf->root_connection_num());
  } else if (get_item == "slowlog-log-slower-than") {
      ret = "*2\r\n";
      EncodeString(&ret, "slowlog-log-slower-than");
      EncodeInt32(&ret, g_pika_conf->slowlog_slower_than());
  } else if (get_item == "binlog-file-size") {
      ret = "*2\r\n";
      EncodeString(&ret, "binlog-file-size");
      EncodeInt32(&ret, g_pika_conf->binlog_file_size());
  } else if (get_item == "compression") {
      ret = "*2\r\n";
      EncodeString(&ret, "compression");
      EncodeString(&ret, g_pika_conf->compression());
  } else if (get_item == "slave-read-only") {
    ret = "*2\r\n";
    EncodeString(&ret, "slave-read-only");
    if (g_pika_conf->readonly()) {
      EncodeString(&ret, "yes");
    } else {
      EncodeString(&ret, "no");
    }
  } else if (get_item == "slaveof") {
    ret = "*2\r\n";
    EncodeString(&ret, "slaveof");
    EncodeString(&ret, g_pika_conf->slaveof());
  } else if (get_item == "*") {
    ret = "*66\r\n";
    EncodeString(&ret, "port");
    EncodeInt32(&ret, g_pika_conf->port());
    EncodeString(&ret, "thread-num");
    EncodeInt32(&ret, g_pika_conf->thread_num());
    EncodeString(&ret, "sync-thread-num");
    EncodeInt32(&ret, g_pika_conf->sync_thread_num());
    EncodeString(&ret, "sync-buffer-size");
    EncodeInt32(&ret, g_pika_conf->sync_buffer_size());
    EncodeString(&ret, "log-path");
    EncodeString(&ret, g_pika_conf->log_path());
    EncodeString(&ret, "loglevel");
    EncodeString(&ret, g_pika_conf->log_level() ? "ERROR" : "INFO");
    EncodeString(&ret, "db-path");
    EncodeString(&ret, g_pika_conf->db_path());
    EncodeString(&ret, "maxmemory");
    EncodeInt32(&ret, g_pika_conf->write_buffer_size());
    EncodeString(&ret, "write-buffer-size");
    EncodeInt32(&ret, g_pika_conf->write_buffer_size());
    EncodeString(&ret, "timeout");
    EncodeInt32(&ret, g_pika_conf->timeout());
    EncodeString(&ret, "requirepass");
    EncodeString(&ret, g_pika_conf->requirepass());
    EncodeString(&ret, "userpass");
    EncodeString(&ret, g_pika_conf->userpass());
    EncodeString(&ret, "userblacklist");
    EncodeString(&ret, g_pika_conf->suser_blacklist());
    EncodeString(&ret, "daemonize");
    EncodeInt32(&ret, g_pika_conf->daemonize());
    EncodeString(&ret, "slotmigrate");
    EncodeInt32(&ret, g_pika_conf->slotmigrate());
    EncodeString(&ret, "dump-path");
    EncodeString(&ret, g_pika_conf->bgsave_path());
    EncodeString(&ret, "dump-prefix");
    EncodeString(&ret, g_pika_conf->bgsave_prefix());
    EncodeString(&ret, "pidfile");
    EncodeString(&ret, g_pika_conf->pidfile());
    EncodeString(&ret, "maxclients");
    EncodeInt32(&ret, g_pika_conf->maxclients());
    EncodeString(&ret, "target-file-size-base");
    EncodeInt32(&ret, g_pika_conf->target_file_size_base());
    EncodeString(&ret, "max-background-flushes");
    EncodeInt32(&ret, g_pika_conf->max_background_flushes());
    EncodeString(&ret, "max-background-compactions");
    EncodeInt32(&ret, g_pika_conf->max_background_compactions());
    EncodeString(&ret, "max-cache-files");
    EncodeInt32(&ret, g_pika_conf->max_cache_files());
    EncodeString(&ret, "expire-logs-days");
    EncodeInt32(&ret, g_pika_conf->expire_logs_days());
    EncodeString(&ret, "expire-logs-nums");
    EncodeInt32(&ret, g_pika_conf->expire_logs_nums());
    EncodeString(&ret, "root-connection-num");
    EncodeInt32(&ret, g_pika_conf->root_connection_num());
    EncodeString(&ret, "slowlog-log-slower-than");
    EncodeInt32(&ret, g_pika_conf->slowlog_slower_than());
    EncodeString(&ret, "slave-read-only");
    EncodeInt32(&ret, g_pika_conf->readonly());
    EncodeString(&ret, "binlog-file-size");
    EncodeInt32(&ret, g_pika_conf->binlog_file_size());
    EncodeString(&ret, "compression");
    EncodeString(&ret, g_pika_conf->compression());
    EncodeString(&ret, "db-sync-path");
    EncodeString(&ret, g_pika_conf->db_sync_path());
    EncodeString(&ret, "db-sync-speed");
    EncodeInt32(&ret, g_pika_conf->db_sync_speed());
    EncodeString(&ret, "network-interface");
    EncodeString(&ret, g_pika_conf->network_interface());
    EncodeString(&ret, "slaveof");
    EncodeString(&ret, g_pika_conf->slaveof());
  } else {
    ret = "*0\r\n";
  }
}

void ConfigCmd::ConfigSet(std::string& ret) {
  std::string set_item = config_args_v_[1];
  if (set_item == "*") {
    ret = "*13\r\n";
    EncodeString(&ret, "loglevel");
    EncodeString(&ret, "timeout");
    EncodeString(&ret, "requirepass");
    EncodeString(&ret, "slotmigrate");
    EncodeString(&ret, "userpass");
    EncodeString(&ret, "userblacklist");
    EncodeString(&ret, "dump-prefix");
    EncodeString(&ret, "maxclients");
    EncodeString(&ret, "expire-logs-days");
    EncodeString(&ret, "expire-logs-nums");
    EncodeString(&ret, "root-connection-num");
    EncodeString(&ret, "slowlog-log-slower-than");
    EncodeString(&ret, "slave-read-only");
    EncodeString(&ret, "db-sync-speed");
    return;
  }
  std::string value = config_args_v_[2];
  long int ival;
  if (set_item == "loglevel") {
    slash::StringToLower(value);
    if (value == "info") {
      ival = 0;
    } else if (value == "error") {
      ival = 1;
    } else {
      ret = "-ERR Invalid argument " + value + " for CONFIG SET 'loglevel'\r\n";
      return;
    }
    g_pika_conf->SetLogLevel(ival);
    FLAGS_minloglevel = g_pika_conf->log_level();
    ret = "+OK\r\n";
  } else if (set_item == "timeout") {
    if (!slash::string2l(value.data(), value.size(), &ival)) {
      ret = "-ERR Invalid argument " + value + " for CONFIG SET 'timeout'\r\n";
      return;
    }
    g_pika_conf->SetTimeout(ival);
    ret = "+OK\r\n";
  } else if (set_item == "requirepass") {
    g_pika_conf->SetRequirePass(value);
    ret = "+OK\r\n";
  } else if (set_item == "slotmigrate") {
    g_pika_conf->SetSlotMigrate(value);
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
    if (!slash::string2l(value.data(), value.size(), &ival)) {
      ret = "-ERR Invalid argument " + value + " for CONFIG SET 'maxclients'\r\n";
      return;
    }
    g_pika_conf->SetMaxConnection(ival);
    ret = "+OK\r\n";
  } else if (set_item == "expire-logs-days") {
    if (!slash::string2l(value.data(), value.size(), &ival)) {
      ret = "-ERR Invalid argument " + value + " for CONFIG SET 'expire-logs-days'\r\n";
      return;
    }
    g_pika_conf->SetExpireLogsDays(ival);
    ret = "+OK\r\n";
  } else if (set_item == "expire-logs-nums") {
    if (!slash::string2l(value.data(), value.size(), &ival)) {
      ret = "-ERR Invalid argument " + value + " for CONFIG SET 'expire-logs-nums'\r\n";
      return;
    }
    g_pika_conf->SetExpireLogsNums(ival);
    ret = "+OK\r\n";
  } else if (set_item == "root-connection-num") {
    if (!slash::string2l(value.data(), value.size(), &ival)) {
      ret = "-ERR Invalid argument " + value + " for CONFIG SET 'root-connection-num'\r\n";
      return;
    }
    g_pika_conf->SetRootConnectionNum(ival);
    ret = "+OK\r\n";
  } else if (set_item == "slowlog-log-slower-than") {
    if (!slash::string2l(value.data(), value.size(), &ival)) {
      ret = "-ERR Invalid argument " + value + " for CONFIG SET 'slowlog-log-slower-than'\r\n";
      return;
    }
    g_pika_conf->SetSlowlogSlowerThan(ival);
    ret = "+OK\r\n";
  } else if (set_item == "slave-read-only") {
    slash::StringToLower(value);
    bool is_readonly;
    if (value == "1" || value == "yes") {
      is_readonly = true;
    } else if (value == "0" || value == "no") {
      is_readonly = false;
    } else {
      ret = "-ERR Invalid argument " + value + " for CONFIG SET 'slave-read-only'\r\n";
      return;
    }
    g_pika_conf->SetReadonly(is_readonly);
    ret = "+OK\r\n";
  } else if (set_item == "db-sync-speed") {
    if (!slash::string2l(value.data(), value.size(), &ival)) {
      ret = "-ERR Invalid argument " + value + " for CONFIG SET 'db-sync-speed(MB)'\r\n";
      return;
    }
    if (ival < 0 || ival > 125) {
      ival = 125;
    }
    g_pika_conf->SetDbSyncSpeed(ival);
    ret = "+OK\r\n";
  } else {
    ret = "-ERR No such configure item\r\n";
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

void MonitorCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  (void)ptr_info;
  if (argv.size() != 2) { //append a arg in DoCmd for monitor cmd
    res_.SetRes(CmdRes::kWrongNum, kCmdNameMonitor);
    return;
  }
  memcpy(&self_client_, argv[1].data(), sizeof(PikaClientConn*));
}

void MonitorCmd::Do() {
  PikaWorkerThread* self_thread = self_client_->self_thread();
  self_thread->conns_.erase(self_client_->fd());
  self_thread->pink_epoll()->PinkDelEvent(self_client_->fd());
  g_pika_server->AddMonitorClient(self_client_);
  g_pika_server->AddMonitorMessage("OK");
}

void DbsizeCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  (void)ptr_info;
  if (argv.size() != 1) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameDbsize);
    return;
  }
}

void DbsizeCmd::Do() {
  PikaServer::KeyScanInfo key_scan_info = g_pika_server->key_scan_info();
  std::vector<uint64_t> &key_nums_v = key_scan_info.key_nums_v;
  if (key_scan_info.key_nums_v.size() != 5) {
    res_.SetRes(CmdRes::kErrOther, "keyspace error");
    return;
  }
  int32_t dbsize = key_nums_v[0] + key_nums_v[1] + key_nums_v[2] + key_nums_v[3] + key_nums_v[4];
  res_.AppendInteger(dbsize);
}
