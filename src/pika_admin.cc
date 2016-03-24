#include "slash_string.h"
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
  if (master_ip_ == "no" && slash::StringToLower(*it++) == "one") {
    if (argv.end() - it == 0) {
      is_noone_ = true;
    } else {
      res_.SetRes(CmdRes::kWrongNum, kCmdNameSlaveof);
    }
    return;
  }

  std::string str_master_port = *it++;
  if (!slash::string2l(str_master_port.data(), str_master_port.size(), &master_port_) && master_port_ <= 0) {
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
    if (!slash::string2l(str_filenum.data(), str_filenum.size(), &filenum_) && filenum_ < 0) {
      res_.SetRes(CmdRes::kInvalidInt);
      return;
    }
    std::string str_pro_offset = *it++;
    if (!slash::string2l(str_pro_offset.data(), str_pro_offset.size(), &pro_offset_) && pro_offset_ < 0) {
      res_.SetRes(CmdRes::kInvalidInt);
      return;
    }
  } else {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlaveof);
  }
}

void SlaveofCmd::Do() {
  if (is_noone_) {
    g_pika_server->RemoveMaster();
    res_.SetRes(CmdRes::kOk);
    return;
  }
  if (have_offset_) {
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
  if (!slash::string2l(str_slave_port.data(), str_slave_port.size(), &slave_port_) && slave_port_ <= 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }

  std::string str_filenum = *it++;
  if (!slash::string2l(str_filenum.data(), str_filenum.size(), &filenum_) && filenum_ <= 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }

  std::string str_pro_offset = *it++;
  if (!slash::string2l(str_pro_offset.data(), str_pro_offset.size(), &pro_offset_) && pro_offset_ <= 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }

}

void TrysyncCmd::Do() {
  std::string ip_port = slave_ip_;
  char buf[10];
  slash::ll2string(buf, sizeof(buf), slave_port_);
  ip_port.append(":");
  ip_port.append(buf);
  DLOG(INFO) << "Trysync, Slave ip_port: " << ip_port << " filenum: " << filenum_ << " pro_offset: " << pro_offset_;
  if (!g_pika_server->FindSlave(ip_port)) {
    SlaveItem s;
    s.sid = g_pika_server->GenSid();
    s.ip_port = ip_port;
    s.port = slave_port_;
    s.hb_fd = -1;
    s.stage = SLAVE_ITEM_STAGE_ONE;
    gettimeofday(&s.create_time, NULL);
    s.sender = NULL;
    
    DLOG(INFO) << "Trysync, dont FindSlave, so AddBinlogSender";
    Status status = g_pika_server->AddBinlogSender(s, filenum_, pro_offset_);
    if (status.ok()) {
      res_.AppendInteger(s.sid);
      DLOG(INFO) << "Send Sid to Slave: " << s.sid;
      g_pika_server->BecomeMaster();
    } else {
      res_.SetRes(CmdRes::kErrOther, "Error in AddBinlogSender");
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, "Already Exist");
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
  if (g_pika_server->PurgeLogs(num_)) {
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
  slash::RWLock(g_pika_server->rwlock(), true);
  if (g_pika_server->FlushAll()) {
    res_.SetRes(CmdRes::kOk);
  } else {
    res_.SetRes(CmdRes::kErrOther, "There are some bgthread using db now, can not flushall");
  }
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
  slash::RWLock(g_pika_server->rwlock(), true);
  if (is_open_) {
    g_pika_conf->SetReadonly(true);
  } else {
    g_pika_conf->SetReadonly(false);
  }
  res_.SetRes(CmdRes::kOk);
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
    std::vector< std::pair<int, std::string> > clients;
    g_pika_server->ClientList(&clients);
    std::vector<std::pair<int, std::string> >::iterator iter= clients.begin();
    std::string reply = "+";
    char buf[128];
    while (iter != clients.end()) {
      snprintf(buf, sizeof(buf), "addr=%s, fd=%d\n", iter->second.c_str(), iter->first);
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

const std::string InfoCmd::kServerSection = "server";
const std::string InfoCmd::kClientsSection = "clients";
const std::string InfoCmd::kStatsSection = "stats";
const std::string InfoCmd::kReplicationSection = "replication";
const std::string InfoCmd::kKeyspaceSection = "keyspace";

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
  if (argv[1] == kServerSection) {
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
    if (argv[2] == "1") { //only info keyspace 0 or info keyspace 1 two format
      rescan_ = true;
    } else if (argv[2] != "0") {
      res_.SetRes(CmdRes::kSyntaxErr);
    }
    return;
  } else {
    info_section_ = kInfoErr;
  }
  if (argc != 2) {
    res_.SetRes(CmdRes::kSyntaxErr);
  }
}

void InfoCmd::Do() {
  std::string info;
  if (info_section_ == kInfoAll) {
    InfoServer(info);
    info.append("\r\n");
    InfoClients(info);
    info.append("\r\n");
    InfoStats(info);
    info.append("\r\n");
    InfoReplication(info);
    info.append("\r\n");
    InfoKeyspace(info);
  } else if (info_section_ == kInfoServer) {
    InfoServer(info);
  } else if (info_section_ == kInfoClients) {
    InfoClients(info);
  } else if (info_section_ == kInfoStats) {
    InfoStats(info);
  } else if (info_section_ == kInfoReplication) {
    InfoReplication(info);
  } else if (info_section_ == kInfoKeyspace) {
    InfoKeyspace(info);
  } else {
    //kInfoErr is nothing
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
  uint32_t purge_max;
  tmp_stream << "# Server\r\n";
  tmp_stream << "pika_version: " << kPikaVersion << "\r\n";
  tmp_stream << "os: " << host_info.sysname << " " << host_info.release << " " << host_info.machine << "\r\n";
  tmp_stream << "arch_bits: " << (reinterpret_cast<char*>(&host_info.machine) + strlen(host_info.machine) - 2) << "\r\n";
  tmp_stream << "process_id: " << getpid() << "\r\n";
  tmp_stream << "tcp_port: " << g_pika_conf->port() << "\r\n";
  tmp_stream << "thread_num: " << g_pika_conf->thread_num() << "\r\n";
  tmp_stream << "uptime_in_seconds: " << (current_time_s - g_pika_server->start_time_s()) << "\r\n";
  tmp_stream << "uptime_in_days: " << (current_time_s / (24*3600) - g_pika_server->start_time_s() / (24*3600) + 1) << "\r\n";
  tmp_stream << "config_file: " << g_pika_conf->conf_path() << "\r\n";
  PikaServer::BGSaveInfo bgsave_info = g_pika_server->bgsave_info();
  bool is_bgsaving = g_pika_server->bgsaving();
  tmp_stream << "is_bgsaving: " << (is_bgsaving ? "Yes, " : "No, ") << bgsave_info.s_start_time << ", "
                                << (is_bgsaving ? (current_time_s - bgsave_info.start_time) : 0) << "\r\n";
  PikaServer::KeyScanInfo key_scan_info = g_pika_server->key_scan_info();
  bool is_scaning = g_pika_server->key_scaning();
  tmp_stream << "is_scaning_keyspace: " << (is_scaning ? ("Yes, " + key_scan_info.s_start_time) + "," : "No");
  if (is_scaning) {
    tmp_stream << current_time_s - key_scan_info.start_time;
  }
  tmp_stream << "\r\n";
  tmp_stream << "is_compact: " << g_pika_server->db()->GetCurrentTaskType() << "\r\n";
  tmp_stream << "db_size: " << (slash::Du(g_pika_conf->db_path()) >> 20)  << "M\r\n";
  tmp_stream << "log_size: " << (slash::Du(g_pika_conf->log_path()) >> 20) << "M\r\n";
  tmp_stream << "compression: " << g_pika_conf->compression() << "\r\n";
  tmp_stream << "safety purge: " << (g_pika_server->GetPurgeWindow(purge_max) ? static_cast<int32_t>(purge_max) : -1) << "\r\n"; 
  tmp_stream << "expire_logs_days: " << "\r\n";
  tmp_stream << "expire_logs_num: " << "\r\n"; //TODO

  info.append(tmp_stream.str());
}

void InfoCmd::InfoClients(std::string &info) {
  std::stringstream tmp_stream;
  tmp_stream << "# Clients\r\n";
  tmp_stream << "connected_clients: " << g_pika_server->ClientList() << "\r\n";

  info.append(tmp_stream.str());
}

void InfoCmd::InfoStats(std::string &info) {
  std::stringstream tmp_stream;
  tmp_stream << "# Stats\r\n";
  tmp_stream << "total_connections_received: " << g_pika_server->accumulative_connections() << "\r\n";
  tmp_stream << "instances_ops_per_sec: " << g_pika_server->ServerCurrentQps() << "\r\n";
  tmp_stream << "accumulative_query_nums: " << g_pika_server->ServerQueryNum() << "\r\n";

  info.append(tmp_stream.str());
}

void InfoCmd::InfoReplication(std::string &info) {
  int host_role = g_pika_server->role();
  std::stringstream tmp_stream;
  tmp_stream << "# replication(";
  switch (host_role) {
    case PIKA_ROLE_MASTER :
    case PIKA_ROLE_SINGLE : tmp_stream << "MASTER)\r\nrole: master\r\n"; break;
    case PIKA_ROLE_SLAVE : tmp_stream << "SLAVE)\r\nrole: slave\r\n"; break;
    case PIKA_ROLE_MASTER | PIKA_ROLE_SLAVE : tmp_stream << "MASTER/SLAVE)\r\nrole: slave\r\n"; break;
    default: info.append("ERR: server role is error\r\n"); return;
  }
  
  std::string slaves_list_str;
  //int32_t slaves_num = g_pika_server->GetSlaveListString(slaves_list_str);
  switch (host_role) {
    case PIKA_ROLE_MASTER | PIKA_ROLE_SLAVE :
    case PIKA_ROLE_SLAVE :
      tmp_stream << "master_host: " << g_pika_server->master_ip() << "\r\n";
      tmp_stream << "master_port: " << g_pika_server->master_port() << "\r\n";
      tmp_stream << "master_link_status: " << (g_pika_server->repl_state() == PIKA_REPL_CONNECTED ? "up" : "down") << "\r\n";
      tmp_stream << "slave_read_only: " << g_pika_conf->readonly() << "\r\n";
      if (host_role == PIKA_ROLE_SLAVE) {
        break;
      }
    case PIKA_ROLE_SINGLE :
    case PIKA_ROLE_MASTER :
      tmp_stream << "connected_slaves: " << g_pika_server->GetSlaveListString(slaves_list_str) << "\r\n" << slaves_list_str;
  }
  
  info.append(tmp_stream.str());
}

void InfoCmd::InfoKeyspace(std::string &info) {
  PikaServer::KeyScanInfo key_scan_info = g_pika_server->key_scan_info();
  std::vector<uint64_t> &key_nums_v = key_scan_info.key_nums_v;
  if (key_scan_info.key_nums_v.size() != 5) {
    info.append("info keyspace error\r\n");
    return;
  }
  std::stringstream tmp_stream;
  tmp_stream << "# Keyspace\r\n";
  tmp_stream << "# Time: " << key_scan_info.s_start_time << "\r\n";
  tmp_stream << "kv keys: " << key_nums_v[0] << "\r\n";
  tmp_stream << "hash keys: " << key_nums_v[1] << "\r\n";
  tmp_stream << "list keys: " << key_nums_v[2] << "\r\n";
  tmp_stream << "zset keys: " << key_nums_v[3] << "\r\n";
  tmp_stream << "set keys: " << key_nums_v[4] << "\r\n";
  info.append(tmp_stream.str());

  if (rescan_) {
    g_pika_server->KeyScan();
  }
  return;
}
