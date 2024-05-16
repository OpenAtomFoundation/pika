// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_admin.h"

#include <sys/statvfs.h>
#include <sys/time.h>
#include <sys/utsname.h>

#include <algorithm>
#include <unordered_map>

#include <glog/logging.h>

#include "include/build_version.h"
#include "include/pika_cmd_table_manager.h"
#include "include/pika_rm.h"
#include "include/pika_server.h"
#include "include/pika_version.h"
#include "include/pika_conf.h"
#include "pstd/include/rsync.h"
#include "include/throttle.h"
using pstd::Status;

extern PikaServer* g_pika_server;
extern std::unique_ptr<PikaReplicaManager> g_pika_rm;
extern std::unique_ptr<PikaCmdTableManager> g_pika_cmd_table_manager;

static std::string ConstructPinginPubSubResp(const PikaCmdArgsType& argv) {
  if (argv.size() > 2) {
    return "-ERR wrong number of arguments for " + kCmdNamePing + " command\r\n";
  }
  std::stringstream resp;

  resp << "*2\r\n"
       << "$4\r\n"
       << "pong\r\n";
  if (argv.size() == 2) {
    resp << "$" << argv[1].size() << "\r\n" << argv[1] << "\r\n";
  } else {
    resp << "$0\r\n\r\n";
  }
  return resp.str();
}

static double MethodofCommandStatistics(const uint64_t time_consuming, const uint64_t frequency) {
  return (static_cast<double>(time_consuming) / 1000.0) / static_cast<double>(frequency);
}

static double MethodofTotalTimeCalculation(const uint64_t time_consuming) {
  return static_cast<double>(time_consuming) / 1000.0;
}

enum AuthResult {
  OK,
  INVALID_PASSWORD,
  NO_REQUIRE_PASS,
  INVALID_CONN,
};

static AuthResult AuthenticateUser(const std::string& cmdName, const std::string& userName, const std::string& pwd,
                                   const std::shared_ptr<net::NetConn>& conn, bool defaultAuth) {
  if (defaultAuth) {
    auto defaultUser = g_pika_server->Acl()->GetUserLock(Acl::DefaultUser);
    if (defaultUser->HasFlags(static_cast<uint32_t>(AclUserFlag::NO_PASS))) {
      return AuthResult::NO_REQUIRE_PASS;
    }
  }

  auto user = g_pika_server->Acl()->Auth(userName, pwd);

  if (!user) {
    std::string cInfo;
    if (auto ptr = std::dynamic_pointer_cast<PikaClientConn>(conn); ptr) {
      ptr->ClientInfoToString(&cInfo, cmdName);
    }
    g_pika_server->Acl()->AddLogEntry(static_cast<int32_t>(AclDeniedCmd::NO_AUTH),
                                      static_cast<int32_t>(AclLogCtx::TOPLEVEL), userName, "AUTH", cInfo);
    return AuthResult::INVALID_PASSWORD;
  }

  if (!conn) {
    LOG(WARNING) << " weak ptr is empty";
    return AuthResult::INVALID_CONN;
  }
  std::shared_ptr<PikaClientConn> cli_conn = std::dynamic_pointer_cast<PikaClientConn>(conn);

  cli_conn->DoAuth(user);

  return AuthResult::OK;
}

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

  if (argv_.size() > 4) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlaveof);
    return;
  }

  if (argv_.size() == 3 && (strcasecmp(argv_[1].data(), "no") == 0) && (strcasecmp(argv_[2].data(), "one") == 0)) {
    is_none_ = true;
    return;
  }
  // self is master of A , want to slaveof B
  if ((g_pika_server->role() & PIKA_ROLE_MASTER) != 0) {
    res_.SetRes(CmdRes::kErrOther, "already master of others, invalid usage");
    return;
  }

  master_ip_ = argv_[1];
  std::string str_master_port = argv_[2];
  if ((pstd::string2int(str_master_port.data(), str_master_port.size(), &master_port_) == 0) || master_port_ <= 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }

  if ((pstd::StringToLower(master_ip_) == "localhost" || master_ip_ == "127.0.0.1" || master_ip_ == g_pika_server->host()) && master_port_ == g_pika_server->port()) {
    res_.SetRes(CmdRes::kErrOther, "The master ip:port and the slave ip:port are the same");
    return;
  }

  if (argv_.size() == 4) {
    if (strcasecmp(argv_[3].data(), "force") == 0) {
      g_pika_server->SetForceFullSync(true);
    } else {
      res_.SetRes(CmdRes::kWrongNum, kCmdNameSlaveof);
    }
  }
}

void SlaveofCmd::Do() {
  // Check if we are already connected to the specified master
  if ((master_ip_ == "127.0.0.1" || g_pika_server->master_ip() == master_ip_) &&
      g_pika_server->master_port() == master_port_) {
    res_.SetRes(CmdRes::kOk);
    return;
  }

  g_pika_server->RemoveMaster();

  if (is_none_) {
    res_.SetRes(CmdRes::kOk);
    g_pika_conf->SetSlaveof(std::string());
    return;
  }

  /* The return value of the slaveof command OK does not really represent whether
   * the data synchronization was successful, but only changes the status of the
   * slaveof executor to slave */

  bool sm_ret = g_pika_server->SetMaster(master_ip_, static_cast<int32_t>(master_port_));

  if (sm_ret) {
    res_.SetRes(CmdRes::kOk);
    g_pika_server->ClearCacheDbAsync(db_);
    g_pika_conf->SetSlaveof(master_ip_ + ":" + std::to_string(master_port_));
    g_pika_server->SetFirstMetaSync(true);
  } else {
    res_.SetRes(CmdRes::kErrOther, "Server is not in correct state for slaveof");
  }
}

/*
 * dbslaveof db[0 ~ 7]
 * dbslaveof db[0 ~ 7] force
 * dbslaveof db[0 ~ 7] no one
 * dbslaveof db[0 ~ 7] filenum offset
 */
void DbSlaveofCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameDbSlaveof);
    return;
  }
  if (((g_pika_server->role() ^ PIKA_ROLE_SLAVE) != 0) || !g_pika_server->MetaSyncDone()) {
    res_.SetRes(CmdRes::kErrOther, "Not currently a slave");
    return;
  }

  if (argv_.size() > 4) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameDbSlaveof);
    return;
  }

  db_name_ = argv_[1];
  if (!g_pika_server->IsDBExist(db_name_)) {
    res_.SetRes(CmdRes::kErrOther, "Invaild db name");
    return;
  }

  if (argv_.size() == 3 && (strcasecmp(argv_[2].data(), "force") == 0)) {
    force_sync_ = true;
    return;
  }

  if (argv_.size() == 4) {
    if ((strcasecmp(argv_[2].data(), "no") == 0) && (strcasecmp(argv_[3].data(), "one") == 0)) {
      is_none_ = true;
      return;
    }

    if ((pstd::string2int(argv_[2].data(), argv_[2].size(), &filenum_) == 0) || filenum_ < 0) {
      res_.SetRes(CmdRes::kInvalidInt);
      return;
    }
    if ((pstd::string2int(argv_[3].data(), argv_[3].size(), &offset_) == 0) || offset_ < 0) {
      res_.SetRes(CmdRes::kInvalidInt);
      return;
    }
    have_offset_ = true;
  }
}

void DbSlaveofCmd::Do() {
  std::shared_ptr<SyncSlaveDB> slave_db = g_pika_rm->GetSyncSlaveDBByName(DBInfo(db_name_));
  if (!slave_db) {
    res_.SetRes(CmdRes::kErrOther, "Db not found");
    return;
  }

  Status s;
  if (is_none_) {
    s = g_pika_rm->SendRemoveSlaveNodeRequest(db_name_);
  } else {
    if (slave_db->State() == ReplState::kNoConnect || slave_db->State() == ReplState::kError ||
        slave_db->State() == ReplState::kDBNoConnect) {
      if (have_offset_) {
        std::shared_ptr<SyncMasterDB> db = g_pika_rm->GetSyncMasterDBByName(DBInfo(db_name_));
        db->Logger()->SetProducerStatus(filenum_, offset_);
      }
      ReplState state = force_sync_ ? ReplState::kTryDBSync : ReplState::kTryConnect;
      s = g_pika_rm->ActivateSyncSlaveDB(
          RmNode(g_pika_server->master_ip(), g_pika_server->master_port(), db_name_, 0), state);
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
}

void AuthCmd::Do() {
  std::shared_ptr<net::NetConn> conn = GetConn();
  if (!conn) {
    res_.SetRes(CmdRes::kErrOther, kCmdNamePing);
    LOG(WARNING) << name_ << " weak ptr is empty";
    return;
  }

  std::string userName = "";
  std::string pwd = "";
  bool defaultAuth = false;
  if (argv_.size() == 2) {
    pwd = argv_[1];
//    defaultAuth = true;
  } else {
    userName = argv_[1];
    pwd = argv_[2];
  }

  AuthResult authResult;
  if (userName == "") {
    //  default
    authResult = AuthenticateUser(name(), Acl::DefaultUser, pwd, conn, true);
    if (authResult != AuthResult::OK && authResult != AuthResult::NO_REQUIRE_PASS) {
      //  Limit
      authResult = AuthenticateUser(name(), Acl::DefaultLimitUser, pwd, conn, defaultAuth);
    }
  } else {
    authResult = AuthenticateUser(name(), userName, pwd, conn, defaultAuth);
  }

  switch (authResult) {
    case AuthResult::INVALID_CONN:
      res_.SetRes(CmdRes::kErrOther, kCmdNamePing);
      return;
    case AuthResult::INVALID_PASSWORD:
      res_.AppendContent("-WRONGPASS invalid username-password pair or user is disabled.");
      return;
    case AuthResult::NO_REQUIRE_PASS:
      res_.SetRes(CmdRes::kErrOther, "Client sent AUTH, but no password is set");
      return;
    case AuthResult::OK:
      break;
  }
  res_.SetRes(CmdRes::kOk);
}

void BgsaveCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameBgsave);
    return;
  }
  if (argv_.size() == 2) {
    std::vector<std::string> dbs;
    pstd::StringSplit(argv_[1], COMMA, dbs);
    for (const auto& db : dbs) {
      if (!g_pika_server->IsDBExist(db)) {
        res_.SetRes(CmdRes::kInvalidDB, db);
        return;
      } else {
        bgsave_dbs_.insert(db);
      }
    }
  } else {
    bgsave_dbs_ = g_pika_server->GetAllDBName();
  }
}

void BgsaveCmd::Do() {
  g_pika_server->DoSameThingSpecificDB(bgsave_dbs_, {TaskType::kBgSave});
  LogCommand();
  res_.AppendContent("+Background saving started");
}

void CompactCmd::DoInitial() {
  if (!CheckArg(argv_.size()) || argv_.size() > 3) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameCompact);
    return;
  }

  if (g_pika_server->IsKeyScaning()) {
    res_.SetRes(CmdRes::kErrOther, "The info keyspace operation is executing, Try again later");
    return;
  }

  if (argv_.size() == 1) {
    struct_type_ = "all";
    compact_dbs_ = g_pika_server->GetAllDBName();
  } else if (argv_.size() == 2) {
    struct_type_ = argv_[1];
    compact_dbs_ = g_pika_server->GetAllDBName();
  } else if (argv_.size() == 3) {
    std::vector<std::string> dbs;
    pstd::StringSplit(argv_[1], COMMA, dbs);
    for (const auto& db : dbs) {
      if (!g_pika_server->IsDBExist(db)) {
        res_.SetRes(CmdRes::kInvalidDB, db);
        return;
      } else {
        compact_dbs_.insert(db);
      }
    }
    struct_type_ = argv_[2];
  }
}

void CompactCmd::Do() {
  if (strcasecmp(struct_type_.data(), "all") == 0) {
    g_pika_server->DoSameThingSpecificDB(compact_dbs_, {TaskType::kCompactAll});
  } else if (strcasecmp(struct_type_.data(), "string") == 0) {
    g_pika_server->DoSameThingSpecificDB(compact_dbs_, {TaskType::kCompactStrings});
  } else if (strcasecmp(struct_type_.data(), "hash") == 0) {
    g_pika_server->DoSameThingSpecificDB(compact_dbs_, {TaskType::kCompactHashes});
  } else if (strcasecmp(struct_type_.data(), "set") == 0) {
    g_pika_server->DoSameThingSpecificDB(compact_dbs_, {TaskType::kCompactSets});
  } else if (strcasecmp(struct_type_.data(), "zset") == 0) {
    g_pika_server->DoSameThingSpecificDB(compact_dbs_, {TaskType::kCompactZSets});
  } else if (strcasecmp(struct_type_.data(), "list") == 0) {
    g_pika_server->DoSameThingSpecificDB(compact_dbs_, {TaskType::kCompactList});
  } else {
    res_.SetRes(CmdRes::kInvalidDbType, struct_type_);
    return;
  }
  LogCommand();
  res_.SetRes(CmdRes::kOk);
}

void CompactRangeCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameCompactRange);
    return;
  }

  if (g_pika_server->IsKeyScaning()) {
    res_.SetRes(CmdRes::kErrOther, "The info keyspace operation is executing, Try again later");
    return;
  }

  std::vector<std::string> dbs;
  pstd::StringSplit(argv_[1], COMMA, dbs);
  for (const auto& db : dbs) {
    if (!g_pika_server->IsDBExist(db)) {
      res_.SetRes(CmdRes::kInvalidDB, db);
      return;
    } else {
      compact_dbs_.insert(db);
    }
  }
  struct_type_ = argv_[2];
  start_key_ = argv_[3];
  end_key_ = argv_[4];
}

void CompactRangeCmd::Do() {
  if (strcasecmp(struct_type_.data(), "string") == 0) {
    g_pika_server->DoSameThingSpecificDB(compact_dbs_, {TaskType::kCompactRangeStrings, {start_key_, end_key_}});
  } else if (strcasecmp(struct_type_.data(), "hash") == 0) {
    g_pika_server->DoSameThingSpecificDB(compact_dbs_, {TaskType::kCompactRangeHashes, {start_key_, end_key_}});
  } else if (strcasecmp(struct_type_.data(), "set") == 0) {
    g_pika_server->DoSameThingSpecificDB(compact_dbs_, {TaskType::kCompactRangeSets, {start_key_, end_key_}});
  } else if (strcasecmp(struct_type_.data(), "zset") == 0) {
    g_pika_server->DoSameThingSpecificDB(compact_dbs_, {TaskType::kCompactRangeZSets, {start_key_, end_key_}});
  } else if (strcasecmp(struct_type_.data(), "list") == 0) {
    g_pika_server->DoSameThingSpecificDB(compact_dbs_, {TaskType::kCompactRangeList, {start_key_, end_key_}});
  } else {
    res_.SetRes(CmdRes::kInvalidDbType, struct_type_);
    return;
  }
  LogCommand();
  res_.SetRes(CmdRes::kOk);
}

void PurgelogstoCmd::DoInitial() {
  if (!CheckArg(argv_.size()) || argv_.size() > 3) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePurgelogsto);
    return;
  }
  std::string filename = argv_[1];
  if (filename.size() <= kBinlogPrefixLen || kBinlogPrefix != filename.substr(0, kBinlogPrefixLen)) {
    res_.SetRes(CmdRes::kInvalidParameter);
    return;
  }
  std::string str_num = filename.substr(kBinlogPrefixLen);
  int64_t num = 0;
  if ((pstd::string2int(str_num.data(), str_num.size(), &num) == 0) || num < 0) {
    res_.SetRes(CmdRes::kInvalidParameter);
    return;
  }
  num_ = num;

  db_ = (argv_.size() == 3) ? argv_[2] : g_pika_conf->default_db();
  if (!g_pika_server->IsDBExist(db_)) {
    res_.SetRes(CmdRes::kInvalidDB, db_);
    return;
  }
}

void PurgelogstoCmd::Do() {
  std::shared_ptr<SyncMasterDB> sync_db = g_pika_rm->GetSyncMasterDBByName(DBInfo(db_));
  if (!sync_db) {
    res_.SetRes(CmdRes::kErrOther, "DB not found");
  } else {
    sync_db->StableLogger()->PurgeStableLogs(num_, true);
    res_.SetRes(CmdRes::kOk);
  }
}

void PingCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePing);
    return;
  }
}

void PingCmd::Do() {
  std::shared_ptr<net::NetConn> conn = GetConn();
  if (!conn) {
    res_.SetRes(CmdRes::kErrOther, kCmdNamePing);
    LOG(WARNING) << name_ << " weak ptr is empty";
    return;
  }
  std::shared_ptr<PikaClientConn> cli_conn = std::dynamic_pointer_cast<PikaClientConn>(conn);

  if (cli_conn->IsPubSub()) {
    return res_.SetRes(CmdRes::kNone, ConstructPinginPubSubResp(argv_));
  }
  res_.SetRes(CmdRes::kPong);
}
void SelectCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSelect);
    return;
  }
  db_name_ = "db" + argv_[1];
  db_ = g_pika_server->GetDB(db_name_);
  sync_db_ = g_pika_rm->GetSyncMasterDBByName(DBInfo(db_name_));
  int index = atoi(argv_[1].data());
  if (std::to_string(index) != argv_[1]) {
    res_.SetRes(CmdRes::kInvalidIndex, kCmdNameSelect);
    return;
  }
  if (index < 0 || index >= g_pika_conf->databases()) {
    res_.SetRes(CmdRes::kInvalidIndex, kCmdNameSelect + " DB index is out of range");
    return;
  }
  if (db_ == nullptr || sync_db_ == nullptr) {
    res_.SetRes(CmdRes::kInvalidDB, kCmdNameSelect);
    return;
  }
}

void SelectCmd::Do() {
  std::shared_ptr<PikaClientConn> conn = std::dynamic_pointer_cast<PikaClientConn>(GetConn());
  if (!conn) {
    res_.SetRes(CmdRes::kErrOther, kCmdNameSelect);
    LOG(WARNING) << name_ << " weak ptr is empty";
    return;
  }
  conn->SetCurrentDb(db_name_);
  res_.SetRes(CmdRes::kOk);
}

void FlushallCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameFlushall);
    return;
  }
}

void FlushallCmd::Do() {
  std::lock_guard l_trw(g_pika_server->GetDBLock());
  for (const auto& db_item : g_pika_server->GetDB()) {
    if (db_item.second->IsKeyScaning()) {
      res_.SetRes(CmdRes::kErrOther, "The keyscan operation is executing, Try again later");
      return;
    }
  }
  g_pika_rm->DBLock();
  for (const auto& db_item : g_pika_server->GetDB()) {
    db_item.second->DBLock();
  }
  FlushAllWithoutLock();
  for (const auto& db_item : g_pika_server->GetDB()) {
    db_item.second->DBUnlock();
  }
  g_pika_rm->DBUnlock();
  if (res_.ok()) {
    res_.SetRes(CmdRes::kOk);
  }
}

void FlushallCmd::DoThroughDB() {
  Do();
}

void FlushallCmd::DoUpdateCache(std::shared_ptr<DB> db) {
  // clear cache
  if (PIKA_CACHE_NONE != g_pika_conf->cache_mode()) {
    g_pika_server->ClearCacheDbAsync(db);
  }
}

void FlushallCmd::FlushAllWithoutLock() {
  for (const auto& db_item : g_pika_server->GetDB()) {
    std::shared_ptr<DB> db = db_item.second;
    DBInfo p_info(db->GetDBName());
    if (g_pika_rm->GetSyncMasterDBs().find(p_info) == g_pika_rm->GetSyncMasterDBs().end()) {
      res_.SetRes(CmdRes::kErrOther, "DB not found");
      return;
    }
    DoWithoutLock(db);
  }
  if (res_.ok()) {
    res_.SetRes(CmdRes::kOk);
  }
}

void FlushallCmd::DoWithoutLock(std::shared_ptr<DB> db) {
  if (!db) {
    LOG(INFO) << "Flushall, but DB not found";
  } else {
    db->FlushDBWithoutLock();
    DoUpdateCache(db);
  }
}

void FlushdbCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameFlushdb);
    return;
  }
  if (argv_.size() == 1) {
    db_name_ = "all";
  } else {
    LOG(WARNING) << "not supported to flushdb with specific type in Floyd";
    res_.SetRes(CmdRes::kInvalidParameter, "not supported to flushdb with specific type in Floyd");
  }
}

void FlushdbCmd::Do() {
  if (!db_) {
    res_.SetRes(CmdRes::kInvalidDB);
  } else {
    if (db_->IsKeyScaning()) {
      res_.SetRes(CmdRes::kErrOther, "The keyscan operation is executing, Try again later");
    } else {
      std::lock_guard s_prw(g_pika_rm->GetDBLock());
      std::lock_guard l_prw(db_->GetDBLock());
      FlushAllDBsWithoutLock();
      res_.SetRes(CmdRes::kOk);
    }
  }
}

void FlushdbCmd::DoThroughDB() {
  Do();
}

void FlushdbCmd::DoUpdateCache() {
  // clear cache
  if (g_pika_conf->cache_mode() != PIKA_CACHE_NONE) {
    g_pika_server->ClearCacheDbAsync(db_);
  }
}

void FlushdbCmd::FlushAllDBsWithoutLock() {
  DBInfo p_info(db_->GetDBName());
  if (g_pika_rm->GetSyncMasterDBs().find(p_info) == g_pika_rm->GetSyncMasterDBs().end()) {
    res_.SetRes(CmdRes::kErrOther, "DB not found");
    return;
  }
  DoWithoutLock();
}

void FlushdbCmd::DoWithoutLock() {
  if (!db_) {
    LOG(INFO) << "Flushdb, but DB not found";
  } else {
    if (db_name_ == "all") {
      db_->FlushDBWithoutLock();
    } else {
      //Floyd does not support flushdb by type
      LOG(ERROR) << "cannot flushdb by type in floyd";
    }
  }
}

void ClientCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameClient);
    return;
  }

  if ((strcasecmp(argv_[1].data(), "getname") == 0) && argv_.size() == 2) {
    operation_ = argv_[1];
    return;
  }

  if ((strcasecmp(argv_[1].data(), "setname") == 0) && argv_.size() != 3) {
    res_.SetRes(CmdRes::kErrOther,
                "Unknown subcommand or wrong number of arguments for "
                "'SETNAME'., try CLIENT SETNAME <name>");
    return;
  }
  if ((strcasecmp(argv_[1].data(), "setname") == 0) && argv_.size() == 3) {
    operation_ = argv_[1];
    return;
  }

  if ((strcasecmp(argv_[1].data(), "list") == 0) && argv_.size() == 2) {
    // nothing
  } else if ((strcasecmp(argv_[1].data(), "list") == 0) && argv_.size() == 5) {
    if ((strcasecmp(argv_[2].data(), "order") == 0) && (strcasecmp(argv_[3].data(), "by") == 0)) {
      info_ = argv_[4];
    } else {
      res_.SetRes(CmdRes::kErrOther, "Syntax error, try CLIENT (LIST [order by [addr|idle])");
      return;
    }
  } else if ((strcasecmp(argv_[1].data(), "kill") == 0) && argv_.size() == 3) {
    info_ = argv_[2];
  } else {
    res_.SetRes(CmdRes::kErrOther, "Syntax error, try CLIENT (LIST [order by [addr|idle]| KILL ip:port)");
    return;
  }
  operation_ = argv_[1];
}

void ClientCmd::Do() {
  std::shared_ptr<net::NetConn> conn = GetConn();
  if (!conn) {
    res_.SetRes(CmdRes::kErrOther, kCmdNameClient);
    return;
  }

  if ((strcasecmp(operation_.data(), "getname") == 0) && argv_.size() == 2) {
    res_.AppendString(conn->name());
    return;
  }

  if ((strcasecmp(operation_.data(), "setname") == 0) && argv_.size() == 3) {
    std::string name = argv_[2];
    conn->set_name(name);
    res_.SetRes(CmdRes::kOk);
    return;
  }

  if (strcasecmp(operation_.data(), "list") == 0) {
    struct timeval now;
    gettimeofday(&now, nullptr);
    std::vector<ClientInfo> clients;
    g_pika_server->ClientList(&clients);
    auto iter = clients.begin();
    std::string reply;
    char buf[128];
    if (strcasecmp(info_.data(), "addr") == 0) {
      std::sort(clients.begin(), clients.end(), AddrCompare);
    } else if (strcasecmp(info_.data(), "idle") == 0) {
      std::sort(clients.begin(), clients.end(), IdleCompare);
    }
    while (iter != clients.end()) {
      snprintf(buf, sizeof(buf), "addr=%s fd=%d idle=%ld\n", iter->ip_port.c_str(), iter->fd,
               iter->last_interaction == 0 ? 0 : now.tv_sec - iter->last_interaction);  // NOLINT
      reply.append(buf);
      iter++;
    }
    res_.AppendString(reply);
  } else if ((strcasecmp(operation_.data(), "kill") == 0) && (strcasecmp(info_.data(), "all") == 0)) {
    g_pika_server->ClientKillAll();
    res_.SetRes(CmdRes::kOk);
  } else if (g_pika_server->ClientKill(info_) == 1) {
    res_.SetRes(CmdRes::kOk);
  } else {
    res_.SetRes(CmdRes::kErrOther, "No such client");
  }
}

void ShutdownCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameShutdown);
    return;
  }

  // For now, only shutdown need check local
  if (IsLocal()) {
    std::shared_ptr<net::NetConn> conn = GetConn();
    if (conn) {
      if (conn->ip_port().find("127.0.0.1") == std::string::npos &&
          conn->ip_port().find(g_pika_server->host()) == std::string::npos) {
        LOG(WARNING) << "\'shutdown\' should be localhost"
                     << " command from " << conn->ip_port();
        res_.SetRes(CmdRes::kErrOther, kCmdNameShutdown + " should be localhost");
      }
    } else {
      LOG(WARNING) << name_ << " weak ptr is empty";
      res_.SetRes(CmdRes::kErrOther, kCmdNameShutdown);
      return;
    }
  }
}
// no return
void ShutdownCmd::Do() {
  DLOG(WARNING) << "handle \'shutdown\'";
  db_->DBUnlockShared();
  g_pika_server->Exit();
  db_->DBLockShared();
  res_.SetRes(CmdRes::kNone);
}

const std::string InfoCmd::kInfoSection = "info";
const std::string InfoCmd::kAllSection = "all";
const std::string InfoCmd::kServerSection = "server";
const std::string InfoCmd::kClientsSection = "clients";
const std::string InfoCmd::kStatsSection = "stats";
const std::string InfoCmd::kExecCountSection = "command_exec_count";
const std::string InfoCmd::kCPUSection = "cpu";
const std::string InfoCmd::kReplicationSection = "replication";
const std::string InfoCmd::kKeyspaceSection = "keyspace";
const std::string InfoCmd::kDataSection = "data";
const std::string InfoCmd::kRocksDBSection = "rocksdb";
const std::string InfoCmd::kDebugSection = "debug";
const std::string InfoCmd::kCommandStatsSection = "commandstats";
const std::string InfoCmd::kCacheSection = "cache";

void InfoCmd::DoInitial() {
  size_t argc = argv_.size();
  if (argc > 4) {
    res_.SetRes(CmdRes::kSyntaxErr);
    return;
  }
  if (argc == 1) {
    info_section_ = kInfo;
    return;
  }  // then the agc is 2 or 3

  if (strcasecmp(argv_[1].data(), kAllSection.data()) == 0) {
    info_section_ = kInfoAll;
    keyspace_scan_dbs_ = g_pika_server->GetAllDBName();
  } else if (strcasecmp(argv_[1].data(), kServerSection.data()) == 0) {
    info_section_ = kInfoServer;
  } else if (strcasecmp(argv_[1].data(), kClientsSection.data()) == 0) {
    info_section_ = kInfoClients;
  } else if (strcasecmp(argv_[1].data(), kStatsSection.data()) == 0) {
    info_section_ = kInfoStats;
  } else if (strcasecmp(argv_[1].data(), kExecCountSection.data()) == 0) {
    info_section_ = kInfoExecCount;
  } else if (strcasecmp(argv_[1].data(), kCPUSection.data()) == 0) {
    info_section_ = kInfoCPU;
  } else if (strcasecmp(argv_[1].data(), kReplicationSection.data()) == 0) {
    info_section_ = kInfoReplication;
  } else if (strcasecmp(argv_[1].data(), kKeyspaceSection.data()) == 0) {
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
      std::vector<std::string> dbs;
      pstd::StringSplit(argv_[3], COMMA, dbs);
      for (const auto& db : dbs) {
        if (!g_pika_server->IsDBExist(db)) {
          res_.SetRes(CmdRes::kInvalidDB, db);
          return;
        } else {
          keyspace_scan_dbs_.insert(db);
        }
      }
    } else {
      keyspace_scan_dbs_ = g_pika_server->GetAllDBName();
    }
    LogCommand();
    return;
  } else if (strcasecmp(argv_[1].data(), kDataSection.data()) == 0) {
    info_section_ = kInfoData;
  } else if (strcasecmp(argv_[1].data(), kRocksDBSection.data()) == 0) {
    info_section_ = kInfoRocksDB;
  } else if (strcasecmp(argv_[1].data(), kDebugSection.data()) == 0) {
    info_section_ = kInfoDebug;
  } else if (strcasecmp(argv_[1].data(), kCommandStatsSection.data()) == 0) {
    info_section_ = kInfoCommandStats;
  } else if (strcasecmp(argv_[1].data(), kCacheSection.data()) == 0) {
    info_section_ = kInfoCache;
  } else {
    info_section_ = kInfoErr;
  }
  if (argc != 2) {
    res_.SetRes(CmdRes::kSyntaxErr);
  }
}

void InfoCmd::Do() {
  std::string info;
  switch (info_section_) {
    case kInfo:
      InfoServer(info);
      info.append("\r\n");
      InfoData(info);
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
      InfoClients(info);
      info.append("\r\n");
      InfoStats(info);
      info.append("\r\n");
      InfoExecCount(info);
      info.append("\r\n");
      InfoCommandStats(info);
      info.append("\r\n");
      InfoCache(info, db_);
      info.append("\r\n");
      InfoCPU(info);
      info.append("\r\n");
      InfoReplication(info);
      info.append("\r\n");
      InfoKeyspace(info);
      info.append("\r\n");
      InfoRocksDB(info);
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
    case kInfoData:
      InfoData(info);
      break;
    case kInfoRocksDB:
      InfoRocksDB(info);
      break;
    case kInfoDebug:
      InfoDebug(info);
      break;
    case kInfoCommandStats:
      InfoCommandStats(info);
      break;
    case kInfoCache:
      InfoCache(info, db_);
      break;
    default:
      // kInfoErr is nothing
      break;
  }

  res_.AppendString(info);
}

void InfoCmd::InfoServer(std::string& info) {
  static struct utsname host_info;
  static bool host_info_valid = false;
  if (!host_info_valid) {
    uname(&host_info);
    host_info_valid = true;
  }

  time_t current_time_s = time(nullptr);
  std::stringstream tmp_stream;
  char version[32];
  snprintf(version, sizeof(version), "%d.%d.%d", PIKA_MAJOR, PIKA_MINOR, PIKA_PATCH);
  tmp_stream << "# Server\r\n";
  tmp_stream << "pika_version:" << version << "\r\n";
  tmp_stream << pika_build_git_sha << "\r\n";
  tmp_stream << "pika_build_compile_date: " << pika_build_compile_date << "\r\n";
  tmp_stream << "os:" << host_info.sysname << " " << host_info.release << " " << host_info.machine << "\r\n";
  tmp_stream << "arch_bits:" << (reinterpret_cast<char*>(&host_info.machine) + strlen(host_info.machine) - 2) << "\r\n";
  tmp_stream << "process_id:" << getpid() << "\r\n";
  tmp_stream << "tcp_port:" << g_pika_conf->port() << "\r\n";
  tmp_stream << "thread_num:" << g_pika_conf->thread_num() << "\r\n";
  tmp_stream << "sync_thread_num:" << g_pika_conf->sync_thread_num() << "\r\n";
  tmp_stream << "uptime_in_seconds:" << (current_time_s - g_pika_server->start_time_s()) << "\r\n";
  tmp_stream << "uptime_in_days:" << (current_time_s / (24 * 3600) - g_pika_server->start_time_s() / (24 * 3600) + 1)
             << "\r\n";
  tmp_stream << "config_file:" << g_pika_conf->conf_path() << "\r\n";
  tmp_stream << "server_id:" << g_pika_conf->server_id() << "\r\n";
  tmp_stream << "run_id:" << g_pika_conf->run_id() << "\r\n";

  info.append(tmp_stream.str());
}

void InfoCmd::InfoClients(std::string& info) {
  std::stringstream tmp_stream;
  tmp_stream << "# Clients"
             << "\r\n";
  tmp_stream << "connected_clients:" << g_pika_server->ClientList() << "\r\n";

  info.append(tmp_stream.str());
}

void InfoCmd::InfoStats(std::string& info) {
  std::stringstream tmp_stream;
  tmp_stream << "# Stats"
             << "\r\n";
  tmp_stream << "total_connections_received:" << g_pika_server->accumulative_connections() << "\r\n";
  tmp_stream << "instantaneous_ops_per_sec:" << g_pika_server->ServerCurrentQps() << "\r\n";
  tmp_stream << "total_commands_processed:" << g_pika_server->ServerQueryNum() << "\r\n";

  // Network stats
  tmp_stream << "total_net_input_bytes:" << g_pika_server->NetInputBytes() + g_pika_server->NetReplInputBytes()
             << "\r\n";
  tmp_stream << "total_net_output_bytes:" << g_pika_server->NetOutputBytes() + g_pika_server->NetReplOutputBytes()
             << "\r\n";
  tmp_stream << "total_net_repl_input_bytes:" << g_pika_server->NetReplInputBytes() << "\r\n";
  tmp_stream << "total_net_repl_output_bytes:" << g_pika_server->NetReplOutputBytes() << "\r\n";
  tmp_stream << "instantaneous_input_kbps:" << g_pika_server->InstantaneousInputKbps() << "\r\n";
  tmp_stream << "instantaneous_output_kbps:" << g_pika_server->InstantaneousOutputKbps() << "\r\n";
  tmp_stream << "instantaneous_input_repl_kbps:" << g_pika_server->InstantaneousInputReplKbps() << "\r\n";
  tmp_stream << "instantaneous_output_repl_kbps:" << g_pika_server->InstantaneousOutputReplKbps() << "\r\n";

  tmp_stream << "is_bgsaving:" << (g_pika_server->IsBgSaving() ? "Yes" : "No") << "\r\n";
  tmp_stream << "is_scaning_keyspace:" << (g_pika_server->IsKeyScaning() ? "Yes" : "No") << "\r\n";
  tmp_stream << "is_compact:" << (g_pika_server->IsCompacting() ? "Yes" : "No") << "\r\n";
  tmp_stream << "compact_cron:" << g_pika_conf->compact_cron() << "\r\n";
  tmp_stream << "compact_interval:" << g_pika_conf->compact_interval() << "\r\n";
  time_t current_time_s = time(nullptr);
  PikaServer::BGSlotsReload bgslotsreload_info = g_pika_server->bgslots_reload();
  bool is_reloading = g_pika_server->GetSlotsreloading();
  tmp_stream << "is_slots_reloading:" << (is_reloading ? "Yes, " : "No, ") << bgslotsreload_info.s_start_time << ", "
             << (is_reloading ? (current_time_s - bgslotsreload_info.start_time)
                              : (bgslotsreload_info.end_time - bgslotsreload_info.start_time))
             << "\r\n";
  PikaServer::BGSlotsCleanup bgslotscleanup_info = g_pika_server->bgslots_cleanup();
  bool is_cleaningup = g_pika_server->GetSlotscleaningup();
  tmp_stream << "is_slots_cleaningup:" << (is_cleaningup ? "Yes, " : "No, ") << bgslotscleanup_info.s_start_time << ", "
             << (is_cleaningup ? (current_time_s - bgslotscleanup_info.start_time)
                               : (bgslotscleanup_info.end_time - bgslotscleanup_info.start_time))
             << "\r\n";
  bool is_migrating = g_pika_server->pika_migrate_thread_->IsMigrating();
  time_t start_migration_time = g_pika_server->pika_migrate_thread_->GetStartTime();
  time_t end_migration_time = g_pika_server->pika_migrate_thread_->GetEndTime();
  std::string start_migration_time_str = g_pika_server->pika_migrate_thread_->GetStartTimeStr();
  tmp_stream << "is_slots_migrating:" << (is_migrating ? "Yes, " : "No, ") << start_migration_time_str << ", "
             << (is_migrating ? (current_time_s - start_migration_time) : (end_migration_time - start_migration_time))
             << "\r\n";
  tmp_stream << "slow_logs_count:" << g_pika_server->SlowlogCount() << "\r\n";
  info.append(tmp_stream.str());
}

void InfoCmd::InfoExecCount(std::string& info) {
  std::stringstream tmp_stream;
  tmp_stream << "# Command_Exec_Count\r\n";

  std::unordered_map<std::string, uint64_t> command_exec_count_db = g_pika_server->ServerExecCountDB();
  for (const auto& item : command_exec_count_db) {
    if (item.second == 0) {
      continue;
    }
    tmp_stream << item.first << ":" << item.second << "\r\n";
  }
  info.append(tmp_stream.str());
}

void InfoCmd::InfoCPU(std::string& info) {
  struct rusage self_ru;
  struct rusage c_ru;
  getrusage(RUSAGE_SELF, &self_ru);
  getrusage(RUSAGE_CHILDREN, &c_ru);
  std::stringstream tmp_stream;
  tmp_stream << "# CPU"
             << "\r\n";
  tmp_stream << "used_cpu_sys:" << std::setiosflags(std::ios::fixed) << std::setprecision(2)
             << static_cast<float>(self_ru.ru_stime.tv_sec) + static_cast<float>(self_ru.ru_stime.tv_usec) / 1000000
             << "\r\n";
  tmp_stream << "used_cpu_user:" << std::setiosflags(std::ios::fixed) << std::setprecision(2)
             << static_cast<float>(self_ru.ru_utime.tv_sec) + static_cast<float>(self_ru.ru_utime.tv_usec) / 1000000
             << "\r\n";
  tmp_stream << "used_cpu_sys_children:" << std::setiosflags(std::ios::fixed) << std::setprecision(2)
             << static_cast<float>(c_ru.ru_stime.tv_sec) + static_cast<float>(c_ru.ru_stime.tv_usec) / 1000000
             << "\r\n";
  tmp_stream << "used_cpu_user_children:" << std::setiosflags(std::ios::fixed) << std::setprecision(2)
             << static_cast<float>(c_ru.ru_utime.tv_sec) + static_cast<float>(c_ru.ru_utime.tv_usec) / 1000000
             << "\r\n";
  info.append(tmp_stream.str());
}

void InfoCmd::InfoReplication(std::string& info) {
  int host_role = g_pika_server->role();
  std::stringstream tmp_stream;
  std::stringstream out_of_sync;
  std::stringstream repl_connect_status;
  bool all_db_sync = true;
  std::shared_lock db_rwl(g_pika_server->dbs_rw_);
  for (const auto& db_item : g_pika_server->GetDB()) {
    std::shared_ptr<SyncSlaveDB> slave_db =
        g_pika_rm->GetSyncSlaveDBByName(DBInfo(db_item.second->GetDBName()));
    if (!slave_db) {
      out_of_sync << "(" << db_item.first << ": InternalError)";
      continue;
    }
    repl_connect_status << db_item.first << ":";
    if (slave_db->State() != ReplState::kConnected) {
      all_db_sync = false;
      out_of_sync << "(" << db_item.first << ":";
      if (slave_db->State() == ReplState::kNoConnect) {
        out_of_sync << "NoConnect)";
        repl_connect_status << "no_connect";
      } else if (slave_db->State() == ReplState::kWaitDBSync) {
        out_of_sync << "WaitDBSync)";
        repl_connect_status << "syncing_full";
      } else if (slave_db->State() == ReplState::kError) {
        out_of_sync << "Error)";
        repl_connect_status << "error";
      } else if (slave_db->State() == ReplState::kWaitReply) {
        out_of_sync << "kWaitReply)";
        repl_connect_status << "connecting";
      } else if (slave_db->State() == ReplState::kTryConnect) {
        out_of_sync << "kTryConnect)";
        repl_connect_status << "try_to_incr_sync";
      } else if (slave_db->State() == ReplState::kTryDBSync) {
        out_of_sync << "kTryDBSync)";
        repl_connect_status << "try_to_full_sync";
      } else if (slave_db->State() == ReplState::kDBNoConnect) {
        out_of_sync << "kDBNoConnect)";
        repl_connect_status << "no_connect";
      } else {
        out_of_sync << "Other)";
        repl_connect_status << "error";
      }
    } else { //slave_db->State() equal to kConnected
        repl_connect_status << "connected";
    }
    repl_connect_status << "\r\n";
  }

  tmp_stream << "# Replication(";
  switch (host_role) {
    case PIKA_ROLE_SINGLE:
    case PIKA_ROLE_MASTER:
      tmp_stream << "MASTER)\r\nrole:master\r\n";
      break;
    case PIKA_ROLE_SLAVE:
      tmp_stream << "SLAVE)\r\nrole:slave\r\n";
      break;
    case PIKA_ROLE_MASTER | PIKA_ROLE_SLAVE:
      tmp_stream << "Master && SLAVE)\r\nrole:master&&slave\r\n";
      break;
    default:
      info.append("ERR: server role is error\r\n");
      return;
  }
  tmp_stream << "ReplicationID:" << g_pika_conf->replication_id() << "\r\n";
  std::string slaves_list_str;
  switch (host_role) {
    case PIKA_ROLE_SLAVE:
      tmp_stream << "master_host:" << g_pika_server->master_ip() << "\r\n";
      tmp_stream << "master_port:" << g_pika_server->master_port() << "\r\n";
      tmp_stream << "master_link_status:"
                 << (((g_pika_server->repl_state() == PIKA_REPL_META_SYNC_DONE) && all_db_sync) ? "up" : "down")
                 << "\r\n";
      tmp_stream << "repl_connect_status:\r\n"  << repl_connect_status.str();
      tmp_stream << "slave_priority:" << g_pika_conf->slave_priority() << "\r\n";
      tmp_stream << "slave_read_only:" << g_pika_conf->slave_read_only() << "\r\n";
      if (!all_db_sync) {
        tmp_stream << "db_repl_state:" << out_of_sync.str() << "\r\n";
      }
      break;
    case PIKA_ROLE_MASTER | PIKA_ROLE_SLAVE:
      tmp_stream << "master_host:" << g_pika_server->master_ip() << "\r\n";
      tmp_stream << "master_port:" << g_pika_server->master_port() << "\r\n";
      tmp_stream << "master_link_status:"
                 << (((g_pika_server->repl_state() == PIKA_REPL_META_SYNC_DONE) && all_db_sync) ? "up" : "down")
                 << "\r\n";
      tmp_stream << "repl_connect_status:\r\n"  << repl_connect_status.str();
      tmp_stream << "slave_read_only:" << g_pika_conf->slave_read_only() << "\r\n";
      if (!all_db_sync) {
        tmp_stream << "db_repl_state:" << out_of_sync.str() << "\r\n";
      }
    case PIKA_ROLE_SINGLE:
    case PIKA_ROLE_MASTER:
      tmp_stream << "connected_slaves:" << g_pika_server->GetSlaveListString(slaves_list_str) << "\r\n"
                 << slaves_list_str;
  }

  Status s;
  uint32_t filenum = 0;
  uint64_t offset = 0;
  std::string safety_purge;
  std::shared_ptr<SyncMasterDB> master_db = nullptr;
  for (const auto& t_item : g_pika_server->dbs_) {
    std::shared_lock db_rwl(t_item.second->dbs_rw_);
    std::string db_name = t_item.first;
    master_db = g_pika_rm->GetSyncMasterDBByName(DBInfo(db_name));
    if (!master_db) {
      LOG(WARNING) << "Sync Master DB: " << db_name << " NotFound";
      continue;
    }
    master_db->Logger()->GetProducerStatus(&filenum, &offset);
    tmp_stream << db_name << ":binlog_offset=" << filenum << " " << offset;
    s = master_db->GetSafetyPurgeBinlog(&safety_purge);
    tmp_stream << ",safety_purge=" << (s.ok() ? safety_purge : "error") << "\r\n";
  }

  info.append(tmp_stream.str());
}

void InfoCmd::InfoKeyspace(std::string& info) {
  if (off_) {
    g_pika_server->DoSameThingSpecificDB(keyspace_scan_dbs_, {TaskType::kStopKeyScan});
    info.append("OK\r\n");
    return;
  }

  std::string db_name;
  KeyScanInfo key_scan_info;
  int32_t duration = 0;
  std::vector<storage::KeyInfo> key_infos;
  std::stringstream tmp_stream;
  tmp_stream << "# Keyspace"
             << "\r\n";
  if (argv_.size() > 1 && strcasecmp(argv_[1].data(), kAllSection.data()) == 0) {
    tmp_stream << "# Start async statistics\r\n";
  } else if (argv_.size() == 3 && strcasecmp(argv_[1].data(), kKeyspaceSection.data()) == 0) {
    tmp_stream << "# Start async statistics\r\n";    
  } else {
    tmp_stream << "# Use \"info keyspace 1\" to do async statistics\r\n";  
  }
  std::shared_lock rwl(g_pika_server->dbs_rw_);
  for (const auto& db_item : g_pika_server->dbs_) {
    if (keyspace_scan_dbs_.find(db_item.first) != keyspace_scan_dbs_.end()) {
      db_name = db_item.second->GetDBName();
      key_scan_info = db_item.second->GetKeyScanInfo();
      key_infos = key_scan_info.key_infos;
      duration = key_scan_info.duration;
      if (key_infos.size() != 5) {
        info.append("info keyspace error\r\n");
        return;
      }
      tmp_stream << "# Time:" << key_scan_info.s_start_time << "\r\n";
      if (duration == -2) {
        tmp_stream << "# Duration: "
                   << "In Waiting\r\n";
      } else if (duration == -1) {
        tmp_stream << "# Duration: "
                   << "In Processing\r\n";
      } else if (duration >= 0) {
        tmp_stream << "# Duration: " << std::to_string(duration) + "s"
                   << "\r\n";
      }
      
      tmp_stream << db_name << " Strings_keys=" << key_infos[0].keys << ", expires=" << key_infos[0].expires
                 << ", invalid_keys=" << key_infos[0].invaild_keys << "\r\n";
      tmp_stream << db_name << " Hashes_keys=" << key_infos[1].keys << ", expires=" << key_infos[1].expires
                 << ", invalid_keys=" << key_infos[1].invaild_keys << "\r\n";
      tmp_stream << db_name << " Lists_keys=" << key_infos[2].keys << ", expires=" << key_infos[2].expires
                 << ", invalid_keys=" << key_infos[2].invaild_keys << "\r\n";
      tmp_stream << db_name << " Zsets_keys=" << key_infos[3].keys << ", expires=" << key_infos[3].expires
                 << ", invalid_keys=" << key_infos[3].invaild_keys << "\r\n";
      tmp_stream << db_name << " Sets_keys=" << key_infos[4].keys << ", expires=" << key_infos[4].expires
                 << ", invalid_keys=" << key_infos[4].invaild_keys << "\r\n\r\n";
    }
  }
  info.append(tmp_stream.str());
  if (rescan_) {
    g_pika_server->DoSameThingSpecificDB(keyspace_scan_dbs_, {TaskType::kStartKeyScan});
  }
}

void InfoCmd::InfoData(std::string& info) {
  std::stringstream tmp_stream;
  std::stringstream db_fatal_msg_stream;

  uint64_t db_size = g_pika_server->GetDBSize();
  uint64_t log_size = g_pika_server->GetLogSize();

  tmp_stream << "# Data"
             << "\r\n";
  tmp_stream << "db_size:" << db_size << "\r\n";
  tmp_stream << "db_size_human:" << (db_size >> 20) << "M\r\n";
  tmp_stream << "log_size:" << log_size << "\r\n";
  tmp_stream << "log_size_human:" << (log_size >> 20) << "M\r\n";
  tmp_stream << "compression:" << g_pika_conf->compression() << "\r\n";

  // rocksdb related memory usage
  std::map<int, uint64_t> background_errors;
  uint64_t total_background_errors = 0;
  uint64_t total_memtable_usage = 0;
  uint64_t total_table_reader_usage = 0;
  uint64_t memtable_usage = 0;
  uint64_t table_reader_usage = 0;
  std::shared_lock db_rwl(g_pika_server->dbs_rw_);
  for (const auto& db_item : g_pika_server->dbs_) {
    if (!db_item.second) {
      continue;
    }
    background_errors.clear();
    memtable_usage = table_reader_usage = 0;
    db_item.second->DBLockShared();
    db_item.second->storage()->GetUsage(storage::PROPERTY_TYPE_ROCKSDB_CUR_SIZE_ALL_MEM_TABLES, &memtable_usage);
    db_item.second->storage()->GetUsage(storage::PROPERTY_TYPE_ROCKSDB_ESTIMATE_TABLE_READER_MEM, &table_reader_usage);
    db_item.second->storage()->GetUsage(storage::PROPERTY_TYPE_ROCKSDB_BACKGROUND_ERRORS, &background_errors);
    db_item.second->DBUnlockShared();
    total_memtable_usage += memtable_usage;
    total_table_reader_usage += table_reader_usage;
    for (const auto& item : background_errors) {
      if (item.second != 0) {
        db_fatal_msg_stream << (total_background_errors != 0 ? "," : "");
        db_fatal_msg_stream << db_item.first << "/" << item.first;
        total_background_errors += item.second;
      }
    }
  }

  tmp_stream << "used_memory:" << (total_memtable_usage + total_table_reader_usage) << "\r\n";
  tmp_stream << "used_memory_human:" << ((total_memtable_usage + total_table_reader_usage) >> 20) << "M\r\n";
  tmp_stream << "db_memtable_usage:" << total_memtable_usage << "\r\n";
  tmp_stream << "db_tablereader_usage:" << total_table_reader_usage << "\r\n";
  tmp_stream << "db_fatal:" << (total_background_errors != 0 ? "1" : "0") << "\r\n";
  tmp_stream << "db_fatal_msg:" << (total_background_errors != 0 ? db_fatal_msg_stream.str() : "nullptr") << "\r\n";

  info.append(tmp_stream.str());
}

void InfoCmd::InfoRocksDB(std::string& info) {
  std::stringstream tmp_stream;

  tmp_stream << "# RocksDB"
             << "\r\n";

  std::shared_lock db_rwl(g_pika_server->dbs_rw_);
  for (const auto& db_item : g_pika_server->dbs_) {
    if (!db_item.second) {
      continue;
    }
    std::string rocksdb_info;
    db_item.second->DBLockShared();
    db_item.second->storage()->GetRocksDBInfo(rocksdb_info);
    db_item.second->DBUnlockShared();
    tmp_stream << rocksdb_info;
  }
  info.append(tmp_stream.str());
}

void InfoCmd::InfoDebug(std::string& info) {
  std::stringstream tmp_stream;
  tmp_stream << "# Synchronization Status"
             << "\r\n";

  info.append(tmp_stream.str());
  g_pika_rm->RmStatus(&info);

  tmp_stream.str(std::string());
  tmp_stream << "# Running Status "
             << "\r\n";

  info.append(tmp_stream.str());
  g_pika_server->ServerStatus(&info);
}

void InfoCmd::InfoCommandStats(std::string& info) {
  std::stringstream tmp_stream;
  tmp_stream.precision(2);
  tmp_stream.setf(std::ios::fixed);
  tmp_stream << "# Commandstats" << "\r\n";
  auto cmdstat_map = g_pika_cmd_table_manager->GetCommandStatMap();
  for (auto iter : *cmdstat_map) {
    if (iter.second.cmd_count != 0) {
      tmp_stream << iter.first << ":"
                 << "calls=" << iter.second.cmd_count << ", usec="
                 << MethodofTotalTimeCalculation(iter.second.cmd_time_consuming)
                 << ", usec_per_call=";
      if (!iter.second.cmd_time_consuming) {
        tmp_stream << 0 << "\r\n";
      } else {
        tmp_stream << MethodofCommandStatistics(iter.second.cmd_time_consuming, iter.second.cmd_count)
                   << "\r\n";
      }
    }
  }
  info.append(tmp_stream.str());
}

void InfoCmd::InfoCache(std::string& info, std::shared_ptr<DB> db) {
  std::stringstream tmp_stream;
  tmp_stream << "# Cache" << "\r\n";
  if (PIKA_CACHE_NONE == g_pika_conf->cache_mode()) {
    tmp_stream << "cache_status:Disable" << "\r\n";
  } else {
    auto cache_info = db->GetCacheInfo();
    tmp_stream << "cache_status:" << CacheStatusToString(cache_info.status) << "\r\n";
    tmp_stream << "cache_db_num:" << cache_info.cache_num << "\r\n";
    tmp_stream << "cache_keys:" << cache_info.keys_num << "\r\n";
    tmp_stream << "cache_memory:" << cache_info.used_memory << "\r\n";
    tmp_stream << "cache_memory_human:" << (cache_info.used_memory >> 20) << "M\r\n";
    tmp_stream << "hits:" << cache_info.hits << "\r\n";
    tmp_stream << "all_cmds:" << cache_info.hits + cache_info.misses << "\r\n";
    tmp_stream << "hits_per_sec:" << cache_info.hits_per_sec << "\r\n";
    tmp_stream << "read_cmd_per_sec:" << cache_info.read_cmd_per_sec << "\r\n";
    tmp_stream << "hitratio_per_sec:" << std::setprecision(4) << cache_info.hitratio_per_sec << "%" << "\r\n";
    tmp_stream << "hitratio_all:" << std::setprecision(4) << cache_info.hitratio_all << "%" << "\r\n";
    tmp_stream << "load_keys_per_sec:" << cache_info.load_keys_per_sec << "\r\n";
    tmp_stream << "waitting_load_keys_num:" << cache_info.waitting_load_keys_num << "\r\n";
  }
  info.append(tmp_stream.str());
}

std::string InfoCmd::CacheStatusToString(int status) {
  switch (status) {
    case PIKA_CACHE_STATUS_NONE:
      return std::string("None");
    case PIKA_CACHE_STATUS_OK:
      return std::string("Ok");
    case PIKA_CACHE_STATUS_INIT:
      return std::string("Init");
    case PIKA_CACHE_STATUS_RESET:
      return std::string("Reset");
    case PIKA_CACHE_STATUS_DESTROY:
      return std::string("Destroy");
    case PIKA_CACHE_STATUS_CLEAR:
      return std::string("Clear");
    default:
      return std::string("Unknown");
  }
}

void ConfigCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameConfig);
    return;
  }
  size_t argc = argv_.size();
  if (strcasecmp(argv_[1].data(), "get") == 0) {
    if (argc != 3) {
      res_.SetRes(CmdRes::kErrOther, "Wrong number of arguments for CONFIG get");
      return;
    }
  } else if (strcasecmp(argv_[1].data(), "set") == 0) {
    if (argc == 3 && argv_[2] != "*") {
      res_.SetRes(CmdRes::kErrOther, "Wrong number of arguments for CONFIG set");
      return;
    } else if (argc != 4 && argc != 3) {
      res_.SetRes(CmdRes::kErrOther, "Wrong number of arguments for CONFIG set");
      return;
    }
  } else if (strcasecmp(argv_[1].data(), "rewrite") == 0) {
    if (argc != 2) {
      res_.SetRes(CmdRes::kErrOther, "Wrong number of arguments for CONFIG rewrite");
      return;
    }
  } else if (strcasecmp(argv_[1].data(), "resetstat") == 0) {
    if (argc != 2) {
      res_.SetRes(CmdRes::kErrOther, "Wrong number of arguments for CONFIG resetstat");
      return;
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, "CONFIG subcommand must be one of GET, SET, RESETSTAT, REWRITE");
    return;
  }
  config_args_v_.assign(argv_.begin() + 1, argv_.end());
}

void ConfigCmd::Do() {
  std::string config_ret;
  if (strcasecmp(config_args_v_[0].data(), "get") == 0) {
    ConfigGet(config_ret);
  } else if (strcasecmp(config_args_v_[0].data(), "set") == 0) {
    ConfigSet(db_);
  } else if (strcasecmp(config_args_v_[0].data(), "rewrite") == 0) {
    ConfigRewrite(config_ret);
  } else if (strcasecmp(config_args_v_[0].data(), "resetstat") == 0) {
    ConfigResetstat(config_ret);
  } else if (strcasecmp(config_args_v_[0].data(), "rewritereplicationid") == 0) {
    ConfigRewriteReplicationID(config_ret);
  }
  res_.AppendStringRaw(config_ret);
}

static void EncodeString(std::string* dst, const std::string& value) {
  dst->append("$");
  dst->append(std::to_string(value.size()));
  dst->append(kNewLine);
  dst->append(value.data(), value.size());
  dst->append(kNewLine);
}

template <class T>
static void EncodeNumber(std::string* dst, const T v) {
  std::string vstr = std::to_string(v);
  dst->append("$");
  dst->append(std::to_string(vstr.length()));
  dst->append(kNewLine);
  dst->append(vstr);
  dst->append(kNewLine);
}

void ConfigCmd::ConfigGet(std::string& ret) {
  size_t elements = 0;
  std::string config_body;
  std::string pattern = config_args_v_[1];

  if (pstd::stringmatch(pattern.data(), "port", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "port");
    EncodeNumber(&config_body, g_pika_conf->port());
  }

  if (pstd::stringmatch(pattern.data(), "thread-num", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "thread-num");
    EncodeNumber(&config_body, g_pika_conf->thread_num());
  }

  if (pstd::stringmatch(pattern.data(), "thread-pool-size", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "thread-pool-size");
    EncodeNumber(&config_body, g_pika_conf->thread_pool_size());
  }

  if (pstd::stringmatch(pattern.data(), "slow-cmd-thread-pool-size", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "slow-cmd-thread-pool-size");
    EncodeNumber(&config_body, g_pika_conf->slow_cmd_thread_pool_size());
  }
  if (pstd::stringmatch(pattern.data(), "userblacklist", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "userblacklist");
    EncodeString(&config_body, g_pika_conf->user_blacklist_string());
  }
  if (pstd::stringmatch(pattern.data(), "slow-cmd-list", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "slow-cmd-list");
    EncodeString(&config_body, g_pika_conf->GetSlowCmd());
  }

  if (pstd::stringmatch(pattern.data(), "sync-thread-num", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "sync-thread-num");
    EncodeNumber(&config_body, g_pika_conf->sync_thread_num());
  }

  if (pstd::stringmatch(pattern.data(), "log-path", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "log-path");
    EncodeString(&config_body, g_pika_conf->log_path());
  }

  if (pstd::stringmatch(pattern.data(), "db-path", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "db-path");
    EncodeString(&config_body, g_pika_conf->db_path());
  }

  if (pstd::stringmatch(pattern.data(), "maxmemory", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "maxmemory");
    EncodeNumber(&config_body, g_pika_conf->write_buffer_size());
  }

  if (pstd::stringmatch(pattern.data(), "write-buffer-size", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "write-buffer-size");
    EncodeNumber(&config_body, g_pika_conf->write_buffer_size());
  }

  if (pstd::stringmatch(pattern.data(), "arena-block-size", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "arena-block-size");
    EncodeNumber(&config_body, g_pika_conf->arena_block_size());
  }

  if (pstd::stringmatch(pattern.data(), "max-write-buffer-num", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "max-write-buffer-num");
    EncodeNumber(&config_body, g_pika_conf->max_write_buffer_number());
  }

  if (pstd::stringmatch(pattern.data(), "timeout", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "timeout");
    EncodeNumber(&config_body, g_pika_conf->timeout());
  }

  if (pstd::stringmatch(pattern.data(), "requirepass", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "requirepass");
    EncodeString(&config_body, g_pika_conf->requirepass());
  }

  if (pstd::stringmatch(pattern.data(), "masterauth", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "masterauth");
    EncodeString(&config_body, g_pika_conf->masterauth());
  }

  if (pstd::stringmatch(pattern.data(), "userpass", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "userpass");
    EncodeString(&config_body, g_pika_conf->userpass());
  }

  if (pstd::stringmatch(pattern.data(), "instance-mode", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "instance-mode");
    EncodeString(&config_body, "classic");
  }

  if (pstd::stringmatch(pattern.data(), "databases", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "databases");
    EncodeNumber(&config_body, g_pika_conf->databases());
  }

  if (pstd::stringmatch(pattern.data(), "daemonize", 1)) {
    elements += 2;
    EncodeString(&config_body, "daemonize");
    EncodeString(&config_body, g_pika_conf->daemonize() ? "yes" : "no");
  }

  if (pstd::stringmatch(pattern.data(), "slotmigrate", 1)) {
    elements += 2;
    EncodeString(&config_body, "slotmigrate");
    EncodeString(&config_body, g_pika_conf->slotmigrate() ? "yes" : "no");
  }

  if (pstd::stringmatch(pattern.data(), "slotmigrate-thread-num", 1)!= 0) {
    elements += 2;
    EncodeString(&config_body, "slotmigrate-thread-num");
    EncodeNumber(&config_body, g_pika_conf->slotmigrate_thread_num());
  }

  if (pstd::stringmatch(pattern.data(), "thread-migrate-keys-num", 1)!= 0) {
    elements += 2;
    EncodeString(&config_body, "thread-migrate-keys-num");
    EncodeNumber(&config_body, g_pika_conf->thread_migrate_keys_num());
  }

  if (pstd::stringmatch(pattern.data(), "dump-path", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "dump-path");
    EncodeString(&config_body, g_pika_conf->bgsave_path());
  }

  if (pstd::stringmatch(pattern.data(), "dump-expire", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "dump-expire");
    EncodeNumber(&config_body, g_pika_conf->expire_dump_days());
  }

  if (pstd::stringmatch(pattern.data(), "dump-prefix", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "dump-prefix");
    EncodeString(&config_body, g_pika_conf->bgsave_prefix());
  }

  if (pstd::stringmatch(pattern.data(), "pidfile", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "pidfile");
    EncodeString(&config_body, g_pika_conf->pidfile());
  }

  if (pstd::stringmatch(pattern.data(), "maxclients", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "maxclients");
    EncodeNumber(&config_body, g_pika_conf->maxclients());
  }

  if (pstd::stringmatch(pattern.data(), "target-file-size-base", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "target-file-size-base");
    EncodeNumber(&config_body, g_pika_conf->target_file_size_base());
  }

  if (pstd::stringmatch(pattern.data(), "max-cache-statistic-keys", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "max-cache-statistic-keys");
    EncodeNumber(&config_body, g_pika_conf->max_cache_statistic_keys());
  }

  if (pstd::stringmatch(pattern.data(), "small-compaction-threshold", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "small-compaction-threshold");
    EncodeNumber(&config_body, g_pika_conf->small_compaction_threshold());
  }

  if (pstd::stringmatch(pattern.data(), "small-compaction-duration-threshold", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "small-compaction-duration-threshold");
    EncodeNumber(&config_body, g_pika_conf->small_compaction_duration_threshold());
  }

  if (pstd::stringmatch(pattern.data(), "max-background-flushes", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "max-background-flushes");
    EncodeNumber(&config_body, g_pika_conf->max_background_flushes());
  }

  if (pstd::stringmatch(pattern.data(), "max-background-compactions", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "max-background-compactions");
    EncodeNumber(&config_body, g_pika_conf->max_background_compactions());
  }

  if (pstd::stringmatch(pattern.data(), "max-background-jobs", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "max-background-jobs");
    EncodeNumber(&config_body, g_pika_conf->max_background_jobs());
  }

  if (pstd::stringmatch(pattern.data(), "max-cache-files", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "max-cache-files");
    EncodeNumber(&config_body, g_pika_conf->max_cache_files());
  }

  if (pstd::stringmatch(pattern.data(), "max-bytes-for-level-multiplier", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "max-bytes-for-level-multiplier");
    EncodeNumber(&config_body, g_pika_conf->max_bytes_for_level_multiplier());
  }

  if (pstd::stringmatch(pattern.data(), "block-size", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "block-size");
    EncodeNumber(&config_body, g_pika_conf->block_size());
  }

  if (pstd::stringmatch(pattern.data(), "block-cache", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "block-cache");
    EncodeNumber(&config_body, g_pika_conf->block_cache());
  }

  if (pstd::stringmatch(pattern.data(), "share-block-cache", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "share-block-cache");
    EncodeString(&config_body, g_pika_conf->share_block_cache() ? "yes" : "no");
  }

  if (pstd::stringmatch(pattern.data(), "enable-partitioned-index-filters", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "enable-partitioned-index-filters");
    EncodeString(&config_body, g_pika_conf->enable_partitioned_index_filters() ? "yes" : "no");
  }

  if (pstd::stringmatch(pattern.data(), "cache-index-and-filter-blocks", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "cache-index-and-filter-blocks");
    EncodeString(&config_body, g_pika_conf->cache_index_and_filter_blocks() ? "yes" : "no");
  }

  if (pstd::stringmatch(pattern.data(), "optimize-filters-for-hits", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "optimize-filters-for-hits");
    EncodeString(&config_body, g_pika_conf->optimize_filters_for_hits() ? "yes" : "no");
  }

  if (pstd::stringmatch(pattern.data(), "level-compaction-dynamic-level-bytes", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "level-compaction-dynamic-level-bytes");
    EncodeString(&config_body, g_pika_conf->level_compaction_dynamic_level_bytes() ? "yes" : "no");
  }

  if (pstd::stringmatch(pattern.data(), "expire-logs-days", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "expire-logs-days");
    EncodeNumber(&config_body, g_pika_conf->expire_logs_days());
  }

  if (pstd::stringmatch(pattern.data(), "expire-logs-nums", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "expire-logs-nums");
    EncodeNumber(&config_body, g_pika_conf->expire_logs_nums());
  }

  if (pstd::stringmatch(pattern.data(), "root-connection-num", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "root-connection-num");
    EncodeNumber(&config_body, g_pika_conf->root_connection_num());
  }

  if (pstd::stringmatch(pattern.data(), "slowlog-write-errorlog", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "slowlog-write-errorlog");
    EncodeString(&config_body, g_pika_conf->slowlog_write_errorlog() ? "yes" : "no");
  }

  if (pstd::stringmatch(pattern.data(), "slowlog-log-slower-than", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "slowlog-log-slower-than");
    EncodeNumber(&config_body, g_pika_conf->slowlog_slower_than());
  }

  if (pstd::stringmatch(pattern.data(), "slowlog-max-len", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "slowlog-max-len");
    EncodeNumber(&config_body, g_pika_conf->slowlog_max_len());
  }

  if (pstd::stringmatch(pattern.data(), "write-binlog", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "write-binlog");
    EncodeString(&config_body, g_pika_conf->write_binlog() ? "yes" : "no");
  }
  if (pstd::stringmatch(pattern.data(), "binlog-file-size", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "binlog-file-size");
    EncodeNumber(&config_body, g_pika_conf->binlog_file_size());
  }

  if (pstd::stringmatch(pattern.data(), "max-write-buffer-size", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "max-write-buffer-size");
    EncodeNumber(&config_body, g_pika_conf->max_write_buffer_size());
  }

  if (pstd::stringmatch(pattern.data(), "max-total-wal-size", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "max-total-wal-size");
    EncodeNumber(&config_body, g_pika_conf->MaxTotalWalSize());
  }

  if (pstd::stringmatch(pattern.data(), "min-write-buffer-number-to-merge", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "min-write-buffer-number-to-merge");
    EncodeNumber(&config_body, g_pika_conf->min_write_buffer_number_to_merge());
  }

  if (pstd::stringmatch(pattern.data(), "level0-stop-writes-trigger", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "level0-stop-writes-trigger");
    EncodeNumber(&config_body, g_pika_conf->level0_stop_writes_trigger());
  }

  if (pstd::stringmatch(pattern.data(), "level0-slowdown-writes-trigger", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "level0-slowdown-writes-trigger");
    EncodeNumber(&config_body, g_pika_conf->level0_slowdown_writes_trigger());
  }

  if (pstd::stringmatch(pattern.data(), "level0-file-num-compaction-trigger", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "level0-file-num-compaction-trigger");
    EncodeNumber(&config_body, g_pika_conf->level0_file_num_compaction_trigger());
  }

  if (pstd::stringmatch(pattern.data(), "max-client-response-size", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "max-client-response-size");
    EncodeNumber(&config_body, g_pika_conf->max_client_response_size());
  }

  if (pstd::stringmatch(pattern.data(), "compression", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "compression");
    EncodeString(&config_body, g_pika_conf->compression());
  }

  if (pstd::stringmatch(pattern.data(), "db-sync-path", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "db-sync-path");
    EncodeString(&config_body, g_pika_conf->db_sync_path());
  }

  if (pstd::stringmatch(pattern.data(), "db-sync-speed", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "db-sync-speed");
    EncodeNumber(&config_body, g_pika_conf->db_sync_speed());
  }

  if (pstd::stringmatch(pattern.data(), "compact-cron", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "compact-cron");
    EncodeString(&config_body, g_pika_conf->compact_cron());
  }

  if (pstd::stringmatch(pattern.data(), "compact-interval", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "compact-interval");
    EncodeString(&config_body, g_pika_conf->compact_interval());
  }
  if (pstd::stringmatch(pattern.data(), "disable_auto_compactions", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "disable_auto_compactions");
    EncodeString(&config_body, g_pika_conf->disable_auto_compactions() ? "true" : "false");
  }
  if (pstd::stringmatch(pattern.data(), "network-interface", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "network-interface");
    EncodeString(&config_body, g_pika_conf->network_interface());
  }

  if (pstd::stringmatch(pattern.data(), "slaveof", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "slaveof");
    EncodeString(&config_body, g_pika_conf->slaveof());
  }

  if (pstd::stringmatch(pattern.data(), "slave-priority", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "slave-priority");
    EncodeNumber(&config_body, g_pika_conf->slave_priority());
  }

  // fake string for redis-benchmark
  if (pstd::stringmatch(pattern.data(), "save", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "save");
    EncodeString(&config_body, "");
  }

  if (pstd::stringmatch(pattern.data(), "appendonly", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "appendonly");
    EncodeString(&config_body, "no");
  }

  if (pstd::stringmatch(pattern.data(), "sync-window-size", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "sync-window-size");
    EncodeNumber(&config_body, g_pika_conf->sync_window_size());
  }

  if (pstd::stringmatch(pattern.data(), "max-conn-rbuf-size", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "max-conn-rbuf-size");
    EncodeNumber(&config_body, g_pika_conf->max_conn_rbuf_size());
  }

  if (pstd::stringmatch(pattern.data(), "replication-num", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "replication-num");
    EncodeNumber(&config_body, g_pika_conf->replication_num());
  }
  if (pstd::stringmatch(pattern.data(), "consensus-level", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "consensus-level");
    EncodeNumber(&config_body, g_pika_conf->consensus_level());
  }

  if (pstd::stringmatch(pattern.data(), "rate-limiter-mode", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "rate-limiter-mode");
    EncodeNumber(&config_body, g_pika_conf->rate_limiter_mode());
  }

  if (pstd::stringmatch(pattern.data(), "rate-limiter-bandwidth", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "rate-limiter-bandwidth");
    EncodeNumber(&config_body, g_pika_conf->rate_limiter_bandwidth());
  }

  if (pstd::stringmatch(pattern.data(), "rate-limiter-refill-period-us", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "rate-limiter-refill-period-us");
    EncodeNumber(&config_body, g_pika_conf->rate_limiter_refill_period_us());
  }

  if (pstd::stringmatch(pattern.data(), "rate-limiter-fairness", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "rate-limiter-fairness");
    EncodeNumber(&config_body, g_pika_conf->rate_limiter_fairness());
  }

  if (pstd::stringmatch(pattern.data(), "rate-limiter-auto-tuned", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "rate-limiter-auto-tuned");
    EncodeString(&config_body, g_pika_conf->rate_limiter_auto_tuned() ? "yes" : "no");
  }

  if (pstd::stringmatch(pattern.data(), "run-id", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "run-id");
    EncodeString(&config_body, g_pika_conf->run_id());
  }

  if (pstd::stringmatch(pattern.data(), "blob-cache", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "blob-cache");
    EncodeNumber(&config_body, g_pika_conf->blob_cache());
  }

  if (pstd::stringmatch(pattern.data(), "blob-compression-type", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "blob-compression-type");
    EncodeString(&config_body, g_pika_conf->blob_compression_type());
  }

  if (pstd::stringmatch(pattern.data(), "blob-file-size", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "blob-file-size");
    EncodeNumber(&config_body, g_pika_conf->blob_file_size());
  }

  if (pstd::stringmatch(pattern.data(), "blob-garbage-collection-age-cutoff", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "blob-garbage-collection-age-cutoff");
    EncodeNumber(&config_body, g_pika_conf->blob_garbage_collection_age_cutoff());
  }

  if (pstd::stringmatch(pattern.data(), "blob-garbage-collection-force-threshold", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "blob-garbage-collection-force-threshold");
    EncodeNumber(&config_body, g_pika_conf->blob_garbage_collection_force_threshold());
  }

  if (pstd::stringmatch(pattern.data(), "blob-num-shard-bits", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "blob-num-shard-bits");
    EncodeNumber(&config_body, g_pika_conf->blob_num_shard_bits());
  }

  if (pstd::stringmatch(pattern.data(), "compression-per-level", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "compression-per-level");
    EncodeString(&config_body, g_pika_conf->compression_all_levels());
  }

  if (pstd::stringmatch(pattern.data(), "default-slot-num", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "default-slot-num");
    EncodeNumber(&config_body, g_pika_conf->default_slot_num());
  }

  if (pstd::stringmatch(pattern.data(), "enable-blob-files", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "enable-blob-files");
    EncodeString(&config_body, g_pika_conf->enable_blob_files() ? "yes" : "no");
  }

  if (pstd::stringmatch(pattern.data(), "enable-blob-garbage-collection", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "enable-blob-garbage-collection");
    EncodeString(&config_body, g_pika_conf->enable_blob_garbage_collection() ? "yes" : "no");
  }

  if (pstd::stringmatch(pattern.data(), "loglevel", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "loglevel");
    EncodeString(&config_body, g_pika_conf->log_level());
  }

  if (pstd::stringmatch(pattern.data(), "min-blob-size", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "min-blob-size");
    EncodeNumber(&config_body, g_pika_conf->min_blob_size());
  }

  if (pstd::stringmatch(pattern.data(), "pin_l0_filter_and_index_blocks_in_cache", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "pin_l0_filter_and_index_blocks_in_cache");
    EncodeString(&config_body, g_pika_conf->pin_l0_filter_and_index_blocks_in_cache() ? "yes" : "no");
  }

  if (pstd::stringmatch(pattern.data(), "slave-read-only", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "slave-read-only");
    EncodeString(&config_body, g_pika_conf->slave_read_only() ? "yes" : "no");
  }

  if (pstd::stringmatch(pattern.data(), "throttle-bytes-per-second", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "throttle-bytes-per-second");
    EncodeNumber(&config_body, g_pika_conf->throttle_bytes_per_second());
  }

  if (pstd::stringmatch(pattern.data(), "max-rsync-parallel-num", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "max-rsync-parallel-num");
    EncodeNumber(&config_body, g_pika_conf->max_rsync_parallel_num());
  }

  if (pstd::stringmatch(pattern.data(), "replication-id", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "replication-id");
    EncodeString(&config_body, g_pika_conf->replication_id());
  }


  if (pstd::stringmatch(pattern.data(), "cache-num", 1)) {
    elements += 2;
    EncodeString(&config_body, "cache-num");
    EncodeNumber(&config_body, g_pika_conf->GetCacheNum());
  }

  if (pstd::stringmatch(pattern.data(), "cache-model", 1)) {
    elements += 2;
    EncodeString(&config_body, "cache-model");
    EncodeNumber(&config_body, g_pika_conf->cache_mode());
  }

  if (pstd::stringmatch(pattern.data(), "cache-type", 1)) {
    elements += 2;
    EncodeString(&config_body, "cache-type");
    EncodeString(&config_body, g_pika_conf->scache_type());
  }

  if (pstd::stringmatch(pattern.data(), "zset-cache-start-direction", 1)) {
    elements += 2;
    EncodeString(&config_body, "zset-cache-start-direction");
    EncodeNumber(&config_body, g_pika_conf->zset_cache_start_direction());
  }

  if (pstd::stringmatch(pattern.data(), "zset-cache-field-num-per-key", 1)) {
    elements += 2;
    EncodeString(&config_body, "zset-cache-field-num-per-key");
    EncodeNumber(&config_body, g_pika_conf->zset_cache_field_num_per_key());
  }

  if (pstd::stringmatch(pattern.data(), "cache-maxmemory", 1)) {
    elements += 2;
    EncodeString(&config_body, "cache-maxmemory");
    EncodeNumber(&config_body, g_pika_conf->cache_maxmemory());
  }

  if (pstd::stringmatch(pattern.data(), "cache-maxmemory-policy", 1)) {
    elements += 2;
    EncodeString(&config_body, "cache-maxmemory-policy");
    EncodeNumber(&config_body, g_pika_conf->cache_maxmemory_policy());
  }

  if (pstd::stringmatch(pattern.data(), "cache-maxmemory-samples", 1)) {
    elements += 2;
    EncodeString(&config_body, "cache-maxmemory-samples");
    EncodeNumber(&config_body, g_pika_conf->cache_maxmemory_samples());
  }

  if (pstd::stringmatch(pattern.data(), "cache-lfu-decay-time", 1)) {
    elements += 2;
    EncodeString(&config_body, "cache-lfu-decay-time");
    EncodeNumber(&config_body, g_pika_conf->cache_lfu_decay_time());
  }

  if (pstd::stringmatch(pattern.data(), "acl-pubsub-default", 1) != 0) {
    elements += 2;
    EncodeString(&config_body, "acl-pubsub-default");
    g_pika_conf->acl_pubsub_default() ? EncodeString(&config_body, "allchannels")
                                      : EncodeString(&config_body, "resetchannels");
  }

  std::stringstream resp;
  resp << "*" << std::to_string(elements) << "\r\n" << config_body;
  ret = resp.str();
}

// Remember to sync change PikaConf::ConfigRewrite();
void ConfigCmd::ConfigSet(std::shared_ptr<DB> db) {
  std::string set_item = config_args_v_[1];
  if (set_item == "*") {
    std::vector<std::string> replyVt({
        "timeout",
        "requirepass",
        "masterauth",
        "slotmigrate",
        "slotmigrate-thread-num",
        "thread-migrate-keys-num",
        "userpass",
        "userblacklist",
        "dump-prefix",
        "maxclients",
        "dump-expire",
        "expire-logs-days",
        "expire-logs-nums",
        "root-connection-num",
        "slowlog-write-errorlog",
        "slowlog-log-slower-than",
        "slowlog-max-len",
        "write-binlog",
        "max-cache-statistic-keys",
        "small-compaction-threshold",
        "small-compaction-duration-threshold",
        "max-client-response-size",
        "db-sync-speed",
        "compact-cron",
        "compact-interval",
        "disable_auto_compactions",
        "slave-priority",
        "sync-window-size",
        "slow-cmd-list",
        // Options for storage engine
        // MutableDBOptions
        "max-cache-files",
        "max-background-compactions",
        "max-background-jobs",
        // MutableColumnFamilyOptions
        "write-buffer-size",
        "max-write-buffer-num",
        "min-write-buffer-number-to-merge",
        "max-total-wal-size",
        "level0-slowdown-writes-trigger",
        "level0-stop-writes-trigger",
        "level0-file-num-compaction-trigger",
        "arena-block-size",
        "throttle-bytes-per-second",
        "max-rsync-parallel-num",
        "cache-model",
        "cache-type",
        "zset-cache-start-direction",
        "zset-cache-field-num-per-key",
        "cache-lfu-decay-time",
        "max-conn-rbuf-size",
    });
    res_.AppendStringVector(replyVt);
    return;
  }
  long int ival = 0;
  std::string value = config_args_v_[2];
  if (set_item == "timeout") {
    if (pstd::string2int(value.data(), value.size(), &ival) == 0) {
      res_.AppendStringRaw("-ERR Invalid argument " + value + " for CONFIG SET 'timeout'\r\n");
      return;
    }
    g_pika_conf->SetTimeout(static_cast<int>(ival));
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "requirepass") {
    g_pika_conf->SetRequirePass(value);
    g_pika_server->Acl()->UpdateDefaultUserPassword(value);
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "masterauth") {
    g_pika_conf->SetMasterAuth(value);
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "userpass") {
    g_pika_conf->SetUserPass(value);
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "userblacklist") {
    g_pika_conf->SetUserBlackList(value);
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "dump-prefix") {
    g_pika_conf->SetBgsavePrefix(value);
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "maxclients") {
    if ((pstd::string2int(value.data(), value.size(), &ival) == 0) || ival <= 0) {
      res_.AppendStringRaw("-ERR Invalid argument \'" + value + "\' for CONFIG SET 'maxclients'\r\n");
      return;
    }
    g_pika_conf->SetMaxConnection(static_cast<int>(ival));
    g_pika_server->SetDispatchQueueLimit(static_cast<int>(ival));
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "dump-expire") {
    if (pstd::string2int(value.data(), value.size(), &ival) == 0) {
      res_.AppendStringRaw("-ERR Invalid argument \'" + value + "\' for CONFIG SET 'dump-expire'\r\n");
      return;
    }
    g_pika_conf->SetExpireDumpDays(static_cast<int>(ival));
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "slave-priority") {
    if (pstd::string2int(value.data(), value.size(), &ival) == 0) {
      res_.AppendStringRaw("-ERR Invalid argument \'" + value + "\' for CONFIG SET 'slave-priority'\r\n");
      return;
    }
    g_pika_conf->SetSlavePriority(static_cast<int>(ival));
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "expire-logs-days") {
    if ((pstd::string2int(value.data(), value.size(), &ival) == 0) || ival <= 0) {
      res_.AppendStringRaw( "-ERR Invalid argument \'" + value + "\' for CONFIG SET 'expire-logs-days'\r\n");
      return;
    }
    g_pika_conf->SetExpireLogsDays(static_cast<int>(ival));
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "expire-logs-nums") {
    if ((pstd::string2int(value.data(), value.size(), &ival) == 0) || ival <= 0) {
      res_.AppendStringRaw("-ERR Invalid argument \'" + value + "\' for CONFIG SET 'expire-logs-nums'\r\n");
      return;
    }
    g_pika_conf->SetExpireLogsNums(static_cast<int>(ival));
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "root-connection-num") {
    if ((pstd::string2int(value.data(), value.size(), &ival) == 0) || ival <= 0) {
      res_.AppendStringRaw("-ERR Invalid argument \'" + value + "\' for CONFIG SET 'root-connection-num'\r\n");
      return;
    }
    g_pika_conf->SetRootConnectionNum(static_cast<int>(ival));
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "slotmigrate-thread-num") {
    if ((pstd::string2int(value.data(), value.size(), &ival) == 0) || ival <= 0) {
      res_.AppendStringRaw("-ERR Invalid argument \'" + value + "\' for CONFIG SET 'slotmigrate-thread-num'\r\n");
      return;
    }
    long int migrate_thread_num = (1 > ival || 24 < ival) ? 8 : ival;
    g_pika_conf->SetSlotMigrateThreadNum(migrate_thread_num);
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "thread-migrate-keys-num") {
    if ((pstd::string2int(value.data(), value.size(), &ival) == 0) || ival <= 0) {
      res_.AppendStringRaw("-ERR Invalid argument \'" + value + "\' for CONFIG SET 'thread-migrate-keys-num'\r\n");
      return;
    }
    long int thread_migrate_keys_num = (8 > ival || 128 < ival) ? 64 : ival;
    g_pika_conf->SetThreadMigrateKeysNum(thread_migrate_keys_num);
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "slowlog-write-errorlog") {
    bool is_write_errorlog;
    if (value == "yes") {
      is_write_errorlog = true;
    } else if (value == "no") {
      is_write_errorlog = false;
    } else {
      res_.AppendStringRaw( "-ERR Invalid argument \'" + value + "\' for CONFIG SET 'slowlog-write-errorlog'\r\n");
      return;
    }
    g_pika_conf->SetSlowlogWriteErrorlog(is_write_errorlog);
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "slotmigrate") {
    bool slotmigrate;
    if (value == "yes") {
      slotmigrate = true;
    } else if (value == "no") {
      slotmigrate = false;
    } else {
      res_.AppendStringRaw( "-ERR Invalid argument \'" + value + "\' for CONFIG SET 'slotmigrate'\r\n");
      return;
    }
    g_pika_conf->SetSlotMigrate(slotmigrate);
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "slowlog-log-slower-than") {
    if ((pstd::string2int(value.data(), value.size(), &ival) == 0) || ival < 0) {
      res_.AppendStringRaw("-ERR Invalid argument \'" + value + "\' for CONFIG SET 'slowlog-log-slower-than'\r\n");
      return;
    }
    g_pika_conf->SetSlowlogSlowerThan(static_cast<int>(ival));
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "slowlog-max-len") {
    if ((pstd::string2int(value.data(), value.size(), &ival) == 0) || ival < 0) {
      res_.AppendStringRaw("-ERR Invalid argument \'" + value + "\' for CONFIG SET 'slowlog-max-len'\r\n");
      return;
    }
    g_pika_conf->SetSlowlogMaxLen(static_cast<int>(ival));
    g_pika_server->SlowlogTrim();
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "max-cache-statistic-keys") {
    if ((pstd::string2int(value.data(), value.size(), &ival) == 0) || ival < 0) {
      res_.AppendStringRaw("-ERR Invalid argument \'" + value + "\' for CONFIG SET 'max-cache-statistic-keys'\r\n");
      return;
    }
    g_pika_conf->SetMaxCacheStatisticKeys(static_cast<int>(ival));
    g_pika_server->DBSetMaxCacheStatisticKeys(static_cast<int>(ival));
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "small-compaction-threshold") {
    if ((pstd::string2int(value.data(), value.size(), &ival) == 0) || ival < 0) {
      res_.AppendStringRaw("-ERR Invalid argument \'" + value + "\' for CONFIG SET 'small-compaction-threshold'\r\n");
      return;
    }
    g_pika_conf->SetSmallCompactionThreshold(static_cast<int>(ival));
    g_pika_server->DBSetSmallCompactionThreshold(static_cast<int>(ival));
    res_.AppendStringRaw( "+OK\r\n");
  } else if (set_item == "small-compaction-duration-threshold") {
    if ((pstd::string2int(value.data(), value.size(), &ival) == 0) || ival < 0) {
      res_.AppendStringRaw("-ERR Invalid argument \'" + value + "\' for CONFIG SET 'small-compaction-duration-threshold'\r\n");
      return;
    }
    g_pika_conf->SetSmallCompactionDurationThreshold(static_cast<int>(ival));
    g_pika_server->DBSetSmallCompactionDurationThreshold(static_cast<int>(ival));
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "disable_auto_compactions") {
    if (value != "true" && value != "false") {
      res_.AppendStringRaw("-ERR invalid disable_auto_compactions (true or false)\r\n");
      return;
    }
    std::unordered_map<std::string, std::string> options_map{{"disable_auto_compactions", value}};
    storage::Status s = g_pika_server->RewriteStorageOptions(storage::OptionType::kColumnFamily, options_map);
    if (!s.ok()) {
      res_.AppendStringRaw("-ERR Set storage::OptionType::kColumnFamily disable_auto_compactions wrong: " + s.ToString() + "\r\n");
      return;
    }
    g_pika_conf->SetDisableAutoCompaction(value);
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "max-client-response-size") {
    if ((pstd::string2int(value.data(), value.size(), &ival) == 0) || ival < 0) {
      res_.AppendStringRaw("-ERR Invalid argument \'" + value + "\' for CONFIG SET 'max-client-response-size'\r\n");
      return;
    }
    g_pika_conf->SetMaxClientResponseSize(static_cast<int>(ival));
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "write-binlog") {
    int role = g_pika_server->role();
    if (role == PIKA_ROLE_SLAVE) {
      res_.AppendStringRaw("-ERR need to close master-slave mode first\r\n");
      return;
    } else if (value != "yes" && value != "no") {
      res_.AppendStringRaw("-ERR invalid write-binlog (yes or no)\r\n");
      return;
    } else {
      g_pika_conf->SetWriteBinlog(value);
      res_.AppendStringRaw("+OK\r\n");
    }
  } else if (set_item == "db-sync-speed") {
    if (pstd::string2int(value.data(), value.size(), &ival) == 0) {
      res_.AppendStringRaw("-ERR Invalid argument \'" + value + "\' for CONFIG SET 'db-sync-speed(MB)'\r\n");
      return;
    }
    if (ival < 0 || ival > 1024) {
      ival = 1024;
    }
    g_pika_conf->SetDbSyncSpeed(static_cast<int>(ival));
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "compact-cron") {
    bool invalid = false;
    if (!value.empty()) {
      bool have_week = false;
      std::string compact_cron;
      std::string week_str;
      int64_t slash_num = count(value.begin(), value.end(), '/');
      if (slash_num == 2) {
        have_week = true;
        std::string::size_type first_slash = value.find('/');
        week_str = value.substr(0, first_slash);
        compact_cron = value.substr(first_slash + 1);
      } else {
        compact_cron = value;
      }

      std::string::size_type len = compact_cron.length();
      std::string::size_type colon = compact_cron.find('-');
      std::string::size_type underline = compact_cron.find('/');
      if (colon == std::string::npos || underline == std::string::npos || colon >= underline || colon + 1 >= len ||
          colon + 1 == underline || underline + 1 >= len) {
        invalid = true;
      } else {
        int week = std::atoi(week_str.c_str());
        int start = std::atoi(compact_cron.substr(0, colon).c_str());
        int end = std::atoi(compact_cron.substr(colon + 1, underline).c_str());
        int usage = std::atoi(compact_cron.substr(underline + 1).c_str());
        if ((have_week && (week < 1 || week > 7)) || start < 0 || start > 23 || end < 0 || end > 23 || usage < 0 ||
            usage > 100) {
          invalid = true;
        }
      }
    }
    if (invalid) {
      res_.AppendStringRaw("-ERR invalid compact-cron\r\n");
      return;
    } else {
      g_pika_conf->SetCompactCron(value);
      res_.AppendStringRaw("+OK\r\n");
    }
  } else if (set_item == "compact-interval") {
    bool invalid = false;
    if (!value.empty()) {
      std::string::size_type len = value.length();
      std::string::size_type slash = value.find('/');
      if (slash == std::string::npos || slash + 1 >= len) {
        invalid = true;
      } else {
        int interval = std::atoi(value.substr(0, slash).c_str());
        int usage = std::atoi(value.substr(slash + 1).c_str());
        if (interval <= 0 || usage < 0 || usage > 100) {
          invalid = true;
        }
      }
    }
    if (invalid) {
      res_.AppendStringRaw("-ERR invalid compact-interval\r\n");
      return;
    } else {
      g_pika_conf->SetCompactInterval(value);
      res_.AppendStringRaw("+OK\r\n");
    }
  } else if (set_item == "sync-window-size") {
    if (pstd::string2int(value.data(), value.size(), &ival) == 0) {
      res_.AppendStringRaw("-ERR Invalid argument \'" + value + "\' for CONFIG SET 'sync-window-size'\r\n");
      return;
    }
    if (ival <= 0 || ival > kBinlogReadWinMaxSize) {
      res_.AppendStringRaw("-ERR Argument exceed range \'" + value + "\' for CONFIG SET 'sync-window-size'\r\n");
      return;
    }
    g_pika_conf->SetSyncWindowSize(static_cast<int>(ival));
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "slow-cmd-list") {
    g_pika_conf->SetSlowCmd(value);
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "max-cache-files") {
    if (pstd::string2int(value.data(), value.size(), &ival) == 0) {
      res_.AppendStringRaw("-ERR Invalid argument \'" + value + "\' for CONFIG SET 'max-cache-files'\r\n");
      return;
    }
    std::unordered_map<std::string, std::string> options_map{{"max_open_files", value}};
    storage::Status s = g_pika_server->RewriteStorageOptions(storage::OptionType::kDB, options_map);
    if (!s.ok()) {
      res_.AppendStringRaw("-ERR Set max-cache-files wrong: " + s.ToString() + "\r\n");
      return;
    }
    g_pika_conf->SetMaxCacheFiles(static_cast<int>(ival));
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "max-background-compactions") {
    if (pstd::string2int(value.data(), value.size(), &ival) == 0) {
      res_.AppendStringRaw( "-ERR Invalid argument \'" + value + "\' for CONFIG SET 'max-background-compactions'\r\n");
      return;
    }
    std::unordered_map<std::string, std::string> options_map{{"max_background_compactions", value}};
    storage::Status s = g_pika_server->RewriteStorageOptions(storage::OptionType::kDB, options_map);
    if (!s.ok()) {
      res_.AppendStringRaw("-ERR Set max-background-compactions wrong: " + s.ToString() + "\r\n");
      return;
    }
    g_pika_conf->SetMaxBackgroudCompactions(static_cast<int>(ival));
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "rocksdb-periodic-second") {
    if (pstd::string2int(value.data(), value.size(), &ival) == 0) {
      res_.AppendStringRaw("-ERR Invalid argument \'" + value + "\' for CONFIG SET 'rocksdb-periodic-second'\r\n");
      return;
    }
    std::unordered_map<std::string, std::string> options_map{{"periodic_compaction_seconds", value}};
    storage::Status s = g_pika_server->RewriteStorageOptions(storage::OptionType::kColumnFamily, options_map);
    if (!s.ok()) {
      res_.AppendStringRaw("-ERR Set rocksdb-periodic-second wrong: " + s.ToString() + "\r\n");
      return;
    }
    g_pika_conf->SetRocksdbPeriodicSecond(static_cast<uint64_t>(ival));
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "rocksdb-ttl-second") {
    if (pstd::string2int(value.data(), value.size(), &ival) == 0) {
      res_.AppendStringRaw("-ERR Invalid argument \'" + value + "\' for CONFIG SET 'rocksdb-ttl-second'\r\n");
      return;
    }
    std::unordered_map<std::string, std::string> options_map{{"ttl", value}};
    storage::Status s = g_pika_server->RewriteStorageOptions(storage::OptionType::kColumnFamily, options_map);
    if (!s.ok()) {
      res_.AppendStringRaw("-ERR Set rocksdb-ttl-second wrong: " + s.ToString() + "\r\n");
      return;
    }
    g_pika_conf->SetRocksdbTTLSecond(static_cast<uint64_t>(ival));
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "max-background-jobs") {
    if (pstd::string2int(value.data(), value.size(), &ival) == 0) {
      res_.AppendStringRaw("-ERR Invalid argument \'" + value + "\' for CONFIG SET 'max-background-jobs'\r\n");
      return;
    }
    std::unordered_map<std::string, std::string> options_map{{"max_background_jobs", value}};
    storage::Status s = g_pika_server->RewriteStorageOptions(storage::OptionType::kDB, options_map);
    if (!s.ok()) {
      res_.AppendStringRaw("-ERR Set max-background-jobs wrong: " + s.ToString() + "\r\n");
      return;
    }
    g_pika_conf->SetMaxBackgroudJobs(static_cast<int>(ival));
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "write-buffer-size") {
    if (pstd::string2int(value.data(), value.size(), &ival) == 0) {
      res_.AppendStringRaw("-ERR Invalid argument \'" + value + "\' for CONFIG SET 'write-buffer-size'\r\n");
      return;
    }
    std::unordered_map<std::string, std::string> options_map{{"write_buffer_size", value}};
    storage::Status s = g_pika_server->RewriteStorageOptions(storage::OptionType::kColumnFamily, options_map);
    if (!s.ok()) {
      res_.AppendStringRaw("-ERR Set write-buffer-size wrong: " + s.ToString() + "\r\n");
      return;
    }
    g_pika_conf->SetWriteBufferSize(static_cast<int>(ival));
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "max-write-buffer-num") {
    if (pstd::string2int(value.data(), value.size(), &ival) == 0) {
      res_.AppendStringRaw("-ERR Invalid argument \'" + value + "\' for CONFIG SET 'max-write-buffer-number'\r\n");
      return;
    }
    std::unordered_map<std::string, std::string> options_map{{"max_write_buffer_number", value}};
    storage::Status s = g_pika_server->RewriteStorageOptions(storage::OptionType::kColumnFamily, options_map);
    if (!s.ok()) {
      res_.AppendStringRaw("-ERR Set max-write-buffer-number wrong: " + s.ToString() + "\r\n");
      return;
    }
    g_pika_conf->SetMaxWriteBufferNumber(static_cast<int>(ival));
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "min-write-buffer-number-to-merge") {
    if (pstd::string2int(value.data(), value.size(), &ival) == 0) {
      res_.AppendStringRaw("-ERR Invalid argument \'" + value + "\' for CONFIG SET 'min-write-buffer-number-to-merge'\r\n");
      return;
    }
    std::unordered_map<std::string, std::string> options_map{{"min_write_buffer_number_to_merge", value}};
    storage::Status s = g_pika_server->RewriteStorageOptions(storage::OptionType::kColumnFamily, options_map);
    if (!s.ok()) {
      res_.AppendStringRaw("-ERR Set min-write-buffer-number-to-merge wrong: " + s.ToString() + "\r\n");
      return;
    }
    g_pika_conf->SetMinWriteBufferNumberToMerge(static_cast<int>(ival));
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "level0-stop-writes-trigger") {
    if (pstd::string2int(value.data(), value.size(), &ival) == 0) {
      res_.AppendStringRaw("-ERR Invalid argument \'" + value + "\' for CONFIG SET 'level0-stop-writes-trigger'\r\n");
      return;
    }
    std::unordered_map<std::string, std::string> options_map{{"level0_stop_writes_trigger", value}};
    storage::Status s = g_pika_server->RewriteStorageOptions(storage::OptionType::kColumnFamily, options_map);
    if (!s.ok()) {
      res_.AppendStringRaw("-ERR Set level0-stop-writes-trigger wrong: " + s.ToString() + "\r\n");
      return;
    }
    g_pika_conf->SetLevel0StopWritesTrigger(static_cast<int>(ival));
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "level0-slowdown-writes-trigger") {
    if (pstd::string2int(value.data(), value.size(), &ival) == 0) {
      res_.AppendStringRaw("-ERR Invalid argument \'" + value + "\' for CONFIG SET 'level0-slowdown-writes-trigger'\r\n");
      return;
    }
    std::unordered_map<std::string, std::string> options_map{{"level0_slowdown_writes_trigger", value}};
    storage::Status s = g_pika_server->RewriteStorageOptions(storage::OptionType::kColumnFamily, options_map);
    if (!s.ok()) {
      res_.AppendStringRaw("-ERR Set level0-slowdown-writes-trigger wrong: " + s.ToString() + "\r\n");
      return;
    }
    g_pika_conf->SetLevel0SlowdownWritesTrigger(static_cast<int>(ival));
    res_.AppendStringRaw("+OK\r\n");

  } else if (set_item == "max-total-wal-size") {
    if (pstd::string2int(value.data(), value.size(), &ival) == 0) {
      res_.AppendStringRaw("-ERR Invalid argument \'" + value + "\' for CONFIG SET 'max-total-wal-size'\r\n");
      return;
    }
    std::unordered_map<std::string, std::string> options_map{{"max_total_wal_size", value}};
    storage::Status s = g_pika_server->RewriteStorageOptions(storage::OptionType::kDB, options_map);
    if (!s.ok()) {
      res_.AppendStringRaw("-ERR Set max-total-wal-size: " + s.ToString() + "\r\n");
      return;
    }
    g_pika_conf->SetMaxTotalWalSize(static_cast<uint64_t>(ival));
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "level0-file-num-compaction-trigger") {
    if (pstd::string2int(value.data(), value.size(), &ival) == 0) {
      res_.AppendStringRaw("-ERR Invalid argument \'" + value + "\' for CONFIG SET 'level0-file-num-compaction-trigger'\r\n");
      return;
    }
    std::unordered_map<std::string, std::string> options_map{{"level0_file_num_compaction_trigger", value}};
    storage::Status s = g_pika_server->RewriteStorageOptions(storage::OptionType::kColumnFamily, options_map);
    if (!s.ok()) {
      res_.AppendStringRaw("-ERR Set level0-file-num-compaction-trigger wrong: " + s.ToString() + "\r\n");
      return;
    }
    g_pika_conf->SetLevel0FileNumCompactionTrigger(static_cast<int>(ival));
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "arena-block-size") {
    if (pstd::string2int(value.data(), value.size(), &ival) == 0) {
      res_.AppendStringRaw("-ERR Invalid argument \'" + value + "\' for CONFIG SET 'arena-block-size'\r\n");
      return;
    }
    std::unordered_map<std::string, std::string> options_map{{"arena_block_size", value}};
    storage::Status s = g_pika_server->RewriteStorageOptions(storage::OptionType::kColumnFamily, options_map);
    if (!s.ok()) {
      res_.AppendStringRaw( "-ERR Set arena-block-size wrong: " + s.ToString() + "\r\n");
      return;
    }
    g_pika_conf->SetArenaBlockSize(static_cast<int>(ival));
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "throttle-bytes-per-second") {
    if ((pstd::string2int(value.data(), value.size(), &ival) == 0) || ival <= 0) {
      res_.AppendStringRaw("-ERR Invalid argument \'" + value + "\' for CONFIG SET 'throttle-bytes-per-second'\r\n");
      return;
    }
    int32_t new_throughput_limit = static_cast<int>(ival);
    g_pika_conf->SetThrottleBytesPerSecond(new_throughput_limit);
    //The rate limiter of rsync(Throttle) is used in singleton mode, all db shares the same rate limiter
    rsync::Throttle::GetInstance().ResetThrottleThroughputBytes(new_throughput_limit);
    LOG(INFO) << "The conf item [throttle-bytes-per-second] is changed by Config Set command. "
                   "The rsync rate limit now is "
                << new_throughput_limit << "(Which Is Around " << (new_throughput_limit >> 20) << " MB/s)";
    res_.AppendStringRaw("+OK\r\n");
  } else if(set_item == "rsync-timeout-ms"){
    if(pstd::string2int(value.data(), value.size(), &ival) == 0 || ival <= 0){
      res_.AppendStringRaw("-ERR Invalid argument \'" + value + "\' for CONFIG SET 'rsync-timeout-ms'\r\n");
      return;
    }
    g_pika_conf->SetRsyncTimeoutMs(ival);
    LOG(INFO) << "The conf item [rsync-timeout-ms] is changed by Config Set command. "
                   "The rsync-timeout-ms now is " << ival << " ms";
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "max-rsync-parallel-num") {
    if ((pstd::string2int(value.data(), value.size(), &ival) == 0) || ival > kMaxRsyncParallelNum || ival <= 0) {
      res_.AppendStringRaw( "-ERR Invalid argument \'" + value + "\' for CONFIG SET 'max-rsync-parallel-num'\r\n");
      return;
    }
    g_pika_conf->SetMaxRsyncParallelNum(static_cast<int>(ival));
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "cache-num") {
    if (!pstd::string2int(value.data(), value.size(), &ival) || ival < 0) {
      res_.AppendStringRaw("-ERR Invalid argument " + value + " for CONFIG SET 'cache-num'\r\n");
      return;
    }

    int cache_num = (ival <= 0 || ival > 48) ? 16 : ival;
    if (cache_num != g_pika_conf->GetCacheNum()) {
      g_pika_conf->SetCacheNum(cache_num);
      g_pika_server->ResetCacheAsync(cache_num, db);
    }
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "cache-model") {
    if (!pstd::string2int(value.data(), value.size(), &ival) || ival < 0) {
      res_.AppendStringRaw( "-ERR Invalid argument " + value + " for CONFIG SET 'cache-model'\r\n");
      return;
    }
    if (PIKA_CACHE_NONE > ival || PIKA_CACHE_READ < ival) {
      res_.AppendStringRaw("-ERR Invalid cache model\r\n");
    } else {
      g_pika_conf->SetCacheMode(ival);
      if (PIKA_CACHE_NONE == ival) {
        g_pika_server->ClearCacheDbAsync(db);
      }
      res_.AppendStringRaw("+OK\r\n");
    }
  } else if (set_item == "cache-type") {
    pstd::StringToLower(value);
    std::set<std::string> available_types = {"string", "set", "zset", "list", "hash", "bit"};
    std::string type_str = value;
    std::vector<std::string> types;
    type_str.erase(remove_if(type_str.begin(), type_str.end(), ::isspace), type_str.end());
    pstd::StringSplit(type_str, COMMA, types);
    for (auto& type : types) {
      if (available_types.find(type) == available_types.end()) {
        res_.AppendStringRaw("-ERR Invalid cache type: " + type + "\r\n");
        return;
      }
    }
    g_pika_conf->SetCacheType(value);
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "zset-cache-start-direction") {
    if (!pstd::string2int(value.data(), value.size(), &ival)) {
      res_.AppendStringRaw("-ERR Invalid argument " + value + " for CONFIG SET 'zset-cache-start-direction'\r\n");
      return;
    }
    if (ival != CACHE_START_FROM_BEGIN && ival != CACHE_START_FROM_END) {
      res_.AppendStringRaw("-ERR Invalid zset-cache-start-direction\r\n");
      return;
    }
    auto origin_start_pos = g_pika_conf->zset_cache_start_direction();
    if (origin_start_pos != ival) {
      g_pika_conf->SetCacheStartDirection(ival);
      g_pika_server->OnCacheStartPosChanged(ival, db);
    }
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "zset-cache-field-num-per-key") {
    if (!pstd::string2int(value.data(), value.size(), &ival) || ival < 0) {
      res_.AppendStringRaw("-ERR Invalid argument " + value + " for CONFIG SET 'zset-cache-field-num-per-key'\r\n");
      return;
    }
    g_pika_conf->SetCacheItemsPerKey(ival);
    g_pika_server->ResetCacheConfig(db);
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "cache-maxmemory") {
    if (!pstd::string2int(value.data(), value.size(), &ival) || ival < 0) {
      res_.AppendStringRaw("-ERR Invalid argument " + value + " for CONFIG SET 'cache-maxmemory'\r\n");
      return;
    }
    int64_t cache_maxmemory = (PIKA_CACHE_SIZE_MIN > ival) ? PIKA_CACHE_SIZE_DEFAULT : ival;
    g_pika_conf->SetCacheMaxmemory(cache_maxmemory);
    g_pika_server->ResetCacheConfig(db);
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "cache-maxmemory-policy") {
    if (!pstd::string2int(value.data(), value.size(), &ival) || ival < 0) {
      res_.AppendStringRaw("-ERR Invalid argument " + value + " for CONFIG SET 'cache-maxmemory-policy'\r\n");
      return;
    }
    int cache_maxmemory_policy_ = (ival < 0|| ival > 5) ? 3 : ival; // default allkeys-lru
    g_pika_conf->SetCacheMaxmemoryPolicy(cache_maxmemory_policy_);
    g_pika_server->ResetCacheConfig(db);
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "cache-maxmemory-samples") {
    if (!pstd::string2int(value.data(), value.size(), &ival) || ival < 0) {
      res_.AppendStringRaw("-ERR Invalid argument " + value + " for CONFIG SET 'cache-maxmemory-samples'\r\n");
      return;
    }
    int cache_maxmemory_samples = (ival > 1) ? 5 : ival;
    g_pika_conf->SetCacheMaxmemorySamples(cache_maxmemory_samples);
    g_pika_server->ResetCacheConfig(db);
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "cache-lfu-decay-time") {
    if (!pstd::string2int(value.data(), value.size(), &ival) || ival < 0) {
      res_.AppendStringRaw("-ERR Invalid argument " + value + " for CONFIG SET 'cache-lfu-decay-time'\r\n");
      return;
    }
    int cache_lfu_decay_time = (ival < 0) ? 1 : ival;
    g_pika_conf->SetCacheLFUDecayTime(cache_lfu_decay_time);
    g_pika_server->ResetCacheConfig(db);
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "acl-pubsub-default") {
    std::string v(value);
    pstd::StringToLower(v);
    if (v != "allchannels" && v != "resetchannels") {
      res_.AppendStringRaw("-ERR Invalid argument \'" + value + "\' for CONFIG SET 'acl-pubsub-default'\r\n");
      return;
    }
    g_pika_conf->SetAclPubsubDefault(v);
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "acllog-max-len") {
    if (pstd::string2int(value.data(), value.size(), &ival) == 0 || ival < 0) {
      res_.AppendStringRaw( "-ERR Invalid argument \'" + value + "\' for CONFIG SET 'acllog-max-len'\r\n");
      return;
    }
    g_pika_conf->SetAclLogMaxLen(static_cast<int>(ival));
    res_.AppendStringRaw("+OK\r\n");
  } else if (set_item == "max-conn-rbuf-size") {
    if (pstd::string2int(value.data(), value.size(), &ival) == 0 || ival < PIKA_MAX_CONN_RBUF_LB || ival > PIKA_MAX_CONN_RBUF_HB * 2) {
      res_.AppendStringRaw( "-ERR Invalid argument \'" + value + "\' for CONFIG SET 'max-conn-rbuf-size'\r\n");
      return;
    }
    g_pika_conf->SetMaxConnRbufSize(static_cast<int>(ival));
    res_.AppendStringRaw("+OK\r\n");
  } else {
    res_.AppendStringRaw("-ERR Unsupported CONFIG parameter: " + set_item + "\r\n");
  }
}

void ConfigCmd::ConfigRewrite(std::string& ret) {
  if (g_pika_conf->ConfigRewrite() != 0) {
    ret = "+OK\r\n";
  } else {
    ret = "-ERR Rewire CONFIG fail\r\n";
  }
}

void ConfigCmd::ConfigRewriteReplicationID(std::string& ret) {
  if (g_pika_conf->ConfigRewriteReplicationID() != 0) {
    ret = "+OK\r\n";
  } else {
    ret = "-ERR Rewire ReplicationID CONFIG fail\r\n";
  }
}

void ConfigCmd::ConfigResetstat(std::string& ret) {
  g_pika_server->ResetStat();
  ret = "+OK\r\n";
}

void MonitorCmd::DoInitial() {
  if (argv_.size() != 1) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameMonitor);
    return;
  }
}

void MonitorCmd::Do() {
  std::shared_ptr<net::NetConn> conn_repl = GetConn();
  if (!conn_repl) {
    res_.SetRes(CmdRes::kErrOther, kCmdNameMonitor);
    LOG(WARNING) << name_ << " weak ptr is empty";
    return;
  }

  g_pika_server->AddMonitorClient(std::dynamic_pointer_cast<PikaClientConn>(conn_repl));
  res_.SetRes(CmdRes::kOk);
}

void DbsizeCmd::DoInitial() {
  if (argv_.size() != 1) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameDbsize);
    return;
  }
}

void DbsizeCmd::Do() {
  std::shared_ptr<DB> dbs = g_pika_server->GetDB(db_name_);
  if (!dbs) {
    res_.SetRes(CmdRes::kInvalidDB);
  } else {
    if (g_pika_conf->slotmigrate()){
      int64_t dbsize = 0;
      for (int i = 0; i < g_pika_conf->default_slot_num(); ++i){
        int32_t card = 0;
        rocksdb::Status s = dbs->storage()->SCard(SlotKeyPrefix+std::to_string(i), &card);
        if (s.ok() && card >= 0) {
          dbsize += card;
        } else {
          res_.SetRes(CmdRes::kErrOther, "Get dbsize error");
          return;
        }
      }
      res_.AppendInteger(dbsize);
    }
    KeyScanInfo key_scan_info = dbs->GetKeyScanInfo();
    std::vector<storage::KeyInfo> key_infos = key_scan_info.key_infos;
    if (key_infos.size() != 5) {
      res_.SetRes(CmdRes::kErrOther, "keyspace error");
      return;
    }
    uint64_t dbsize = key_infos[0].keys + key_infos[1].keys + key_infos[2].keys + key_infos[3].keys + key_infos[4].keys;
    res_.AppendInteger(static_cast<int64_t>(dbsize));
  }
}

void TimeCmd::DoInitial() {
  if (argv_.size() != 1) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameTime);
    return;
  }
}

void TimeCmd::Do() {
  struct timeval tv;
  if (gettimeofday(&tv, nullptr) == 0) {
    res_.AppendArrayLen(2);
    char buf[32];
    int32_t len = pstd::ll2string(buf, sizeof(buf), tv.tv_sec);
    res_.AppendStringLen(len);
    res_.AppendContent(buf);

    len = pstd::ll2string(buf, sizeof(buf), tv.tv_usec);
    res_.AppendStringLen(len);
    res_.AppendContent(buf);
  } else {
    res_.SetRes(CmdRes::kErrOther, strerror(errno));
  }
}

void LastsaveCmd::DoInitial() {
  if (argv_.size() != 1) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLastSave);
    return;
  }
}

void LastsaveCmd::Do() {
  res_.AppendInteger(g_pika_server->GetLastSave());
}

void DelbackupCmd::DoInitial() {
  if (argv_.size() != 1) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameDelbackup);
    return;
  }
}

void DelbackupCmd::Do() {
  std::string db_sync_prefix = g_pika_conf->bgsave_prefix();
  std::string db_sync_path = g_pika_conf->bgsave_path();
  std::vector<std::string> dump_dir;

  // Dump file is not exist
  if (!pstd::FileExists(db_sync_path)) {
    res_.SetRes(CmdRes::kOk);
    return;
  }
  // Directory traversal
  if (pstd::GetChildren(db_sync_path, dump_dir) != 0) {
    res_.SetRes(CmdRes::kOk);
    return;
  }

  int len = static_cast<int>(dump_dir.size());
  for (auto& i : dump_dir) {
    if (i.substr(0, db_sync_prefix.size()) != db_sync_prefix || i.size() != (db_sync_prefix.size() + 8)) {
      continue;
    }

    std::string str_date = i.substr(db_sync_prefix.size(), (i.size() - db_sync_prefix.size()));
    char* end = nullptr;
    std::strtol(str_date.c_str(), &end, 10);
    if (*end != 0) {
      continue;
    }

    std::string dump_dir_name = db_sync_path + i + "/" + db_name_;
    if (g_pika_server->CountSyncSlaves() == 0) {
      LOG(INFO) << "Not syncing, delete dump file: " << dump_dir_name;
      pstd::DeleteDirIfExist(dump_dir_name);
      len--;
    } else {
      LOG(INFO) << "Syncing, can not delete " << dump_dir_name << " dump file" << std::endl;
    }
  }
  res_.SetRes(CmdRes::kOk);
}

void EchoCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameEcho);
    return;
  }
  body_ = argv_[1];
}

void EchoCmd::Do() { res_.AppendString(body_); }

void ScandbCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameEcho);
    return;
  }
  if (argv_.size() == 1) {
    type_ = storage::kAll;
  } else {
    if (strcasecmp(argv_[1].data(), "string") == 0) {
      type_ = storage::kStrings;
    } else if (strcasecmp(argv_[1].data(), "hash") == 0) {
      type_ = storage::kHashes;
    } else if (strcasecmp(argv_[1].data(), "set") == 0) {
      type_ = storage::kSets;
    } else if (strcasecmp(argv_[1].data(), "zset") == 0) {
      type_ = storage::kZSets;
    } else if (strcasecmp(argv_[1].data(), "list") == 0) {
      type_ = storage::kLists;
    } else {
      res_.SetRes(CmdRes::kInvalidDbType);
    }
  }
}

void ScandbCmd::Do() {
  std::shared_ptr<DB> dbs = g_pika_server->GetDB(db_name_);
  if (!dbs) {
    res_.SetRes(CmdRes::kInvalidDB);
  } else {
    dbs->ScanDatabase(type_);
    res_.SetRes(CmdRes::kOk);
  }
}

void SlowlogCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlowlog);
    return;
  }
  if (argv_.size() == 2 && (strcasecmp(argv_[1].data(), "reset") == 0)) {
    condition_ = SlowlogCmd::kRESET;
  } else if (argv_.size() == 2 && (strcasecmp(argv_[1].data(), "len") == 0)) {
    condition_ = SlowlogCmd::kLEN;
  } else if ((argv_.size() == 2 || argv_.size() == 3) && (strcasecmp(argv_[1].data(), "get") == 0)) {
    condition_ = SlowlogCmd::kGET;
    if (argv_.size() == 3 && (pstd::string2int(argv_[2].data(), argv_[2].size(), &number_) == 0)) {
      res_.SetRes(CmdRes::kInvalidInt);
      return;
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, "Unknown SLOWLOG subcommand or wrong # of args. Try GET, RESET, LEN.");
    return;
  }
}

void SlowlogCmd::Do() {
  if (condition_ == SlowlogCmd::kRESET) {
    g_pika_server->SlowlogReset();
    res_.SetRes(CmdRes::kOk);
  } else if (condition_ == SlowlogCmd::kLEN) {
    res_.AppendInteger(g_pika_server->SlowlogLen());
  } else {
    std::vector<SlowlogEntry> slowlogs;
    g_pika_server->SlowlogObtain(number_, &slowlogs);
    res_.AppendArrayLenUint64(slowlogs.size());
    for (const auto& slowlog : slowlogs) {
      res_.AppendArrayLen(4);
      res_.AppendInteger(slowlog.id);
      res_.AppendInteger(slowlog.start_time);
      res_.AppendInteger(slowlog.duration);
      res_.AppendArrayLenUint64(slowlog.argv.size());
      for (const auto& arg : slowlog.argv) {
        res_.AppendString(arg);
      }
    }
  }
}

void PaddingCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePadding);
    return;
  }
}

void PaddingCmd::Do() { res_.SetRes(CmdRes::kOk); }

std::string PaddingCmd::ToRedisProtocol() {
  return PikaBinlogTransverter::ConstructPaddingBinlog(
      BinlogType::TypeFirst,
      argv_[1].size() + BINLOG_ITEM_HEADER_SIZE + PADDING_BINLOG_PROTOCOL_SIZE + SPACE_STROE_PARAMETER_LENGTH);
}

void PKPatternMatchDelCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePKPatternMatchDel);
    return;
  }
  pattern_ = argv_[1];
  if (strcasecmp(argv_[2].data(), "set") == 0) {
    type_ = storage::kSets;
  } else if (strcasecmp(argv_[2].data(), "list") == 0) {
    type_ = storage::kLists;
  } else if (strcasecmp(argv_[2].data(), "string") == 0) {
    type_ = storage::kStrings;
  } else if (strcasecmp(argv_[2].data(), "zset") == 0) {
    type_ = storage::kZSets;
  } else if (strcasecmp(argv_[2].data(), "hash") == 0) {
    type_ = storage::kHashes;
  } else {
    res_.SetRes(CmdRes::kInvalidDbType, kCmdNamePKPatternMatchDel);
    return;
  }
}

void PKPatternMatchDelCmd::Do() {
  int ret = 0;
  rocksdb::Status s = db_->storage()->PKPatternMatchDel(type_, pattern_, &ret);
  if (s.ok()) {
    res_.AppendInteger(ret);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void DummyCmd::DoInitial() {}

void DummyCmd::Do() {}

void QuitCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameQuit);
  }
}

void QuitCmd::Do() {
  res_.SetRes(CmdRes::kOk);
  LOG(INFO) << "QutCmd will close connection " << GetConn()->String();
  GetConn()->SetClose(true);
}

/*
 * HELLO [<protocol-version> [AUTH <password>] [SETNAME <name>] ]
 */
void HelloCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHello);
    return;
  }
}

void HelloCmd::Do() {
  size_t next_arg = 1;
  long ver = 0;
  if (argv_.size() >= 2) {
    if (pstd::string2int(argv_[next_arg].data(), argv_[next_arg].size(), &ver) == 0) {
      res_.SetRes(CmdRes::kErrOther, "Protocol version is not an integer or out of range");
      return;
    }
    next_arg++;

    if (ver < 2 || ver > 3) {
      res_.AppendContent("-NOPROTO unsupported protocol version");
      return;
    }
  }

  std::shared_ptr<net::NetConn> conn = GetConn();
  if (!conn) {
    res_.SetRes(CmdRes::kErrOther, kCmdNameHello);
    return;
  }

  for (; next_arg < argv_.size(); next_arg++) {
    size_t more_args = argv_.size() - next_arg - 1;
    const std::string opt = argv_[next_arg];
    if ((strcasecmp(opt.data(), "AUTH") == 0) && (more_args >= 2)) {
      const std::string userName = argv_[next_arg + 1];
      const std::string pwd = argv_[next_arg + 2];
      bool defaultAuth = false;
      if (userName == Acl::DefaultUser) {
        defaultAuth = true;
      }
      auto authResult = AuthenticateUser(name(), userName, pwd, conn, defaultAuth);
      switch (authResult) {
        case AuthResult::INVALID_CONN:
          res_.SetRes(CmdRes::kErrOther, kCmdNamePing);
          return;
        case AuthResult::INVALID_PASSWORD:
          res_.AppendContent("-WRONGPASS invalid username-password pair or user is disabled.");
          return;
        case AuthResult::NO_REQUIRE_PASS:
          res_.SetRes(CmdRes::kErrOther, "Client sent AUTH, but no password is set");
        default:
          break;
      }
      next_arg += 2;
    } else if ((strcasecmp(opt.data(), "SETNAME") == 0) && (more_args != 0U)) {
      const std::string name = argv_[next_arg + 1];
      if (pstd::isspace(name)) {
        res_.SetRes(CmdRes::kErrOther, "Client names cannot contain spaces, newlines or special characters.");
        return;
      }
      conn->set_name(name);
      next_arg++;
    } else {
      res_.SetRes(CmdRes::kErrOther, "Syntax error in HELLO option " + opt);
      return;
    }
  }

  std::string raw;
  std::vector<storage::FieldValue> fvs{
      {"server", "redis"},
  };
  // just for redis resp2 protocol
  fvs.push_back({"proto", "2"});
  fvs.push_back({"mode", "classic"});
  int host_role = g_pika_server->role();
  switch (host_role) {
    case PIKA_ROLE_SINGLE:
    case PIKA_ROLE_MASTER:
      fvs.push_back({"role", "master"});
      break;
    case PIKA_ROLE_SLAVE:
      fvs.push_back({"role", "slave"});
      break;
    case PIKA_ROLE_MASTER | PIKA_ROLE_SLAVE:
      fvs.push_back({"role", "master&&slave"});
      break;
    default:
      LOG(INFO) << "unknown role" << host_role << " client ip:port " << conn->ip_port();
      return;
  }

  for (const auto& fv : fvs) {
    RedisAppendLenUint64(raw, fv.field.size(), "$");
    RedisAppendContent(raw, fv.field);
    if (fv.field == "proto") {
      pstd::string2int(fv.value.data(), fv.value.size(), &ver);
      RedisAppendLen(raw, static_cast<int64_t>(ver), ":");
      continue;
    }
    RedisAppendLenUint64(raw, fv.value.size(), "$");
    RedisAppendContent(raw, fv.value);
  }
  res_.AppendArrayLenUint64(fvs.size() * 2);
  res_.AppendStringRaw(raw);
}

void DiskRecoveryCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameDiskRecovery);
    return;
  }
}

void DiskRecoveryCmd::Do() {
  struct statvfs disk_info;
  int ret = statvfs(g_pika_conf->db_path().c_str(), &disk_info);
  if (ret == -1) {
    std::stringstream tmp_stream;
    tmp_stream << "statvfs error:" << strerror(errno);
    const std::string res = tmp_stream.str();
    res_.SetRes(CmdRes::kErrOther, res);
    return;
  }
  int64_t least_free_size = g_pika_conf->least_resume_free_disk_size();
  uint64_t free_size = disk_info.f_bsize * disk_info.f_bfree;
  if (free_size < least_free_size) {
    res_.SetRes(CmdRes::kErrOther, "The available disk capacity is insufficient");
    return;
  }
  std::shared_mutex dbs_rw;
  std::shared_lock db_rwl(dbs_rw);
  // loop every db
  for (const auto& db_item : g_pika_server->GetDB()) {
    if (!db_item.second) {
      continue;
    }
    db_item.second->SetBinlogIoErrorrelieve();
    background_errors_.clear();
    db_item.second->DBLockShared();
    db_item.second->storage()->GetUsage(storage::PROPERTY_TYPE_ROCKSDB_BACKGROUND_ERRORS, &background_errors_);
    db_item.second->DBUnlockShared();
    for (const auto &item: background_errors_) {
      if (item.second != 0) {
        rocksdb::Status s = db_item.second->storage()->GetDBByIndex(item.first)->Resume();
        if (!s.ok()) {
          res_.SetRes(CmdRes::kErrOther, "The restore operation failed.");
        }
      }
    }
  }
  res_.SetRes(CmdRes::kOk, "The disk error has been recovered");
}

void ClearReplicationIDCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameClearReplicationID);
    return;
  }
}

void ClearReplicationIDCmd::Do() {
  g_pika_conf->SetReplicationID("");
  g_pika_conf->ConfigRewriteReplicationID();
  res_.SetRes(CmdRes::kOk, "ReplicationID is cleared");
}

void DisableWalCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameDisableWal);
    return;
  }
}

void DisableWalCmd::Do() {
  std::string option = argv_[1].data();
  bool is_wal_disable = false;
  if (option.compare("true") == 0) {
    is_wal_disable = true;
  } else if (option.compare("false") == 0) {
    is_wal_disable = false;
  } else {
    res_.SetRes(CmdRes::kErrOther, "Invalid parameter");
    return;
  }
  db_->storage()->DisableWal(is_wal_disable);
  res_.SetRes(CmdRes::kOk, "Wal options is changed");
}

void CacheCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameCache);
    return;
  }
  if (!strcasecmp(argv_[1].data(), "clear")) {
    if (argv_.size() == 3 && !strcasecmp(argv_[2].data(), "db")) {
      condition_ = kCLEAR_DB;
    } else if (argv_.size() == 3 && !strcasecmp(argv_[2].data(), "hitratio")) {
      condition_ = kCLEAR_HITRATIO;
    } else {
      res_.SetRes(CmdRes::kErrOther, "Unknown cache subcommand or wrong # of args.");
    }
  } else if (argv_.size() >= 3 && !strcasecmp(argv_[1].data(), "del")) {
    condition_ = kDEL_KEYS;
    keys_.assign(argv_.begin() + 2, argv_.end());
  } else if (argv_.size() == 2 && !strcasecmp(argv_[1].data(), "randomkey")) {
    condition_ = kRANDOM_KEY;
  } else {
    res_.SetRes(CmdRes::kErrOther, "Unknown cache subcommand or wrong # of args.");
  }
  return;
}

void CacheCmd::Do() {
  std::string key;
  switch (condition_) {
    case kCLEAR_DB:
      g_pika_server->ClearCacheDbAsync(db_);
      res_.SetRes(CmdRes::kOk);
      break;
    case kCLEAR_HITRATIO:
      g_pika_server->ClearHitRatio(db_);
      res_.SetRes(CmdRes::kOk);
      break;
    case kDEL_KEYS:
      db_->cache()->Del(keys_);
      res_.SetRes(CmdRes::kOk);
      break;
    case kRANDOM_KEY:
      s_ = db_->cache()->RandomKey(&key);
      if (!s_.ok()) {
        res_.AppendStringLen(-1);
      } else {
        res_.AppendStringLen(key.size());
        res_.AppendContent(key);
      }
      break;
    default:
      res_.SetRes(CmdRes::kErrOther, "Unknown cmd");
      break;
  }
  return;
}

void ClearCacheCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameClearCache);
    return;
  }
}

void ClearCacheCmd::Do() {
  // clean cache
  if (PIKA_CACHE_NONE != g_pika_conf->cache_mode()) {
    g_pika_server->ClearCacheDbAsync(db_);
  }
  res_.SetRes(CmdRes::kOk, "Cache is cleared");
}

#ifdef WITH_COMMAND_DOCS

bool CommandCmd::CommandFieldCompare::operator()(const std::string& a, const std::string& b) const {
  int av{0};
  int bv{0};
  if (auto avi = kFieldNameOrder.find(a); avi != kFieldNameOrder.end()) {
    av = avi->second;
  }
  if (auto bvi = kFieldNameOrder.find(b); bvi != kFieldNameOrder.end()) {
    bv = bvi->second;
  }
  return av < bv;
}

CmdRes& CommandCmd::EncodableInt::EncodeTo(CmdRes& res) const {
  res.AppendInteger(value_);
  return res;
}

CommandCmd::EncodablePtr CommandCmd::EncodableInt::MergeFrom(const CommandCmd::EncodablePtr& other) const {
  if (auto pe = std::dynamic_pointer_cast<CommandCmd::EncodableInt>(other)) {
    return std::make_shared<CommandCmd::EncodableInt>(value_ + pe->value_);
  }
  return std::make_shared<CommandCmd::EncodableInt>(value_);
}

CmdRes& CommandCmd::EncodableString::EncodeTo(CmdRes& res) const {
  res.AppendString(value_);
  return res;
}

CommandCmd::EncodablePtr CommandCmd::EncodableString::MergeFrom(const CommandCmd::EncodablePtr& other) const {
  if (auto pe = std::dynamic_pointer_cast<CommandCmd::EncodableString>(other)) {
    return std::make_shared<CommandCmd::EncodableString>(value_ + pe->value_);
  }
  return std::make_shared<CommandCmd::EncodableString>(value_);
}

template <typename Map>
CmdRes& CommandCmd::EncodableMap::EncodeTo(CmdRes& res, const Map& map, const Map& specialization) {
  std::string raw_string;
  RedisAppendLen(raw_string, map.size() * 2, kPrefix);
  res.AppendStringRaw(raw_string);
  for (const auto& kv : map) {
    res.AppendString(kv.first);
    if (auto iter = specialization.find(kv.first); iter != specialization.end()) {
      res << *(*kv.second + iter->second);
    } else {
      res << *kv.second;
    }
  }
  return res;
}

CmdRes& CommandCmd::EncodableMap::EncodeTo(CmdRes& res) const { return EncodeTo(res, values_); }

CommandCmd::EncodablePtr CommandCmd::EncodableMap::MergeFrom(const CommandCmd::EncodablePtr& other) const {
  if (auto pe = std::dynamic_pointer_cast<CommandCmd::EncodableMap>(other)) {
    auto values = CommandCmd::EncodableMap::RedisMap(values_.cbegin(), values_.cend());
    for (const auto& pair : pe->values_) {
      auto iter = values.find(pair.first);
      if (iter == values.end()) {
        values[pair.first] = pair.second;
      } else {
        iter->second = (*iter->second + pair.second);
      }
    }
    return std::make_shared<CommandCmd::EncodableMap>(values);
  }
  return std::make_shared<CommandCmd::EncodableMap>(
      CommandCmd::EncodableMap::RedisMap(values_.cbegin(), values_.cend()));
}

CmdRes& CommandCmd::EncodableSet::EncodeTo(CmdRes& res) const {
  std::string raw_string;
  RedisAppendLen(raw_string, values_.size(), kPrefix);
  res.AppendStringRaw(raw_string);
  for (const auto& item : values_) {
    res << *item;
  }
  return res;
}

CommandCmd::EncodablePtr CommandCmd::EncodableSet::MergeFrom(const CommandCmd::EncodablePtr& other) const {
  if (auto pe = std::dynamic_pointer_cast<CommandCmd::EncodableSet>(other)) {
    auto values = std::vector<CommandCmd::EncodablePtr>(values_.cbegin(), values_.cend());
    values.insert(values.end(), pe->values_.cbegin(), pe->values_.cend());
    return std::make_shared<CommandCmd::EncodableSet>(values);
  }
  return std::make_shared<CommandCmd::EncodableSet>(
      std::vector<CommandCmd::EncodablePtr>(values_.cbegin(), values_.cend()));
}

CmdRes& CommandCmd::EncodableArray::EncodeTo(CmdRes& res) const {
  res.AppendArrayLen(values_.size());
  for (const auto& item : values_) {
    res << *item;
  }
  return res;
}

CommandCmd::EncodablePtr CommandCmd::EncodableArray::MergeFrom(const CommandCmd::EncodablePtr& other) const {
  if (auto pe = std::dynamic_pointer_cast<CommandCmd::EncodableArray>(other)) {
    auto values = std::vector<CommandCmd::EncodablePtr>(values_.cbegin(), values_.cend());
    values.insert(values.end(), pe->values_.cbegin(), pe->values_.cend());
    return std::make_shared<CommandCmd::EncodableArray>(values);
  }
  return std::make_shared<CommandCmd::EncodableArray>(
      std::vector<CommandCmd::EncodablePtr>(values_.cbegin(), values_.cend()));
}

CmdRes& CommandCmd::EncodableStatus::EncodeTo(CmdRes& res) const {
  res.AppendStringRaw(kPrefix + value_ + kNewLine);
  return res;
}

CommandCmd::EncodablePtr CommandCmd::EncodableStatus::MergeFrom(const CommandCmd::EncodablePtr& other) const {
  if (auto pe = std::dynamic_pointer_cast<CommandCmd::EncodableStatus>(other)) {
    return std::make_shared<CommandCmd::EncodableStatus>(value_ + pe->value_);
  }
  return std::make_shared<CommandCmd::EncodableStatus>(value_);
}

const std::unordered_map<std::string, int> CommandCmd::CommandFieldCompare::kFieldNameOrder{
    {kPikaField, 0},         {"name", 100},      {"type", 101},
    {"spec", 102},           {"index", 103},     {"display_text", 104},
    {"key_spec_index", 105}, {"token", 106},     {"summary", 107},
    {"since", 108},          {"group", 109},     {"complexity", 110},
    {"module", 111},         {"doc_flags", 112}, {"deprecated_since", 113},
    {"notes", 114},          {"flags", 15},      {"begin_search", 116},
    {"replaced_by", 17},     {"history", 18},    {"arguments", 119},
    {"subcommands", 120},    {"keyword", 121},   {"startfrom", 122},
    {"find_keys", 123},      {"lastkey", 124},   {"keynum", 125},
    {"keynumidx", 126},      {"firstkey", 127},  {"keystep", 128},
    {"limit", 129},
};
const std::string CommandCmd::EncodableMap::kPrefix = "*";
const std::string CommandCmd::EncodableSet::kPrefix = "*";
const std::string CommandCmd::EncodableStatus::kPrefix = "+";

void CommandCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {  // The original redis command's arity is -1
    res_.SetRes(CmdRes::kWrongNum, kCmdNameEcho);
    return;
  }
  if (argv_.size() < 2) {  // But currently only docs subcommand is impled
    res_.SetRes(CmdRes::kErrOther, "only docs subcommand supported");
    return;
  }
  if (command_ = argv_[1]; strcasecmp(command_.data(), "docs") != 0) {
    res_.SetRes(CmdRes::kErrOther, "unknown command '" + command_ + "'");
    return;
  }
  cmds_begin_ = argv_.cbegin() + 2;
  cmds_end_ = argv_.cend();
}

extern std::unique_ptr<PikaCmdTableManager> g_pika_cmd_table_manager;

void CommandCmd::Do(std::shared_ptr<DB> dbs) {
  std::unordered_map<std::string, CommandCmd::EncodablePtr> cmds;
  std::unordered_map<std::string, CommandCmd::EncodablePtr> specializations;
  if (cmds_begin_ == cmds_end_) {
    cmds = kCommandDocs;
    specializations.insert(kPikaSpecialization.cbegin(), kPikaSpecialization.cend());
  } else {
    for (auto iter = cmds_begin_; iter != cmds_end_; ++iter) {
      if (auto cmd = kCommandDocs.find(*iter); cmd != kCommandDocs.end()) {
        cmds.insert(*cmd);
      }
      if (auto specialization = kPikaSpecialization.find(*iter); specialization != kPikaSpecialization.end()) {
        specializations.insert(*specialization);
      }
    }
  }
  for (const auto& cmd : cmds) {
    if (!g_pika_cmd_table_manager->CmdExist(cmd.first)) {
      specializations[cmd.first] = kNotSupportedSpecialization;
    } else if (auto iter = specializations.find(cmd.first); iter == specializations.end()) {
      specializations[cmd.first] = kCompatibleSpecialization;
    }
  }
  EncodableMap::EncodeTo(res_, cmds, specializations);
}

#endif  // WITH_COMMAND_DOCS
