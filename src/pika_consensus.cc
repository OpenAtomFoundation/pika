// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_consensus.h"

#include "include/pika_conf.h"
#include "include/pika_server.h"
#include "include/pika_client_conn.h"
#include "include/pika_rm.h"
#include "include/pika_cmd_table_manager.h"

extern PikaServer* g_pika_server;
extern PikaConf* g_pika_conf;
extern PikaReplicaManager* g_pika_rm;
extern PikaCmdTableManager* g_pika_cmd_table_manager;

/* Context */

Context::Context(const std::string path) : applied_index_(), path_(path), save_(NULL) {
  pthread_rwlock_init(&rwlock_, NULL);
}

Context::~Context() {
  pthread_rwlock_destroy(&rwlock_);
  delete save_;
}

Status Context::StableSave() {
  char *p = save_->GetData();
  memcpy(p, &(applied_index_.b_offset.filenum), sizeof(uint32_t));
  p += 4;
  memcpy(p, &(applied_index_.b_offset.offset), sizeof(uint64_t));
  p += 8;
  memcpy(p, &(applied_index_.l_offset.term), sizeof(uint32_t));
  p += 4;
  memcpy(p, &(applied_index_.l_offset.index), sizeof(uint64_t));
  return Status::OK();
}

Status Context::Init() {
  if (!slash::FileExists(path_)) {
    Status s = slash::NewRWFile(path_, &save_);
    if (!s.ok()) {
      LOG(FATAL) << "Context new file failed " << s.ToString();
    }
    StableSave();
  } else {
    Status s = slash::NewRWFile(path_, &save_);
    if (!s.ok()) {
      LOG(FATAL) << "Context new file failed " << s.ToString();
    }
  }
  if (save_->GetData() != NULL) {
    memcpy((char*)(&(applied_index_.b_offset.filenum)), save_->GetData(), sizeof(uint32_t));
    memcpy((char*)(&(applied_index_.b_offset.offset)), save_->GetData() + 4, sizeof(uint64_t));
    memcpy((char*)(&(applied_index_.l_offset.term)), save_->GetData() + 12, sizeof(uint32_t));
    memcpy((char*)(&(applied_index_.l_offset.index)), save_->GetData() + 16, sizeof(uint64_t));
    return Status::OK();
  } else {
    return Status::Corruption("Context init error");
  }
}

void Context::PrepareUpdateAppliedIndex(const LogOffset& offset) {
  slash::RWLock l(&rwlock_, true);
  applied_win_.Push(SyncWinItem(offset));
}

void Context::UpdateAppliedIndex(const LogOffset& offset) {
  slash::RWLock l(&rwlock_, true);
  LogOffset cur_offset;
  applied_win_.Update(SyncWinItem(offset), SyncWinItem(offset), &cur_offset);
  if (cur_offset > applied_index_) {
    applied_index_ = cur_offset;
    StableSave();
  }
}

void Context::Reset(const LogOffset& offset) {
  slash::RWLock l(&rwlock_, true);
  applied_index_ = offset;
  applied_win_.Reset();
  StableSave();
}

/* SyncProgress */

SyncProgress::SyncProgress() {
  pthread_rwlock_init(&rwlock_, NULL);
}

SyncProgress::~SyncProgress() {
  pthread_rwlock_destroy(&rwlock_);
}

std::shared_ptr<SlaveNode> SyncProgress::GetSlaveNode(const std::string& ip, int port) {
  std::string slave_key = ip + std::to_string(port);
  slash::RWLock l(&rwlock_, false);
  if (slaves_.find(slave_key) == slaves_.end()) {
    return nullptr;
  }
  return slaves_[slave_key];
}

std::unordered_map<std::string, std::shared_ptr<SlaveNode>> SyncProgress::GetAllSlaveNodes() {
  slash::RWLock l(&rwlock_, false);
  return slaves_;
}

std::unordered_map<std::string, LogOffset> SyncProgress::GetAllMatchIndex() {
  slash::RWLock l(&rwlock_, false);
  return match_index_;
}

Status SyncProgress::AddSlaveNode(const std::string& ip, int port,
    const std::string& table_name, uint32_t partition_id, int session_id) {
  std::string slave_key = ip + std::to_string(port);
  std::shared_ptr<SlaveNode> exist_ptr = GetSlaveNode(ip, port);
  if (exist_ptr) {
    LOG(WARNING) << "SlaveNode " << exist_ptr->ToString() <<
      " already exist, set new session " << session_id;
    exist_ptr->SetSessionId(session_id);
    return Status::OK();
  }
  std::shared_ptr<SlaveNode> slave_ptr =
    std::make_shared<SlaveNode>(ip, port, table_name, partition_id, session_id);
  slave_ptr->SetLastSendTime(slash::NowMicros());
  slave_ptr->SetLastRecvTime(slash::NowMicros());

  {
    slash::RWLock l(&rwlock_, true);
    slaves_[slave_key] = slave_ptr;
    // add slave to match_index
    match_index_[slave_key] = LogOffset();
  }
  return Status::OK();
}

Status SyncProgress::RemoveSlaveNode(const std::string& ip, int port) {
  std::string slave_key = ip + std::to_string(port);
  {
    slash::RWLock l(&rwlock_, true);
    slaves_.erase(slave_key);
    // remove slave to match_index
    match_index_.erase(slave_key);
  }
  return Status::OK();
}

Status SyncProgress::Update(const std::string& ip, int port, const LogOffset& start,
      const LogOffset& end, LogOffset* committed_index) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    return Status::NotFound("ip " + ip  + " port " + std::to_string(port));
  }

  LogOffset acked_offset;
  {
    // update slave_ptr
    slash::MutexLock l(&slave_ptr->slave_mu);
    Status s = slave_ptr->Update(start, end, &acked_offset);
    if (!s.ok()) {
      return s;
    }
    // update match_index_
    // shared slave_ptr->slave_mu
    match_index_[ip+std::to_string(port)] = acked_offset;
  }

  // if consensus_level == 0 LogOffset() return
  *committed_index = InternalCalCommittedIndex(GetAllMatchIndex());
  return Status::OK();
}

int SyncProgress::SlaveSize() {
  slash::RWLock l(&rwlock_, false);
  return slaves_.size();
}

LogOffset SyncProgress::InternalCalCommittedIndex(std::unordered_map<std::string, LogOffset> match_index) {
  int consensus_level = g_pika_conf->consensus_level();
  if (consensus_level == 0) {
    return LogOffset();
  }
  if (static_cast<int>(match_index.size()) < consensus_level) {
    return LogOffset();
  }
  std::vector<LogOffset> offsets;
  for (const auto& index : match_index) {
    offsets.push_back(index.second);
  }
  std::sort(offsets.begin(), offsets.end());
  LogOffset offset = offsets[offsets.size() - consensus_level];
  return offset;
}

/* MemLog */

MemLog::MemLog() : last_offset_() {
}

int MemLog::Size() {
  return static_cast<int>(logs_.size());
}

// purge [begin, offset]
Status MemLog::PurgeLogs(const LogOffset& offset, std::vector<LogItem>* logs) {
  slash::MutexLock l_logs(&logs_mu_);
  int index = InternalFindLogByBinlogOffset(offset);
  if (index < 0) {
    return Status::NotFound("Cant find correct index");
  }
  logs->assign(logs_.begin(), logs_.begin() + index + 1);
  logs_.erase(logs_.begin(), logs_.begin() + index + 1);
  return Status::OK();
}

// keep mem_log [mem_log.begin, offset]
Status MemLog::TruncateTo(const LogOffset& offset) {
  slash::MutexLock l_logs(&logs_mu_);
  int index = InternalFindLogByBinlogOffset(offset);
  if (index < 0) {
    return Status::Corruption("Cant find correct index");
  }
  last_offset_ = logs_[index].offset;
  logs_.erase(logs_.begin() + index + 1, logs_.end());
  return Status::OK();
}

void MemLog::Reset(const LogOffset& offset) {
  slash::MutexLock l_logs(&logs_mu_);
  logs_.erase(logs_.begin(), logs_.end());
  last_offset_ = offset;
}

Status MemLog::GetRangeLogs(int start, int end, std::vector<LogItem>* logs) {
  slash::MutexLock l_logs(&logs_mu_);
  int log_size = static_cast<int>(logs_.size());
  if (start > end || start >= log_size || end >= log_size) {
    return Status::Corruption("Invalid index");
  }
  logs->assign(logs_.begin() + start, logs_.begin() + end + 1);
  return Status::OK();
}

bool MemLog::FindLogItem(const LogOffset& offset, LogOffset* found_offset) {
  slash::MutexLock l_logs(&logs_mu_);
  int index = InternalFindLogByLogicIndex(offset);
  if (index < 0) {
    return false;
  }
  *found_offset = logs_[index].offset;
  return true;
}

int MemLog::InternalFindLogByLogicIndex(const LogOffset& offset) {
  for (size_t i = 0; i < logs_.size(); ++i) {
    if (logs_[i].offset.l_offset.index > offset.l_offset.index) {
      return -1;
    }
    if (logs_[i].offset.l_offset.index == offset.l_offset.index) {
      return i;
    }
  }
  return -1;
}

int MemLog::InternalFindLogByBinlogOffset(const LogOffset& offset) {
  for (size_t i = 0; i < logs_.size(); ++i) {
    if (logs_[i].offset > offset) {
      return -1;
    }
    if (logs_[i].offset == offset) {
      return i;
    }
  }
  return -1;
}

/* ConsensusCoordinator */

ConsensusCoordinator::ConsensusCoordinator(const std::string& table_name, uint32_t partition_id) : table_name_(table_name), partition_id_(partition_id) {
  std::string table_log_path = g_pika_conf->log_path() + "log_" + table_name + "/";
  std::string log_path = g_pika_conf->classic_mode() ?
    table_log_path : table_log_path + std::to_string(partition_id) + "/";
  context_ = std::make_shared<Context>(log_path + kContext);
  stable_logger_ = std::make_shared<StableLog>(table_name, partition_id, log_path);
  mem_logger_ = std::make_shared<MemLog>();
  pthread_rwlock_init(&term_rwlock_, NULL);
  if (g_pika_conf->consensus_level() != 0) {
    Init();
  }
}

ConsensusCoordinator::~ConsensusCoordinator() {
  pthread_rwlock_destroy(&term_rwlock_);
}

// since it is invoked in constructor all locks not hold
void ConsensusCoordinator::Init() {
  // load committed_index_ & applied_index
  context_->Init();
  committed_index_ = context_->applied_index_;

  //load term_
  term_ = stable_logger_->Logger()->term();

  LOG(INFO) << PartitionInfo(table_name_, partition_id_).ToString()
    << "Restore applied index " << context_->applied_index_.ToString() << " current term " << term_;
  if (committed_index_ == LogOffset()) {
    return;
  }
  // load mem_logger_
  mem_logger_->SetLastOffset(committed_index_);
  pink::RedisParserSettings settings;
  settings.DealMessage = &(ConsensusCoordinator::InitCmd);
  pink::RedisParser redis_parser;
  redis_parser.RedisParserInit(REDIS_PARSER_REQUEST, settings);
  PikaBinlogReader binlog_reader;
  int res = binlog_reader.Seek(stable_logger_->Logger(),
      committed_index_.b_offset.filenum, committed_index_.b_offset.offset);
  if (res) {
    LOG(FATAL) << PartitionInfo(table_name_, partition_id_).ToString() << "Binlog reader init failed";
  }

  while(1) {
    LogOffset offset;
    std::string binlog;
    Status s = binlog_reader.Get(&binlog, &(offset.b_offset.filenum), &(offset.b_offset.offset));
    if (s.IsEndFile()) {
      break;
    } else if (s.IsCorruption() || s.IsIOError()) {
      LOG(FATAL) << PartitionInfo(table_name_, partition_id_).ToString() << "Read Binlog error";
    }
    BinlogItem item;
    if (!PikaBinlogTransverter::BinlogItemWithoutContentDecode(TypeFirst, binlog, &item)) {
      LOG(FATAL) << PartitionInfo(table_name_, partition_id_).ToString() << "Binlog item decode failed";
    }
    offset.l_offset.term = item.term_id();
    offset.l_offset.index = item.logic_id();

    redis_parser.data = static_cast<void*>(&table_name_);
    const char* redis_parser_start = binlog.data() + BINLOG_ENCODE_LEN;
    int redis_parser_len = static_cast<int>(binlog.size()) - BINLOG_ENCODE_LEN;
    int processed_len = 0;
    pink::RedisParserStatus ret = redis_parser.ProcessInputBuffer(
        redis_parser_start, redis_parser_len, &processed_len);
    if (ret != pink::kRedisParserDone) {
      LOG(FATAL) << PartitionInfo(table_name_, partition_id_).ToString() << "Redis parser parse failed";
      return;
    }
    CmdPtrArg* arg = static_cast<CmdPtrArg*>(redis_parser.data);
    std::shared_ptr<Cmd> cmd_ptr = arg->cmd_ptr;
    delete arg;
    redis_parser.data = NULL;

    mem_logger_->AppendLog(MemLog::LogItem(offset, cmd_ptr, nullptr, nullptr));
  }
}

Status ConsensusCoordinator::Reset(const LogOffset& offset) {
  context_->Reset(offset);
  {
    slash::MutexLock l(&index_mu_);
    committed_index_ = offset;
  }

  UpdateTerm(offset.l_offset.term);
  Status s = stable_logger_->Logger()->SetProducerStatus(
      offset.b_offset.filenum, offset.b_offset.offset,
      offset.l_offset.term, offset.l_offset.index);
  if (!s.ok()) {
    LOG(WARNING) << PartitionInfo(table_name_, partition_id_).ToString() << "Consensus reset status failed " << s.ToString();
    return s;
  }

  stable_logger_->SetFirstOffset(offset);

  stable_logger_->Logger()->Lock();
  mem_logger_->Reset(offset);
  stable_logger_->Logger()->Unlock();
  return Status::OK();
}

Status ConsensusCoordinator::ProposeLog(
    std::shared_ptr<Cmd> cmd_ptr,
    std::shared_ptr<PikaClientConn> conn_ptr,
    std::shared_ptr<std::string> resp_ptr) {
  LogOffset log_offset;

  stable_logger_->Logger()->Lock();
  // build BinlogItem
  uint32_t filenum = 0, term = 0;
  uint64_t offset = 0, logic_id = 0;
  Status s = stable_logger_->Logger()->GetProducerStatus(&filenum, &offset, &term, &logic_id);
  if (!s.ok()) {
    stable_logger_->Logger()->Unlock();
    return s;
  }
  BinlogItem item;
  item.set_exec_time(time(nullptr));
  item.set_term_id(term);
  item.set_logic_id(logic_id + 1);
  item.set_filenum(filenum);
  item.set_offset(offset);
  // make sure stable log and mem log consistent
  s = InternalAppendLog(item, cmd_ptr, conn_ptr, resp_ptr);
  if (!s.ok()) {
    stable_logger_->Logger()->Unlock();
    return s;
  }
  stable_logger_->Logger()->Unlock();

  g_pika_server->SignalAuxiliary();
  return Status::OK();
}

Status ConsensusCoordinator::InternalAppendLog(const BinlogItem& item,
    std::shared_ptr<Cmd> cmd_ptr, std::shared_ptr<PikaClientConn> conn_ptr,
    std::shared_ptr<std::string> resp_ptr) {
  LogOffset log_offset;
  Status s = InternalAppendBinlog(item, cmd_ptr, &log_offset);
  if (!s.ok()) {
    return s;
  }
  if (g_pika_conf->consensus_level() == 0) {
    return Status::OK();
  }
  mem_logger_->AppendLog(MemLog::LogItem(log_offset, cmd_ptr, conn_ptr, resp_ptr));
  return Status::OK();
}

// precheck if prev_offset match && drop this log if this log exist
Status ConsensusCoordinator::ProcessLeaderLog(std::shared_ptr<Cmd> cmd_ptr, const BinlogItem& attribute) {
  LogOffset last_index = mem_logger_->last_offset();
  if (attribute.logic_id() < last_index.l_offset.index) {
    LOG(WARNING) << PartitionInfo(table_name_, partition_id_).ToString()
      << "Drop log from leader logic_id " << attribute.logic_id() << " cur last index " << last_index.l_offset.index;
    return Status::OK();
  }

  stable_logger_->Logger()->Lock();
  Status s = InternalAppendLog(attribute, cmd_ptr, nullptr, nullptr);
  stable_logger_->Logger()->Unlock();

  if (g_pika_conf->consensus_level() == 0) {
    InternalApplyFollower(MemLog::LogItem(LogOffset(), cmd_ptr, nullptr, nullptr));
    return Status::OK();
  }

  return Status::OK();
}

Status ConsensusCoordinator::ProcessLocalUpdate(const LogOffset& leader_commit) {
  if (g_pika_conf->consensus_level() == 0) {
    return Status::OK();
  }

  LogOffset last_index = mem_logger_->last_offset();
  LogOffset committed_index = last_index < leader_commit ? last_index : leader_commit;

  LogOffset updated_committed_index;
  bool need_update = false;
  {
    slash::MutexLock l(&index_mu_);
    need_update = InternalUpdateCommittedIndex(committed_index, &updated_committed_index);
  }
  if (need_update) {
    Status s = ScheduleApplyFollowerLog(updated_committed_index);
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

Status ConsensusCoordinator::UpdateSlave(const std::string& ip, int port,
      const LogOffset& start, const LogOffset& end) {
  LogOffset committed_index;
  Status s = sync_pros_.Update(ip, port, start, end, &committed_index);
  if (!s.ok()) {
    return s;
  }

  if (g_pika_conf->consensus_level() == 0) {
    return Status::OK();
  }

  // do not commit log which is not current term log
  if (committed_index.l_offset.term != term()) {
    LOG_EVERY_N(INFO, 1000) << "Will not commit log term which is not equals to current term"
      << " To updated committed_index" << committed_index.ToString() << " current term " << term()
      << " from " << ip << " " <<  port << " start " << start.ToString() << " end " << end.ToString();
    return Status::OK();
  }

  LogOffset updated_committed_index;
  bool need_update = false;
  {
    slash::MutexLock l(&index_mu_);
    need_update = InternalUpdateCommittedIndex(committed_index, &updated_committed_index);
  }
  if (need_update) {
    s = ScheduleApplyLog(updated_committed_index);
    // updateslave could be invoked by many thread
    // not found means a late offset pass in ScheduleApplyLog
    // an early offset is not found
    if (!s.ok() && !s.IsNotFound()) {
      return s;
    }
  }

  return Status::OK();
}

bool ConsensusCoordinator::InternalUpdateCommittedIndex(const LogOffset& slave_committed_index, LogOffset* updated_committed_index) {
  if (slave_committed_index <= committed_index_) {
    return false;
  }
  committed_index_ = slave_committed_index;
  *updated_committed_index = slave_committed_index;
  return true;
}

Status ConsensusCoordinator::InternalAppendBinlog(const BinlogItem& item,
    std::shared_ptr<Cmd> cmd_ptr, LogOffset* log_offset) {
  std::string binlog = cmd_ptr->ToBinlog(item.exec_time(),
                                    item.term_id(),
                                    item.logic_id(),
                                    item.filenum(),
                                    item.offset());
  Status s = stable_logger_->Logger()->Put(binlog);
  if (!s.ok()) {
    std::string table_name = cmd_ptr->table_name().empty()
      ? g_pika_conf->default_table() : cmd_ptr->table_name();
    std::shared_ptr<Table> table = g_pika_server->GetTable(table_name);
    if (table) {
      table->SetBinlogIoError();
    }
    return s;
  }
  uint32_t filenum;
  uint64_t offset;
  stable_logger_->Logger()->GetProducerStatus(&filenum, &offset);
  *log_offset = LogOffset(BinlogOffset(filenum, offset),
      LogicOffset(item.term_id(), item.logic_id()));
  return Status::OK();
}

Status ConsensusCoordinator::ScheduleApplyLog(const LogOffset& committed_index) {
  // logs from PurgeLogs goes to InternalApply in order
  slash::MutexLock l(&order_mu_);
  std::vector<MemLog::LogItem> logs;
  Status s = mem_logger_->PurgeLogs(committed_index, &logs);
  if (!s.ok()) {
    return Status::NotFound("committed index not found " + committed_index.ToString());
  }
  for (const auto& log : logs) {
    context_->PrepareUpdateAppliedIndex(log.offset);
    InternalApply(log);
  }
  return Status::OK();
}

Status ConsensusCoordinator::ScheduleApplyFollowerLog(const LogOffset& committed_index) {
  // logs from PurgeLogs goes to InternalApply in order
  slash::MutexLock l(&order_mu_);
  std::vector<MemLog::LogItem> logs;
  Status s = mem_logger_->PurgeLogs(committed_index, &logs);
  if (!s.ok()) {
    return Status::NotFound("committed index not found " + committed_index.ToString());
  }
  for (const auto& log : logs) {
    context_->PrepareUpdateAppliedIndex(log.offset);
    InternalApplyFollower(log);
  }
  return Status::OK();
}

Status ConsensusCoordinator::CheckEnoughFollower() {
  if (!MatchConsensusLevel()) {
    return Status::Incomplete("Not enough follower");
  }
  return Status::OK();
}

Status ConsensusCoordinator::AddSlaveNode(const std::string& ip, int port, int session_id) {
  Status s = sync_pros_.AddSlaveNode(ip, port, table_name_, partition_id_, session_id);
  if (!s.ok()) {
    return s;
  }
  return Status::OK();
}

Status ConsensusCoordinator::RemoveSlaveNode(const std::string& ip, int port) {
  Status s = sync_pros_.RemoveSlaveNode(ip, port);
  if (!s.ok()) {
    return s;
  }
  return Status::OK();
}

void ConsensusCoordinator::UpdateTerm(uint32_t term) {
  stable_logger_->Logger()->Lock();
  slash::RWLock l(&term_rwlock_, true);
  term_ = term;
  stable_logger_->Logger()->SetTerm(term);
  stable_logger_->Logger()->Unlock();
}

uint32_t ConsensusCoordinator::term() {
  slash::RWLock l(&term_rwlock_, false);
  return term_;
}

bool ConsensusCoordinator::MatchConsensusLevel() {
  return sync_pros_.SlaveSize() >= static_cast<int>(g_pika_conf->consensus_level());
}

void ConsensusCoordinator::InternalApply(const MemLog::LogItem& log) {
  PikaClientConn::BgTaskArg* arg = new PikaClientConn::BgTaskArg();
  arg->cmd_ptr = log.cmd_ptr;
  arg->conn_ptr = log.conn_ptr;
  arg->resp_ptr = log.resp_ptr;
  arg->offset = log.offset;
  arg->table_name = table_name_;
  arg->partition_id = partition_id_;
  g_pika_server->ScheduleClientBgThreads(
      PikaClientConn::DoExecTask, arg, log.cmd_ptr->current_key().front());
}

void ConsensusCoordinator::InternalApplyFollower(const MemLog::LogItem& log) {
  g_pika_rm->ScheduleWriteDBTask(log.cmd_ptr, log.offset, table_name_, partition_id_);
}

int ConsensusCoordinator::InitCmd(pink::RedisParser* parser, const pink::RedisCmdArgsType& argv) {
  std::string* table_name = static_cast<std::string*>(parser->data);
  std::string opt = argv[0];
  std::shared_ptr<Cmd> c_ptr = g_pika_cmd_table_manager->GetCmd(slash::StringToLower(opt));
  if (!c_ptr) {
    LOG(WARNING) << "Command " << opt << " not in the command table";
    return -1;
  }
  // Initial
  c_ptr->Initial(argv, *table_name);
  if (!c_ptr->res().ok()) {
    LOG(WARNING) << "Fail to initial command from binlog: " << opt;
    return -1;
  }
  parser->data = static_cast<void*>(new CmdPtrArg(c_ptr));
  return 0;
}

Status ConsensusCoordinator::TruncateTo(const LogOffset& offset) {
  LOG(INFO) << PartitionInfo(table_name_, partition_id_).ToString() << "Truncate to " << offset.ToString();
  LogOffset founded_offset;
  Status s = FindLogicOffset(offset.b_offset, offset.l_offset.index, &founded_offset);
  if (!s.ok()) {
    return s;
  }
  LOG(INFO) << PartitionInfo(table_name_, partition_id_).ToString() << " Founded truncate pos " << founded_offset.ToString();
  LogOffset committed = committed_index();
  stable_logger_->Logger()->Lock();
  if (founded_offset.l_offset.index == committed.l_offset.index) {
    mem_logger_->Reset(committed);
  } else {
    Status s  = mem_logger_->TruncateTo(founded_offset);
    if (!s.ok()) {
      stable_logger_->Logger()->Unlock();
      return s;
    }
  }
  s = stable_logger_->TruncateTo(founded_offset);
  if (!s.ok()) {
    stable_logger_->Logger()->Unlock();
    return s;
  }
  stable_logger_->Logger()->Unlock();
  return Status::OK();
}

Status ConsensusCoordinator::GetBinlogOffset(const BinlogOffset& start_offset, LogOffset* log_offset) {
  PikaBinlogReader binlog_reader;
  int res = binlog_reader.Seek(stable_logger_->Logger(),
      start_offset.filenum, start_offset.offset);
  if (res) {
    return Status::Corruption("Binlog reader init failed");
  }
  std::string binlog;
  BinlogOffset offset;
  Status s = binlog_reader.Get(&binlog, &(offset.filenum), &(offset.offset));
  if (!s.ok()) {
    return Status::Corruption("Binlog reader get failed");
  }
  BinlogItem item;
  if (!PikaBinlogTransverter::BinlogItemWithoutContentDecode(TypeFirst, binlog, &item)) {
    return Status::Corruption("Binlog item decode failed");
  }
  log_offset->b_offset = offset;
  log_offset->l_offset.term = item.term_id();
  log_offset->l_offset.index = item.logic_id();
  return Status::OK();
}

// get binlog offset range [start_offset, end_offset]
// start_offset 0,0 end_offset 1,129, result will include binlog (1,129)
// start_offset 0,0 end_offset 1,0, result will NOT include binlog (1,xxx)
// start_offset 0,0 end_offset 0,0, resulet will NOT include binlog(0,xxx)
Status ConsensusCoordinator::GetBinlogOffset(
    const BinlogOffset& start_offset,
    const BinlogOffset& end_offset,
    std::vector<LogOffset>* log_offset) {
  PikaBinlogReader binlog_reader;
  int res = binlog_reader.Seek(stable_logger_->Logger(),
      start_offset.filenum, start_offset.offset);
  if (res) {
    return Status::Corruption("Binlog reader init failed");
  }
  while(1) {
    BinlogOffset b_offset;
    std::string binlog;
    Status s = binlog_reader.Get(&binlog, &(b_offset.filenum), &(b_offset.offset));
    if (s.IsEndFile()) {
      return Status::OK();
    } else if (s.IsCorruption() || s.IsIOError()) {
      return Status::Corruption("Read Binlog error");
    }
    BinlogItem item;
    if (!PikaBinlogTransverter::BinlogItemWithoutContentDecode(TypeFirst, binlog, &item)) {
      return Status::Corruption("Binlog item decode failed");
    }
    LogOffset offset;
    offset.b_offset = b_offset;
    offset.l_offset.term = item.term_id();
    offset.l_offset.index = item.logic_id();
    if (offset.b_offset > end_offset) {
      return Status::OK();
    }
    log_offset->push_back(offset);
  }
  return Status::OK();
}

Status ConsensusCoordinator::FindBinlogFileNum(
    const std::map<uint32_t, std::string> binlogs,
    uint64_t target_index, uint32_t start_filenum,
    uint32_t* founded_filenum) {
  // low boundary & high boundary
  uint32_t lb_binlogs = binlogs.begin()->first;
  uint32_t hb_binlogs = binlogs.rbegin()->first;
  bool first_time_left = false;
  bool first_time_right = false;
  uint32_t filenum = start_filenum;
  while(1) {
    LogOffset first_offset;
    Status s = GetBinlogOffset(BinlogOffset(filenum, 0), &first_offset);
    if (!s.ok()) {
      return s;
    }
    if (target_index < first_offset.l_offset.index) {
      if (first_time_right) {
        // last filenum
        filenum = filenum -1;
        break;
      }
      // move left
      first_time_left = true;
      if (filenum == 0 || filenum  - 1 < lb_binlogs) {
        return Status::NotFound(std::to_string(target_index) + " hit low boundary");
      }
      filenum = filenum - 1;
    } else if (target_index > first_offset.l_offset.index) {
      if (first_time_left) {
        break;
      }
      // move right
      first_time_right = true;
      if (filenum + 1 > hb_binlogs) {
        break;
      }
      filenum = filenum + 1;
    } else {
      break;
    }
  }
  *founded_filenum = filenum;
  return Status::OK();
}

Status ConsensusCoordinator::FindLogicOffsetBySearchingBinlog(
    const BinlogOffset& hint_offset, uint64_t target_index, LogOffset* found_offset) {
  LOG(INFO) << PartitionInfo(table_name_, partition_id_).ToString()
    << "FindLogicOffsetBySearchingBinlog hint offset " << hint_offset.ToString()
    << " target_index " << target_index;
  BinlogOffset start_offset;
  std::map<uint32_t, std::string> binlogs;
  if (!stable_logger_->GetBinlogFiles(&binlogs)) {
    return Status::Corruption("Get binlog files failed");
  }
  if (binlogs.empty()) {
    return Status::NotFound("Binlogs is empty");
  }
  if (binlogs.find(hint_offset.filenum) == binlogs.end()) {
    start_offset = BinlogOffset(binlogs.crbegin()->first, 0);
  } else {
    start_offset = hint_offset;
  }

  uint32_t found_filenum;
  Status s = FindBinlogFileNum(binlogs, target_index, start_offset.filenum, &found_filenum);
  if (!s.ok()) {
    return s;
  }

  LOG(INFO) << PartitionInfo(table_name_, partition_id_).ToString() << "FindBinlogFilenum res " << found_filenum;
  BinlogOffset traversal_start(found_filenum, 0);
  BinlogOffset traversal_end(found_filenum + 1, 0);
  std::vector<LogOffset> offsets;
  s = GetBinlogOffset(traversal_start, traversal_end, &offsets);
  if (!s.ok()) {
    return s;
  }
  for (auto& offset : offsets) {
    if (offset.l_offset.index == target_index) {
      LOG(INFO) << PartitionInfo(table_name_, partition_id_).ToString() << "Founded " << target_index << " " << offset.ToString();
      *found_offset = offset;
      return Status::OK();
    }
  }
  return Status::NotFound("Logic index not found");
}

Status ConsensusCoordinator::FindLogicOffset(const BinlogOffset& start_offset, uint64_t target_index, LogOffset* found_offset) {
  LogOffset possible_offset;
  Status s = GetBinlogOffset(start_offset, &possible_offset);
  if (!s.ok() || possible_offset.l_offset.index != target_index) {
    if (!s.ok()) {
      LOG(INFO) << PartitionInfo(table_name_, partition_id_).ToString() << "GetBinlogOffset res: " << s.ToString();
    } else {
      LOG(INFO) << PartitionInfo(table_name_, partition_id_).ToString() << "GetBInlogOffset res: " << s.ToString() <<
        " possible_offset " << possible_offset.ToString() << " target_index " << target_index;
    }
    return FindLogicOffsetBySearchingBinlog(start_offset, target_index, found_offset);
  }
  *found_offset = possible_offset;
  return Status::OK();
}

Status ConsensusCoordinator::GetLogsBefore(const BinlogOffset& start_offset, std::vector<LogOffset>* hints) {
  BinlogOffset traversal_end = start_offset;
  BinlogOffset traversal_start(traversal_end.filenum, 0);
  traversal_start.filenum =
    traversal_start.filenum == 0 ? 0 : traversal_start.filenum - 1;
  std::map<uint32_t, std::string> binlogs;
  if (!stable_logger_->GetBinlogFiles(&binlogs)) {
    return Status::Corruption("Get binlog files failed");
  }
  if (binlogs.find(traversal_start.filenum) == binlogs.end()) {
    traversal_start.filenum = traversal_end.filenum;
  }
  std::vector<LogOffset> res;
  Status s = GetBinlogOffset(traversal_start, traversal_end, &res);
  if (!s.ok()) {
    return s;
  }
  if (res.size() > 100) {
    res.assign(res.end() - 100, res.end());
  }
  *hints = res;
  return Status::OK();
}

Status ConsensusCoordinator::LeaderNegotiate(
    const LogOffset& f_last_offset, bool* reject, std::vector<LogOffset>* hints) {
  uint64_t f_index = f_last_offset.l_offset.index;
  LOG(INFO) << PartitionInfo(table_name_, partition_id_).ToString()
    << "LeaderNeotiate follower last offset "
    << f_last_offset.ToString()
    << " first_offsert " << stable_logger_->first_offset().ToString()
    << " last_offset " << mem_logger_->last_offset().ToString();
  *reject = true;
  if (f_index > mem_logger_->last_offset().l_offset.index) {
    // hints starts from last_offset() - 100;
    Status s = GetLogsBefore(mem_logger_->last_offset().b_offset, hints);
    if (!s.ok()) {
      LOG(WARNING) << f_index << " is larger than last index " << mem_logger_->last_offset().ToString() << " get logs before last index failed " << s.ToString();
      return s;
    }
    LOG(INFO) << PartitionInfo(table_name_, partition_id_).ToString()
      << "follower index larger then last_offset index, get logs before " << mem_logger_->last_offset().ToString();
    return Status::OK();
  }
  if (f_index < stable_logger_->first_offset().l_offset.index) {
    // need full sync
    LOG(INFO) << PartitionInfo(table_name_, partition_id_).ToString()
      << f_index << " not found current first index" << stable_logger_->first_offset().ToString();
    return Status::NotFound("logic index");
  }
  if (f_last_offset.l_offset.index == 0) {
    *reject = false;
    return Status::OK();
  }

  LogOffset found_offset;
  Status s = FindLogicOffset(f_last_offset.b_offset, f_index, &found_offset);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      LOG(INFO) << PartitionInfo(table_name_, partition_id_).ToString()
        << f_last_offset.ToString() << " not found " << s.ToString();
      return s;
    } else {
      LOG(WARNING) << PartitionInfo(table_name_, partition_id_).ToString()
        << "find logic offset failed" << s.ToString();
      return s;
    }
  }

  if (found_offset.l_offset.term != f_last_offset.l_offset.term
      || !(f_last_offset.b_offset == found_offset.b_offset)) {
    Status s = GetLogsBefore(found_offset.b_offset, hints);
    if (!s.ok()) {
      LOG(WARNING) << PartitionInfo(table_name_, partition_id_).ToString()
        << "Try to get logs before " << found_offset.ToString() << " failed";
      return s;
    }
    return Status::OK();
  }

  LOG(INFO) << PartitionInfo(table_name_, partition_id_).ToString()
    << "Found equal offset " << found_offset.ToString();
  *reject = false;
  return Status::OK();
}

// memlog order: committed_index , [committed_index + 1, memlogger.end()]
Status ConsensusCoordinator::FollowerNegotiate(const std::vector<LogOffset>& hints, LogOffset* reply_offset) {
  if (hints.empty()) {
    return Status::Corruption("hints empty");
  }
  LOG(INFO) << PartitionInfo(table_name_, partition_id_).ToString()
    << "FollowerNegotiate from " << hints[0].ToString() << " to " << hints[hints.size() - 1].ToString();
  if (mem_logger_->last_offset().l_offset.index < hints[0].l_offset.index) {
    *reply_offset = mem_logger_->last_offset();
    return Status::OK();
  }
  if (committed_index().l_offset.index > hints[hints.size() - 1].l_offset.index) {
    return Status::Corruption("invalid hints all smaller than committed_index");
  }
  if (mem_logger_->last_offset().l_offset.index >  hints[hints.size() - 1].l_offset.index) {
    LogOffset truncate_offset = hints[hints.size() - 1];
    // trunck to hints end
    Status s = TruncateTo(truncate_offset);
    if (!s.ok()) {
      return s;
    }
  }

  LogOffset committed = committed_index();
  for (int i = hints.size() - 1; i >= 0; i--) {
    if (hints[i].l_offset.index < committed.l_offset.index) {
      return Status::Corruption("hints less than committed index");
    }
    if (hints[i].l_offset.index == committed.l_offset.index) {
      if (hints[i].l_offset.term == committed.l_offset.term) {
        Status s = TruncateTo(hints[i]);
        if (!s.ok()) {
          return s;
        }
        *reply_offset = mem_logger_->last_offset();
        return Status::OK();
      }
    }
    LogOffset found_offset;
    bool res = mem_logger_->FindLogItem(hints[i], &found_offset);
    if (!res) {
      return Status::Corruption("hints not found " + hints[i].ToString());
    }
    if (found_offset.l_offset.term == hints[i].l_offset.term) {
      // trunk to found_offsett
      Status s = TruncateTo(found_offset);
      if (!s.ok()) {
        return s;
      }
      *reply_offset = mem_logger_->last_offset();
      return Status::OK();
    }
  }

  Status s = TruncateTo(hints[0]);
  if (!s.ok()) {
    return s;
  }
  *reply_offset = mem_logger_->last_offset();
  return Status::OK();
}
