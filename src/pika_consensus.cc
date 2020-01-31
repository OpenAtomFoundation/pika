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

  *committed_index = InternalCalCommittedIndex(GetAllMatchIndex());
  return Status::OK();
}

int SyncProgress::SlaveSize() {
  slash::RWLock l(&rwlock_, false);
  return slaves_.size();
}

LogOffset SyncProgress::InternalCalCommittedIndex(std::unordered_map<std::string, LogOffset> match_index) {
  int consensus_level = g_pika_conf->consensus_level();
  std::vector<LogOffset> offsets;
  for (auto index : match_index) {
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

Status MemLog::PurdgeLogs(const LogOffset& offset, std::vector<LogItem>* logs) {
  slash::MutexLock l_logs(&logs_mu_);
  int index = InternalFindLogIndex(offset);
  if (index < 0) {
    return Status::Corruption("Cant find correct index");
  }
  logs->assign(logs_.begin(), logs_.begin() + index + 1);
  logs_.erase(logs_.begin(), logs_.begin() + index + 1);
  return Status::OK();
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
  int index = InternalFindLogIndex(offset);
  if (index < 0) {
    return false;
  }
  *found_offset = logs_[index].offset;
  return true;
}

int MemLog::InternalFindLogIndex(const LogOffset& offset) {
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
  context_ = std::make_shared<Context>(table_log_path + kContext);
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

  LOG(INFO) << "Restore applied index " << context_->applied_index_.ToString();
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
    LOG(FATAL) << "Binlog reader init failed";
  }

  while(1) {
    LogOffset offset;
    std::string binlog;
    Status s = binlog_reader.Get(&binlog, &(offset.b_offset.filenum), &(offset.b_offset.offset));
    if (s.IsEndFile()) {
      break;
    } else if (s.IsCorruption() || s.IsIOError()) {
      LOG(FATAL) << "Read Binlog error";
    }
    BinlogItem item;
    if (!PikaBinlogTransverter::BinlogItemWithoutContentDecode(TypeFirst, binlog, &item)) {
      LOG(FATAL) << "Binlog item decode failed";
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
      LOG(FATAL) << "Redis parser parse failed";
      return;
    }
    CmdPtrArg* arg = static_cast<CmdPtrArg*>(redis_parser.data);
    std::shared_ptr<Cmd> cmd_ptr = arg->cmd_ptr;
    delete arg;
    redis_parser.data = NULL;

    mem_logger_->AppendLog(MemLog::LogItem(offset, cmd_ptr, nullptr, nullptr));
  }
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
  Status s = stable_logger_->Logger()->GetProducerStatus(&filenum, &offset, &logic_id, &term);
  if (!s.ok()) {
    stable_logger_->Logger()->Unlock();
    return s;
  }
  BinlogItem item;
  item.set_exec_time(time(nullptr));
  item.set_term_id(term);
  item.set_logic_id(logic_id);
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
    LOG(WARNING) << "Drop log from leader logic_id " << attribute.logic_id() << " cur last index " << last_index.l_offset.index;
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

  LogOffset updated_committed_index;
  bool need_update = false;
  {
    slash::MutexLock l(&index_mu_);
    need_update = InternalUpdateCommittedIndex(committed_index, &updated_committed_index);
  }
  if (need_update) {
    s = ScheduleApplyLog(updated_committed_index);
    if (!s.ok()) {
      return s;
    }
  }

  return Status::OK();
}

bool ConsensusCoordinator::InternalUpdateCommittedIndex(const LogOffset& slave_committed_index, LogOffset* updated_committed_index) {
  if (slave_committed_index < committed_index_ ||
      slave_committed_index == committed_index_) {
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
  std::vector<MemLog::LogItem> logs;
  Status s = mem_logger_->PurdgeLogs(committed_index, &logs);
  if (!s.ok()) {
    return Status::NotFound("committed index not found " + committed_index.ToString());
  }
  for (auto log : logs) {
    context_->PrepareUpdateAppliedIndex(log.offset);
    InternalApply(log);
  }
  return Status::OK();
}

Status ConsensusCoordinator::ScheduleApplyFollowerLog(const LogOffset& committed_index) {
  std::vector<MemLog::LogItem> logs;
  Status s = mem_logger_->PurdgeLogs(committed_index, &logs);
  if (!s.ok()) {
    return Status::NotFound("committed index not found " + committed_index.ToString());
  }
  for (auto log : logs) {
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

Status ConsensusCoordinator::TryGetBinlogOffset(const BinlogOffset& start_offset, LogOffset* log_offset) {
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
  log_offset->b_offset.filenum = item.filenum();
  log_offset->b_offset.offset = item.offset();
  log_offset->l_offset.term = item.term_id();
  log_offset->l_offset.index = item.logic_id();
  return Status::OK();
}

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
    offset.b_offset.filenum = item.filenum();
    offset.b_offset.offset = item.offset();
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
    std::vector<LogOffset> offsets;
    Status s = GetBinlogOffset(BinlogOffset(filenum, 0), BinlogOffset(filenum, 0), &offsets);
    if (!s.ok()) {
      return s;
    }
    if (!offsets.empty()) {
      first_offset = offsets[0];
    }
    if (target_index <= first_offset.l_offset.index) {
      if (first_time_right) {
        break;
      }
      // move left
      first_time_left = true;
      if (filenum  - 1 < lb_binlogs) {
        return Status::NotFound(std::to_string(target_index) + " hit low boundary");
      }
      filenum = filenum - 1;
    } else if (target_index >= first_offset.l_offset.index) {
      if (first_time_left) {
        break;
      }
      // move right
      first_time_right = true;
      if (filenum + 1 > hb_binlogs) {
        break;
      }
      filenum = filenum + 1;
    }
  }
  *founded_filenum = filenum;
  return Status::OK();
}

Status ConsensusCoordinator::FindLogicOffsetBySearchingBinlog(
    const BinlogOffset& hint_offset, uint64_t target_index, LogOffset* found_offset) {
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

  BinlogOffset traversal_start(found_filenum, 0);
  BinlogOffset traversal_end(found_filenum + 1, 0);
  std::vector<LogOffset> offsets;
  s = GetBinlogOffset(traversal_start, traversal_end, &offsets);
  if (!s.ok()) {
    return s;
  }
  for (auto& offset : offsets) {
    if (offset.l_offset.index == target_index) {
      *found_offset = offset;
      return Status::OK();
    }
  }
  return Status::NotFound("Logic index not found");
}

Status ConsensusCoordinator::FindLogicOffset(const BinlogOffset& start_offset, uint64_t target_index, LogOffset* found_offset) {
  LogOffset possible_offset;
  Status s = TryGetBinlogOffset(start_offset, &possible_offset);
  if (!s.ok() || possible_offset.l_offset.index != target_index) {
    return FindLogicOffsetBySearchingBinlog(start_offset, target_index, found_offset);
  }
  *found_offset = possible_offset;
  return Status::OK();
}

Status ConsensusCoordinator::GetLogsBefore(const BinlogOffset& start_offset, std::vector<LogOffset>* hints) {
  BinlogOffset traversal_end = start_offset;
  BinlogOffset traversal_start(traversal_start.filenum - 1, 0);
  std::map<uint32_t, std::string> binlogs;
  if (!stable_logger_->GetBinlogFiles(&binlogs)) {
    return Status::Corruption("Get binlog files failed");
  }
  if (binlogs.find(traversal_start.filenum) == binlogs.end()) {
    traversal_start.filenum = traversal_end.filenum;
  }
  std::vector<LogOffset> res;
  GetBinlogOffset(traversal_start, traversal_end, &res);
  if (res.size() > 100) {
    res.assign(res.end() - 100, res.end());
  }
  *hints = res;
  return Status::OK();
}

Status ConsensusCoordinator::LeaderNegotiate(
    const LogOffset& f_last_offset, bool* reject, std::vector<LogOffset>* hints) {
  uint64_t f_index = f_last_offset.l_offset.index;
  *reject = true;
  if (f_index > mem_logger_->last_offset().l_offset.index) {
    // hints starts from last_offset() - 100;
    Status s = GetLogsBefore(mem_logger_->last_offset().b_offset, hints);
    if (!s.ok()) {
      LOG(WARNING) << f_index << " is larger than last index " << mem_logger_->last_offset().ToString() << " get logs before last index failed " << s.ToString();
      return s;
    }
    return Status::OK();
  } else if (f_index < stable_logger_->first_offset().l_offset.index)  {
    // need full sync
    LOG(WARNING) << f_index << " not found current first index" << stable_logger_->first_offset().ToString();
    return Status::NotFound("logic index");
  }

  LogOffset found_offset;
  Status s = FindLogicOffset(f_last_offset.b_offset, f_index, &found_offset);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      LOG(WARNING) << f_last_offset.ToString() << " not found " << s.ToString();
      return s;
    } else {
      LOG(WARNING) << "find logic offset failed" << s.ToString();
      return s;
    }
  }

  if (found_offset.l_offset.term != f_last_offset.l_offset.term
      || !(f_last_offset.b_offset == found_offset.b_offset)) {
    Status s = GetLogsBefore(found_offset.b_offset, hints);
    if (!s.ok()) {
      LOG(WARNING) << "Try to get logs before " << found_offset.ToString() << " failed";
      return s;
    }
    return Status::OK();
  }

  *reject = false;
  return Status::OK();
}

Status ConsensusCoordinator::FollowerNegotiate(const std::vector<LogOffset>& hints, LogOffset* reply_offset) {
  if (hints.empty()) {
    return Status::Corruption("hints empty");
  }
  if (mem_logger_->last_offset().l_offset.index < hints[0].l_offset.index) {
    *reply_offset = mem_logger_->last_offset();
    return Status::OK();
  }
  if (mem_logger_->last_offset().l_offset.index >  hints[hints.size() - 1].l_offset.index) {
    BinlogOffset truncate_offset = hints[hints.size() -1].b_offset;
    // trunck to hints end
    stable_logger_->Logger()->Truncate(truncate_offset.filenum, truncate_offset.offset);
  }
  for (int i = hints.size() - 1; i >= 0; i--)  {
    LogOffset found_offset;
    bool res = mem_logger_->FindLogItem(hints[i], &found_offset);
    if (!res) {
      return Status::Corruption("hints not found");
    }
    if (found_offset.l_offset.term == hints[i].l_offset.term) {
      // trunk to found_offsett
      stable_logger_->Logger()->Truncate(
          found_offset.b_offset.filenum, found_offset.b_offset.offset);
      *reply_offset = mem_logger_->last_offset();
      return Status::OK();
    }
  }
  return Status::Corruption("hints not found");
}
