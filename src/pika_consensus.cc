// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <utility>

#include "include/pika_consensus.h"

#include "include/pika_client_conn.h"
#include "include/pika_cmd_table_manager.h"
#include "include/pika_conf.h"
#include "include/pika_rm.h"
#include "include/pika_server.h"

using pstd::Status;

extern PikaServer* g_pika_server;
extern std::unique_ptr<PikaConf> g_pika_conf;
extern std::unique_ptr<PikaReplicaManager> g_pika_rm;
extern std::unique_ptr<PikaCmdTableManager> g_pika_cmd_table_manager;

/* Context */

Context::Context(std::string path) :  path_(std::move(path)) {}

Status Context::StableSave() {
  char* p = save_->GetData();
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
  if (!pstd::FileExists(path_)) {
    Status s = pstd::NewRWFile(path_, save_);
    if (!s.ok()) {
      LOG(FATAL) << "Context new file failed " << s.ToString();
    }
    StableSave();
  } else {
    std::unique_ptr<pstd::RWFile> tmp_file;
    Status s = pstd::NewRWFile(path_, tmp_file);
    save_.reset(tmp_file.release());
    if (!s.ok()) {
      LOG(FATAL) << "Context new file failed " << s.ToString();
    }
  }
  if (save_->GetData()) {
    memcpy(reinterpret_cast<char*>(&(applied_index_.b_offset.filenum)), save_->GetData(), sizeof(uint32_t));
    memcpy(reinterpret_cast<char*>(&(applied_index_.b_offset.offset)), save_->GetData() + 4, sizeof(uint64_t));
    memcpy(reinterpret_cast<char*>(&(applied_index_.l_offset.term)), save_->GetData() + 12, sizeof(uint32_t));
    memcpy(reinterpret_cast<char*>(&(applied_index_.l_offset.index)), save_->GetData() + 16, sizeof(uint64_t));
    return Status::OK();
  } else {
    return Status::Corruption("Context init error");
  }
}

void Context::UpdateAppliedIndex(const LogOffset& offset) {
  std::lock_guard l(rwlock_);
  LogOffset cur_offset;
  applied_win_.Update(SyncWinItem(offset), SyncWinItem(offset), &cur_offset);
  if (cur_offset > applied_index_) {
    applied_index_ = cur_offset;
    StableSave();
  }
}

void Context::Reset(const LogOffset& offset) {
  std::lock_guard l(rwlock_);
  applied_index_ = offset;
  applied_win_.Reset();
  StableSave();
}

/* SyncProgress */

std::string MakeSlaveKey(const std::string& ip, int port) {
  return ip + ":" + std::to_string(port);
}

std::shared_ptr<SlaveNode> SyncProgress::GetSlaveNode(const std::string& ip, int port) {
  std::string slave_key = MakeSlaveKey(ip, port);
  std::shared_lock l(rwlock_);
  if (slaves_.find(slave_key) == slaves_.end()) {
    return nullptr;
  }
  return slaves_[slave_key];
}

std::unordered_map<std::string, std::shared_ptr<SlaveNode>> SyncProgress::GetAllSlaveNodes() {
  std::shared_lock l(rwlock_);
  return slaves_;
}

Status SyncProgress::AddSlaveNode(const std::string& ip, int port, const std::string& db_name, int session_id) {
  std::string slave_key = MakeSlaveKey(ip, port);
  std::shared_ptr<SlaveNode> exist_ptr = GetSlaveNode(ip, port);
  if (exist_ptr) {
    LOG(WARNING) << "SlaveNode " << exist_ptr->ToString() << " already exist, set new session " << session_id;
    exist_ptr->SetSessionId(session_id);
    return Status::OK();
  }
  std::shared_ptr<SlaveNode> slave_ptr = std::make_shared<SlaveNode>(ip, port, db_name, session_id);
  slave_ptr->SetLastSendTime(pstd::NowMicros());
  slave_ptr->SetLastRecvTime(pstd::NowMicros());

  {
    std::lock_guard l(rwlock_);
    slaves_[slave_key] = slave_ptr;
    // add slave to match_index
    match_index_[slave_key] = LogOffset();
  }
  return Status::OK();
}

Status SyncProgress::RemoveSlaveNode(const std::string& ip, int port) {
  std::string slave_key = MakeSlaveKey(ip, port);
  {
    std::lock_guard l(rwlock_);
    slaves_.erase(slave_key);
    // remove slave to match_index
    match_index_.erase(slave_key);
  }
  return Status::OK();
}

Status SyncProgress::Update(const std::string& ip, int port, const LogOffset& start, const LogOffset& end,
                            LogOffset* committed_index) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    return Status::NotFound("ip " + ip + " port " + std::to_string(port));
  }

  LogOffset acked_offset;
  {
    // update slave_ptr
    std::lock_guard l(slave_ptr->slave_mu);
    Status s = slave_ptr->Update(start, end, &acked_offset);
    if (!s.ok()) {
      return s;
    }
    // update match_index_
    // shared slave_ptr->slave_mu
    match_index_[ip + std::to_string(port)] = acked_offset;
  }

  return Status::OK();
}

int SyncProgress::SlaveSize() {
  std::shared_lock l(rwlock_);
  return static_cast<int32_t>(slaves_.size());
}

/* MemLog */

MemLog::MemLog()  = default;

int MemLog::Size() { return static_cast<int>(logs_.size()); }

// keep mem_log [mem_log.begin, offset]
Status MemLog::TruncateTo(const LogOffset& offset) {
  std::lock_guard l_logs(logs_mu_);
  int index = InternalFindLogByBinlogOffset(offset);
  if (index < 0) {
    return Status::Corruption("Cant find correct index");
  }
  last_offset_ = logs_[index].offset;
  logs_.erase(logs_.begin() + index + 1, logs_.end());
  return Status::OK();
}

void MemLog::Reset(const LogOffset& offset) {
  std::lock_guard l_logs(logs_mu_);
  logs_.erase(logs_.begin(), logs_.end());
  last_offset_ = offset;
}

bool MemLog::FindLogItem(const LogOffset& offset, LogOffset* found_offset) {
  std::lock_guard l_logs(logs_mu_);
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
      return static_cast<int32_t>(i);
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
      return static_cast<int32_t>(i);
    }
  }
  return -1;
}

/* ConsensusCoordinator */

ConsensusCoordinator::ConsensusCoordinator(const std::string& db_name)
    : db_name_(db_name) {
  std::string db_log_path = g_pika_conf->log_path() + "log_" + db_name + "/";
  std::string log_path = db_log_path;
  context_ = std::make_shared<Context>(log_path + kContext);
  stable_logger_ = std::make_shared<StableLog>(db_name, log_path);
  mem_logger_ = std::make_shared<MemLog>();
}

ConsensusCoordinator::~ConsensusCoordinator() = default;

// since it is invoked in constructor all locks not hold
void ConsensusCoordinator::Init() {
  // load committed_index_ & applied_index
  context_->Init();
  committed_index_ = context_->applied_index_;

  // load term_
  term_ = stable_logger_->Logger()->term();

  LOG(INFO) << DBInfo(db_name_).ToString() << "Restore applied index "
            << context_->applied_index_.ToString() << " current term " << term_;
  if (committed_index_ == LogOffset()) {
    return;
  }
  // load mem_logger_
  mem_logger_->SetLastOffset(committed_index_);
  net::RedisParserSettings settings;
  settings.DealMessage = &(ConsensusCoordinator::InitCmd);
  net::RedisParser redis_parser;
  redis_parser.RedisParserInit(REDIS_PARSER_REQUEST, settings);
  PikaBinlogReader binlog_reader;
  int res =
      binlog_reader.Seek(stable_logger_->Logger(), committed_index_.b_offset.filenum, committed_index_.b_offset.offset);
  if (res != 0) {
    LOG(FATAL) << DBInfo(db_name_).ToString() << "Binlog reader init failed";
  }

  while (true) {
    LogOffset offset;
    std::string binlog;
    Status s = binlog_reader.Get(&binlog, &(offset.b_offset.filenum), &(offset.b_offset.offset));
    if (s.IsEndFile()) {
      break;
    } else if (s.IsCorruption() || s.IsIOError()) {
      LOG(FATAL) << DBInfo(db_name_).ToString() << "Read Binlog error";
    }
    BinlogItem item;
    if (!PikaBinlogTransverter::BinlogItemWithoutContentDecode(TypeFirst, binlog, &item)) {
      LOG(FATAL) << DBInfo(db_name_).ToString() << "Binlog item decode failed";
    }
    offset.l_offset.term = item.term_id();
    offset.l_offset.index = item.logic_id();

    redis_parser.data = static_cast<void*>(&db_name_);
    const char* redis_parser_start = binlog.data() + BINLOG_ENCODE_LEN;
    int redis_parser_len = static_cast<int>(binlog.size()) - BINLOG_ENCODE_LEN;
    int processed_len = 0;
    net::RedisParserStatus ret = redis_parser.ProcessInputBuffer(redis_parser_start, redis_parser_len, &processed_len);
    if (ret != net::kRedisParserDone) {
      LOG(FATAL) << DBInfo(db_name_).ToString() << "Redis parser parse failed";
      return;
    }
    auto arg = static_cast<CmdPtrArg*>(redis_parser.data);
    std::shared_ptr<Cmd> cmd_ptr = arg->cmd_ptr;
    delete arg;
    redis_parser.data = nullptr;

    mem_logger_->AppendLog(MemLog::LogItem(offset, cmd_ptr, nullptr, nullptr));
  }
}

Status ConsensusCoordinator::Reset(const LogOffset& offset) {
  context_->Reset(offset);
  {
    std::lock_guard l(index_mu_);
    committed_index_ = offset;
  }

  UpdateTerm(offset.l_offset.term);
  Status s = stable_logger_->Logger()->SetProducerStatus(offset.b_offset.filenum, offset.b_offset.offset,
                                                         offset.l_offset.term, offset.l_offset.index);
  if (!s.ok()) {
    LOG(WARNING) << DBInfo(db_name_).ToString() << "Consensus reset status failed "
                 << s.ToString();
    return s;
  }

  stable_logger_->SetFirstOffset(offset);

  stable_logger_->Logger()->Lock();
  mem_logger_->Reset(offset);
  stable_logger_->Logger()->Unlock();
  return Status::OK();
}

Status ConsensusCoordinator::ProposeLog(const std::shared_ptr<Cmd>& cmd_ptr) {
  std::vector<std::string> keys = cmd_ptr->current_key();
  // slotkey shouldn't add binlog
  if (cmd_ptr->name() == kCmdNameSAdd && !keys.empty() &&
      (keys[0].compare(0, SlotKeyPrefix.length(), SlotKeyPrefix) == 0 || keys[0].compare(0, SlotTagPrefix.length(), SlotTagPrefix) == 0)) {
    return Status::OK();
  }

  // make sure stable log and mem log consistent
  Status s = InternalAppendLog(cmd_ptr);
  if (!s.ok()) {
    return s;
  }

  g_pika_server->SignalAuxiliary();
  return Status::OK();
}

Status ConsensusCoordinator::InternalAppendLog(const std::shared_ptr<Cmd>& cmd_ptr) {
  return InternalAppendBinlog(cmd_ptr);
}

// precheck if prev_offset match && drop this log if this log exist
Status ConsensusCoordinator::ProcessLeaderLog(const std::shared_ptr<Cmd>& cmd_ptr, const BinlogItem& attribute) {
  LogOffset last_index = mem_logger_->last_offset();
  if (attribute.logic_id() < last_index.l_offset.index) {
    LOG(WARNING) << DBInfo(db_name_).ToString() << "Drop log from leader logic_id "
                 << attribute.logic_id() << " cur last index " << last_index.l_offset.index;
    return Status::OK();
  }

  auto opt = cmd_ptr->argv()[0];
  if (pstd::StringToLower(opt) != kCmdNameFlushdb) {
    // apply binlog in sync way
    Status s = InternalAppendLog(cmd_ptr);
    // apply db in async way
    IncrUnfinishedAsyncWriteDbTaskCount(1);
    std::function<void()> call_back = [this]() { this->DecrUnfinishedAsyncWriteDbTaskCount(1); };
    InternalApplyFollower(cmd_ptr, call_back);
  } else {
    LOG(INFO) << "Writing flushdb binlog in sync way";
    // this is a flushdb-binlog, both apply binlog and apply db are in sync way
    // and flushdb should wait until all async WriteDB task submitted before flushdb finished exec
    //ensure all writeDB task has finished before we exec flushdb
    int32_t wait_ms = 250;
    while (unfinished_async_write_db_task_count_.load(std::memory_order::memory_order_seq_cst) > 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(wait_ms));
      wait_ms *= 2;
      wait_ms = wait_ms < 3000 ? wait_ms : 3000;
    }
    // apply flushdb-binlog in sync way
    Status s = InternalAppendLog(cmd_ptr);
    // applyDB in sync way
    PikaReplBgWorker::WriteDBInSyncWay(cmd_ptr);
  }
  return Status::OK();
}

Status ConsensusCoordinator::UpdateSlave(const std::string& ip, int port, const LogOffset& start,
                                         const LogOffset& end) {
  LogOffset committed_index;
  Status s = sync_pros_.Update(ip, port, start, end, &committed_index);
  if (!s.ok()) {
    return s;
  }

  return Status::OK();
}

Status ConsensusCoordinator::InternalAppendBinlog(const std::shared_ptr<Cmd>& cmd_ptr) {
  std::string content = cmd_ptr->ToRedisProtocol();
  Status s = stable_logger_->Logger()->Put(content);
  if (!s.ok()) {
    std::string db_name = cmd_ptr->db_name().empty() ? g_pika_conf->default_db() : cmd_ptr->db_name();
    std::shared_ptr<DB> db = g_pika_server->GetDB(db_name);
    if (db) {
      db->SetBinlogIoError();
    }
    return s;
  }
  return stable_logger_->Logger()->IsOpened();
}

Status ConsensusCoordinator::AddSlaveNode(const std::string& ip, int port, int session_id) {
  Status s = sync_pros_.AddSlaveNode(ip, port, db_name_, session_id);
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
  std::lock_guard l(term_rwlock_);
  term_ = term;
  stable_logger_->Logger()->SetTerm(term);
  stable_logger_->Logger()->Unlock();
}

uint32_t ConsensusCoordinator::term() {
  std::shared_lock l(term_rwlock_);
  return term_;
}

void ConsensusCoordinator::InternalApplyFollower(std::shared_ptr<Cmd> cmd_ptr, std::function<void()>& call_back_fun) {
  g_pika_rm->ScheduleWriteDBTask(std::move(cmd_ptr), call_back_fun);
}

int ConsensusCoordinator::InitCmd(net::RedisParser* parser, const net::RedisCmdArgsType& argv) {
  auto db_name = static_cast<std::string*>(parser->data);
  std::string opt = argv[0];
  std::shared_ptr<Cmd> c_ptr = g_pika_cmd_table_manager->GetCmd(pstd::StringToLower(opt));
  if (!c_ptr) {
    LOG(WARNING) << "Command " << opt << " not in the command table";
    return -1;
  }
  // Initial
  c_ptr->Initial(argv, *db_name);
  if (!c_ptr->res().ok()) {
    LOG(WARNING) << "Fail to initial command from binlog: " << opt;
    return -1;
  }
  parser->data = static_cast<void*>(new CmdPtrArg(c_ptr));
  return 0;
}

Status ConsensusCoordinator::TruncateTo(const LogOffset& offset) {
  LOG(INFO) << DBInfo(db_name_).ToString() << "Truncate to " << offset.ToString();
  LogOffset founded_offset;
  Status s = FindLogicOffset(offset.b_offset, offset.l_offset.index, &founded_offset);
  if (!s.ok()) {
    return s;
  }
  LOG(INFO) << DBInfo(db_name_).ToString() << " Founded truncate pos "
            << founded_offset.ToString();
  LogOffset committed = committed_index();
  stable_logger_->Logger()->Lock();
  if (founded_offset.l_offset.index == committed.l_offset.index) {
    mem_logger_->Reset(committed);
  } else {
    Status s = mem_logger_->TruncateTo(founded_offset);
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
  int res = binlog_reader.Seek(stable_logger_->Logger(), start_offset.filenum, start_offset.offset);
  if (res != 0) {
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
Status ConsensusCoordinator::GetBinlogOffset(const BinlogOffset& start_offset, const BinlogOffset& end_offset,
                                             std::vector<LogOffset>* log_offset) {
  PikaBinlogReader binlog_reader;
  int res = binlog_reader.Seek(stable_logger_->Logger(), start_offset.filenum, start_offset.offset);
  if (res != 0) {
    return Status::Corruption("Binlog reader init failed");
  }
  while (true) {
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

Status ConsensusCoordinator::FindBinlogFileNum(const std::map<uint32_t, std::string>& binlogs, uint64_t target_index,
                                               uint32_t start_filenum, uint32_t* founded_filenum) {
  // low boundary & high boundary
  uint32_t lb_binlogs = binlogs.begin()->first;
  uint32_t hb_binlogs = binlogs.rbegin()->first;
  bool first_time_left = false;
  bool first_time_right = false;
  uint32_t filenum = start_filenum;
  while (true) {
    LogOffset first_offset;
    Status s = GetBinlogOffset(BinlogOffset(filenum, 0), &first_offset);
    if (!s.ok()) {
      return s;
    }
    if (target_index < first_offset.l_offset.index) {
      if (first_time_right) {
        // last filenum
        filenum = filenum - 1;
        break;
      }
      // move left
      first_time_left = true;
      if (filenum == 0 || filenum - 1 < lb_binlogs) {
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

Status ConsensusCoordinator::FindLogicOffsetBySearchingBinlog(const BinlogOffset& hint_offset, uint64_t target_index,
                                                              LogOffset* found_offset) {
  LOG(INFO) << DBInfo(db_name_).ToString() << "FindLogicOffsetBySearchingBinlog hint offset "
            << hint_offset.ToString() << " target_index " << target_index;
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

  LOG(INFO) << DBInfo(db_name_).ToString() << "FindBinlogFilenum res "  // NOLINT
            << found_filenum;
  BinlogOffset traversal_start(found_filenum, 0);
  BinlogOffset traversal_end(found_filenum + 1, 0);
  std::vector<LogOffset> offsets;
  s = GetBinlogOffset(traversal_start, traversal_end, &offsets);
  if (!s.ok()) {
    return s;
  }
  for (auto& offset : offsets) {
    if (offset.l_offset.index == target_index) {
      LOG(INFO) << DBInfo(db_name_).ToString() << "Founded " << target_index << " "
                << offset.ToString();
      *found_offset = offset;
      return Status::OK();
    }
  }
  return Status::NotFound("Logic index not found");
}

Status ConsensusCoordinator::FindLogicOffset(const BinlogOffset& start_offset, uint64_t target_index,
                                             LogOffset* found_offset) {
  LogOffset possible_offset;
  Status s = GetBinlogOffset(start_offset, &possible_offset);
  if (!s.ok() || possible_offset.l_offset.index != target_index) {
    if (!s.ok()) {
      LOG(INFO) << DBInfo(db_name_).ToString() << "GetBinlogOffset res: " << s.ToString();
    } else {
      LOG(INFO) << DBInfo(db_name_).ToString() << "GetBInlogOffset res: " << s.ToString()
                << " possible_offset " << possible_offset.ToString() << " target_index " << target_index;
    }
    return FindLogicOffsetBySearchingBinlog(start_offset, target_index, found_offset);
  }
  *found_offset = possible_offset;
  return Status::OK();
}

Status ConsensusCoordinator::GetLogsBefore(const BinlogOffset& start_offset, std::vector<LogOffset>* hints) {
  BinlogOffset traversal_end = start_offset;
  BinlogOffset traversal_start(traversal_end.filenum, 0);
  traversal_start.filenum = traversal_start.filenum == 0 ? 0 : traversal_start.filenum - 1;
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

Status ConsensusCoordinator::LeaderNegotiate(const LogOffset& f_last_offset, bool* reject,
                                             std::vector<LogOffset>* hints) {
  uint64_t f_index = f_last_offset.l_offset.index;
  LOG(INFO) << DBInfo(db_name_).ToString() << "LeaderNeotiate follower last offset "
            << f_last_offset.ToString() << " first_offsert " << stable_logger_->first_offset().ToString()
            << " last_offset " << mem_logger_->last_offset().ToString();
  *reject = true;
  if (f_index > mem_logger_->last_offset().l_offset.index) {
    // hints starts from last_offset() - 100;
    Status s = GetLogsBefore(mem_logger_->last_offset().b_offset, hints);
    if (!s.ok()) {
      LOG(WARNING) << f_index << " is larger than last index " << mem_logger_->last_offset().ToString()
                   << " get logs before last index failed " << s.ToString();
      return s;
    }
    LOG(INFO) << DBInfo(db_name_).ToString()
              << "follower index larger then last_offset index, get logs before "
              << mem_logger_->last_offset().ToString();
    return Status::OK();
  }
  if (f_index < stable_logger_->first_offset().l_offset.index) {
    // need full sync
    LOG(INFO) << DBInfo(db_name_).ToString() << f_index << " not found current first index"
              << stable_logger_->first_offset().ToString();
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
      LOG(INFO) << DBInfo(db_name_).ToString() << f_last_offset.ToString() << " not found "
                << s.ToString();
      return s;
    } else {
      LOG(WARNING) << DBInfo(db_name_).ToString() << "find logic offset failed"
                   << s.ToString();
      return s;
    }
  }

  if (found_offset.l_offset.term != f_last_offset.l_offset.term || !(f_last_offset.b_offset == found_offset.b_offset)) {
    Status s = GetLogsBefore(found_offset.b_offset, hints);
    if (!s.ok()) {
      LOG(WARNING) << DBInfo(db_name_).ToString() << "Try to get logs before "
                   << found_offset.ToString() << " failed";
      return s;
    }
    return Status::OK();
  }

  LOG(INFO) << DBInfo(db_name_).ToString() << "Found equal offset " << found_offset.ToString();
  *reject = false;
  return Status::OK();
}

// memlog order: committed_index , [committed_index + 1, memlogger.end()]
Status ConsensusCoordinator::FollowerNegotiate(const std::vector<LogOffset>& hints, LogOffset* reply_offset) {
  if (hints.empty()) {
    return Status::Corruption("hints empty");
  }
  LOG(INFO) << DBInfo(db_name_).ToString() << "FollowerNegotiate from " << hints[0].ToString()
            << " to " << hints[hints.size() - 1].ToString();
  if (mem_logger_->last_offset().l_offset.index < hints[0].l_offset.index) {
    *reply_offset = mem_logger_->last_offset();
    return Status::OK();
  }
  if (committed_index().l_offset.index > hints[hints.size() - 1].l_offset.index) {
    return Status::Corruption("invalid hints all smaller than committed_index");
  }
  if (mem_logger_->last_offset().l_offset.index > hints[hints.size() - 1].l_offset.index) {
    const auto &truncate_offset = hints[hints.size() - 1];
    // trunck to hints end
    Status s = TruncateTo(truncate_offset);
    if (!s.ok()) {
      return s;
    }
  }

  LogOffset committed = committed_index();
  for (size_t i = hints.size() - 1; i >= 0; i--) {
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
