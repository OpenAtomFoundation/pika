// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_raft_server.h"

#include "net/include/net_interfaces.h"

#include "include/pika_conf.h"

extern std::unique_ptr<PikaConf> g_pika_conf;
extern PikaServer* g_pika_server;


/* RaftMemLog */

RaftMemLog::RaftMemLog()  = default;

int RaftMemLog::Size() { return static_cast<int>(logs_.size()); }

// purge [begin, offset]
Status RaftMemLog::PurgeLogs(const uint64_t& log_index, std::vector<LogItem>* logs) {
  std::lock_guard l_logs(logs_mu_);
  int index = InternalFindLogByLogIndex(log_index);
  if (index < 0) {
    return Status::NotFound("Cant find correct index");
  }
	last_commit_index_ = log_index;
  logs->assign(logs_.begin(), logs_.begin() + index + 1);
  logs_.erase(logs_.begin(), logs_.begin() + index + 1);
  return Status::OK();
}

// remove logitem
Status RaftMemLog::Remove(const LogItem& item) {
	std::lock_guard l_logs(logs_mu_);
  int index = InternalFindLogByLogContent(item);
  if (index < 0) {
    return Status::NotFound("Cant find correct index");
  }
  logs_.erase(logs_.begin() + index, logs_.begin() + index + 1);
  return Status::OK();
}

// keep mem_log [mem_log.begin, offset]
Status RaftMemLog::TruncateTo(const uint64_t& log_index, std::vector<LogItem>* logs) {
  std::lock_guard l_logs(logs_mu_);
  int index = InternalFindLogByLogIndex(log_index);
  if (index < 0) {
    return Status::Corruption("Cant find correct index");
  }
	logs->assign(logs_.begin() + index + 1, logs_.end());
  logs_.erase(logs_.begin() + index + 1, logs_.end());
  return Status::OK();
}

void RaftMemLog::Reset(const uint64_t& log_index, std::vector<LogItem>* logs) {
  std::lock_guard l_logs(logs_mu_);
	logs->assign(logs_.begin(), logs_.end());
  logs_.erase(logs_.begin(), logs_.end());
  last_commit_index_ = log_index;
}

bool RaftMemLog::FindLogItem(const uint64_t& log_index, uint64_t* found_index) {
  std::lock_guard l_logs(logs_mu_);
  int index = InternalFindLogByLogIndex(log_index);
  if (index < 0) {
    return false;
  }
  *found_index = logs_[index].index;
  return true;
}

int RaftMemLog::InternalFindLogByLogIndex(const uint64_t& log_index) {
	if (last_commit_index_ >= log_index) return -1;
	uint64_t duplicate_log_idx = 0;
	uint64_t duplicate_count = 0;
  for (size_t i = 0; i < logs_.size(); ++i) {
		if (duplicate_log_idx == logs_[i].index) {
			duplicate_count ++;
		} else {
			duplicate_log_idx = logs_[i].index;
			duplicate_count = 0;
		}
		if (duplicate_log_idx + duplicate_count > log_index) {
      return -1;
    }
    if (duplicate_log_idx + duplicate_count == log_index) {
      return static_cast<int64_t>(i);
    }
  }
  return -1;
}

int RaftMemLog::InternalFindLogByLogContent(const LogItem& item) {
	for (size_t i = 0; i < logs_.size(); ++i) {
		if (logs_[i].index > item.index) {
      return -1;
    }
    if ((logs_[i].index == item.index)
				&& (logs_[i].cmd_ptr == item.cmd_ptr)
				&& (logs_[i].conn_ptr == item.conn_ptr)
				&& (logs_[i].resp_ptr == item.resp_ptr)) {
      return static_cast<int64_t>(i);
    }
  }
  return -1;
}

/* PikaRaftServer */

PikaRaftServer::PikaRaftServer()
		: server_id_(g_pika_conf->raft_server_id())
		, addr_(GetNetIP())
		, port_(g_pika_conf->port() + kPortShiftRaftServer)
		, endpoint_(addr_ + ":" + std::to_string(port_))
		, raft_logger_(nullptr)
		, sm_(nullptr)
		, smgr_(nullptr)
		, raft_instance_(nullptr)
		, mem_logger_(std::make_shared<RaftMemLog>()) {
	// State manager.
	{
		std::lock_guard l(smgr_mutex_);
		smgr_ = nuraft::cs_new<PikaRaftStateManager>(server_id_
																								, endpoint_
																								, g_pika_conf->raft_cluster_endpoints()
																								, g_pika_conf->raft_path());
																								
	}

	// Logger.
	// TODO(lap): replace with glog
	nuraft::ptr<PikaRaftLoggerWrapper> log_wrap = nuraft::cs_new<PikaRaftLoggerWrapper>( g_pika_conf->log_path(), 4 );
	raft_logger_ = log_wrap;
	
	// State machine.
	{
		std::lock_guard l(sm_mutex_);
		sm_ = nuraft::cs_new<PikaStateMachine>(std::bind(&PikaRaftServer::PrecommitLog, 
																										this, 
																										std::placeholders::_1,
																										std::placeholders::_2,
																										std::placeholders::_3,
																										std::placeholders::_4),
																					 std::bind(&PikaRaftServer::RollbackLog, 
																										this, 
																										std::placeholders::_1,
																										std::placeholders::_2,
																										std::placeholders::_3,
																										std::placeholders::_4),
																					 std::bind(&PikaRaftServer::ApplyLog, 
																										this, 
																										std::placeholders::_1,
																										std::placeholders::_2,
																										std::placeholders::_3,
																										std::placeholders::_4));
		// Initialize Raft Memlog
		mem_logger_->SetLastCommitIndex(sm_->last_commit_index());
	}

	// ASIO options.
	asio_opt_.thread_pool_size_ = g_pika_conf->asio_thread_pool_size();

	// Raft parameters.
	params_.heart_beat_interval_ = g_pika_conf->heart_beat_interval();
	params_.election_timeout_lower_bound_ = g_pika_conf->election_timeout_lower_bound();
	params_.election_timeout_upper_bound_ = g_pika_conf->election_timeout_upper_bound();
	// Upto 5 logs will be preserved ahead the last snapshot.
	params_.reserved_log_items_ = g_pika_conf->reserved_log_items();
	// Snapshot will be created for every 5 log appends.
	params_.snapshot_distance_ = g_pika_conf->snapshot_distance();
	// Client timeout: 3000 ms.
	params_.client_req_timeout_ = g_pika_conf->client_req_timeout();
	// async or not
	if (g_pika_conf->async_log_append()) {
		params_.return_method_ = nuraft::raft_params::async_handler;
	} else {
		params_.return_method_ = nuraft::raft_params::blocking;
	}

	// append log on follower re-direct to leader
	params_.auto_forwarding_ = g_pika_conf->auto_forwarding();
  // max connection of auto forwarding
	if (g_pika_conf->auto_forwarding_limit_connections()) {
		params_.auto_forwarding_max_connections_ = g_pika_conf->auto_forwarding_max_connections();
	}
	// auto forwarding timeout.
	params_.auto_forwarding_req_timeout_ = g_pika_conf->auto_forwarding_req_timeout();
}

PikaRaftServer::~PikaRaftServer() {
	LOG(INFO) << "PikaRaftServer exit!!!";
}

void PikaRaftServer::Start() {
	// Launch Raft server.
	{
	std::lock_guard l(raft_mutex_);
	raft_instance_ = launcher_.init(sm_,
																	smgr_,
																	raft_logger_,
																	port_,
																	asio_opt_,
																	params_);
	}

	std::lock_guard l(raft_mutex_);
	if (!raft_instance_) {
		raft_logger_.reset();
		LOG(FATAL) << "Raft Server Launcher Failed";
	}

	LOG(INFO) << "Starting Pika Raft Service";
}

void PikaRaftServer::reset()  {
    raft_logger_.reset();
    {
      std::lock_guard l(sm_mutex_);
      sm_.reset();
    }

    {
      std::lock_guard l(smgr_mutex_);
      smgr_.reset();
    }

    {
      std::lock_guard l(raft_mutex_);
      raft_instance_.reset();
    }
}

void PikaRaftServer::HandleRaftLogResult(Status& s, RaftMemLog::LogItem& memlog_item, raft_result& result, nuraft::ptr<std::exception>& err) {
	// Log Store Unaccepted
	if (!result.get_accepted()) {
		// Memlog pop only when rollback or apply or here, 
		// so if it can't find target memlog meaning corruption
		s = mem_logger_->Remove(memlog_item);
		if (!s.ok()) {
			LOG(WARNING) << "MemLog Corrupted!";
		}
		s = Status::IOError("Raft Append Log Unaccepted, " + result.get_result_str());
		auto arg = new PikaClientConn::BgTaskArg();
		arg->cmd_ptr = memlog_item.cmd_ptr;
		arg->conn_ptr = memlog_item.conn_ptr;
		arg->resp_ptr = memlog_item.resp_ptr;
		arg->cmd_ptr->res().SetRes(CmdRes::kErrOther, s.ToString());
		g_pika_server->ScheduleClientBgThreads(PikaClientConn::DoRaftRollBackTask, arg, memlog_item.cmd_ptr->current_key().front());
		return;
	}
	// Something went wrong.
	// This means committing this log failed, but the log itself is still in the log store.
	if (result.get_result_code() != nuraft::cmd_result_code::OK) {
    s = Status::Incomplete("Commit Log Failed");
    return;
  }
	s = Status::OK();
	return;
}

//TODO(lap): batched append logs
Status PikaRaftServer::AppendRaftlog(const std::shared_ptr<Cmd>& cmd_ptr, std::shared_ptr<PikaClientConn> conn_ptr,
                                        std::shared_ptr<std::string> resp_ptr, std::string _db_name, uint32_t _slot_id) {
	Status s;
	std::string raftlog =
		cmd_ptr->ToRaftlog(time(nullptr));
	size_t content_size = sizeof(uint32_t) + _db_name.size() + raftlog.size();

	// Create a new log which will contain
	// 4-byte length(store data size) and sizeof data.
	nuraft::ptr<nuraft::buffer> new_log = nuraft::buffer::alloc(3*sizeof(int) + content_size);
	nuraft::buffer_serializer bs(new_log);
	bs.put_u32(_slot_id);
	bs.put_str(_db_name);
	bs.put_str(raftlog);

	// append local raftlog success, add to mem_logger
	uint64_t new_log_index;
	{
		std::lock_guard l(raft_mutex_);
		new_log_index = raft_instance_->get_last_log_idx() + 1;
	}
	// notice that last_log_index may be same for last log has not been in consensus
	// but memlog item with same index still keep order by real log_idx
	RaftMemLog::LogItem memlog_item(new_log_index, cmd_ptr, std::move(conn_ptr), std::move(resp_ptr));
	mem_logger_->AppendLog(memlog_item);
	
	nuraft::ptr<raft_result> raft_ret = raft_instance_->append_entries( {new_log} );

  if (!g_pika_conf->async_log_append()) {
    // Blocking mode:
    //   "append_entries" returns after getting a consensus, so that "ret" already has the result from state machine.
    nuraft::ptr<std::exception> err(nullptr);
    HandleRaftLogResult(s, memlog_item, *raft_ret, err);
  } else {
    // Async mode:
    //   "append_entries" returns immediately. "HandleRaftLogResult" will be invoked asynchronously, after getting a consensus.
    raft_ret->when_ready(std::bind(&PikaRaftServer::HandleRaftLogResult
																	, this
																	, s
																	, memlog_item
																	, std::placeholders::_1 
																	, std::placeholders::_2));
  }

  return s;
}

bool PikaRaftServer::HasLeader() {
	std::lock_guard l(raft_mutex_);
	return raft_instance_ && raft_instance_->is_leader_alive();
}

bool PikaRaftServer::IsLeader() {
	std::lock_guard l(raft_mutex_);
	return raft_instance_ && raft_instance_->is_leader();
}

void PikaRaftServer::PrecommitLog(ulong log_idx, uint32_t slot_id, std::string db_name, std::string raftlog) {
	uint64_t found_index;
	if(!mem_logger_->FindLogItem(log_idx, &found_index)) {
		net::RedisParserSettings settings;
		settings.DealMessage = &(ConsensusCoordinator::InitCmd);
		net::RedisParser redis_parser;
		redis_parser.RedisParserInit(REDIS_PARSER_REQUEST, settings);

		redis_parser.data = static_cast<void*>(&db_name);
		const char* redis_parser_start = raftlog.data() + RAFTLOG_ENCODE_LEN;
		int redis_parser_len = static_cast<int>(raftlog.size()) - RAFTLOG_ENCODE_LEN;
		int processed_len = 0;
		net::RedisParserStatus ret = redis_parser.ProcessInputBuffer(redis_parser_start, redis_parser_len, &processed_len);
		if (ret != net::kRedisParserDone) {
			LOG(FATAL) << SlotInfo(db_name, slot_id).ToString() << "Redis parser parse failed";
			return;
		}
		auto arg = static_cast<CmdPtrArg*>(redis_parser.data);
		std::shared_ptr<Cmd> cmd_ptr = arg->cmd_ptr;
		delete arg;
		redis_parser.data = nullptr;

  	mem_logger_->AppendLog(RaftMemLog::LogItem(log_idx, cmd_ptr, nullptr, nullptr));
	}
}

void PikaRaftServer::RollbackLog(ulong log_idx, uint32_t slot_id, std::string db_name, std::string raftlog) {
	uint64_t last_commit_idx = 0;
	{
		std::lock_guard l(raft_mutex_);
	 	last_commit_idx = raft_instance_->get_committed_log_idx();
	}
	
	std::vector<RaftMemLog::LogItem> logs;
	if (log_idx == last_commit_idx + 1) {
		mem_logger_->Reset(last_commit_idx, &logs);
	} else {
		mem_logger_->TruncateTo(log_idx - 1, &logs);
	}

	for (const auto log: logs) {
		auto arg = new PikaClientConn::BgTaskArg();
		arg->cmd_ptr = log.cmd_ptr;
		arg->conn_ptr = log.conn_ptr;
		arg->resp_ptr = log.resp_ptr;
		arg->cmd_ptr->res().SetRes(CmdRes::kErrOther, "Raft Log Lost Consensus in Majority, RollBack!");
		g_pika_server->ScheduleClientBgThreads(PikaClientConn::DoRaftRollBackTask, arg, log.cmd_ptr->current_key().front());
  }

}

void PikaRaftServer::ApplyLog(ulong log_idx, uint32_t slot_id, std::string db_name, std::string raftlog) {
	
	BinlogItem item;
  if (!PikaBinlogTransverter::RaftlogDecode(TypeFirst, raftlog, &item)) {
    LOG(FATAL) << SlotInfo(db_name, slot_id).ToString() << "Raftlog item decode failed";
  }

	std::vector<RaftMemLog::LogItem> logs;
	mem_logger_->PurgeLogs(log_idx, &logs);

	for (const auto log: logs) {
		auto arg = new PikaClientConn::BgTaskArg();
		arg->cmd_ptr = log.cmd_ptr;
		arg->conn_ptr = log.conn_ptr;
		arg->resp_ptr = log.resp_ptr;
		arg->db_name = db_name;
		arg->slot_id = slot_id;
		g_pika_server->ScheduleClientBgThreads(PikaClientConn::DoExecTask, arg, log.cmd_ptr->current_key().front());
  }
}

std::string PikaRaftServer::GetNetIP() {
	std::string network_interface = g_pika_conf->network_interface();
  if (network_interface.empty()) {
    network_interface = GetDefaultInterface();
  }
  if (network_interface.empty()) {
    LOG(FATAL) << "Can't get Networker Interface";
		return "";
  }
  std::string host_ = GetIpByInterface(network_interface);
  if (host_.empty()) {
    LOG(FATAL) << "can't get host ip for " << network_interface;
		return "";
  }
	return host_;
}