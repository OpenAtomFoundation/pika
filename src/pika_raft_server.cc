// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_raft_server.h"

#include "net/include/net_interfaces.h"

#include "include/pika_consensus.h"
#include "include/pika_conf.h"

extern std::unique_ptr<PikaConf> g_pika_conf;
extern PikaServer* g_pika_server;

PikaRaftServer::PikaRaftServer()
		: server_id_(g_pika_conf->raft_server_id())
		, addr_(GetNetIP())
		, port_(g_pika_conf->port() + kPortShiftRaftServer)
		, endpoint_(addr_ + ":" + std::to_string(port_))
		, raft_logger_(nullptr)
		, sm_(nullptr)
		, smgr_(nullptr)
		, raft_instance_(nullptr)
		, mem_logger_(std::make_shared<MemLog>())
		{}

PikaRaftServer::~PikaRaftServer() {
	LOG(INFO) << "PikaRaftServer exit!!!";
}

void PikaRaftServer::Start() {
	// State machine.
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
	
	// State manager.
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
	}

	// ASIO options.
	nuraft::asio_service::options asio_opt;
	asio_opt.thread_pool_size_ = g_pika_conf->asio_thread_pool_size();

	// Raft parameters.
	nuraft::raft_params params;
	params.heart_beat_interval_ = g_pika_conf->heart_beat_interval();
	params.election_timeout_lower_bound_ = g_pika_conf->election_timeout_lower_bound();
	params.election_timeout_upper_bound_ = g_pika_conf->election_timeout_upper_bound();
	// Upto 5 logs will be preserved ahead the last snapshot.
	params.reserved_log_items_ = g_pika_conf->reserved_log_items();
	// Snapshot will be created for every 5 log appends.
	params.snapshot_distance_ = g_pika_conf->snapshot_distance();
	// Client timeout: 3000 ms.
	params.client_req_timeout_ = g_pika_conf->client_req_timeout();
	// async or not
	if (g_pika_conf->async_log_append()) {
		params.return_method_ = nuraft::raft_params::async_handler;
	} else {
		params.return_method_ = nuraft::raft_params::blocking;
	}
	

	// append log on follower re-direct to leader
	params.auto_forwarding_ = g_pika_conf->auto_forwarding();
  // max connection of auto forwarding
	params.auto_forwarding_max_connections_ = g_pika_conf->auto_forwarding_max_connections();
	// auto forwarding timeout: 3000 ms.
	params.auto_forwarding_req_timeout_ = g_pika_conf->auto_forwarding_req_timeout();

	// Initialize Raft server.
	{
	std::lock_guard l(raft_mutex_);
	raft_instance_ = launcher_.init(sm_,
																	smgr_,
																	raft_logger_,
																	port_,
																	asio_opt,
																	params);
	}

	std::lock_guard l(raft_mutex_);
	if (!raft_instance_) {
		log_wrap.reset();
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

void PikaRaftServer::HandleRaftLogResult(Status& s, raft_result& result, nuraft::ptr<std::exception>& err) {
	if (result.get_result_code() != nuraft::cmd_result_code::OK) {
    // Something went wrong.
    // This means committing this log failed, but the log itself is still in the log store.
    s = Status::Incomplete("Commit Log Failed");
    return;
  }
	s = Status::OK();
	return;
}

//TODO(lap): batched append logs
Status PikaRaftServer::AppendRaftlog(const std::shared_ptr<Cmd>& cmd_ptr, std::shared_ptr<PikaClientConn> conn_ptr,
                                        std::shared_ptr<std::string> resp_ptr, std::string _db_name, uint32_t _slot_id) {
	uint32_t filenum;
	uint64_t offset;
	Status s;
	{
		std::lock_guard ll(sm_mutex_);
		s = sm_->GetLogStatus(filenum, offset);
	}
	if (!s.ok()) {
		return s;
	}
	std::string binlog =
		cmd_ptr->ToRaftlog(time(nullptr), filenum, offset);
	size_t content_size = sizeof(uint32_t) + _db_name.size() + binlog.size();

	// Create a new log which will contain
	// 4-byte length(store data size) and sizeof data.
	nuraft::ptr<nuraft::buffer> new_log = nuraft::buffer::alloc(3*sizeof(int) + content_size);
	nuraft::buffer_serializer bs(new_log);
	bs.put_u32(_slot_id);
	bs.put_str(_db_name);
	bs.put_str(binlog);

	// append local raftlog success, add to mem_logger 
	std::lock_guard l(raft_mutex_);
	LogOffset log_offset;
	log_offset.l_offset.term = raft_instance_->get_term();
  log_offset.l_offset.index = raft_instance_->get_last_log_idx() + 1;
	mem_logger_->AppendLog(MemLog::LogItem(log_offset, cmd_ptr, std::move(conn_ptr), std::move(resp_ptr)));
	
	nuraft::ptr<raft_result> raft_ret = raft_instance_->append_entries( {new_log} );

	// Log append Failed
	// TODO(lap): distinguish io error of binlog (i.e. no enough space left)
	//						from from raft service unavaliable
	if (!raft_ret->get_accepted()) {
			s = mem_logger_->TruncateToBefore(log_offset);
			if (!s.ok()) {
				LOG(WARNING) << "MemLog Corrupted! at " << log_offset.ToString();
			}
			s = Status::IOError("Raft Append Log Unaccepted");
			return s;
	}

	// Log append accepted, but that doesn't mean the log is committed.
  // Commit result can be obtained below.
  if (!g_pika_conf->async_log_append()) {
    // Blocking mode:
    //   "append_entries" returns after getting a consensus, so that "ret" already has the result from state machine.
    nuraft::ptr<std::exception> err(nullptr);
    HandleRaftLogResult(s, *raft_ret, err);
  } else {
    // Async mode:
    //   "append_entries" returns immediately. "HandleRaftLogResult" will be invoked asynchronously, after getting a consensus.
    raft_ret->when_ready(std::bind(&PikaRaftServer::HandleRaftLogResult, 
																		this,
																		s,
																		std::placeholders::_1, 
																		std::placeholders::_2));
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

void PikaRaftServer::PrecommitLog(ulong log_idx, uint32_t slot_id, std::string db_name, std::string binlog) {
	LogOffset offset;
  BinlogItem item;
  if (!PikaBinlogTransverter::BinlogItemWithoutContentDecode(TypeFirst, binlog, &item)) {
    LOG(FATAL) << SlotInfo(db_name, slot_id).ToString() << "Binlog item decode failed";
  }
	offset.b_offset = BinlogOffset(item.filenum(), item.offset());
	offset.l_offset.term = raft_instance_->get_log_term(log_idx);
	offset.l_offset.index = log_idx;

  net::RedisParserSettings settings;
  settings.DealMessage = &(ConsensusCoordinator::InitCmd);
  net::RedisParser redis_parser;
  redis_parser.RedisParserInit(REDIS_PARSER_REQUEST, settings);

  redis_parser.data = static_cast<void*>(&db_name);
  const char* redis_parser_start = binlog.data() + BINLOG_ENCODE_LEN;
  int redis_parser_len = static_cast<int>(binlog.size()) - BINLOG_ENCODE_LEN;
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

	LogOffset found_offset;
	if(!mem_logger_->FindLogItem(offset, &found_offset)) {
  	mem_logger_->AppendLog(MemLog::LogItem(offset, cmd_ptr, nullptr, nullptr));
	}
}

void PikaRaftServer::RollbackLog(ulong log_idx, uint32_t slot_id, std::string db_name, std::string binlog) {
	LogOffset offset;
  BinlogItem item;
  if (!PikaBinlogTransverter::BinlogItemWithoutContentDecode(TypeFirst, binlog, &item)) {
    LOG(FATAL) << SlotInfo(db_name, slot_id).ToString() << "Binlog item decode failed";
  }
	offset.b_offset = BinlogOffset(item.filenum(), item.offset());
	offset.l_offset.term = raft_instance_->get_log_term(log_idx);
	offset.l_offset.index = log_idx;

	mem_logger_->TruncateToBefore(offset);
}

void PikaRaftServer::ApplyLog(ulong log_idx, uint32_t slot_id, std::string db_name, std::string binlog) {
	
	BinlogItem item;
  if (!PikaBinlogTransverter::BinlogItemWithoutContentDecode(TypeFirst, binlog, &item)) {
    LOG(FATAL) << SlotInfo(db_name, slot_id).ToString() << "Binlog item decode failed";
  }

	LogOffset offset;
	offset.b_offset = BinlogOffset(item.filenum(), item.offset());
	offset.l_offset.term = raft_instance_->get_log_term(log_idx);
	offset.l_offset.index = log_idx;

	std::vector<MemLog::LogItem> logs;
	mem_logger_->PurgeLogs(offset, &logs);
	for (const auto log: logs) {
		auto arg = new PikaClientConn::BgTaskArg();
		arg->cmd_ptr = log.cmd_ptr;
		arg->conn_ptr = log.conn_ptr;
		arg->resp_ptr = log.resp_ptr;
		arg->offset = log.offset;
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