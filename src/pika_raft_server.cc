// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_raft_server.h"

#include "net/include/net_interfaces.h"

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
		, raft_instance_(nullptr) {
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

void PikaRaftServer::HandleRaftLogResult(RaftClientConn& cli_conn, raft_result& result, nuraft::ptr<std::exception>& err) {
	Status s = Status::OK();
	// Log Store Unaccepted
	if (!result.get_accepted()) {
		s = Status::IOError("Raft Append Log Unaccepted, " + result.get_result_str());
	} else if (result.get_result_code() != nuraft::cmd_result_code::OK) {
		 	// Something went wrong.
			// This means committing this log failed, but the log itself is still in the log store.
    s = Status::Incomplete("Commit Log Failed");
  }

	if (!s.ok()) {
		auto arg = new PikaClientConn::BgTaskArg();
		arg->cmd_ptr = cli_conn.cmd_ptr;
		arg->conn_ptr = cli_conn.conn_ptr;
		arg->resp_ptr = cli_conn.resp_ptr;
		arg->db_name = cli_conn.db_name;
		arg->slot_id = cli_conn.slot_id;
		arg->cmd_ptr->res().SetRes(CmdRes::kErrOther, s.ToString());
		g_pika_server->ScheduleClientBgThreads(PikaClientConn::DoRaftRollBackTask, arg, cli_conn.cmd_ptr->current_key().front());
	} else {
		auto arg = new PikaClientConn::BgTaskArg();
		arg->cmd_ptr = cli_conn.cmd_ptr;
		arg->conn_ptr = cli_conn.conn_ptr;
		arg->resp_ptr = cli_conn.resp_ptr;
		arg->db_name = cli_conn.db_name;
		arg->slot_id = cli_conn.slot_id;
		g_pika_server->ScheduleClientBgThreads(PikaClientConn::DoExecTask, arg, cli_conn.cmd_ptr->current_key().front());
  
	}
}

//TODO(lap): batched append logs
Status PikaRaftServer::AppendRaftlog(const std::shared_ptr<Cmd>& cmd_ptr, std::shared_ptr<PikaClientConn> conn_ptr,
                                        std::shared_ptr<std::string> resp_ptr, std::string _db_name, uint32_t _slot_id) {
	Status s = Status::OK();
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

	RaftClientConn cli_conn(_db_name, _slot_id, cmd_ptr, conn_ptr, resp_ptr);
	
	nuraft::ptr<raft_result> raft_ret = raft_instance_->append_entries( {new_log} );

  if (!g_pika_conf->async_log_append()) {
    // Blocking mode:
    //   "append_entries" returns after getting a consensus, so that "ret" already has the result from state machine.
    nuraft::ptr<std::exception> err(nullptr);
    HandleRaftLogResult(cli_conn, *raft_ret, err);
  } else {
    // Async mode:
    //   "append_entries" returns immediately. "HandleRaftLogResult" will be invoked asynchronously, after getting a consensus.
    raft_ret->when_ready(std::bind(&PikaRaftServer::HandleRaftLogResult
																	, this
																	, cli_conn
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
	// do nothing
}

void PikaRaftServer::RollbackLog(ulong log_idx, uint32_t slot_id, std::string db_name, std::string raftlog) {
	// do nothing
}

void PikaRaftServer::ApplyLog(ulong log_idx, uint32_t slot_id, std::string db_name, std::string raftlog) {
	// do nothing
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