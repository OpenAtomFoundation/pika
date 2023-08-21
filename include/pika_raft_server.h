// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_RAFT_SERVER_H_
#define PIKA_RAFT_SERVER_H_

#include <libnuraft/nuraft.hxx>

#include "pstd/include/pstd_status.h"
#include "include/pika_raft_state_machine.h"
#include "include/pika_raft_logger_wrapper.h"
#include "include/pika_raft_state_manager.h"
#include "include/pika_server.h"

using pstd::Status;

class PikaRaftServer {
 public:
 	struct CmdPtrArg {
    CmdPtrArg(std::shared_ptr<Cmd> ptr) : cmd_ptr(std::move(ptr)) {}
    std::shared_ptr<Cmd> cmd_ptr;
  };

	using raft_result = nuraft::cmd_result<nuraft::ptr<nuraft::buffer>>;

	PikaRaftServer();
	~PikaRaftServer();
	void reset();
	Status AppendRaftlog(const std::shared_ptr<Cmd>& cmd_ptr, std::shared_ptr<PikaClientConn> conn_ptr,
											 std::shared_ptr<std::string> resp_ptr, std::string _db_name, uint32_t _slot_id);
	void HandleRaftLogResult(Status& s, raft_result& result, nuraft::ptr<std::exception>& err);
	void Start();
	bool HasLeader();
	bool IsLeader();
	void PrecommitLog(ulong log_idx, uint32_t slot_id, std::string db_name, std::string binlog);
	void RollbackLog(ulong log_idx, uint32_t slot_id, std::string db_name, std::string binlog);
	void ApplyLog(ulong log_idx, uint32_t slot_id, std::string db_name, std::string binlog);

 private:
 	std::string GetNetIP();
	// Server ID.
	int32_t server_id_ = 0;
	// Server address.
	std::string addr_ = "";
	// Server port.
	int port_ = 0;
	// Endpoint: `<addr>:<port>`.
	std::string endpoint_ = "";
	// Logger.
	nuraft::ptr<PikaRaftLoggerWrapper> raft_logger_ = nullptr;
	// State machine.
	nuraft::ptr<PikaStateMachine> sm_ = nullptr;
	std::shared_mutex sm_mutex_;
	// State manager.
	nuraft::ptr<PikaRaftStateManager> smgr_ = nullptr;
	std::shared_mutex smgr_mutex_;
	// Raft launcher.
	nuraft::raft_launcher launcher_;
	// Raft server instance.
	nuraft::ptr<nuraft::raft_server> raft_instance_ = nullptr;
	std::shared_mutex raft_mutex_;

	std::shared_ptr<MemLog> mem_logger_;

};

#endif