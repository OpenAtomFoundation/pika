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

class RaftMemLog {
public:
  struct LogItem {
    LogItem(const uint64_t& _index, std::shared_ptr<Cmd> _cmd_ptr, std::shared_ptr<PikaClientConn> _conn_ptr,
            std::shared_ptr<std::string> _resp_ptr)
        : index(_index), cmd_ptr(std::move(_cmd_ptr)), conn_ptr(std::move(_conn_ptr)), resp_ptr(std::move(_resp_ptr)) {}
		uint64_t index;
    std::shared_ptr<Cmd> cmd_ptr;
    std::shared_ptr<PikaClientConn> conn_ptr;
    std::shared_ptr<std::string> resp_ptr;
  };

  RaftMemLog();
  int Size();
  void AppendLog(const LogItem& item) {
    std::lock_guard lock(logs_mu_);
    logs_.push_back(item);
  }
  pstd::Status PurgeLogs(const uint64_t& log_index, std::vector<LogItem>* logs);
	pstd::Status Remove(const LogItem& item);
  pstd::Status TruncateTo(const uint64_t& log_index, std::vector<LogItem>* logs);

  void Reset(const uint64_t& log_index, std::vector<LogItem>* logs);

  void SetLastCommitIndex(const uint64_t& log_index) {
    std::lock_guard lock(logs_mu_);
    last_commit_index_ = log_index;
  }
  bool FindLogItem(const uint64_t& log_index, uint64_t* found_index);

 private:
  int InternalFindLogByLogIndex(const uint64_t& log_index);
	int InternalFindLogByLogContent(const LogItem& item);
  pstd::Mutex logs_mu_;
  std::vector<LogItem> logs_;
  uint64_t last_commit_index_;
};

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
	void HandleRaftLogResult(Status& s, RaftMemLog::LogItem& memlog_item, raft_result& result, nuraft::ptr<std::exception>& err);
	void HandleRaftLogAccept(raft_result& result, RaftMemLog::LogItem& memlog_item);
	void Start();
	bool HasLeader();
	bool IsLeader();
	void PrecommitLog(ulong log_idx, uint32_t slot_id, std::string db_name, std::string raftlog);
	void RollbackLog(ulong log_idx, uint32_t slot_id, std::string db_name, std::string raftlog);
	void ApplyLog(ulong log_idx, uint32_t slot_id, std::string db_name, std::string raftlog);

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
	// ASIO options
	nuraft::asio_service::options asio_opt_;
	// Raft parameters.
	nuraft::raft_params params_;
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

	std::shared_ptr<RaftMemLog> mem_logger_= nullptr;

};

#endif