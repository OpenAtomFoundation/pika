// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_STATE_MACHINE_H_
#define PIKA_STATE_MACHINE_H_

#include <libnuraft/nuraft.hxx>

#include "include/pika_consensus.h"
#include "include/pika_client_conn.h"
#include "include/pika_raft_snapshot.h"

using pstd::Status;

class PikaStateMachine : public nuraft::state_machine {
 public:
  using OnApply = std::function<void(ulong log_idx, uint32_t slot_id, 
                                    std::string db_name, std::string raftlog)>;
  using OnRollback = std::function<void(ulong log_idx, uint32_t slot_id, 
                                    std::string db_name, std::string raftlog)>;
  using OnPrecommit = std::function<void(ulong log_idx, uint32_t slot_id, 
                                        std::string db_name, std::string raftlog)>;

  PikaStateMachine(OnPrecommit _on_precommit, OnRollback _on_rollback, OnApply _on_apply, bool async_snapshot = false)
    : last_committed_idx_(0)
    , on_precommit_(_on_precommit)
    , on_rollback_(_on_rollback)
    , on_apply_(_on_apply)
    , async_snapshot_(async_snapshot)
    , snapshot_manager_(std::make_unique<RaftSnapshotManager>())
    {}
  ~PikaStateMachine() {}
  nuraft::ptr<nuraft::buffer> pre_commit(const ulong log_idx, nuraft::buffer& data) override;
  nuraft::ptr<nuraft::buffer> commit(const ulong log_idx, nuraft::buffer& data) override;
  void commit_config(const ulong log_idx, nuraft::ptr<nuraft::cluster_config>& new_conf) override;
  void rollback(const ulong log_idx, nuraft::buffer& data) override;
  int read_logical_snp_obj(nuraft::snapshot& s,
                            void*& user_snp_ctx,
                            ulong obj_id,
                            nuraft::ptr<nuraft::buffer>& data_out,
                            bool& is_last_obj) override;
  void save_logical_snp_obj(nuraft::snapshot& s,
                            ulong& obj_id,
                            nuraft::buffer& data,
                            bool is_first_obj,
                            bool is_last_obj) override;
  bool apply_snapshot(nuraft::snapshot& s) override;
  void free_user_snp_ctx(void*& user_snp_ctx) override;
  nuraft::ptr<nuraft::snapshot> last_snapshot() override;
  ulong last_commit_index() override;
  void create_snapshot(nuraft::snapshot& s,
                        nuraft::async_result<bool>::handler_type& when_done) override;

 private:
  void create_snapshot_internal(nuraft::ptr<nuraft::snapshot> ss);
  void create_snapshot_sync(nuraft::snapshot& s,
                            nuraft::async_result<bool>::handler_type& when_done);
  void create_snapshot_async(nuraft::snapshot& s,
                              nuraft::async_result<bool>::handler_type& when_done);

  // Last committed Raft log number.
  pstd::Mutex last_committed_idx_mutex_;
  std::atomic<uint64_t> last_committed_idx_;

  pstd::Mutex snapshot_mutex_;
  std::unique_ptr<RaftSnapshotManager> snapshot_manager_;

  // If `true`, snapshot will be created asynchronously.
  bool async_snapshot_;

  pstd::Mutex   raftlog_mutex_;
  std::string   raftlog_commit_;

  OnPrecommit   on_precommit_;
  OnRollback    on_rollback_;
  OnApply       on_apply_;
};

#endif