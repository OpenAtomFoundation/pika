// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_raft_state_machine.h"

#include "include/pika_consensus.h"
#include "include/pika_client_conn.h"

nuraft::ptr<nuraft::buffer> PikaStateMachine::pre_commit(const ulong log_idx, nuraft::buffer& data) {
  // deserialize from nuraft buffer
  nuraft::buffer_serializer bs(data);
  uint32_t slot_id = bs.get_u32();
  std::string db_name = bs.get_str();
  std::string raftlog = bs.get_str();

  on_precommit_(log_idx, slot_id, db_name, raftlog);
  
  return nullptr;
}

// serve as OnApply
nuraft::ptr<nuraft::buffer> PikaStateMachine::commit(const ulong log_idx, nuraft::buffer& data) {
  // deserialize from nuraft buffer
  nuraft::buffer_serializer bs(data);
  uint32_t slot_id = bs.get_u32();
  std::string db_name = bs.get_str();
  std::string raftlog = bs.get_str();

  on_apply_(log_idx, slot_id, db_name, raftlog);

  // Update last committed index number.
  std::lock_guard ll(last_committed_idx_mutex_);
  last_committed_idx_ = log_idx;

  return nullptr;
}

void PikaStateMachine::commit_config(const ulong log_idx, nuraft::ptr<nuraft::cluster_config>& new_conf) {
  // Nothing to do with configuration change. Just update committed index.
  last_committed_idx_ = log_idx;
}

void PikaStateMachine::rollback(const ulong log_idx, nuraft::buffer& data) {
  // deserialize from nuraft buffer
  nuraft::buffer_serializer bs(data);
  uint32_t slot_id = bs.get_u32();
  std::string db_name = bs.get_str();
  std::string raftlog = bs.get_str();

  on_rollback_(log_idx, slot_id, db_name, raftlog);
}

int PikaStateMachine::read_logical_snp_obj(nuraft::snapshot& s,
                          void*& user_snp_ctx,
                          ulong obj_id,
                          nuraft::ptr<nuraft::buffer>& data_out,
                          bool& is_last_obj)
{
  nuraft::ptr<nuraft::snapshot> last_snapshot = nullptr;
  {   
    std::lock_guard<std::mutex> ll(snapshot_mutex_);
    last_snapshot = snapshot_manager_->GetLastSnapshot();
    if (!last_snapshot) {
      // Snapshot doesn't exist.
      data_out = nullptr;
      is_last_obj = true;
      return 0;
    }
  }
  if (obj_id == 0) {
    snapshot_manager_->StartCreateSnapshot();
    snapshot_manager_->CreateSnapshotMeta(data_out);
    is_last_obj = false;
  } else {
    if (snapshot_manager_->CreateNextSnapshotContent(data_out)) {
      is_last_obj = false;
    } else {
      is_last_obj = true;
    }
  }
  
  return 0;
}

void PikaStateMachine::save_logical_snp_obj(nuraft::snapshot& s,
                          ulong& obj_id,
                          nuraft::buffer& data,
                          bool is_first_obj,
                          bool is_last_obj)
{
  if (obj_id == 0) {
    LOG(INFO) << "start receiving snapshot at idx " << s.get_last_log_idx();
    nuraft::ptr<nuraft::buffer> snp_buf = s.serialize();
    nuraft::ptr<nuraft::snapshot> ss = nuraft::snapshot::deserialize(*snp_buf);
    snapshot_manager_->SetLastSnapshot(ss);
    snapshot_manager_->InstallSnapshotMeta(s, data);
    snapshot_manager_->PrepareInstallSnapshot();
  } else {
    snapshot_manager_->InstallSnapshot(data);
  }
  // Request next object.
  obj_id++;
}

bool PikaStateMachine::apply_snapshot(nuraft::snapshot& s) {
  std::lock_guard<std::mutex> ll(snapshot_mutex_);

  if (!snapshot_manager_->GetLastSnapshot()) {
    return false;
  }

  snapshot_manager_->ApplySnapshot();
  
  return true;
}

void PikaStateMachine::free_user_snp_ctx(void*& user_snp_ctx) {
  // In this example, `read_logical_snp_obj` doesn't create
  // `user_snp_ctx`. Nothing to do in this function.
}

nuraft::ptr<nuraft::snapshot> PikaStateMachine::last_snapshot() {
  // Just return the latest snapshot.
  std::lock_guard<std::mutex> ll(snapshot_mutex_);
  return snapshot_manager_->GetLastSnapshot();
}

ulong PikaStateMachine::last_commit_index() {
  return last_committed_idx_;
}

void PikaStateMachine::create_snapshot(nuraft::snapshot& s,
                      nuraft::async_result<bool>::handler_type& when_done)
{
  LOG(INFO) << "start creating snapshot at idx" << s.get_last_log_idx();
  if (!async_snapshot_) {
      // Create a snapshot in a synchronous way (blocking the thread).
      create_snapshot_sync(s, when_done);
  } else {
      // Create a snapshot in an asynchronous way (in a different thread).
      create_snapshot_async(s, when_done);
  }
}

void PikaStateMachine::create_snapshot_internal(nuraft::ptr<nuraft::snapshot> ss) {
  std::lock_guard<std::mutex> ll(snapshot_mutex_);
  // Put into snapshot store.
  snapshot_manager_->SetLastSnapshot(ss);
  snapshot_manager_->DoBgsave(ss);
  LOG(INFO) << "creating snapshot at idx" << ss->get_last_log_idx() << " done";
}

void PikaStateMachine::create_snapshot_sync(nuraft::snapshot& s,
                          nuraft::async_result<bool>::handler_type& when_done)
{
  // Clone snapshot from `s`.
  nuraft::ptr<nuraft::buffer> snp_buf = s.serialize();
  nuraft::ptr<nuraft::snapshot> ss = nuraft::snapshot::deserialize(*snp_buf);
  create_snapshot_internal(ss);

  nuraft::ptr<std::exception> except(nullptr);
  bool ret = true;
  when_done(ret, except);
}

void PikaStateMachine::create_snapshot_async(nuraft::snapshot& s,
                            nuraft::async_result<bool>::handler_type& when_done)
{
  // Clone snapshot from `s`.
  nuraft::ptr<nuraft::buffer> snp_buf = s.serialize();
  nuraft::ptr<nuraft::snapshot> ss = nuraft::snapshot::deserialize(*snp_buf);

  // Note that this is a very naive and inefficient example
  // that creates a new thread for each snapshot creation.
  std::thread t_hdl([this, ss, when_done]{
    create_snapshot_internal(ss);

    nuraft::ptr<std::exception> except(nullptr);
    bool ret = true;
    when_done(ret, except);
  });
  t_hdl.detach();
}