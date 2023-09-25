// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_RAFT_STATE_MANAGER_H_
#define PIKA_RAFT_STATE_MANAGER_H_

#include <libnuraft/nuraft.hxx>

#include "include/pika_raft_log_store.h"
#include "include/pika_define.h"

// TODO(lap): more mature state store
class PikaRaftStateManager : public nuraft::state_mgr {
 public:
  PikaRaftStateManager(int32_t _server_id, std::string _endpoint
                      , const std::vector<std::string>& _server_list
                      , std::string _raftlog_path);

  ~PikaRaftStateManager() {}

  nuraft::ptr<nuraft::cluster_config> load_config() override;

  void save_config(const nuraft::cluster_config &config) override;

  void save_state(const nuraft::srv_state &state) override;

  nuraft::ptr<nuraft::srv_state> read_state() override;

  nuraft::ptr<nuraft::log_store> load_log_store() override;

  int32_t server_id() override;

  void system_exit(const int exit_code) override;

 private:
  int32_t server_id_ = 0;
  std::string endpoint_;
  std::string raftlog_path_;
  std::string state_path_;
  nuraft::ptr<PikaRaftLogStore> cur_log_store_;
  nuraft::ptr<nuraft::srv_config> srv_config_;
  nuraft::ptr<nuraft::cluster_config> saved_config_;
  nuraft::ptr<nuraft::srv_state> saved_state_;
};

#endif
