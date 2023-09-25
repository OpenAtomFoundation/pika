// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_raft_state_manager.h"

#include <glog/logging.h>

#include <filesystem>
#include <fstream>
#include <cstring>

PikaRaftStateManager::PikaRaftStateManager(int32_t _server_id
                                          , std::string _endpoint
                                          , const std::vector<std::string>& _server_list
                                          , std::string _raftlog_path)
    : server_id_(_server_id)
    , endpoint_(std::move(_endpoint))
    , raftlog_path_(std::move(_raftlog_path))
    , state_path_(raftlog_path_ + "STATE") {
  // local raft server cfg
  srv_config_ = nuraft::cs_new<nuraft::srv_config>(server_id_, endpoint_);

  // Initial cluster config, read from the list loaded from the configuration file.
  saved_config_ = nuraft::cs_new<nuraft::cluster_config>();
  
  for (auto const &s : _server_list) {
    size_t colon_pos = s.find_first_of(":");
    std::string id_str = s.substr(0, colon_pos);
    std::string member_pika_endpoint = s.substr(colon_pos + 1);
    std::string member_ip = member_pika_endpoint.substr(0, member_pika_endpoint.find_first_of(":"));
    std::string member_port = member_pika_endpoint.substr(member_pika_endpoint.find_first_of(":") + 1);
    std::string member_endpoint = member_ip + ":" + std::to_string(std::stoi(member_port) + kPortShiftRaftServer);
    LOG(INFO) << "loading cluster member: " << id_str << " : " << member_ip;
    uint32_t member_id = std::stoi(id_str);
    if (member_id == server_id_) {
      if (member_endpoint != endpoint_ && member_ip != "127.0.0.1") {
        LOG(FATAL) << "Raft Configure Error: actual endpoint of raft server " << server_id_ 
                    << " is " << endpoint_ << ", in config is " << member_endpoint;
      }
    }
    nuraft::ptr<nuraft::srv_config> new_server = nuraft::cs_new<nuraft::srv_config>(member_id, member_endpoint);
    saved_config_->get_servers().push_back(new_server);
  }

}

// TODO(lap): use static config by g_pika_conf temporarily
//            so no need to persist config until support config change
nuraft::ptr<nuraft::cluster_config> PikaRaftStateManager::load_config() {
  return saved_config_;
}

void PikaRaftStateManager::save_config(const nuraft::cluster_config &config) {
  nuraft::ptr<nuraft::buffer> buf = config.serialize();
  saved_config_                   = nuraft::cluster_config::deserialize(*buf);
}

void PikaRaftStateManager::save_state(const nuraft::srv_state &state) {
  nuraft::ptr<nuraft::buffer> buf = state.serialize();
  std::vector<char> char_buf(buf->size());
  std::memcpy(char_buf.data(), buf->data_begin(), char_buf.size());

  const auto tmp_file = state_path_ + ".tmp";
  std::ofstream file(tmp_file, std::ios::binary | std::ios::trunc);
  assert(file.good());

  file.write(char_buf.data(),
              static_cast<std::streamsize>(char_buf.size()));
  file.flush();
  file.close();

  std::filesystem::rename(tmp_file, state_path_);
}

nuraft::ptr<nuraft::srv_state> PikaRaftStateManager::read_state() {
  std::ifstream file(state_path_, std::ios::binary);
  if(!file.good()) {
      return nullptr;
  }

  auto file_sz = std::filesystem::file_size(state_path_);
  std::vector<char> buf(file_sz);
  if(!file.read(buf.data(), static_cast<std::streamsize>(file_sz))) {
      return nullptr;
  }

  auto nuraft_buf = nuraft::buffer::alloc(file_sz);
  std::memcpy(nuraft_buf->data_begin(), buf.data(), nuraft_buf->size());

  saved_state_ = nuraft::srv_state::deserialize(*nuraft_buf);
  return saved_state_;
}

nuraft::ptr<nuraft::log_store> PikaRaftStateManager::load_log_store() {
  cur_log_store_ = nuraft::cs_new<PikaRaftLogStore>();
  if (! cur_log_store_->load(raftlog_path_)) {
    LOG(FATAL) << "Raft Log Store: can not load path: "
               << raftlog_path_;
    return nullptr;
  }
  return cur_log_store_;
}

int32_t PikaRaftStateManager::server_id() {
  return server_id_;
}

void PikaRaftStateManager::system_exit(const int exit_code) {
  abort();
}
