// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_rsync_service.h"

#include <glog/logging.h>
#include <fstream>

#include "pstd/include/env.h"
#include "pstd/include/rsync.h"

#include "include/pika_conf.h"
#include "include/pika_define.h"

extern PikaConf* g_pika_conf;

PikaRsyncService::PikaRsyncService(const std::string& raw_path, const int port) : raw_path_(raw_path), port_(port) {
  if (raw_path_.back() != '/') {
    raw_path_ += "/";
  }
  rsync_path_ = raw_path_ + pstd::kRsyncSubDir + "/";
  pid_path_ = rsync_path_ + pstd::kRsyncPidFile;
}

PikaRsyncService::~PikaRsyncService() {
  if (!CheckRsyncAlive()) {
    pstd::DeleteDirIfExist(rsync_path_);
  } else {
    pstd::StopRsync(raw_path_);
  }
  LOG(INFO) << "PikaRsyncService exit!!!";
}

int PikaRsyncService::StartRsync() {
  int ret = 0;
  std::string auth;
  if (g_pika_conf->masterauth().empty()) {
    auth = kDefaultRsyncAuth;
  } else {
    auth = g_pika_conf->masterauth();
  }
  ret = pstd::StartRsync(raw_path_, kDBSyncModule, "0.0.0.0", port_, auth);
  if (ret != 0) {
    LOG(WARNING) << "Failed to start rsync, path:" << raw_path_ << " error : " << ret;
    return -1;
  }
  ret = CreateSecretFile();
  if (ret != 0) {
    LOG(WARNING) << "Failed to create secret file";
    return -1;
  }
  // Make sure the listening addr of rsyncd is accessible, avoid the corner case
  // that rsync --daemon process is started but not finished listening on the socket
  sleep(1);

  if (!CheckRsyncAlive()) {
    LOG(WARNING) << "Rsync service is no live, path:" << raw_path_;
    return -1;
  }
  return 0;
}

int PikaRsyncService::CreateSecretFile() {
  std::string secret_file_path = g_pika_conf->db_sync_path();
  if (g_pika_conf->db_sync_path().back() != '/') {
    secret_file_path += "/";
  }
  secret_file_path += pstd::kRsyncSubDir + "/";
  pstd::CreatePath(secret_file_path);
  secret_file_path += kPikaSecretFile;

  std::string auth;
  // unify rsync auth with masterauth
  if (g_pika_conf->masterauth().empty()) {
    auth = kDefaultRsyncAuth;
  } else {
    auth = g_pika_conf->masterauth();
  }

  std::ofstream secret_stream(secret_file_path.c_str());
  if (!secret_stream) {
    return -1;
  }
  secret_stream << auth;
  secret_stream.close();

  // secret file cant be other-accessible
  std::string cmd = "chmod 600 " + secret_file_path;
  int ret = system(cmd.c_str());
  if (ret == 0 || (WIFEXITED(ret) && !WEXITSTATUS(ret))) {
    return 0;
  }
  return ret;
}

bool PikaRsyncService::CheckRsyncAlive() { return pstd::FileExists(pid_path_); }

int PikaRsyncService::ListenPort() { return port_; }
