// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_RSYNC_SERVICE_H_
#define PIKA_RSYNC_SERVICE_H_

#include "iostream"

class PikaRsyncService {
 public:
  PikaRsyncService(const std::string& raw_path, int32_t port);
  ~PikaRsyncService();
  int32_t StartRsync();
  bool CheckRsyncAlive();
  int32_t ListenPort();

 private:
  int32_t CreateSecretFile();
  std::string raw_path_;
  std::string rsync_path_;
  std::string pid_path_;
  int32_t port_ = 0;
};

#endif
