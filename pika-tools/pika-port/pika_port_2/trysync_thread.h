// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef TRYSYNC_THREAD_H_
#define TRYSYNC_THREAD_H_

#include "net/include/net_thread.h"
#include "net/include/redis_cli.h"

class TrysyncThread : public net::Thread {
 public:
  TrysyncThread() {
    cli_ = net::NewRedisCli();
    cli_->set_connect_timeout(1500);
  };
  virtual ~TrysyncThread();

 private:
  int sockfd_;
  int64_t sid_;
  net::PinkCli* cli_;

  bool Send();
  bool RecvProc();
  void PrepareRsync();
  bool TryUpdateMasterOffset();
  int Retransmit();

  virtual void* ThreadMain();
};

#endif
