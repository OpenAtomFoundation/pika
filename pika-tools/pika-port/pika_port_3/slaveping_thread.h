// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef SLAVEPING_THREAD_H_
#define SLAVEPING_THREAD_H_

#include "net/include/net_thread.h"
#include "net/include/net_cli.h"
#include "net/include/redis_cli.h"
#include "slash/include/slash_mutex.h"
#include "slash/include/slash_status.h"

#include <glog/logging.h>

using slash::Status;

class SlavepingThread : public net::Thread {
public:
  SlavepingThread(int64_t sid) : sid_(sid),
  is_first_send_(true) {
    cli_ = net::NewRedisCli();
    cli_->set_connect_timeout(1500);
	};
  virtual ~SlavepingThread() {
    StopThread();
    delete cli_;
    LOG(INFO) << " Slaveping thread " << pthread_self() << " exit!!!";
  };

  Status Send();
  Status RecvProc();

private:
  int64_t sid_;
  bool is_first_send_;

  int sockfd_;
  net::PinkCli *cli_;

  virtual void* ThreadMain();

};

#endif
