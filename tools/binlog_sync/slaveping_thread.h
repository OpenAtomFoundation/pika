// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef SLAVEPING_THREAD_H_
#define SLAVEPING_THREAD_H_

#include "pink/include/pink_thread.h"
#include "pink/include/pink_cli.h"
#include "pink/include/redis_cli.h"
#include "slash/include/slash_mutex.h"
#include "slash/include/slash_status.h"

using slash::Status;

class SlavepingThread : public pink::Thread {
public:
  SlavepingThread(int64_t sid) : sid_(sid),
  is_first_send_(true) {
    cli_ = pink::NewRedisCli();
    cli_->set_connect_timeout(1500);
	};
  virtual ~SlavepingThread() {
    StopThread();
    delete cli_;
    DLOG(INFO) << " Slaveping thread " << pthread_self() << " exit!!!";
	};

  Status Send();
  Status RecvProc();

private:
  int64_t sid_;
  bool is_first_send_;

  int sockfd_;
  pink::PinkCli *cli_;

  virtual void* ThreadMain();

};

#endif
