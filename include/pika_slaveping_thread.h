// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_SLAVEPING_THREAD_H_
#define PIKA_SLAVEPING_THREAD_H_

#include "slash/include/slash_mutex.h"
#include "slash/include/slash_status.h"
#include "pink/include/pink_thread.h"
#include "pink/include/pink_cli.h"

using slash::Status;

class PikaSlavepingThread : public pink::Thread {
public:
  PikaSlavepingThread(int64_t sid) : sid_(sid),
  is_first_send_(true) {
    cli_ = pink::NewRedisCli();
    cli_->set_connect_timeout(1500);
	};
  virtual ~PikaSlavepingThread() {
    StopThread();
    delete cli_;
    DLOG(INFO) << " Slaveping thread " << thread_id() << " exit!!!";
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
