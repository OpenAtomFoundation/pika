// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_SLAVEPING_THREAD_H_
#define PIKA_SLAVEPING_THREAD_H_

#include <glog/logging.h>

#include "pstd/include/pstd_status.h"
#include "pink/include/pink_cli.h"
#include "pink/include/pink_thread.h"

using pstd::Status;

class PikaSlavepingThread : public pink::Thread {
 public:
  PikaSlavepingThread(int64_t sid)
      : sid_(sid), is_first_send_(true) {
    cli_ = pink::NewPbCli();
    cli_->set_connect_timeout(1500);
    set_thread_name("SlavePingThread");
  };
  virtual ~PikaSlavepingThread() {
    StopThread();
    delete cli_;
    LOG(INFO) << "SlavepingThread " << thread_id() << " exit!!!";
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
