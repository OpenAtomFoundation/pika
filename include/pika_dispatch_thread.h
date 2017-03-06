// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_DISPATCH_THREAD_H_
#define PIKA_DISPATCH_THREAD_H_

#include "pika_worker_thread.h"
#include "dispatch_thread.h"
#include "pika_client_conn.h"

class PikaDispatchThread : public pink::DispatchThread<PikaClientConn>
{
public:
  PikaDispatchThread(int port, int work_num, PikaWorkerThread** pika_worker_thread,
                     int cron_interval, int queue_limit);
  PikaDispatchThread(std::string &ip, int port, int work_num,
                     PikaWorkerThread** pika_worker_thread,
                     int cron_interval, int queue_limit);
  PikaDispatchThread(std::set<std::string> &ips, int port, int work_num,
                     PikaWorkerThread** pika_worker_thread,
                     int cron_interval, int queue_limit);
  virtual ~PikaDispatchThread();
  virtual bool AccessHandle(std::string& ip);

  int ClientNum();
};
#endif
