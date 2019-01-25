// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_BINLOG_SENDER_THREAD_H_
#define PIKA_BINLOG_SENDER_THREAD_H_

#include "pink/include/pink_cli.h"
#include "pink/include/pink_thread.h"
#include "slash/include/env.h"
#include "slash/include/slash_slice.h"
#include "slash/include/slash_status.h"

using slash::Status;
using slash::Slice;

class PikaBinlogSenderThread : public pink::Thread {
 public:

  PikaBinlogSenderThread(const std::string &ip, int port,
                         int64_t sid,
                         slash::SequentialFile *queue,
                         uint32_t filenum, uint64_t con_offset);

  virtual ~PikaBinlogSenderThread();

  uint32_t filenum() {
    return filenum_;
  }
  uint64_t con_offset() {
    return con_offset_;
  }

  int trim();

 private:
  uint64_t get_next(bool &is_error);
  Status Parse(std::string &scratch);
  Status Consume(std::string &scratch);
  unsigned int ReadPhysicalRecord(slash::Slice *fragment);

  uint32_t filenum_;
  uint64_t con_offset_;
  uint64_t last_record_offset_;

  slash::SequentialFile* queue_;
  char* const backing_store_;
  Slice buffer_;

  std::string ip_;
  int port_;
  int64_t sid_;

  int timeout_ms_;
  pink::PinkCli *cli_;

  virtual void* ThreadMain();
};

#endif
