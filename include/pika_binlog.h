// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_BINLOG_H_
#define PIKA_BINLOG_H_

#include <cstdio>
#include <list>
#include <string>
#include <deque>
#include <pthread.h>

#ifndef __STDC_FORMAT_MACROS
# define __STDC_FORMAT_MACROS
# include <inttypes.h>
#endif 

#include "slash/include/slash_status.h"
#include "slash/include/slash_mutex.h"
#include "slash/include/env.h"
#include "include/pika_define.h"

using slash::Status;
using slash::Slice;

std::string NewFileName(const std::string name, const uint32_t current);

class Version;

class Binlog {
 public:
  Binlog(const std::string& Binlog_path, const int file_size = 100 * 1024 * 1024);
  ~Binlog();

  void Lock()         { mutex_.Lock(); }
  void Unlock()       { mutex_.Unlock(); }

  Status Put(const std::string &item);
  Status Put(const char* item, int len);

  Status GetProducerStatus(uint32_t* filenum, uint64_t* pro_offset, uint64_t* logic_id = NULL);
  /*
   * Set Producer pro_num and pro_offset with lock
   */
  Status SetProducerStatus(uint32_t filenum, uint64_t pro_offset);

  static Status AppendBlank(slash::WritableFile *file, uint64_t len);

  slash::WritableFile *queue() { return queue_; }

  uint64_t file_size() {
    return file_size_;
  }

  std::string filename;

 private:

  void InitLogFile();
  Status EmitPhysicalRecord(RecordType t, const char *ptr, size_t n, int *temp_pro_offset);


  /*
   * Produce
   */
  Status Produce(const Slice &item, int *pro_offset);

  uint32_t consumer_num_;
  uint64_t item_num_;

  Version* version_;
  slash::WritableFile *queue_;
  slash::RWFile *versionfile_;

  slash::Mutex mutex_;

  uint32_t pro_num_;

  int block_offset_;

  char* pool_;
  bool exit_all_consume_;
  const std::string binlog_path_;

  uint64_t file_size_;

  // Not use
  //int32_t retry_;

  // No copying allowed
  Binlog(const Binlog&);
  void operator=(const Binlog&);
};

class Version {
 public:
  Version(slash::RWFile *save);
  ~Version();

  Status Init();

  // RWLock should be held when access members.
  Status StableSave();

  uint32_t pro_num_;
  uint64_t pro_offset_;
  uint64_t logic_id_;

  pthread_rwlock_t rwlock_;

  void debug() {
    slash::RWLock(&rwlock_, false);
    printf ("Current pro_num %u pro_offset %lu\n", pro_num_, pro_offset_);
  }

 private:

  slash::RWFile *save_;

  // No copying allowed;
  Version(const Version&);
  void operator=(const Version&);
};

#endif
