// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_TOOLS_BINLOGTOOLS_BINLOG_H_
#define PIKA_TOOLS_BINLOGTOOLS_BINLOG_H_

#include <cstdio>
#include <list>
#include <string>
#include <deque>
#include <pthread.h>

#ifndef __STDC_FORMAT_MACROS
# define __STDC_FORMAT_MACROS
# include <inttypes.h>
#endif 

#include "slash/include/env.h"
#include "slash/include/slash_mutex.h"
#include "slash/include/slash_status.h"
#include "slash/include/slash_slice.h"
#include "pika_define.h"

using slash::RWLock;
using slash::Status;
using slash::Mutex;
using slash::Slice;

class Version;

std::string NewFileName(const std::string name, const uint32_t current);

class Binlog
{
 public:
  Binlog(const std::string& Binlog_path, const int file_size = 100 * 1024 * 1024);
  virtual ~Binlog();

  void Lock()         { mutex_.Lock(); }
  void Unlock()       { mutex_.Unlock(); }

  Status Put(const std::string &item);
  Status Put(const char* item, int len);

  Status GetProducerStatus(uint32_t* filenum, uint64_t* pro_offset);
  /*
   * Set Producer pro_num and pro_offset with lock
   */
  Status SetProducerStatus(uint32_t filenum, uint64_t pro_offset);




  uint64_t file_size() {
    return file_size_;
  }

  std::string filename;






  Version* version_;
  slash::RWFile *versionfile_;

  slash::Mutex mutex_;

  uint32_t pro_num_;

  int block_offset_;

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

  uint32_t item_num()                  { return item_num_; }
  void set_item_num(uint32_t item_num) { item_num_ = item_num; }
  void plus_item_num()                 { item_num_++; }
  void minus_item_num()                { item_num_--; }

  uint64_t pro_offset_;
  uint32_t pro_num_;
  pthread_rwlock_t rwlock_;

  void debug() {
    slash::RWLock(&rwlock_, false);
    printf ("Current pro_num %u pro_offset %lu\n", pro_num_, pro_offset_);
  }

 private:

  slash::RWFile *save_;

  // Not used
  uint64_t con_offset_;
  uint32_t con_num_;
  uint32_t item_num_;

  // No copying allowed;
  Version(const Version&);
  void operator=(const Version&);
};


#endif
