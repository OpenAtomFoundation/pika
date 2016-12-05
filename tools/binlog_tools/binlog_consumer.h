// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_TOOLS_BINLOGTOOLS_BINLOG_CONSUMER_H_
#define PIKA_TOOLS_BINLOGTOOLS_BINLOG_CONSUMER_H_

//#include "redis_cli.h"
#include <atomic>
#include "slice.h"
#include "status.h"

#include "binlog.h"
#include "env.h"
#include "slash_mutex.h"

// binlog consumer can read a binlog file and parse the data in it
class BinlogConsumer{
 public:

  BinlogConsumer(Binlog * logger);

  virtual ~BinlogConsumer();

  /*
   * Get and Set
   */
  uint64_t last_record_offset () {
    slash::RWLock l(&rwlock_, false);
    return last_record_offset_;
  }
  uint32_t current_file() {
    slash::RWLock l(&rwlock_, false);
    return current_file_;
  }
  uint64_t con_offset() {
    slash::RWLock l(&rwlock_, false);
    return con_offset_;
  }

  int Trim();
  virtual uint64_t GetNext(bool &is_error) = 0;

  slash::Status LoadFile(uint32_t file);
  slash::Status LoadNextFile();
  slash::Status Parse(std::string &scratch, uint64_t* produce_time);


  slash::Status Consume(std::string &scratch, uint64_t* produce_time);
  virtual unsigned int ReadPhysicalRecord(slash::Slice *fragment, uint64_t * produce_time) = 0;

  size_t header_size_;
  Binlog* logger_;
  uint64_t con_offset_;
  uint32_t current_file_;
  uint32_t pro_num_;
  uint64_t pro_offset_;

  uint64_t initial_offset_;
  uint64_t last_record_offset_;
  uint64_t end_of_buffer_offset_;

  slash::SequentialFile* queue_;
  char* const backing_store_;
  slash::Slice buffer_;

  pthread_rwlock_t rwlock_;

};

class OldBinlogConsumer: public BinlogConsumer {
 public:

  OldBinlogConsumer(Binlog * logger);

  virtual ~OldBinlogConsumer();

  virtual uint64_t GetNext(bool &is_error);

 private:
  virtual unsigned int ReadPhysicalRecord(slash::Slice *fragment, uint64_t* produce_time);
};

class NewBinlogConsumer: public BinlogConsumer {
 public:

  NewBinlogConsumer(Binlog * logger);

  virtual ~NewBinlogConsumer();

  virtual uint64_t GetNext(bool &is_error);

 private:
  virtual unsigned int ReadPhysicalRecord(slash::Slice *fragment, uint64_t* produce_time);
};

#endif
