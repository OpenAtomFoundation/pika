//  Copyright (c) 2018-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef INCLUDE_BINLOG_CONSUMER_H_
#define INCLUDE_BINLOG_CONSUMER_H_

#include "stddef.h"
#include "stdint.h"

#include "slash/include/slash_slice.h"
#include "slash/include/slash_status.h"
#include "slash/include/env.h"
#include "slash/include/slash_mutex.h"

enum RecordType {
  kZeroType = 0,
  kFullType = 1,
  kFirstType = 2,
  kMiddleType = 3,
  kLastType = 4,
  kEof = 5,
  kBadRecord = 6,
  kOldRecord = 7
};

/*
 * Header is Type(1 byte), length (3 bytes), time (4 bytes)
 */
static const size_t kHeaderSize = 1 + 3 + 4;
static const size_t kBlockSize = 64 * 1024;
static const std::string kBinlogPrefix = "write2file";

class BinlogConsumer {
  public:
    BinlogConsumer(const std::string& binlog_path,
                   uint32_t first_filenum,
                   uint32_t last_filenum,
                   uint64_t offset);
    virtual ~BinlogConsumer();

    std::string NewFileName(const std::string& name,
                            const uint32_t current);
    bool Init();
    bool trim();
    uint32_t current_filenum();
    uint64_t current_offset();
    slash::Status Parse(std::string* scratch);

  private:
    uint64_t get_next(bool* is_error);
    uint32_t ReadPhysicalRecord(slash::Slice* result);
    slash::Status Consume(std::string* scratch);
    std::string filename_;
    uint32_t first_filenum_;
    uint32_t last_filenum_;

    uint32_t current_filenum_; 
    uint64_t current_offset_;
    uint64_t last_record_offset_;


    slash::Slice buffer_;
    char* const backing_store_;
    slash::SequentialFile* queue_;
};

#endif  //  INCLUDE_BINLOG_Consumber_H_ 
