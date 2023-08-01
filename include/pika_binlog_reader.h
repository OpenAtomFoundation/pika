// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_BINLOG_READER_H_
#define PIKA_BINLOG_READER_H_

#include <memory>
#include <shared_mutex>
#include <string>

#include "pstd/include/env.h"
#include "pstd/include/pstd_slice.h"
#include "pstd/include/pstd_status.h"

#include "include/pika_binlog.h"

// using pstd::Slice;
// using pstd::Status;

class PikaBinlogReader {
 public:
  PikaBinlogReader(uint32_t cur_filenum, uint64_t cur_offset);
  PikaBinlogReader();
  ~PikaBinlogReader() = default;

  pstd::Status Get(std::string* scratch, uint32_t* filenum, uint64_t* offset);
  int32_t Seek(const std::shared_ptr<Binlog>& logger, uint32_t filenum, uint64_t offset);
  bool ReadToTheEnd();
  void GetReaderStatus(uint32_t* cur_filenum, uint64_t* cur_offset);

 private:
  bool GetNext(uint64_t* size);
  uint32_t ReadPhysicalRecord(pstd::Slice* result, uint32_t* filenum, uint64_t* offset);
  // Returns scratch binflog and corresponding offset
  pstd::Status Consume(std::string* scratch, uint32_t* filenum, uint64_t* offset);

  std::shared_mutex rwlock_;
  uint32_t cur_filenum_ = 0;
  uint64_t cur_offset_ = 0;
  uint64_t last_record_offset_ = 0;

  std::shared_ptr<Binlog> logger_;
  std::unique_ptr<pstd::SequentialFile> queue_;

  std::unique_ptr<char[]> const backing_store_;
  pstd::Slice buffer_;
};

#endif  // PIKA_BINLOG_READER_H_
