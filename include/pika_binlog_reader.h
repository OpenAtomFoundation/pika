// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_BINLOG_READER_H_
#define PIKA_BINLOG_READER_H_

#include <memory>
#include <string>

#include "pstd/include/env.h"
#include "pstd/include/pstd_slice.h"
#include "pstd/include/pstd_status.h"

#include "include/pika_binlog.h"

using pstd::Slice;
using pstd::Status;

class PikaBinlogReader {
 public:
  PikaBinlogReader(uint32_t cur_filenum, uint64_t cur_offset);
  PikaBinlogReader();
  ~PikaBinlogReader();
  Status Get(std::string* scratch, uint32_t* filenum, uint64_t* offset);
  int Seek(std::shared_ptr<Binlog> logger, uint32_t filenum, uint64_t offset);
  bool ReadToTheEnd();
  void GetReaderStatus(uint32_t* cur_filenum, uint64_t* cur_offset);

 private:
  bool GetNext(uint64_t* size);
  unsigned int ReadPhysicalRecord(pstd::Slice* redult, uint32_t* filenum, uint64_t* offset);
  // Returns scratch binflog and corresponding offset
  Status Consume(std::string* scratch, uint32_t* filenum, uint64_t* offset);

  pthread_rwlock_t rwlock_;
  uint32_t cur_filenum_;
  uint64_t cur_offset_;
  uint64_t last_record_offset_;

  std::shared_ptr<Binlog> logger_;
  pstd::SequentialFile* queue_;

  char* const backing_store_;
  Slice buffer_;
};

#endif  // PIKA_BINLOG_READER_H_
