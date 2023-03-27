// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef __PSTD_BINLOG_H__
#define __PSTD_BINLOG_H__

#include <assert.h>
#include <string>

#include "pstd/include/pstd_status.h"
#include "pstd/include/xdebug.h"
//#include "pstd_mutex.h"

namespace pstd {

// SyncPoint is a file number and an offset;

class BinlogReader;

class Binlog {
 public:
  static Status Open(const std::string& path, Binlog** logptr);

  Binlog() { }
  virtual ~Binlog() { }

  // TODO (aa) 
  //   1. maybe add Options
  
  //
  // Basic API
  //
  virtual Status Append(const std::string &item) = 0;
  virtual BinlogReader* NewBinlogReader(uint32_t filenum, uint64_t offset) = 0;

  // Set/Get Producer filenum and offset with lock
  virtual Status GetProducerStatus(uint32_t* filenum, uint64_t* pro_offset) = 0;
  virtual Status SetProducerStatus(uint32_t filenum, uint64_t pro_offset) = 0;

 private:

  // No copying allowed
  Binlog(const Binlog&);
  void operator=(const Binlog&);
};

class BinlogReader {
 public:
  BinlogReader() { }
  virtual ~BinlogReader() { }

  virtual Status ReadRecord(std::string &record) = 0;
  //bool ReadRecord(Slice* record, std::string* scratch) = 0;

 private:

  // No copying allowed;
  BinlogReader(const BinlogReader&);
  void operator=(const BinlogReader&);
};

}   // namespace pstd


#endif  // __PSTD_BINLOG_H__
