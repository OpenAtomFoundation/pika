// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef SLASH_BINLOG_IMPL_H_
#define SLASH_BINLOG_IMPL_H_

#include <atomic>
#include <stddef.h>
#include <string>
#include <assert.h>

#include "slash/include/env.h"
#include "slash/include/slash_binlog.h"
#include "slash/include/slash_status.h"
#include "slash/include/slash_mutex.h"

namespace slash {

class Version;
class BinlogReader;

// SyncPoint is a file number and an offset;

const std::string kBinlogPrefix = "binlog";
const std::string kManifest = "manifest";
const int kBinlogSize = 128;
//const int kBinlogSize = (100 << 20);
const int kBlockSize = (64 << 10);
// Header is Type(1 byte), length (3 bytes), time (4 bytes)
const size_t kHeaderSize = 1 + 3 + 4;

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

class BinlogImpl : public Binlog {
 public:
  BinlogImpl(const std::string& path, const int file_size = (100 < 20));
  virtual ~BinlogImpl();

  //
  // Basic API
  //
  virtual Status Append(const std::string &item);
  //Status Append(const char* item, int len);
  virtual BinlogReader* NewBinlogReader(uint32_t filenum, uint64_t offset);

  virtual Status GetProducerStatus(uint32_t* filenum, uint64_t* offset);
  virtual Status SetProducerStatus(uint32_t filenum, uint64_t pro_offset);

 private:
  friend class Binlog;

  //
  // More specify API, used by Pika
  //
  Status Recover();
  static Status AppendBlank(WritableFile *file, uint64_t len);
  WritableFile *queue() { return queue_; }
  uint64_t file_size() {
    return file_size_;
  }

  void Lock()         { mutex_.Lock(); }
  void Unlock()       { mutex_.Unlock(); }

  void InitOffset();
  Status EmitPhysicalRecord(RecordType t, const char *ptr, size_t n, int *temp_pro_offset);

  // Produce
  Status Produce(const Slice &item, int *pro_offset);

 private:
  Mutex mutex_;
  bool exit_all_consume_;
  std::string path_;
  uint64_t file_size_;
  uint32_t pro_num_;
  uint64_t record_num_;

  Version* version_;
  WritableFile *queue_;
  RWFile *versionfile_;

  int block_offset_;
  char* pool_;

  //std::unordered_map<uint32_t, MemTable*> memtables_;

  // Not use
  //std::string filename;
  //int32_t retry_;
  //uint32_t consumer_num_;

  // No copying allowed
  BinlogImpl(const BinlogImpl&);
  void operator=(const BinlogImpl&);
};

class Version {
 public:
  Version(RWFile *save);
  ~Version();

  Status Init();
  // RWLock should be held when access members.
  Status StableSave();

  uint64_t pro_offset_;
  uint32_t pro_num_;
  uint32_t item_num_;

  RWMutex rwlock_;

  void debug() {
    ReadLock(&this->rwlock_);
    printf ("Current pro_num %u pro_offset %lu\n", pro_num_, pro_offset_);
  }

 private:

  RWFile *save_;

  // Not used
  //uint64_t con_offset_;
  //uint32_t con_num_;

  // No copying allowed;
  Version(const Version&);
  void operator=(const Version&);
};

class BinlogReaderImpl : public BinlogReader {
 public:
  BinlogReaderImpl(Binlog* log, const std::string& path, uint32_t filenum, uint64_t offset);
  ~BinlogReaderImpl();

  //bool ReadRecord(Slice* record, std::string* scratch);
  virtual Status ReadRecord(std::string &record);

 private:
  friend class BinlogImpl;

  //Status Parse(std::string &scratch);
  Status Consume(std::string &scratch);
  unsigned int ReadPhysicalRecord(Slice *fragment);

  // Tirm offset to first record behind offered offset.
  Status Trim();
  // Return next record end offset in a block, store in result if error encounted.
  uint64_t GetNext(Status &result);

  Binlog* log_;
  std::string path_;
  uint32_t filenum_;
  uint64_t offset_;
  std::atomic<bool> should_exit_;

  // not used
  uint64_t initial_offset_;
  uint64_t last_record_offset_;
  uint64_t end_of_buffer_offset_;

  SequentialFile* queue_;
  char* const backing_store_;
  Slice buffer_;

  // No copying allowed;
  BinlogReaderImpl(const BinlogReaderImpl&);
  void operator=(const BinlogReaderImpl&);
};

}   // namespace slash


#endif  // SLASH_BINLOG_IMPL_H_
