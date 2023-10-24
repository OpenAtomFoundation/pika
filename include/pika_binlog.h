// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_BINLOG_H_
#define PIKA_BINLOG_H_

#include <atomic>

#include "pstd/include/env.h"
#include "pstd/include/pstd_mutex.h"
#include "pstd/include/pstd_status.h"
#include "pstd/include/noncopyable.h"
#include "pstd/include/pika_define.h"

std::string NewFileName(const std::string& name, uint32_t current);

class Version final : public pstd::noncopyable {
 public:
  Version(const std::shared_ptr<pstd::RWFile>& save);
  ~Version();

  pstd::Status Init();

  // RWLock should be held when access members.
  pstd::Status StableSave();

  uint32_t pro_num_ = 0;
  uint64_t pro_offset_ = 0;
  uint64_t logic_id_ = 0;
  uint32_t term_ = 0;

  std::shared_mutex rwlock_;

  void debug() {
    std::shared_lock l(rwlock_);
    printf("Current pro_num %u pro_offset %llu\n", pro_num_, pro_offset_);
  }

 private:
  // shared with versionfile_
  std::shared_ptr<pstd::RWFile> save_;
};

class Binlog : public pstd::noncopyable {
 public:
  Binlog(std::string  Binlog_path, int file_size = 100 * 1024 * 1024);
  ~Binlog();

  void Lock() { mutex_.lock(); }
  void Unlock() { mutex_.unlock(); }

  pstd::Status Put(const std::string& item);

  pstd::Status GetProducerStatus(uint32_t* filenum, uint64_t* pro_offset, uint32_t* term = nullptr, uint64_t* logic_id = nullptr);
  /*
   * Set Producer pro_num and pro_offset with lock
   */
  pstd::Status SetProducerStatus(uint32_t pro_num, uint64_t pro_offset, uint32_t term = 0, uint64_t index = 0);
  // Need to hold Lock();
  pstd::Status Truncate(uint32_t pro_num, uint64_t pro_offset, uint64_t index);

  std::string filename() { return filename_; }

  // need to hold mutex_
  void SetTerm(uint32_t term) {
    std::lock_guard l(version_->rwlock_);
    version_->term_ = term;
    version_->StableSave();
  }

  uint32_t term() {
    std::shared_lock l(version_->rwlock_);
    return version_->term_;
  }

  void Close();

 private:
  pstd::Status Put(const char* item, int len);
  pstd::Status EmitPhysicalRecord(RecordType t, const char* ptr, size_t n, int* temp_pro_offset);
  static pstd::Status AppendPadding(pstd::WritableFile* file, uint64_t* len);
  void InitLogFile();

  /*
   * Produce
   */
  pstd::Status Produce(const pstd::Slice& item, int* pro_offset);

  std::atomic<bool> opened_;

  std::unique_ptr<Version> version_;
  std::unique_ptr<pstd::WritableFile> queue_;
  // versionfile_ can only be used as a shared_ptr, and it will be used as a variable version_ in the ~Version() function.
  std::shared_ptr<pstd::RWFile> versionfile_;

  pstd::Mutex mutex_;

  uint32_t pro_num_ = 0;

  int block_offset_ = 0;

  const std::string binlog_path_;

  uint64_t file_size_ = 0;

  std::string filename_;

  std::atomic<bool> binlog_io_error_;
};

#endif
