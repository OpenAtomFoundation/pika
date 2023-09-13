/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cassert>
#include <cstring>

#include "memory_file.h"

using std::size_t;

static const size_t kDefaultSize = 1 * 1024 * 1024;

static const int kInvalidFile = -1;
static char* const kInvalidAddr = reinterpret_cast<char*>(-1);

InputMemoryFile::InputMemoryFile() : file_(kInvalidFile), pMemory_(kInvalidAddr), offset_(0), size_(0) {}

InputMemoryFile::~InputMemoryFile() { Close(); }

bool InputMemoryFile::_MapReadOnly() {
  assert(file_ != kInvalidFile);
  assert(size_ == 0);

  struct stat st;
  fstat(file_, &st);
  size_ = st.st_size;

  pMemory_ = (char*)::mmap(0, size_, PROT_READ, MAP_PRIVATE, file_, 0);
  return pMemory_ != kInvalidAddr;
}

bool InputMemoryFile::Open(const char* file) {
  Close();

  file_ = ::open(file, O_RDONLY);

  if (file_ == kInvalidFile) {
    char err[128];
    snprintf(err, sizeof err - 1, "OpenForRead %s failed\n", file);
    return false;
  }

  offset_ = 0;
  return _MapReadOnly();
}

void InputMemoryFile::Close() {
  if (file_ != kInvalidFile) {
    ::munmap(pMemory_, size_);
    ::close(file_);

    file_ = kInvalidFile;
    size_ = 0;
    pMemory_ = kInvalidAddr;
    offset_ = 0;
  }
}

const char* InputMemoryFile::Read(std::size_t& len) {
  if (size_ <= offset_) {
    return nullptr;
  }

  if (offset_ + len > size_) {
    len = size_ - offset_;
  }

  return pMemory_ + offset_;
}

void InputMemoryFile::Skip(size_t len) {
  offset_ += len;
  assert(offset_ <= size_);
}

bool InputMemoryFile::IsOpen() const { return file_ != kInvalidFile; }

// OutputMemoryFile

OutputMemoryFile::OutputMemoryFile() : file_(kInvalidFile), pMemory_(kInvalidAddr), offset_(0), size_(0), syncPos_(0) {}

OutputMemoryFile::~OutputMemoryFile() { Close(); }

void OutputMemoryFile::_ExtendFileSize(size_t size) {
  assert(file_ != kInvalidFile);

  if (size > size_) {
    Truncate(size);
  }
}

bool OutputMemoryFile::Open(const std::string& file, bool bAppend) { return Open(file.c_str(), bAppend); }

bool OutputMemoryFile::Open(const char* file, bool bAppend) {
  Close();

  file_ = ::open(file, O_RDWR | O_CREAT | (bAppend ? O_APPEND : 0), 0644);

  if (file_ == kInvalidFile) {
    char err[128];
    snprintf(err, sizeof err - 1, "OpenWriteOnly %s failed\n", file);
    perror(err);
    return false;
  }
  if (bAppend) {
    struct stat st;
    fstat(file_, &st);
    size_ = std::max<decltype(size_)>(st.st_size, kDefaultSize);
    offset_ = st.st_size;
  } else {
    size_ = kDefaultSize;
    offset_ = 0;
  }

  ::ftruncate(file_, size_);
  return _MapWriteOnly();
}

void OutputMemoryFile::Close() {
  if (file_ != kInvalidFile) {
    ::munmap(pMemory_, size_);
    ::ftruncate(file_, offset_);
    ::close(file_);

    file_ = kInvalidFile;
    size_ = 0;
    pMemory_ = kInvalidAddr;
    offset_ = 0;
    syncPos_ = 0;
  }
}

bool OutputMemoryFile::Sync() {
  if (file_ == kInvalidFile) {
    return false;
  }

  if (syncPos_ >= offset_) {
    return false;
  }

  ::msync(pMemory_ + syncPos_, offset_ - syncPos_, MS_SYNC);
  syncPos_ = offset_;

  return true;
}

bool OutputMemoryFile::_MapWriteOnly() {
  if (size_ == 0 || file_ == kInvalidFile) {
    return false;
  }

#if 0
    // codes below cause coredump when file size > 4MB
    if (m_pMemory != kInvalidAddr) {
      ::munmap(m_pMemory, m_size);
    }
#endif
  pMemory_ = (char*)::mmap(0, size_, PROT_WRITE, MAP_SHARED, file_, 0);
  return (pMemory_ != kInvalidAddr);
}

void OutputMemoryFile::Truncate(std::size_t size) {
  if (size == size_) {
    return;
  }

  size_ = size;
  ::ftruncate(file_, size);

  if (offset_ > size_) {
    offset_ = size_;
  }

  _MapWriteOnly();
}

void OutputMemoryFile::TruncateTailZero() {
  if (file_ == kInvalidFile) {
    return;
  }

  size_t tail = size_;
  while (tail > 0 && pMemory_[--tail] == '\0')
    ;

  ++tail;

  Truncate(tail);
}

bool OutputMemoryFile::IsOpen() const { return file_ != kInvalidFile; }

// consumer
void OutputMemoryFile::Write(const void* data, size_t len) {
  _AssureSpace(len);

  ::memcpy(pMemory_ + offset_, data, len);
  offset_ += len;
  assert(offset_ <= size_);
}

void OutputMemoryFile::_AssureSpace(size_t size) {
  size_t newSize = size_;

  while (offset_ + size > newSize) {
    if (newSize == 0) {
      newSize = kDefaultSize;
    } else {
      newSize <<= 1;
    }
  }

  _ExtendFileSize(newSize);
}
