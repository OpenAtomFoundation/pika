/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */


#pragma once

#include <cstring>
#include <vector>

namespace pikiwidb {

class UnboundedBuffer {
 public:
  UnboundedBuffer() : readPos_(0), writePos_(0) {}

  std::size_t PushDataAt(const void* pData, std::size_t nSize, std::size_t offset = 0);
  std::size_t PushData(const void* pData, std::size_t nSize);
  std::size_t Write(const void* pData, std::size_t nSize);
  void AdjustWritePtr(std::size_t nBytes) { writePos_ += nBytes; }

  std::size_t PeekDataAt(void* pBuf, std::size_t nSize, std::size_t offset = 0);
  std::size_t PeekData(void* pBuf, std::size_t nSize);
  void AdjustReadPtr(std::size_t nBytes) { readPos_ += nBytes; }

  char* ReadAddr() { return &buffer_[readPos_]; }
  char* WriteAddr() { return &buffer_[writePos_]; }

  bool IsEmpty() const { return ReadableSize() == 0; }
  std::size_t ReadableSize() const { return writePos_ - readPos_; }
  std::size_t WritableSize() const { return buffer_.size() - writePos_; }

  void Shrink(bool tight = false);
  void Clear();
  void Swap(UnboundedBuffer& buf);

  static const std::size_t MAX_BUFFER_SIZE;

 private:
  void assureSpace(std::size_t size);
  std::size_t readPos_ = 0;
  std::size_t writePos_ = 0;
  std::vector<char> buffer_;
};

}  // namespace pikiwidb

