/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */


#include "unbounded_buffer.h"
#include <cassert>
#include <iostream>
#include <limits>

namespace pikiwidb {

const std::size_t UnboundedBuffer::MAX_BUFFER_SIZE = std::numeric_limits<std::size_t>::max() / 2;

std::size_t UnboundedBuffer::Write(const void* pData, std::size_t nSize) { return PushData(pData, nSize); }

std::size_t UnboundedBuffer::PushData(const void* pData, std::size_t nSize) {
  std::size_t nBytes = PushDataAt(pData, nSize);
  AdjustWritePtr(nBytes);

  return nBytes;
}

std::size_t UnboundedBuffer::PushDataAt(const void* pData, std::size_t nSize, std::size_t offset) {
  if (!pData || nSize == 0) {
    return 0;
  }

  if (ReadableSize() == UnboundedBuffer::MAX_BUFFER_SIZE) {
    return 0;
  }

  assureSpace(nSize + offset);

  assert(nSize + offset <= WritableSize());

  ::memcpy(&buffer_[writePos_ + offset], pData, nSize);
  return nSize;
}

std::size_t UnboundedBuffer::PeekData(void* pBuf, std::size_t nSize) {
  std::size_t nBytes = PeekDataAt(pBuf, nSize);
  AdjustReadPtr(nBytes);

  return nBytes;
}

std::size_t UnboundedBuffer::PeekDataAt(void* pBuf, std::size_t nSize, std::size_t offset) {
  const std::size_t dataSize = ReadableSize();
  if (!pBuf || nSize == 0 || dataSize <= offset) {
    return 0;
  }

  if (nSize + offset > dataSize) {
    nSize = dataSize - offset;
  }

  ::memcpy(pBuf, &buffer_[readPos_ + offset], nSize);

  return nSize;
}

void UnboundedBuffer::assureSpace(std::size_t nSize) {
  if (nSize <= WritableSize()) {
    return;
  }

  std::size_t maxSize = buffer_.size();

  while (nSize > WritableSize() + readPos_) {
    if (maxSize < 64) {
      maxSize = 64;
    } else if (maxSize <= UnboundedBuffer::MAX_BUFFER_SIZE) {
      maxSize += (maxSize / 2);
    } else {
      break;
    }

    buffer_.resize(maxSize);
  }

  if (readPos_ > 0) {
    std::size_t dataSize = ReadableSize();
    std::cout << dataSize << " bytes moved from " << readPos_ << std::endl;
    ::memmove(&buffer_[0], &buffer_[readPos_], dataSize);
    readPos_ = 0;
    writePos_ = dataSize;
  }
}

void UnboundedBuffer::Shrink(bool tight) {
  assert(buffer_.capacity() == buffer_.size());

  if (buffer_.empty()) {
    assert(readPos_ == 0);
    assert(writePos_ == 0);
    return;
  }

  std::size_t oldCap = buffer_.size();
  std::size_t dataSize = ReadableSize();
  if (!tight && dataSize > oldCap / 2) {
    return;
  }

  std::vector<char> tmp;
  tmp.resize(dataSize);
  memcpy(&tmp[0], &buffer_[readPos_], dataSize);
  tmp.swap(buffer_);

  readPos_ = 0;
  writePos_ = dataSize;

  std::cout << oldCap << " shrink to " << buffer_.size() << std::endl;
}

void UnboundedBuffer::Clear() { readPos_ = writePos_ = 0; }

void UnboundedBuffer::Swap(UnboundedBuffer& buf) {
  buffer_.swap(buf.buffer_);
  std::swap(readPos_, buf.readPos_);
  std::swap(writePos_, buf.writePos_);
}

#if 0
int main()
{
    UnboundedBuffer    buf;
    std::size_t ret = buf.PushData("hello", 5);
    assert (ret == 5);

    char tmp[10];
    ret = buf.PeekData(tmp, sizeof tmp);
    assert(ret == 5);
    assert(tmp[0] == 'h');

    assert(buf.IsEmpty());

    ret = buf.PushData("world", 5);
    assert (ret == 5);
    ret = buf.PushData("abcde", 5);
    assert (ret == 5);
    ret = buf.PeekData(tmp, 5);
    assert(tmp[0] == 'w');

    buf.Clear();
    buf.Shrink();

#  if 1
    ret = buf.PeekData(tmp, 5);
    if (ret == 5) {
        assert(tmp[0] == 'a');
        assert(tmp[1] == 'b');
    }
#  endif
    buf.Shrink();

    return 0;
}

#endif

}  // namespace pikiwidb
