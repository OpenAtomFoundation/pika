/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <sys/uio.h>
#include <atomic>
#include <cassert>
#include <cstring>
#include <string>
#include <vector>

struct BufferSequence {
  static const std::size_t kMaxIovec = 16;

  iovec buffers[kMaxIovec];
  std::size_t count;

  std::size_t TotalBytes() const {
    assert(count <= kMaxIovec);
    std::size_t nBytes = 0;
    for (std::size_t i = 0; i < count; ++i) {
      nBytes += buffers[i].iov_len;
    }

    return nBytes;
  }
};

// static const std::size_t  DEFAULT_BUFFER_SIZE = 4 * 1024;

inline std::size_t RoundUp2Power(std::size_t size) {
  if (0 == size) {
    return 0;
  }

  std::size_t roundSize = 1;
  while (roundSize < size) {
    roundSize <<= 1;
  }

  return roundSize;
}

template <typename BUFFER>
class CircularBuffer {
 public:
  // Constructor  to be specialized
  explicit CircularBuffer(std::size_t size = 0) : maxSize_(size), readPos_(0), writePos_(0) {}
  CircularBuffer(const BufferSequence& bf);
  CircularBuffer(char*, std::size_t);
  ~CircularBuffer() {}

  bool IsEmpty() const { return writePos_ == readPos_; }
  bool IsFull() const { return ((writePos_ + 1) & (maxSize_ - 1)) == readPos_; }

  // For gather write
  void GetDatum(BufferSequence& buffer, std::size_t maxSize, std::size_t offset = 0);

  // For scatter read
  void GetSpace(BufferSequence& buffer, std::size_t offset = 0);

  // Put data into internal buffer_
  bool PushData(const void* pData, std::size_t nSize);
  bool PushDataAt(const void* pData, std::size_t nSize, std::size_t offset = 0);

  // Get data from internal buffer_, adjust read ptr
  bool PeekData(void* pBuf, std::size_t nSize);
  bool PeekDataAt(void* pBuf, std::size_t nSize, std::size_t offset = 0);

  char* ReadAddr() { return &buffer_[readPos_]; }
  char* WriteAddr() { return &buffer_[writePos_]; }

  void AdjustWritePtr(std::size_t size);
  void AdjustReadPtr(std::size_t size);

  std::size_t ReadableSize() const { return (writePos_ - readPos_) & (maxSize_ - 1); }
  std::size_t WritableSize() const { return maxSize_ - ReadableSize() - 1; }

  std::size_t Capacity() const { return maxSize_; }
  void InitCapacity(std::size_t size);

  template <typename T>
  CircularBuffer& operator<<(const T& data);
  template <typename T>
  CircularBuffer& operator>>(T& data);

  template <typename T>
  CircularBuffer& operator<<(const std::vector<T>&);
  template <typename T>
  CircularBuffer& operator>>(std::vector<T>&);

  CircularBuffer& operator<<(const std::string& str);
  CircularBuffer& operator>>(std::string& str);

 protected:
  // The max capacity of buffer_
  std::size_t maxSize_ = 0;

 private:
  // The starting address can be read
  std::atomic<std::size_t> readPos_;

  // The starting address can be write
  std::atomic<std::size_t> writePos_;

  // The real internal buffer
  BUFFER buffer_;

  bool owned_ = false;
};

template <typename BUFFER>
void CircularBuffer<BUFFER>::GetDatum(BufferSequence& buffer, std::size_t maxSize, std::size_t offset) {
  if (maxSize == 0 || offset >= ReadableSize()) {
    buffer.count = 0;
    return;
  }

  assert(readPos_ < maxSize_);
  assert(writePos_ < maxSize_);

  std::size_t bufferIndex = 0;
  const std::size_t readPos = (readPos_ + offset) & (maxSize_ - 1);
  const std::size_t writePos = writePos_;
  assert(readPos != writePos);

  buffer.buffers[bufferIndex].iov_base = &buffer_[readPos];
  if (readPos < writePos) {
    if (maxSize < writePos - readPos) {
      buffer.buffers[bufferIndex].iov_len = maxSize;
    } else {
      buffer.buffers[bufferIndex].iov_len = writePos - readPos;
    }
  } else {
    std::size_t nLeft = maxSize;
    if (nLeft > (maxSize_ - readPos)) {
      nLeft = (maxSize_ - readPos);
    }
    buffer.buffers[bufferIndex].iov_len = nLeft;
    nLeft = maxSize - nLeft;

    if (nLeft > 0 && writePos > 0) {
      if (nLeft > writePos) {
        nLeft = writePos;
      }

      ++bufferIndex;
      buffer.buffers[bufferIndex].iov_base = &buffer_[0];
      buffer.buffers[bufferIndex].iov_len = nLeft;
    }
  }

  buffer.count = bufferIndex + 1;
}

template <typename BUFFER>
void CircularBuffer<BUFFER>::GetSpace(BufferSequence& buffer, std::size_t offset) {
  assert(readPos_ >= 0 && readPos_ < maxSize_);
  assert(writePos_ >= 0 && writePos_ < maxSize_);

  if (WritableSize() <= offset + 1) {
    buffer.count = 0;
    return;
  }

  std::size_t bufferIndex = 0;
  const std::size_t readPos = readPos_;
  const std::size_t writePos = (writePos_ + offset) & (maxSize_ - 1);

  buffer.buffers[bufferIndex].iov_base = &buffer_[writePos];

  if (readPos > writePos) {
    buffer.buffers[bufferIndex].iov_len = readPos - writePos - 1;
    assert(buffer.buffers[bufferIndex].iov_len > 0);
  } else {
    buffer.buffers[bufferIndex].iov_len = maxSize_ - writePos;
    if (0 == readPos) {
      buffer.buffers[bufferIndex].iov_len -= 1;
    } else if (readPos > 1) {
      ++bufferIndex;
      buffer.buffers[bufferIndex].iov_base = &buffer_[0];
      buffer.buffers[bufferIndex].iov_len = readPos - 1;
    }
  }

  buffer.count = bufferIndex + 1;
}

template <typename BUFFER>
bool CircularBuffer<BUFFER>::PushDataAt(const void* pData, std::size_t nSize, std::size_t offset) {
  if (!pData || 0 == nSize) {
    return true;
  }

  if (offset + nSize > WritableSize()) {
    return false;
  }

  const std::size_t readPos = readPos_;
  const std::size_t writePos = (writePos_ + offset) & (maxSize_ - 1);
  if (readPos > writePos) {
    assert(readPos - writePos > nSize);
    ::memcpy(&buffer_[writePos], pData, nSize);
  } else {
    std::size_t availBytes1 = maxSize_ - writePos;
    std::size_t availBytes2 = readPos - 0;
    assert(availBytes1 + availBytes2 >= 1 + nSize);

    if (availBytes1 >= nSize + 1) {
      ::memcpy(&buffer_[writePos], pData, nSize);
    } else {
      ::memcpy(&buffer_[writePos], pData, availBytes1);
      int bytesLeft = static_cast<int>(nSize - availBytes1);
      if (bytesLeft > 0) {
        ::memcpy(&buffer_[0], static_cast<const char*>(pData) + availBytes1, bytesLeft);
      }
    }
  }

  return true;
}

template <typename BUFFER>
bool CircularBuffer<BUFFER>::PushData(const void* pData, std::size_t nSize) {
  if (!PushDataAt(pData, nSize)) {
    return false;
  }

  AdjustWritePtr(nSize);
  return true;
}

template <typename BUFFER>
bool CircularBuffer<BUFFER>::PeekDataAt(void* pBuf, std::size_t nSize, std::size_t offset) {
  if (!pBuf || 0 == nSize) {
    return true;
  }

  if (nSize + offset > ReadableSize()) {
    return false;
  }

  const std::size_t writePos = writePos_;
  const std::size_t readPos = (readPos_ + offset) & (maxSize_ - 1);
  if (readPos < writePos) {
    assert(writePos - readPos >= nSize);
    ::memcpy(pBuf, &buffer_[readPos], nSize);
  } else {
    assert(readPos > writePos);
    std::size_t availBytes1 = maxSize_ - readPos;
    std::size_t availBytes2 = writePos - 0;
    assert(availBytes1 + availBytes2 >= nSize);

    if (availBytes1 >= nSize) {
      ::memcpy(pBuf, &buffer_[readPos], nSize);
    } else {
      ::memcpy(pBuf, &buffer_[readPos], availBytes1);
      assert(nSize - availBytes1 > 0);
      ::memcpy(static_cast<char*>(pBuf) + availBytes1, &buffer_[0], nSize - availBytes1);
    }
  }

  return true;
}

template <typename BUFFER>
bool CircularBuffer<BUFFER>::PeekData(void* pBuf, std::size_t nSize) {
  if (PeekDataAt(pBuf, nSize)) {
    AdjustReadPtr(nSize);
  } else {
    return false;
  }

  return true;
}

template <typename BUFFER>
inline void CircularBuffer<BUFFER>::AdjustWritePtr(std::size_t size) {
  std::size_t writePos = writePos_;
  writePos += size;
  writePos &= maxSize_ - 1;

  writePos_ = writePos;
}

template <typename BUFFER>
inline void CircularBuffer<BUFFER>::AdjustReadPtr(std::size_t size) {
  std::size_t readPos = readPos_;
  readPos += size;
  readPos &= maxSize_ - 1;

  readPos_ = readPos;
}

template <typename BUFFER>
inline void CircularBuffer<BUFFER>::InitCapacity(std::size_t size) {
  assert(size > 0 && size <= 1 * 1024 * 1024 * 1024);

  maxSize_ = RoundUp2Power(size);
  buffer_.resize(maxSize_);
  std::vector<char>(buffer_).swap(buffer_);
}

template <typename BUFFER>
template <typename T>
inline CircularBuffer<BUFFER>& CircularBuffer<BUFFER>::operator<<(const T& data) {
  if (!PushData(&data, sizeof(data))) {
    assert(!!!"Please modify the DEFAULT_BUFFER_SIZE");
  }

  return *this;
}

template <typename BUFFER>
template <typename T>
inline CircularBuffer<BUFFER>& CircularBuffer<BUFFER>::operator>>(T& data) {
  if (!PeekData(&data, sizeof(data))) {
    assert(!!!"Not enough data in buffer_");
  }

  return *this;
}

template <typename BUFFER>
template <typename T>
inline CircularBuffer<BUFFER>& CircularBuffer<BUFFER>::operator<<(const std::vector<T>& v) {
  if (!v.empty()) {
    (*this) << static_cast<unsigned short>(v.size());
    for (typename std::vector<T>::const_iterator it = v.begin(); it != v.end(); ++it) {
      (*this) << *it;
    }
  }
  return *this;
}

template <typename BUFFER>
template <typename T>
inline CircularBuffer<BUFFER>& CircularBuffer<BUFFER>::operator>>(std::vector<T>& v) {
  v.clear();
  unsigned short size;
  *this >> size;
  v.reserve(size);

  while (size--) {
    T t;
    *this >> t;
    v.push_back(t);
  }
  return *this;
}

template <typename BUFFER>
inline CircularBuffer<BUFFER>& CircularBuffer<BUFFER>::operator<<(const std::string& str) {
  *this << static_cast<unsigned short>(str.size());
  if (!PushData(str.data(), str.size())) {
    AdjustWritePtr(static_cast<int>(0 - sizeof(unsigned short)));
    assert(!!!"2Please modify the DEFAULT_BUFFER_SIZE");
  }
  return *this;
}

template <typename BUFFER>
inline CircularBuffer<BUFFER>& CircularBuffer<BUFFER>::operator>>(std::string& str) {
  unsigned short size = 0;
  *this >> size;
  str.clear();
  str.reserve(size);

  char ch;
  while (size--) {
    *this >> ch;
    str += ch;
  }
  return *this;
}

///////////////////////////////////////////////////////////////
typedef CircularBuffer< ::std::vector<char> > Buffer;

template <>
inline Buffer::CircularBuffer(std::size_t maxSize)
    : maxSize_(RoundUp2Power(maxSize)), readPos_(0), writePos_(0), buffer_(maxSize_) {
  assert(0 == (maxSize_ & (maxSize_ - 1)) && "maxSize_ MUST BE power of 2");
}

template <int N>
class StackBuffer : public CircularBuffer<char[N]> {
  using CircularBuffer<char[N]>::maxSize_;

 public:
  StackBuffer() {
    maxSize_ = N;
    if (maxSize_ < 0) {
      maxSize_ = 1;
    }

    if (0 != (maxSize_ & (maxSize_ - 1))) {
      maxSize_ = RoundUp2Power(maxSize_);
    }

    assert(0 == (maxSize_ & (maxSize_ - 1)) && "maxSize_ MUST BE power of 2");
  }
};

typedef CircularBuffer<char*> AttachedBuffer;

template <>
inline AttachedBuffer::CircularBuffer(char* pBuf, std::size_t len)
    : maxSize_(RoundUp2Power(len + 1)), readPos_(0), writePos_(0) {
  buffer_ = pBuf;
  owned_ = false;
}

template <>
inline AttachedBuffer::CircularBuffer(const BufferSequence& bf) : readPos_(0), writePos_(0) {
  owned_ = false;

  if (0 == bf.count) {
    buffer_ = 0;
  } else if (1 == bf.count) {
    buffer_ = (char*)bf.buffers[0].iov_base;
    writePos_ = static_cast<int>(bf.buffers[0].iov_len);
  } else if (bf.count > 1) {
    owned_ = true;
    buffer_ = new char[bf.TotalBytes()];

    std::size_t off = 0;
    for (std::size_t i = 0; i < bf.count; ++i) {
      memcpy(buffer_ + off, bf.buffers[i].iov_base, bf.buffers[i].iov_len);
      off += bf.buffers[i].iov_len;
    }

    writePos_ = bf.TotalBytes();
  }

  maxSize_ = RoundUp2Power(writePos_ - readPos_ + 1);
}

template <>
inline AttachedBuffer::~CircularBuffer() {
  if (owned_) {
    delete[] buffer_;
  }
}

template <typename T>
inline void OverwriteAt(void* addr, T data) {
  memcpy(addr, &data, sizeof(data));
}
