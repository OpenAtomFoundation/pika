/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <assert.h>

#if defined(__APPLE__)
#  include <unistd.h>
#endif

#include "async_buffer.h"

using std::size_t;

AsyncBuffer::AsyncBuffer(size_t size) : buffer_(size), backBytes_(0) {}

AsyncBuffer::~AsyncBuffer() {
  //  assert (buffer_.IsEmpty());
  // assert (backBytes_ == 0);
}

void AsyncBuffer::Write(const void* data, size_t len) {
  BufferSequence bf;
  bf.buffers[0].iov_base = const_cast<void*>(data);
  bf.buffers[0].iov_len = len;
  bf.count = 1;

  this->Write(bf);
}

void AsyncBuffer::Write(const BufferSequence& data) {
  auto len = data.TotalBytes();

  if (backBytes_ > 0 || buffer_.WritableSize() < len) {
    std::lock_guard<std::mutex> guard(backBufLock_);

    if (backBytes_ > 0 || buffer_.WritableSize() < len) {
      for (size_t i = 0; i < data.count; ++i) {
        backBuf_.PushData(data.buffers[i].iov_base, data.buffers[i].iov_len);
      }

      backBytes_ += len;
      assert(backBytes_ == backBuf_.ReadableSize());

      return;
    }
  }

  assert(backBytes_ == 0 && buffer_.WritableSize() >= len);

  for (size_t i = 0; i < data.count; ++i) {
    buffer_.PushData(data.buffers[i].iov_base, data.buffers[i].iov_len);
  }
}

void AsyncBuffer::ProcessBuffer(BufferSequence& data) {
  data.count = 0;

  // Here be dragons! see below...
  if (!tmpBuf_.IsEmpty()) {
    data.count = 1;
    data.buffers[0].iov_base = tmpBuf_.ReadAddr();
    data.buffers[0].iov_len = tmpBuf_.ReadableSize();
  } else if (!buffer_.IsEmpty()) {
    auto nLen = buffer_.ReadableSize();

    buffer_.GetDatum(data, nLen);
    assert(nLen == data.TotalBytes());
  } else {
    if (backBytes_ > 0 && backBufLock_.try_lock()) {
      // tmpBuf_ is used for process backBuf_ without held mutex!
      backBytes_ = 0;
      tmpBuf_.Swap(backBuf_);
      backBufLock_.unlock();

      data.count = 1;
      data.buffers[0].iov_base = tmpBuf_.ReadAddr();
      data.buffers[0].iov_len = tmpBuf_.ReadableSize();
    }
  }
}

void AsyncBuffer::Skip(size_t size) {
  if (!tmpBuf_.IsEmpty()) {
    assert(size <= tmpBuf_.ReadableSize());
    tmpBuf_.AdjustReadPtr(size);
  } else {
    assert(buffer_.ReadableSize() >= size);
    buffer_.AdjustReadPtr(size);
  }
}
