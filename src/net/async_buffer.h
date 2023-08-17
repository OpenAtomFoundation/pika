/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <atomic>
#include <mutex>

#include "buffer.h"
#include "unbounded_buffer.h"

class AsyncBuffer {
 public:
  explicit AsyncBuffer(std::size_t size = 128 * 1024);
  ~AsyncBuffer();

  void Write(const void* data, std::size_t len);
  void Write(const BufferSequence& data);

  void ProcessBuffer(BufferSequence& data);
  void Skip(std::size_t size);

 private:
  // for async write
  Buffer buffer_;

  // double buffer
  pikiwidb::UnboundedBuffer tmpBuf_;

  std::mutex backBufLock_;
  std::atomic<std::size_t> backBytes_;
  pikiwidb::UnboundedBuffer backBuf_;
};
