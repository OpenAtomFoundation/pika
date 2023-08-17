/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <memory>
#include <thread>
#include "async_buffer.h"
#include "memory_file.h"
#include "pstring.h"
#include "store.h"

namespace pikiwidb {
extern pid_t g_rewritePid;

class PAOFThreadController {
 public:
  static PAOFThreadController& Instance();

  PAOFThreadController(const PAOFThreadController&) = delete;
  void operator=(const PAOFThreadController&) = delete;

  ~PAOFThreadController();

  void Start();
  void Stop();

  void SaveCommand(const std::vector<PString>& params, int db);
  bool ProcessTmpBuffer(BufferSequence& bf);
  void SkipTmpBuffer(size_t n);

  static void RewriteDoneHandler(int exit, int signal);

 private:
  PAOFThreadController() : lastDB_(-1) {}

  class AOFThread {
    friend class PAOFThreadController;

   public:
    AOFThread() : alive_(false) {}
    ~AOFThread();

    void SetAlive() { alive_ = true; }
    bool IsAlive() const { return alive_; }
    void Stop();

    // void  Close();
    void SaveCommand(const std::vector<PString>& params);

    bool Flush();

    void Run();

    std::thread thread_;
    std::atomic<bool> alive_;

    OutputMemoryFile file_;
    AsyncBuffer buf_;
  };

  void writeSelectDB(int db, AsyncBuffer& dst);

  std::shared_ptr<AOFThread> aofThread_;
  AsyncBuffer aofBuffer_;
  int lastDB_ = -1;
};

class PAOFLoader {
 public:
  PAOFLoader();

  bool Load(const char* name);

  const std::vector<std::vector<PString> >& GetCmds() const { return cmds_; }

 private:
  std::vector<std::vector<PString> > cmds_;
};

template <typename DEST>
inline void WriteBulkString(const char* str, size_t strLen, DEST& dst) {
  char tmp[32];
  size_t n = snprintf(tmp, sizeof tmp, "$%lu\r\n", strLen);

  dst.Write(tmp, n);
  dst.Write(str, strLen);
  dst.Write("\r\n", 2);
}

template <typename DEST>
inline void WriteBulkString(const PString& str, DEST& dst) {
  WriteBulkString(str.data(), str.size(), dst);
}

template <typename DEST>
inline void WriteMultiBulkLong(long val, DEST& dst) {
  char tmp[32];
  size_t n = snprintf(tmp, sizeof tmp, "*%lu\r\n", val);
  dst.Write(tmp, n);
}

template <typename DEST>
inline void WriteBulkLong(long val, DEST& dst) {
  char tmp[32];
  size_t n = snprintf(tmp, sizeof tmp, "%lu", val);

  WriteBulkString(tmp, n, dst);
}

template <typename DEST>
inline void SaveCommand(const std::vector<PString>& params, DEST& dst) {
  WriteMultiBulkLong(params.size(), dst);

  for (const auto& s : params) {
    WriteBulkString(s, dst);
  }
}

}  // namespace pikiwidb
