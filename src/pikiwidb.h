/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "cmd_table_manager.h"
#include "event_loop.h"
#include "pstring.h"
#include "tcp_obj.h"

#define PIKIWIDB_VERSION "4.0.0"

class PikiwiDB final {
 public:
  PikiwiDB();
  ~PikiwiDB();

  bool ParseArgs(int ac, char* av[]);
  const pikiwidb::PString& GetConfigName() const { return cfgFile_; }

  bool Init();
  void Run();
  void Recycle();
  void Stop();

  void OnNewConnection(pikiwidb::TcpObject* obj);

  std::unique_ptr<pikiwidb::CmdTableManager>& CmdTableManager();

 public:
  pikiwidb::EventLoop event_loop_;

  pikiwidb::PString cfgFile_;
  unsigned short port_;
  pikiwidb::PString logLevel_;

  pikiwidb::PString master_;
  unsigned short masterPort_;

  static const unsigned kRunidSize;

 private:
  std::unique_ptr<pikiwidb::CmdTableManager> cmdTableManager_;
};

extern std::unique_ptr<PikiwiDB> g_pikiwidb;