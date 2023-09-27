/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "io_thread_pool.h"
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
  const pikiwidb::PString& GetConfigName() const { return cfg_file_; }

  bool Init();
  void Run();
  void Recycle();
  void Stop();

  void OnNewConnection(pikiwidb::TcpObject* obj);

  std::unique_ptr<pikiwidb::CmdTableManager>& CmdTableManager();

 public:
  pikiwidb::PString cfg_file_;
  unsigned short port_;
  pikiwidb::PString log_level_;

  pikiwidb::PString master_;
  unsigned short master_port_;

  static const unsigned kRunidSize;

 private:
  pikiwidb::IOThreadPool& io_threads_;
  std::unique_ptr<pikiwidb::CmdTableManager> cmd_table_manager_;
};

extern std::unique_ptr<PikiwiDB> g_pikiwidb;
