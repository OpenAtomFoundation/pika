// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_BINLOG_BGWORKER_H_
#define PIKA_BINLOG_BGWORKER_H_
#include "pika_command.h"
#include "pink/include/bg_thread.h"

class BinlogBGWorker {
 public:
  BinlogBGWorker(int full)
      : binlogbg_thread_(full) {
    cmds_.reserve(300);
    InitCmdTable(&cmds_);
    binlogbg_thread_.set_thread_name("BinlogBGWorker");
  }
  ~BinlogBGWorker() {
    DestoryCmdTable(&cmds_);
  }
  Cmd* GetCmd(const std::string& opt) {
    return GetCmdFromTable(opt, cmds_);
  }
  void Schedule(PikaCmdArgsType *argv, const std::string& raw_args,
                uint64_t serial, bool readonly) {
    BinlogBGArg *arg = new BinlogBGArg(argv, raw_args, serial, readonly, this);
    binlogbg_thread_.StartThread();
    binlogbg_thread_.Schedule(&DoBinlogBG, static_cast<void*>(arg));
  }
  static void DoBinlogBG(void* arg);

 private:
  CmdTable cmds_;
  pink::BGThread binlogbg_thread_;
  
  struct BinlogBGArg {
    PikaCmdArgsType *argv;
    std::string raw_args;
    uint64_t serial;
    bool readonly; // Server readonly status at the view of binlog dispatch thread
    BinlogBGWorker *myself;
    BinlogBGArg(PikaCmdArgsType* _argv, const std::string& _raw, uint64_t _s,
                bool _readonly, BinlogBGWorker* _my)
        : argv(_argv), raw_args(_raw), serial(_s), readonly(_readonly), myself(_my) {
    }
  };

};


#endif
