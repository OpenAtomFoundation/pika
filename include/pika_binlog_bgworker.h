#ifndef PIKA_BINLOG_BGWORKER_H_
#define PIKA_BINLOG_BGWORKER_H_
#include "pika_command.h"
#include "bg_thread.h"

class BinlogBGWorker{
public:
  BinlogBGWorker(int full) : binlogbg_thread_(full){
    cmds_.reserve(300);
    InitCmdTable(&cmds_);
  }
  ~BinlogBGWorker() {
    binlogbg_thread_.Stop();
    DestoryCmdTable(cmds_);
  }
  Cmd* GetCmd(const std::string& opt) {
    return GetCmdFromTable(opt, cmds_);
  }
  void Schedule(PikaCmdArgsType *argv, const std::string& raw_args, uint64_t serial, bool readonly) {
    BinlogBGArg *arg = new BinlogBGArg(argv, raw_args, serial, readonly, this);
    binlogbg_thread_.StartIfNeed();
    binlogbg_thread_.Schedule(&DoBinlogBG, static_cast<void*>(arg));
  }
  static void DoBinlogBG(void* arg);

private:
  std::unordered_map<std::string, Cmd*> cmds_;
  pink::BGThread binlogbg_thread_;
  
  struct BinlogBGArg {
    PikaCmdArgsType *argv;
    std::string raw_args;
    uint64_t serial;
    bool readonly; // Server readonly status at the view of binlog dispatch thread
    BinlogBGWorker *myself;
    BinlogBGArg(PikaCmdArgsType* _argv, const std::string& _raw, uint64_t _s, bool _readonly, BinlogBGWorker* _my) :
      argv(_argv), raw_args(_raw), serial(_s), readonly(_readonly), myself(_my) {}
  };

};


#endif
