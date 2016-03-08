#ifndef __PIKA_KV_H__
#define __PIKA_KV_H__
#include "pika_command.h"

/*
 * kv
 */
class SetCmd : public Cmd {
public:
  enum SetCondition{kANY, kNX, kXX};
  SetCmd() : sec_(0), condition_(kANY) {};
  virtual void Do();
  virtual void Initial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
private:
  std::string key_;
  std::string value_;
  int64_t sec_;
  SetCmd::SetCondition condition_;
};

class GetCmd : public Cmd {
public:
  GetCmd() {};
  virtual void Do();
  virtual void Initial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
private:
  std::string key_;
};
#endif
