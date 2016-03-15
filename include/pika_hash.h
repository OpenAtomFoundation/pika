#ifndef __PIKA_HASH_H__
#define __PIKA_HASH_H__
#include "pika_command.h"
#include "nemo.h"


/*
 * hash
 */

class HDelCmd : public Cmd {
public:
  HDelCmd() {}
  virtual void Do();
private:
  std::string key_;
  std::vector<std::string> fields_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class HGetCmd : public Cmd {
public:
  HGetCmd() {}
  virtual void Do();
private:
  std::string key_, field_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class HSetCmd : public Cmd {
public:
  HSetCmd() {}
  virtual void Do();
private:
  std::string key_, field_, value_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

#endif
