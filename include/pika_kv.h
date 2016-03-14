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
private:
  std::string key_;
  std::string value_;
  int64_t sec_;
  SetCmd::SetCondition condition_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
  virtual void Clear() {
    sec_ = 0;
    condition_ = kANY;
  }
};

class GetCmd : public Cmd {
public:
  GetCmd() {};
  virtual void Do();
private:
  std::string key_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class DelCmd : public Cmd {
public:
  DelCmd() {}
  virtual void Do();
  virtual void Initial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
private:
  std::vector<std::string> keys_;
};

class IncrCmd : public Cmd {
public:
  IncrCmd() {}
  virtual void Do();
  virtual void Initial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
private:
  std::string key_;
};

class IncrbyCmd : public Cmd {
public:
  IncrbyCmd() {}
  virtual void Do();
  virtual void Initial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
private:
  std::string key_;
  int64_t by_;
};

class IncrbyfloatCmd : public Cmd {
public:
  IncrbyfloatCmd() {}
  virtual void Do();
  virtual void Initial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
private:
  std::string key_;
  double by_;
};

class DecrCmd : public Cmd {
public:
  DecrCmd() {}
  virtual void Do();
  virtual void Initial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
private:
  std::string key_;
};

class DecrbyCmd : public Cmd {
public:
  DecrbyCmd() {}
  virtual void Do();
  virtual void Initial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
private:
  std::string key_;
  int64_t by_;
};

class GetsetCmd : public Cmd {
public:
  GetsetCmd() {}
  virtual void Do();
  virtual void Initial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
private:
  std::string key_;
  std::string new_value_;
};

class AppendCmd : public Cmd {
public:
  AppendCmd() {}
  virtual void Do();
  virtual void Initial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
private:
  std::string key_;
  std::string value_;
};

class MgetCmd : public Cmd {
public:
  MgetCmd() {}
  virtual void Do();
  virtual void Initial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
private:
  std::vector<std::string> keys_;
};
#endif
