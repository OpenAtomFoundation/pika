#ifndef PIKA_KV_H_
#define PIKA_KV_H_
#include "pika_command.h"
#include "nemo.h"


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
private:
  std::vector<std::string> keys_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const  CmdInfo* const ptr_info);
};

class IncrCmd : public Cmd {
public:
  IncrCmd() {}
  virtual void Do();
private:
  std::string key_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class IncrbyCmd : public Cmd {
public:
  IncrbyCmd() {}
  virtual void Do();
private:
  std::string key_;
  int64_t by_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class IncrbyfloatCmd : public Cmd {
public:
  IncrbyfloatCmd() {}
  virtual void Do();
private:
  std::string key_;
  double by_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class DecrCmd : public Cmd {
public:
  DecrCmd() {}
  virtual void Do();
private:
  std::string key_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class DecrbyCmd : public Cmd {
public:
  DecrbyCmd() {}
  virtual void Do();
private:
  std::string key_;
  int64_t by_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class GetsetCmd : public Cmd {
public:
  GetsetCmd() {}
  virtual void Do();
private:
  std::string key_;
  std::string new_value_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class AppendCmd : public Cmd {
public:
  AppendCmd() {}
  virtual void Do();
private:
  std::string key_;
  std::string value_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class MgetCmd : public Cmd {
public:
  MgetCmd() {}
  virtual void Do();
private:
  std::vector<std::string> keys_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class KeysCmd : public Cmd {
public:
  KeysCmd() {}
  virtual void Do();
private:
  std::string pattern_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class SetnxCmd : public Cmd {
public:
  SetnxCmd() {}
  virtual void Do();
private:
  std::string key_;
  std::string value_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class SetexCmd : public Cmd {
public:
  SetexCmd() {}
  virtual void Do();
private:
  std::string key_;
  int64_t sec_;
  std::string value_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class MsetCmd : public Cmd {
public:
  MsetCmd() {}
  virtual void Do();
private:
  std::vector<nemo::KV> kvs_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class MsetnxCmd : public Cmd {
public:
  MsetnxCmd() {}
  virtual void Do();
private:
  std::vector<nemo::KV> kvs_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class GetrangeCmd : public Cmd {
public:
  GetrangeCmd() {}
  virtual void Do();
private:
  std::string key_;
  int64_t start_;
  int64_t end_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class SetrangeCmd : public Cmd {
public:
  SetrangeCmd() {}
  virtual void Do();
private:
  std::string key_;
  int64_t offset_;
  std::string value_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class StrlenCmd : public Cmd {
public:
  StrlenCmd() {}
  virtual void Do();
private:
  std::string key_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class ExistsCmd : public Cmd {
public:
  ExistsCmd() {}
  virtual void Do();
private:
  std::string key_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class ExpireCmd : public Cmd {
public:
  ExpireCmd() {}
  virtual void Do();
private:
  std::string key_;
  int64_t sec_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class PexpireCmd : public Cmd {
public:
  PexpireCmd() {}
  virtual void Do();
private:
  std::string key_;
  int64_t msec_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class ExpireatCmd : public Cmd {
public:
  ExpireatCmd() {}
  virtual void Do();
private:
  std::string key_;
  int64_t time_stamp_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class PexpireatCmd : public Cmd {
public:
    PexpireatCmd() {}
    virtual void Do();
private:
  std::string key_;
  int64_t time_stamp_ms_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class TtlCmd : public Cmd {
public:
    TtlCmd() {}
    virtual void Do();
private:
  std::string key_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class PttlCmd : public Cmd {
public:
    PttlCmd() {}
    virtual void Do();
private:
  std::string key_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class PersistCmd : public Cmd {
public:
    PersistCmd() {}
    virtual void Do();
private:
  std::string key_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class TypeCmd : public Cmd {
public:
    TypeCmd() {}
    virtual void Do();
private:
  std::string key_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class ScanCmd : public Cmd {
public:
  ScanCmd() : pattern_("*"), count_(10) {}
  virtual void Do();
private:
  int64_t cursor_;
  std::string pattern_;
  int64_t count_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
  virtual void Clear() {
    pattern_ = "*";
    count_ = 10;
  }
};
#endif
