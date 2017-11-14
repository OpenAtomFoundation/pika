// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_SET_H_
#define PIKA_SET_H_
#include "include/pika_command.h"
#include "nemo.h"


/*
 * set
 */

class SAddCmd : public Cmd {
public:
  SAddCmd() {}
  virtual void Do();
private:
  std::string key_;
  std::vector<std::string> members_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class SPopCmd : public Cmd {
public:
  SPopCmd() {}
  virtual void Do();
private:
  std::string key_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class SCardCmd : public Cmd {
public:
  SCardCmd() {}
  virtual void Do();
private:
  std::string key_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class SMembersCmd : public Cmd {
public:
  SMembersCmd() {}
  virtual void Do();
private:
  std::string key_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class SScanCmd : public Cmd {
public:
  SScanCmd() : pattern_("*"), count_(10) {}
  virtual void Do();
private:
  std::string key_, pattern_;
  int64_t cursor_, count_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
  virtual void Clear() {
    pattern_ = "*";
    count_ = 10;
  }
};

class SRemCmd : public Cmd {
public:
  SRemCmd() {}
  virtual void Do();
private:
  std::string key_;
  std::vector<std::string> members_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class SUnionCmd : public Cmd {
public:
  SUnionCmd() {}
  virtual void Do();
private:
  std::vector<std::string> keys_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class SUnionstoreCmd : public Cmd {
public:
  SUnionstoreCmd() {}
  virtual void Do();
private:
  std::string dest_key_;
  std::vector<std::string> keys_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class SInterCmd : public Cmd {
public:
  SInterCmd() {}
  virtual void Do();
private:
  std::vector<std::string> keys_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class SInterstoreCmd : public Cmd {
public:
  SInterstoreCmd() {}
  virtual void Do();
private:
  std::string dest_key_;
  std::vector<std::string> keys_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class SIsmemberCmd : public Cmd {
public:
  SIsmemberCmd() {}
  virtual void Do();
private:
  std::string key_, member_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class SDiffCmd : public Cmd {
public:
  SDiffCmd() {}
  virtual void Do();
private:
  std::vector<std::string> keys_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class SDiffstoreCmd : public Cmd {
public:
  SDiffstoreCmd() {}
  virtual void Do();
private:
  std::string dest_key_;
  std::vector<std::string> keys_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class SMoveCmd : public Cmd {
public:
  SMoveCmd() {}
  virtual void Do();
private:
  std::string src_key_, dest_key_, member_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class SRandmemberCmd : public Cmd {
public:
  SRandmemberCmd() : count_(1) {}
  virtual void Do();
private:
  std::string key_;
  int64_t count_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
  virtual void Clear() {
    count_ = 1;
  }
};

#endif
