// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_HASH_H_
#define PIKA_HASH_H_
#include "include/pika_command.h"
#include "blackwidow/blackwidow.h"


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

class HGetallCmd : public Cmd {
public:
  HGetallCmd() {}
  virtual void Do();
private:
  std::string key_;
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

class HExistsCmd : public Cmd {
public:
  HExistsCmd() {}
  virtual void Do();
private:
  std::string key_, field_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class HIncrbyCmd : public Cmd {
public:
  HIncrbyCmd() {}
  virtual void Do();
private:
  std::string key_, field_;
  int64_t by_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class HIncrbyfloatCmd : public Cmd {
public:
  HIncrbyfloatCmd() {}
  virtual void Do();
private:
  std::string key_, field_, by_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class HKeysCmd : public Cmd {
public:
  HKeysCmd() {}
  virtual void Do();
private:
  std::string key_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class HLenCmd : public Cmd {
public:
  HLenCmd() {}
  virtual void Do();
private:
  std::string key_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class HMgetCmd : public Cmd {
public:
  HMgetCmd() {}
  virtual void Do();
private:
  std::string key_;
  std::vector<std::string> fields_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class HMsetCmd : public Cmd {
public:
  HMsetCmd() {}
  virtual void Do();
private:
  std::string key_;
  std::vector<blackwidow::FieldValue> fvs_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class HSetnxCmd : public Cmd {
public:
  HSetnxCmd() {}
  virtual void Do();
private:
  std::string key_, field_, value_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class HStrlenCmd : public Cmd {
public:
  HStrlenCmd() {}
  virtual void Do();
private:
  std::string key_, field_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class HValsCmd : public Cmd {
public:
  HValsCmd() {}
  virtual void Do();
private:
  std::string key_, field_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class HScanCmd : public Cmd {
public:
  HScanCmd() : pattern_("*"), count_(10) {}
  virtual void Do();
private:
  std::string key_, pattern_;
  int64_t cursor_, count_;
  bool use_pattern_, use_count_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
  virtual void Clear() {
    pattern_ = "*";
    count_ = 10;
  }
};
#endif
