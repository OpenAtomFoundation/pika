// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_LIST_H_
#define PIKA_LIST_H_
#include "include/pika_command.h"
#include "nemo.h"


/*
 * list
 */
class LIndexCmd : public Cmd {
  public:
    LIndexCmd() : index_(0) {};
    virtual void Do();
  private:
    std::string key_;
    int64_t index_;
    virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
    virtual void Clear() {
      index_ = 0;
    }
};

class LInsertCmd : public Cmd {
  public:
    LInsertCmd() : dir_(nemo::AFTER) {};
    virtual void Do();
  private:
    std::string key_;
    nemo::Position dir_;
    std::string pivot_;
    std::string value_;
    virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class LLenCmd : public Cmd {
  public:
    LLenCmd() {};
    virtual void Do();
  private:
    std::string key_;
    virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class LPopCmd : public Cmd {
  public:
    LPopCmd() {};
    virtual void Do();
  private:
    std::string key_;
    virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class LPushCmd : public Cmd {
  public:
    LPushCmd() {};
    virtual void Do();
  private:
    std::string key_;
    std::vector<std::string> values_;
    virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
    virtual void Clear() {
      values_.clear();
    }
};

class LPushxCmd : public Cmd {
  public:
    LPushxCmd() {};
    virtual void Do();
  private:
    std::string key_;
    std::string value_;
    virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class LRangeCmd : public Cmd {
  public:
    LRangeCmd() : left_(0), right_(0) {};
    virtual void Do();
  private:
    std::string key_;
    int64_t left_;
    int64_t right_;
    virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class LRemCmd : public Cmd {
  public:
    LRemCmd() : count_(0) {};
    virtual void Do();
  private:
    std::string key_;
    int64_t count_;
    std::string value_;
    virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class LSetCmd : public Cmd {
  public:
    LSetCmd() : index_(0){};
    virtual void Do();
  private:
    std::string key_;
    int64_t index_;
    std::string value_;
    virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class LTrimCmd : public Cmd {
  public:
    LTrimCmd() : start_(0), stop_(0) {};
    virtual void Do();
  private:
    std::string key_;
    int64_t start_;
    int64_t stop_;
    virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class RPopCmd : public Cmd {
  public:
    RPopCmd() {};
    virtual void Do();
  private:
    std::string key_;
    virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class RPopLPushCmd : public Cmd {
  public:
    RPopLPushCmd() {};
    virtual void Do();
  private:
    std::string source_;
    std::string receiver_;
    virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class RPushCmd : public Cmd {
  public:
    RPushCmd() {};
    virtual void Do();
  private:
    std::string key_;
    std::vector<std::string> values_;
    virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
    virtual void Clear() {
      values_.clear();
    }
};
class RPushxCmd : public Cmd {
  public:
    RPushxCmd() {};
    virtual void Do();
  private:
    std::string key_;
    std::string value_;
    virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};
#endif
