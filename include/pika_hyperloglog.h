// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_HYPERLOGLOG_H_
#define PIKA_HYPERLOGLOG_H_

#include "include/pika_command.h"

/*
 * hyperloglog
 */
class PfAddCmd : public Cmd {
public:
  PfAddCmd() {};
  virtual void Do();
private:
  std::string key_;
  std::vector<std::string> values_;
  virtual void DoInitial(const PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
  virtual void Clear() {
    values_.clear();
  }
};

class PfCountCmd : public Cmd {
public:
  PfCountCmd() {};
  virtual void Do();
private:
  std::vector<std::string> keys_;
  virtual void DoInitial(const PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
  virtual void Clear() {
    keys_.clear();
  }
};

class PfMergeCmd : public Cmd {
public:
  PfMergeCmd() {};
  virtual void Do();
private:
  std::vector<std::string> keys_;
  virtual void DoInitial(const PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
  virtual void Clear() {
    keys_.clear();
  }
};

#endif
