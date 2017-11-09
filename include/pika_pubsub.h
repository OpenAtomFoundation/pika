// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_PUBSUB_H_
#define PIKA_PUBSUB_H_
#include "pika_command.h"

/*
 * pubsub
 */
class PublishCmd : public Cmd {
public:
  PublishCmd() {
  }
  virtual void Do();
private:
  std::string channel_;
  std::string msg_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class SubscribeCmd : public Cmd {
public:
  SubscribeCmd() {
  }
  virtual void Do();
private:
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class UnSubscribeCmd : public Cmd {
public:
  UnSubscribeCmd() {
  }
  virtual void Do();
private:
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class PUnSubscribeCmd : public Cmd {
public:
  PUnSubscribeCmd() {
  }
  virtual void Do();
private:
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class PSubscribeCmd : public Cmd {
public:
  PSubscribeCmd() {
  }
  virtual void Do();
private:
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class PubSubCmd : public Cmd {
public:
  PubSubCmd() {
  }
  virtual void Do();
private:
  std::string subcommand_;
  std::vector<std::string > arguments_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

#endif
