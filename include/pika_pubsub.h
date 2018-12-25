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
  PublishCmd(const std::string& name, int arity, uint16_t flag)
     : Cmd(name,  arity, flag) {}
  virtual void Do();
 private:
  std::string channel_;
  std::string msg_;
  virtual void DoInitial(const PikaCmdArgsType& argv) override;
};

class SubscribeCmd : public Cmd {
 public:
  SubscribeCmd(const std::string& name, int arity, uint16_t flag)
     : Cmd(name,  arity, flag) {}
  virtual void Do();
 private:
  virtual void DoInitial(const PikaCmdArgsType& argv) override;
};

class UnSubscribeCmd : public Cmd {
 public:
  UnSubscribeCmd(const std::string& name, int arity, uint16_t flag)
     : Cmd(name,  arity, flag) {}
  virtual void Do();
 private:
  virtual void DoInitial(const PikaCmdArgsType& argv) override;
};

class PUnSubscribeCmd : public Cmd {
 public:
  PUnSubscribeCmd(const std::string& name, int arity, uint16_t flag)
     : Cmd(name,  arity, flag) {}
  virtual void Do();
 private:
  virtual void DoInitial(const PikaCmdArgsType& argv) override;
};

class PSubscribeCmd : public Cmd {
 public:
  PSubscribeCmd(const std::string& name, int arity, uint16_t flag)
     : Cmd(name,  arity, flag) {}
  virtual void Do();
 private:
  virtual void DoInitial(const PikaCmdArgsType& argv) override;
};

class PubSubCmd : public Cmd {
 public:
  PubSubCmd(const std::string& name, int arity, uint16_t flag)
     : Cmd(name,  arity, flag) {}
  virtual void Do();
 private:
  std::string subcommand_;
  std::vector<std::string > arguments_;
  virtual void DoInitial(const PikaCmdArgsType& argv) override;
  virtual void Clear() {
    arguments_.clear();
  }
};

#endif  // INCLUDE_PIKA_PUBSUB_H_
