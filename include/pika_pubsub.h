// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_PUBSUB_H_
#define PIKA_PUBSUB_H_

#include "acl.h"
#include "pika_command.h"

/*
 * pubsub
 */
class PublishCmd : public Cmd {
 public:
  PublishCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::PUBSUB)) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new PublishCmd(*this); }
  std::vector<std::string> current_key() const override { return {channel_}; }

 private:
  std::string channel_;
  std::string msg_;
  void DoInitial() override;
};

class SubscribeCmd : public Cmd {
 public:
  SubscribeCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::PUBSUB)) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new SubscribeCmd(*this); }
  std::vector<std::string> current_key() const override { return channels_; }

 private:
  std::vector<std::string> channels_;
  void DoInitial() override;
};

class UnSubscribeCmd : public Cmd {
 public:
  UnSubscribeCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::PUBSUB)) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new UnSubscribeCmd(*this); }
  std::vector<std::string> current_key() const override { return channels_; }

 private:
  std::vector<std::string> channels_;
  void DoInitial() override;
};

class PUnSubscribeCmd : public Cmd {
 public:
  PUnSubscribeCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::PUBSUB)) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new PUnSubscribeCmd(*this); }
  std::vector<std::string> current_key() const override { return {channels_}; }

 private:
  std::vector<std::string> channels_;
  void DoInitial() override;
};

class PSubscribeCmd : public Cmd {
 public:
  PSubscribeCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::PUBSUB)) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new PSubscribeCmd(*this); }
  std::vector<std::string> current_key() const override { return {channels_}; }

  std::vector<std::string> channels_;
 private:
  void DoInitial() override;
};

class PubSubCmd : public Cmd {
 public:
  PubSubCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::PUBSUB)) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new PubSubCmd(*this); }

 private:
  std::string subcommand_;
  std::vector<std::string> arguments_;
  void DoInitial() override;
  void Clear() override { arguments_.clear(); }
};

#endif  // INCLUDE_PIKA_PUBSUB_H_
