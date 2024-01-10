// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_STREAM_H_
#define PIKA_STREAM_H_

#include "include/pika_command.h"
#include "include/pika_slot.h"
#include "include/pika_stream_base.h"
#include "include/pika_stream_meta_value.h"
#include "include/pika_stream_types.h"
#include "storage/storage.h"

/*
 * stream
 */

inline void ParseAddOrTrimArgsOrReply(CmdRes& res, const PikaCmdArgsType& argv, StreamAddTrimArgs& args, int* idpos,
                                      bool is_xadd);

inline void ParseReadOrReadGroupArgsOrReply(CmdRes& res, const PikaCmdArgsType& argv, StreamReadGroupReadArgs& args,
                                            bool is_xreadgroup);

// @field_values is the result of ScanStream.
// field is the serialized message id,
// value is the serialized message.
inline void AppendMessagesToRes(CmdRes& res, std::vector<storage::FieldValue>& field_values, const Slot* slot);

class XAddCmd : public Cmd {
 public:
  XAddCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::STREAM)){};
  std::vector<std::string> current_key() const override { return {key_}; }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new XAddCmd(*this); }

 private:
  std::string key_;
  StreamAddTrimArgs args_;
  int field_pos_{0};

  void DoInitial() override;
  inline void GenerateStreamIDOrReply(const StreamMetaValue& stream_meta);
};

class XDelCmd : public Cmd {
 public:
  XDelCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::STREAM)){};
  std::vector<std::string> current_key() const override { return {key_}; }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new XDelCmd(*this); }

 private:
  std::string key_;
  std::vector<streamID> ids_;

  void DoInitial() override;
  void Clear() override { ids_.clear(); }
  inline void SetFirstOrLastIDOrReply(StreamMetaValue& stream_meta, const Slot* slot, bool is_set_first);
  inline void SetFirstIDOrReply(StreamMetaValue& stream_meta, const Slot* slot);
  inline void SetLastIDOrReply(StreamMetaValue& stream_meta, const Slot* slot);
};

class XReadCmd : public Cmd {
 public:
  XReadCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::STREAM)){};
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new XReadCmd(*this); }

 private:
  StreamReadGroupReadArgs args_;

  void DoInitial() override;
  void Clear() override {
    args_.unparsed_ids.clear();
    args_.keys.clear();
  }
};

class XRangeCmd : public Cmd {
 public:
  XRangeCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::STREAM)){};
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new XRangeCmd(*this); }

 protected:
  std::string key_;
  streamID start_sid;
  streamID end_sid;
  int32_t count_{INT32_MAX};
  bool start_ex_{false};
  bool end_ex_{false};

  void DoInitial() override;
};

class XRevrangeCmd : public XRangeCmd {
 public:
  XRevrangeCmd(const std::string& name, int arity, uint32_t flag) : XRangeCmd(name, arity, flag){};
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new XRevrangeCmd(*this); }
};

class XLenCmd : public Cmd {
 public:
  XLenCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::STREAM)){};
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new XLenCmd(*this); }

 private:
  std::string key_;

  void DoInitial() override;
};

class XTrimCmd : public Cmd {
 public:
  XTrimCmd(const std::string& name, int arity, uint32_t flag) : Cmd(name, arity, flag){};
  std::vector<std::string> current_key() const override { return {key_}; }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new XTrimCmd(*this); }

 private:
  std::string key_;
  StreamAddTrimArgs args_;

  void DoInitial() override;
};

class XInfoCmd : public Cmd {
 public:
  XInfoCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::STREAM)){};
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new XInfoCmd(*this); }

 private:
  std::string key_;
  std::string cgroupname_;
  std::string consumername_;
  std::string subcmd_;
  uint64_t count_{0};
  bool is_full_{false};

  void DoInitial() override;
  void StreamInfo(std::shared_ptr<Slot>& slot);
  void GroupsInfo(std::shared_ptr<Slot>& slot);
  void ConsumersInfo(std::shared_ptr<Slot>& slot);
};

#endif  //  PIKA_STREAM_H_
