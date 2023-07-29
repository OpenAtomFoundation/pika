
#ifndef PIKA_STREAM_H_
#define PIKA_STREAM_H_

#include <cstdint>
#include <string>
#include <vector>
#include "gflags/gflags_declare.h"
#include "include/pika_command.h"
#include "include/pika_slot.h"
#include "include/pika_stream_meta_value.h"
#include "include/pika_stream_types.h"
#include "storage/storage.h"

/*
 * stream
 */
class XAddCmd : public Cmd {
 public:
  XAddCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag){};
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new XAddCmd(*this); }

 private:
  std::string key_;
  std::vector<std::pair<std::string, std::string>> filed_values_;
  StreamAddTrimArgs args_;
  int field_pos_{0};

  void DoInitial() override;
  void Clear() override { filed_values_.clear(); }
  static CmdRes GenerateStreamID(const StreamMetaValue& stream_meta, const StreamAddTrimArgs& args_, streamID& id);
};

class XReadCmd : public Cmd {
 public:
  XReadCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag){};
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

class XReadGroupCmd : public Cmd {
 public:
  XReadGroupCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag){};
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new XReadGroupCmd(*this); }

 private:
  StreamReadGroupReadArgs args_;

  void DoInitial() override;
  void Clear() override {
    args_.unparsed_ids.clear();
    args_.keys.clear();
  }
};

class XGROUP : public Cmd {
 public:
  XGROUP(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag){};
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new XGROUP(*this); }

 private:
  // create a consumer group, initialize the pel and consumers
  void Create(const std::shared_ptr<Slot>& slot = nullptr);
  void CreateConsumer(const std::shared_ptr<Slot>& slot = nullptr);

 private:
  // XGROUP common options
  std::string opt_;
  std::string key_;
  std::string group_name_;
  streamID sid_;

  // CREATE and SETID options
  uint64_t entries_read_{0};
  bool id_given_{false};

  // CREATE options
  bool mkstream_{false};

  // CREATECONSUMER and DELCONSUMER options
  std::string consumer_name_;

  void DoInitial() override;
  // void Clear() override { .clear(); }
  static CmdRes GenerateStreamID(const StreamMetaValue& stream_meta, const StreamAddTrimArgs& args_, streamID& id);
};

class XRangeCmd : public Cmd {
 public:
  XRangeCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag){};
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new XRangeCmd(*this); }

 private:
  std::string key_;
  streamID start_sid;
  streamID end_sid;
  int32_t count_{INT32_MAX};
  bool start_ex_{false};
  bool end_ex_{false};

  void DoInitial() override;
};

#endif
