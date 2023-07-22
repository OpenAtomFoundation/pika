
#ifndef PIKA_STREAM_H_
#define PIKA_STREAM_H_

#include <cstdint>
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
  static CmdRes GenerateStreamID(const StreamMetaValue &stream_meta, const StreamAddTrimArgs &args_, streamID &id) ;
};

class XReadCmd : public Cmd {
 public:
  XReadCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag){};
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new XReadCmd(*this); }

 private:
  std::vector<std::string> keys_;
  std::vector<streamID> ids_;
  uint64_t count_{0};
  uint64_t block_{0};  // 0 means no block

  void DoInitial() override;
  void Clear() override {
    ids_.clear();
    keys_.clear();
  }
};

#endif
