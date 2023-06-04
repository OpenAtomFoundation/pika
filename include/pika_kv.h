// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_KV_H_
#define PIKA_KV_H_

#include "storage/storage.h"

#include "include/pika_command.h"
#include "include/pika_slot.h"

/*
 * kv
 */
class SetCmd : public Cmd {
 public:
  enum SetCondition { kNONE, kNX, kXX, kVX, kEXORPX };
  SetCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag),  condition_(kNONE){};
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SetCmd(*this); }

 private:
  std::string key_;
  std::string value_;
  std::string target_;
  int32_t success_ = 0;
  int64_t sec_ = 0;
  SetCmd::SetCondition condition_;
  void DoInitial() override;
  void Clear() override {
    sec_ = 0;
    success_ = 0;
    condition_ = kNONE;
  }
  std::string ToBinlog(uint32_t exec_time, uint32_t term_id, uint64_t logic_id, uint32_t filenum,
                               uint64_t offset) override;
};

class GetCmd : public Cmd {
 public:
  GetCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag){};
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new GetCmd(*this); }

 private:
  std::string key_;
  void DoInitial() override;
};

class DelCmd : public Cmd {
 public:
  DelCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {};
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  std::vector<std::string> current_key() const override { return keys_; }
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override;
  void Merge() override;
  Cmd* Clone() override { return new DelCmd(*this); }

 private:
  std::vector<std::string> keys_;
  int64_t split_res_ = 0;
  void DoInitial() override;
};

class IncrCmd : public Cmd {
 public:
  IncrCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag){};
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new IncrCmd(*this); }

 private:
  std::string key_;
  int64_t new_value_ = 0;
  void DoInitial() override;
};

class IncrbyCmd : public Cmd {
 public:
  IncrbyCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag){};
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new IncrbyCmd(*this); }

 private:
  std::string key_;
  int64_t by_ = 0, new_value_ = 0;
  void DoInitial() override;
};

class IncrbyfloatCmd : public Cmd {
 public:
  IncrbyfloatCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag){};
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new IncrbyfloatCmd(*this); }

 private:
  std::string key_, value_, new_value_;
  double by_ = 0;
  void DoInitial() override;
};

class DecrCmd : public Cmd {
 public:
  DecrCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag){};
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new DecrCmd(*this); }

 private:
  std::string key_;
  int64_t new_value_ = 0;
  void DoInitial() override;
};

class DecrbyCmd : public Cmd {
 public:
  DecrbyCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag){};
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new DecrbyCmd(*this); }

 private:
  std::string key_;
  int64_t by_ = 0, new_value_ = 0;
  void DoInitial() override;
};

class GetsetCmd : public Cmd {
 public:
  GetsetCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag){};
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new GetsetCmd(*this); }

 private:
  std::string key_;
  std::string new_value_;
  void DoInitial() override;
};

class AppendCmd : public Cmd {
 public:
  AppendCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag){};
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new AppendCmd(*this); }

 private:
  std::string key_;
  std::string value_;
  void DoInitial() override;
};

class MgetCmd : public Cmd {
 public:
  MgetCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag){};
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  std::vector<std::string> current_key() const override { return keys_; }
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override;
  void Merge() override;
  Cmd* Clone() override { return new MgetCmd(*this); }

 private:
  std::vector<std::string> keys_;
  std::vector<storage::ValueStatus> split_res_;
  void DoInitial() override;
};

class KeysCmd : public Cmd {
 public:
  KeysCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag), type_(storage::DataType::kAll) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new KeysCmd(*this); }

 private:
  std::string pattern_;
  storage::DataType type_;
  void DoInitial() override;
  void Clear() override { type_ = storage::DataType::kAll; }
};

class SetnxCmd : public Cmd {
 public:
  SetnxCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SetnxCmd(*this); }

 private:
  std::string key_;
  std::string value_;
  int32_t success_ = 0;
  void DoInitial() override;
  std::string ToBinlog(uint32_t exec_time, uint32_t term_id, uint64_t logic_id, uint32_t filenum,
                               uint64_t offset) override;
};

class SetexCmd : public Cmd {
 public:
  SetexCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SetexCmd(*this); }

 private:
  std::string key_;
  int64_t sec_ = 0;
  std::string value_;
  void DoInitial() override;
  std::string ToBinlog(uint32_t exec_time, uint32_t term_id, uint64_t logic_id, uint32_t filenum,
                               uint64_t offset) override;
};

class PsetexCmd : public Cmd {
 public:
  PsetexCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new PsetexCmd(*this); }

 private:
  std::string key_;
  int64_t usec_ = 0;
  std::string value_;
  void DoInitial() override;
  std::string ToBinlog(uint32_t exec_time, uint32_t term_id, uint64_t logic_id, uint32_t filenum,
                               uint64_t offset) override;
};

class DelvxCmd : public Cmd {
 public:
  DelvxCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new DelvxCmd(*this); }

 private:
  std::string key_;
  std::string value_;
  int32_t success_ = 0;
  void DoInitial() override;
};

class MsetCmd : public Cmd {
 public:
  MsetCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    for (auto& kv : kvs_) {
      res.push_back(kv.key);
    }
    return res;
  }
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override;
  void Merge() override;
  Cmd* Clone() override { return new MsetCmd(*this); }

 private:
  std::vector<storage::KeyValue> kvs_;
  void DoInitial() override;
};

class MsetnxCmd : public Cmd {
 public:
  MsetnxCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new MsetnxCmd(*this); }

 private:
  std::vector<storage::KeyValue> kvs_;
  int32_t success_ = 0;
  void DoInitial() override;
};

class GetrangeCmd : public Cmd {
 public:
  GetrangeCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new GetrangeCmd(*this); }

 private:
  std::string key_;
  int64_t start_ = 0;
  int64_t end_ = 0;
  void DoInitial() override;
};

class SetrangeCmd : public Cmd {
 public:
  SetrangeCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SetrangeCmd(*this); }

 private:
  std::string key_;
  int64_t offset_ = 0;
  std::string value_;
  void DoInitial() override;
};

class StrlenCmd : public Cmd {
 public:
  StrlenCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new StrlenCmd(*this); }

 private:
  std::string key_;
  void DoInitial() override;
};

class ExistsCmd : public Cmd {
 public:
  ExistsCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  std::vector<std::string> current_key() const override { return keys_; }
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override;
  void Merge() override;
  Cmd* Clone() override { return new ExistsCmd(*this); }

 private:
  std::vector<std::string> keys_;
  int64_t split_res_ = 0;
  void DoInitial() override;
};

class ExpireCmd : public Cmd {
 public:
  ExpireCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new ExpireCmd(*this); }

 private:
  std::string key_;
  int64_t sec_ = 0;
  void DoInitial() override;
  std::string ToBinlog(uint32_t exec_time, uint32_t term_id, uint64_t logic_id, uint32_t filenum,
                               uint64_t offset) override;
};

class PexpireCmd : public Cmd {
 public:
  PexpireCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new PexpireCmd(*this); }

 private:
  std::string key_;
  int64_t msec_ = 0;
  void DoInitial() override;
  std::string ToBinlog(uint32_t exec_time, uint32_t term_id, uint64_t logic_id, uint32_t filenum,
                               uint64_t offset) override;
};

class ExpireatCmd : public Cmd {
 public:
  ExpireatCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new ExpireatCmd(*this); }

 private:
  std::string key_;
  int64_t time_stamp_ = 0;
  void DoInitial() override;
};

class PexpireatCmd : public Cmd {
 public:
  PexpireatCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new PexpireatCmd(*this); }

 private:
  std::string key_;
  int64_t time_stamp_ms_ = 0;
  void DoInitial() override;
  std::string ToBinlog(uint32_t exec_time, uint32_t term_id, uint64_t logic_id, uint32_t filenum,
                               uint64_t offset) override;
};

class TtlCmd : public Cmd {
 public:
  TtlCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new TtlCmd(*this); }

 private:
  std::string key_;
  void DoInitial() override;
};

class PttlCmd : public Cmd {
 public:
  PttlCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new PttlCmd(*this); }

 private:
  std::string key_;
  void DoInitial() override;
};

class PersistCmd : public Cmd {
 public:
  PersistCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new PersistCmd(*this); }

 private:
  std::string key_;
  void DoInitial() override;
};

class TypeCmd : public Cmd {
 public:
  TypeCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new TypeCmd(*this); }

 private:
  std::string key_;
  void DoInitial() override;
};

class PTypeCmd : public Cmd {
 public:
  PTypeCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new PTypeCmd(*this); }

 private:
  std::string key_;
  void DoInitial() override;
};

class ScanCmd : public Cmd {
 public:
  ScanCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag), pattern_("*") {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new ScanCmd(*this); }

 private:
  int64_t cursor_ = 0;
  std::string pattern_ = "*";
  int64_t count_ = 10;
  void DoInitial() override;
  void Clear() override {
    pattern_ = "*";
    count_ = 10;
  }
};

class ScanxCmd : public Cmd {
 public:
  ScanxCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag), pattern_("*") {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new ScanxCmd(*this); }

 private:
  storage::DataType type_;
  std::string start_key_;
  std::string pattern_ = "*";
  int64_t count_ = 10;
  void DoInitial() override;
  void Clear() override {
    pattern_ = "*";
    count_ = 10;
  }
};

class PKSetexAtCmd : public Cmd {
 public:
  PKSetexAtCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new PKSetexAtCmd(*this); }

 private:
  std::string key_;
  std::string value_;
  int64_t time_stamp_ = 0;
  void DoInitial() override;
  void Clear() override { time_stamp_ = 0; }
};

class PKScanRangeCmd : public Cmd {
 public:
  PKScanRangeCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag), pattern_("*") {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_start_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new PKScanRangeCmd(*this); }

 private:
  storage::DataType type_;
  std::string key_start_;
  std::string key_end_;
  std::string pattern_ = "*";
  int64_t limit_ = 10;
  bool string_with_value = false;
  void DoInitial() override;
  void Clear() override {
    pattern_ = "*";
    limit_ = 10;
    string_with_value = false;
  }
};

class PKRScanRangeCmd : public Cmd {
 public:
  PKRScanRangeCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag), pattern_("*") {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_start_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new PKRScanRangeCmd(*this); }

 private:
  storage::DataType type_ = storage::kAll;
  std::string key_start_;
  std::string key_end_;
  std::string pattern_ = "*";
  int64_t limit_ = 10;
  bool string_with_value = false;
  void DoInitial() override;
  void Clear() override {
    pattern_ = "*";
    limit_ = 10;
    string_with_value = false;
  }
};
#endif
