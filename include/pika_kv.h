// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_KV_H_
#define PIKA_KV_H_
#include "include/pika_command.h"
#include "blackwidow/blackwidow.h"


/*
 * kv
 */
class SetCmd : public Cmd {
 public:
  enum SetCondition{kANY, kNX, kXX, kVX};
  SetCmd() : sec_(0), condition_(kANY) {};
  virtual void Do() override;

 private:
  std::string key_;
  std::string value_;
  std::string target_;
  int32_t success_;
  int64_t sec_;
  SetCmd::SetCondition condition_;
  virtual void DoInitial(const PikaCmdArgsType &argvs, const CmdInfo* const ptr_info) override;
  virtual void Clear() override {
    sec_ = 0;
    success_ = 0;
    condition_ = kANY;
  }
};

class GetCmd : public Cmd {
public:
  GetCmd() {};
  virtual void Do();
private:
  std::string key_;
  virtual void DoInitial(const PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class DelCmd : public Cmd {
 public:
  DelCmd() {}
  virtual void Do();
 private:
  std::vector<std::string> keys_;
  virtual void DoInitial(const PikaCmdArgsType &argvs, const  CmdInfo* const ptr_info);
};

class IncrCmd : public Cmd {
 public:
  IncrCmd() {}
  virtual void Do();
 private:
  std::string key_;
  int64_t new_value_;
  virtual void DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
  virtual std::string ToBinlog(
      const PikaCmdArgsType& argv,
      uint32_t exec_time,
      const std::string& server_id,
      uint64_t logic_id,
      uint32_t filenum,
      uint64_t offset) override;
};

class IncrbyCmd : public Cmd {
 public:
  IncrbyCmd() {}
  virtual void Do();
 private:
  std::string key_;
  int64_t by_, new_value_;
  virtual void DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
  virtual std::string ToBinlog(
      const PikaCmdArgsType& argv,
      uint32_t exec_time,
      const std::string& server_id,
      uint64_t logic_id,
      uint32_t filenum,
      uint64_t offset) override;
};

class IncrbyfloatCmd : public Cmd {
 public:
  IncrbyfloatCmd() {}
  virtual void Do();
 private:
  std::string key_, value_, new_value_;
  double by_;
  virtual void DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
  virtual std::string ToBinlog(
      const PikaCmdArgsType& argv,
      uint32_t exec_time,
      const std::string& server_id,
      uint64_t logic_id,
      uint32_t filenum,
      uint64_t offset) override;
};

class DecrCmd : public Cmd {
 public:
  DecrCmd() {}
  virtual void Do();
 private:
  std::string key_;
  int64_t new_value_;
  virtual void DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
  virtual std::string ToBinlog(
      const PikaCmdArgsType& argv,
      uint32_t exec_time,
      const std::string& server_id,
      uint64_t logic_id,
      uint32_t filenum,
      uint64_t offset) override;
};

class DecrbyCmd : public Cmd {
 public:
  DecrbyCmd() {}
  virtual void Do();
 private:
  std::string key_;
  int64_t by_, new_value_;
  virtual void DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
  virtual std::string ToBinlog(
      const PikaCmdArgsType& argv,
      uint32_t exec_time,
      const std::string& server_id,
      uint64_t logic_id,
      uint32_t filenum,
      uint64_t offset) override;
};

class GetsetCmd : public Cmd {
public:
  GetsetCmd() {}
  virtual void Do();
private:
  std::string key_;
  std::string new_value_;
  virtual void DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class AppendCmd : public Cmd {
public:
  AppendCmd() {}
  virtual void Do();
private:
  std::string key_;
  std::string value_;
  virtual void DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class MgetCmd : public Cmd {
public:
  MgetCmd() {}
  virtual void Do();
private:
  std::vector<std::string> keys_;
  virtual void DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class KeysCmd : public Cmd {
public:
  KeysCmd() : type_("all") {}
  virtual void Do();
private:
  std::string pattern_;
  std::string type_;
  virtual void DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
  virtual void Clear() {
    type_ = "all";
  }
};

class SetnxCmd : public Cmd {
 public:
  SetnxCmd() {}
  virtual void Do();
 private:
  std::string key_;
  std::string value_;
  int32_t success_;
  virtual void DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
  virtual std::string ToBinlog(
      const PikaCmdArgsType& argv,
      uint32_t exec_time,
      const std::string& server_id,
      uint64_t logic_id,
      uint32_t filenum,
      uint64_t offset) override;
};

class SetexCmd : public Cmd {
public:
  SetexCmd() {}
  virtual void Do();
private:
  std::string key_;
  int64_t sec_;
  std::string value_;
  virtual void DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class PsetexCmd : public Cmd {
public:
  PsetexCmd() {}
  virtual void Do();
private:
  std::string key_;
  int64_t usec_;
  std::string value_;
  virtual void DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class DelvxCmd : public Cmd {
public:
  DelvxCmd() {}
  virtual void Do();
private:
  std::string key_;
  std::string value_;
  int32_t success_;
  virtual void DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class MsetCmd : public Cmd {
 public:
  MsetCmd() {}
  virtual void Do();
 private:
  std::vector<blackwidow::KeyValue> kvs_;
  virtual void DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class MsetnxCmd : public Cmd {
 public:
  MsetnxCmd() {}
  virtual void Do();
 private:
  std::vector<blackwidow::KeyValue> kvs_;
  int32_t success_;
  virtual void DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class GetrangeCmd : public Cmd {
public:
  GetrangeCmd() {}
  virtual void Do();
private:
  std::string key_;
  int64_t start_;
  int64_t end_;
  virtual void DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class SetrangeCmd : public Cmd {
public:
  SetrangeCmd() {}
  virtual void Do();
private:
  std::string key_;
  int64_t offset_;
  std::string value_;
  virtual void DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class StrlenCmd : public Cmd {
public:
  StrlenCmd() {}
  virtual void Do();
private:
  std::string key_;
  virtual void DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class ExistsCmd : public Cmd {
public:
  ExistsCmd() {}
  virtual void Do();
private:
  std::vector<std::string> keys_;
  virtual void DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class ExpireCmd : public Cmd {
 public:
  ExpireCmd() {}
  virtual void Do();

 private:
  std::string key_;
  int64_t sec_;
  virtual void DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) override;
  virtual std::string ToBinlog(
      const PikaCmdArgsType& argv,
      uint32_t exec_time,
      const std::string& server_id,
      uint64_t logic_id,
      uint32_t filenum,
      uint64_t offset) override;
};

class PexpireCmd : public Cmd {
 public:
  PexpireCmd() {}
  virtual void Do();

 private:
  std::string key_;
  int64_t msec_;
  virtual void DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) override;
  virtual std::string ToBinlog(
      const PikaCmdArgsType& argv,
      uint32_t exec_time,
      const std::string& server_id,
      uint64_t logic_id,
      uint32_t filenum,
      uint64_t offset) override;
};

class ExpireatCmd : public Cmd {
 public:
  ExpireatCmd() {}
  virtual void Do();

 private:
  std::string key_;
  int64_t time_stamp_;
  virtual void DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) override;
};

class PexpireatCmd : public Cmd {
 public:
  PexpireatCmd() {}
  virtual void Do();

 private:
  std::string key_;
  int64_t time_stamp_ms_;
  virtual void DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) override;
  virtual std::string ToBinlog(
      const PikaCmdArgsType& argv,
      uint32_t exec_time,
      const std::string& server_id,
      uint64_t logic_id,
      uint32_t filenum,
      uint64_t offset) override;
};

class TtlCmd : public Cmd {
public:
    TtlCmd() {}
    virtual void Do();
private:
  std::string key_;
  virtual void DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class PttlCmd : public Cmd {
public:
    PttlCmd() {}
    virtual void Do();
private:
  std::string key_;
  virtual void DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class PersistCmd : public Cmd {
public:
    PersistCmd() {}
    virtual void Do();
private:
  std::string key_;
  virtual void DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class TypeCmd : public Cmd {
public:
    TypeCmd() {}
    virtual void Do();
private:
  std::string key_;
  virtual void DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class ScanCmd : public Cmd {
public:
  ScanCmd() : pattern_("*"), count_(10) {}
  virtual void Do();
private:
  int64_t cursor_;
  std::string pattern_;
  int64_t count_;
  virtual void DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
  virtual void Clear() {
    pattern_ = "*";
    count_ = 10;
  }
};

class ScanxCmd : public Cmd {
public:
  ScanxCmd() : pattern_("*"), count_(10) {}
  virtual void Do();
private:
  blackwidow::DataType type_;
  std::string start_key_;
  std::string pattern_;
  int64_t count_;
  virtual void DoInitial(const PikaCmdArgsType& argv, const CmdInfo* const ptr_info);
  virtual void Clear() {
    pattern_ = "*";
    count_ = 10;
  }
};

class PKSetexAtCmd : public Cmd {
public:
  PKSetexAtCmd() : time_stamp_(0) {}
  virtual void Do();
private:
  std::string key_;
  std::string value_;
  int64_t time_stamp_;
  virtual void DoInitial(const PikaCmdArgsType& argv, const CmdInfo* const ptr_info);
  virtual void Clear() {
    time_stamp_ = 0;
  }
};

class PKScanRangeCmd : public Cmd {
public:
  PKScanRangeCmd() : pattern_("*"), limit_(10), string_with_value(false) {}
  virtual void Do();
private:
  blackwidow::DataType type_;
  std::string key_start_;
  std::string key_end_;
  std::string pattern_;
  int64_t limit_;
  bool string_with_value;
  virtual void DoInitial(const PikaCmdArgsType& argv, const CmdInfo* const ptr_info);
  virtual void Clear() {
    pattern_ = "*";
    limit_ = 10;
    string_with_value = false;
  }
};

class PKRScanRangeCmd : public Cmd {
public:
  PKRScanRangeCmd() : pattern_("*"), limit_(10), string_with_value(false) {}
  virtual void Do();
private:
  blackwidow::DataType type_;
  std::string key_start_;
  std::string key_end_;
  std::string pattern_;
  int64_t limit_;
  bool string_with_value;
  virtual void DoInitial(const PikaCmdArgsType& argv, const CmdInfo* const ptr_info);
  virtual void Clear() {
    pattern_ = "*";
    limit_ = 10;
    string_with_value = false;
  }
};
#endif
