// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_ADMIN_H_
#define PIKA_ADMIN_H_

#include <sys/resource.h>
#include <sys/time.h>
#include <iomanip>
#include <sstream>

#include "storage/storage.h"

#include "include/pika_command.h"

/*
 * Admin
 */
class SlaveofCmd : public Cmd {
 public:
  SlaveofCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Partition> partition = nullptr) override;
  void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlaveofCmd(*this); }

 private:
  std::string master_ip_;
  int64_t master_port_ = -1;
  bool is_none_ = false;
  void DoInitial() override;
  void Clear() override {
    is_none_ = false;
    master_ip_.clear();
    master_port_ = 0;
  }
};

class DbSlaveofCmd : public Cmd {
 public:
  DbSlaveofCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Partition> partition = nullptr) override;
  void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new DbSlaveofCmd(*this); }

 private:
  std::string db_name_;
  bool force_sync_ = false;
  bool is_none_ = false;
  bool have_offset_ = false;
  int64_t filenum_ = 0;
  int64_t offset_ = 0;
  void DoInitial() override;
  void Clear() override {
    db_name_.clear();
    force_sync_ = false;
    is_none_ = false;
    have_offset_ = false;
  }
};

class AuthCmd : public Cmd {
 public:
  AuthCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Partition> partition = nullptr) override;
  void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new AuthCmd(*this); }

 private:
  std::string pwd_;
  void DoInitial() override;
};

class BgsaveCmd : public Cmd {
 public:
  BgsaveCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Partition> partition = nullptr) override;
  void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new BgsaveCmd(*this); }

 private:
  void DoInitial() override;
  void Clear() override { bgsave_tables_.clear(); }
  std::set<std::string> bgsave_tables_;
};

class CompactCmd : public Cmd {
 public:
  CompactCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Partition> partition = nullptr) override;
  void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new CompactCmd(*this); }

 private:
  void DoInitial() override;
  void Clear() override {
    struct_type_.clear();
    compact_tables_.clear();
  }
  std::string struct_type_;
  std::set<std::string> compact_tables_;
};

class PurgelogstoCmd : public Cmd {
 public:
  PurgelogstoCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Partition> partition = nullptr) override;
  void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new PurgelogstoCmd(*this); }

 private:
  uint32_t num_ = 0;
  std::string table_;
  void DoInitial() override;
};

class PingCmd : public Cmd {
 public:
  PingCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Partition> partition = nullptr) override;
  void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new PingCmd(*this); }

 private:
  void DoInitial() override;
};

class SelectCmd : public Cmd {
 public:
  SelectCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Partition> partition = nullptr) override;
  void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SelectCmd(*this); }

 private:
  void DoInitial() override;
  void Clear() override { table_name_.clear(); }
  std::string table_name_;
};

class FlushallCmd : public Cmd {
 public:
  FlushallCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Partition> partition = nullptr) override;
  void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new FlushallCmd(*this); }

 private:
  void DoInitial() override;
  std::string ToBinlog(uint32_t exec_time, uint32_t term_id, uint64_t logic_id, uint32_t filenum,
                               uint64_t offset) override;
};

class FlushdbCmd : public Cmd {
 public:
  FlushdbCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Partition> partition = nullptr) override;
  void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new FlushdbCmd(*this); }

 private:
  std::string db_name_;
  void DoInitial() override;
  void Clear() override { db_name_.clear(); }
};

class ClientCmd : public Cmd {
 public:
  ClientCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Partition> partition = nullptr) override;
  const static std::string CLIENT_LIST_S;
  const static std::string CLIENT_KILL_S;
  void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new ClientCmd(*this); }

 private:
  std::string operation_, info_;
  void DoInitial() override;
};

class InfoCmd : public Cmd {
 public:
  enum InfoSection {
    kInfoErr = 0x0,
    kInfoServer,
    kInfoClients,
    kInfoStats,
    kInfoExecCount,
    kInfoCPU,
    kInfoReplication,
    kInfoKeyspace,
    kInfoLog,
    kInfoData,
    kInfoRocksDB,
    kInfo,
    kInfoAll,
    kInfoDebug
  };

  InfoCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Partition> partition = nullptr) override;
  void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new InfoCmd(*this); }

 private:
  InfoSection info_section_;
  bool rescan_ = false;  // whether to rescan the keyspace
  bool off_ = false;
  std::set<std::string> keyspace_scan_tables_;

  const static std::string kInfoSection;
  const static std::string kAllSection;
  const static std::string kServerSection;
  const static std::string kClientsSection;
  const static std::string kStatsSection;
  const static std::string kExecCountSection;
  const static std::string kCPUSection;
  const static std::string kReplicationSection;
  const static std::string kKeyspaceSection;
  const static std::string kDataSection;
  const static std::string kRocksDBSection;
  const static std::string kDebugSection;

  void DoInitial() override;
  void Clear() override {
    rescan_ = false;
    off_ = false;
    keyspace_scan_tables_.clear();
  }

  void InfoServer(std::string& info);
  void InfoClients(std::string& info);
  void InfoStats(std::string& info);
  void InfoExecCount(std::string& info);
  void InfoCPU(std::string& info);
  void InfoShardingReplication(std::string& info);
  void InfoReplication(std::string& info);
  void InfoKeyspace(std::string& info);
  void InfoData(std::string& info);
  void InfoRocksDB(std::string& info);
  void InfoDebug(std::string& info);
};

class ShutdownCmd : public Cmd {
 public:
  ShutdownCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Partition> partition = nullptr) override;
  void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new ShutdownCmd(*this); }

 private:
  void DoInitial() override;
};

class ConfigCmd : public Cmd {
 public:
  ConfigCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Partition> partition = nullptr) override;
  void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new ConfigCmd(*this); }

 private:
  std::vector<std::string> config_args_v_;
  void DoInitial() override;
  void ConfigGet(std::string& ret);
  void ConfigSet(std::string& ret);
  void ConfigRewrite(std::string& ret);
  void ConfigResetstat(std::string& ret);
};

class MonitorCmd : public Cmd {
 public:
  MonitorCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Partition> partition = nullptr) override;
  void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new MonitorCmd(*this); }

 private:
  void DoInitial() override;
};

class DbsizeCmd : public Cmd {
 public:
  DbsizeCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Partition> partition = nullptr) override;
  void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new DbsizeCmd(*this); }

 private:
  void DoInitial() override;
};

class TimeCmd : public Cmd {
 public:
  TimeCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Partition> partition = nullptr) override;
  void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new TimeCmd(*this); }

 private:
  void DoInitial() override;
};

class DelbackupCmd : public Cmd {
 public:
  DelbackupCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Partition> partition = nullptr) override;
  void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new DelbackupCmd(*this); }

 private:
  void DoInitial() override;
};

class EchoCmd : public Cmd {
 public:
  EchoCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Partition> partition = nullptr) override;
  void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new EchoCmd(*this); }

 private:
  std::string body_;
  void DoInitial() override;
};

class ScandbCmd : public Cmd {
 public:
  ScandbCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Partition> partition = nullptr) override;
  void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new ScandbCmd(*this); }

 private:
  storage::DataType type_ = storage::kAll;
  void DoInitial() override;
  void Clear() override { type_ = storage::kAll; }
};

class SlowlogCmd : public Cmd {
 public:
  enum SlowlogCondition { kGET, kLEN, kRESET };
  SlowlogCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Partition> partition = nullptr) override;
  void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlowlogCmd(*this); }

 private:
  int64_t number_ = 10;
  SlowlogCmd::SlowlogCondition condition_ = kGET;
  void DoInitial() override;
  void Clear() override {
    number_ = 10;
    condition_ = kGET;
  }
};

class PaddingCmd : public Cmd {
 public:
  PaddingCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Partition> partition = nullptr) override;
  void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new PaddingCmd(*this); }

 private:
  void DoInitial() override;
  std::string ToBinlog(uint32_t exec_time, uint32_t term_id, uint64_t logic_id, uint32_t filenum,
                               uint64_t offset) override;
};

class PKPatternMatchDelCmd : public Cmd {
 public:
  PKPatternMatchDelCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Partition> partition = nullptr) override;
  void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new PKPatternMatchDelCmd(*this); }

 private:
  storage::DataType type_ = storage::kAll;
  std::string pattern_;
  void DoInitial() override;
};

class DummyCmd : public Cmd {
 public:
  DummyCmd() : Cmd("", 0, 0) {}
  DummyCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Partition> partition = nullptr) override;
  void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new DummyCmd(*this); }

 private:
  void DoInitial() override;
};

class QuitCmd : public Cmd {
 public:
  QuitCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Partition> partition = nullptr) override;
  void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new QuitCmd(*this); }

 private:
  void DoInitial() override;
};

class HelloCmd : public Cmd {
 public:
  HelloCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Partition> partition = nullptr) override;
  void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new HelloCmd(*this); }

 private:
  void DoInitial() override;
};

#endif  // PIKA_ADMIN_H_
