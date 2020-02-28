// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_ADMIN_H_
#define PIKA_ADMIN_H_

#include <sstream>
#include <sys/time.h>
#include <sys/resource.h>
#include <iomanip>

#include "blackwidow/blackwidow.h"

#include "include/pika_command.h"

/*
 * Admin
 */
class SlaveofCmd : public Cmd {
 public:
  SlaveofCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag), is_noone_(false) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new SlaveofCmd(*this);
  }

 private:
  std::string master_ip_;
  int64_t master_port_;
  bool is_noone_;
  virtual void DoInitial() override;
  virtual void Clear() {
    is_noone_ = false;
    master_ip_.clear();
    master_port_ = 0;
  }
};

class DbSlaveofCmd : public Cmd {
 public:
  DbSlaveofCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new DbSlaveofCmd(*this);
  }

 private:
  std::string db_name_;
  bool force_sync_;
  bool is_noone_;
  bool have_offset_;
  int64_t filenum_;
  int64_t offset_;
  virtual void DoInitial() override;
  virtual void Clear() {
    db_name_.clear();
    force_sync_ = false;
    is_noone_ = false;
    have_offset_ = false;
  }
};

class AuthCmd : public Cmd {
 public:
  AuthCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new AuthCmd(*this);
  }

 private:
  std::string pwd_;
  virtual void DoInitial() override;
};

class BgsaveCmd : public Cmd {
 public:
  BgsaveCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new BgsaveCmd(*this);
  }

 private:
  virtual void DoInitial() override;
  virtual void Clear() {
    bgsave_tables_.clear();
  }
  std::set<std::string> bgsave_tables_;
};

class CompactCmd : public Cmd {
 public:
  CompactCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new CompactCmd(*this);
  }

 private:
  virtual void DoInitial() override;
  virtual void Clear() {
    struct_type_.clear();
    compact_tables_.clear();
  }
  std::string struct_type_;
  std::set<std::string> compact_tables_;
};

class PurgelogstoCmd : public Cmd {
 public:
  PurgelogstoCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag), num_(0) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new PurgelogstoCmd(*this);
  }

 private:
  uint32_t num_;
  std::string table_;
  virtual void DoInitial() override;
};

class PingCmd : public Cmd {
 public:
  PingCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new PingCmd(*this);
  }

 private:
  virtual void DoInitial() override;
};

class SelectCmd : public Cmd {
 public:
  SelectCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new SelectCmd(*this);
  }

 private:
  virtual void DoInitial() override;
  virtual void Clear() {
    table_name_.clear();
  }
  std::string table_name_;
};

class FlushallCmd : public Cmd {
 public:
  FlushallCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new FlushallCmd(*this);
  }

 private:
  virtual void DoInitial() override;
  virtual std::string ToBinlog(
      uint32_t exec_time,
      uint32_t term_id,
      uint64_t logic_id,
      uint32_t filenum,
      uint64_t offset) override;
};

class FlushdbCmd : public Cmd {
 public:
  FlushdbCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new FlushdbCmd(*this);
  }

 private:
  std::string db_name_;
  virtual void DoInitial() override;
  virtual void Clear() {
    db_name_.clear();
  }
};

class ClientCmd : public Cmd {
 public:
  ClientCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  const static std::string CLIENT_LIST_S;
  const static std::string CLIENT_KILL_S;
  virtual Cmd* Clone() override {
    return new ClientCmd(*this);
  }

 private:
  std::string operation_, info_;
  virtual void DoInitial() override;
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
    kInfo,
    kInfoAll,
    kInfoDebug
  };

  InfoCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag), rescan_(false), off_(false) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new InfoCmd(*this);
  }

 private:
  InfoSection info_section_;
  bool rescan_; //whether to rescan the keyspace
  bool off_;
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
  const static std::string kDebugSection;

  virtual void DoInitial() override;
  virtual void Clear() {
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
  void InfoDebug(std::string& info);
};

class ShutdownCmd : public Cmd {
 public:
  ShutdownCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new ShutdownCmd(*this);
  }

 private:
  virtual void DoInitial() override;
};

class ConfigCmd : public Cmd {
 public:
  ConfigCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new ConfigCmd(*this);
  }

 private:
  std::vector<std::string> config_args_v_;
  virtual void DoInitial() override;
  void ConfigGet(std::string &ret);
  void ConfigSet(std::string &ret);
  void ConfigRewrite(std::string &ret);
  void ConfigResetstat(std::string &ret);
};

class MonitorCmd : public Cmd {
 public:
  MonitorCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new MonitorCmd(*this);
  }

 private:
  virtual void DoInitial() override;
};

class DbsizeCmd : public Cmd {
 public:
  DbsizeCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new DbsizeCmd(*this);
  }

 private:
  virtual void DoInitial() override;
};

class TimeCmd : public Cmd {
 public:
  TimeCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new TimeCmd(*this);
  }

 private:
  virtual void DoInitial() override;
};

class DelbackupCmd : public Cmd {
 public:
  DelbackupCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new DelbackupCmd(*this);
  }

 private:
  virtual void DoInitial() override;
};

class EchoCmd : public Cmd {
 public:
  EchoCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new EchoCmd(*this);
  }

 private:
  std::string body_;
  virtual void DoInitial() override;
};

class ScandbCmd : public Cmd {
 public:
  ScandbCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag), type_(blackwidow::kAll) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new ScandbCmd(*this);
  }

 private:
  blackwidow::DataType type_;
  virtual void DoInitial() override;
  virtual void Clear() {
    type_ = blackwidow::kAll;
  }
};

class SlowlogCmd : public Cmd {
 public:
  enum SlowlogCondition{kGET, kLEN, kRESET};
  SlowlogCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag), condition_(kGET) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new SlowlogCmd(*this);
  }
 private:
  int64_t number_;
  SlowlogCmd::SlowlogCondition condition_;
  virtual void DoInitial() override;
  virtual void Clear() {
    number_ = 10;
    condition_ = kGET;
  }
};

class PaddingCmd : public Cmd {
 public:
  PaddingCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new PaddingCmd(*this);
  }

 private:
  virtual void DoInitial();
  virtual std::string ToBinlog(
      uint32_t exec_time,
      uint32_t term_id,
      uint64_t logic_id,
      uint32_t filenum,
      uint64_t offset) override;
};

#ifdef TCMALLOC_EXTENSION
class TcmallocCmd : public Cmd {
 public:
  TcmallocCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new TcmallocCmd(*this);
  }

 private:
  int64_t type_;
  int64_t rate_;
  virtual void DoInitial() override;
};
#endif

class PKPatternMatchDelCmd : public Cmd {
 public: 
  PKPatternMatchDelCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new PKPatternMatchDelCmd(*this);
  } 

 private:
  blackwidow::DataType type_;
  std::string pattern_; 
  virtual void DoInitial() override;
};

class DummyCmd : public Cmd {
 public:
  DummyCmd() : Cmd("", 0, 0) {}
  DummyCmd(const std::string& name, int arity, uint16_t flag)
     : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new DummyCmd(*this);
  }

 private:
  virtual void DoInitial() override;
};

#endif  // PIKA_ADMIN_H_
