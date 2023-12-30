// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_ADMIN_H_
#define PIKA_ADMIN_H_

#include <sys/resource.h>
#include <sys/time.h>
#include <iomanip>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "include/acl.h"
#include "include/pika_command.h"
#include "storage/storage.h"
#include "pika_db.h"

/*
 * Admin
 */
class SlaveofCmd : public Cmd {
 public:
  SlaveofCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::ADMIN)) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
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
  DbSlaveofCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::ADMIN)) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
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
  AuthCmd(const std::string& name, int arity, uint32_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new AuthCmd(*this); }

 private:
  void DoInitial() override;
};

class BgsaveCmd : public Cmd {
 public:
  BgsaveCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::ADMIN)) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new BgsaveCmd(*this); }

 private:
  void DoInitial() override;
  void Clear() override { bgsave_dbs_.clear(); }
  std::set<std::string> bgsave_dbs_;
};

class CompactCmd : public Cmd {
 public:
  CompactCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::ADMIN)) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new CompactCmd(*this); }

 private:
  void DoInitial() override;
  void Clear() override {
    struct_type_.clear();
    compact_dbs_.clear();
  }
  std::string struct_type_;
  std::set<std::string> compact_dbs_;
};

// we can use pika/tests/helpers/test_queue.py to test this command
class CompactRangeCmd : public Cmd {
 public:
  CompactRangeCmd(const std::string& name, int arity, uint32_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new CompactRangeCmd(*this); }

 private:
  void DoInitial() override;
  void Clear() override {
    struct_type_.clear();
    compact_dbs_.clear();
    start_key_.clear();
    end_key_.clear();
  }
  std::string struct_type_;
  std::set<std::string> compact_dbs_;
  std::string start_key_;
  std::string end_key_;
};

class PurgelogstoCmd : public Cmd {
 public:
  PurgelogstoCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::ADMIN)) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new PurgelogstoCmd(*this); }

 private:
  uint32_t num_ = 0;
  std::string db_;
  void DoInitial() override;
};

class PingCmd : public Cmd {
 public:
  PingCmd(const std::string& name, int arity, uint32_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new PingCmd(*this); }

 private:
  void DoInitial() override;
};

class SelectCmd : public Cmd {
 public:
  SelectCmd(const std::string& name, int arity, uint32_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SelectCmd(*this); }

 private:
  void DoInitial() override;
  void Clear() override { db_name_.clear(); }
  std::string db_name_;
  std::shared_ptr<DB> select_db_;
};

class FlushallCmd : public Cmd {
 public:
  FlushallCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::KEYSPACE)) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new FlushallCmd(*this); }
  void Execute() override;
  void FlushAllWithoutLock();
 private:
  void DoInitial() override;
  std::string ToRedisProtocol() override;
  void DoWithoutLock(std::shared_ptr<Slot> slot);
};

class FlushdbCmd : public Cmd {
 public:
  FlushdbCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::KEYSPACE)) {}
  // The flush command belongs to the write categories, so the key cannot be empty
  std::vector<std::string> current_key() const override { return {""}; }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new FlushdbCmd(*this); }
  void FlushAllSlotsWithoutLock(std::shared_ptr<DB> db);
  void Execute() override;
  std::string GetFlushDname() { return db_name_; }

 private:
  std::string db_name_;
  void DoInitial() override;
  void Clear() override { db_name_.clear(); }
  void DoWithoutLock(std::shared_ptr<Slot> slot);
};

class ClientCmd : public Cmd {
 public:
  ClientCmd(const std::string& name, int arity, uint32_t flag) : Cmd(name, arity, flag) {
    subCmdName_ = {"getname", "setname", "list", "addr", "kill"};
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  const static std::string CLIENT_LIST_S;
  const static std::string CLIENT_KILL_S;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
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
    kInfoDebug,
    kInfoCommandStats,
    kInfoCache
  };

  InfoCmd(const std::string& name, int arity, uint32_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new InfoCmd(*this); }
  void Execute() override;

 private:
  InfoSection info_section_;
  bool rescan_ = false;  // whether to rescan the keyspace
  bool off_ = false;
  std::set<std::string> keyspace_scan_dbs_;
  time_t db_size_last_time_ = 0;
  uint64_t db_size_ = 0;
  uint64_t log_size_ = 0;
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
  const static std::string kCommandStatsSection;
  const static std::string kCacheSection;

  void DoInitial() override;
  void Clear() override {
    rescan_ = false;
    off_ = false;
    keyspace_scan_dbs_.clear();
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
  void InfoCommandStats(std::string& info);
  void InfoCache(std::string& info, std::shared_ptr<Slot> slot);

  std::string CacheStatusToString(int status);
};

class ShutdownCmd : public Cmd {
 public:
  ShutdownCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::ADMIN)) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new ShutdownCmd(*this); }

 private:
  void DoInitial() override;
};

class ConfigCmd : public Cmd {
 public:
  ConfigCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::ADMIN)) {
    subCmdName_ = {"get", "set", "rewrite", "resetstat"};
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new ConfigCmd(*this); }
  void Execute() override;

 private:
  std::vector<std::string> config_args_v_;
  void DoInitial() override;
  void ConfigGet(std::string& ret);
  void ConfigSet(std::string& ret, std::shared_ptr<Slot> slot);
  void ConfigRewrite(std::string& ret);
  void ConfigResetstat(std::string& ret);
  void ConfigRewriteReplicationID(std::string& ret);
};

class MonitorCmd : public Cmd {
 public:
  MonitorCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::ADMIN)) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new MonitorCmd(*this); }

 private:
  void DoInitial() override;
};

class DbsizeCmd : public Cmd {
 public:
  DbsizeCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::ADMIN)) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new DbsizeCmd(*this); }

 private:
  void DoInitial() override;
};

class TimeCmd : public Cmd {
 public:
  TimeCmd(const std::string& name, int arity, uint32_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new TimeCmd(*this); }

 private:
  void DoInitial() override;
};

class LastsaveCmd : public Cmd {
 public:
  LastsaveCmd(const std::string& name, int arity, uint32_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new LastsaveCmd(*this); }

 private:
  void DoInitial() override;
};

class DelbackupCmd : public Cmd {
 public:
  DelbackupCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::ADMIN)) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new DelbackupCmd(*this); }

 private:
  void DoInitial() override;
};

class EchoCmd : public Cmd {
 public:
  EchoCmd(const std::string& name, int arity, uint32_t flag) : Cmd(name, arity, flag) {}
  void Merge() override{};
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  Cmd* Clone() override { return new EchoCmd(*this); }

 private:
  std::string body_;
  void DoInitial() override;
};

class ScandbCmd : public Cmd {
 public:
  ScandbCmd(const std::string& name, int arity, uint32_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
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
  SlowlogCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::ADMIN)) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
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
  PaddingCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::ADMIN)) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new PaddingCmd(*this); }

 private:
  void DoInitial() override;
  std::string ToRedisProtocol() override;
};

class PKPatternMatchDelCmd : public Cmd {
 public:
  PKPatternMatchDelCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::ADMIN)) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
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
  DummyCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::ADMIN)) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new DummyCmd(*this); }

 private:
  void DoInitial() override;
};

class QuitCmd : public Cmd {
 public:
  QuitCmd(const std::string& name, int arity, uint32_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new QuitCmd(*this); }

 private:
  void DoInitial() override;
};

class HelloCmd : public Cmd {
 public:
  HelloCmd(const std::string& name, int arity, uint32_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new HelloCmd(*this); }

 private:
  void DoInitial() override;
};

class DiskRecoveryCmd : public Cmd {
 public:
  DiskRecoveryCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::ADMIN)) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new DiskRecoveryCmd(*this); }

 private:
  void DoInitial() override;
  std::map<std::string, uint64_t> background_errors_;
};

class ClearReplicationIDCmd : public Cmd {
 public:
  ClearReplicationIDCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::ADMIN)) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new ClearReplicationIDCmd(*this); }
  void DoBinlog(const std::shared_ptr<SyncMasterSlot>& slot) override {};

 private:
  void DoInitial() override;
};

class DisableWalCmd : public Cmd {
 public:
  DisableWalCmd(const std::string& name, int arity, uint32_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new DisableWalCmd(*this); }

 private:
  void DoInitial() override;
};

class CacheCmd : public Cmd {
 public:
  enum CacheCondition {kCLEAR_DB, kCLEAR_HITRATIO, kDEL_KEYS, kRANDOM_KEY};
  CacheCmd(const std::string& name, int arity, uint32_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new CacheCmd(*this); }

 private:
  CacheCondition condition_;
  std::vector<std::string> keys_;
  rocksdb::Status s_;
  void DoInitial() override;
  void Clear() override {
    keys_.clear();
  }
};

class ClearCacheCmd : public Cmd {
 public:
  ClearCacheCmd(const std::string& name, int arity, uint32_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new ClearCacheCmd(*this); }

 private:
  void DoInitial() override;
};

#ifdef WITH_COMMAND_DOCS
class CommandCmd : public Cmd {
 public:
  CommandCmd(const std::string& name, int arity, uint32_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new CommandCmd(*this); }

  class CommandFieldCompare {
   public:
    CommandFieldCompare() = default;
    bool operator()(const std::string&, const std::string&) const;

   private:
    const static std::unordered_map<std::string, int> kFieldNameOrder;
  };

  class Encodable;
  using EncodablePtr = std::shared_ptr<Encodable>;

  class Encodable {
   public:
    friend CmdRes& operator<<(CmdRes& res, const Encodable& e) { return e.EncodeTo(res); }
    EncodablePtr operator+(const EncodablePtr& other) { return MergeFrom(other); }

   protected:
    virtual CmdRes& EncodeTo(CmdRes&) const = 0;
    virtual EncodablePtr MergeFrom(const EncodablePtr& other) const = 0;
  };

  class EncodableInt : public Encodable {
   public:
    EncodableInt(int value) : value_(value) {}
    EncodableInt(unsigned long long value) : value_(value) {}

   protected:
    CmdRes& EncodeTo(CmdRes& res) const override;
    EncodablePtr MergeFrom(const EncodablePtr& other) const override;

   private:
    int value_;
  };

  class EncodableString : public Encodable {
   public:
    EncodableString(std::string value) : value_(std::move(value)) {}

   protected:
    CmdRes& EncodeTo(CmdRes& res) const override;
    EncodablePtr MergeFrom(const EncodablePtr& other) const override;

   private:
    std::string value_;
  };

  class EncodableMap : public Encodable {
   public:
    using RedisMap = std::map<std::string, EncodablePtr, CommandFieldCompare>;
    EncodableMap(RedisMap values) : values_(std::move(values)) {}
    template <typename Map>
    static CmdRes& EncodeTo(CmdRes& res, const Map& map, const Map& specialization = Map());

   protected:
    CmdRes& EncodeTo(CmdRes& res) const override;
    EncodablePtr MergeFrom(const EncodablePtr& other) const override;

   private:
    RedisMap values_;

    const static std::string kPrefix;
  };

  class EncodableSet : public Encodable {
   public:
    EncodableSet(std::vector<EncodablePtr> values) : values_(std::move(values)) {}

   protected:
    CmdRes& EncodeTo(CmdRes& res) const override;
    EncodablePtr MergeFrom(const EncodablePtr& other) const override;

   private:
    std::vector<EncodablePtr> values_;

    const static std::string kPrefix;
  };

  class EncodableArray : public Encodable {
   public:
    EncodableArray(std::vector<EncodablePtr> values) : values_(std::move(values)) {}

   protected:
    CmdRes& EncodeTo(CmdRes& res) const override;
    EncodablePtr MergeFrom(const EncodablePtr& other) const override;

   private:
    std::vector<EncodablePtr> values_;
  };

  class EncodableStatus : public Encodable {
   public:
    EncodableStatus(std::string value) : value_(std::move(value)) {}

   protected:
    CmdRes& EncodeTo(CmdRes& res) const override;
    EncodablePtr MergeFrom(const EncodablePtr& other) const override;

   private:
    std::string value_;

    const static std::string kPrefix;
  };

 private:
  void DoInitial() override;

  std::string command_;
  std::vector<std::string>::const_iterator cmds_begin_, cmds_end_;

  const static std::string kPikaField;
  const static EncodablePtr kNotSupportedLiteral;
  const static EncodablePtr kCompatibleLiteral;
  const static EncodablePtr kBitSpecLiteral;
  const static EncodablePtr kHyperLogLiteral;
  const static EncodablePtr kPubSubLiteral;

  const static EncodablePtr kNotSupportedSpecialization;
  const static EncodablePtr kCompatibleSpecialization;
  const static EncodablePtr kBitSpecialization;
  const static EncodablePtr kHyperLogSpecialization;
  const static EncodablePtr kPubSubSpecialization;

  const static std::unordered_map<std::string, EncodablePtr> kPikaSpecialization;
  const static std::unordered_map<std::string, EncodablePtr> kCommandDocs;
};

static CommandCmd::EncodablePtr operator""_RedisInt(unsigned long long value);
static CommandCmd::EncodablePtr operator""_RedisString(const char* value);
static CommandCmd::EncodablePtr operator""_RedisStatus(const char* value);
static CommandCmd::EncodablePtr RedisMap(CommandCmd::EncodableMap::RedisMap values);
static CommandCmd::EncodablePtr RedisSet(std::vector<CommandCmd::EncodablePtr> values);
static CommandCmd::EncodablePtr RedisArray(std::vector<CommandCmd::EncodablePtr> values);

#endif  // WITH_COMMAND_DOCS

#endif  // PIKA_ADMIN_H_
