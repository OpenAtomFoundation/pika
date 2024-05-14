#ifndef PIKA_SLOT_COMMAND_H_
#define PIKA_SLOT_COMMAND_H_

#include "include/pika_client_conn.h"
#include "include/pika_command.h"
#include "net/include/net_cli.h"
#include "net/include/net_thread.h"
#include "storage/storage.h"
#include "storage/src/base_data_key_format.h"
#include "strings.h"

const std::string SlotKeyPrefix = "_internal:slotkey:4migrate:";
const std::string SlotTagPrefix = "_internal:slottag:4migrate:";

const size_t MaxKeySendSize = 10 * 1024;

int GetKeyType(const std::string& key, std::string &key_type, const std::shared_ptr<DB>& db);
void AddSlotKey(const std::string& type, const std::string& key, const std::shared_ptr<DB>& db);
void RemSlotKey(const std::string& key, const std::shared_ptr<DB>& db);
int DeleteKey(const std::string& key, const char key_type, const std::shared_ptr<DB>& db);
void RemSlotKeyByType(const std::string& type, const std::string& key, const std::shared_ptr<DB>& db);
std::string GetSlotKey(uint32_t slot);
std::string GetSlotsTagKey(uint32_t crc);

class PikaMigrate {
 public:
  PikaMigrate();
  virtual ~PikaMigrate();

  int MigrateKey(const std::string& host, const int port, int timeout, const std::string& key, const char type,
                 std::string& detail, const std::shared_ptr<DB>& db);
  void CleanMigrateClient();

  void Lock() {
    mutex_.lock();
  }
  int Trylock() {
    return mutex_.try_lock();
  }
  void Unlock() {
    mutex_.unlock();
  }
  net::NetCli* GetMigrateClient(const std::string& host, const int port, int timeout);

 private:
  std::map<std::string, void*> migrate_clients_;
  pstd::Mutex mutex_;
  void KillMigrateClient(net::NetCli* migrate_cli);
  void KillAllMigrateClient();
  int64_t TTLByType(const char key_type, const std::string& key, const std::shared_ptr<DB>& db);
  int MigrateSend(net::NetCli* migrate_cli, const std::string& key, const char type, std::string& detail,
                  const std::shared_ptr<DB>& db);
  bool MigrateRecv(net::NetCli* migrate_cli, int need_receive, std::string& detail);
  int ParseKey(const std::string& key, const char type, std::string& wbuf_str, const std::shared_ptr<DB>& db);
  int ParseKKey(const std::string& key, std::string& wbuf_str, const std::shared_ptr<DB>& db);
  int ParseZKey(const std::string& key, std::string& wbuf_str, const std::shared_ptr<DB>& db);
  int ParseSKey(const std::string& key, std::string& wbuf_str, const std::shared_ptr<DB>& db);
  int ParseHKey(const std::string& key, std::string& wbuf_str, const std::shared_ptr<DB>& db);
  int ParseLKey(const std::string& key, std::string& wbuf_str, const std::shared_ptr<DB>& db);
  int ParseMKey(const std::string& key, std::string& wbuf_str, const std::shared_ptr<DB>& db);
  bool SetTTL(const std::string& key, std::string& wbuf_str, int64_t ttl);
};

class SlotsMgrtTagSlotCmd : public Cmd {
 public:
  SlotsMgrtTagSlotCmd(const std::string& name, int arity, uint32_t flag) : Cmd(name, arity, flag) {}
  void Do() override;
  void Split(const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlotsMgrtTagSlotCmd(*this); }

 private:
  std::string dest_ip_;
  int64_t dest_port_ = 0;
  int64_t timeout_ms_ = 60;
  int64_t slot_id_ = 0;
  std::basic_string<char, std::char_traits<char>, std::allocator<char>> key_;
  void DoInitial() override;
};

class SlotsMgrtTagSlotAsyncCmd : public Cmd {
 public:
  SlotsMgrtTagSlotAsyncCmd(const std::string& name, int arity, uint32_t flag) : Cmd(name, arity, flag){}
  void Do() override;
  void Split(const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlotsMgrtTagSlotAsyncCmd(*this); }

 private:
  std::string dest_ip_;
  int64_t dest_port_ = 0;
  int64_t timeout_ms_ = 60;
  int64_t max_bulks_ = 0;
  int64_t max_bytes_ = 0;
  int64_t slot_id_ = 0;
  int64_t keys_num_ = 0;
  void DoInitial() override;
};

class SlotsMgrtTagOneCmd : public Cmd {
 public:
  SlotsMgrtTagOneCmd(const std::string& name, int arity, uint32_t flag) : Cmd(name, arity, flag) {}
  void Do() override;
  void Split(const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlotsMgrtTagOneCmd(*this); }

 private:
  std::string dest_ip_;
  int64_t dest_port_ = 0;
  int64_t timeout_ms_ = 60;
  std::string key_;
  int64_t slot_id_ = 0;
  char key_type_ = '\0';
  void DoInitial() override;
  int KeyTypeCheck(const std::shared_ptr<DB>& db);
};

class SlotsMgrtAsyncStatusCmd : public Cmd {
 public:
  SlotsMgrtAsyncStatusCmd(const std::string& name, int arity, uint32_t flag) : Cmd(name, arity, flag) {}
  void Do() override;
  void Split(const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlotsMgrtAsyncStatusCmd(*this); }

 private:
  void DoInitial() override;
};

class SlotsInfoCmd : public Cmd {
 public:
  SlotsInfoCmd(const std::string& name, int arity, uint32_t flag) : Cmd(name, arity, flag) {}
  void Do() override;
  void Split(const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlotsInfoCmd(*this); }

 private:
  void DoInitial() override;

  int64_t begin_ = 0;
  int64_t end_ = 1024;
};

class SlotsMgrtAsyncCancelCmd : public Cmd {
 public:
  SlotsMgrtAsyncCancelCmd(const std::string& name, int arity, uint32_t flag) : Cmd(name, arity, flag) {}
  void Do() override;
  void Split(const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlotsMgrtAsyncCancelCmd(*this); }

 private:
  void DoInitial() override;
};

class SlotsDelCmd : public Cmd {
 public:
  SlotsDelCmd(const std::string& name, int arity, uint32_t flag) : Cmd(name, arity, flag) {}
  void Do() override;
  void Split(const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlotsDelCmd(*this); }

 private:
  std::vector<std::string> slots_;
  void DoInitial() override;
};

class SlotsHashKeyCmd : public Cmd {
 public:
  SlotsHashKeyCmd(const std::string& name, int arity, uint32_t flag) : Cmd(name, arity, flag) {}
  void Do() override;
  void Split(const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlotsHashKeyCmd(*this); }

 private:
  std::vector<std::string> keys_;
  void DoInitial() override;
};

class SlotsScanCmd : public Cmd {
 public:
  SlotsScanCmd(const std::string& name, int arity, uint32_t flag) : Cmd(name, arity, flag) {}
  void Do() override;
  void Split(const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlotsScanCmd(*this); }

 private:
  std::string key_;
  std::string pattern_ = "*";
  int64_t cursor_ = 0;
  int64_t count_ = 10;
  void DoInitial()  override;
  void Clear() override {
    pattern_ = "*";
    count_ = 10;
  }
};

/* *
* SLOTSMGRT-EXEC-WRAPPER $hashkey $command [$arg1 ...]
* SLOTSMGRT-EXEC-WRAPPER $hashkey $command [$key1 $arg1 ...]
* SLOTSMGRT-EXEC-WRAPPER $hashkey $command [$key1 $arg1 ...] [$key2 $arg2 ...]
* */
class SlotsMgrtExecWrapperCmd : public Cmd {
 public:
  SlotsMgrtExecWrapperCmd(const std::string& name, int arity, uint32_t flag) : Cmd(name, arity, flag) {}
  void Do() override;
  void Split(const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlotsMgrtExecWrapperCmd(*this); }

 private:
  std::string key_;
  std::vector<std::string> args;
  void DoInitial() override;
};


class SlotsReloadCmd : public Cmd {
 public:
  SlotsReloadCmd(const std::string& name, int arity, uint32_t flag) : Cmd(name, arity, flag) {}
  void Do() override;
  void Split(const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlotsReloadCmd(*this); }

 private:
  void DoInitial() override;
};

class SlotsReloadOffCmd : public Cmd {
 public:
  SlotsReloadOffCmd(const std::string& name, int arity, uint32_t flag) : Cmd(name, arity, flag) {}
  void Do() override;
  void Split(const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlotsReloadOffCmd(*this); }

 private:
  void DoInitial() override;
};

class SlotsCleanupCmd : public Cmd {
 public:
  SlotsCleanupCmd(const std::string& name, int arity, uint32_t flag) : Cmd(name, arity, flag) {}
  void Do() override;
  void Split(const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlotsCleanupCmd(*this); }
  std::vector<int> cleanup_slots_;

 private:
  void DoInitial() override;
};

class SlotsCleanupOffCmd : public Cmd {
 public:
  SlotsCleanupOffCmd(const std::string& name, int arity, uint32_t flag) : Cmd(name, arity, flag) {}
  void Do() override;
  void Split(const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlotsCleanupOffCmd(*this); }

 private:
  void DoInitial() override;
};

#endif
