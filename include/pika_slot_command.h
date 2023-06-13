#ifndef PIKA_SLOT_COMMAND_H_
#define PIKA_SLOT_COMMAND_H_

#include "include/pika_client_conn.h"
#include "include/pika_command.h"
#include "include/pika_slot.h"
#include "net/include/net_cli.h"
#include "net/include/net_thread.h"
#include "storage/storage.h"
#include "strings.h"

const std::string SlotKeyPrefix = "_internal:slotkey:4migrate:";
const std::string SlotTagPrefix = "_internal:slottag:4migrate:";
const size_t MaxKeySendSize = 10 * 1024;
const int asyncRecvsNum = 64;

// crc32
#define HASH_SLOTS_MASK 0x000003ff
#define HASH_SLOTS_SIZE (HASH_SLOTS_MASK + 1)

extern uint32_t crc32tab[256];

void CRC32TableInit(uint32_t poly);

extern void InitCRC32Table();

extern uint32_t CRC32Update(uint32_t crc, const char *buf, int len);
extern uint32_t CRC32CheckSum(const char *buf, int len);

int GetSlotID(const std::string &str);
int GetKeyType(const std::string key, std::string &key_type, std::shared_ptr<Slot> slot);
void AddSlotKey(const std::string type, const std::string key, std::shared_ptr<Slot> slot);
void RemKeyNotExists(const std::string type, const std::string key, std::shared_ptr<Slot> slot);
void RemSlotKey(const std::string key, std::shared_ptr<Slot> slot);
int DeleteKey(const std::string key, const char key_type, std::shared_ptr<Slot> slot);
std::string GetSlotKey(int slot);
std::string GetSlotsTagKey(uint32_t crc);
int GetSlotsID(const std::string &str, uint32_t *pcrc, int *phastag);
void SlotKeyRemByType(const std::string &type, const std::string &key, std::shared_ptr<Slot> slot);

class PikaMigrate {
 public:
  PikaMigrate();
  virtual ~PikaMigrate();

  int MigrateKey(const std::string &host, const int port, int db, int timeout, const std::string &key, const char type,
                 std::string &detail, std::shared_ptr<Slot> slot);
  void CleanMigrateClient();

  void Lock() {
    LOG(INFO) << "migrate lock";
    mutex_.lock();
  }
  int Trylock() {
    LOG(INFO) << "migrate trylock";
    return mutex_.try_lock();
  }
  void Unlock() {
    LOG(INFO) << "migrate unlock";
    mutex_.unlock();
  }
  net::NetCli *GetMigrateClient(const std::string &host, const int port, int timeout);

 private:
  std::map<std::string, void *> migrate_clients_;
  pstd::Mutex mutex_;

  void KillMigrateClient(net::NetCli *migrate_cli);
  void KillAllMigrateClient();

  int MigrateSend(net::NetCli *migrate_cli, const std::string &key, const char type, std::string &detail,
                  std::shared_ptr<Slot> slot);
  bool MigrateRecv(net::NetCli *migrate_cli, int need_receive, std::string &detail);

  int ParseKey(const std::string &key, const char type, std::string &wbuf_str, std::shared_ptr<Slot> slot);
  int64_t TTLByType(const char key_type, const std::string &key, std::shared_ptr<Slot> slot);
  int ParseKKey(const std::string &key, std::string &wbuf_str, std::shared_ptr<Slot> slot);
  int ParseZKey(const std::string &key, std::string &wbuf_str, std::shared_ptr<Slot> slot);
  int ParseSKey(const std::string &key, std::string &wbuf_str, std::shared_ptr<Slot> slot);
  int ParseHKey(const std::string &key, std::string &wbuf_str, std::shared_ptr<Slot> slot);
  int ParseLKey(const std::string &key, std::string &wbuf_str, std::shared_ptr<Slot> slot);
  bool SetTTL(const std::string &key, std::string &wbuf_str, int64_t ttl);
};

class SlotsMgrtTagSlotCmd : public Cmd {
 public:
  SlotsMgrtTagSlotCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot>slot) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlotsMgrtTagSlotCmd(*this); }
 private:
  std::string dest_ip_;
  int64_t dest_port_;
  int64_t timeout_ms_;
  int64_t slot_id_;
  std::basic_string<char, std::char_traits<char>, std::allocator<char>> key_;
  char key_type_;

  void DoInitial() override;
  int SlotKeyPop(std::shared_ptr<Slot>slot);
};

class SlotsMgrtTagSlotAsyncCmd : public Cmd {
 public:
  SlotsMgrtTagSlotAsyncCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag){}
  void Do(std::shared_ptr<Slot>slot) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge(){};
  Cmd* Clone() override { return new SlotsMgrtTagSlotAsyncCmd(*this); }
 private:
  std::string dest_ip_;
  int64_t dest_port_;
  int64_t timeout_ms_;
  int64_t max_bulks_;
  int64_t max_bytes_;
  int64_t slot_num_;
  int64_t keys_num_;

  void DoInitial() override;
};

class SlotsMgrtTagOneCmd : public Cmd {
 public:
  SlotsMgrtTagOneCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot>slot) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlotsMgrtTagOneCmd(*this); }
 private:
  std::string dest_ip_;
  int64_t dest_port_;
  int64_t timeout_ms_;
  std::string key_;
  int64_t slot_id_;
  char key_type_;
  void DoInitial() override;
  int SlotKeyRemCheck(std::shared_ptr<Slot>slot);
  int KeyTypeCheck(std::shared_ptr<Slot>slot);
};

class SlotsMgrtAsyncStatusCmd : public Cmd {
 public:
  SlotsMgrtAsyncStatusCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlotsMgrtAsyncStatusCmd(*this); }

 private:
  void DoInitial() override;
};

class SlotsInfoCmd : public Cmd {
 public:
  SlotsInfoCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot>slot) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlotsInfoCmd(*this); }
 private:
  void DoInitial() override;

  int64_t begin_;
  int64_t end_ = 1024;
};

class SlotsMgrtAsyncCancelCmd : public Cmd {
 public:
  SlotsMgrtAsyncCancelCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot>slot) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlotsMgrtAsyncCancelCmd(*this); }
 private:
  void DoInitial() override;
};

class SlotsDelCmd : public Cmd {
 public:
  SlotsDelCmd(const std::string& name, int arity, uint16_t flag):Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot>slot) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlotsDelCmd(*this); }
 private:
  std::vector<std::string> slots_;
  void DoInitial() override;
};

class SlotsHashKeyCmd : public Cmd {
 public:
  SlotsHashKeyCmd(const std::string& name, int arity, uint16_t flag):Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot>slot) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlotsHashKeyCmd(*this); }
 private:
  std::vector<std::string> keys_;
  void DoInitial() override;
};

class SlotsScanCmd : public Cmd {
 public:
  SlotsScanCmd(const std::string& name, int arity, uint16_t flag):Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot>slot) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlotsScanCmd(*this); }
 private:
  std::string key_, pattern_;
  int64_t cursor_, count_;
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
  SlotsMgrtExecWrapperCmd(const std::string& name, int arity, uint16_t flag):Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Slot>slot);
  virtual void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys){};
  virtual void Merge(){};
  virtual Cmd* Clone() override { return new SlotsMgrtExecWrapperCmd(*this); }
 private:
  std::string key_;
  std::vector<std::string> args;
  virtual void DoInitial();
};


class SlotsMgrtSenderThread: public net::Thread {
 public:
  SlotsMgrtSenderThread();
  virtual ~SlotsMgrtSenderThread();
  int SlotsMigrateOne(const std::string &key, std::shared_ptr<Slot>slot);
  bool SlotsMigrateBatch(const std::string &ip, int64_t port, int64_t time_out, int64_t slots, int64_t keys_num, std::shared_ptr<Slot>slot);
  bool GetSlotsMigrateResult(int64_t *moved, int64_t *remained);
  void GetSlotsMgrtSenderStatus(std::string *ip, int64_t *port, int64_t *slot, bool *migrating, int64_t *moved, int64_t *remained);
  bool SlotsMigrateAsyncCancel();
 private:
  std::string dest_ip_;
  int64_t dest_port_;
  int64_t timeout_ms_;
  int64_t slot_num_;
  int64_t keys_num_;
  int64_t moved_keys_num_; // during one batch moved
  int64_t moved_keys_all_; // all keys moved in the slot
  int64_t remained_keys_num_;
  bool error_;
  std::vector<std::pair<const char, std::string>> migrating_batch_;
  std::vector<std::pair<const char, std::string>> migrating_ones_;
  net::NetCli *cli_;
  pstd::Mutex rwlock_;
  pstd::Mutex rwlock_db_;
  pstd::Mutex rwlock_batch_;
  pstd::Mutex rwlock_ones_;
  pstd::Mutex slotsmgrt_cond_mutex_;
  pstd::Mutex mutex_;
  std::atomic<bool> is_migrating_ = false;
  pstd::CondVar slotsmgrt_cond_;
  std::shared_ptr<Slot>slot_;

  void* ThreadMain() override;

  bool ElectMigrateKeys(std::shared_ptr<Slot>slot);
};


#endif
