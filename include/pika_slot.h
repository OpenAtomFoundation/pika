#ifndef PIKA_SLOT_H_
#define PIKA_SLOT_H_

#include "pink/include/pink_thread.h"
#include "pink/include/pink_cli.h"
#include "include/pika_command.h"
#include "include/pika_client_conn.h"
#include "blackwidow/blackwidow.h"
#include "strings.h"

const std::string SlotKeyPrefix = "_internal:slotkey:4migrate:";
const size_t MaxKeySendSize = 10 * 1024;
//crc 32
#define HASH_SLOTS_MASK 0x000003ff
#define HASH_SLOTS_SIZE (HASH_SLOTS_MASK + 1)

const uint32_t IEEE_POLY = 0xedb88320;
extern uint32_t crc32tab[256];

void CRC32TableInit(uint32_t poly);

extern void InitCRC32Table();

extern uint32_t CRC32Update(uint32_t crc, const char *buf, int len);

extern int SlotNum(const std::string &str);
extern int KeyType(const std::string key, std::string &key_type);

extern void SlotKeyAdd(const std::string type, const std::string key);
extern void SlotKeyRem(const std::string key);
extern void KeyNotExistsRem(const std::string type, const std::string key);
extern int KeyDelete(const std::string key, const char key_type);

class SlotsMgrtTagSlotCmd : public Cmd {
public:
    SlotsMgrtTagSlotCmd() {}
    virtual void Do();
private:
    std::string dest_ip_;
    int64_t dest_port_;
    int64_t timeout_ms_;
    int64_t slot_num_;
    std::string key_;
    char key_type_;

    virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
    int SlotKeyPop();
};

class SlotsMgrtTagOneCmd : public Cmd {
public:
    SlotsMgrtTagOneCmd() {}
    virtual void Do();
private:
    std::string dest_ip_;
    int64_t dest_port_;
    int64_t timeout_ms_;
    std::string key_;
    int64_t slot_num_;
    char key_type_;

    virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
    int KeyTypeCheck();
    int SlotKeyRemCheck();
};

class SlotsInfoCmd : public Cmd {
public:
    SlotsInfoCmd() {}
    virtual void Do();
private:
    virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class SlotsHashKeyCmd : public Cmd {
public:
    SlotsHashKeyCmd() {}
    virtual void Do();
private:
    std::vector<std::string> keys_;
    virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class SlotsReloadCmd : public Cmd {
public:
    SlotsReloadCmd() {}
    virtual void Do();
private:
    virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class SlotsReloadOffCmd : public Cmd {
public:
    SlotsReloadOffCmd() {}
    virtual void Do();
private:
    virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class SlotsDelCmd : public Cmd {
public:
    SlotsDelCmd() {}
    virtual void Do();
private:
    std::vector<std::string> slots_;
    virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class SlotsScanCmd : public Cmd {
public:
  SlotsScanCmd() : pattern_("*"), count_(10) {}
  virtual void Do();
private:
  std::string key_, pattern_;
  int64_t cursor_, count_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
  virtual void Clear() {
    pattern_ = "*";
    count_ = 10;
  }
};

class SlotsCleanupCmd : public Cmd {
public:
    SlotsCleanupCmd() {}
    virtual void Do();
    std::vector<int> cleanup_slots_;
private:
    virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class SlotsCleanupOffCmd : public Cmd {
public:
    SlotsCleanupOffCmd() {}
    virtual void Do();
private:
    virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class SlotsMgrtTagSlotAsyncCmd : public Cmd {
public:
    SlotsMgrtTagSlotAsyncCmd() {}
    virtual void Do();
private:
    std::string dest_ip_;
    int64_t dest_port_;
    int64_t timeout_ms_;
    int64_t max_bulks_;
    int64_t max_bytes_;
    int64_t slot_num_;
    int64_t keys_num_;

    virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class SlotsMgrtExecWrapperCmd : public Cmd {
public:
    SlotsMgrtExecWrapperCmd() {}
    virtual void Do();
private:
    std::string key_;
    virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class SlotsMgrtAsyncStatusCmd : public Cmd {
public:
    SlotsMgrtAsyncStatusCmd() {}
    virtual void Do();
private:
    virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class SlotsMgrtAsyncCancelCmd : public Cmd {
public:
    SlotsMgrtAsyncCancelCmd() {}
    virtual void Do();
private:
    virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class SlotsMgrtSenderThread: public pink::Thread {
public:
    SlotsMgrtSenderThread();
    virtual ~SlotsMgrtSenderThread();
    int SlotsMigrateOne(const std::string &key);
    bool SlotsMigrateBatch(const std::string &ip, int64_t port, int64_t time_out, int64_t slot, int64_t keys_num);
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
    std::vector<std::pair<const char, std::string>> migrating_batch_;
    std::vector<std::pair<const char, std::string>> migrating_ones_;
    pink::PinkCli *cli_;
    pthread_rwlock_t rwlock_db_;
    pthread_rwlock_t rwlock_batch_;
    pthread_rwlock_t rwlock_ones_;
    slash::CondVar slotsmgrt_cond_;
    slash::Mutex slotsmgrt_cond_mutex_;
    std::atomic<bool> is_migrating_;
    std::atomic<bool> should_exit_;

    bool ElectMigrateKeys();
    virtual void* ThreadMain();
};

#endif
