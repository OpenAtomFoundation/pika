#ifndef PIKA_SLOT_H_
#define PIKA_SLOT_H_

#include "pika_command.h"
#include "pika_client_conn.h"
#include "strings.h"

const std::string SlotKeyPrefix = "_internal:slotkey:4migrate:";

//crc 32
#define HASH_SLOTS_MASK 0x000003ff
#define HASH_SLOTS_SIZE (HASH_SLOTS_MASK + 1)

const uint32_t IEEE_POLY = 0xedb88320;
extern uint32_t crc32tab[256];

void CRC32_TableInit(uint32_t poly);

extern void InitCRC32Table();

extern uint32_t CRC32Update(uint32_t crc, const char *buf, int len);

extern int SlotNum(const std::string &str);
extern int KeyType(const std::string key, std::string &key_type);

extern void SlotKeyAdd(const std::string type, const std::string key);
extern void SlotKeyRem(const std::string key);
extern void KeyNotExistsRem(const std::string type, const std::string key);


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

#endif
