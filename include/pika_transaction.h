// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_TRANSACTION_H_
#define PIKA_TRANSACTION_H_

#include "acl.h"
#include "include/pika_command.h"
#include "net/include/redis_conn.h"
#include "pika_db.h"
#include "storage/storage.h"

class MultiCmd : public Cmd {
 public:
  MultiCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::TRANSACTION)) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  Cmd* Clone() override { return new MultiCmd(*this); }
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {}
  void Merge() override {}

 private:
  void DoInitial() override;
};

class ExecCmd : public Cmd {
 public:
  ExecCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::TRANSACTION)) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  Cmd* Clone() override { return new ExecCmd(*this); }
  void Execute() override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {}
  void Merge() override {}
  std::vector<std::string> current_key() const override { return {}; }

 private:
  struct CmdInfo {
   public:
    CmdInfo(std::shared_ptr<Cmd> cmd, std::shared_ptr<DB> db, std::shared_ptr<Slot> slot,
            std::shared_ptr<SyncMasterSlot> sync_slot)
        : cmd_(cmd), db_(db), slot_(slot), sync_slot_(sync_slot) {}
    std::shared_ptr<Cmd> cmd_;
    std::shared_ptr<DB> db_;
    std::shared_ptr<Slot> slot_;
    std::shared_ptr<SyncMasterSlot> sync_slot_;
  };
  void DoInitial() override;
  void Lock();
  void Unlock();
  bool IsTxnFailedAndSetState();
  void SetCmdsVec();
  void ServeToBLrPopWithKeys();
  std::unordered_set<std::shared_ptr<DB>> lock_db_{};
  std::unordered_map<std::shared_ptr<Slot>, std::vector<std::string>> lock_slot_keys_{};
  std::unordered_set<std::shared_ptr<Slot>> r_lock_slots_{};
  bool is_lock_rm_slots_{false};  // g_pika_rm->slots_rw_;
  std::vector<CmdInfo> cmds_;
  std::vector<CmdInfo> list_cmd_;
  std::vector<std::string> keys_;
};

class DiscardCmd : public Cmd {
 public:
  DiscardCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::TRANSACTION)) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  Cmd* Clone() override { return new DiscardCmd(*this); }
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {}
  void Merge() override {}

 private:
  void DoInitial() override;
};

class WatchCmd : public Cmd {
 public:
  WatchCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::TRANSACTION)) {}

  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Execute() override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {}
  Cmd* Clone() override { return new WatchCmd(*this); }
  void Merge() override {}
  std::vector<std::string> current_key() const override { return keys_; }

 private:
  void DoInitial() override;
  std::vector<std::string> keys_;
  std::vector<std::string> db_keys_;  // cause the keys watched may cross different dbs, so add dbname as keys prefix
};

class UnwatchCmd : public Cmd {
 public:
  UnwatchCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::TRANSACTION)) {}

  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  Cmd* Clone() override { return new UnwatchCmd(*this); }
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {}
  void Merge() override {}

 private:
  void DoInitial() override;
};

#endif  // PIKA_TRANSACTION_H_
