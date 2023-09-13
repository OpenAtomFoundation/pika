/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#ifndef PIKIWIDB_SRC_BASE_CMD_H
#define PIKIWIDB_SRC_BASE_CMD_H

#include <atomic>
#include <memory>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

#include "client.h"
#include "cmd_context.h"

namespace pikiwidb {

// command definition
// 目前，为了兼容以前的，所有的命令都加了 `n_`前缀
const std::string kCmdNameSet = "set";
const std::string kCmdNameGet = "get";
const std::string kCmdNameConfig = "config";

enum CmdFlags {
  CmdFlagsWrite = (1 << 0),             // May modify the dataset
  CmdFlagsReadonly = (1 << 1),          // Doesn't modify the dataset
  CmdFlagsModule = (1 << 2),            // Implemented by a module
  CmdFlagsAdmin = (1 << 3),             // Administrative command
  CmdFlagsPubsub = (1 << 4),            // Pub/Sub related command
  CmdFlagsNoscript = (1 << 5),          // Not allowed in Lua scripts
  CmdFlagsBlocking = (1 << 6),          // May block the server
  CmdFlagsSkipMonitor = (1 << 7),       // Don't propagate to MONITOR
  CmdFlagsSkipSlowlog = (1 << 8),       // Don't log to slowlog
  CmdFlagsFast = (1 << 9),              // Tagged as fast by developer
  CmdFlagsNoAuth = (1 << 10),           // Skip ACL checks
  CmdFlagsMayReplicate = (1 << 11),     // May replicate even if writes are disabled
  CmdFlagsProtected = (1 << 12),        // Don't accept in scripts
  CmdFlagsModuleNoCluster = (1 << 13),  // No cluster mode support
  CmdFlagsNoMulti = (1 << 14),          // Cannot be pipelined
};

enum AclCategory {
  AclCategoryKeyspace = (1 << 0),
  AclCategoryRead = (1 << 1),
  AclCategoryWrite = (1 << 2),
  AclCategorySet = (1 << 3),
  AclCategorySortedSet = (1 << 4),
  AclCategoryList = (1 << 5),
  AclCategoryHash = (1 << 6),
  AclCategoryString = (1 << 7),
  AclCategoryBitmap = (1 << 8),
  AclCategoryHyperloglog = (1 << 9),
  AclCategoryGeo = (1 << 10),
  AclCategoryStream = (1 << 11),
  AclCategoryPubsub = (1 << 12),
  AclCategoryAdmin = (1 << 13),
  AclCategoryFast = (1 << 14),
  AclCategorySlow = (1 << 15),
  AclCategoryBlocking = (1 << 16),
  AclCategoryDangerous = (1 << 17),
  AclCategoryConnection = (1 << 18),
  AclCategoryTransaction = (1 << 19),
  AclCategoryScripting = (1 << 20)
};

/**
 * @brief Base class for all commands
 * BaseCmd, as the base class for all commands, mainly implements some common functions
 * such as command name, number of parameters, command flag
 * All data related to a single command execution cannot be defined in Base Cmd and its derived classes.
 * Because the command may be executed in multiple threads at the same time, the data defined in the command
 * will be overwritten by other threads, causing the command to be executed incorrectly.
 * Therefore, the data related to the execution of the command must be defined in the `CmdContext` class.
 * The `CmdContext` class is passed to the command for execution.
 * Base Cmd and its derived classes only provide corresponding functions and logical processing for command execution,
 * but do not provide data storage.
 *
 * This avoids creating a new object every time a command is executed and reduces memory allocation
 * But some data that does not change during command execution
 * (data that does not need to be changed after command initialization) can be placed in Base Cmd
 * For example: command name, number of parameters, command flag, etc.
 */
class BaseCmd : public std::enable_shared_from_this<BaseCmd> {
 public:
  // 这些感觉不需要了

  //  enum CmdStage { kNone, kBinlogStage, kExecuteStage };
  //  struct HintKeys {
  //    HintKeys() = default;
  //    void Push(const std::string& key, int hint) {
  //      keys.push_back(key);
  //      hints.push_back(hint);
  //    }
  //    bool empty() const { return keys.empty() && hints.empty(); }
  //    std::vector<std::string> keys;
  //    std::vector<int> hints;
  //  };

  //  struct CommandStatistics {
  //    CommandStatistics() = default;
  //    CommandStatistics(const CommandStatistics& other) {
  //      cmd_time_consuming.store(other.cmd_time_consuming.load());
  //      cmd_count.store(other.cmd_count.load());
  //    }
  //    std::atomic<int32_t> cmd_count = {0};
  //    std::atomic<int32_t> cmd_time_consuming = {0};
  //  };
  //  CommandStatistics state;

  /**
   * @brief Construct a new Base Cmd object
   * @param name command name
   * @param arity number of parameters
   * @param flag command flag
   * @param aclCategory command acl category
   */
  BaseCmd(std::string name, int16_t arity, uint32_t flag, uint32_t aclCategory);
  virtual ~BaseCmd() = default;

  // check that each parameter meets the requirements
  bool CheckArg(size_t num) const;

  // get the key in the current command
  // e.g: set myKey value, return myKey
  std::vector<std::string> CurrentKey(const CmdContext& context) const;

  // the entry point for the entire cmd execution
  // 后续如果需要拓展，在这个函数里面拓展
  // 对外部调用者来说，只暴露这个函数，其他的都是内部实现
  void Execute(CmdContext& ctx);

  // binlog 相关的函数，我对这块不熟悉，就没有移植，后面binlog应该可以在Execute里面调用
  virtual std::string ToBinlog(uint32_t exec_time, uint32_t term_id, uint64_t logic_id, uint32_t filenum,
                               uint64_t offset);
  virtual void DoBinlog();

  //  virtual void ProcessFlushDBCmd();
  //  virtual void ProcessFlushAllCmd();
  //  virtual void ProcessSingleSlotCmd();
  //  virtual void ProcessMultiSlotCmd();
  //  virtual void ProcessDoNotSpecifySlotCmd();
  //  virtual void Do(std::shared_ptr<Slot> slot = nullptr) = 0;
  //  virtual Cmd* Clone() = 0;
  //  used for execute multikey command into different slots
  //  virtual void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) = 0;
  //  virtual void Merge() = 0;

  bool IsWrite() const;
  bool HasFlag(uint32_t flag) const;
  void SetFlag(uint32_t flag);
  void ResetFlag(uint32_t flag);

  // Processing functions related to subcommands. If this command is not a subcommand,
  // then these functions do not need to be implemented.
  // If it is a subcommand, you need to implement these functions
  // e.g: CmdConfig is a subcommand, and the subcommand is set and get
  virtual bool HasSubCommand() const;                      // The command is there a sub command
  virtual std::vector<std::string> SubCommand() const;     // Get command is there a sub command
  virtual int8_t SubCmdIndex(const std::string& cmdName);  // if the command no subCommand，return -1；

  uint32_t AclCategory() const;
  void AddAclCategory(uint32_t aclCategory);
  std::string Name() const;
  //  CmdRes& Res();
  //  std::string db_name() const;
  //  BinlogOffset binlog_offset() const;
  //  PikaCmdArgsType& argv();

  //  void SetConn(const std::shared_ptr<net::NetConn>& conn);
  //  std::shared_ptr<net::NetConn> GetConn();

  //  void SetResp(const std::shared_ptr<std::string>& resp);
  //  std::shared_ptr<std::string> GetResp();

  uint32_t GetCmdId() const;

 protected:
  // Execute a specific command
  virtual void DoCmd(CmdContext& ctx) = 0;

  std::string name_;
  int16_t arity_ = 0;
  uint32_t flag_ = 0;
  std::vector<std::string> subCmdName_;  // sub command name, may be empty

  //  CmdRes res_;
  //  std::string dbName_;
  //  std::weak_ptr<net::NetConn> conn_;
  //  std::weak_ptr<std::string> resp_;
  //  uint64_t doDuration_ = 0;

  uint32_t cmdId_ = 0;
  uint32_t aclCategory_ = 0;

 private:
  // The function to be executed first before executing `DoCmd`
  // What needs to be done at present are: extract the key in the command and fill it into the context
  // If this function returns false, then Do Cmd will not be executed
  virtual bool DoInitial(CmdContext& ctx) = 0;

  //  virtual void Clear(){};
  //  BaseCmd& operator=(const BaseCmd&);
};

}  // namespace pikiwidb
#endif  // PIKIWIDB_SRC_BASE_CMD_H
