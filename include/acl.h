// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_ACL_H
#define PIKA_ACL_H

#include <array>
#include <atomic>
#include <bitset>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>
#include "pika_command.h"
#include "pstd_status.h"

static const int USER_COMMAND_BITS_COUNT = 1024;

enum class AclSelectorFlag {
  ROOT = (1 << 0),          // This is the root user permission selector
  ALL_KEYS = (1 << 1),      // The user can mention any key
  ALL_COMMANDS = (1 << 2),  // The user can run all commands
  ALL_CHANNELS = (1 << 3),  // The user can mention any Pub/Sub channel
};

enum class AclCategory {
  KEYSPACE = (1ULL << 0),
  READ = (1ULL << 1),
  WRITE = (1ULL << 2),
  SET = (1ULL << 3),
  SORTEDSET = (1ULL << 4),
  LIST = (1ULL << 5),
  HASH = (1ULL << 6),
  STRING = (1ULL << 7),
  BITMAP = (1ULL << 8),
  HYPERLOGLOG = (1ULL << 9),
  GEO = (1ULL << 10),
  STREAM = (1ULL << 11),
  PUBSUB = (1ULL << 12),
  ADMIN = (1ULL << 13),
  FAST = (1ULL << 14),
  SLOW = (1ULL << 15),
  BLOCKING = (1ULL << 16),
  DANGEROUS = (1ULL << 17),
  CONNECTION = (1ULL << 18),
  TRANSACTION = (1ULL << 19),
  SCRIPTING = (1ULL << 20),
};

enum class AclUserFlag {
  ENABLED = (1 << 0),   // The user is active
  DISABLED = (1 << 1),  // The user is disabled
  NO_PASS = (1 << 2),   /* The user requires no password, any provided password will work. For the
                         default user, this also means that no AUTH is needed, and every
                         connection is immediately authenticated. */
};

enum class AclDeniedCmd { OK, CMD, KEY, CHANNEL, NUMBER, NO_SUB_CMD, NO_AUTH };

enum class AclLogCtx {
  TOPLEVEL,
  MULTI,
  LUA,
};

// ACL key permission types
enum class AclPermission {
  READ = (1 << 0),
  WRITE = (1 << 1),
  ALL = (READ | WRITE),
};

struct AclKeyPattern {
  void ToString(std::string* str) {
    if (flags & static_cast<uint32_t>(AclPermission::ALL)) {
      str->append("~");
    } else if (flags & static_cast<uint32_t>(AclPermission::WRITE)) {
      str->append("%W~");
    } else if (flags & static_cast<uint32_t>(AclPermission::READ)) {
      str->append("%R~");
    }
    str->append(pattern);
  }

  uint32_t flags;      /* The CMD_KEYS_* flags for this key pattern */
  std::string pattern; /* The pattern to match keys against */
};

class ACLLogEntry {
 public:
  ACLLogEntry() = delete;
  ACLLogEntry(int32_t reason, int32_t context, const std::string& object, const std::string& username, int64_t ctime,
              const std::string& cinfo)
      : count_(1),
        reason_(reason),
        context_(context),
        object_(object),
        username_(username),
        ctime_(ctime),
        cinfo_(cinfo) {}

  bool Match(int32_t reason, int32_t context, int64_t ctime, const std::string& object, const std::string& username);

  void AddEntry(const std::string& cinfo, u_int64_t ctime);

  void GetReplyInfo(std::vector<std::string>* vector);

 private:
  uint64_t count_;
  int32_t reason_;
  int32_t context_;
  std::string object_;
  std::string username_;
  int64_t ctime_;
  std::string cinfo_;
};

class User;
class Acl;

class AclSelector {
  friend User;

 public:
  explicit AclSelector() : AclSelector(0){};
  explicit AclSelector(uint32_t flag);
  explicit AclSelector(const AclSelector& selector);
  ~AclSelector() = default;

  inline uint32_t Flags() const { return flags_; };
  inline bool HasFlags(uint32_t flag) const { return flags_ & flag; };
  inline void AddFlags(uint32_t flag) { flags_ |= flag; };
  inline void DecFlags(uint32_t flag) { flags_ &= ~flag; };
  bool EqualChannel(const std::vector<std::string> &allChannel);

 private:
  pstd::Status SetSelector(const std::string& op);

  pstd::Status SetSelectorFromOpSet(const std::string& opSet);

  void ACLDescribeSelector(std::string* str);

  void ACLDescribeSelector(std::vector<std::string>& vector);

  AclDeniedCmd CheckCanExecCmd(std::shared_ptr<Cmd>& cmd, int8_t subCmdIndex, const std::vector<std::string>& keys,
                               std::string* errKey);

  bool SetSelectorCommandBitsForCategory(const std::string& categoryName, bool allow);
  void SetAllCommandSelector();
  void RestAllCommandSelector();

  void InsertKeyPattern(const std::string& str, uint32_t flags);

  void InsertChannel(const std::string& str);

  void ChangeSelector(const Cmd* cmd, bool allow);
  void ChangeSelector(const std::shared_ptr<Cmd>& cmd, bool allow);
  pstd::Status ChangeSelector(const std::shared_ptr<Cmd>& cmd, const std::string& subCmd, bool allow);

  void SetSubCommand(uint32_t cmdId);
  void SetSubCommand(uint32_t cmdId, uint32_t subCmdIndex);
  void ResetSubCommand();
  void ResetSubCommand(uint32_t cmdId);
  void ResetSubCommand(uint32_t cmdId, uint32_t subCmdIndex);

  bool CheckSubCommand(uint32_t cmdId, uint32_t subCmdIndex);

  void DescribeSelectorCommandRules(std::string* str);

  // process acl command op, and sub command
  pstd::Status SetCommandOp(const std::string& op, bool allow);

  // when modify command, do update Selector commandRule string
  void UpdateCommonRule(const std::string& rule, bool allow);

  // remove rule string from Selector commandRule
  void RemoveCommonRule(const std::string& rule);

  // clean commandRule
  void CleanCommandRule();

  bool CheckKey(const std::string& key, const uint32_t cmdFlag);

  bool CheckChannel(const std::string& key, bool isPattern);

  uint32_t flags_;  // See SELECTOR_FLAG_*

  /* The bit in allowed_commands is set if this user has the right to
   * execute this command.*/
  std::bitset<USER_COMMAND_BITS_COUNT> allowedCommands_;

  // record subcommands，key is commandId，value subCommand bit index
  std::map<uint32_t, uint32_t> subCommand_;

  /* A list of allowed key patterns. If this field is empty the user cannot mention any key in a command,
   * unless the flag ALLKEYS is set in the user. */
  std::list<std::shared_ptr<AclKeyPattern>> patterns_;

  /* A list of allowed Pub/Sub channel patterns. If this field is empty the user cannot mention any
   * channel in a `PUBLISH` or [P][UNSUBSCRIBE] command, unless the flag ALLCHANNELS is set in the user. */
  std::list<std::string> channels_;

  /* A string representation of the ordered categories and commands, this
   * is used to regenerate the original ACL string for display.
   */
  std::string commandRules_;
};

// acl user
class User {
  friend Acl;

 public:
  User() = delete;
  explicit User(std::string name);
  explicit User(const User& user);
  ~User() = default;

  std::string Name() const;

  //  inline uint32_t Flags() const { return flags_; };
  inline bool HasFlags(uint32_t flag) const { return flags_ & flag; };
  inline void AddFlags(uint32_t flag) { flags_ |= flag; };
  inline void DecFlags(uint32_t flag) { flags_ &= ~flag; };

  void CleanAclString();

  /**
   * store a password
   * A lock is required before the call
   * @param password
   */
  void AddPassword(const std::string& password);

  /**
   * delete a stored password
   * A lock is required before the call
   * @param password
   */
  void RemovePassword(const std::string& password);

  // clean the user password
  // A lock is required before the call
  void CleanPassword();

  // Add a selector to the user
  // A lock is required before the call
  void AddSelector(const std::shared_ptr<AclSelector>& selector);

  // Set rule for user based on given parameters
  // Use this function to handle it because it allows locking specified users
  pstd::Status SetUser(const std::vector<std::string>& rules);

  // Set the user rule with the given string
  // A lock is required before the call
  pstd::Status SetUser(const std::string& op);

  pstd::Status CreateSelectorFromOpSet(const std::string& opSet);

  // Get the user default selector
  // A lock is required before the call
  std::shared_ptr<AclSelector> GetRootSelector();

  void DescribeUser(std::string* str);

  // match the user password, when do auth,
  // if match,return true, else return false
  bool MatchPassword(const std::string& password);

  // handle Cmd Acl|get
  void GetUserDescribe(CmdRes* res);

  // Get the user Channel key
  // A lock is required before the call
  std::vector<std::string> AllChannelKey();

  // check the user can exec the cmd
  AclDeniedCmd CheckUserPermission(std::shared_ptr<Cmd>& cmd, const PikaCmdArgsType& argv, int8_t& subCmdIndex,
                                   std::string* errKey);

 private:
  mutable std::shared_mutex mutex_;

  const std::string name_;  // The username

  std::atomic<uint32_t> flags_ = static_cast<uint32_t>(AclUserFlag::DISABLED);  // See USER_FLAG_*

  std::set<std::string> passwords_;  // passwords for this user

  std::list<std::shared_ptr<AclSelector>> selectors_; /* A set of selectors this user validates commands
                        against. This list will always contain at least
                        one selector for backwards compatibility. */

  std::string aclString_; /* cached string represent of ACLs */
};

class Acl {
  friend User;
  friend AclSelector;

 public:
  explicit Acl() = default;
  ~Acl() = default;

  /**
   * Initialization all acl
   * @return
   */
  pstd::Status Initialization();

  /**
   * create acl default user
   * @return
   */
  std::shared_ptr<User> CreateDefaultUser();

  std::shared_ptr<User> CreatedUser(const std::string& name);

  /**
   * Set user properties according to the string "op".
   * @param op acl rule string
   */
  pstd::Status SetUser(const std::string& userName, std::vector<std::string>& op);

  /**
   * get user from users_ map
   * @param userName
   * @return
   */
  std::shared_ptr<User> GetUser(const std::string& userName);

  std::shared_ptr<User> GetUserLock(const std::string& userName);

  /**
   * store a user to users_ map
   * @param user
   */
  void AddUser(const std::shared_ptr<User>& user);

  void AddUserLock(const std::shared_ptr<User>& user);

  // bo user auth, pass not is sha256
  std::shared_ptr<User> Auth(const std::string& userName, const std::string& password);

  // get all user
  std::vector<std::string> Users();

  void DescribeAllUser(std::vector<std::string>* content);

  // save acl rule to file
  pstd::Status SaveToFile();

  // delete a user from users
  std::set<std::string> DeleteUser(const std::vector<std::string>& userNames);

  // reload User from acl file, whe exec acl|load command
  pstd::Status LoadUserFromFile(std::set<std::string>* toUnAuthUsers);

  void UpdateDefaultUserPassword(const std::string& pass);

  // After the user channel is modified, determine whether the current channel needs to be disconnected
  void KillPubsubClientsIfNeeded(const std::shared_ptr<User>& origin, const std::shared_ptr<User>& newUser);

  // check the user can be exec the command, after exec command
  //  bool CheckUserCanExec(const std::shared_ptr<Cmd>& cmd, const PikaCmdArgsType& argv);

  // Gets the value of the classification based on the cmd classification name
  static uint32_t GetCommandCategoryFlagByName(const std::string& name);

  // Obtain the corresponding name based on category
  static std::string GetCommandCategoryFlagByName(const uint32_t category);

  static std::vector<std::string> GetAllCategoryName();

  static const std::string DefaultUser;
  static const int64_t LogGroupingMaxTimeDelta;

  // Adds a new entry in the ACL log, making sure to delete the old entry
  // if we reach the maximum length allowed for the log.
  void AddLogEntry(int32_t reason, int32_t context, const std::string& username, const std::string& object,
                   const std::string& cInfo);

  void GetLog(long count, CmdRes* res);
  void ResetLog();

 private:
  /**
   * This function is called once the server is already running,we are ready to start,
   * in order to load the ACLs either from the pending list of users defined in redis.conf,
   * or from the ACL file.The function will just exit with an error if the user is trying to mix
   * both the loading methods.
   */
  pstd::Status LoadUsersAtStartup();

  /**
   * Loads the ACL from the specified filename: every line
   * is validated and should be either empty or in the format used to specify
   * users in the pika.conf configuration or in the ACL file, that is:
   *
   *  user <username> ... rules ...
   *
   * @param users pika.conf users rule
   */
  pstd::Status LoadUserConfigured(std::vector<std::string>& users);

  /**
   * Load ACL from acl rule file
   * @param fileName file full name
   */
  pstd::Status LoadUserFromFile(const std::string& fileName);

  void ACLMergeSelectorArguments(std::vector<std::string>& argv, std::vector<std::string>* merged);
  mutable std::shared_mutex mutex_;

  static std::array<std::pair<std::string, uint32_t>, 21> CommandCategories;

  static std::array<std::pair<std::string, uint32_t>, 3> UserFlags;

  static std::array<std::pair<std::string, uint32_t>, 3> SelectorFlags;

  std::map<std::string, std::shared_ptr<User>> users_;

  std::list<std::unique_ptr<ACLLogEntry>> logEntries_;
};

#endif  // PIKA_ACL_H
