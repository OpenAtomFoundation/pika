// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <fmt/format.h>
#include <cstring>
#include <fstream>
#include <shared_mutex>

#include "include/acl.h"
#include "include/pika_cmd_table_manager.h"
#include "include/pika_server.h"
#include "pstd_defer.h"
#include "pstd_hash.h"

extern PikaServer* g_pika_server;

extern std::unique_ptr<PikaCmdTableManager> g_pika_cmd_table_manager;

// class User
User::User(std::string name) : name_(std::move(name)) {
  selectors_.emplace_back(std::make_shared<AclSelector>(static_cast<uint32_t>(AclSelectorFlag::ROOT)));
}

User::User(const User& user) : name_(user.Name()) {
  flags_ = user.flags_.load();
  passwords_ = user.passwords_;
  aclString_ = user.aclString_;
  for (const auto& item : user.selectors_) {
    selectors_.emplace_back(std::make_shared<AclSelector>(*item));
  }
}

std::string User::Name() const { return name_; }

void User::CleanAclString() { aclString_.clear(); }

void User::AddPassword(const std::string& password) { passwords_.insert(password); }

void User::RemovePassword(const std::string& password) { passwords_.erase(password); }

void User::CleanPassword() { passwords_.clear(); }

void User::AddSelector(const std::shared_ptr<AclSelector>& selector) { selectors_.push_back(selector); }

pstd::Status User::SetUser(const std::vector<std::string>& rules) {
  std::unique_lock wl(mutex_);

  for (const auto& rule : rules) {
    auto status = SetUser(rule);
    if (!status.ok()) {
      LOG(ERROR) << "SetUser rule:" << rule << status.ToString();
      return status;
    }
  }

  return pstd::Status::OK();
}

pstd::Status User::SetUser(const std::string& op) {
  CleanAclString();
  if (op.empty()) {
    return pstd::Status::OK();
  }
  if (!strcasecmp(op.data(), "on")) {
    AddFlags(static_cast<uint32_t>(AclUserFlag::ENABLED));
    DecFlags(static_cast<uint32_t>(AclUserFlag::DISABLED));
  } else if (!strcasecmp(op.data(), "off")) {
    AddFlags(static_cast<uint32_t>(AclUserFlag::DISABLED));
    DecFlags(static_cast<uint32_t>(AclUserFlag::ENABLED));
  } else if (!strcasecmp(op.data(), "nopass")) {
    AddFlags(static_cast<uint32_t>(AclUserFlag::NO_PASS));
    CleanPassword();
  } else if (!strcasecmp(op.data(), "resetpass")) {
    DecFlags(static_cast<uint32_t>(AclUserFlag::NO_PASS));
    CleanPassword();
  } else if (op[0] == '>' || op[0] == '#') {
    std::string newpass;
    if (op[0] == '>') {
      newpass = pstd::sha256(op.data() + 1);
    } else {
      if (!pstd::isSha256(op.data() + 1)) {
        return pstd::Status::Error("password not sha256");
      }
      newpass = op.data() + 1;
    }
    AddPassword(newpass);
    DecFlags(static_cast<uint32_t>(AclUserFlag::NO_PASS));
  } else if (op[0] == '<' || op[0] == '!') {
    std::string delpass;
    if (op[0] == '<') {
      delpass = pstd::sha256(op.data() + 1);
    } else {
      if (!pstd::isSha256(op.data() + 1)) {
        return pstd::Status::Error("password not sha256");
      }
      delpass = op.data() + 1;
    }
    //    passwords_.erase(delpass);
    RemovePassword(delpass);
  } else if (op[0] == '(' && op[op.size() - 1] == ')') {
    auto status = CreateSelectorFromOpSet(op);
    if (!status.ok()) {
      return status;
    }
  } else if (!strcasecmp(op.data(), "clearselectors")) {
    selectors_.clear();
    return pstd::Status::OK();
  } else if (!strcasecmp(op.data(), "reset")) {
    auto status = SetUser("resetpass");
    if (!status.ok()) {
      return status;
    }
    status = SetUser("resetkeys");
    if (!status.ok()) {
      return status;
    }
    status = SetUser("resetchannels");
    if (!status.ok()) {
      return status;
    }
    if (g_pika_conf->acl_pubsub_default() & static_cast<uint32_t>(AclSelectorFlag::ALL_CHANNELS)) {
      status = SetUser("allchannels");
      if (!status.ok()) {
        return status;
      }
    }
    status = SetUser("off");
    if (!status.ok()) {
      return status;
    }
    status = SetUser("-@all");
    if (!status.ok()) {
      return status;
    }
  } else {
    auto root = GetRootSelector();
    if (!root) {  // does not appear under normal circumstances
      LOG(ERROR) << "set user:" << Name() << " not find root selector";
      return pstd::Status::Error("set user error,See pika log for details");
    }
    auto status = root->SetSelector(op);
    if (!status.ok()) {
      return status;
    }
  }

  return pstd::Status::OK();
}

pstd::Status User::CreateSelectorFromOpSet(const std::string& opSet) {
  auto selector = std::make_shared<AclSelector>();
  auto status = selector->SetSelectorFromOpSet(opSet);
  if (!status.ok()) {
    return status;
  }
  AddSelector(selector);
  return status;
}

std::shared_ptr<AclSelector> User::GetRootSelector() {
  for (const auto& item : selectors_) {
    if (item->HasFlags(static_cast<uint32_t>(AclSelectorFlag::ROOT))) {
      return item;
    }
  }
  return nullptr;
}

void User::DescribeUser(std::string* str) {
  std::unique_lock wl(mutex_);

  if (!aclString_.empty()) {
    str->append(aclString_);
    return;
  }

  // flag
  for (const auto& item : Acl::UserFlags) {
    if (HasFlags(item.second)) {
      aclString_ += " ";
      aclString_ += item.first;
    }
  }

  // password
  for (const auto& item : passwords_) {
    aclString_ += " #" + item;
  }

  // selector
  std::string selectorStr;
  for (const auto& item : selectors_) {
    selectorStr.clear();
    item->ACLDescribeSelector(&selectorStr);

    if (item->HasFlags(static_cast<uint32_t>(AclSelectorFlag::ROOT))) {
      aclString_ += selectorStr;
    } else {
      aclString_ += fmt::format(" ({})", selectorStr.data() + 1);
    }
  }

  str->append(aclString_);
}

bool User::MatchPassword(const std::string& password) {
  std::shared_lock l(mutex_);
  return passwords_.find(password) != passwords_.end();
}

void User::GetUserDescribe(CmdRes* res) {
  std::shared_lock l(mutex_);

  res->AppendArrayLen(12);

  res->AppendString("flags");
  std::vector<std::string> vector;
  for (const auto& item : Acl::UserFlags) {
    if (HasFlags(item.second)) {
      vector.emplace_back(item.first);
    }
  }
  res->AppendStringVector(vector);

  vector.clear();
  res->AppendString("passwords");
  for (const auto& item : passwords_) {
    vector.emplace_back(item);
  }
  res->AppendStringVector(vector);

  size_t i = 0;
  for (const auto& selector : selectors_) {
    vector.clear();
    if (i == 0) {  // root selector
      selector->ACLDescribeSelector(vector);
      for (const auto& item : vector) {
        res->AppendString(item);
      }

      res->AppendString("selectors");
      if (selectors_.size() == 1) {
        res->AppendArrayLen(0);
      }
      ++i;
      continue;
    }
    if (i == 1) {
      res->AppendArrayLen(static_cast<int64_t>(selectors_.size()) - 1);
    }
    selector->ACLDescribeSelector(vector);
    res->AppendStringVector(vector);
    ++i;
  }
}

AclDeniedCmd User::CheckUserPermission(std::shared_ptr<Cmd>& cmd, const PikaCmdArgsType& argv, int8_t& subCmdIndex,
                                       std::string* errKey) {
  std::shared_lock l(mutex_);

  subCmdIndex = -1;
  if (cmd->HasSubCommand()) {
    subCmdIndex = cmd->SubCmdIndex(argv[1]);
    if (subCmdIndex < 0) {
      return AclDeniedCmd::NO_SUB_CMD;
    }
  }
  auto keys = cmd->current_key();
  AclDeniedCmd res = AclDeniedCmd::OK;
  for (const auto& selector : selectors_) {
    res = selector->CheckCanExecCmd(cmd, subCmdIndex, keys, errKey);
    if (res == AclDeniedCmd::OK) {
      return AclDeniedCmd::OK;
    }
  }
  return res;
}

std::vector<std::string> User::AllChannelKey() {
  std::vector<std::string> result;
  for (const auto& selector : selectors_) {
    for (const auto& item : selector->channels_) {
      result.emplace_back(item);
    }
  }
  return result;
}
// class User end

// class Acl
pstd::Status Acl::Initialization() {
  AddUser(CreateDefaultUser());
  UpdateDefaultUserPassword(g_pika_conf->requirepass());
  auto status = LoadUsersAtStartup();
  if (!status.ok()) {
    return status;
  }
  return status;
}

std::shared_ptr<User> Acl::GetUser(const std::string& userName) {
  auto u = users_.find(userName);
  if (u == users_.end()) {
    return nullptr;
  }
  return u->second;
}

std::shared_ptr<User> Acl::GetUserLock(const std::string& userName) {
  std::shared_lock rl(mutex_);
  auto u = users_.find(userName);
  if (u == users_.end()) {
    return nullptr;
  }
  return u->second;
}

void Acl::AddUser(const std::shared_ptr<User>& user) { users_[user->Name()] = user; }

void Acl::AddUserLock(const std::shared_ptr<User>& user) {
  std::unique_lock wl(mutex_);
  users_[user->Name()] = user;
}

pstd::Status Acl::LoadUsersAtStartup() {
  if (!g_pika_conf->users().empty() && !g_pika_conf->acl_file().empty()) {
    return pstd::Status::NotSupported("Only one configuration file and acl file can be used", "");
  }

  if (g_pika_conf->users().empty()) {
    return LoadUserFromFile(g_pika_conf->acl_file());
  } else {
    return LoadUserConfigured(g_pika_conf->users());
  }
}

pstd::Status Acl::LoadUserConfigured(std::vector<std::string>& users) {
  std::vector<std::string> userRules;
  for (const auto& item : users) {
    userRules.clear();
    pstd::StringSplit(item, ' ', userRules);
    if (userRules.size() < 2) {
      return pstd::Status::Error("acl from configuration file read rules error");
    }
    auto user = GetUser(userRules[0]);
    if (user) {
      if (user->Name() != DefaultUser) {  // only `default` users are allowed to repeat
        return pstd::Status::Error("acl user: " + user->Name() + " is repeated");
      } else {
        user->SetUser("reset");
      }
    } else {
      user = CreatedUser(userRules[0]);
    }
    std::vector<std::string> aclArgc;
    auto subRule = std::vector<std::string>(userRules.begin() + 1, userRules.end());
    ACLMergeSelectorArguments(subRule, &aclArgc);

    for (const auto& rule : aclArgc) {
      auto status = user->SetUser(rule);
      if (!status.ok()) {
        LOG(ERROR) << "load user from configured file error," << status.ToString();
        return status;
      }
    }
    AddUser(user);
  }

  return pstd::Status().OK();
}

pstd::Status Acl::LoadUserFromFile(std::set<std::string>* toUnAuthUsers) {
  std::unique_lock wl(mutex_);

  for (const auto& item : users_) {
    if (item.first != DefaultUser) {
      toUnAuthUsers->insert(item.first);
    }
  }

  auto status = LoadUserFromFile(g_pika_conf->acl_file());
  if (!status.ok()) {
    return status;
  }

  return status;
}

pstd::Status Acl::LoadUserFromFile(const std::string& fileName) {
  if (fileName.empty()) {
    return pstd::Status::OK();
  }

  std::map<std::string, std::shared_ptr<User>> users;
  std::vector<std::string> rules;

  bool hasDefaultUser = false;

  std::ifstream ruleFile(fileName);
  if (!ruleFile) {
    return pstd::Status::IOError(fmt::format("open file {} fail"), fileName);
  }

  DEFER { ruleFile.close(); };

  int lineNum = 0;
  std::string lineContent;
  while (std::getline(ruleFile, lineContent)) {
    ++lineNum;
    if (lineContent.empty()) {
      continue;
    }

    lineContent = pstd::StringTrim(lineContent, "\r\n ");
    rules.clear();
    pstd::StringSplit(lineContent, ' ', rules);
    if (rules.empty()) {
      continue;
    }

    if (rules[0] != "user" || rules.size() < 2) {
      LOG(ERROR) << fmt::format("load user from acl file,line:{} '{}' illegal", lineNum, lineContent);
      return pstd::Status::Error(fmt::format("line:{} '{}' illegal", lineNum, lineContent));
    }

    auto user = users.find(rules[1]);
    if (user != users.end()) {
      // if user is exists, exit
      auto err = fmt::format("Duplicate user '{}' found on line {}.", rules[1], lineNum);
      LOG(ERROR) << err;
      return pstd::Status::Error(err);
    }

    std::vector<std::string> aclArgc;
    auto subRule = std::vector<std::string>(rules.begin() + 2, rules.end());
    ACLMergeSelectorArguments(subRule, &aclArgc);

    auto u = CreatedUser(rules[1]);
    for (const auto& item : aclArgc) {
      auto status = u->SetUser(item);
      if (!status.ok()) {
        LOG(ERROR) << "load user from acl file error," << status.ToString();
        return status;
      }
    }
    if (rules[1] == DefaultUser) {
      hasDefaultUser = true;
    }
    users[rules[1]] = u;
  }

  if (!hasDefaultUser) {
    users[DefaultUser] = GetUser(DefaultUser);
  }

  users_ = std::move(users);

  return pstd::Status().OK();
}

void Acl::UpdateDefaultUserPassword(const std::string& pass) {
  std::unique_lock wl(mutex_);
  auto u = GetUser(DefaultUser);
  u->SetUser("resetpass");
  if (pass.empty()) {
    u->SetUser("nopass");
  } else {
    u->SetUser(">" + pass);
  }
}

// bool Acl::CheckUserCanExec(const std::shared_ptr<Cmd>& cmd, const PikaCmdArgsType& argv) { cmd->name(); }

std::shared_ptr<User> Acl::CreateDefaultUser() {
  auto defaultUser = std::make_shared<User>(DefaultUser);
  defaultUser->SetUser("+@all");
  defaultUser->SetUser("~*");
  defaultUser->SetUser("&*");
  defaultUser->SetUser("on");
  defaultUser->SetUser("nopass");
  return defaultUser;
}

std::shared_ptr<User> Acl::CreatedUser(const std::string& name) { return std::make_shared<User>(name); }

pstd::Status Acl::SetUser(const std::string& userName, std::vector<std::string>& op) {
  auto user = GetUserLock(userName);

  std::shared_ptr<User> tempUser = nullptr;
  bool add = false;
  if (!user) {  // if the user not exist, create new user
    user = CreatedUser(userName);
    add = true;
  } else {
    tempUser = std::make_shared<User>(*user);
  }

  std::vector<std::string> aclArgc;
  ACLMergeSelectorArguments(op, &aclArgc);

  auto status = user->SetUser(aclArgc);
  if (!status.ok()) {
    return status;
  }

  if (add) {
    AddUserLock(user);
  } else {
    KillPubsubClientsIfNeeded(tempUser, user);
  }
  return pstd::Status::OK();
}

void Acl::KillPubsubClientsIfNeeded(const std::shared_ptr<User>& origin, const std::shared_ptr<User>& newUser) {
  std::shared_lock l(mutex_);
  bool match = true;
  for (const auto& newUserSelector : newUser->selectors_) {
    if (newUserSelector->HasFlags(static_cast<uint32_t>(AclSelectorFlag::ALL_CHANNELS))) {  // new user has all channels
      return;
    }
  }
  auto newChKey = newUser->AllChannelKey();

  for (const auto& selector : origin->selectors_) {
    if (selector->HasFlags(static_cast<uint32_t>(AclSelectorFlag::ALL_CHANNELS))) {
      match = false;
      break;
    }
    if (!selector->EqualChannel(newChKey)) {
      match = false;
      break;
    }
  }
  if (match) {
    return;
  }
  g_pika_server->CheckPubsubClientKill(newUser->Name(), newChKey);
}

uint32_t Acl::GetCommandCategoryFlagByName(const std::string& name) {
  for (const auto& item : CommandCategories) {
    if (item.first == name) {
      return item.second;
    }
  }
  return 0;
}

std::string Acl::GetCommandCategoryFlagByName(const uint32_t category) {
  for (const auto& item : CommandCategories) {
    if (item.second == category) {
      return item.first;
    }
  }

  return "";
}

std::vector<std::string> Acl::GetAllCategoryName() {
  std::vector<std::string> result;
  result.reserve(CommandCategories.size());
  for (const auto& item : CommandCategories) {
    result.emplace_back(item.first);
  }
  return result;
}

void Acl::ACLMergeSelectorArguments(std::vector<std::string>& argv, std::vector<std::string>* merged) {
  bool openBracketStart = false;
  std::string selector;
  for (const auto& item : argv) {
    if (item[0] == '(' && item[item.size() - 1] != ')') {
      selector = item;
      openBracketStart = true;
      continue;
    }

    if (openBracketStart) {
      selector += " " + item;
      if (item[item.size() - 1] == ')') {
        openBracketStart = false;
        merged->emplace_back(selector);
      }
      continue;
    }

    merged->emplace_back(item);
  }
}

std::shared_ptr<User> Acl::Auth(const std::string& userName, const std::string& password) {
  std::shared_lock l(mutex_);

  auto user = GetUser(userName);
  if (!user) {
    return nullptr;
  }
  if (user->HasFlags(static_cast<uint32_t>(AclUserFlag::DISABLED))) {
    return nullptr;
  }

  if (user->HasFlags(static_cast<uint32_t>(AclUserFlag::NO_PASS))) {
    return user;
  }

  if (user->MatchPassword(pstd::sha256(password))) {
    return user;
  }
  return nullptr;
}

std::vector<std::string> Acl::Users() {
  std::shared_lock l(mutex_);
  std::vector<std::string> result;
  result.reserve(users_.size());

  for (const auto& item : users_) {
    result.emplace_back(item.first);
  }

  return result;
}

void Acl::DescribeAllUser(std::vector<std::string>* content) {
  std::shared_lock l(mutex_);
  content->reserve(users_.size());

  for (const auto& item : users_) {
    std::string saveContent;
    saveContent += "user ";
    saveContent += item.first;

    item.second->DescribeUser(&saveContent);
    content->emplace_back(saveContent);
  }
}

pstd::Status Acl::SaveToFile() {
  std::string aclFileName = g_pika_conf->acl_file();
  if (aclFileName.empty()) {
    LOG(ERROR) << "save user to acl file, file name is empty";
    return pstd::Status::Error("acl file name is empty");
  }

  std::unique_lock wl(mutex_);

  std::unique_ptr<pstd::WritableFile> file;
  const std::string tmpFile = aclFileName + ".tmp";
  auto status = pstd::NewWritableFile(tmpFile, file);
  if (!status.ok()) {
    auto error = fmt::format("open acl user file:{} fail, error:{}", aclFileName, status.ToString());
    LOG(ERROR) << error;
    return pstd::Status::Error(error);
  }

  std::string saveContent;
  for (const auto& item : users_) {
    saveContent += "user ";
    saveContent += item.first;

    item.second->DescribeUser(&saveContent);
    saveContent += "\n";
  }

  file->Append(saveContent);
  file->Sync();
  file->Close();

  if (pstd::RenameFile(tmpFile, aclFileName) < 0) {  // rename fail
    return pstd::Status::Error("save acl rule to file fail. specific information see pika log");
  }
  return pstd::Status::OK();
}

std::set<std::string> Acl::DeleteUser(const std::vector<std::string>& userNames) {
  std::unique_lock wl(mutex_);

  std::set<std::string> delUserNames;
  for (const auto& userName : userNames) {
    if (users_.erase(userName)) {
      delUserNames.insert(userName);
    }
  }

  return delUserNames;
}

std::array<std::pair<std::string, uint32_t>, 21> Acl::CommandCategories = {{
    {"keyspace", static_cast<uint32_t>(AclCategory::KEYSPACE)},
    {"read", static_cast<uint32_t>(AclCategory::READ)},
    {"write", static_cast<uint32_t>(AclCategory::WRITE)},
    {"set", static_cast<uint32_t>(AclCategory::SET)},
    {"sortedset", static_cast<uint32_t>(AclCategory::SORTEDSET)},
    {"list", static_cast<uint32_t>(AclCategory::LIST)},
    {"hash", static_cast<uint32_t>(AclCategory::HASH)},
    {"string", static_cast<uint32_t>(AclCategory::STRING)},
    {"bitmap", static_cast<uint32_t>(AclCategory::BITMAP)},
    {"hyperloglog", static_cast<uint32_t>(AclCategory::HYPERLOGLOG)},
    {"geo", static_cast<uint32_t>(AclCategory::GEO)},
    {"stream", static_cast<uint32_t>(AclCategory::STREAM)},
    {"pubsub", static_cast<uint32_t>(AclCategory::PUBSUB)},
    {"admin", static_cast<uint32_t>(AclCategory::ADMIN)},
    {"fast", static_cast<uint32_t>(AclCategory::FAST)},
    {"slow", static_cast<uint32_t>(AclCategory::SLOW)},
    {"blocking", static_cast<uint32_t>(AclCategory::BLOCKING)},
    {"dangerous", static_cast<uint32_t>(AclCategory::DANGEROUS)},
    {"connection", static_cast<uint32_t>(AclCategory::CONNECTION)},
    {"transaction", static_cast<uint32_t>(AclCategory::TRANSACTION)},
    {"scripting", static_cast<uint32_t>(AclCategory::SCRIPTING)},
}};

std::array<std::pair<std::string, uint32_t>, 3> Acl::UserFlags = {{
    {"on", static_cast<uint32_t>(AclUserFlag::ENABLED)},
    {"off", static_cast<uint32_t>(AclUserFlag::DISABLED)},
    {"nopass", static_cast<uint32_t>(AclUserFlag::NO_PASS)},
}};

std::array<std::pair<std::string, uint32_t>, 3> Acl::SelectorFlags = {{
    {"allkeys", static_cast<uint32_t>(AclSelectorFlag::ALL_KEYS)},
    {"allchannels", static_cast<uint32_t>(AclSelectorFlag::ALL_CHANNELS)},
    {"allcommands", static_cast<uint32_t>(AclSelectorFlag::ALL_COMMANDS)},
}};

const std::string Acl::DefaultUser = "default";
const int64_t Acl::LogGroupingMaxTimeDelta = 60000;

void Acl::AddLogEntry(int32_t reason, int32_t context, const std::string& username, const std::string& object,
                      const std::string& cInfo) {
  int64_t nowUnix =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
          .count();
  {
    std::unique_lock wl(mutex_);
    for (const auto& item : logEntries_) {
      if (item->Match(reason, context, nowUnix, object, username)) {
        item->AddEntry(cInfo, nowUnix);
        return;
      }
    }
    auto entry = std::make_unique<ACLLogEntry>(reason, context, object, username, nowUnix, cInfo);
    logEntries_.push_front(std::move(entry));

    auto maxLen = g_pika_conf->acl_log_max_len();
    if (logEntries_.size() > maxLen) {  // remove overflow log
      if (maxLen == 0) {
        logEntries_.clear();
      } else {
        logEntries_.erase(std::next(logEntries_.begin(), maxLen), logEntries_.end());
      }
    }
  }
}

void Acl::GetLog(long count, CmdRes* res) {
  std::shared_lock rl(mutex_);
  auto size = static_cast<long>(logEntries_.size());
  if (count == -1) {
    count = size;
  }
  if (count > size) {
    count = size;
  }
  if (count == 0) {
    res->AppendArrayLen(0);
    return;
  }

  std::vector<std::string> items;
  res->AppendArrayLen(static_cast<int64_t>(count));
  items.reserve(14);
  for (const auto& item : logEntries_) {
    items.clear();
    item->GetReplyInfo(&items);
    res->AppendStringVector(items);
    count--;
    if (count == 0) {
      break;
    }
  }
}

void Acl::ResetLog() {
  std::unique_lock wl(mutex_);
  logEntries_.clear();
}
// class Acl end

// class ACLLogEntry
bool ACLLogEntry::Match(int32_t reason, int32_t context, int64_t ctime, const std::string& object,
                        const std::string& username) {
  if (reason_ != reason) {
    return false;
  }
  if (context_ != context) {
    return false;
  }
  auto delta = ctime_ - ctime;
  if (delta > Acl::LogGroupingMaxTimeDelta) {
    return false;
  };
  if (object_ != object) {
    return false;
  }
  if (username_ != username) {
    return false;
  }
  return true;
}

void ACLLogEntry::AddEntry(const std::string& cinfo, u_int64_t ctime) {
  cinfo_ = cinfo;
  ctime_ = ctime;
  ++count_;
}

void ACLLogEntry::GetReplyInfo(std::vector<std::string>* vector) {
  vector->emplace_back("count");
  vector->emplace_back(std::to_string(count_));
  vector->emplace_back("reason");
  switch (reason_) {
    case static_cast<int32_t>(AclDeniedCmd::CMD):
      vector->emplace_back("command");
      break;
    case static_cast<int32_t>(AclDeniedCmd::KEY):
      vector->emplace_back("key");
      break;
    case static_cast<int32_t>(AclDeniedCmd::CHANNEL):
      vector->emplace_back("channel");
      break;
    case static_cast<int32_t>(AclDeniedCmd::NO_AUTH):
      vector->emplace_back("auth");
      break;
    default:
      vector->emplace_back("unknown");
      break;
  }

  vector->emplace_back("context");
  switch (context_) {
    case static_cast<int32_t>(AclLogCtx::TOPLEVEL):
      vector->emplace_back("toplevel");
      break;
    case static_cast<int32_t>(AclLogCtx::MULTI):
      vector->emplace_back("multi");
      break;
    case static_cast<int32_t>(AclLogCtx::LUA):
      vector->emplace_back("lua");
      break;
    default:
      vector->emplace_back("unknown");
      break;
  }

  vector->emplace_back("object");
  vector->emplace_back(object_);
  vector->emplace_back("username");
  vector->emplace_back(username_);
  vector->emplace_back("age-seconds");
  int64_t nowUnix =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
          .count();

  char latitude[32];
  pstd::d2string(latitude, 32, static_cast<double>(nowUnix - ctime_) / 1000);
  vector->emplace_back(latitude);
  vector->emplace_back("client-info");
  vector->emplace_back(cinfo_);
}

// class ACLLogEntry end

// class AclSelector
AclSelector::AclSelector(uint32_t flag) : flags_(flag) {
  if (g_pika_conf->acl_pubsub_default()) {
    AddFlags(static_cast<uint32_t>(AclSelectorFlag::ALL_CHANNELS));
  }
}

AclSelector::AclSelector(const AclSelector& selector) {
  flags_ = selector.Flags();
  allowedCommands_ = selector.allowedCommands_;
  subCommand_ = selector.subCommand_;
  channels_ = selector.channels_;
  commandRules_ = selector.commandRules_;

  for (const auto& item : selector.patterns_) {
    auto pattern = std::make_shared<AclKeyPattern>();
    pattern->flags = item->flags;
    pattern->pattern = item->pattern;
    patterns_.emplace_back(pattern);
  }
}

pstd::Status AclSelector::SetSelector(const std::string& op) {
  if (!strcasecmp(op.data(), "allkeys") || op == "~*") {
    AddFlags(static_cast<uint32_t>(AclSelectorFlag::ALL_KEYS));
    patterns_.clear();
  } else if (!strcasecmp(op.data(), "resetkeys")) {
    DecFlags(static_cast<uint32_t>(AclSelectorFlag::ALL_KEYS));
    patterns_.clear();
  } else if (!strcasecmp(op.data(), "allchannels") || !strcasecmp(op.data(), "&*")) {
    AddFlags(static_cast<uint32_t>(AclSelectorFlag::ALL_CHANNELS));
    channels_.clear();
  } else if (!strcasecmp(op.data(), "resetchannels")) {
    DecFlags(static_cast<uint32_t>(AclSelectorFlag::ALL_CHANNELS));
    channels_.clear();
  } else if (!strcasecmp(op.data(), "allcommands") || !strcasecmp(op.data(), "+@all")) {
    SetAllCommandSelector();
  } else if (!strcasecmp(op.data(), "nocommands") || !strcasecmp(op.data(), "-@all")) {
    RestAllCommandSelector();
  } else if (op[0] == '~' || op[0] == '%') {
    if (HasFlags(static_cast<int>(AclSelectorFlag::ALL_KEYS))) {
      return pstd::Status::Error(
          fmt::format("Error in ACL SETUSER modifier '{}': Adding a pattern after the * "
                      "pattern (or the 'allkeys' flag) is not valid and does not have any effect."
                      " Try 'resetkeys' to start with an empty list of patterns",
                      op));
    }
    int flags = 0;
    size_t offset = 1;
    if (op[0] == '%') {
      for (; offset < op.size(); offset++) {
        if (toupper(op[offset]) == 'R' && !(flags & static_cast<int>(AclPermission::READ))) {
          flags |= static_cast<int>(AclPermission::READ);
        } else if (toupper(op[offset]) == 'W' && !(flags & static_cast<int>(AclPermission::WRITE))) {
          flags |= static_cast<int>(AclPermission::WRITE);
        } else if (op[offset] == '~') {
          offset++;
          break;
        } else {
          return pstd::Status::Error("Syntax error");
        }
      }
    } else {
      flags = static_cast<int>(AclPermission::ALL);
    }

    if (pstd::isspace(op)) {
      return pstd::Status::Error("Syntax error");
    }

    InsertKeyPattern(op.substr(offset, std::string::npos), flags);
    DecFlags(static_cast<uint32_t>(AclSelectorFlag::ALL_KEYS));
  } else if (op[0] == '&') {
    if (HasFlags(static_cast<uint32_t>(AclSelectorFlag::ALL_CHANNELS))) {
      return pstd::Status::Error(
          "Adding a pattern after the * pattern (or the 'allchannels' flag) is not valid and does not have any effect. "
          "Try 'resetchannels' to start with an empty list of channels");
    }
    if (pstd::isspace(op)) {
      return pstd::Status::Error("Syntax error");
    }
    InsertChannel(op.substr(1, std::string::npos));
    DecFlags(static_cast<uint32_t>(AclSelectorFlag::ALL_CHANNELS));
  } else if (op[0] == '+' && op[1] != '@') {
    auto status = SetCommandOp(op, true);
    if (!status.ok()) {
      return status;
    }
    UpdateCommonRule(op.data() + 1, true);
  } else if (op[0] == '-' && op[1] != '@') {
    auto status = SetCommandOp(op, false);
    if (!status.ok()) {
      return status;
    }
    UpdateCommonRule(op.data() + 1, false);
  } else if ((op[0] == '+' || op[0] == '-') && op[1] == '@') {
    bool allow = op[0] == '+' ? true : false;
    if (!SetSelectorCommandBitsForCategory(op.data() + 1, allow)) {
      return pstd::Status::Error("Unknown command or category name in ACL");
    }
  } else {
    return pstd::Status::Error("Syntax error");
  }
  return pstd::Status();
}

pstd::Status AclSelector::SetSelectorFromOpSet(const std::string& opSet) {
  if (opSet[0] != '(' || opSet[opSet.size() - 1] != ')') {
    return pstd::Status::Error("Unmatched parenthesis in acl selector starting at" + opSet);
  }

  std::vector<std::string> args;
  pstd::StringSplit(opSet.substr(1, opSet.size() - 2), ' ', args);

  for (const auto& item : args) {
    auto status = SetSelector(item);
    if (!status.ok()) {
      return status;
    }
  }
  return pstd::Status().OK();
}

bool AclSelector::SetSelectorCommandBitsForCategory(const std::string& categoryName, bool allow) {
  std::string lowerCategoryName(categoryName);
  std::transform(categoryName.begin(), categoryName.end(), lowerCategoryName.begin(), ::tolower);
  auto category = Acl::GetCommandCategoryFlagByName(lowerCategoryName.data() + 1);
  if (!category) {  // not find category
    return false;
  }
  UpdateCommonRule(categoryName, allow);
  for (const auto& cmd : *g_pika_cmd_table_manager->cmds_) {
    if (cmd.second->AclCategory() & category) {  // this cmd belongs to this category
      ChangeSelector(cmd.second.get(), allow);
    }
  }
  return true;
}

void AclSelector::SetAllCommandSelector() {
  AddFlags(static_cast<uint32_t>(AclSelectorFlag::ALL_COMMANDS));
  allowedCommands_.set();
  for (const auto& cmd : *g_pika_cmd_table_manager->cmds_) {
    if (cmd.second->HasSubCommand()) {
      SetSubCommand(cmd.second->GetCmdId());
    }
  }
  CleanCommandRule();
}

void AclSelector::RestAllCommandSelector() {
  DecFlags(static_cast<uint32_t>(AclSelectorFlag::ALL_COMMANDS));
  allowedCommands_.reset();
  ResetSubCommand();
  CleanCommandRule();
}

void AclSelector::InsertKeyPattern(const std::string& str, uint32_t flags) {
  for (const auto& item : patterns_) {
    if (item->pattern == str) {
      item->flags |= flags;
      return;
    }
  }
  auto pattern = std::make_shared<AclKeyPattern>();
  pattern->flags = flags;
  pattern->pattern = str;
  patterns_.emplace_back(pattern);
  return;
}

void AclSelector::InsertChannel(const std::string& str) {
  for (const auto& item : channels_) {
    if (item == str) {
      return;
    }
  }
  channels_.emplace_back(str);
}

void AclSelector::ChangeSelector(const Cmd* cmd, bool allow) {
  if (allow) {
    allowedCommands_.set(cmd->GetCmdId());
    if (cmd->HasSubCommand()) {
      SetSubCommand(cmd->GetCmdId());
    }
  } else {
    allowedCommands_.reset(cmd->GetCmdId());
    DecFlags(static_cast<uint32_t>(AclSelectorFlag::ALL_COMMANDS));
    if (cmd->HasSubCommand()) {
      ResetSubCommand(cmd->GetCmdId());
    }
  }
}

void AclSelector::ChangeSelector(const std::shared_ptr<Cmd>& cmd, bool allow) { ChangeSelector(cmd.get(), allow); }

pstd::Status AclSelector::ChangeSelector(const std::shared_ptr<Cmd>& cmd, const std::string& subCmd, bool allow) {
  if (cmd->HasSubCommand()) {
    auto index = cmd->SubCmdIndex(subCmd);
    if (index == -1) {
      return pstd::Status::Error("Unknown command or category name in ACL");
    }
    if (allow) {
      SetSubCommand(cmd->GetCmdId(), index);
    } else {
      ResetSubCommand(cmd->GetCmdId(), index);
    }
  }
  return pstd::Status::OK();
}

void AclSelector::SetSubCommand(uint32_t cmdId) { subCommand_[cmdId] = 0xFFFFFFFF; }

void AclSelector::SetSubCommand(uint32_t cmdId, uint32_t subCmdIndex) { subCommand_[cmdId] |= (1 << subCmdIndex); }

void AclSelector::ResetSubCommand() { subCommand_.clear(); }

void AclSelector::ResetSubCommand(uint32_t cmdId) { subCommand_[cmdId] = 0; }

void AclSelector::ResetSubCommand(uint32_t cmdId, uint32_t subCmdIndex) {
  DecFlags(static_cast<uint32_t>(AclSelectorFlag::ALL_COMMANDS));
  subCommand_[cmdId] &= ~(1 << subCmdIndex);
}

bool AclSelector::CheckSubCommand(uint32_t cmdId, uint32_t subCmdIndex) {
  if (subCmdIndex < 0) {
    return false;
  }
  auto bit = subCommand_.find(cmdId);
  if (bit == subCommand_.end()) {
    return false;
  }

  return bit->second & (1 << subCmdIndex);
}

void AclSelector::ACLDescribeSelector(std::string* str) {
  if (HasFlags(static_cast<uint32_t>(AclSelectorFlag::ALL_KEYS))) {
    str->append(" ~*");
  } else {
    for (const auto& item : patterns_) {
      str->append(" ");
      item->ToString(str);
    }
  }

  // Pub/sub channel patterns
  if (HasFlags(static_cast<uint32_t>(AclSelectorFlag::ALL_CHANNELS))) {
    str->append(" &*");
  } else if (channels_.empty()) {
    str->append(" resetchannels");
  } else {
    for (const auto& item : channels_) {
      str->append(" &" + item);
    }
  }

  // Command rules
  DescribeSelectorCommandRules(str);
}

void AclSelector::ACLDescribeSelector(std::vector<std::string>& vector) {
  vector.emplace_back("commands");
  if (allowedCommands_.test(USER_COMMAND_BITS_COUNT - 1)) {
    if (commandRules_.empty()) {
      vector.emplace_back("+@all");
    } else {
      vector.emplace_back("+@all " + commandRules_);
    }
  } else {
    if (commandRules_.empty()) {
      vector.emplace_back("-@all");
    } else {
      vector.emplace_back("-@all " + commandRules_);
    }
  }

  vector.emplace_back("key");
  if (HasFlags(static_cast<uint32_t>(AclSelectorFlag::ALL_KEYS))) {
    vector.emplace_back("~*");
  } else if (patterns_.empty()) {
    vector.emplace_back("");
  } else {
    std::string keys;
    for (auto it = patterns_.begin(); it != patterns_.end(); ++it) {
      if (it != patterns_.begin()) {
        keys += " ";
        (*it)->ToString(&keys);
      }
    }
    vector.emplace_back(keys);
  }

  vector.emplace_back("channels");
  if (HasFlags(static_cast<uint32_t>(AclSelectorFlag::ALL_CHANNELS))) {
    vector.emplace_back("&*");
  } else if (channels_.empty()) {
    vector.emplace_back("");
  } else if (channels_.size() == 1) {
    vector.emplace_back("&" + channels_.front());
  } else {
    vector.emplace_back(fmt::format("{}", fmt::join(channels_, " &")));
  }
}

AclDeniedCmd AclSelector::CheckCanExecCmd(std::shared_ptr<Cmd>& cmd, int8_t subCmdIndex,
                                          const std::vector<std::string>& keys, std::string* errKey) {
  if (!HasFlags(static_cast<uint32_t>(AclSelectorFlag::ALL_COMMANDS)) && !(cmd->flag() & kCmdFlagsNoAuth)) {
    if (subCmdIndex < 0) {
      if (!allowedCommands_.test(cmd->GetCmdId())) {
        return AclDeniedCmd::CMD;
      }
    } else {  // if the command has subCmd
      if (!CheckSubCommand(cmd->GetCmdId(), subCmdIndex)) {
        return AclDeniedCmd::CMD;
      }
    }
  }

  // key match
  if (!HasFlags(static_cast<uint32_t>(AclSelectorFlag::ALL_KEYS)) && !keys.empty() && !cmd->hasFlag(kCmdFlagsPubSub)) {
    for (const auto& key : keys) {
      // if the key is empty, skip, because some command keys for write categories are empty
      if (!key.empty() && !CheckKey(key, cmd->flag())) {
        if (errKey) {
          *errKey = key;
        }
        return AclDeniedCmd::KEY;
      }
    }
  }

  // channel match
  if (!HasFlags(static_cast<uint32_t>(AclSelectorFlag::ALL_CHANNELS)) && cmd->hasFlag(kCmdFlagsPubSub)) {
    bool isPattern = cmd->name() == kCmdNamePSubscribe || cmd->name() == kCmdNamePUnSubscribe;
    for (const auto& key : keys) {
      if (!CheckChannel(key, isPattern)) {
        if (errKey) {
          *errKey = key;
        }
        return AclDeniedCmd::CHANNEL;
      }
    }
  }
  return AclDeniedCmd::OK;
}

bool AclSelector::EqualChannel(const std::vector<std::string>& allChannel) {
  for (const auto& item : channels_) {
    if (std::count(allChannel.begin(), allChannel.end(), item) == 0) {
      return false;
    }
  }
  return true;
}

void AclSelector::DescribeSelectorCommandRules(std::string* str) {
  allowedCommands_.test(USER_COMMAND_BITS_COUNT - 1) ? str->append(" +@all") : str->append(" -@all");

  // Category
  if (!commandRules_.empty()) {
    str->append(" ");
    str->append(commandRules_);
  }
}

pstd::Status AclSelector::SetCommandOp(const std::string& op, bool allow) {
  std::string _op(op.data() + 1);
  pstd::StringToLower(_op);
  if (_op.find('|') == std::string::npos) {
    auto cmd = g_pika_cmd_table_manager->GetCmd(_op);
    if (!cmd) {
      return pstd::Status::Error("Unknown command or category name in ACL");
    }
    ChangeSelector(cmd, allow);
    return pstd::Status::OK();
  } else {
    /* Split the command and subcommand parts. */
    std::vector<std::string> cmds;
    pstd::StringSplit(_op, '|', cmds);

    /* The subcommand cannot be empty, so things like CONFIG|
     * are syntax errors of course. */
    if (cmds.size() != 2) {
      return pstd::Status::Error("Allowing first-arg of a subcommand is not supported");
    }

    auto parentCmd = g_pika_cmd_table_manager->GetCmd(cmds[0]);
    if (!parentCmd) {
      return pstd::Status::Error("Unknown command or category name in ACL");
    }

    return ChangeSelector(parentCmd, cmds[1], allow);

    // not support Redis ACL `first-arg` feature
  }
}

void AclSelector::UpdateCommonRule(const std::string& rule, bool allow) {
  std::string _rule(rule);
  pstd::StringToLower(_rule);
  RemoveCommonRule(_rule);
  if (commandRules_.empty()) {
    commandRules_ += allow ? "+" : "-";
  } else {
    commandRules_ += allow ? " +" : " -";
  }
  commandRules_ += _rule;
}

void AclSelector::RemoveCommonRule(const std::string& rule) {
  if (commandRules_.empty()) {
    return;
  }

  const size_t ruleLen = rule.size();

  size_t start = 0;
  while (true) {
    start = commandRules_.find(rule, start);
    if (start == std::string::npos) {
      return;
    }

    size_t delNum = 0;                              // the length to be deleted this time
    if (start + ruleLen >= commandRules_.size()) {  // the remaining commandRule == rule, delete to end
      delNum = ruleLen;
      --start;
      ++delNum;
    } else {
      if (commandRules_[start + ruleLen] == ' ') {
        delNum = ruleLen + 1;
      } else if (commandRules_[start + ruleLen] == '|') {
        size_t end = commandRules_.find(' ', start);  // find next ' '
        if (end == std::string::npos) {               // not find ' ', delete to end
          delNum = commandRules_.size() - start;
          --start;
          ++delNum;
        } else {
          delNum = end + 1 - start;
        }
      } else {
        start += ruleLen;
        continue;  // not match
      }
    }

    if (start > 0) {  // the rule not included '-'/'+', but need delete need
      --start;
      ++delNum;  // star position moved one forward So delNum takes +1
    }

    commandRules_.erase(start, delNum);
  }
}

void AclSelector::CleanCommandRule() { commandRules_.clear(); }

bool AclSelector::CheckKey(const std::string& key, const uint32_t cmdFlag) {
  uint32_t selectorFlag = 0;
  if (cmdFlag & kCmdFlagsRead) {
    selectorFlag |= static_cast<uint32_t>(AclPermission::READ);
  }
  if (cmdFlag & kCmdFlagsWrite) {
    selectorFlag |= static_cast<uint32_t>(AclPermission::WRITE);
  }
  if ((selectorFlag & static_cast<uint32_t>(AclPermission::WRITE)) &&
      (selectorFlag & static_cast<uint32_t>(AclPermission::READ))) {
    selectorFlag |= static_cast<uint32_t>(AclPermission::ALL);
  }

  for (const auto& item : patterns_) {
    if ((item->flags & selectorFlag) != selectorFlag) {
      continue;
    }

    if (pstd::stringmatchlen(item->pattern.data(), static_cast<int>(item->pattern.size()), key.data(),
                             static_cast<int>(key.size()), 0)) {
      return true;
    }
  }
  return false;
}

bool AclSelector::CheckChannel(const std::string& key, bool isPattern) {
  for (const auto& channel : channels_) {
    if (isPattern ? (channel == key)
                  : (pstd::stringmatchlen(channel.data(), static_cast<int>(channel.size()), key.data(),
                                          static_cast<int>(key.size()), 0))) {
      return true;
    }
  }
  return false;
}
// class AclSelector end