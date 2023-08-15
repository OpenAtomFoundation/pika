// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/acl.h"
#include <fmt/format.h>
#include <cstring>
#include <shared_mutex>

#include "include/pika_cmd_table_manager.h"
#include "include/pika_server.h"
#include "pstd_hash.h"

extern std::unique_ptr<PikaCmdTableManager> g_pika_cmd_table_manager;

// class User

User::User(const std::string& name) : name_(name) {
  selectors_.emplace_back(std::make_shared<AclSelector>(static_cast<uint32_t>(AclSelectorFlag::ROOT)));
}

std::string User::Name() const { return name_; }

void User::CleanAclString(bool lock) {
  if (lock) {
    std::shared_lock rl(mutex_);
  }
  aclString_.clear();
}

void User::AddPassword(const std::string& password) { passwords_.insert(password); }

void User::RemovePassword(const std::string& password) { passwords_.erase(password); }

void User::CleanPassword() { passwords_.clear(); }

void User::AddSelector(const std::shared_ptr<AclSelector>& selector) { selectors_.push_back(selector); }

pstd::Status User::SetUser(const std::string& op, bool look) {
  if (look) {
    std::unique_lock wl(mutex_);
  }

  CleanAclString();
  if (op.empty()) {
    return pstd::Status::OK();
  }
  if (!strcasecmp(op.data(), "on")) {
    //    flags_ |= static_cast<uint32_t>(AclUserFlag::ENABLED);
    //    flags_ &= ~static_cast<uint32_t>(AclUserFlag::DISABLED);
    AddFlags(static_cast<uint32_t>(AclUserFlag::ENABLED));
    DecFlags(static_cast<uint32_t>(AclUserFlag::DISABLED));
  } else if (!strcasecmp(op.data(), "off")) {
    //    flags_ |= static_cast<uint32_t>(AclUserFlag::DISABLED);
    //    flags_ &= ~static_cast<uint32_t>(AclUserFlag::ENABLED);
    AddFlags(static_cast<uint32_t>(AclUserFlag::DISABLED));
    DecFlags(static_cast<uint32_t>(AclUserFlag::ENABLED));
  } else if (!strcasecmp(op.data(), "nopass")) {
    //    flags_ |= static_cast<uint32_t>(AclUserFlag::NO_PASS);
    //    passwords_.clear();
    AddFlags(static_cast<uint32_t>(AclUserFlag::NO_PASS));
    CleanPassword();
  } else if (!strcasecmp(op.data(), "resetpass")) {
    //    flags_ &= ~static_cast<uint32_t>(AclUserFlag::NO_PASS);
    //    passwords_.clear();
    DecFlags(static_cast<uint32_t>(AclUserFlag::NO_PASS));
    CleanPassword();
  } else if (op[0] == '>' || op[0] == '#') {
    std::string newpass;
    if (op[0] == '>') {
      newpass = pstd::sha256(op.data() + 1);
    } else {
      if (!pstd::isSha256(op.data() + 1)) {
        return pstd::Status::Corruption("acl password not sha256");
      }
      newpass = op.data() + 1;
    }
    //    passwords_.insert(newpass);
    //    flags_ &= ~static_cast<uint32_t>(AclUserFlag::NO_PASS);
    AddPassword(newpass);
    DecFlags(static_cast<uint32_t>(AclUserFlag::NO_PASS));
  } else if (op[0] == '<' || op[0] == '!') {
    std::string delpass;
    if (op[0] == '<') {
      delpass = pstd::sha256(op.data() + 1);
    } else {
      if (!pstd::isSha256(op.data() + 1)) {
        return pstd::Status::Corruption("acl password not sha256");
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
  } else if (op == "reset") {
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
    auto status = GetRootSelector()->SetSelector(op);
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
  if (!aclString_.empty()) {
    str->append(aclString_);
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

// class User end

// class Acl

pstd::Status Acl::Initialization() {
  AddUser(CreateDefaultUser());
  LoadUsersAtStartup();
  return LoadUsersAtStartup();
}

std::shared_ptr<User> Acl::GetUser(const std::string& userName, bool look) {
  if (look) {
    std::shared_lock rl(mutex_);
  }

  return users_[userName];
}

void Acl::AddUser(const std::shared_ptr<User>& user) {
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
      return pstd::Status::Corruption("acl from configuration file read rules error");
    }
    auto user = GetUser(userRules[0]);
    if (user) {
      if (user->Name() != "default") {  // 只允许`default`用户可以重复
        return pstd::Status::Corruption("acl user: " + user->Name() + " is repeated");
      }
    } else {
      user = std::make_shared<User>(userRules[0]);
    }
    for (int i = 1; i < userRules.size(); ++i) {
      auto status = user->SetUser(userRules[i]);
      if (!status.ok()) {
        LOG(ERROR) << "load user from configured file error," << status.ToString();
        return status;
      }
    }
    users_[userRules[0]] = user;
  }

  return pstd::Status().OK();
}

pstd::Status Acl::LoadUserFromFile(const std::string& fileName) {
  std::unique_lock wl(mutex_);
  if (fileName.empty()) {
    return pstd::Status::OK();
  }

  std::unique_ptr<pstd::SequentialFile> sequentialFile;
  auto status = NewSequentialFile(fileName, sequentialFile);
  if (!status.ok()) {
    return status;
  }

  std::map<std::string, std::shared_ptr<User>> users;
  std::vector<std::string> rules;
  const int lineLength = 1024 * 1024;
  char line[lineLength];

  while (sequentialFile->ReadLine(line, lineLength) != nullptr) {
    int lineLen = strlen(line);
    if (lineLen == 0) {
      continue;
    }

    std::string lineContent = pstd::StringTrim(line, "\r\n ");
    rules.clear();
    pstd::StringSplit(lineContent, ' ', rules);
    if (rules.empty()) {
      continue;
    }

    if (rules[0] != "user" || rules.size() < 2) {
      LOG(ERROR) << "load user from acl file,line: '" << lineContent << "' illegal";
      return pstd::Status::Corruption("line: '" + lineContent + "' illegal");
    }

    auto u = GetUser(rules[1]);
    if (u && u->Name() != "default") {
      // if user is exists, exit
      LOG(ERROR) << "load user from acl file error," << status.ToString();
      return pstd::Status::Corruption("user: " + rules[1] + " is repeated");
    }

    std::vector<std::string> aclArgc;
    // 去掉 user <user name>, 这里复制了一次字符串 这里应该可以优化，暂时还没想到
    auto subRule = std::vector<std::string>(rules.begin() + 2, rules.end());
    ACLMergeSelectorArguments(subRule, &aclArgc);

    u = std::make_shared<User>(rules[1]);
    for (const auto& item : aclArgc) {
      status = u->SetUser(item);
      if (!status.ok()) {
        LOG(ERROR) << "load user from acl file error," << status.ToString();
        return status;
      }
    }
    users[rules[1]] = u;
  }

  auto defaultUser = users.find("default");
  if (defaultUser == users.end()) {  // 新的map里没有 default user
    users["default"] = CreateDefaultUser();
  }

  users_ = std::move(users);

  return pstd::Status().OK();
}

std::shared_ptr<User> Acl::CreateDefaultUser() {
  auto defaultUser = std::make_shared<User>("default");
  defaultUser->SetUser("+@all");
  defaultUser->SetUser("~*");
  defaultUser->SetUser("&*");
  defaultUser->SetUser("on");
  defaultUser->SetUser("nopass");
  return defaultUser;
}

bool Acl::SetUser(const std::string& op) {
  // todo
  return false;
}

uint32_t Acl::GetCommandCategoryFlagByName(const std::string& name) { return CommandCategories[name]; }

std::string Acl::GetCommandCategoryFlagByName(const uint32_t category) {
  for (const auto& item : CommandCategories) {
    if (item.second == category) {
      return item.first;
    }
  }

  return "";
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

pstd::Status Acl::SaveToFile() {
  std::unique_lock wl(mutex_);
  std::string aclFileName = g_pika_conf->acl_file();
  if (aclFileName.empty()) {
    LOG(ERROR) << "save user to acl file, file name is empty";
    return pstd::Status::Corruption("acl file name is empty");
  }

  // todo
  aclFileName = "../conf/users2222.acl";

  std::unique_ptr<pstd::WritableFile> file;
  auto status = pstd::NewWritableFile(aclFileName, file);
  if (!status.ok()) {
    auto error = fmt::format("open acl user file:{} fail, error:{}", aclFileName, status.ToString());
    LOG(ERROR) << error;
    return pstd::Status::Corruption(error);
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

  return pstd::Status::OK();
}

std::map<std::string, uint32_t> Acl::CommandCategories = {
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
};

std::map<std::string, uint32_t> Acl::UserFlags = {
    {"on", static_cast<uint32_t>(AclUserFlag::ENABLED)},
    {"off", static_cast<uint32_t>(AclUserFlag::DISABLED)},
    {"nopass", static_cast<uint32_t>(AclUserFlag::NO_PASS)},
};

std::map<std::string, uint32_t> Acl::SelectorFlags = {
    {"allkeys", static_cast<uint32_t>(AclSelectorFlag::ALL_KEYS)},
    {"allchannels", static_cast<uint32_t>(AclSelectorFlag::ALL_CHANNELS)},
    {"allcommands", static_cast<uint32_t>(AclSelectorFlag::ALL_COMMANDS)},
};

// class Acl end

// class AclSelector
pstd::Status AclSelector::SetSelector(const std::string& op) {
  if (!strcasecmp(op.data(), "allkeys") || op == "~*") {
    //    flags_ |= static_cast<uint32_t>(AclSelectorFlag::ALL_KEYS);
    AddFlags(static_cast<uint32_t>(AclSelectorFlag::ALL_KEYS));
    patterns_.clear();
  } else if (!strcasecmp(op.data(), "resetkeys")) {
    //    flags_ &= ~static_cast<uint32_t>(AclSelectorFlag::ALL_KEYS);
    DecFlags(static_cast<uint32_t>(AclSelectorFlag::ALL_KEYS));
    patterns_.clear();
  } else if (!strcasecmp(op.data(), "allchannels") || !strcasecmp(op.data(), "&*")) {
    //    flags_ |= static_cast<uint32_t>(AclSelectorFlag::ALL_CHANNELS);
    AddFlags(static_cast<uint32_t>(AclSelectorFlag::ALL_CHANNELS));
    channels_.clear();
  } else if (!strcasecmp(op.data(), "resetchannels")) {
    //    flags_ &= ~static_cast<uint32_t>(AclSelectorFlag::ALL_CHANNELS);
    DecFlags(static_cast<uint32_t>(AclSelectorFlag::ALL_CHANNELS));
    channels_.clear();
  } else if (!strcasecmp(op.data(), "allcommands") || !strcasecmp(op.data(), "+@all")) {
    //    flags_ |= static_cast<uint32_t>(AclSelectorFlag::ALL_COMMANDS);
    AddFlags(static_cast<uint32_t>(AclSelectorFlag::ALL_COMMANDS));
    allowedCommands_.set();
    ResetSubCommand();
    CleanCommandRule();
  } else if (!strcasecmp(op.data(), "nocommands") || !strcasecmp(op.data(), "-@all")) {
    //    flags_ &= ~static_cast<uint32_t>(AclSelectorFlag::ALL_COMMANDS);
    DecFlags(static_cast<uint32_t>(AclSelectorFlag::ALL_COMMANDS));
    allowedCommands_.reset();
    ResetSubCommand();
    CleanCommandRule();
  } else if (op[0] == '~' || op[0] == '%') {
    if (HasFlags(static_cast<int>(AclSelectorFlag::ALL_KEYS))) {
      return pstd::Status::Corruption(
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
          return pstd::Status::Corruption("Syntax error");
        }
      }
    } else {
      flags = static_cast<int>(AclPermission::ALL);
    }

    if (pstd::isspace(op)) {
      return pstd::Status::Corruption("Syntax error");
    }

    InsertKeyPattern(op.substr(offset, std::string::npos), flags);
    //    flags_ &= ~static_cast<uint32_t>(AclSelectorFlag::ALL_KEYS);
    DecFlags(static_cast<uint32_t>(AclSelectorFlag::ALL_KEYS));
  } else if (op[0] == '&') {
    if (HasFlags(static_cast<uint32_t>(AclSelectorFlag::ALL_CHANNELS))) {
      return pstd::Status::Corruption(
          "Adding a pattern after the * pattern (or the 'allchannels' flag) is not valid and does not have any effect. "
          "Try 'resetchannels' to start with an empty list of channels");
    }
    if (pstd::isspace(op)) {
      return pstd::Status::Corruption("Syntax error");
    }
    InsertChannel(op.substr(1, std::string::npos));
    //    flags_ &= ~static_cast<uint32_t>(AclSelectorFlag::ALL_CHANNELS);
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
      return pstd::Status::Corruption("Unknown command or category name in ACL");
    }
  } else {
    return pstd::Status::Corruption("Syntax error");
  }
  return pstd::Status();
}

pstd::Status AclSelector::SetSelectorFromOpSet(const std::string& opSet) {
  if (opSet[0] != '(' || opSet[opSet.size() - 1] != ')') {
    return pstd::Status::Corruption("Unmatched parenthesis in acl selector starting at" + opSet);
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
    if (cmd.second->AclCategory() & category) {  // 这个cmd 属于这个分类
      ChangeSelector(cmd.second.get(), allow);
    }
  }
  return true;
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
  for (const auto& item : patterns_) {
    if (item->pattern == str) {
      return;
    }
  }
  channels_.emplace_back(str);
}

void AclSelector::ChangeSelector(const Cmd* cmd, bool allow) {
  if (allow) {
    allowedCommands_.set(cmd->GetCmdId());
  } else {
    allowedCommands_.reset(cmd->GetCmdId());
  }

  if (cmd->HasSubCommand()) {
    ResetSubCommand(cmd->GetCmdId());
  }
}

void AclSelector::ChangeSelector(const std::shared_ptr<Cmd>& cmd, bool allow) { ChangeSelector(cmd.get(), allow); }

pstd::Status AclSelector::ChangeSelector(const std::shared_ptr<Cmd>& cmd, const std::string& subCmd, bool allow) {
  if (cmd->HasSubCommand()) {
    auto index = cmd->SubCmdIndex(subCmd);
    if (index == -1) {
      return pstd::Status::Corruption("Unknown command or category name in ACL");
    }
    if (allow) {
      SetSubCommand(cmd->GetCmdId(), index);
    } else {
      ResetSubCommand(cmd->GetCmdId(), index);
    }
  }
  return pstd::Status::OK();
}

void AclSelector::SetSubCommand(const uint32_t cmdId) { subCommand_[cmdId] = ~0; }

void AclSelector::SetSubCommand(const uint32_t cmdId, const uint32_t subCmdIndex) {
  subCommand_[cmdId] = (1 << subCmdIndex);
}

void AclSelector::ResetSubCommand() { subCommand_.clear(); }

void AclSelector::ResetSubCommand(const uint32_t cmdId) { subCommand_[cmdId] = 0; }

void AclSelector::ResetSubCommand(const uint32_t cmdId, const uint32_t subCmdIndex) {
  subCommand_[cmdId] = ~(1 << subCmdIndex);
}

void AclSelector::ACLDescribeSelector(std::string* str) {
  if (HasFlags(static_cast<uint32_t>(AclSelectorFlag::ALL_COMMANDS))) {
    str->append(" ~*");
  } else {
    for (const auto& item : patterns_) {
      str->append(" ");
      item->ToString(str);
    }
  }

  // Pub/sub channel patterns
  if (flags_ & static_cast<uint32_t>(AclSelectorFlag::ALL_CHANNELS)) {
    str->append(" &*");
  } else {
    for (const auto& item : channels_) {
      str->append(" &" + item);
    }
  }

  // Command rules
  DescribeSelectorCommandRules(str);
}

void AclSelector::DescribeSelectorCommandRules(std::string* str) {
  if (allowedCommands_.all()) {
    str->append(" +@all");
  } else {
    str->append(" -@all");
  }

  // Category
  str->append(commandRules);
}

pstd::Status AclSelector::SetCommandOp(const std::string& op, bool allow) {
  if (op.find('|') == std::string::npos) {
    auto cmd = g_pika_cmd_table_manager->GetCmd(op.data() + 1);
    if (!cmd) {
      return pstd::Status::Corruption("Unknown command or category name in ACL");
    }
    ChangeSelector(cmd, allow);
    return pstd::Status::OK();
  } else {
    /* Split the command and subcommand parts. */
    std::vector<std::string> cmds;
    pstd::StringSplit(op.data() + 1, '|', cmds);

    /* The subcommand cannot be empty, so things like CONFIG|
     * are syntax errors of course. */
    if (cmds.size() != 2) {
      return pstd::Status::Corruption("Syntax error");
    }

    auto parentCmd = g_pika_cmd_table_manager->GetCmd(cmds[0]);
    if (!parentCmd) {
      return pstd::Status::Corruption("Unknown command or category name in ACL");
    }

    return ChangeSelector(parentCmd, cmds[1], allow);

    // not support Redis ACL `first-arg` feature
  }
}

void AclSelector::UpdateCommonRule(const std::string& rule, bool allow) {
  RemoveCommonRule(rule);
  commandRules += allow ? " +" : " -";
  commandRules += rule;
}

void AclSelector::RemoveCommonRule(const std::string& rule) {
  if (commandRules.empty()) {
    return;
  }

  const size_t ruleLen = rule.size();

  size_t start = 0;
  while (true) {
    start = commandRules.find(rule, start);
    if (start == std::string::npos) {
      return;
    }

    size_t delNum = 0;                             // the length to be deleted this time
    if (start + ruleLen >= commandRules.size()) {  // the remaining commandRule == rule, delete to end
      delNum = ruleLen;
    } else {
      if (commandRules[start + ruleLen] == ' ') {
        delNum = ruleLen + 1;
      } else if (commandRules[start + ruleLen] == '|') {
        size_t end = commandRules.find(' ', start);  // find next ' '
        if (end == std::string::npos) {              // not find ' ', delete to end
          delNum = commandRules.size() - start;
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

    commandRules.erase(start, delNum);
  }
}

void AclSelector::CleanCommandRule() { commandRules.clear(); }
// class AclSelector end