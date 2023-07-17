// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/acl.h"
#include <shared_mutex>
#include "include/pika_server.h"
#include "pstd_hash.h"

// class User
std::string User::Name() const {
  std::shared_lock rl(mutex_);
  return name_;
}

std::string User::AclString() const {
  std::shared_lock rl(mutex_);
  return aclString_;
}

void User::SetAclString(const std::string& aclString) {
  std::unique_lock wl(mutex_);
  aclString_ = aclString;
}

void User::AddPassword(const std::string& password) {
  std::unique_lock wl(mutex_);
  passwords_.insert(password);
}

void User::RemovePassword(const std::string& password, bool look) {
  if (look) {
    std::unique_lock wl(mutex_);
  }
  passwords_.erase(password);
}

void User::AddSelector(const std::shared_ptr<AclSelector>& selector) {
  std::unique_lock wl(mutex_);
  selectors_.push_back(selector);
}

pstd::Status User::SetUser(const std::string& op, bool look) {
  if (look) {
    std::unique_lock wl(mutex_);
  }

  aclString_.clear();
  if (op.empty()) {
    return pstd::Status::OK();
  }

  if (op == "on") {
    flags_ |= static_cast<uint32_t>(AclUserFlag::ENABLED);
    flags_ &= ~static_cast<uint32_t>(AclUserFlag::DISABLED);
  } else if (op == "off") {
    flags_ |= static_cast<uint32_t>(AclUserFlag::DISABLED);
    flags_ &= ~static_cast<uint32_t>(AclUserFlag::ENABLED);
  } else if (op == "nopass") {
    flags_ |= static_cast<uint32_t>(AclUserFlag::NO_PASS);
    passwords_.clear();
  } else if (op == "resetpass") {
    flags_ &= ~static_cast<uint32_t>(AclUserFlag::NO_PASS);
    passwords_.clear();
  } else if (op[0] == '>' || op[0] == '#') {
    std::string newpass;
    if (op[0] == '>') {
      newpass = pstd::sha256(op.substr(1));
    } else {
      if (!pstd::isSha256(op.substr(1))) {
        return pstd::Status::Corruption("acl password not sha256");
      }
      newpass = op.substr(1);
    }
    passwords_.insert(newpass);
    flags_ &= ~static_cast<uint32_t>(AclUserFlag::NO_PASS);
  } else if (op[0] == '<' || op[0] == '!') {
    std::string delpass;
    if (op[0] == '<') {
      delpass = pstd::sha256(op.substr(1));
    } else {
      if (!pstd::isSha256(op.substr(1))) {
        return pstd::Status::Corruption("acl password not sha256");
      }
      delpass = op.substr(1);
    }
    passwords_.erase(delpass);
  } else if (op[0] == '(' && op[op.size() - 1] == ')') {
    // todo

    //    aclSelector* selector = aclCreateSelectorFromOpSet(op, oplen);
    //    if (!selector) {
    //      /* No errorno set, propagate it from interior error. */
    //      return C_ERR;
    //    }
    //    listAddNodeTail(u->selectors, selector);
    //    return C_OK;
  } else if (op == "clearselectors") {
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

std::shared_ptr<AclSelector> User::GetRootSelector() {
  for (const auto& item : selectors_) {
    if (item->Flags() & static_cast<uint32_t>(AclSelectorFlag::ROOT)) {
      return item;
    }
  }
  return nullptr;
}

// class User end

// class Acl

pstd::Status Acl::Initialization() {
  users_["default"] = CreateDefaultUser();
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
    for (int i = 1; i < item.size(); ++i) {
      auto status = user->SetUser(userRules[i]);
      if (!status.ok()) {
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
  std::vector<std::string> rule;
  const int lineLength = 1024 * 1024;
  char line[lineLength];

  while (sequentialFile->ReadLine(line, lineLength) != nullptr) {
    int line_len = strlen(line);
    if (line_len == 0) {
      continue;
    }

    std::string lineContent = pstd::StringTrim(line, "\r\n ");
    rule.clear();
    pstd::StringSplit(lineContent, ' ', rule);
    if (rule.empty()) {
      continue;
    }

    if (rule[0] != "user" || rule.size() < 2) {
      return pstd::Status::Corruption("line: '" + lineContent + "' illegal");
    }

    auto u = GetUser(rule[1]);
    if (u) {
      // if user is exists, exit
      return pstd::Status::Corruption("user: " + rule[1] + " is repeated");
    }

    std::vector<std::string> aclArgc;
    // 去掉 user <user name>, 这里复制了一次字符串 这里应该可以优化，暂时还没想到
    auto subRule = std::vector<std::string>(rule.begin() + 2, rule.end());
    ACLMergeSelectorArguments(subRule, &aclArgc);

    u = std::make_shared<User>(rule[0]);
    for (const auto& item : aclArgc) {
      status = u->SetUser(item);
      if (!status.ok()) {
        return status;
      }
    }
    users[rule[0]] = u;
  }

  auto defaultUser = users.find("default");
  if (defaultUser == users.end()) {  // 新的map里没有 default user
    users["default"] = CreateDefaultUser();
  }

  users_ = users;

  return pstd::Status().OK();
}

std::shared_ptr<User> Acl::CreateDefaultUser() {
  auto defaultUser = std::make_shared<User>("default");
  defaultUser->SetUser("+@all");
  defaultUser->SetUser("~*");
  defaultUser->SetUser("&*");
  defaultUser->SetUser("no");
  defaultUser->SetUser("nopass");
  return defaultUser;
}

bool Acl::SetUser(const std::string& op) {
  // todo
  return false;
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
      selector = " " + item;
      if (item[item.size() - 1] == ')') {
        openBracketStart = false;
        merged->emplace_back(selector);
      }
      continue;
    }

    merged->emplace_back(selector);
  }
}

pstd::Status AclSelector::SetSelector(const std::string op) {
  //  todo
  return pstd::Status();
}

// class Acl end