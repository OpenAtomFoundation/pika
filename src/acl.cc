// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/acl.h"
#include <shared_mutex>

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

void User::RemovePassword(const std::string& password) {
  std::unique_lock wl(mutex_);
  passwords_.erase(password);
}

void User::AddSelector(const std::shared_ptr<AclSelector>& selector) {
  std::unique_lock wl(mutex_);
  selectors_.push_back(selector);
}

// class User end

// class Acl
std::shared_ptr<User> Acl::GetUser(const std::string& userName) {
  std::shared_lock rl(mutex_);

  return users_[userName];
}

void Acl::AddUser(const std::shared_ptr<User>& user) {
  std::unique_lock l(mutex_);

  users_[user->Name()] = user;
}

void Acl::LoadUsersAtStartup(void) {
  // todo
}

bool Acl::LoadUserConfigured(std::vector<std::string>& users) {
  // todo
  return false;
}

bool Acl::LoadUserFromFile(const std::string& fileName) {
  // todo
  return false;
}

std::shared_ptr<User> Acl::CreateUse(const std::string& userName) {
  // todo
  return nullptr;
}

bool Acl::SetUser(const std::string& op) {
  // todo
  return false;
}

// class Acl end