//  Copyright (c) 2017-present The storage Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "pstd/include/scope_record_lock.h"

namespace pstd {

namespace lock {

MultiScopeRecordLock::MultiScopeRecordLock(LockMgr* lock_mgr, const std::vector<std::string>& keys)
    : lock_mgr_(lock_mgr), keys_(keys) {
  std::string pre_key;
  std::sort(keys_.begin(), keys_.end());
  if (!keys_.empty() && keys_[0].empty()) {
    lock_mgr_->TryLock(pre_key);
  }

  for (const auto& key : keys_) {
    if (pre_key != key) {
      lock_mgr_->TryLock(key);
      pre_key = key;
    }
  }
}
MultiScopeRecordLock::~MultiScopeRecordLock() {
  std::string pre_key;
  if (!keys_.empty() && keys_[0].empty()) {
    lock_mgr_->UnLock(pre_key);
  }

  for (const auto& key : keys_) {
    if (pre_key != key) {
      lock_mgr_->UnLock(key);
      pre_key = key;
    }
  }
}

void MultiRecordLock::Lock(const std::vector<std::string>& keys) {
  std::vector<std::string> internal_keys = keys;
  std::sort(internal_keys.begin(), internal_keys.end());
  // init to be ""
  std::string pre_key;
  // consider internal_keys "" "" "a"
  if (!internal_keys.empty()) {
    lock_mgr_->TryLock(internal_keys.front());
    pre_key = internal_keys.front();
  }

  for (const auto& key : internal_keys) {
    if (pre_key != key) {
      lock_mgr_->TryLock(key);
      pre_key = key;
    }
  }
}

void MultiRecordLock::Unlock(const std::vector<std::string>& keys) {
  std::vector<std::string> internal_keys = keys;
  std::sort(internal_keys.begin(), internal_keys.end());
  std::string pre_key;
  if (!internal_keys.empty()) {
    lock_mgr_->UnLock(internal_keys.front());
    pre_key = internal_keys.front();
  }

  for (const auto& key : internal_keys) {
    if (pre_key != key) {
      lock_mgr_->UnLock(key);
      pre_key = key;
    }
  }
}
}  // namespace lock
}  // namespace pstd
