//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_SCOPE_RECORD_LOCK_H_
#define SRC_SCOPE_RECORD_LOCK_H_

#include <algorithm>
#include <string>
#include <vector>

#include "src/lock_mgr.h"

namespace storage {
class ScopeRecordLock {
 public:
  ScopeRecordLock(std::shared_ptr<LockMgr> lock_mgr, const Slice& key) : lock_mgr_(lock_mgr), key_(key) {
    lock_mgr_->TryLock(key_.ToString());
  }
  ~ScopeRecordLock() { lock_mgr_->UnLock(key_.ToString()); }

 private:
  std::shared_ptr<LockMgr> const lock_mgr_;
  Slice key_;
  ScopeRecordLock(const ScopeRecordLock&);
  void operator=(const ScopeRecordLock&);
};

class MultiScopeRecordLock {
 public:
  MultiScopeRecordLock(std::shared_ptr<LockMgr> lock_mgr, const std::vector<std::string>& keys) : lock_mgr_(lock_mgr), keys_(keys) {
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
  ~MultiScopeRecordLock() {
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

 private:
  std::shared_ptr<LockMgr> const lock_mgr_;
  std::vector<std::string> keys_;
  MultiScopeRecordLock(const MultiScopeRecordLock&);
  void operator=(const MultiScopeRecordLock&);
};

}  // namespace storage
#endif  // SRC_SCOPE_RECORD_LOCK_H_
