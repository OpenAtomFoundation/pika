//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_SCOPE_RECORD_LOCK_H_
#define SRC_SCOPE_RECORD_LOCK_H_

#include <vector>
#include <string>
#include <algorithm>

#include "slash/include/lock_mgr.h"

namespace slash {

namespace lock {

class ScopeRecordLock {
 public:
  ScopeRecordLock(LockMgr* lock_mgr, const Slice& key) :
    lock_mgr_(lock_mgr), key_(key) {
    lock_mgr_->TryLock(key_.ToString());
  }
  ~ScopeRecordLock() {
    lock_mgr_->UnLock(key_.ToString());
  }

 private:
  LockMgr* const lock_mgr_;
  Slice key_;
  ScopeRecordLock(const ScopeRecordLock&);
  void operator=(const ScopeRecordLock&);
};

class MultiScopeRecordLock {
 public:
  MultiScopeRecordLock(LockMgr* lock_mgr,
      const std::vector<std::string>& keys);
  ~MultiScopeRecordLock();

 private:
  LockMgr* const lock_mgr_;
  std::vector<std::string> keys_;
  MultiScopeRecordLock(const MultiScopeRecordLock&);
  void operator=(const MultiScopeRecordLock&);
};

class MultiRecordLock {
 public:
  explicit MultiRecordLock(LockMgr* lock_mgr) : lock_mgr_(lock_mgr) {
  }
  ~MultiRecordLock() {
  }
  void Lock(const std::vector<std::string>& keys);
  void Unlock(const std::vector<std::string>& keys);

 private:
  LockMgr* const lock_mgr_;
  MultiRecordLock(const MultiRecordLock&);
  void operator=(const MultiRecordLock&);
};

}  // namespace lock
}  // namespace slash
#endif  // SRC_SCOPE_RECORD_LOCK_H_
