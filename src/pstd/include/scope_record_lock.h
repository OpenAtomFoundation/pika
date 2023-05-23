//  Copyright (c) 2017-present The storage Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef __SRC_SCOPE_RECORD_LOCK_H__
#define __SRC_SCOPE_RECORD_LOCK_H__

#include <algorithm>
#include <string>
#include <vector>

#include "pstd/include/pika_noncopyable.h"
#include "pstd/include/lock_mgr.h"
#include "rocksdb/slice.h"

namespace pstd::lock {

using Slice = rocksdb::Slice;

class ScopeRecordLock final : public pstd::noncopyable {
 public:
  ScopeRecordLock(std::shared_ptr<LockMgr> lock_mgr, const Slice& key) : lock_mgr_(lock_mgr), key_(key) {
    lock_mgr_->TryLock(key_.ToString());
  }
  ~ScopeRecordLock() { lock_mgr_->UnLock(key_.ToString()); }

 private:
  std::shared_ptr<LockMgr> const lock_mgr_;
  Slice key_;
};

class MultiScopeRecordLock final : public pstd::noncopyable {
 public:
  MultiScopeRecordLock(std::shared_ptr<LockMgr> lock_mgr, const std::vector<std::string>& keys);
  ~MultiScopeRecordLock();

 private:
  std::shared_ptr<LockMgr> const lock_mgr_;
  std::vector<std::string> keys_;
};

class MultiRecordLock : public noncopyable {
 public:
  explicit MultiRecordLock(std::shared_ptr<LockMgr> lock_mgr) : lock_mgr_(lock_mgr) {}
  ~MultiRecordLock() = default;

  void Lock(const std::vector<std::string>& keys);
  void Unlock(const std::vector<std::string>& keys);
 private:
  std::shared_ptr<LockMgr> const lock_mgr_;
};

}  // namespace pstd::lock
#endif  // __SRC_SCOPE_RECORD_LOCK_H__
