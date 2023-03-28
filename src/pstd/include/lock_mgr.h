//  Copyright (c) 2017-present The storage Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef __SRC_LOCK_MGR_H__
#define __SRC_LOCK_MGR_H__

#include <string>
#include <memory>

#include "pstd/include/mutex.h"

namespace pstd {

namespace lock {
struct LockMap;
struct LockMapStripe;

class LockMgr {
 public:
  LockMgr(size_t default_num_stripes, int64_t max_num_locks,
          std::shared_ptr<MutexFactory> factory);

  ~LockMgr();

  // Attempt to lock key.  If OK status is returned, the caller is responsible
  // for calling UnLock() on this key.
  Status TryLock(const std::string& key);

  // Unlock a key locked by TryLock().
  void UnLock(const std::string& key);

 private:
  // Default number of lock map stripes
  const size_t default_num_stripes_;

  // Limit on number of keys locked per column family
  const int64_t max_num_locks_;

  // Used to allocate mutexes/condvars to use when locking keys
  std::shared_ptr<MutexFactory> mutex_factory_;

  // Map to locked key info
  std::shared_ptr<LockMap> lock_map_;

  Status Acquire(LockMapStripe* stripe, const std::string& key);

  Status AcquireLocked(LockMapStripe* stripe, const std::string& key);

  void UnLockKey(const std::string& key, LockMapStripe* stripe);

  // No copying allowed
  LockMgr(const LockMgr&);
  void operator=(const LockMgr&);
};

}  //  namespace lock
}  //  namespace pstd
#endif  // __SRC_LOCK_MGR_H__
