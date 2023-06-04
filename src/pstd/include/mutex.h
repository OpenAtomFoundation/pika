//  Copyright (c) 2017-present The storage Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef __SRC_MUTEX_H__
#define __SRC_MUTEX_H__

#include <memory>

#include "pstd/include/pstd_status.h"

namespace pstd::lock {

using Status = pstd::Status;

class Mutex {
 public:
  virtual ~Mutex() = default;

  // Attempt to acquire lock.  Return OK on success, or other Status on failure.
  // If returned status is OK, Storage will eventually call UnLock().
  virtual Status Lock() = 0;

  // Attempt to acquire lock.  If timeout is non-negative, operation may be
  // failed after this many microseconds.
  // Returns OK on success,
  //         TimedOut if timed out,
  //         or other Status on failure.
  // If returned status is OK, Storage will eventually call UnLock().
  virtual Status TryLockFor(int64_t timeout_time) = 0;

  // Unlock Mutex that was successfully locked by Lock() or TryLockUntil()
  virtual void UnLock() = 0;
};

class CondVar {
 public:
  virtual ~CondVar() = default;

  // Block current thread until condition variable is notified by a call to
  // Notify() or NotifyAll().  Wait() will be called with mutex locked.
  // Returns OK if notified.
  // Returns non-OK if Storage should stop waiting and fail the operation.
  // May return OK spuriously even if not notified.
  virtual Status Wait(std::shared_ptr<Mutex> mutex) = 0;

  // Block current thread until condition variable is notified by a call to
  // Notify() or NotifyAll(), or if the timeout is reached.
  // Wait() will be called with mutex locked.
  //
  // If timeout is non-negative, operation should be failed after this many
  // microseconds.
  // If implementing a custom version of this class, the implementation may
  // choose to ignore the timeout.
  //
  // Returns OK if notified.
  // Returns TimedOut if timeout is reached.
  // Returns other status if Storage should otherwis stop waiting and
  //  fail the operation.
  // May return OK spuriously even if not notified.
  virtual Status WaitFor(std::shared_ptr<Mutex> mutex, int64_t timeout_time) = 0;

  // If any threads are waiting on *this, unblock at least one of the
  // waiting threads.
  virtual void Notify() = 0;

  // Unblocks all threads waiting on *this.
  virtual void NotifyAll() = 0;
};

// Factory class that can allocate mutexes and condition variables.
class MutexFactory {
 public:
  // Create a Mutex object.
  virtual std::shared_ptr<Mutex> AllocateMutex() = 0;

  // Create a CondVar object.
  virtual std::shared_ptr<CondVar> AllocateCondVar() = 0;

  virtual ~MutexFactory() = default;
};

}  // namespace pstd::lock

#endif  // __SRC_MUTEX_H__
