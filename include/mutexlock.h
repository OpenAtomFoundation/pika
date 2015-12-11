// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef PIKA_UTIL_MUTEXLOCK_H_
#define PIKA_UTIL_MUTEXLOCK_H_

#include "port.h"
//#include <pthread.h>

// Helper class that locks a mutex on construction and unlocks the mutex when
// the destructor of the MutexLock object is invoked.
//
// Typical usage:
//
//   void MyClass::MyMethod() {
//     MutexLock l(&mu_);       // mu_ is an instance variable
//     ... some complex code, possibly with multiple return paths ...
//   }

class MutexLock {
    public:
        explicit MutexLock(port::Mutex *mu)
            : mu_(mu)  {
                this->mu_->Lock();
            }
        ~MutexLock() { this->mu_->Unlock(); }

    private:
        port::Mutex *const mu_;
        // No copying allowed
        MutexLock(const MutexLock&);
        void operator=(const MutexLock&);
};

class RWLock {
    public:
        explicit RWLock(pthread_rwlock_t *mu, bool is_rwlock)
            : mu_(mu)  {
                if (is_rwlock) {
                    pthread_rwlock_wrlock(this->mu_);
                } else {
                    pthread_rwlock_rdlock(this->mu_);
                }
            }
        ~RWLock() { pthread_rwlock_unlock(this->mu_); }

    private:
        pthread_rwlock_t *const mu_;
        // No copying allowed
        RWLock(const RWLock&);
        void operator=(const RWLock&);
};


#endif  // STORAGE_LEVELDB_UTIL_MUTEXLOCK_H_
