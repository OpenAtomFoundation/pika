#include "pstd/include/pstd_mutex.h"

#include <cstdlib>
#include <cstring>
#include <ctime>
#include <pthread.h>
#include <algorithm>

#include <glog/logging.h>

namespace pstd {

static void PthreadCall(const char* label, int result) {
//  if (result != 0) {
//    fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
//    abort();
//  }
}

void RefMutex::Ref() { refs_++; }

void RefMutex::Unref() {
  --refs_;
  if (refs_ == 0) {
    delete this;
  }
}

void RefMutex::Lock() { mu_.lock(); }

void RefMutex::Unlock() { mu_.unlock(); }

RecordMutex::~RecordMutex() {
  mutex_.lock();

  auto it = records_.begin();
  for (; it != records_.end(); it++) {
    delete it->second;
  }
  mutex_.unlock();
}

void RecordMutex::Lock(const std::string& key) {
  mutex_.lock();
  auto it = records_.find(key);

  if (it != records_.end()) {
    RefMutex* ref_mutex = it->second;
    ref_mutex->Ref();
    mutex_.unlock();

    ref_mutex->Lock();
  } else {
    auto ref_mutex = new RefMutex();

    records_.emplace(key, ref_mutex);
    ref_mutex->Ref();
    mutex_.unlock();

    ref_mutex->Lock();
  }
}

void RecordMutex::Unlock(const std::string& key) {
  mutex_.lock();
  auto it = records_.find(key);

  if (it != records_.end()) {
    RefMutex* ref_mutex = it->second;

    if (ref_mutex->IsLastRef()) {
      records_.erase(it);
    }
    ref_mutex->Unlock();
    ref_mutex->Unref();
  }

  mutex_.unlock();
}

CondVars::CondVars(Mutexs* mu)
        : mu_(mu) {
    pthread_condattr_t condattr;
    PthreadCall("pthread_condattr_init", pthread_condattr_init(&condattr));
    PthreadCall("pthread_condattr_setclock", pthread_condattr_setclock(&condattr, CLOCK_MONOTONIC));
    PthreadCall("init cv", pthread_cond_init(&cv_, &condattr));
    PthreadCall("pthread_condattr_destroy", pthread_condattr_destroy(&condattr));
}

CondVars::~CondVars() {
    PthreadCall("destroy cv", pthread_cond_destroy(&cv_));
}

Mutexs::Mutexs() {
  PthreadCall("init mutex", pthread_mutex_init(&mu_, nullptr));
}

Mutexs::~Mutexs() {
  PthreadCall("destroy mutex", pthread_mutex_destroy(&mu_));
}

void CondVars::Wait() {
  PthreadCall("wait", pthread_cond_wait(&cv_, &mu_->mu_));
}

void CondVars::Signal() {
  PthreadCall("signal", pthread_cond_signal(&cv_));
}

}  // namespace pstd
