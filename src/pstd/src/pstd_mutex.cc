#include "pstd/include/pstd_mutex.h"

#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <cstdlib>

#include <algorithm>

namespace pstd {

static void PthreadCall(const char* label, int result) {
  if (result != 0) {
    fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
    abort();
  }
}

// Return false if timeout
static bool PthreadTimeoutCall(const char* label, int result) {
  if (result != 0) {
    if (result == ETIMEDOUT) {
      return false;
    }
    fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
    abort();
  }
  return true;
}

Mutex::Mutex() { PthreadCall("init mutex", pthread_mutex_init(&mu_, NULL)); }

Mutex::~Mutex() { PthreadCall("destroy mutex", pthread_mutex_destroy(&mu_)); }

int Mutex::Trylock() { return pthread_mutex_trylock(&mu_); }

void Mutex::Lock() { PthreadCall("lock", pthread_mutex_lock(&mu_)); }

void Mutex::Unlock() { PthreadCall("unlock", pthread_mutex_unlock(&mu_)); }

RWMutex::RWMutex() { PthreadCall("init rw mutex", pthread_rwlock_init(&rw_mu_, NULL)); }

RWMutex::~RWMutex() { PthreadCall("destroy rw mutex", pthread_rwlock_destroy(&rw_mu_)); }

void RWMutex::ReadLock() { PthreadCall("rw readlock", pthread_rwlock_rdlock(&rw_mu_)); }

void RWMutex::WriteLock() { PthreadCall("rw writelock", pthread_rwlock_wrlock(&rw_mu_)); }

void RWMutex::WriteUnlock() { PthreadCall("rw write unlock", pthread_rwlock_unlock(&rw_mu_)); }

void RWMutex::ReadUnlock() { PthreadCall("rw read unlock", pthread_rwlock_unlock(&rw_mu_)); }

CondVar::CondVar(Mutex* mu) : mu_(mu) {
#if defined(__linux__)
  pthread_condattr_t condattr;
  PthreadCall("pthread_condattr_init", pthread_condattr_init(&condattr));
  PthreadCall("pthread_condattr_setclock", pthread_condattr_setclock(&condattr, CLOCK_MONOTONIC));
  PthreadCall("init cv", pthread_cond_init(&cv_, &condattr));
  PthreadCall("pthread_condattr_destroy", pthread_condattr_destroy(&condattr));
#else
  PthreadCall("init cv", pthread_cond_init(&cv_, nullptr));
#endif
}

CondVar::~CondVar() { PthreadCall("destroy cv", pthread_cond_destroy(&cv_)); }

void CondVar::Wait() { PthreadCall("wait", pthread_cond_wait(&cv_, &mu_->mu_)); }

// return false if timeout
bool CondVar::TimedWait(uint32_t timeout) {
  /*
   * pthread_cond_timedwait api use absolute API
   * so we need gettimeofday + timeout
   */
  struct timeval now;
  gettimeofday(&now, NULL);
  struct timespec tsp;

  int64_t usec = now.tv_usec + timeout * 1000LL;
  tsp.tv_sec = now.tv_sec + usec / 1000000;
  tsp.tv_nsec = (usec % 1000000) * 1000;

  return PthreadTimeoutCall("timewait", pthread_cond_timedwait(&cv_, &mu_->mu_, &tsp));
}

void CondVar::Signal() { PthreadCall("signal", pthread_cond_signal(&cv_)); }

void CondVar::SignalAll() { PthreadCall("broadcast", pthread_cond_broadcast(&cv_)); }

void InitOnce(OnceType* once, void (*initializer)()) { PthreadCall("once", pthread_once(once, initializer)); }

RefMutex::RefMutex() {
  refs_ = 0;
  PthreadCall("init mutex", pthread_mutex_init(&mu_, nullptr));
}

RefMutex::~RefMutex() { PthreadCall("destroy mutex", pthread_mutex_destroy(&mu_)); }

void RefMutex::Ref() { refs_++; }
void RefMutex::Unref() {
  --refs_;
  if (refs_ == 0) {
    delete this;
  }
}

void RefMutex::Lock() { PthreadCall("lock", pthread_mutex_lock(&mu_)); }

void RefMutex::Unlock() { PthreadCall("unlock", pthread_mutex_unlock(&mu_)); }

RecordMutex::~RecordMutex() {
  mutex_.Lock();

  std::unordered_map<std::string, RefMutex*>::const_iterator it = records_.begin();
  for (; it != records_.end(); it++) {
    delete it->second;
  }
  mutex_.Unlock();
}

void RecordMutex::Lock(const std::string& key) {
  mutex_.Lock();
  std::unordered_map<std::string, RefMutex*>::const_iterator it = records_.find(key);

  if (it != records_.end()) {
    RefMutex* ref_mutex = it->second;
    ref_mutex->Ref();
    mutex_.Unlock();

    ref_mutex->Lock();
  } else {
    RefMutex* ref_mutex = new RefMutex();

    records_.insert(std::make_pair(key, ref_mutex));
    ref_mutex->Ref();
    mutex_.Unlock();

    ref_mutex->Lock();
  }
}

void RecordMutex::Unlock(const std::string& key) {
  mutex_.Lock();
  std::unordered_map<std::string, RefMutex*>::const_iterator it = records_.find(key);

  if (it != records_.end()) {
    RefMutex* ref_mutex = it->second;

    if (ref_mutex->IsLastRef()) {
      records_.erase(it);
    }
    ref_mutex->Unlock();
    ref_mutex->Unref();
  }

  mutex_.Unlock();
}

}  // namespace pstd
