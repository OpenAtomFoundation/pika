#ifndef __PSTD_MUTEXLOCK_H__
#define __PSTD_MUTEXLOCK_H__

#include <pthread.h>
#include <string>
#include <unordered_map>
#include <vector>

namespace pstd {

class CondVar;

class Mutex {
 public:
  Mutex();
  ~Mutex();

  void Lock();
  // if lock return 0; else return errorno
  int Trylock();
  void Unlock();
  void AssertHeld() {}

 private:
  friend class CondVar;
  pthread_mutex_t mu_;

  // No copying
  Mutex(const Mutex&);
  void operator=(const Mutex&);
};

// DEPRECATED
class RWLock {
 public:
  RWLock(pthread_rwlock_t* mu, bool is_rwlock) : mu_(mu) {
    if (is_rwlock) {
      res = pthread_rwlock_wrlock(this->mu_);
    } else {
      res = pthread_rwlock_rdlock(this->mu_);
    }
  }
  ~RWLock() {
    if (!res) {
      pthread_rwlock_unlock(this->mu_);
    }
  }

 private:
  int res;
  pthread_rwlock_t* const mu_;
  // No copying allowed
  RWLock(const RWLock&);
  void operator=(const RWLock&);
};

class RWMutex {
 public:
  RWMutex();
  ~RWMutex();

  void ReadLock();
  void WriteLock();
  void ReadUnlock();
  void WriteUnlock();

 private:
  pthread_rwlock_t rw_mu_;

  // No copying
  RWMutex(const RWMutex&);
  void operator=(const RWMutex&);
};

class ReadLock {
 public:
  explicit ReadLock(RWMutex* rw_mu) : rw_mu_(rw_mu) { this->rw_mu_->ReadLock(); }
  ~ReadLock() { this->rw_mu_->ReadUnlock(); }

 private:
  RWMutex* const rw_mu_;
  // No copying
  ReadLock(const ReadLock&);
  void operator=(const ReadLock&);
};

class WriteLock {
 public:
  WriteLock(RWMutex* rw_mu) : rw_mu_(rw_mu) { this->rw_mu_->WriteLock(); }
  ~WriteLock() { this->rw_mu_->WriteUnlock(); }

 private:
  RWMutex* const rw_mu_;
  // No copying allowed
  WriteLock(const WriteLock&);
  void operator=(const WriteLock&);
};

class CondVar {
 public:
  explicit CondVar(Mutex* mu);
  ~CondVar();
  void Wait();
  /*
   * timeout is millisecond
   * so if you want to wait for 1 s, you should call
   * TimeWait(1000);
   * return false if timeout
   */
  bool TimedWait(uint32_t timeout);
  void Signal();
  void SignalAll();

 private:
  pthread_cond_t cv_;
  Mutex* mu_;
};

class MutexLock {
 public:
  explicit MutexLock(Mutex* mu) : mu_(mu) { this->mu_->Lock(); }
  ~MutexLock() { this->mu_->Unlock(); }

 private:
  Mutex* const mu_;
  // No copying allowed
  MutexLock(const MutexLock&);
  void operator=(const MutexLock&);
};

typedef pthread_once_t OnceType;
extern void InitOnce(OnceType* once, void (*initializer)());

class RefMutex {
 public:
  RefMutex();
  ~RefMutex();

  // Lock and Unlock will increase and decrease refs_,
  // should check refs before Unlock
  void Lock();
  void Unlock();

  void Ref();
  void Unref();
  bool IsLastRef() { return refs_ == 1; }

 private:
  pthread_mutex_t mu_;
  int refs_;

  // No copying
  RefMutex(const RefMutex&);
  void operator=(const RefMutex&);
};

class RecordMutex {
 public:
  RecordMutex(){};
  ~RecordMutex();

  void MultiLock(const std::vector<std::string>& keys);
  void Lock(const std::string& key);
  void MultiUnlock(const std::vector<std::string>& keys);
  void Unlock(const std::string& key);

 private:
  Mutex mutex_;

  std::unordered_map<std::string, RefMutex*> records_;

  // No copying
  RecordMutex(const RecordMutex&);
  void operator=(const RecordMutex&);
};

class RecordLock {
 public:
  RecordLock(RecordMutex* mu, const std::string& key) : mu_(mu), key_(key) { mu_->Lock(key_); }
  ~RecordLock() { mu_->Unlock(key_); }

 private:
  RecordMutex* const mu_;
  std::string key_;

  // No copying allowed
  RecordLock(const RecordLock&);
  void operator=(const RecordLock&);
};

}  // namespace pstd

#endif
