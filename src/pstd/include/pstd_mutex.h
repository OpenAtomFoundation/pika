#ifndef __PSTD_MUTEXLOCK_H__
#define __PSTD_MUTEXLOCK_H__

#include <pthread.h>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "noncopyable.h"

namespace pstd {

using Mutex = std::mutex;
using CondVar = std::condition_variable;
using RWMutex = std::shared_mutex;

using OnceType = std::once_flag;

template <class F, class... Args>
void InitOnce(OnceType& once, F&& f, Args&&... args) {
  return std::call_once(once, std::forward<F>(f), std::forward<Args>(args)...);
}

class RefMutex : public pstd::noncopyable {
 public:
  RefMutex() = default;
  ~RefMutex() = default;

  // Lock and Unlock will increase and decrease refs_,
  // should check refs before Unlock
  void Lock();
  void Unlock();

  void Ref();
  void Unref();
  bool IsLastRef() { return refs_ == 1; }

 private:
  std::mutex mu_;
  int refs_ = 0;
};

class RecordMutex : public pstd::noncopyable {
 public:
  RecordMutex()= default;;
  ~RecordMutex();

  void MultiLock(const std::vector<std::string>& keys);
  void Lock(const std::string& key);
  void MultiUnlock(const std::vector<std::string>& keys);
  void Unlock(const std::string& key);

 private:
  Mutex mutex_;

  std::unordered_map<std::string, RefMutex*> records_;
};

class RecordLock : public pstd::noncopyable {
 public:
  RecordLock(RecordMutex* mu, std::string  key) : mu_(mu), key_(std::move(key)) { mu_->Lock(key_); }
  ~RecordLock() { mu_->Unlock(key_); }

 private:
  RecordMutex* const mu_;
  std::string key_;
};

class Mutexs {
 public:
  Mutexs();
  ~Mutexs();

  void Lock();
  int Trylock();
  void Unlock();
  void AssertHeld() { }
 private:

  friend class CondVars;
  pthread_mutex_t mu_;

  // No copying
  Mutexs(const Mutexs&);
  void operator=(const Mutexs&);
};

class CondVars {
 public:
  CondVars(Mutexs* mu);
  ~CondVars();
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
  Mutexs* mu_;
};

}  // namespace pstd

#endif
