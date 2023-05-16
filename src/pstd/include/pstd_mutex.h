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

namespace pstd {

using Mutex = std::mutex;
using CondVar = std::condition_variable;
using RWMutex = std::shared_mutex;

using OnceType = std::once_flag;

template <class F, class... Args>
void InitOnce(OnceType& once, F&& f, Args&&... args) {
  return std::call_once(once, std::forward<F>(f), std::forward<Args>(args)...);
}

class RefMutex {
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

  // No copying
  RefMutex(const RefMutex&) = delete;
  void operator=(const RefMutex&) = delete;

 private:
  std::mutex mu_;
  int refs_ = 0;

};

class RecordMutex {
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

  // No copying
  RecordMutex(const RecordMutex&);
  void operator=(const RecordMutex&);
};

class RecordLock {
 public:
  RecordLock(RecordMutex* mu, std::string  key) : mu_(mu), key_(std::move(std::move(std::move(key)))) { mu_->Lock(key_); }
  ~RecordLock() { mu_->Unlock(key_); }
  // No copying allowed
  RecordLock(const RecordLock&) = delete;
  void operator=(const RecordLock&) = delete;

 private:
  RecordMutex* const mu_;
  std::string key_;
};

}  // namespace pstd

#endif
