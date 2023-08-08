#include "pstd/include/pstd_mutex.h"

#include <cstdlib>
#include <cstring>
#include <ctime>

#include <algorithm>

#include <glog/logging.h>

namespace pstd {

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

}  // namespace pstd
