//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_LRU_CACHE_H_
#define SRC_LRU_CACHE_H_

#include <stdio.h>
#include <assert.h>
#include <unordered_map>

#include "pstd/include/pstd_mutex.h"

namespace blackwidow {

template <typename T1, typename T2>
struct LRUHandle {
  T1 key;
  T2 value;
  size_t charge;
  LRUHandle* next;
  LRUHandle* prev;
};

template <typename T1, typename T2>
class HandleTable {
 public:
  HandleTable();
  ~HandleTable();

  size_t TableSize();
  LRUHandle<T1, T2>* Lookup(const T1& key);
  LRUHandle<T1, T2>* Remove(const T1& key);
  LRUHandle<T1, T2>* Insert(const T1& key, LRUHandle<T1, T2>* const handle);

 private:
  std::unordered_map<T1, LRUHandle<T1, T2>*> table_;
};

template <typename T1, typename T2>
HandleTable<T1, T2>::HandleTable() {
}

template <typename T1, typename T2>
HandleTable<T1, T2>::~HandleTable() {
}

template <typename T1, typename T2>
size_t HandleTable<T1, T2>::TableSize() {
  return table_.size();
}

template <typename T1, typename T2>
LRUHandle<T1, T2>* HandleTable<T1, T2>::Lookup(const T1& key) {
  if (table_.find(key) != table_.end()) {
    return table_[key];
  } else {
    return NULL;
  }
}

template <typename T1, typename T2>
LRUHandle<T1, T2>* HandleTable<T1, T2>::Remove(const T1& key) {
  LRUHandle<T1, T2>* old = NULL;
  if (table_.find(key) != table_.end()) {
    old = table_[key];
    table_.erase(key);
  }
  return old;
}

template <typename T1, typename T2>
LRUHandle<T1, T2>* HandleTable<T1, T2>::Insert(const T1& key,
                                               LRUHandle<T1, T2>* const handle) {
  LRUHandle<T1, T2>* old = NULL;
  if (table_.find(key) != table_.end()) {
    old = table_[key];
    table_.erase(key);
  }
  table_.insert({key, handle});
  return old;
}


template <typename T1, typename T2>
class LRUCache {
 public:
  LRUCache();
  ~LRUCache();

  size_t Size();
  size_t TotalCharge();
  size_t Capacity();
  void SetCapacity(size_t capacity);

  Status Lookup(const T1& key, T2* value);
  Status Insert(const T1& key, const T2& value, size_t charge = 1);
  Status Remove(const T1& key);
  Status Clear();

  // Just for test
  bool LRUAndHandleTableConsistent();
  bool LRUAsExpected(const std::vector<std::pair<T1, T2>>& expect);

 private:
  void LRU_Trim();
  void LRU_Remove(LRUHandle<T1, T2>* const e);
  void LRU_Append(LRUHandle<T1, T2>* const e);
  void LRU_MoveToHead(LRUHandle<T1, T2>* const e);
  bool FinishErase(LRUHandle<T1, T2>* const e);

  // Initialized before use.
  size_t capacity_;
  size_t usage_;
  size_t size_;

  pstd::Mutex mutex_;

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  LRUHandle<T1, T2> lru_;

  HandleTable<T1, T2> handle_table_;
};

template <typename T1, typename T2>
LRUCache<T1, T2>::LRUCache()
    : capacity_(0),
      usage_(0),
      size_(0) {
  // Make empty circular linked lists.
  lru_.next = &lru_;
  lru_.prev = &lru_;
}

template <typename T1, typename T2>
LRUCache<T1, T2>::~LRUCache() {
  Clear();
}

template <typename T1, typename T2>
size_t LRUCache<T1, T2>::Size() {
  pstd::MutexLock l(&mutex_);
  return size_;
}

template <typename T1, typename T2>
size_t LRUCache<T1, T2>::TotalCharge() {
  pstd::MutexLock l(&mutex_);
  return usage_;
}

template <typename T1, typename T2>
size_t LRUCache<T1, T2>::Capacity() {
  pstd::MutexLock l(&mutex_);
  return capacity_;
}

template <typename T1, typename T2>
void LRUCache<T1, T2>::SetCapacity(size_t capacity) {
  pstd::MutexLock l(&mutex_);
  capacity_ = capacity;
  LRU_Trim();
}

template <typename T1, typename T2>
Status LRUCache<T1, T2>::Lookup(const T1& key, T2* const value) {
  pstd::MutexLock l(&mutex_);
  LRUHandle<T1, T2>* handle = handle_table_.Lookup(key);
  if (handle != NULL) {
    LRU_MoveToHead(handle);
    *value = handle->value;
  }
  return (handle == NULL) ? Status::NotFound() : Status::OK();
}

template <typename T1, typename T2>
Status LRUCache<T1, T2>::Insert(const T1& key, const T2& value, size_t charge) {
  pstd::MutexLock l(&mutex_);
  if (capacity_ == 0) {
    return Status::Corruption("capacity is empty");
  } else {
    LRUHandle<T1, T2>* handle = new LRUHandle<T1, T2>();
    handle->key = key;
    handle->value = value;
    handle->charge = charge;
    LRU_Append(handle);
    size_++;
    usage_ += charge;
    FinishErase(handle_table_.Insert(key, handle));
    LRU_Trim();
  }
  return Status::OK();
}

template <typename T1, typename T2>
Status LRUCache<T1, T2>::Remove(const T1& key) {
  pstd::MutexLock l(&mutex_);
  bool erased = FinishErase(handle_table_.Remove(key));
  return erased ? Status::OK() : Status::NotFound();
}

template <typename T1, typename T2>
Status LRUCache<T1, T2>::Clear() {
  pstd::MutexLock l(&mutex_);
  LRUHandle<T1, T2>* old = NULL;
  while (lru_.next != &lru_) {
    old = lru_.next;
    bool erased =  FinishErase(handle_table_.Remove(old->key));
    if (!erased) {   // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }
  return Status::OK();
}

template <typename T1, typename T2>
bool LRUCache<T1, T2>::LRUAndHandleTableConsistent() {
  size_t count = 0;
  pstd::MutexLock l(&mutex_);
  LRUHandle<T1, T2>* handle = NULL;
  LRUHandle<T1, T2>* current = lru_.prev;
  while (current != &lru_) {
    handle = handle_table_.Lookup(current->key);
    if (handle == NULL || handle != current) {
      return false;
    } else {
      count++;
      current = current->prev;
    }
  }
  return count == handle_table_.TableSize();
}

template <typename T1, typename T2>
bool LRUCache<T1, T2>::LRUAsExpected(
        const std::vector<std::pair<T1, T2>>& expect) {
  if (Size() != expect.size()) {
    return false;
  } else {
    size_t idx = 0;
    LRUHandle<T1, T2>* current = lru_.prev;
    while (current != &lru_) {
      if (current->key != expect[idx].first
        || current->value != expect[idx].second) {
        return false;
      } else {
        idx++;
        current = current->prev;
      }
    }
  }
  return true;
}

template <typename T1, typename T2>
void LRUCache<T1, T2>::LRU_Trim() {
  LRUHandle<T1, T2>* old = NULL;
  while (usage_ > capacity_ && lru_.next != &lru_) {
    old = lru_.next;
    bool erased =  FinishErase(handle_table_.Remove(old->key));
    if (!erased) {   // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }
}

template <typename T1, typename T2>
void LRUCache<T1, T2>::LRU_Remove(LRUHandle<T1, T2>* const e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

template <typename T1, typename T2>
void LRUCache<T1, T2>::LRU_Append(LRUHandle<T1, T2>* const e) {
  // Make "e" newest entry by inserting just before lru_
  e->next = &lru_;
  e->prev = lru_.prev;
  e->prev->next = e;
  e->next->prev = e;
}

template <typename T1, typename T2>
void LRUCache<T1, T2>::LRU_MoveToHead(LRUHandle<T1, T2>* const e) {
  LRU_Remove(e);
  LRU_Append(e);
}

template <typename T1, typename T2>
bool LRUCache<T1, T2>::FinishErase(LRUHandle<T1, T2>* const e) {
  bool erased = false;
  if (e != NULL) {
    LRU_Remove(e);
    size_--;
    usage_ -= e->charge;
    delete e;
    erased = true;
  }
  return erased;
}

}  //  namespace blackwidow
#endif  // SRC_LRU_CACHE_H_

