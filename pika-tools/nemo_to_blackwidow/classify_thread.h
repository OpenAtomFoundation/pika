//  Copyright (c) 2018-present The pika-tools Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef INCLUDE_CLASSIFY_THREAD_H_
#define INCLUDE_CLASSIFY_THREAD_H_

#include "iostream"
#include "vector"

#include <glog/logging.h>

#include "nemo.h"
#include "net/include/net_thread.h"
#include "slash/include/slash_mutex.h"

#include "migrator.h"
#include "utils.h"

extern slash::Mutex mutex;

class ClassifyThread : public net::Thread {
 public:
  ClassifyThread(nemo::Nemo* nemo_db, std::vector<Migrator*> migrators, const std::string& type);
  virtual ~ClassifyThread();
  int64_t key_num();
  bool is_finish() { return is_finish_; }
  std::string type() { return type_; }
  int64_t consume_index() { return consume_index_; }

 private:
  void PlusProcessKeyNum();
  void DispatchItem(const std::string& item);
  virtual void* ThreadMain();

  bool is_finish_;
  int64_t key_num_;
  int64_t consume_index_;
  pthread_rwlock_t rwlock_;
  nemo::Nemo* nemo_db_;
  std::vector<Migrator*> migrators_;
  std::string type_;
};

#endif  //  INCLUDE_CLASSIFY_THREAD_H_
