//  Copyright (c) 2018-present The pika-tools Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef INCLUDE_MIGRATOR_H_
#define INCLUDE_MIGRATOR_H_

#define MAX_QUEUE_SIZE 10000

#include <glog/logging.h>
#include "iostream"

#include "blackwidow/blackwidow.h"
#include "net/include/net_thread.h"

extern slash::Mutex mutex;

class Migrator: public net::Thread {
  public:
    Migrator(int32_t migrator_id, nemo::Nemo* nemo_db, blackwidow::BlackWidow* blackwidow_db)
        : nemo_db_(nemo_db),
          blackwidow_db_(blackwidow_db),
          migrator_id_(migrator_id),
          migrate_key_num_(0),
          should_exit_(false),
          queue_cond_(&queue_mutex_) {}
    virtual ~Migrator() {}

    int32_t queue_size();
    void PlusMigrateKey();
    void SetShouldExit();
    bool LoadItem(const std::string& item);

  private:
    virtual void *ThreadMain();
    nemo::Nemo* nemo_db_;
    blackwidow::BlackWidow* blackwidow_db_;

    int32_t migrator_id_;
    int64_t migrate_key_num_;
    std::atomic<bool> should_exit_;
    slash::Mutex queue_mutex_;
    slash::CondVar queue_cond_;
    std::queue<std::string> items_queue_;
};

#endif  //  INCLUDE_MIGRATOR_H_
