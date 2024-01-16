//  Copyright (c) 2018-present The pika-tools Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef INCLUDE_SCAN_THREAD_H_
#define INCLUDE_SCAN_THREAD_H_

#include "iostream"
#include "vector"

#include "net/include/net_thread.h"
#include "pstd/include/pstd_coding.h"
#include "storage/storage.h"

#include "write_thread.h"

class ScanThread : public net::Thread {
 public:
  ScanThread(WriteThread* write_thread, storage::Storage* storage_db)
      : is_finish_(false), scan_number_(0), write_thread_(write_thread), storage_db_(storage_db) {}
  bool is_finish();
  int32_t scan_number();

 private:
  void* ThreadMain() override;
  bool is_finish_;
  int32_t scan_number_;
  WriteThread* write_thread_;
  storage::Storage* storage_db_;
};

#endif  //  INCLUDE_SCAN_THREAD_H_
