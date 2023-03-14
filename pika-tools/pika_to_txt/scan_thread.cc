//  Copyright (c) 2018-present The pika-tools Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "scan_thread.h"

extern int32_t scan_batch_limit;

bool ScanThread::is_finish() {
  return is_finish_;
}

int32_t ScanThread::scan_number() {
  return scan_number_;
}

void* ScanThread::ThreadMain() {
  std::string key_start = "";
  std::string key_end = "";
  std::string pattern = "*";
  std::string next_key;
  std::vector<std::string> keys;
  std::vector<blackwidow::KeyValue> kvs;

  do {
    blackwidow_db_->PKScanRange(blackwidow::DataType::kStrings, key_start, key_end,
            "*", scan_batch_limit, &keys, &kvs, &next_key);
    if (!kvs.empty()) {
      scan_number_ += kvs.size();
      std::string data;
      for (const auto& kv : kvs) {
        slash::PutFixed32(&data, kv.key.size());
        data.append(kv.key);
        slash::PutFixed32(&data, kv.value.size());
        data.append(kv.value);
      }
      kvs.clear();
      key_start = next_key;
      write_thread_->Load(data);
    }
  } while (strcmp(next_key.data(), ""));
  is_finish_ = true;
  return NULL;
}

