//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <thread>

#include "storage/storage.h"

using namespace storage;

int main() {
  storage::Storage db;
  StorageOptions storage_options;
  storage_options.options.create_if_missing = true;
  storage::Status s = db.Open(storage_options, "./db");
  if (s.ok()) {
    printf("Open success\n");
  } else {
    printf("Open failed, error: %s\n", s.ToString().c_str());
    return -1;
  }
  // HSet
  int32_t res;
  s = db.HSet("TEST_KEY1", "TEST_FIELD1", "TEST_VALUE1", &res);
  printf("HSet return: %s, res = %d\n", s.ToString().c_str(), res);
  s = db.HSet("TEST_KEY1", "TEST_FIELD2", "TEST_VALUE2", &res);
  printf("HSet return: %s, res = %d\n", s.ToString().c_str(), res);

  s = db.HSet("TEST_KEY2", "TEST_FIELD1", "TEST_VALUE1", &res);
  printf("HSet return: %s, res = %d\n", s.ToString().c_str(), res);
  s = db.HSet("TEST_KEY2", "TEST_FIELD2", "TEST_VALUE2", &res);
  printf("HSet return: %s, res = %d\n", s.ToString().c_str(), res);
  s = db.HSet("TEST_KEY2", "TEST_FIELD3", "TEST_VALUE3", &res);
  printf("HSet return: %s, res = %d\n", s.ToString().c_str(), res);

  // HGet
  std::string value;
  s = db.HGet("TEST_KEY1", "TEST_FIELD1", &value);
  printf("HGet return: %s, value = %s\n", s.ToString().c_str(), value.c_str());
  s = db.HGet("TEST_KEY1", "TEST_FIELD2", &value);
  printf("HGet return: %s, value = %s\n", s.ToString().c_str(), value.c_str());
  s = db.HGet("TEST_KEY1", "TEST_FIELD3", &value);
  printf("HGet return: %s, value = %s\n", s.ToString().c_str(), value.c_str());
  s = db.HGet("TEST_KEY_NOT_EXIST", "TEST_FIELD", &value);
  printf("HGet return: %s, value = %s\n", s.ToString().c_str(), value.c_str());

  // HMSet
  std::vector<storage::FieldValue> fvs;
  fvs.push_back({"TEST_FIELD1", "TEST_VALUE1"});
  fvs.push_back({"TEST_FIELD2", "TEST_VALUE2"});
  s = db.HMSet("TEST_HASH", fvs);
  printf("HMset return: %s\n", s.ToString().c_str());

  // HMGet
  std::vector<storage::ValueStatus> vss;
  std::vector<std::string> fields;
  fields.push_back("TEST_FIELD1");
  fields.push_back("TEST_FIELD2");
  s = db.HMGet("TEST_HASH", fields, &vss);
  printf("HMget return: %s\n", s.ToString().c_str());
  for (uint32_t idx = 0; idx != fields.size(); idx++) {
    printf("idx = %d, field = %s, value = %s\n", idx, fields[idx].c_str(),
           vss[idx].value.c_str());
  }

  // HLEN
  s = db.HLen("TEST_HASH", &res);
  printf("HLen return : %s, len = %d\n", s.ToString().c_str(), res);

  // Compact
  s = db.Compact(storage::DataType::kHashes);
  printf("Compact return: %s\n", s.ToString().c_str());

  // Expire
  std::map<storage::DataType, Status> key_status;
  db.Expire("TEST_KEY1", 1, &key_status);
  printf("Expire return: %s\n", s.ToString().c_str());
  std::this_thread::sleep_for(std::chrono::milliseconds(2500));
  s = db.HGet("TEST_KEY1", "TEST_FIELD1", &value);
  printf("HGet return: %s, value = %s\n", s.ToString().c_str(), value.c_str());
  s = db.HGet("TEST_KEY1", "TEST_FIELD2", &value);
  printf("HGet return: %s, value = %s\n", s.ToString().c_str(), value.c_str());

  s = db.HGet("TEST_KEY2", "TEST_FIELD1", &value);
  printf("HGet return: %s, value = %s\n", s.ToString().c_str(), value.c_str());
  s = db.HGet("TEST_KEY2", "TEST_FIELD2", &value);
  printf("HGet return: %s, value = %s\n", s.ToString().c_str(), value.c_str());
  s = db.HGet("TEST_KEY2", "TEST_FIELD3", &value);
  printf("HGet return: %s, value = %s\n", s.ToString().c_str(), value.c_str());

  // Compact
  s = db.Compact(storage::DataType::kHashes);
  printf("Compact return: %s\n", s.ToString().c_str());

  s = db.HGet("TEST_KEY2", "TEST_FIELD1", &value);
  printf("HGet return: %s, value = %s\n", s.ToString().c_str(), value.c_str());
  s = db.HGet("TEST_KEY2", "TEST_FIELD2", &value);
  printf("HGet return: %s, value = %s\n", s.ToString().c_str(), value.c_str());
  s = db.HGet("TEST_KEY2", "TEST_FIELD3", &value);
  printf("HGet return: %s, value = %s\n", s.ToString().c_str(), value.c_str());

  // Exists
  s = db.HSet("TEST_KEY1", "TEST_FIELD1", "TEST_VALUE1", &res);
  printf("HSet return: %s, res = %d\n", s.ToString().c_str(), res);
  s = db.HExists("TEST_KEY1", "TEST_FIELD1");
  printf("HExists return: %s\n", s.ToString().c_str());

  // HIncrby
  int64_t hincrby_value;
  s = db.HIncrby("TEST_KEY1", "TEST_HINCRBY_FIELD", 100, &hincrby_value);
  printf("HIncrby return: %s, value = %lld\n", s.ToString().c_str(),
         hincrby_value);
  return 0;
}
