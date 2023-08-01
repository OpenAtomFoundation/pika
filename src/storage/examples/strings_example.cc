//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <thread>

#include "storage/storage.h"

using namespace storage;

int32_t main() {
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

  int32_t ret;
  // Set
  s = db.Set("TEST_KEY", "TEST_VALUE");
  printf("Set return: %s\n", s.ToString().c_str());

  // Get
  std::string value;
  s = db.Get("TEST_KEY", &value);
  printf("Get return: %s, value: %s\n", s.ToString().c_str(), value.c_str());

  // SetBit
  s = db.SetBit("SETBIT_KEY", 7, 1, &ret);
  printf("SetBit return: %s, ret: %d\n", s.ToString().c_str(), ret);

  // GetSet
  s = db.GetSet("TEST_KEY", "Hello", &value);
  printf("GetSet return: %s, old_value: %s", s.ToString().c_str(), value.c_str());

  // SetBit
  s = db.SetBit("SETBIT_KEY", 7, 1, &ret);
  printf("Setbit return: %s\n", s.ToString().c_str());

  // GetBit
  s = db.GetBit("SETBIT_KEY", 7, &ret);
  printf("GetBit return: %s, ret: %d\n", s.ToString().c_str(), ret);

  // MSet
  std::vector<storage::KeyValue> kvs;
  kvs.push_back({"TEST_KEY1", "TEST_VALUE1"});
  kvs.push_back({"TEST_KEY2", "TEST_VALUE2"});
  s = db.MSet(kvs);
  printf("MSet return: %s\n", s.ToString().c_str());

  // MGet
  std::vector<storage::ValueStatus> vss;
  std::vector<std::string> keys{"TEST_KEY1", "TEST_KEY2", "TEST_KEY_NOT_EXIST"};
  s = db.MGet(keys, &vss);
  printf("MGet return: %s\n", s.ToString().c_str());
  for (size_t idx = 0; idx != keys.size(); idx++) {
    printf("idx = %d, keys = %s, value = %s\n", idx, keys[idx].c_str(), vss[idx].value.c_str());
  }

  // Setnx
  s = db.Setnx("TEST_KEY", "TEST_VALUE", &ret);
  printf("Setnx return: %s, value: %s, ret: %d\n", s.ToString().c_str(), value.c_str(), ret);

  // MSetnx
  s = db.MSetnx(kvs, &ret);
  printf("MSetnx return: %s, ret: %d\n", s.ToString().c_str(), ret);

  // Setrange
  s = db.Setrange("TEST_KEY", 10, "APPEND_VALUE", &ret);
  printf("Setrange return: %s, ret: %d\n", s.ToString().c_str(), ret);

  // Getrange
  s = db.Getrange("TEST_KEY", 0, -1, &value);
  printf("Getrange return: %s, value: %s\n", s.ToString().c_str(), value.c_str());

  // Append
  std::string append_value;
  s = db.Set("TEST_KEY", "TEST_VALUE");
  s = db.Append("TEST_KEY", "APPEND_VALUE", &ret);
  s = db.Get("TEST_KEY", &append_value);
  printf("Append return: %s, value: %s, ret: %d\n", s.ToString().c_str(), append_value.c_str(), ret);

  // BitCount
  s = db.BitCount("TEST_KEY", 0, -1, &ret, false);
  printf("BitCount return: %s, ret: %d\n", s.ToString().c_str(), ret);

  // BitCount
  s = db.BitCount("TEST_KEY", 0, -1, &ret, true);
  printf("BitCount return: %s, ret: %d\n", s.ToString().c_str(), ret);

  // BitOp
  int64_t bitop_ret;
  s = db.Set("BITOP_KEY1", "FOOBAR");
  s = db.Set("BITOP_KEY2", "ABCDEF");
  s = db.Set("BITOP_KEY3", "STORAGE");
  std::vector<std::string> src_keys{"BITOP_KEY1", "BITOP_KEY2", "BITOP_KEY3"};
  // and
  s = db.BitOp(storage::BitOpType::kBitOpAnd, "BITOP_DESTKEY", src_keys, &bitop_ret);
  printf("BitOp return: %s, ret: %d\n", s.ToString().c_str(), bitop_ret);
  // or
  s = db.BitOp(storage::BitOpType::kBitOpOr, "BITOP_DESTKEY", src_keys, &bitop_ret);
  printf("BitOp return: %s, ret: %d\n", s.ToString().c_str(), bitop_ret);
  // xor
  s = db.BitOp(storage::BitOpType::kBitOpXor, "BITOP_DESTKEY", src_keys, &bitop_ret);
  printf("BitOp return: %s, ret: %d\n", s.ToString().c_str(), bitop_ret);
  // not
  std::vector<std::string> not_keys{"BITOP_KEY1"};
  s = db.BitOp(storage::BitOpType::kBitOpNot, "BITOP_DESTKEY", not_keys, &bitop_ret);
  printf("BitOp return: %s, ret: %d\n", s.ToString().c_str(), bitop_ret);

  // BitPos
  int64_t bitpos_ret;
  s = db.Set("BITPOS_KEY", "\xff\x00\x00");
  // bitpos key bit
  s = db.BitPos("BITPOS_KEY", 1, &bitpos_ret);
  printf("BitPos return: %s, ret: %d\n", s.ToString().c_str(), bitpos_ret);
  // bitpos key bit [start]
  s = db.BitPos("BITPOS_KEY", 1, 0, &bitpos_ret);
  printf("BitPos return: %s, ret: %d\n", s.ToString().c_str(), bitpos_ret);
  // bitpos key bit [start] [end]
  s = db.BitPos("BITPOS_KEY", 1, 0, 4, &bitpos_ret);
  printf("BitPos return: %s, ret: %d\n", s.ToString().c_str(), bitpos_ret);

  // Decrby
  int64_t decrby_ret;
  s = db.Set("TEST_KEY", "12345");
  s = db.Decrby("TEST_KEY", 5, &decrby_ret);
  printf("Decrby return: %s, ret: %d\n", s.ToString().c_str(), decrby_ret);

  // Incrby
  int64_t incrby_ret;
  s = db.Incrby("INCRBY_KEY", 5, &incrby_ret);
  printf("Incrby return: %s, ret: %d\n", s.ToString().c_str(), incrby_ret);

  // Incrbyfloat
  s = db.Set("INCRBYFLOAT_KEY", "10.50");
  s = db.Incrbyfloat("INCRBYFLOAT_KEY", "0.1", &value);
  printf("Incrbyfloat return: %s, value: %s\n", s.ToString().c_str(), value.c_str());

  // Setex
  s = db.Setex("TEST_KEY", "TEST_VALUE", 1);
  printf("Setex return: %s\n", s.ToString().c_str());
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  s = db.Get("TEST_KEY", &value);
  printf("Get return: %s, value: %s\n", s.ToString().c_str(), value.c_str());

  // Strlen
  s = db.Set("TEST_KEY", "TEST_VALUE");
  int32_t len = 0;
  s = db.Strlen("TEST_KEY", &len);
  printf("Strlen return: %s, strlen: %d\n", s.ToString().c_str(), len);

  // Expire
  std::map<storage::DataType, Status> key_status;
  s = db.Set("EXPIRE_KEY", "EXPIREVALUE");
  printf("Set return: %s\n", s.ToString().c_str());
  db.Expire("EXPIRE_KEY", 1, &key_status);
  std::this_thread::sleep_for(std::chrono::milliseconds(2500));
  s = db.Get("EXPIRE_KEY", &value);
  printf("Get return: %s, value: %s\n", s.ToString().c_str(), value.c_str());

  // Compact
  s = db.Compact(storage::DataType::kStrings);
  printf("Compact return: %s\n", s.ToString().c_str());

  return 0;
}
