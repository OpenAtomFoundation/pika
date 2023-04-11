//  Copyright (c) 2017-present The storage Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <functional>
#include <iostream>
#include <thread>
#include <vector>

#include "storage/storage.h"

const int KEYLENGTH = 1024 * 10;
const int VALUELENGTH = 1024 * 10;
const int THREADNUM = 20;
const int HASH_TABLE_FIELD_SIZE = 10000000;

using namespace storage;
using namespace std::chrono;

static const std::string key(KEYLENGTH, 'a');
static const std::string value(VALUELENGTH, 'a');

void BenchSet() {
  printf("====== Set ======\n");
  storage::Options options;
  options.create_if_missing = true;
  storage::Storage db;
  storage::Status s = db.Open(options, "./db");

  if (!s.ok()) {
    printf("Open db failed, error: %s\n", s.ToString().c_str());
    return;
  }

  std::vector<std::thread> jobs;
  size_t kv_num = 10000;
  jobs.clear();
  auto start = std::chrono::system_clock::now();
  for (size_t i = 0; i < THREADNUM; ++i) {
    jobs.emplace_back(
        [&db](size_t kv_num) {
          for (size_t j = 0; j < kv_num; ++j) {
            db.Set(key, value);
          }
        },
        kv_num);
  }

  for (auto& job : jobs) {
    job.join();
  }
  auto end = system_clock::now();
  duration<double> elapsed_seconds = end - start;
  auto cost = duration_cast<std::chrono::seconds>(elapsed_seconds).count();
  std::cout << "Test case 1, Set " << THREADNUM * kv_num << " Cost: " << cost
            << "s QPS: " << (THREADNUM * kv_num) / cost << std::endl;

  kv_num = 100000;
  jobs.clear();
  start = system_clock::now();
  for (size_t i = 0; i < THREADNUM; ++i) {
    jobs.emplace_back(
        [&db](size_t kv_num) {
          for (size_t j = 0; j < kv_num; ++j) {
            db.Set(key, value);
          }
        },
        kv_num);
  }

  for (auto& job : jobs) {
    job.join();
  }
  end = system_clock::now();
  elapsed_seconds = end - start;
  cost = duration_cast<seconds>(elapsed_seconds).count();
  std::cout << "Test case 2, Set " << THREADNUM * kv_num << " Cost: " << cost
            << "s QPS: " << (THREADNUM * kv_num) / cost << std::endl;
}

void BenchHGetall() {
  printf("====== HGetall ======\n");
  storage::Options options;
  options.create_if_missing = true;
  storage::Storage db;
  storage::Status s = db.Open(options, "./db");

  if (!s.ok()) {
    printf("Open db failed, error: %s\n", s.ToString().c_str());
    return;
  }

  int32_t ret = 0;
  Storage::FieldValue fv;
  std::vector<std::string> fields;
  std::vector<Storage::FieldValue> fvs_in;
  std::vector<Storage::FieldValue> fvs_out;

  // 1. Create the hash table then insert hash table 10000 field
  // 2. HGetall the hash table 10000 field (statistics cost time)
  fvs_in.clear();
  for (size_t i = 0; i < 10000; ++i) {
    fv.field = "field_" + std::to_string(i);
    fv.value = "value_" + std::to_string(i);
    fvs_in.push_back(fv);
  }
  db.HMSet("HGETALL_KEY1", fvs_in);

  fvs_out.clear();
  auto start = system_clock::now();
  db.HGetall("HGETALL_KEY1", &fvs_out);
  auto end = system_clock::now();
  duration<double> elapsed_seconds = end - start;
  auto cost = duration_cast<milliseconds>(elapsed_seconds).count();
  std::cout << "Test case 1, HGetall " << fvs_out.size() << " Field HashTable Cost: " << cost << "ms" << std::endl;

  // 1. Create the hash table then insert hash table 10000000 field
  // 2. Delete the hash table
  // 3. Create the hash table whos key same as before,
  //    then insert the hash table 10000 field
  // 4. HGetall the hash table 10000 field (statistics cost time)
  fvs_in.clear();
  for (size_t i = 0; i < HASH_TABLE_FIELD_SIZE; ++i) {
    fv.field = "field_" + std::to_string(i);
    fv.value = "value_" + std::to_string(i);
    fvs_in.push_back(fv);
  }
  db.HMSet("HGETALL_KEY2", fvs_in);
  std::vector<Slice> del_keys({"HGETALL_KEY2"});
  std::map<Storage::DataType, Status> type_status;
  db.Del(del_keys, &type_status);
  fvs_in.clear();
  for (size_t i = 0; i < 10000; ++i) {
    fv.field = "field_" + std::to_string(i);
    fv.value = "value_" + std::to_string(i);
    fvs_in.push_back(fv);
  }
  db.HMSet("HGETALL_KEY2", fvs_in);

  fvs_out.clear();
  start = system_clock::now();
  db.HGetall("HGETALL_KEY2", &fvs_out);
  end = system_clock::now();
  elapsed_seconds = end - start;
  cost = duration_cast<milliseconds>(elapsed_seconds).count();
  std::cout << "Test case 2, HGetall " << fvs_out.size() << " Field HashTable Cost: " << cost << "ms" << std::endl;

  // 1. Create the hash table then insert hash table 10000000 field
  // 2. Delete hash table 9990000 field, the hash table remain 10000 field
  // 3. HGetall the hash table 10000 field (statistics cost time)
  fvs_in.clear();
  for (size_t i = 0; i < HASH_TABLE_FIELD_SIZE; ++i) {
    fv.field = "field_" + std::to_string(i);
    fv.value = "value_" + std::to_string(i);
    fvs_in.push_back(fv);
  }
  db.HMSet("HGETALL_KEY3", fvs_in);
  fields.clear();
  for (size_t i = 0; i < HASH_TABLE_FIELD_SIZE - 10000; ++i) {
    fields.push_back("field_" + std::to_string(i));
  }
  db.HDel("HGETALL_KEY3", fields, &ret);

  fvs_out.clear();
  start = system_clock::now();
  db.HGetall("HGETALL_KEY3", &fvs_out);
  end = system_clock::now();
  elapsed_seconds = end - start;
  cost = duration_cast<milliseconds>(elapsed_seconds).count();
  std::cout << "Test case 3, HGetall " << fvs_out.size() << " Field HashTable Cost: " << cost << "ms" << std::endl;
}

void BenchScan() {
  printf("====== Scan ======\n");
  storage::Options options;
  options.create_if_missing = true;
  storage::Storage db;
  storage::Status s = db.Open(options, "./db");

  if (!s.ok()) {
    printf("Open db failed, error: %s\n", s.ToString().c_str());
    return;
  }

  std::vector<std::thread> jobs;
  size_t kv_num = 10000000;
  jobs.clear();
  auto start = std::chrono::system_clock::now();
  for (size_t i = 0; i < THREADNUM; ++i) {
    jobs.emplace_back(
        [&db](size_t kv_num) {
          for (size_t j = 0; j < kv_num; ++j) {
            std::string key_prefix = key + std::to_string(j);
            db.Set(key_prefix, value);
          }
        },
        kv_num);
  }

  for (auto& job : jobs) {
    job.join();
  }
  auto end = system_clock::now();
  duration<double> elapsed_seconds = end - start;
  auto cost = duration_cast<std::chrono::seconds>(elapsed_seconds).count();
  std::cout << "Test case 1, Set " << THREADNUM * kv_num << " Cost: " << cost
            << "s QPS: " << (THREADNUM * kv_num) / cost << std::endl;

  // Scan 100000
  std::vector<std::string> keys;
  start = system_clock::now();
  db.Scan(0, "*", 100000, &keys);
  end = system_clock::now();
  elapsed_seconds = end - start;
  cost = duration_cast<seconds>(elapsed_seconds).count();
  std::cout << "Test case 2, Scan " << 100000 << " Cost: " << cost << "s" << std::endl;

  // Scan 10000000
  keys.clear();
  start = system_clock::now();
  db.Scan(0, "*", kv_num, &keys);
  end = system_clock::now();
  elapsed_seconds = end - start;
  cost = duration_cast<seconds>(elapsed_seconds).count();
  std::cout << "Test case 3, Scan " << kv_num << " Cost: " << cost << "s" << std::endl;
}

int main(int argc, char** argv) {
  // keys
  BenchSet();

  // hashes
  BenchHGetall();

  // Iterator
  BenchScan();
}
