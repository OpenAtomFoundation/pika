//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <gtest/gtest.h>
#include <iostream>
#include <thread>

#include "glog/logging.h"

#include "pstd/include/pika_codis_slot.h"
#include "pstd/include/env.h"
#include "storage/storage.h"
#include "storage/util.h"

// using namespace storage;
using storage::DataType;
using storage::Slice;
using storage::Status;

class KeysTest : public ::testing::Test {
 public:
  KeysTest() = default;
  ~KeysTest() override = default;

  void SetUp() override {
    std::string path = "./db/keys";
    pstd::DeleteDirIfExist(path);
    mkdir(path.c_str(), 0755);
    storage_options.options.create_if_missing = true;
    s = db.Open(storage_options, path);
  }

  void TearDown() override {
    std::string path = "./db/keys";
    storage::DeleteFiles(path.c_str());
  }

  static void SetUpTestSuite() {}
  static void TearDownTestSuite() {}

  storage::StorageOptions storage_options;
  storage::Storage db;
  storage::Status s;
};

static bool make_expired(storage::Storage* const db, const Slice& key) {
  std::map<storage::DataType, rocksdb::Status> type_status;
  int32_t ret = db->Expire(key, 1);
  if ((ret == 0) || !type_status[storage::DataType::kStrings].ok()) {
    return false;
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  return true;
}

static bool key_value_match(const std::vector<storage::KeyValue>& key_value_out, const std::vector<storage::KeyValue>& expect_key_value) {
  if (key_value_out.size() != expect_key_value.size()) {
    LOG(WARNING) << "key_value_out.size: " << key_value_out.size() << " expect_key_value.size: " << expect_key_value.size();
    return false;
  }
  for (int32_t idx = 0; idx < key_value_out.size(); ++idx) {
    LOG(WARNING) << "key_value_out[idx]: "<< key_value_out[idx].key << " expect_key_value[idx]: " << expect_key_value[idx].key;
    LOG(WARNING) << "key_value_out[idx]: "<< key_value_out[idx].value << " expect_key_value[idx]: " << expect_key_value[idx].value;
    if (key_value_out[idx].key != expect_key_value[idx].key ||
        key_value_out[idx].value != expect_key_value[idx].value) {
      return false;
    }
  }
  return true;
}

static bool key_match(const std::vector<std::string>& keys_out, const std::vector<std::string>& expect_keys) {
  if (keys_out.size() != expect_keys.size()) {
    return false;
  }
  for (int32_t idx = 0; idx < keys_out.size(); ++idx) {
    if (keys_out[idx] != expect_keys[idx]) {
      return false;
    }
  }
  return true;
}

// PKScanRange
// Note: This test needs to execute at first because all of the data is
// predetermined.
TEST_F(KeysTest, PKScanRangeTest) {  // NOLINT
  int32_t ret;
  uint64_t ret_u64;
  std::string next_key;
  std::vector<std::string> keys_del;
  std::vector<std::string> keys_out;
  std::vector<std::string> expect_keys;
  std::map<DataType, storage::Status> type_status;
  std::vector<storage::KeyValue> kvs_out;
  std::vector<storage::KeyValue> expect_kvs;
  std::vector<storage::KeyValue> kvs{{"PKSCANRANGE_A", "VALUE"}, {"PKSCANRANGE_C", "VALUE"}, {"PKSCANRANGE_E", "VALUE"},
                                     {"PKSCANRANGE_G", "VALUE"}, {"PKSCANRANGE_I", "VALUE"}, {"PKSCANRANGE_K", "VALUE"},
                                     {"PKSCANRANGE_M", "VALUE"}, {"PKSCANRANGE_O", "VALUE"}, {"PKSCANRANGE_Q", "VALUE"},
                                     {"PKSCANRANGE_S", "VALUE"}};
  keys_del.reserve(kvs.size());
for (const auto& kv : kvs) {
    keys_del.push_back(kv.key);
  }

  //=============================== Strings ===============================
  s = db.MSet(kvs);
  ASSERT_TRUE(s.ok());

  // ************************** Group 1 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //    ^                                                         ^
  // key_start                                             key_end/next_key
  kvs_out.clear();
  expect_kvs.clear();
  s = db.PKScanRange(DataType::kStrings, "", "", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 0; idx <= 9; ++idx) {
    expect_kvs.push_back(kvs[idx]);
  }
  ASSERT_TRUE(key_value_match(kvs_out, expect_kvs));
  ASSERT_EQ(next_key, "");

  // ************************** Group 2 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //         ^                                                    ^
  //      key_start                                        key_end/next_key
  kvs_out.clear();
  expect_kvs.clear();
  s = db.PKScanRange(DataType::kStrings, "PKSCANRANGE_B", "", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 9; ++idx) {
    expect_kvs.push_back(kvs[idx]);
  }
  ASSERT_TRUE(key_value_match(kvs_out, expect_kvs));
  ASSERT_EQ(next_key, "");

  // ************************** Group 3 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //    ^                                                    ^
  // key_start                                            key_end
  kvs_out.clear();
  expect_kvs.clear();
  s = db.PKScanRange(DataType::kStrings, "", "PKSCANRANGE_R", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 0; idx <= 8; ++idx) {
    expect_kvs.push_back(kvs[idx]);
  }
  ASSERT_TRUE(key_value_match(kvs_out, expect_kvs));
  ASSERT_EQ(next_key, "");

  // ************************** Group 4 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //               ^                                   ^
  //            key_start                           key_end
  kvs_out.clear();
  expect_kvs.clear();
  s = db.PKScanRange(DataType::kStrings, "PKSCANRANGE_D", "PKSCANRANGE_P", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 2; idx <= 7; ++idx) {
    expect_kvs.push_back(kvs[idx]);
  }
  ASSERT_TRUE(key_value_match(kvs_out, expect_kvs));
  ASSERT_EQ(next_key, "");

  // ************************** Group 5 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^                                         ^
  //         key_start                                 key_end
  kvs_out.clear();
  expect_kvs.clear();
  s = db.PKScanRange(DataType::kStrings, "PKSCANRANGE_C", "PKSCANRANGE_Q", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 8; ++idx) {
    expect_kvs.push_back(kvs[idx]);
  }
  ASSERT_TRUE(key_value_match(kvs_out, expect_kvs));
  ASSERT_EQ(next_key, "");

  // ************************** Group 6 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //                              ^     ^
  //                      key_start     key_end
  kvs_out.clear();
  expect_kvs.clear();
  s = db.PKScanRange(DataType::kStrings, "PKSCANRANGE_I", "PKSCANRANGE_K", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 4; idx <= 5; ++idx) {
    expect_kvs.push_back(kvs[idx]);
  }
  ASSERT_TRUE(key_value_match(kvs_out, expect_kvs));
  ASSERT_EQ(next_key, "");

  // ************************** Group 7 Test **************************
  //      0     1     2     3     4        5     6     7     8     9
  //      A     C     E     G     I        K     M     O     Q     S
  //                              ^
  //                     key_start/key_end
  kvs_out.clear();
  expect_kvs.clear();
  s = db.PKScanRange(DataType::kStrings, "PKSCANRANGE_I", "PKSCANRANGE_I", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 4; idx <= 4; ++idx) {
    expect_kvs.push_back(kvs[idx]);
  }
  ASSERT_TRUE(key_value_match(kvs_out, expect_kvs));
  ASSERT_EQ(next_key, "");

  // ************************** Group 8 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //                              ^     ^
  //                        key_end     key_start
  kvs_out.clear();
  expect_kvs.clear();
  s = db.PKScanRange(DataType::kStrings, "PKSCANRANGE_K", "PKSCANRANGE_I", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_TRUE(key_value_match(kvs_out, expect_kvs));
  ASSERT_EQ(next_key, "");

  // ************************** Group 9 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^                             ^           ^
  //         key_start                    next_key     key_end
  kvs_out.clear();
  expect_kvs.clear();
  s = db.PKScanRange(DataType::kStrings, "PKSCANRANGE_C", "PKSCANRANGE_Q", "*", 5, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 5; ++idx) {
    expect_kvs.push_back(kvs[idx]);
  }
  ASSERT_TRUE(key_value_match(kvs_out, expect_kvs));
  ASSERT_TRUE(key_value_match(kvs_out, expect_kvs));
  ASSERT_EQ(next_key, "PKSCANRANGE_M");

  // ************************** Group 10 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^           ^                       ^     ^
  //         key_start   expire              next_key  key_end
  ASSERT_TRUE(make_expired(&db, "PKSCANRANGE_G"));
  kvs_out.clear();
  expect_kvs.clear();
  s = db.PKScanRange(DataType::kStrings, "PKSCANRANGE_C", "PKSCANRANGE_Q", "*", 5, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 6; ++idx) {
    if (idx != 3) {
      expect_kvs.push_back(kvs[idx]);
    }
  }
  ASSERT_TRUE(key_value_match(kvs_out, expect_kvs));
  ASSERT_EQ(next_key, "PKSCANRANGE_O");

  // ************************** Group 11 Test **************************
  //      0     1     2     3        4     5     6     7     8     9
  //      A     C     E     G        I     K     M     O     Q     S
  //            ^           ^        ^                       ^
  //         key_start   expire  next_key                 key_end
  kvs_out.clear();
  expect_kvs.clear();
  s = db.PKScanRange(DataType::kStrings, "PKSCANRANGE_C", "PKSCANRANGE_Q", "*", 2, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 2; ++idx) {
    expect_kvs.push_back(kvs[idx]);
  }
  ASSERT_TRUE(key_value_match(kvs_out, expect_kvs));
  ASSERT_EQ(next_key, "PKSCANRANGE_I");

  //=============================== Sets ===============================
  std::vector<storage::KeyValue> kvset{{"PKSCANRANGE_A1", "VALUE"}, {"PKSCANRANGE_C1", "VALUE"}, {"PKSCANRANGE_E1", "VALUE"},
                                     {"PKSCANRANGE_G1", "VALUE"}, {"PKSCANRANGE_I1", "VALUE"}, {"PKSCANRANGE_K1", "VALUE"},
                                     {"PKSCANRANGE_M1", "VALUE"}, {"PKSCANRANGE_O1", "VALUE"}, {"PKSCANRANGE_Q1", "VALUE"},
                                     {"PKSCANRANGE_S1", "VALUE"}};
  for (const auto& kv : kvset) {
    s = db.SAdd(kv.key, {"MEMBER"}, &ret);
  }

  // ************************** Group 1 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //    ^                                                         ^
  // key_start                                             key_end/next_key
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kSets, "", "", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 0; idx <= 9; ++idx) {
    expect_keys.push_back(kvset[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 2 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //         ^                                                    ^
  //      key_start                                        key_end/next_key
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kSets, "PKSCANRANGE_B1", "", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 9; ++idx) {
    expect_keys.push_back(kvset[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 3 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //    ^                                                    ^
  // key_start                                            key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kSets, "", "PKSCANRANGE_R1", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 0; idx <= 8; ++idx) {
    expect_keys.push_back(kvset[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 4 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //               ^                                   ^
  //            key_start                           key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kSets, "PKSCANRANGE_D1", "PKSCANRANGE_P1", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 2; idx <= 7; ++idx) {
    expect_keys.push_back(kvset[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 5 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^                                         ^
  //         key_start                                 key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kSets, "PKSCANRANGE_C1", "PKSCANRANGE_Q1", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 8; ++idx) {
    expect_keys.push_back(kvset[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 6 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //                              ^     ^
  //                      key_start  key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kSets, "PKSCANRANGE_I1", "PKSCANRANGE_K1", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 4; idx <= 5; ++idx) {
    expect_keys.push_back(kvset[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 7 Test **************************
  //      0     1     2     3     4        5     6     7     8     9
  //      A     C     E     G     I        K     M     O     Q     S
  //                              ^
  //                     key_start/key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kSets, "PKSCANRANGE_I1", "PKSCANRANGE_I1", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 4; idx <= 4; ++idx) {
    expect_keys.push_back(kvset[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 8 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //                              ^     ^
  //                        key_end     key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kSets, "PKSCANRANGE_K1", "PKSCANRANGE_I1", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 9 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^                             ^           ^
  //         key_start                    next_key     key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kSets, "PKSCANRANGE_C1", "PKSCANRANGE_Q1", "*", 5, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 5; ++idx) {
    expect_keys.push_back(kvset[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKSCANRANGE_M1");

  // ************************** Group 10 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^           ^                       ^     ^
  //         key_start   expire              next_key  key_end
  ASSERT_TRUE(make_expired(&db, "PKSCANRANGE_G1"));
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kSets, "PKSCANRANGE_C1", "PKSCANRANGE_Q1", "*", 5, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 6; ++idx) {
    if (idx != 3) {
      expect_keys.push_back(kvset[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKSCANRANGE_O1");

  // ************************** Group 11 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^           ^     ^                       ^
  //         key_start   expire next_key               key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kSets, "PKSCANRANGE_C1", "PKSCANRANGE_Q1", "*", 2, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 2; ++idx) {
    if (idx != 3) {
      expect_keys.push_back(kvset[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKSCANRANGE_I1");

  // ************************** Group 12 Test **************************
  //      0     1     2     3      4        5     6     7     8     9
  //      A     C     E     G      I        K     M     O     Q     S
  //            ^           ^      ^        ^                 ^
  //         key_start   expire deleted next_key           key_end
  keys_out.clear();
  expect_keys.clear();
  db.Del({"PKSCANRANGE_I1"});
  s = db.PKScanRange(DataType::kSets, "PKSCANRANGE_C1", "PKSCANRANGE_Q1", "*", 2, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 2; ++idx) {
    if (idx != 3) {
      expect_keys.push_back(kvset[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKSCANRANGE_K1");

  //=============================== Hashes ===============================
  std::vector<storage::KeyValue> kvhash{{"PKSCANRANGE_A2", "VALUE"}, {"PKSCANRANGE_C2", "VALUE"}, {"PKSCANRANGE_E2", "VALUE"},
                                       {"PKSCANRANGE_G2", "VALUE"}, {"PKSCANRANGE_I2", "VALUE"}, {"PKSCANRANGE_K2", "VALUE"},
                                       {"PKSCANRANGE_M2", "VALUE"}, {"PKSCANRANGE_O2", "VALUE"}, {"PKSCANRANGE_Q2", "VALUE"},
                                       {"PKSCANRANGE_S2", "VALUE"}};
  for (const auto& kv : kvhash) {
    s = db.HMSet(kv.key, {{"FIELD", "VALUE"}});
  }

  // ************************** Group 1 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //    ^                                                         ^
  // key_start                                             key_end/next_key
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kHashes, "", "", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 0; idx <= 9; ++idx) {
    expect_keys.push_back(kvhash[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 2 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //         ^                                                    ^
  //      key_start                                        key_end/next_key
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kHashes, "PKSCANRANGE_B2", "", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 9; ++idx) {
    expect_keys.push_back(kvhash[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 3 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //    ^                                                    ^
  // key_start                                            key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kHashes, "", "PKSCANRANGE_R2", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 0; idx <= 8; ++idx) {
    expect_keys.push_back(kvhash[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 4 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //               ^                                   ^
  //            key_start                           key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kHashes, "PKSCANRANGE_D2", "PKSCANRANGE_P2", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 2; idx <= 7; ++idx) {
    expect_keys.push_back(kvhash[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 5 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^                                         ^
  //         key_start                                 key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kHashes, "PKSCANRANGE_C2", "PKSCANRANGE_Q2", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 8; ++idx) {
    expect_keys.push_back(kvhash[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 6 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //                              ^     ^
  //                      key_start  key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kHashes, "PKSCANRANGE_I2", "PKSCANRANGE_K2", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 4; idx <= 5; ++idx) {
    expect_keys.push_back(kvhash[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 7 Test **************************
  //      0     1     2     3     4        5     6     7     8     9
  //      A     C     E     G     I        K     M     O     Q     S
  //                              ^
  //                     key_start/key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kHashes, "PKSCANRANGE_I2", "PKSCANRANGE_I2", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 4; idx <= 4; ++idx) {
    expect_keys.push_back(kvhash[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 8 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //                              ^     ^
  //                        key_end     key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kHashes, "PKSCANRANGE_K2", "PKSCANRANGE_I2", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 9 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^                             ^           ^
  //         key_start                    next_key     key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kHashes, "PKSCANRANGE_C2", "PKSCANRANGE_Q2", "*", 5, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 5; ++idx) {
    expect_keys.push_back(kvhash[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKSCANRANGE_M2");

  // ************************** Group 10 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^           ^                       ^     ^
  //         key_start   expire              next_key  key_end
  ASSERT_TRUE(make_expired(&db, "PKSCANRANGE_G2"));
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kHashes, "PKSCANRANGE_C2", "PKSCANRANGE_Q2", "*", 5, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 6; ++idx) {
    if (idx != 3) {
      expect_keys.push_back(kvhash[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKSCANRANGE_O2");

  // ************************** Group 11 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^           ^     ^                       ^
  //         key_start   expire next_key               key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kHashes, "PKSCANRANGE_C2", "PKSCANRANGE_Q2", "*", 2, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 2; ++idx) {
    expect_keys.push_back(kvhash[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKSCANRANGE_I2");

  // ************************** Group 12 Test **************************
  //      0     1     2     3      4        5     6     7     8     9
  //      A     C     E     G      I        K     M     O     Q     S
  //            ^           ^      ^        ^                 ^
  //         key_start   expire deleted next_key           key_end
  keys_out.clear();
  expect_keys.clear();
  db.Del({"PKSCANRANGE_I2"});
  s = db.PKScanRange(DataType::kHashes, "PKSCANRANGE_C2", "PKSCANRANGE_Q2", "*", 2, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 2; ++idx) {
    expect_keys.push_back(kvhash[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKSCANRANGE_K2");

  //=============================== ZSets ===============================


  std::vector<storage::KeyValue> kvzset{{"PKSCANRANGE_A3", "VALUE"}, {"PKSCANRANGE_C3", "VALUE"}, {"PKSCANRANGE_E3", "VALUE"},
                                       {"PKSCANRANGE_G3", "VALUE"}, {"PKSCANRANGE_I3", "VALUE"}, {"PKSCANRANGE_K3", "VALUE"},
                                       {"PKSCANRANGE_M3", "VALUE"}, {"PKSCANRANGE_O3", "VALUE"}, {"PKSCANRANGE_Q3", "VALUE"},
                                       {"PKSCANRANGE_S3", "VALUE"}};
  for (const auto& kv : kvzset) {
    s = db.ZAdd(kv.key, {{1, "MEMBER"}}, &ret);
  }

  // ************************** Group 1 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //    ^                                                         ^
  // key_start                                             key_end/next_key
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kZSets, "", "", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 0; idx <= 9; ++idx) {
    expect_keys.push_back(kvzset[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 2 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //         ^                                                    ^
  //      key_start                                        key_end/next_key
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kZSets, "PKSCANRANGE_B3", "", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 9; ++idx) {
    expect_keys.push_back(kvzset[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 3 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //    ^                                                    ^
  // key_start                                            key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kZSets, "", "PKSCANRANGE_R3", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 0; idx <= 8; ++idx) {
    expect_keys.push_back(kvzset[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 4 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //               ^                                   ^
  //            key_start                           key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kZSets, "PKSCANRANGE_D3", "PKSCANRANGE_P3", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 2; idx <= 7; ++idx) {
    expect_keys.push_back(kvzset[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 5 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^                                         ^
  //         key_start                                 key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kZSets, "PKSCANRANGE_C3", "PKSCANRANGE_Q3", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 8; ++idx) {
    expect_keys.push_back(kvzset[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 6 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //                              ^     ^
  //                      key_start  key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kZSets, "PKSCANRANGE_I3", "PKSCANRANGE_K3", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 4; idx <= 5; ++idx) {
    expect_keys.push_back(kvzset[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 7 Test **************************
  //      0     1     2     3     4        5     6     7     8     9
  //      A     C     E     G     I        K     M     O     Q     S
  //                              ^
  //                     key_start/key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kZSets, "PKSCANRANGE_I3", "PKSCANRANGE_I3", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 4; idx <= 4; ++idx) {
    expect_keys.push_back(kvzset[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 8 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //                              ^     ^
  //                        key_end     key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kZSets, "PKSCANRANGE_K3", "PKSCANRANGE_I3", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 9 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^                             ^           ^
  //         key_start                    next_key     key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kZSets, "PKSCANRANGE_C3", "PKSCANRANGE_Q3", "*", 5, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 5; ++idx) {
    expect_keys.push_back(kvzset[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKSCANRANGE_M3");

  // ************************** Group 10 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^           ^                       ^     ^
  //         key_start   expire              next_key  key_end
  ASSERT_TRUE(make_expired(&db, "PKSCANRANGE_G3"));
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kZSets, "PKSCANRANGE_C3", "PKSCANRANGE_Q3", "*", 5, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 6; ++idx) {
    if (idx != 3) {
      expect_keys.push_back(kvzset[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKSCANRANGE_O3");

  // ************************** Group 11 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^           ^     ^                       ^
  //         key_start   expire next_key               key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kZSets, "PKSCANRANGE_C3", "PKSCANRANGE_Q3", "*", 2, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 2; ++idx) {
    expect_keys.push_back(kvzset[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKSCANRANGE_I3");

  // ************************** Group 12 Test **************************
  //      0     1     2     3      4        5     6     7     8     9
  //      A     C     E     G      I        K     M     O     Q     S
  //            ^           ^      ^        ^                 ^
  //         key_start   expire deleted next_key           key_end
  keys_out.clear();
  expect_keys.clear();
  db.Del({"PKSCANRANGE_I3"});
  s = db.PKScanRange(DataType::kZSets, "PKSCANRANGE_C3", "PKSCANRANGE_Q3", "*", 2, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 2; ++idx) {
    if (idx != 3) {
      expect_keys.push_back(kvzset[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKSCANRANGE_K3");

  //=============================== Lists  ===============================
  std::vector<storage::KeyValue> kvlist{{"PKSCANRANGE_A4", "VALUE"}, {"PKSCANRANGE_C4", "VALUE"}, {"PKSCANRANGE_E4", "VALUE"},
                                       {"PKSCANRANGE_G4", "VALUE"}, {"PKSCANRANGE_I4", "VALUE"}, {"PKSCANRANGE_K4", "VALUE"},
                                       {"PKSCANRANGE_M4", "VALUE"}, {"PKSCANRANGE_O4", "VALUE"}, {"PKSCANRANGE_Q4", "VALUE"},
                                       {"PKSCANRANGE_S4", "VALUE"}};
  for (const auto& kv : kvlist) {
    s = db.LPush(kv.key, {"NODE"}, &ret_u64);
  }

  // ************************** Group 1 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //    ^                                                         ^
  // key_start                                             key_end/next_key
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kLists, "", "", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 0; idx <= 9; ++idx) {
    expect_keys.push_back(kvlist[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 2 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //         ^                                                    ^
  //      key_start                                        key_end/next_key
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kLists, "PKSCANRANGE_B4", "", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 9; ++idx) {
    expect_keys.push_back(kvlist[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 3 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //    ^                                                    ^
  // key_start                                            key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kLists, "", "PKSCANRANGE_R4", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 0; idx <= 8; ++idx) {
    expect_keys.push_back(kvlist[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 4 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //               ^                                   ^
  //            key_start                           key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kLists, "PKSCANRANGE_D4", "PKSCANRANGE_P4", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 2; idx <= 7; ++idx) {
    expect_keys.push_back(kvlist[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 5 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^                                         ^
  //         key_start                                 key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kLists, "PKSCANRANGE_C4", "PKSCANRANGE_Q4", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 8; ++idx) {
    expect_keys.push_back(kvlist[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 6 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //                              ^     ^
  //                      key_start  key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kLists, "PKSCANRANGE_I4", "PKSCANRANGE_K4", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 4; idx <= 5; ++idx) {
    expect_keys.push_back(kvlist[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 7 Test **************************
  //      0     1     2     3     4        5     6     7     8     9
  //      A     C     E     G     I        K     M     O     Q     S
  //                              ^
  //                     key_start/key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kLists, "PKSCANRANGE_I4", "PKSCANRANGE_I4", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 4; idx <= 4; ++idx) {
    expect_keys.push_back(kvlist[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 8 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //                              ^     ^
  //                        key_end     key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kLists, "PKSCANRANGE_K4", "PKSCANRANGE_I4", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 9 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^                             ^           ^
  //         key_start                    next_key     key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kLists, "PKSCANRANGE_C4", "PKSCANRANGE_Q4", "*", 5, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 5; ++idx) {
    expect_keys.push_back(kvlist[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKSCANRANGE_M4");

  // ************************** Group 10 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^           ^                       ^     ^
  //         key_start   expire              next_key  key_end
  ASSERT_TRUE(make_expired(&db, "PKSCANRANGE_G4"));
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kLists, "PKSCANRANGE_C4", "PKSCANRANGE_Q4", "*", 5, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 6; ++idx) {
    if (idx != 3) {
      expect_keys.push_back(kvlist[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKSCANRANGE_O4");

  // ************************** Group 11 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^           ^     ^                       ^
  //         key_start   expire next_key               key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kLists, "PKSCANRANGE_C4", "PKSCANRANGE_Q4", "*", 2, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 2; ++idx) {
    expect_keys.push_back(kvlist[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKSCANRANGE_I4");

  // ************************** Group 12 Test **************************
  //      0     1     2     3      4        5     6     7     8     9
  //      A     C     E     G      I        K     M     O     Q     S
  //            ^           ^      ^        ^                 ^
  //         key_start   expire deleted next_key           key_end
  keys_out.clear();
  expect_keys.clear();
  db.Del({"PKSCANRANGE_I4"});
  s = db.PKScanRange(DataType::kLists, "PKSCANRANGE_C4", "PKSCANRANGE_Q4", "*", 2, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 2; ++idx) {
    if (idx != 3) {
      expect_keys.push_back(kvlist[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKSCANRANGE_K4");

  type_status.clear();
  db.Del(keys_del);
  sleep(2);
  db.Compact(DataType::kAll, true);
}

// PKRScanRange
// Note: This test needs to execute at first because all of the data is
// predetermined.
TEST_F(KeysTest, PKRScanRangeTest) {  // NOLINT
  int32_t ret;
  uint64_t ret_u64;
  std::string next_key;
  std::vector<std::string> keys_del;
  std::vector<std::string> keys_out;
  std::vector<std::string> expect_keys;
  std::map<DataType, Status> type_status;
  std::vector<storage::KeyValue> kvs_out;
  std::vector<storage::KeyValue> expect_kvs;
  std::vector<storage::KeyValue> kvs{{"PKRSCANRANGE_A", "VALUE"}, {"PKRSCANRANGE_C", "VALUE"},
                                     {"PKRSCANRANGE_E", "VALUE"}, {"PKRSCANRANGE_G", "VALUE"},
                                     {"PKRSCANRANGE_I", "VALUE"}, {"PKRSCANRANGE_K", "VALUE"},
                                     {"PKRSCANRANGE_M", "VALUE"}, {"PKRSCANRANGE_O", "VALUE"},
                                     {"PKRSCANRANGE_Q", "VALUE"}, {"PKRSCANRANGE_S", "VALUE"}};
  keys_del.reserve(kvs.size());
for (const auto& kv : kvs) {
    keys_del.push_back(kv.key);
  }

  //=============================== Strings ===============================
  s = db.MSet(kvs);
  ASSERT_TRUE(s.ok());

  // ************************** Group 1 Test **************************
  //         0     1     2     3     4     5     6     7     8     9
  //         A     C     E     G     I     K     M     O     Q     S
  //       ^                                                         ^
  // key_end/next_key                                             key_start
  kvs_out.clear();
  expect_kvs.clear();
  s = db.PKRScanRange(DataType::kStrings, "", "", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 9; idx >= 0; --idx) {
    expect_kvs.push_back(kvs[idx]);
  }
  ASSERT_TRUE(key_value_match(kvs_out, expect_kvs));
  ASSERT_EQ(next_key, "");

  // ************************** Group 2 Test **************************
  //       0     1     2     3     4     5     6     7     8     9
  //       A     C     E     G     I     K     M     O     Q     S
  //          ^                                                    ^
  //       key_end                                              key_start
  kvs_out.clear();
  expect_kvs.clear();
  s = db.PKRScanRange(DataType::kStrings, "", "PKRSCANRANGE_B", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 9; idx >= 1; --idx) {
    expect_kvs.push_back(kvs[idx]);
  }
  ASSERT_TRUE(key_value_match(kvs_out, expect_kvs));
  ASSERT_EQ(next_key, "");

  // ************************** Group 3 Test **************************
  //         0     1     2     3     4     5     6     7     8     9
  //         A     C     E     G     I     K     M     O     Q     S
  //       ^                                                    ^
  // key_end/next_key                                         key_start
  kvs_out.clear();
  expect_kvs.clear();
  s = db.PKRScanRange(DataType::kStrings, "PKRSCANRANGE_R", "", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 0; --idx) {
    expect_kvs.push_back(kvs[idx]);
  }
  ASSERT_TRUE(key_value_match(kvs_out, expect_kvs));
  ASSERT_EQ(next_key, "");

  // ************************** Group 4 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //               ^                                   ^
  //            key_end                             key_start
  kvs_out.clear();
  expect_kvs.clear();
  s = db.PKRScanRange(DataType::kStrings, "PKRSCANRANGE_P", "PKRSCANRANGE_D", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 7; idx >= 2; --idx) {
    expect_kvs.push_back(kvs[idx]);
  }
  ASSERT_TRUE(key_value_match(kvs_out, expect_kvs));
  ASSERT_EQ(next_key, "");

  // ************************** Group 5 Test **************************
  //       0     1     2     3     4     5     6     7     8     9
  //       A     C     E     G     I     K     M     O     Q     S
  //             ^                                         ^
  //          key_end                                   key_start
  kvs_out.clear();
  expect_kvs.clear();
  s = db.PKRScanRange(DataType::kStrings, "PKRSCANRANGE_Q", "PKRSCANRANGE_C", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 1; --idx) {
    expect_kvs.push_back(kvs[idx]);
  }
  ASSERT_TRUE(key_value_match(kvs_out, expect_kvs));
  ASSERT_EQ(next_key, "");

  // ************************** Group 6 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //                              ^     ^
  //                          key_end key_start
  kvs_out.clear();
  expect_kvs.clear();
  s = db.PKRScanRange(DataType::kStrings, "PKRSCANRANGE_K", "PKRSCANRANGE_I", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 5; idx >= 4; --idx) {
    expect_kvs.push_back(kvs[idx]);
  }
  ASSERT_TRUE(key_value_match(kvs_out, expect_kvs));
  ASSERT_EQ(next_key, "");

  // ************************** Group 7 Test **************************
  //      0     1     2     3       4     5     6     7     8     9
  //      A     C     E     G       I     K     M     O     Q     S
  //                                ^
  //                       key_start/key_end
  kvs_out.clear();
  expect_kvs.clear();
  s = db.PKRScanRange(DataType::kStrings, "PKRSCANRANGE_I", "PKRSCANRANGE_I", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 4; idx >= 4; --idx) {
    expect_kvs.push_back(kvs[idx]);
  }
  ASSERT_TRUE(key_value_match(kvs_out, expect_kvs));
  ASSERT_EQ(next_key, "");

  // ************************** Group 8 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //                              ^     ^
  //                      key_start     key_end
  kvs_out.clear();
  expect_kvs.clear();
  s = db.PKRScanRange(DataType::kStrings, "PKRSCANRANGE_I", "PKRSCANRANGE_K", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_TRUE(key_value_match(kvs_out, expect_kvs));
  ASSERT_EQ(next_key, "");

  // ************************** Group 9 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^           ^                             ^
  //         key_end    next_key                       key_start
  kvs_out.clear();
  expect_kvs.clear();
  s = db.PKRScanRange(DataType::kStrings, "PKRSCANRANGE_Q", "PKRSCANRANGE_C", "*", 5, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 4; --idx) {
    expect_kvs.push_back(kvs[idx]);
  }
  ASSERT_TRUE(key_value_match(kvs_out, expect_kvs));
  ASSERT_EQ(next_key, "PKRSCANRANGE_G");

  // ************************** Group 10 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^     ^                       ^           ^
  //         key_end  next_key              expire     key_start
  kvs_out.clear();
  expect_kvs.clear();
  ASSERT_TRUE(make_expired(&db, "PKRSCANRANGE_M"));
  s = db.PKRScanRange(DataType::kStrings, "PKRSCANRANGE_Q", "PKRSCANRANGE_C", "*", 5, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 3; --idx) {
    if (idx != 6) {
      expect_kvs.push_back(kvs[idx]);
    }
  }
  ASSERT_TRUE(key_value_match(kvs_out, expect_kvs));
  ASSERT_EQ(next_key, "PKRSCANRANGE_E");

  // ************************** Group 11 Test **************************
  //      0     1     2     3     4        5        6     7     8     9
  //      A     C     E     G     I        K        M     O     Q     S
  //            ^                          ^        ^           ^
  //         key_end                   next_key   expire     key_start
  kvs_out.clear();
  expect_kvs.clear();
  s = db.PKRScanRange(DataType::kStrings, "PKRSCANRANGE_Q", "PKRSCANRANGE_C", "*", 2, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 7; --idx) {
    expect_kvs.push_back(kvs[idx]);
  }
  ASSERT_TRUE(key_value_match(kvs_out, expect_kvs));
  ASSERT_EQ(next_key, "PKRSCANRANGE_K");

  //=============================== Sets ===============================
  std::vector<storage::KeyValue> kvset{{"PKRSCANRANGE_A1", "VALUE"}, {"PKRSCANRANGE_C1", "VALUE"},
                                     {"PKRSCANRANGE_E1", "VALUE"}, {"PKRSCANRANGE_G1", "VALUE"},
                                     {"PKRSCANRANGE_I1", "VALUE"}, {"PKRSCANRANGE_K1", "VALUE"},
                                     {"PKRSCANRANGE_M1", "VALUE"}, {"PKRSCANRANGE_O1", "VALUE"},
                                     {"PKRSCANRANGE_Q1", "VALUE"}, {"PKRSCANRANGE_S1", "VALUE"}};
  for (const auto& kv : kvset) {
    s = db.SAdd(kv.key, {"MEMBER"}, &ret);
  }

  // ************************** Group 1 Test **************************
  //         0     1     2     3     4     5     6     7     8     9
  //         A     C     E     G     I     K     M     O     Q     S
  //       ^                                                         ^
  // key_end/next_key                                             key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kSets, "", "", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 9; idx >= 0; --idx) {
    expect_keys.push_back(kvset[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 2 Test **************************
  //       0     1     2     3     4     5     6     7     8     9
  //       A     C     E     G     I     K     M     O     Q     S
  //          ^                                                    ^
  //       key_end                                              key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kSets, "", "PKRSCANRANGE_B1", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 9; idx >= 1; --idx) {
    expect_keys.push_back(kvset[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 3 Test **************************
  //         0     1     2     3     4     5     6     7     8     9
  //         A     C     E     G     I     K     M     O     Q     S
  //       ^                                                    ^
  // key_end/next_key                                         key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kSets, "PKRSCANRANGE_R1", "", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 0; --idx) {
    expect_keys.push_back(kvset[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 4 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //               ^                                   ^
  //            key_end                             key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kSets, "PKRSCANRANGE_P1", "PKRSCANRANGE_D1", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 7; idx >= 2; --idx) {
    expect_keys.push_back(kvset[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 5 Test **************************
  //       0     1     2     3     4     5     6     7     8     9
  //       A     C     E     G     I     K     M     O     Q     S
  //             ^                                         ^
  //          key_end                                   key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kSets, "PKRSCANRANGE_Q1", "PKRSCANRANGE_C1", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 1; --idx) {
    expect_keys.push_back(kvset[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 6 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //                              ^     ^
  //                         key_end   key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kSets, "PKRSCANRANGE_K1", "PKRSCANRANGE_I1", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 5; idx >= 4; --idx) {
    expect_keys.push_back(kvset[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 7 Test **************************
  //      0     1     2     3       4     5     6     7     8     9
  //      A     C     E     G       I     K     M     O     Q     S
  //                                ^
  //                       key_start/key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kSets, "PKRSCANRANGE_I1", "PKRSCANRANGE_I1", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 4; idx >= 4; --idx) {
    expect_keys.push_back(kvset[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 8 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //                              ^     ^
  //                      key_start     key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kSets, "PKRSCANRANGE_I1", "PKRSCANRANGE_K1", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 9 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^           ^                             ^
  //         key_end    next_key                       key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kSets, "PKRSCANRANGE_Q1", "PKRSCANRANGE_C1", "*", 5, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 4; --idx) {
    expect_keys.push_back(kvset[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_G1");

  // ************************** Group 10 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^     ^                       ^           ^
  //         key_end  next_key              expire     key_start
  keys_out.clear();
  expect_keys.clear();
  ASSERT_TRUE(make_expired(&db, "PKRSCANRANGE_M1"));
  s = db.PKRScanRange(DataType::kSets, "PKRSCANRANGE_Q1", "PKRSCANRANGE_C1", "*", 5, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 3; --idx) {
    if (idx != 6) {
      expect_keys.push_back(kvset[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_E1");

  // ************************** Group 11 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^     ^           ^           ^           ^
  //         key_end  next_key  empty       expire     key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.SRem("PKRSCANRANGE_I1", {"MEMBER"}, &ret);
  ASSERT_TRUE(s.ok());
  s = db.PKRScanRange(DataType::kSets, "PKRSCANRANGE_Q1", "PKRSCANRANGE_C1", "*", 4, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 3; --idx) {
    if (idx != 6 && idx != 4) {
      expect_keys.push_back(kvset[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_E1");

  // ************************** Group 12 Test **************************
  //      0     1     2     3     4        5        6     7     8     9
  //      A     C     E     G     I        K        M     O     Q     S
  //            ^                 ^        ^        ^           ^
  //         key_end            empty  next_key   expire     key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kSets, "PKRSCANRANGE_Q1", "PKRSCANRANGE_C1", "*", 2, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 7; --idx) {
    expect_keys.push_back(kvset[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_K1");

  // ************************** Group 13 Test **************************
  //      0     1     2       3       4     5     6     7     8     9
  //      A     C     E       G       I     K     M     O     Q     S
  //            ^             ^       ^           ^           ^
  //         key_end      next_key  empty       expire     key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kSets, "PKRSCANRANGE_Q1", "PKRSCANRANGE_C1", "*", 3, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 5; --idx) {
    if (idx != 6) {
      expect_keys.push_back(kvset[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_G1");

  //=============================== Hashes ===============================
  std::vector<storage::KeyValue> kvhash{{"PKRSCANRANGE_A2", "VALUE"}, {"PKRSCANRANGE_C2", "VALUE"},
                                     {"PKRSCANRANGE_E2", "VALUE"}, {"PKRSCANRANGE_G2", "VALUE"},
                                     {"PKRSCANRANGE_I2", "VALUE"}, {"PKRSCANRANGE_K2", "VALUE"},
                                     {"PKRSCANRANGE_M2", "VALUE"}, {"PKRSCANRANGE_O2", "VALUE"},
                                     {"PKRSCANRANGE_Q2", "VALUE"}, {"PKRSCANRANGE_S2", "VALUE"}};
  for (const auto& kv : kvhash) {
    s = db.HMSet(kv.key, {{"FIELD", "VALUE"}});
  }

  // ************************** Group 1 Test **************************
  //         0     1     2     3     4     5     6     7     8     9
  //         A     C     E     G     I     K     M     O     Q     S
  //       ^                                                         ^
  // key_end/next_key                                             key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kHashes, "", "", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 9; idx >= 0; --idx) {
    expect_keys.push_back(kvhash[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 2 Test **************************
  //       0     1     2     3     4     5     6     7     8     9
  //       A     C     E     G     I     K     M     O     Q     S
  //          ^                                                    ^
  //       key_end                                              key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kHashes, "", "PKRSCANRANGE_B2", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 9; idx >= 1; --idx) {
    expect_keys.push_back(kvhash[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 3 Test **************************
  //         0     1     2     3     4     5     6     7     8     9
  //         A     C     E     G     I     K     M     O     Q     S
  //       ^                                                    ^
  // key_end/next_key                                         key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kHashes, "PKRSCANRANGE_R2", "", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 0; --idx) {
    expect_keys.push_back(kvhash[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 4 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //               ^                                   ^
  //            key_end                             key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kHashes, "PKRSCANRANGE_P2", "PKRSCANRANGE_D2", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 7; idx >= 2; --idx) {
    expect_keys.push_back(kvhash[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 5 Test **************************
  //       0     1     2     3     4     5     6     7     8     9
  //       A     C     E     G     I     K     M     O     Q     S
  //             ^                                         ^
  //          key_end                                   key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kHashes, "PKRSCANRANGE_Q2", "PKRSCANRANGE_C2", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 1; --idx) {
    expect_keys.push_back(kvhash[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 6 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //                              ^     ^
  //                         key_end   key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kHashes, "PKRSCANRANGE_K2", "PKRSCANRANGE_I2", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 5; idx >= 4; --idx) {
    expect_keys.push_back(kvhash[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 7 Test **************************
  //      0     1     2     3       4     5     6     7     8     9
  //      A     C     E     G       I     K     M     O     Q     S
  //                                ^
  //                       key_start/key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kHashes, "PKRSCANRANGE_I2", "PKRSCANRANGE_I2", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 4; idx >= 4; --idx) {
    expect_keys.push_back(kvhash[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 8 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //                              ^     ^
  //                      key_start     key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kHashes, "PKRSCANRANGE_I2", "PKRSCANRANGE_K2", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 9 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^           ^                             ^
  //         key_end    next_key                       key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kHashes, "PKRSCANRANGE_Q2", "PKRSCANRANGE_C2", "*", 5, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 4; --idx) {
    expect_keys.push_back(kvhash[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_G2");

  // ************************** Group 10 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^     ^                       ^           ^
  //         key_end  next_key              expire     key_start
  keys_out.clear();
  expect_keys.clear();
  ASSERT_TRUE(make_expired(&db, "PKRSCANRANGE_M2"));
  s = db.PKRScanRange(DataType::kHashes, "PKRSCANRANGE_Q2", "PKRSCANRANGE_C2", "*", 5, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 3; --idx) {
    if (idx != 6) {
      expect_keys.push_back(kvhash[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_E2");

  // ************************** Group 11 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^     ^           ^           ^           ^
  //         key_end  next_key  empty       expire     key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.HDel("PKRSCANRANGE_I2", {"FIELD"}, &ret);
  ASSERT_TRUE(s.ok());
  s = db.PKRScanRange(DataType::kHashes, "PKRSCANRANGE_Q2", "PKRSCANRANGE_C2", "*", 4, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 3; --idx) {
    if (idx != 6 && idx != 4) {
      expect_keys.push_back(kvhash[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_E2");

  // ************************** Group 12 Test **************************
  //      0     1     2     3     4        5        6     7     8     9
  //      A     C     E     G     I        K        M     O     Q     S
  //            ^                 ^        ^        ^           ^
  //         key_end            empty  next_key   expire     key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kHashes, "PKRSCANRANGE_Q2", "PKRSCANRANGE_C2", "*", 2, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 7; --idx) {
    expect_keys.push_back(kvhash[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_K2");

  // ************************** Group 13 Test **************************
  //      0     1     2       3       4     5     6     7     8     9
  //      A     C     E       G       I     K     M     O     Q     S
  //            ^             ^       ^           ^           ^
  //         key_end      next_key  empty       expire     key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kHashes, "PKRSCANRANGE_Q2", "PKRSCANRANGE_C2", "*", 3, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 5; --idx) {
    if (idx != 6) {
      expect_keys.push_back(kvhash[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_G2");

  //=============================== ZSets ===============================
  std::vector<storage::KeyValue> kvzset{{"PKRSCANRANGE_A3", "VALUE"}, {"PKRSCANRANGE_C3", "VALUE"},
                                     {"PKRSCANRANGE_E3", "VALUE"}, {"PKRSCANRANGE_G3", "VALUE"},
                                     {"PKRSCANRANGE_I3", "VALUE"}, {"PKRSCANRANGE_K3", "VALUE"},
                                     {"PKRSCANRANGE_M3", "VALUE"}, {"PKRSCANRANGE_O3", "VALUE"},
                                     {"PKRSCANRANGE_Q3", "VALUE"}, {"PKRSCANRANGE_S3", "VALUE"}};
  for (const auto& kv : kvzset) {
    s = db.ZAdd(kv.key, {{1, "MEMBER"}}, &ret);
  }

  // ************************** Group 1 Test **************************
  //         0     1     2     3     4     5     6     7     8     9
  //         A     C     E     G     I     K     M     O     Q     S
  //       ^                                                         ^
  // key_end/next_key                                             key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kZSets, "", "", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 9; idx >= 0; --idx) {
    expect_keys.push_back(kvzset[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 2 Test **************************
  //       0     1     2     3     4     5     6     7     8     9
  //       A     C     E     G     I     K     M     O     Q     S
  //          ^                                                    ^
  //       key_end                                              key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kZSets, "", "PKRSCANRANGE_B3", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 9; idx >= 1; --idx) {
    expect_keys.push_back(kvzset[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 3 Test **************************
  //         0     1     2     3     4     5     6     7     8     9
  //         A     C     E     G     I     K     M     O     Q     S
  //       ^                                                    ^
  // key_end/next_key                                         key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kZSets, "PKRSCANRANGE_R3", "", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 0; --idx) {
    expect_keys.push_back(kvzset[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 4 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //               ^                                   ^
  //            key_end                             key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kZSets, "PKRSCANRANGE_P3", "PKRSCANRANGE_D3", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 7; idx >= 2; --idx) {
    expect_keys.push_back(kvzset[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 5 Test **************************
  //       0     1     2     3     4     5     6     7     8     9
  //       A     C     E     G     I     K     M     O     Q     S
  //             ^                                         ^
  //          key_end                                   key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kZSets, "PKRSCANRANGE_Q3", "PKRSCANRANGE_C3", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 1; --idx) {
    expect_keys.push_back(kvzset[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 6 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //                              ^     ^
  //                         key_end   key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kZSets, "PKRSCANRANGE_K3", "PKRSCANRANGE_I3", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 5; idx >= 4; --idx) {
    expect_keys.push_back(kvzset[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 7 Test **************************
  //      0     1     2     3       4     5     6     7     8     9
  //      A     C     E     G       I     K     M     O     Q     S
  //                                ^
  //                       key_start/key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kZSets, "PKRSCANRANGE_I3", "PKRSCANRANGE_I3", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 4; idx >= 4; --idx) {
    expect_keys.push_back(kvzset[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 8 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //                              ^     ^
  //                      key_start     key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kZSets, "PKRSCANRANGE_I3", "PKRSCANRANGE_K3", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 9 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^           ^                             ^
  //         key_end    next_key                       key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kZSets, "PKRSCANRANGE_Q3", "PKRSCANRANGE_C3", "*", 5, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 4; --idx) {
    expect_keys.push_back(kvzset[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_G3");

  // ************************** Group 10 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^     ^                       ^           ^
  //         key_end  next_key              expire     key_start
  keys_out.clear();
  expect_keys.clear();
  ASSERT_TRUE(make_expired(&db, "PKRSCANRANGE_M3"));
  s = db.PKRScanRange(DataType::kZSets, "PKRSCANRANGE_Q3", "PKRSCANRANGE_C3", "*", 5, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 3; --idx) {
    if (idx != 6) {
      expect_keys.push_back(kvzset[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_E3");

  // ************************** Group 11 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^     ^           ^           ^           ^
  //         key_end  next_key  empty       expire     key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.ZRem("PKRSCANRANGE_I3", {"MEMBER"}, &ret);
  ASSERT_TRUE(s.ok());
  s = db.PKRScanRange(DataType::kZSets, "PKRSCANRANGE_Q3", "PKRSCANRANGE_C3", "*", 4, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 3; --idx) {
    if (idx != 6 && idx != 4) {
      expect_keys.push_back(kvzset[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_E3");

  // ************************** Group 12 Test **************************
  //      0     1     2     3     4        5        6     7     8     9
  //      A     C     E     G     I        K        M     O     Q     S
  //            ^                 ^        ^        ^           ^
  //         key_end            empty  next_key   expire     key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kZSets, "PKRSCANRANGE_Q3", "PKRSCANRANGE_C3", "*", 2, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 7; --idx) {
    expect_keys.push_back(kvzset[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_K3");

  // ************************** Group 13 Test **************************
  //      0     1     2       3       4     5     6     7     8     9
  //      A     C     E       G       I     K     M     O     Q     S
  //            ^             ^       ^           ^           ^
  //         key_end      next_key  empty       expire     key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kZSets, "PKRSCANRANGE_Q3", "PKRSCANRANGE_C3", "*", 3, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 5; --idx) {
    if (idx != 6) {
      expect_keys.push_back(kvzset[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_G3");

  //=============================== Lists ===============================
  std::vector<storage::KeyValue> kvlist{{"PKRSCANRANGE_A4", "VALUE"}, {"PKRSCANRANGE_C4", "VALUE"},
                                     {"PKRSCANRANGE_E4", "VALUE"}, {"PKRSCANRANGE_G4", "VALUE"},
                                     {"PKRSCANRANGE_I4", "VALUE"}, {"PKRSCANRANGE_K4", "VALUE"},
                                     {"PKRSCANRANGE_M4", "VALUE"}, {"PKRSCANRANGE_O4", "VALUE"},
                                     {"PKRSCANRANGE_Q4", "VALUE"}, {"PKRSCANRANGE_S4", "VALUE"}};
  for (const auto& kv : kvlist) {
    s = db.LPush(kv.key, {"NODE"}, &ret_u64);
  }

  // ************************** Group 1 Test **************************
  //         0     1     2     3     4     5     6     7     8     9
  //         A     C     E     G     I     K     M     O     Q     S
  //       ^                                                         ^
  // key_end/next_key                                             key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kLists, "", "", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 9; idx >= 0; --idx) {
    expect_keys.push_back(kvlist[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 2 Test **************************
  //       0     1     2     3     4     5     6     7     8     9
  //       A     C     E     G     I     K     M     O     Q     S
  //          ^                                                    ^
  //       key_end                                              key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kLists, "", "PKRSCANRANGE_B4", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 9; idx >= 1; --idx) {
    expect_keys.push_back(kvlist[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 3 Test **************************
  //         0     1     2     3     4     5     6     7     8     9
  //         A     C     E     G     I     K     M     O     Q     S
  //       ^                                                    ^
  // key_end/next_key                                         key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kLists, "PKRSCANRANGE_R4", "", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 0; --idx) {
    expect_keys.push_back(kvlist[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 4 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //               ^                                   ^
  //            key_end                             key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kLists, "PKRSCANRANGE_P4", "PKRSCANRANGE_D4", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 7; idx >= 2; --idx) {
    expect_keys.push_back(kvlist[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 5 Test **************************
  //       0     1     2     3     4     5     6     7     8     9
  //       A     C     E     G     I     K     M     O     Q     S
  //             ^                                         ^
  //          key_end                                   key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kLists, "PKRSCANRANGE_Q4", "PKRSCANRANGE_C4", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 1; --idx) {
    expect_keys.push_back(kvlist[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 6 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //                              ^     ^
  //                         key_end   key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kLists, "PKRSCANRANGE_K4", "PKRSCANRANGE_I4", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 5; idx >= 4; --idx) {
    expect_keys.push_back(kvlist[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 7 Test **************************
  //      0     1     2     3       4     5     6     7     8     9
  //      A     C     E     G       I     K     M     O     Q     S
  //                                ^
  //                       key_start/key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kLists, "PKRSCANRANGE_I4", "PKRSCANRANGE_I4", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 4; idx >= 4; --idx) {
    expect_keys.push_back(kvlist[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 8 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //                              ^     ^
  //                      key_start     key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kLists, "PKRSCANRANGE_I4", "PKRSCANRANGE_K4", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");

  // ************************** Group 9 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^           ^                             ^
  //         key_end    next_key                       key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kLists, "PKRSCANRANGE_Q4", "PKRSCANRANGE_C4", "*", 5, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 4; --idx) {
    expect_keys.push_back(kvlist[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_G4");

  // ************************** Group 10 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^     ^                       ^           ^
  //         key_end  next_key              expire     key_start
  keys_out.clear();
  expect_keys.clear();
  ASSERT_TRUE(make_expired(&db, "PKRSCANRANGE_M4"));
  s = db.PKRScanRange(DataType::kLists, "PKRSCANRANGE_Q4", "PKRSCANRANGE_C4", "*", 5, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 3; --idx) {
    if (idx != 6) {
      expect_keys.push_back(kvlist[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_E4");

  // ************************** Group 11 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^     ^           ^           ^           ^
  //         key_end  next_key  empty       expire     key_start
  keys_out.clear();
  expect_keys.clear();
  std::string element;
  std::vector<std::string> elements;
  s = db.LPop("PKRSCANRANGE_I4",1, &elements);
  ASSERT_TRUE(s.ok());
  s = db.PKRScanRange(DataType::kLists, "PKRSCANRANGE_Q4", "PKRSCANRANGE_C4", "*", 4, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 3; --idx) {
    if (idx != 6 && idx != 4) {
      expect_keys.push_back(kvlist[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_E4");

  // ************************** Group 12 Test **************************
  //      0     1     2     3     4        5        6     7     8     9
  //      A     C     E     G     I        K        M     O     Q     S
  //            ^                 ^        ^        ^           ^
  //         key_end            empty  next_key   expire     key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kLists, "PKRSCANRANGE_Q4", "PKRSCANRANGE_C4", "*", 2, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 7; --idx) {
    expect_keys.push_back(kvlist[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_K4");

  // ************************** Group 13 Test **************************
  //      0     1     2       3       4     5     6     7     8     9
  //      A     C     E       G       I     K     M     O     Q     S
  //            ^             ^       ^           ^           ^
  //         key_end      next_key  empty       expire     key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kLists, "PKRSCANRANGE_Q4", "PKRSCANRANGE_C4", "*", 3, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 5; --idx) {
    if (idx != 6) {
      expect_keys.push_back(kvlist[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_G4");

  type_status.clear();
  db.Del(keys_del);
  sleep(2);
  db.Compact(DataType::kAll, true);
}

TEST_F(KeysTest, PKPatternMatchDel) {
  int32_t ret;
  uint64_t ret64;
  int64_t delete_count = 0;
  std::vector<std::string> keys;
  std::vector<std::string> remove_keys;
  const int64_t max_count = storage::BATCH_DELETE_LIMIT;
  std::map<DataType, Status> type_status;

  //=============================== Strings ===============================

  // ***************** Group 1 Test *****************
  db.Set("GP1_PKPATTERNMATCHDEL_STRING_KEY1", "VALUE");
  db.Set("GP1_PKPATTERNMATCHDEL_STRING_KEY2", "VALUE");
  db.Set("GP1_PKPATTERNMATCHDEL_STRING_KEY3", "VALUE");
  db.Set("GP1_PKPATTERNMATCHDEL_STRING_KEY4", "VALUE");
  db.Set("GP1_PKPATTERNMATCHDEL_STRING_KEY5", "VALUE");
  db.Set("GP1_PKPATTERNMATCHDEL_STRING_KEY6", "VALUE");
  s = db.PKPatternMatchDelWithRemoveKeys("*", &delete_count, &remove_keys, max_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 6);
  ASSERT_EQ(remove_keys.size(), 6);
  keys.clear();
  remove_keys.clear();
  db.Keys(DataType::kStrings, "*", &keys);
  ASSERT_EQ(keys.size(), 0);

  // ***************** Group 2 Test *****************
  db.Set("GP2_PKPATTERNMATCHDEL_STRING_KEY1", "VALUE");
  db.Set("GP2_PKPATTERNMATCHDEL_STRING_KEY2", "VALUE");
  db.Set("GP2_PKPATTERNMATCHDEL_STRING_KEY3", "VALUE");
  db.Set("GP2_PKPATTERNMATCHDEL_STRING_KEY4", "VALUE");
  db.Set("GP2_PKPATTERNMATCHDEL_STRING_KEY5", "VALUE");
  db.Set("GP2_PKPATTERNMATCHDEL_STRING_KEY6", "VALUE");
  ASSERT_TRUE(make_expired(&db, "GP2_PKPATTERNMATCHDEL_STRING_KEY1"));
  ASSERT_TRUE(make_expired(&db, "GP2_PKPATTERNMATCHDEL_STRING_KEY3"));
  ASSERT_TRUE(make_expired(&db, "GP2_PKPATTERNMATCHDEL_STRING_KEY5"));
  s = db.PKPatternMatchDelWithRemoveKeys("*", &delete_count, &remove_keys, max_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 3);
  ASSERT_EQ(remove_keys.size(), 3);
  keys.clear();
  remove_keys.clear();
  db.Keys(DataType::kStrings, "*", &keys);
  db.Keys(DataType::kStrings, "*", &keys);
  ASSERT_EQ(keys.size(), 0);

  // ***************** Group 3 Test *****************
  db.Set("GP3_PKPATTERNMATCHDEL_STRING_KEY1_0xxx0", "VALUE");
  db.Set("GP3_PKPATTERNMATCHDEL_STRING_KEY2_0ooo0", "VALUE");
  db.Set("GP3_PKPATTERNMATCHDEL_STRING_KEY3_0xxx0", "VALUE");
  db.Set("GP3_PKPATTERNMATCHDEL_STRING_KEY4_0ooo0", "VALUE");
  db.Set("GP3_PKPATTERNMATCHDEL_STRING_KEY5_0xxx0", "VALUE");
  db.Set("GP3_PKPATTERNMATCHDEL_STRING_KEY6_0ooo0", "VALUE");
  s = db.PKPatternMatchDelWithRemoveKeys("*0xxx0", &delete_count, &remove_keys, max_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 3);
  ASSERT_EQ(remove_keys.size(), 3);
  keys.clear();
  remove_keys.clear();
  db.Keys(DataType::kStrings, "*", &keys);
  ASSERT_EQ(keys.size(), 3);
  ASSERT_EQ(keys[0], "GP3_PKPATTERNMATCHDEL_STRING_KEY2_0ooo0");
  ASSERT_EQ(keys[1], "GP3_PKPATTERNMATCHDEL_STRING_KEY4_0ooo0");
  ASSERT_EQ(keys[2], "GP3_PKPATTERNMATCHDEL_STRING_KEY6_0ooo0");
  type_status.clear();
  db.Del(keys);

  // ***************** Group 4 Test *****************
  db.Set("GP4_PKPATTERNMATCHDEL_STRING_KEY1", "VALUE");
  db.Set("GP4_PKPATTERNMATCHDEL_STRING_KEY2_0ooo0", "VALUE");
  db.Set("GP4_PKPATTERNMATCHDEL_STRING_KEY3", "VALUE");
  db.Set("GP4_PKPATTERNMATCHDEL_STRING_KEY4_0ooo0", "VALUE");
  db.Set("GP4_PKPATTERNMATCHDEL_STRING_KEY5", "VALUE");
  db.Set("GP4_PKPATTERNMATCHDEL_STRING_KEY6_0ooo0", "VALUE");
  ASSERT_TRUE(make_expired(&db, "GP4_PKPATTERNMATCHDEL_STRING_KEY1"));
  ASSERT_TRUE(make_expired(&db, "GP4_PKPATTERNMATCHDEL_STRING_KEY3"));
  ASSERT_TRUE(make_expired(&db, "GP4_PKPATTERNMATCHDEL_STRING_KEY5"));
  s = db.PKPatternMatchDelWithRemoveKeys("*0ooo0", &delete_count, &remove_keys, max_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 3);
  ASSERT_EQ(remove_keys.size(), 3);
  keys.clear();
  remove_keys.clear();
  db.Keys(DataType::kStrings, "*", &keys);
  ASSERT_EQ(keys.size(), 0);

  // ***************** Group 5 Test *****************
  size_t gp5_total_kv = 23333;
  for (size_t idx = 0; idx < gp5_total_kv; ++idx) {
    db.Set("GP5_PKPATTERNMATCHDEL_STRING_KEY" + std::to_string(idx), "VALUE");
  }
  s = db.PKPatternMatchDelWithRemoveKeys("*", &delete_count, &remove_keys, max_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, max_count);
  ASSERT_EQ(remove_keys.size(), max_count);
  keys.clear();
  remove_keys.clear();
  db.Keys(DataType::kStrings, "*", &keys);
  ASSERT_EQ(keys.size(), gp5_total_kv - max_count);
  db.Del(keys);

  //=============================== Set ===============================

  // ***************** Group 1 Test *****************
  db.SAdd("GP1_PKPATTERNMATCHDEL_SET_KEY1", {"M1"}, &ret);
  db.SAdd("GP1_PKPATTERNMATCHDEL_SET_KEY2", {"M1"}, &ret);
  db.SAdd("GP1_PKPATTERNMATCHDEL_SET_KEY3", {"M1"}, &ret);
  db.SAdd("GP1_PKPATTERNMATCHDEL_SET_KEY4", {"M1"}, &ret);
  db.SAdd("GP1_PKPATTERNMATCHDEL_SET_KEY5", {"M1"}, &ret);
  db.SAdd("GP1_PKPATTERNMATCHDEL_SET_KEY6", {"M1"}, &ret);
  s = db.PKPatternMatchDelWithRemoveKeys("*", &delete_count, &remove_keys, max_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 6);
  ASSERT_EQ(remove_keys.size(), 6);
  keys.clear();
  remove_keys.clear();
  db.Keys(DataType::kSets, "*", &keys);
  ASSERT_EQ(keys.size(), 0);

  // ***************** Group 2 Test *****************
  db.SAdd("GP2_PKPATTERNMATCHDEL_SET_KEY1", {"M1"}, &ret);
  db.SAdd("GP2_PKPATTERNMATCHDEL_SET_KEY2", {"M1"}, &ret);
  db.SAdd("GP2_PKPATTERNMATCHDEL_SET_KEY3", {"M1"}, &ret);
  db.SAdd("GP2_PKPATTERNMATCHDEL_SET_KEY4", {"M1"}, &ret);
  db.SAdd("GP2_PKPATTERNMATCHDEL_SET_KEY5", {"M1"}, &ret);
  db.SAdd("GP2_PKPATTERNMATCHDEL_SET_KEY6", {"M1"}, &ret);
  ASSERT_TRUE(make_expired(&db, "GP2_PKPATTERNMATCHDEL_SET_KEY1"));
  ASSERT_TRUE(make_expired(&db, "GP2_PKPATTERNMATCHDEL_SET_KEY3"));
  ASSERT_TRUE(make_expired(&db, "GP2_PKPATTERNMATCHDEL_SET_KEY5"));
  s = db.PKPatternMatchDelWithRemoveKeys("*", &delete_count, &remove_keys, max_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 3);
  ASSERT_EQ(remove_keys.size(), 3);
  keys.clear();
  remove_keys.clear();
  db.Keys(DataType::kSets, "*", &keys);
  ASSERT_EQ(keys.size(), 0);

  // ***************** Group 3 Test *****************
  db.SAdd("GP3_PKPATTERNMATCHDEL_SET_KEY1_0xxx0", {"M1"}, &ret);
  db.SAdd("GP3_PKPATTERNMATCHDEL_SET_KEY2_0ooo0", {"M1"}, &ret);
  db.SAdd("GP3_PKPATTERNMATCHDEL_SET_KEY3_0xxx0", {"M1"}, &ret);
  db.SAdd("GP3_PKPATTERNMATCHDEL_SET_KEY4_0ooo0", {"M1"}, &ret);
  db.SAdd("GP3_PKPATTERNMATCHDEL_SET_KEY5_0xxx0", {"M1"}, &ret);
  db.SAdd("GP3_PKPATTERNMATCHDEL_SET_KEY6_0ooo0", {"M1"}, &ret);
  s = db.PKPatternMatchDelWithRemoveKeys("*0ooo0", &delete_count, &remove_keys, max_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 3);
  ASSERT_EQ(remove_keys.size(), 3);
  keys.clear();
  remove_keys.clear();
  db.Keys(DataType::kSets, "*", &keys);
  ASSERT_EQ(keys.size(), 3);
  ASSERT_EQ("GP3_PKPATTERNMATCHDEL_SET_KEY1_0xxx0", keys[0]);
  ASSERT_EQ("GP3_PKPATTERNMATCHDEL_SET_KEY3_0xxx0", keys[1]);
  ASSERT_EQ("GP3_PKPATTERNMATCHDEL_SET_KEY5_0xxx0", keys[2]);
  type_status.clear();
  db.Del(keys);

  // ***************** Group 4 Test *****************
  db.SAdd("GP4_PKPATTERNMATCHDEL_SET_KEY1", {"M1"}, &ret);
  db.SAdd("GP4_PKPATTERNMATCHDEL_SET_KEY2", {"M1"}, &ret);
  db.SAdd("GP4_PKPATTERNMATCHDEL_SET_KEY3", {"M1"}, &ret);
  db.SAdd("GP4_PKPATTERNMATCHDEL_SET_KEY4", {"M1"}, &ret);
  db.SAdd("GP4_PKPATTERNMATCHDEL_SET_KEY5", {"M1"}, &ret);
  db.SAdd("GP4_PKPATTERNMATCHDEL_SET_KEY6", {"M1"}, &ret);
  db.SRem("GP4_PKPATTERNMATCHDEL_SET_KEY1", {"M1"}, &ret);
  db.SRem("GP4_PKPATTERNMATCHDEL_SET_KEY3", {"M1"}, &ret);
  db.SRem("GP4_PKPATTERNMATCHDEL_SET_KEY5", {"M1"}, &ret);
  s = db.PKPatternMatchDelWithRemoveKeys("*", &delete_count, &remove_keys, max_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 3);
  ASSERT_EQ(remove_keys.size(), 3);
  keys.clear();
  remove_keys.clear();
  db.Keys(DataType::kSets, "*", &keys);
  ASSERT_EQ(keys.size(), 0);

  // ***************** Group 5 Test *****************
  db.SAdd("GP5_PKPATTERNMATCHDEL_SET_KEY1_0ooo0", {"M1"}, &ret);
  db.SAdd("GP5_PKPATTERNMATCHDEL_SET_KEY2_0xxx0", {"M1"}, &ret);
  db.SAdd("GP5_PKPATTERNMATCHDEL_SET_KEY3_0ooo0", {"M1"}, &ret);
  db.SAdd("GP5_PKPATTERNMATCHDEL_SET_KEY4_0xxx0", {"M1"}, &ret);
  db.SAdd("GP5_PKPATTERNMATCHDEL_SET_KEY5_0ooo0", {"M1"}, &ret);
  db.SAdd("GP5_PKPATTERNMATCHDEL_SET_KEY6_0xxx0", {"M1"}, &ret);
  db.SAdd("GP5_PKPATTERNMATCHDEL_SET_KEY7_0ooo0", {"M1"}, &ret);
  db.SAdd("GP5_PKPATTERNMATCHDEL_SET_KEY8_0xxx0", {"M1"}, &ret);
  ASSERT_TRUE(make_expired(&db, "GP5_PKPATTERNMATCHDEL_SET_KEY1_0ooo0"));
  ASSERT_TRUE(make_expired(&db, "GP5_PKPATTERNMATCHDEL_SET_KEY2_0xxx0"));
  db.SRem("GP5_PKPATTERNMATCHDEL_SET_KEY3_0ooo0", {"M1"}, &ret);
  db.SRem("GP5_PKPATTERNMATCHDEL_SET_KEY4_0xxx0", {"M1"}, &ret);
  s = db.PKPatternMatchDelWithRemoveKeys("*0ooo0", &delete_count, &remove_keys, max_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 2);
  ASSERT_EQ(remove_keys.size(), 2);
  keys.clear();
  remove_keys.clear();
  db.Keys(DataType::kSets, "*", &keys);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP5_PKPATTERNMATCHDEL_SET_KEY6_0xxx0");
  ASSERT_EQ(keys[1], "GP5_PKPATTERNMATCHDEL_SET_KEY8_0xxx0");
  type_status.clear();
  db.Del(keys);

  // ***************** Group 6 Test *****************
  size_t gp6_total_set = 23333;
  for (size_t idx = 0; idx < gp6_total_set; ++idx) {
    db.SAdd("GP6_PKPATTERNMATCHDEL_SET_KEY" + std::to_string(idx), {"M1"}, &ret);
  }
  s = db.PKPatternMatchDelWithRemoveKeys("*", &delete_count, &remove_keys, max_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, max_count);
  ASSERT_EQ(remove_keys.size(), max_count);
  keys.clear();
  remove_keys.clear();
  db.Keys(DataType::kSets, "*", &keys);
  ASSERT_EQ(keys.size(), gp6_total_set - max_count);
  db.Del(keys);

  //=============================== Hashes ===============================

  // ***************** Group 1 Test *****************
  db.HSet("GP1_PKPATTERNMATCHDEL_HASH_KEY1", "FIELD", "VALUE", &ret);
  db.HSet("GP1_PKPATTERNMATCHDEL_HASH_KEY2", "FIELD", "VALUE", &ret);
  db.HSet("GP1_PKPATTERNMATCHDEL_HASH_KEY3", "FIELD", "VALUE", &ret);
  db.HSet("GP1_PKPATTERNMATCHDEL_HASH_KEY4", "FIELD", "VALUE", &ret);
  db.HSet("GP1_PKPATTERNMATCHDEL_HASH_KEY5", "FIELD", "VALUE", &ret);
  db.HSet("GP1_PKPATTERNMATCHDEL_HASH_KEY6", "FIELD", "VALUE", &ret);
  s = db.PKPatternMatchDelWithRemoveKeys("*", &delete_count, &remove_keys, max_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 6);
  ASSERT_EQ(remove_keys.size(), 6);
  keys.clear();
  remove_keys.clear();
  db.Keys(DataType::kHashes, "*", &keys);
  ASSERT_EQ(keys.size(), 0);

  // ***************** Group 2 Test *****************
  db.HSet("GP2_PKPATTERNMATCHDEL_HASH_KEY1", "FIELD", "VALUE", &ret);
  db.HSet("GP2_PKPATTERNMATCHDEL_HASH_KEY2", "FIELD", "VALUE", &ret);
  db.HSet("GP2_PKPATTERNMATCHDEL_HASH_KEY3", "FIELD", "VALUE", &ret);
  db.HSet("GP2_PKPATTERNMATCHDEL_HASH_KEY4", "FIELD", "VALUE", &ret);
  db.HSet("GP2_PKPATTERNMATCHDEL_HASH_KEY5", "FIELD", "VALUE", &ret);
  db.HSet("GP2_PKPATTERNMATCHDEL_HASH_KEY6", "FIELD", "VALUE", &ret);
  ASSERT_TRUE(make_expired(&db, "GP2_PKPATTERNMATCHDEL_HASH_KEY1"));
  ASSERT_TRUE(make_expired(&db, "GP2_PKPATTERNMATCHDEL_HASH_KEY3"));
  ASSERT_TRUE(make_expired(&db, "GP2_PKPATTERNMATCHDEL_HASH_KEY5"));
  s = db.PKPatternMatchDelWithRemoveKeys("*", &delete_count, &remove_keys, max_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 3);
  ASSERT_EQ(remove_keys.size(), 3);
  keys.clear();
  remove_keys.clear();
  db.Keys(DataType::kHashes, "*", &keys);
  ASSERT_EQ(keys.size(), 0);

  // ***************** Group 3 Test *****************
  db.HSet("GP3_PKPATTERNMATCHDEL_HASH_KEY1_0xxx0", "FIELD", "VALUE", &ret);
  db.HSet("GP3_PKPATTERNMATCHDEL_HASH_KEY2_0ooo0", "FIELD", "VALUE", &ret);
  db.HSet("GP3_PKPATTERNMATCHDEL_HASH_KEY3_0xxx0", "FIELD", "VALUE", &ret);
  db.HSet("GP3_PKPATTERNMATCHDEL_HASH_KEY4_0ooo0", "FIELD", "VALUE", &ret);
  db.HSet("GP3_PKPATTERNMATCHDEL_HASH_KEY5_0xxx0", "FIELD", "VALUE", &ret);
  db.HSet("GP3_PKPATTERNMATCHDEL_HASH_KEY6_0ooo0", "FIELD", "VALUE", &ret);
  s = db.PKPatternMatchDelWithRemoveKeys("*0ooo0", &delete_count, &remove_keys, max_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 3);
  ASSERT_EQ(remove_keys.size(), 3);
  keys.clear();
  remove_keys.clear();
  db.Keys(DataType::kHashes, "*", &keys);
  ASSERT_EQ(keys.size(), 3);
  ASSERT_EQ("GP3_PKPATTERNMATCHDEL_HASH_KEY1_0xxx0", keys[0]);
  ASSERT_EQ("GP3_PKPATTERNMATCHDEL_HASH_KEY3_0xxx0", keys[1]);
  ASSERT_EQ("GP3_PKPATTERNMATCHDEL_HASH_KEY5_0xxx0", keys[2]);
  type_status.clear();
  db.Del(keys);

  // ***************** Group 4 Test *****************
  db.HSet("GP4_PKPATTERNMATCHDEL_HASH_KEY1", "FIELD", "VALUE", &ret);
  db.HSet("GP4_PKPATTERNMATCHDEL_HASH_KEY2", "FIELD", "VALUE", &ret);
  db.HSet("GP4_PKPATTERNMATCHDEL_HASH_KEY3", "FIELD", "VALUE", &ret);
  db.HSet("GP4_PKPATTERNMATCHDEL_HASH_KEY4", "FIELD", "VALUE", &ret);
  db.HSet("GP4_PKPATTERNMATCHDEL_HASH_KEY5", "FIELD", "VALUE", &ret);
  db.HSet("GP4_PKPATTERNMATCHDEL_HASH_KEY6", "FIELD", "VALUE", &ret);
  db.HDel("GP4_PKPATTERNMATCHDEL_HASH_KEY1", {"FIELD"}, &ret);
  db.HDel("GP4_PKPATTERNMATCHDEL_HASH_KEY3", {"FIELD"}, &ret);
  db.HDel("GP4_PKPATTERNMATCHDEL_HASH_KEY5", {"FIELD"}, &ret);
  s = db.PKPatternMatchDelWithRemoveKeys("*", &delete_count, &remove_keys, max_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 3);
  ASSERT_EQ(remove_keys.size(), 3);
  keys.clear();
  remove_keys.clear();
  db.Keys(DataType::kHashes, "*", &keys);
  ASSERT_EQ(keys.size(), 0);

  // ***************** Group 5 Test *****************
  db.HSet("GP5_PKPATTERNMATCHDEL_HASH_KEY1_0ooo0", "FIELD", "VALUE", &ret);
  db.HSet("GP5_PKPATTERNMATCHDEL_HASH_KEY2_0xxx0", "FIELD", "VALUE", &ret);
  db.HSet("GP5_PKPATTERNMATCHDEL_HASH_KEY3_0ooo0", "FIELD", "VALUE", &ret);
  db.HSet("GP5_PKPATTERNMATCHDEL_HASH_KEY4_0xxx0", "FIELD", "VALUE", &ret);
  db.HSet("GP5_PKPATTERNMATCHDEL_HASH_KEY5_0ooo0", "FIELD", "VALUE", &ret);
  db.HSet("GP5_PKPATTERNMATCHDEL_HASH_KEY6_0xxx0", "FIELD", "VALUE", &ret);
  db.HSet("GP5_PKPATTERNMATCHDEL_HASH_KEY7_0ooo0", "FIELD", "VALUE", &ret);
  db.HSet("GP5_PKPATTERNMATCHDEL_HASH_KEY8_0xxx0", "FIELD", "VALUE", &ret);
  ASSERT_TRUE(make_expired(&db, "GP5_PKPATTERNMATCHDEL_HASH_KEY1_0ooo0"));
  ASSERT_TRUE(make_expired(&db, "GP5_PKPATTERNMATCHDEL_HASH_KEY2_0xxx0"));
  db.HDel("GP5_PKPATTERNMATCHDEL_HASH_KEY3_0ooo0", {"FIELD"}, &ret);
  db.HDel("GP5_PKPATTERNMATCHDEL_HASH_KEY4_0xxx0", {"FIELD"}, &ret);
  s = db.PKPatternMatchDelWithRemoveKeys("*0ooo0", &delete_count, &remove_keys, max_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 2);
  ASSERT_EQ(remove_keys.size(), 2);
  keys.clear();
  remove_keys.clear();
  db.Keys(DataType::kHashes, "*", &keys);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP5_PKPATTERNMATCHDEL_HASH_KEY6_0xxx0");
  ASSERT_EQ(keys[1], "GP5_PKPATTERNMATCHDEL_HASH_KEY8_0xxx0");
  type_status.clear();
  db.Del(keys);

  // ***************** Group 6 Test *****************
  size_t gp6_total_hash = 23333;
  for (size_t idx = 0; idx < gp6_total_hash; ++idx) {
    db.HSet("GP6_PKPATTERNMATCHDEL_HASH_KEY" + std::to_string(idx), "FIELD", "VALUE", &ret);
  }
  s = db.PKPatternMatchDelWithRemoveKeys("*", &delete_count, &remove_keys, max_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, max_count);
  ASSERT_EQ(remove_keys.size(), max_count);
  keys.clear();
  remove_keys.clear();
  db.Keys(DataType::kHashes, "*", &keys);
  ASSERT_EQ(keys.size(), gp6_total_hash - max_count);
  db.Del(keys);

  //=============================== ZSets ===============================

  // ***************** Group 1 Test *****************
  db.ZAdd("GP1_PKPATTERNMATCHDEL_ZSET_KEY1", {{1, "M"}}, &ret);
  db.ZAdd("GP1_PKPATTERNMATCHDEL_ZSET_KEY2", {{1, "M"}}, &ret);
  db.ZAdd("GP1_PKPATTERNMATCHDEL_ZSET_KEY3", {{1, "M"}}, &ret);
  db.ZAdd("GP1_PKPATTERNMATCHDEL_ZSET_KEY4", {{1, "M"}}, &ret);
  db.ZAdd("GP1_PKPATTERNMATCHDEL_ZSET_KEY5", {{1, "M"}}, &ret);
  db.ZAdd("GP1_PKPATTERNMATCHDEL_ZSET_KEY6", {{1, "M"}}, &ret);
  s = db.PKPatternMatchDelWithRemoveKeys("*", &delete_count, &remove_keys, max_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 6);
  ASSERT_EQ(remove_keys.size(), 6);
  keys.clear();
  remove_keys.clear();
  db.Keys(DataType::kZSets, "*", &keys);
  ASSERT_EQ(keys.size(), 0);

  // ***************** Group 2 Test *****************
  db.ZAdd("GP2_PKPATTERNMATCHDEL_ZSET_KEY1", {{1, "M"}}, &ret);
  db.ZAdd("GP2_PKPATTERNMATCHDEL_ZSET_KEY2", {{1, "M"}}, &ret);
  db.ZAdd("GP2_PKPATTERNMATCHDEL_ZSET_KEY3", {{1, "M"}}, &ret);
  db.ZAdd("GP2_PKPATTERNMATCHDEL_ZSET_KEY4", {{1, "M"}}, &ret);
  db.ZAdd("GP2_PKPATTERNMATCHDEL_ZSET_KEY5", {{1, "M"}}, &ret);
  db.ZAdd("GP2_PKPATTERNMATCHDEL_ZSET_KEY6", {{1, "M"}}, &ret);
  ASSERT_TRUE(make_expired(&db, "GP2_PKPATTERNMATCHDEL_ZSET_KEY1"));
  ASSERT_TRUE(make_expired(&db, "GP2_PKPATTERNMATCHDEL_ZSET_KEY3"));
  ASSERT_TRUE(make_expired(&db, "GP2_PKPATTERNMATCHDEL_ZSET_KEY5"));
  s = db.PKPatternMatchDelWithRemoveKeys("*", &delete_count, &remove_keys, max_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 3);
  ASSERT_EQ(remove_keys.size(), 3);
  keys.clear();
  remove_keys.clear();
  db.Keys(DataType::kZSets, "*", &keys);
  ASSERT_EQ(keys.size(), 0);

  // ***************** Group 3 Test *****************
  db.ZAdd("GP3_PKPATTERNMATCHDEL_ZSET_KEY1_0xxx0", {{1, "M"}}, &ret);
  db.ZAdd("GP3_PKPATTERNMATCHDEL_ZSET_KEY2_0ooo0", {{1, "M"}}, &ret);
  db.ZAdd("GP3_PKPATTERNMATCHDEL_ZSET_KEY3_0xxx0", {{1, "M"}}, &ret);
  db.ZAdd("GP3_PKPATTERNMATCHDEL_ZSET_KEY4_0ooo0", {{1, "M"}}, &ret);
  db.ZAdd("GP3_PKPATTERNMATCHDEL_ZSET_KEY5_0xxx0", {{1, "M"}}, &ret);
  db.ZAdd("GP3_PKPATTERNMATCHDEL_ZSET_KEY6_0ooo0", {{1, "M"}}, &ret);
  s = db.PKPatternMatchDelWithRemoveKeys("*0ooo0", &delete_count, &remove_keys, max_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 3);
  ASSERT_EQ(remove_keys.size(), 3);
  keys.clear();
  remove_keys.clear();
  db.Keys(DataType::kZSets, "*", &keys);
  ASSERT_EQ(keys.size(), 3);
  ASSERT_EQ("GP3_PKPATTERNMATCHDEL_ZSET_KEY1_0xxx0", keys[0]);
  ASSERT_EQ("GP3_PKPATTERNMATCHDEL_ZSET_KEY3_0xxx0", keys[1]);
  ASSERT_EQ("GP3_PKPATTERNMATCHDEL_ZSET_KEY5_0xxx0", keys[2]);
  type_status.clear();
  db.Del(keys);

  // ***************** Group 4 Test *****************
  db.ZAdd("GP4_PKPATTERNMATCHDEL_ZSET_KEY1", {{1, "M"}}, &ret);
  db.ZAdd("GP4_PKPATTERNMATCHDEL_ZSET_KEY2", {{1, "M"}}, &ret);
  db.ZAdd("GP4_PKPATTERNMATCHDEL_ZSET_KEY3", {{1, "M"}}, &ret);
  db.ZAdd("GP4_PKPATTERNMATCHDEL_ZSET_KEY4", {{1, "M"}}, &ret);
  db.ZAdd("GP4_PKPATTERNMATCHDEL_ZSET_KEY5", {{1, "M"}}, &ret);
  db.ZAdd("GP4_PKPATTERNMATCHDEL_ZSET_KEY6", {{1, "M"}}, &ret);
  db.ZRem("GP4_PKPATTERNMATCHDEL_ZSET_KEY1", {"M"}, &ret);
  db.ZRem("GP4_PKPATTERNMATCHDEL_ZSET_KEY3", {"M"}, &ret);
  db.ZRem("GP4_PKPATTERNMATCHDEL_ZSET_KEY5", {"M"}, &ret);
  s = db.PKPatternMatchDelWithRemoveKeys("*", &delete_count, &remove_keys, max_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 3);
  ASSERT_EQ(remove_keys.size(), 3);
  keys.clear();
  remove_keys.clear();
  db.Keys(DataType::kZSets, "*", &keys);
  ASSERT_EQ(keys.size(), 0);

  // ***************** Group 5 Test *****************
  db.ZAdd("GP5_PKPATTERNMATCHDEL_ZSET_KEY1_0ooo0", {{1, "M"}}, &ret);
  db.ZAdd("GP5_PKPATTERNMATCHDEL_ZSET_KEY2_0xxx0", {{1, "M"}}, &ret);
  db.ZAdd("GP5_PKPATTERNMATCHDEL_ZSET_KEY3_0ooo0", {{1, "M"}}, &ret);
  db.ZAdd("GP5_PKPATTERNMATCHDEL_ZSET_KEY4_0xxx0", {{1, "M"}}, &ret);
  db.ZAdd("GP5_PKPATTERNMATCHDEL_ZSET_KEY5_0ooo0", {{1, "M"}}, &ret);
  db.ZAdd("GP5_PKPATTERNMATCHDEL_ZSET_KEY6_0xxx0", {{1, "M"}}, &ret);
  db.ZAdd("GP5_PKPATTERNMATCHDEL_ZSET_KEY7_0ooo0", {{1, "M"}}, &ret);
  db.ZAdd("GP5_PKPATTERNMATCHDEL_ZSET_KEY8_0xxx0", {{1, "M"}}, &ret);
  ASSERT_TRUE(make_expired(&db, "GP5_PKPATTERNMATCHDEL_ZSET_KEY1_0ooo0"));
  ASSERT_TRUE(make_expired(&db, "GP5_PKPATTERNMATCHDEL_ZSET_KEY2_0xxx0"));
  db.ZRem("GP5_PKPATTERNMATCHDEL_ZSET_KEY3_0ooo0", {"M"}, &ret);
  db.ZRem("GP5_PKPATTERNMATCHDEL_ZSET_KEY4_0xxx0", {"M"}, &ret);
  s = db.PKPatternMatchDelWithRemoveKeys("*0ooo0", &delete_count, &remove_keys, max_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 2);
  ASSERT_EQ(remove_keys.size(), 2);
  keys.clear();
  remove_keys.clear();
  db.Keys(DataType::kZSets, "*", &keys);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP5_PKPATTERNMATCHDEL_ZSET_KEY6_0xxx0");
  ASSERT_EQ(keys[1], "GP5_PKPATTERNMATCHDEL_ZSET_KEY8_0xxx0");
  type_status.clear();
  db.Del(keys);

  // ***************** Group 6 Test *****************
  size_t gp6_total_zset = 23333;
  for (size_t idx = 0; idx < gp6_total_zset; ++idx) {
    db.ZAdd("GP6_PKPATTERNMATCHDEL_ZSET_KEY" + std::to_string(idx), {{1, "M"}}, &ret);
  }
  s = db.PKPatternMatchDelWithRemoveKeys("*", &delete_count, &remove_keys, max_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, max_count);
  ASSERT_EQ(remove_keys.size(), max_count);
  keys.clear();
  remove_keys.clear();
  db.Keys(DataType::kZSets, "*", &keys);
  ASSERT_EQ(keys.size(), gp6_total_zset-max_count);
  db.Del(keys);

  //=============================== List ===============================

  // ***************** Group 1 Test *****************
  db.LPush("GP1_PKPATTERNMATCHDEL_LIST_KEY1", {"VALUE"}, &ret64);
  db.LPush("GP1_PKPATTERNMATCHDEL_LIST_KEY2", {"VALUE"}, &ret64);
  db.LPush("GP1_PKPATTERNMATCHDEL_LIST_KEY3", {"VALUE"}, &ret64);
  db.LPush("GP1_PKPATTERNMATCHDEL_LIST_KEY4", {"VALUE"}, &ret64);
  db.LPush("GP1_PKPATTERNMATCHDEL_LIST_KEY5", {"VALUE"}, &ret64);
  db.LPush("GP1_PKPATTERNMATCHDEL_LIST_KEY6", {"VALUE"}, &ret64);
  s = db.PKPatternMatchDelWithRemoveKeys("*", &delete_count, &remove_keys, max_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 6);
  ASSERT_EQ(remove_keys.size(), 6);
  keys.clear();
  remove_keys.clear();
  db.Keys(DataType::kLists, "*", &keys);
  ASSERT_EQ(keys.size(), 0);

  // ***************** Group 2 Test *****************
  db.LPush("GP2_PKPATTERNMATCHDEL_LIST_KEY1", {"VALUE"}, &ret64);
  db.LPush("GP2_PKPATTERNMATCHDEL_LIST_KEY2", {"VALUE"}, &ret64);
  db.LPush("GP2_PKPATTERNMATCHDEL_LIST_KEY3", {"VALUE"}, &ret64);
  db.LPush("GP2_PKPATTERNMATCHDEL_LIST_KEY4", {"VALUE"}, &ret64);
  db.LPush("GP2_PKPATTERNMATCHDEL_LIST_KEY5", {"VALUE"}, &ret64);
  db.LPush("GP2_PKPATTERNMATCHDEL_LIST_KEY6", {"VALUE"}, &ret64);
  ASSERT_TRUE(make_expired(&db, "GP2_PKPATTERNMATCHDEL_LIST_KEY1"));
  ASSERT_TRUE(make_expired(&db, "GP2_PKPATTERNMATCHDEL_LIST_KEY3"));
  ASSERT_TRUE(make_expired(&db, "GP2_PKPATTERNMATCHDEL_LIST_KEY5"));
  s = db.PKPatternMatchDelWithRemoveKeys("*", &delete_count, &remove_keys, max_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 3);
  ASSERT_EQ(remove_keys.size(), 3);
  keys.clear();
  remove_keys.clear();
  db.Keys(DataType::kLists, "*", &keys);
  ASSERT_EQ(keys.size(), 0);

  // ***************** Group 3 Test *****************
  db.LPush("GP3_PKPATTERNMATCHDEL_LIST_KEY1_0xxx0", {"VALUE"}, &ret64);
  db.LPush("GP3_PKPATTERNMATCHDEL_LIST_KEY2_0ooo0", {"VALUE"}, &ret64);
  db.LPush("GP3_PKPATTERNMATCHDEL_LIST_KEY3_0xxx0", {"VALUE"}, &ret64);
  db.LPush("GP3_PKPATTERNMATCHDEL_LIST_KEY4_0ooo0", {"VALUE"}, &ret64);
  db.LPush("GP3_PKPATTERNMATCHDEL_LIST_KEY5_0xxx0", {"VALUE"}, &ret64);
  db.LPush("GP3_PKPATTERNMATCHDEL_LIST_KEY6_0ooo0", {"VALUE"}, &ret64);
  s = db.PKPatternMatchDelWithRemoveKeys("*0ooo0", &delete_count, &remove_keys, max_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 3);
  ASSERT_EQ(remove_keys.size(), 3);
  keys.clear();
  remove_keys.clear();
  db.Keys(DataType::kLists, "*", &keys);
  ASSERT_EQ(keys.size(), 3);
  ASSERT_EQ("GP3_PKPATTERNMATCHDEL_LIST_KEY1_0xxx0", keys[0]);
  ASSERT_EQ("GP3_PKPATTERNMATCHDEL_LIST_KEY3_0xxx0", keys[1]);
  ASSERT_EQ("GP3_PKPATTERNMATCHDEL_LIST_KEY5_0xxx0", keys[2]);
  type_status.clear();
  db.Del(keys);

  // ***************** Group 4 Test *****************
  db.LPush("GP4_PKPATTERNMATCHDEL_LIST_KEY1", {"VALUE"}, &ret64);
  db.LPush("GP4_PKPATTERNMATCHDEL_LIST_KEY2", {"VALUE"}, &ret64);
  db.LPush("GP4_PKPATTERNMATCHDEL_LIST_KEY3", {"VALUE"}, &ret64);
  db.LPush("GP4_PKPATTERNMATCHDEL_LIST_KEY4", {"VALUE"}, &ret64);
  db.LPush("GP4_PKPATTERNMATCHDEL_LIST_KEY5", {"VALUE"}, &ret64);
  db.LPush("GP4_PKPATTERNMATCHDEL_LIST_KEY6", {"VALUE"}, &ret64);
  db.LRem("GP4_PKPATTERNMATCHDEL_LIST_KEY1", 1, "VALUE", &ret64);
  db.LRem("GP4_PKPATTERNMATCHDEL_LIST_KEY3", 1, "VALUE", &ret64);
  db.LRem("GP4_PKPATTERNMATCHDEL_LIST_KEY5", 1, "VALUE", &ret64);
  s = db.PKPatternMatchDelWithRemoveKeys("*", &delete_count, &remove_keys, max_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 3);
  ASSERT_EQ(remove_keys.size(), 3);
  keys.clear();
  remove_keys.clear();
  db.Keys(DataType::kLists, "*", &keys);
  ASSERT_EQ(keys.size(), 0);

  // ***************** Group 5 Test *****************
  db.LPush("GP5_PKPATTERNMATCHDEL_LIST_KEY1_0ooo0", {"VALUE"}, &ret64);
  db.LPush("GP5_PKPATTERNMATCHDEL_LIST_KEY2_0xxx0", {"VALUE"}, &ret64);
  db.LPush("GP5_PKPATTERNMATCHDEL_LIST_KEY3_0ooo0", {"VALUE"}, &ret64);
  db.LPush("GP5_PKPATTERNMATCHDEL_LIST_KEY4_0xxx0", {"VALUE"}, &ret64);
  db.LPush("GP5_PKPATTERNMATCHDEL_LIST_KEY5_0ooo0", {"VALUE"}, &ret64);
  db.LPush("GP5_PKPATTERNMATCHDEL_LIST_KEY6_0xxx0", {"VALUE"}, &ret64);
  db.LPush("GP5_PKPATTERNMATCHDEL_LIST_KEY7_0ooo0", {"VALUE"}, &ret64);
  db.LPush("GP5_PKPATTERNMATCHDEL_LIST_KEY8_0xxx0", {"VALUE"}, &ret64);
  ASSERT_TRUE(make_expired(&db, "GP5_PKPATTERNMATCHDEL_LIST_KEY1_0ooo0"));
  ASSERT_TRUE(make_expired(&db, "GP5_PKPATTERNMATCHDEL_LIST_KEY2_0xxx0"));
  db.LRem("GP5_PKPATTERNMATCHDEL_LIST_KEY3_0ooo0", 1, "VALUE", &ret64);
  db.LRem("GP5_PKPATTERNMATCHDEL_LIST_KEY4_0xxx0", 1, "VALUE", &ret64);
  s = db.PKPatternMatchDelWithRemoveKeys("*0ooo0", &delete_count, &remove_keys, max_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 2);
  ASSERT_EQ(remove_keys.size(), 2);
  keys.clear();
  remove_keys.clear();
  db.Keys(DataType::kLists, "*", &keys);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP5_PKPATTERNMATCHDEL_LIST_KEY6_0xxx0");
  ASSERT_EQ(keys[1], "GP5_PKPATTERNMATCHDEL_LIST_KEY8_0xxx0");
  type_status.clear();
  db.Del(keys);

  // ***************** Group 6 Test *****************
  size_t gp6_total_list = 23333;
  for (size_t idx = 0; idx < gp6_total_list; ++idx) {
    db.LPush("GP6_PKPATTERNMATCHDEL_LIST_KEY" + std::to_string(idx), {"VALUE"}, &ret64);
  }
  s = db.PKPatternMatchDelWithRemoveKeys("*", &delete_count, &remove_keys, max_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, max_count);
  ASSERT_EQ(remove_keys.size(), max_count);
  keys.clear();
  remove_keys.clear();
  db.Keys(DataType::kLists, "*", &keys);
  ASSERT_EQ(keys.size(), gp6_total_list - max_count);
  db.Del(keys);

  sleep(2);
  db.Compact(DataType::kAll, true);
}

// Scan
// Note: This test needs to execute at first because all of the data is
// predetermined.
TEST_F(KeysTest, ScanCaseAllTest) {  // NOLINT
  int64_t cursor;
  int64_t next_cursor;
  int64_t del_num;
  int32_t int32_ret;
  uint64_t uint64_ret;
  std::vector<std::string> keys;
  std::vector<std::string> total_keys;
  std::vector<std::string> delete_keys;
  std::map<storage::DataType, Status> type_status;

  // ***************** Group 1 Test *****************
  // String
  s = db.Set("GP1_SCAN_CASE_ALL_STRING_KEY1", "GP1_SCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP1_SCAN_CASE_ALL_STRING_KEY2", "GP1_SCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP1_SCAN_CASE_ALL_STRING_KEY3", "GP1_SCAN_CASE_ALL_STRING_VALUE3");

  // Hash
  s = db.HSet("GP1_SCAN_CASE_ALL_HASH_KEY1", "GP1_SCAN_CASE_ALL_HASH_FIELD1", "GP1_SCAN_CASE_ALL_HASH_VALUE1",
              &int32_ret);
  s = db.HSet("GP1_SCAN_CASE_ALL_HASH_KEY2", "GP1_SCAN_CASE_ALL_HASH_FIELD2", "GP1_SCAN_CASE_ALL_HASH_VALUE2",
              &int32_ret);
  s = db.HSet("GP1_SCAN_CASE_ALL_HASH_KEY3", "GP1_SCAN_CASE_ALL_HASH_FIELD3", "GP1_SCAN_CASE_ALL_HASH_VALUE3",
              &int32_ret);

  // Set
  s = db.SAdd("GP1_SCAN_CASE_ALL_SET_KEY1", {"GP1_SCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP1_SCAN_CASE_ALL_SET_KEY2", {"GP1_SCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP1_SCAN_CASE_ALL_SET_KEY3", {"GP1_SCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);

  // List
  s = db.LPush("GP1_SCAN_CASE_ALL_LIST_KEY1", {"GP1_SCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP1_SCAN_CASE_ALL_LIST_KEY2", {"GP1_SCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP1_SCAN_CASE_ALL_LIST_KEY3", {"GP1_SCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);

  // ZSet
  s = db.ZAdd("GP1_SCAN_CASE_ALL_ZSET_KEY1", {{1, "GP1_SCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP1_SCAN_CASE_ALL_ZSET_KEY2", {{1, "GP1_SCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP1_SCAN_CASE_ALL_ZSET_KEY3", {{1, "GP1_SCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);

  // Scan
  delete_keys.clear();
  keys.clear();
  cursor = db.Scan(DataType::kAll, 0, "*", 3, &keys);
  ASSERT_EQ(cursor, 3);
  ASSERT_EQ(keys.size(), 3);
  ASSERT_EQ(keys[0], "GP1_SCAN_CASE_ALL_STRING_KEY1");
  ASSERT_EQ(keys[1], "GP1_SCAN_CASE_ALL_STRING_KEY2");
  ASSERT_EQ(keys[2], "GP1_SCAN_CASE_ALL_STRING_KEY3");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.Scan(DataType::kAll, 3, "*", 3, &keys);
  ASSERT_EQ(cursor, 6);
  ASSERT_EQ(keys.size(), 3);
  ASSERT_EQ(keys[0], "GP1_SCAN_CASE_ALL_HASH_KEY1");
  ASSERT_EQ(keys[1], "GP1_SCAN_CASE_ALL_HASH_KEY2");
  ASSERT_EQ(keys[2], "GP1_SCAN_CASE_ALL_HASH_KEY3");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.Scan(DataType::kAll, 6, "*", 3, &keys);
  ASSERT_EQ(cursor, 9);
  ASSERT_EQ(keys.size(), 3);
  ASSERT_EQ(keys[0], "GP1_SCAN_CASE_ALL_SET_KEY1");
  ASSERT_EQ(keys[1], "GP1_SCAN_CASE_ALL_SET_KEY2");
  ASSERT_EQ(keys[2], "GP1_SCAN_CASE_ALL_SET_KEY3");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.Scan(DataType::kAll, 9, "*", 3, &keys);
  ASSERT_EQ(cursor, 12);
  ASSERT_EQ(keys.size(), 3);
  ASSERT_EQ(keys[0], "GP1_SCAN_CASE_ALL_LIST_KEY1");
  ASSERT_EQ(keys[1], "GP1_SCAN_CASE_ALL_LIST_KEY2");
  ASSERT_EQ(keys[2], "GP1_SCAN_CASE_ALL_LIST_KEY3");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.Scan(DataType::kAll, 12, "*", 3, &keys);
  ASSERT_EQ(cursor, 0);
  ASSERT_EQ(keys.size(), 3);
  ASSERT_EQ(keys[0], "GP1_SCAN_CASE_ALL_ZSET_KEY1");
  ASSERT_EQ(keys[1], "GP1_SCAN_CASE_ALL_ZSET_KEY2");
  ASSERT_EQ(keys[2], "GP1_SCAN_CASE_ALL_ZSET_KEY3");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  del_num = db.Del(delete_keys);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);

  // ***************** Group 2 Test *****************
  // String
  s = db.Set("GP2_SCAN_CASE_ALL_STRING_KEY1", "GP2_SCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP2_SCAN_CASE_ALL_STRING_KEY2", "GP2_SCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP2_SCAN_CASE_ALL_STRING_KEY3", "GP2_SCAN_CASE_ALL_STRING_VALUE3");

  // Hash
  s = db.HSet("GP2_SCAN_CASE_ALL_HASH_KEY1", "GP2_SCAN_CASE_ALL_HASH_FIELD1", "GP2_SCAN_CASE_ALL_HASH_VALUE1",
              &int32_ret);
  s = db.HSet("GP2_SCAN_CASE_ALL_HASH_KEY2", "GP2_SCAN_CASE_ALL_HASH_FIELD2", "GP2_SCAN_CASE_ALL_HASH_VALUE2",
              &int32_ret);
  s = db.HSet("GP2_SCAN_CASE_ALL_HASH_KEY3", "GP2_SCAN_CASE_ALL_HASH_FIELD3", "GP2_SCAN_CASE_ALL_HASH_VALUE3",
              &int32_ret);

  // Set
  s = db.SAdd("GP2_SCAN_CASE_ALL_SET_KEY1", {"GP2_SCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP2_SCAN_CASE_ALL_SET_KEY2", {"GP2_SCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP2_SCAN_CASE_ALL_SET_KEY3", {"GP2_SCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);

  // List
  s = db.LPush("GP2_SCAN_CASE_ALL_LIST_KEY1", {"GP2_SCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP2_SCAN_CASE_ALL_LIST_KEY2", {"GP2_SCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP2_SCAN_CASE_ALL_LIST_KEY3", {"GP2_SCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);

  // ZSet
  s = db.ZAdd("GP2_SCAN_CASE_ALL_ZSET_KEY1", {{1, "GP2_SCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP2_SCAN_CASE_ALL_ZSET_KEY2", {{1, "GP2_SCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP2_SCAN_CASE_ALL_ZSET_KEY3", {{1, "GP2_SCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);

  // Scan
  delete_keys.clear();
  keys.clear();
  cursor = db.Scan(DataType::kAll, 0, "*", 2, &keys);
  ASSERT_EQ(cursor, 2);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP2_SCAN_CASE_ALL_STRING_KEY1");
  ASSERT_EQ(keys[1], "GP2_SCAN_CASE_ALL_STRING_KEY2");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.Scan(DataType::kAll, 2, "*", 2, &keys);
  ASSERT_EQ(cursor, 4);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP2_SCAN_CASE_ALL_STRING_KEY3");
  ASSERT_EQ(keys[1], "GP2_SCAN_CASE_ALL_HASH_KEY1");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.Scan(DataType::kAll, 4, "*", 2, &keys);
  ASSERT_EQ(cursor, 6);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP2_SCAN_CASE_ALL_HASH_KEY2");
  ASSERT_EQ(keys[1], "GP2_SCAN_CASE_ALL_HASH_KEY3");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.Scan(DataType::kAll, 6, "*", 2, &keys);
  ASSERT_EQ(cursor, 8);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP2_SCAN_CASE_ALL_SET_KEY1");
  ASSERT_EQ(keys[1], "GP2_SCAN_CASE_ALL_SET_KEY2");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.Scan(DataType::kAll, 8, "*", 2, &keys);
  ASSERT_EQ(cursor, 10);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP2_SCAN_CASE_ALL_SET_KEY3");
  ASSERT_EQ(keys[1], "GP2_SCAN_CASE_ALL_LIST_KEY1");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.Scan(DataType::kAll, 10, "*", 2, &keys);
  ASSERT_EQ(cursor, 12);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP2_SCAN_CASE_ALL_LIST_KEY2");
  ASSERT_EQ(keys[1], "GP2_SCAN_CASE_ALL_LIST_KEY3");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.Scan(DataType::kAll, 12, "*", 2, &keys);
  ASSERT_EQ(cursor, 14);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP2_SCAN_CASE_ALL_ZSET_KEY1");
  ASSERT_EQ(keys[1], "GP2_SCAN_CASE_ALL_ZSET_KEY2");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.Scan(DataType::kAll, 14, "*", 2, &keys);
  ASSERT_EQ(cursor, 0);
  ASSERT_EQ(keys.size(), 1);
  ASSERT_EQ(keys[0], "GP2_SCAN_CASE_ALL_ZSET_KEY3");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  del_num = db.Del(delete_keys);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);

  // ***************** Group 3 Test *****************
  // String
  s = db.Set("GP3_SCAN_CASE_ALL_STRING_KEY1", "GP3_SCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP3_SCAN_CASE_ALL_STRING_KEY2", "GP3_SCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP3_SCAN_CASE_ALL_STRING_KEY3", "GP3_SCAN_CASE_ALL_STRING_VALUE3");

  // Hash
  s = db.HSet("GP3_SCAN_CASE_ALL_HASH_KEY1", "GP3_SCAN_CASE_ALL_HASH_FIELD1", "GP3_SCAN_CASE_ALL_HASH_VALUE1",
              &int32_ret);
  s = db.HSet("GP3_SCAN_CASE_ALL_HASH_KEY2", "GP3_SCAN_CASE_ALL_HASH_FIELD2", "GP3_SCAN_CASE_ALL_HASH_VALUE2",
              &int32_ret);
  s = db.HSet("GP3_SCAN_CASE_ALL_HASH_KEY3", "GP3_SCAN_CASE_ALL_HASH_FIELD3", "GP3_SCAN_CASE_ALL_HASH_VALUE3",
              &int32_ret);

  // Set
  s = db.SAdd("GP3_SCAN_CASE_ALL_SET_KEY1", {"GP3_SCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP3_SCAN_CASE_ALL_SET_KEY2", {"GP3_SCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP3_SCAN_CASE_ALL_SET_KEY3", {"GP3_SCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);

  // List
  s = db.LPush("GP3_SCAN_CASE_ALL_LIST_KEY1", {"GP3_SCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP3_SCAN_CASE_ALL_LIST_KEY2", {"GP3_SCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP3_SCAN_CASE_ALL_LIST_KEY3", {"GP3_SCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);

  // ZSet
  s = db.ZAdd("GP3_SCAN_CASE_ALL_ZSET_KEY1", {{1, "GP3_SCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP3_SCAN_CASE_ALL_ZSET_KEY2", {{1, "GP3_SCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP3_SCAN_CASE_ALL_ZSET_KEY3", {{1, "GP3_SCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);

  // Scan
  delete_keys.clear();
  keys.clear();
  cursor = db.Scan(DataType::kAll, 0, "*", 5, &keys);
  ASSERT_EQ(cursor, 5);
  ASSERT_EQ(keys.size(), 5);
  ASSERT_EQ(keys[0], "GP3_SCAN_CASE_ALL_STRING_KEY1");
  ASSERT_EQ(keys[1], "GP3_SCAN_CASE_ALL_STRING_KEY2");
  ASSERT_EQ(keys[2], "GP3_SCAN_CASE_ALL_STRING_KEY3");
  ASSERT_EQ(keys[3], "GP3_SCAN_CASE_ALL_HASH_KEY1");
  ASSERT_EQ(keys[4], "GP3_SCAN_CASE_ALL_HASH_KEY2");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.Scan(DataType::kAll, 5, "*", 5, &keys);
  ASSERT_EQ(cursor, 10);
  ASSERT_EQ(keys.size(), 5);
  ASSERT_EQ(keys[0], "GP3_SCAN_CASE_ALL_HASH_KEY3");
  ASSERT_EQ(keys[1], "GP3_SCAN_CASE_ALL_SET_KEY1");
  ASSERT_EQ(keys[2], "GP3_SCAN_CASE_ALL_SET_KEY2");
  ASSERT_EQ(keys[3], "GP3_SCAN_CASE_ALL_SET_KEY3");
  ASSERT_EQ(keys[4], "GP3_SCAN_CASE_ALL_LIST_KEY1");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.Scan(DataType::kAll, 10, "*", 5, &keys);
  ASSERT_EQ(cursor, 0);
  ASSERT_EQ(keys.size(), 5);
  ASSERT_EQ(keys[0], "GP3_SCAN_CASE_ALL_LIST_KEY2");
  ASSERT_EQ(keys[1], "GP3_SCAN_CASE_ALL_LIST_KEY3");
  ASSERT_EQ(keys[2], "GP3_SCAN_CASE_ALL_ZSET_KEY1");
  ASSERT_EQ(keys[3], "GP3_SCAN_CASE_ALL_ZSET_KEY2");
  ASSERT_EQ(keys[4], "GP3_SCAN_CASE_ALL_ZSET_KEY3");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  del_num = db.Del(delete_keys);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);

  // ***************** Group 4 Test *****************
  // String
  s = db.Set("GP4_SCAN_CASE_ALL_STRING_KEY1", "GP4_SCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP4_SCAN_CASE_ALL_STRING_KEY2", "GP4_SCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP4_SCAN_CASE_ALL_STRING_KEY3", "GP4_SCAN_CASE_ALL_STRING_VALUE3");

  // Hash
  s = db.HSet("GP4_SCAN_CASE_ALL_HASH_KEY1", "GP4_SCAN_CASE_ALL_HASH_FIELD1", "GP4_SCAN_CASE_ALL_HASH_VALUE1",
              &int32_ret);
  s = db.HSet("GP4_SCAN_CASE_ALL_HASH_KEY2", "GP4_SCAN_CASE_ALL_HASH_FIELD2", "GP4_SCAN_CASE_ALL_HASH_VALUE2",
              &int32_ret);
  s = db.HSet("GP4_SCAN_CASE_ALL_HASH_KEY3", "GP4_SCAN_CASE_ALL_HASH_FIELD3", "GP4_SCAN_CASE_ALL_HASH_VALUE3",
              &int32_ret);

  // Set
  s = db.SAdd("GP4_SCAN_CASE_ALL_SET_KEY1", {"GP4_SCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP4_SCAN_CASE_ALL_SET_KEY2", {"GP4_SCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP4_SCAN_CASE_ALL_SET_KEY3", {"GP4_SCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);

  // List
  s = db.LPush("GP4_SCAN_CASE_ALL_LIST_KEY1", {"GP4_SCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP4_SCAN_CASE_ALL_LIST_KEY2", {"GP4_SCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP4_SCAN_CASE_ALL_LIST_KEY3", {"GP4_SCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);

  // ZSet
  s = db.ZAdd("GP4_SCAN_CASE_ALL_ZSET_KEY1", {{1, "GP4_SCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP4_SCAN_CASE_ALL_ZSET_KEY2", {{1, "GP4_SCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP4_SCAN_CASE_ALL_ZSET_KEY3", {{1, "GP4_SCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);

  delete_keys.clear();
  keys.clear();
  cursor = db.Scan(DataType::kAll, 0, "*", 15, &keys);
  ASSERT_EQ(cursor, 0);
  ASSERT_EQ(keys.size(), 15);
  ASSERT_EQ(keys[0], "GP4_SCAN_CASE_ALL_STRING_KEY1");
  ASSERT_EQ(keys[1], "GP4_SCAN_CASE_ALL_STRING_KEY2");
  ASSERT_EQ(keys[2], "GP4_SCAN_CASE_ALL_STRING_KEY3");
  ASSERT_EQ(keys[3], "GP4_SCAN_CASE_ALL_HASH_KEY1");
  ASSERT_EQ(keys[4], "GP4_SCAN_CASE_ALL_HASH_KEY2");
  ASSERT_EQ(keys[5], "GP4_SCAN_CASE_ALL_HASH_KEY3");
  ASSERT_EQ(keys[6], "GP4_SCAN_CASE_ALL_SET_KEY1");
  ASSERT_EQ(keys[7], "GP4_SCAN_CASE_ALL_SET_KEY2");
  ASSERT_EQ(keys[8], "GP4_SCAN_CASE_ALL_SET_KEY3");
  ASSERT_EQ(keys[9], "GP4_SCAN_CASE_ALL_LIST_KEY1");
  ASSERT_EQ(keys[10], "GP4_SCAN_CASE_ALL_LIST_KEY2");
  ASSERT_EQ(keys[11], "GP4_SCAN_CASE_ALL_LIST_KEY3");
  ASSERT_EQ(keys[12], "GP4_SCAN_CASE_ALL_ZSET_KEY1");
  ASSERT_EQ(keys[13], "GP4_SCAN_CASE_ALL_ZSET_KEY2");
  ASSERT_EQ(keys[14], "GP4_SCAN_CASE_ALL_ZSET_KEY3");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  del_num = db.Del(delete_keys);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);

  // ***************** Group 5 Test *****************
  delete_keys.clear();
  // String
  s = db.Set("GP5_SCAN_CASE_ALL_STRING_KEY1", "GP5_SCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP5_SCAN_CASE_ALL_STRING_KEY2", "GP5_SCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP5_SCAN_CASE_ALL_STRING_KEY3", "GP5_SCAN_CASE_ALL_STRING_VALUE3");
  delete_keys.emplace_back("GP5_SCAN_CASE_ALL_STRING_KEY1");
  delete_keys.emplace_back("GP5_SCAN_CASE_ALL_STRING_KEY2");
  delete_keys.emplace_back("GP5_SCAN_CASE_ALL_STRING_KEY3");

  // Hash
  s = db.HSet("GP5_SCAN_CASE_ALL_HASH_KEY1", "GP5_SCAN_CASE_ALL_HASH_FIELD1", "GP5_SCAN_CASE_ALL_HASH_VALUE1",
              &int32_ret);
  s = db.HSet("GP5_SCAN_CASE_ALL_HASH_KEY2", "GP5_SCAN_CASE_ALL_HASH_FIELD2", "GP5_SCAN_CASE_ALL_HASH_VALUE2",
              &int32_ret);
  s = db.HSet("GP5_SCAN_CASE_ALL_HASH_KEY3", "GP5_SCAN_CASE_ALL_HASH_FIELD3", "GP5_SCAN_CASE_ALL_HASH_VALUE3",
              &int32_ret);
  delete_keys.emplace_back("GP5_SCAN_CASE_ALL_HASH_KEY1");
  delete_keys.emplace_back("GP5_SCAN_CASE_ALL_HASH_KEY2");
  delete_keys.emplace_back("GP5_SCAN_CASE_ALL_HASH_KEY3");

  // Set
  s = db.SAdd("GP5_SCAN_CASE_ALL_SET_KEY1", {"GP5_SCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP5_SCAN_CASE_ALL_SET_KEY2", {"GP5_SCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP5_SCAN_CASE_ALL_SET_KEY3", {"GP5_SCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);
  delete_keys.emplace_back("GP5_SCAN_CASE_ALL_SET_KEY1");
  delete_keys.emplace_back("GP5_SCAN_CASE_ALL_SET_KEY2");
  delete_keys.emplace_back("GP5_SCAN_CASE_ALL_SET_KEY3");

  // List
  s = db.LPush("GP5_SCAN_CASE_ALL_LIST_KEY1", {"GP5_SCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP5_SCAN_CASE_ALL_LIST_KEY2", {"GP5_SCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP5_SCAN_CASE_ALL_LIST_KEY3", {"GP5_SCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);
  delete_keys.emplace_back("GP5_SCAN_CASE_ALL_LIST_KEY1");
  delete_keys.emplace_back("GP5_SCAN_CASE_ALL_LIST_KEY2");
  delete_keys.emplace_back("GP5_SCAN_CASE_ALL_LIST_KEY3");

  // ZSet
  s = db.ZAdd("GP5_SCAN_CASE_ALL_ZSET_KEY1", {{1, "GP5_SCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP5_SCAN_CASE_ALL_ZSET_KEY2", {{1, "GP5_SCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP5_SCAN_CASE_ALL_ZSET_KEY3", {{1, "GP5_SCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.emplace_back("GP5_SCAN_CASE_ALL_ZSET_KEY1");
  delete_keys.emplace_back("GP5_SCAN_CASE_ALL_ZSET_KEY2");
  delete_keys.emplace_back("GP5_SCAN_CASE_ALL_ZSET_KEY3");

  cursor = 0;
  keys.clear();
  total_keys.clear();
  do {
    next_cursor = db.Scan(DataType::kAll, cursor, "*_SET_*", 5, &keys);
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  } while (cursor != 0);
  ASSERT_EQ(total_keys.size(), 3);
  ASSERT_EQ(total_keys[0], "GP5_SCAN_CASE_ALL_SET_KEY1");
  ASSERT_EQ(total_keys[1], "GP5_SCAN_CASE_ALL_SET_KEY2");
  ASSERT_EQ(total_keys[2], "GP5_SCAN_CASE_ALL_SET_KEY3");

  del_num = db.Del(delete_keys);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);

  // ***************** Group 6 Test *****************
  delete_keys.clear();
  // String
  s = db.Set("GP6_SCAN_CASE_ALL_STRING_KEY1", "GP6_SCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP6_SCAN_CASE_ALL_STRING_KEY2", "GP6_SCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP6_SCAN_CASE_ALL_STRING_KEY3", "GP6_SCAN_CASE_ALL_STRING_VALUE3");
  delete_keys.emplace_back("GP6_SCAN_CASE_ALL_STRING_KEY1");
  delete_keys.emplace_back("GP6_SCAN_CASE_ALL_STRING_KEY2");
  delete_keys.emplace_back("GP6_SCAN_CASE_ALL_STRING_KEY3");

  // Hash
  s = db.HSet("GP6_SCAN_CASE_ALL_HASH_KEY1", "GP6_SCAN_CASE_ALL_HASH_FIELD1", "GP6_SCAN_CASE_ALL_HASH_VALUE1",
              &int32_ret);
  s = db.HSet("GP6_SCAN_CASE_ALL_HASH_KEY2", "GP6_SCAN_CASE_ALL_HASH_FIELD2", "GP6_SCAN_CASE_ALL_HASH_VALUE2",
              &int32_ret);
  s = db.HSet("GP6_SCAN_CASE_ALL_HASH_KEY3", "GP6_SCAN_CASE_ALL_HASH_FIELD3", "GP6_SCAN_CASE_ALL_HASH_VALUE3",
              &int32_ret);
  delete_keys.emplace_back("GP6_SCAN_CASE_ALL_HASH_KEY1");
  delete_keys.emplace_back("GP6_SCAN_CASE_ALL_HASH_KEY2");
  delete_keys.emplace_back("GP6_SCAN_CASE_ALL_HASH_KEY3");

  // Set
  s = db.SAdd("GP6_SCAN_CASE_ALL_SET_KEY1", {"GP6_SCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP6_SCAN_CASE_ALL_SET_KEY2", {"GP6_SCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP6_SCAN_CASE_ALL_SET_KEY3", {"GP6_SCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);
  delete_keys.emplace_back("GP6_SCAN_CASE_ALL_SET_KEY1");
  delete_keys.emplace_back("GP6_SCAN_CASE_ALL_SET_KEY2");
  delete_keys.emplace_back("GP6_SCAN_CASE_ALL_SET_KEY3");

  // List
  s = db.LPush("GP6_SCAN_CASE_ALL_LIST_KEY1", {"GP6_SCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP6_SCAN_CASE_ALL_LIST_KEY2", {"GP6_SCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP6_SCAN_CASE_ALL_LIST_KEY3", {"GP6_SCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);
  delete_keys.emplace_back("GP6_SCAN_CASE_ALL_LIST_KEY1");
  delete_keys.emplace_back("GP6_SCAN_CASE_ALL_LIST_KEY2");
  delete_keys.emplace_back("GP6_SCAN_CASE_ALL_LIST_KEY3");

  // ZSet
  s = db.ZAdd("GP6_SCAN_CASE_ALL_ZSET_KEY1", {{1, "GP6_SCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP6_SCAN_CASE_ALL_ZSET_KEY2", {{1, "GP6_SCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP6_SCAN_CASE_ALL_ZSET_KEY3", {{1, "GP6_SCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.emplace_back("GP6_SCAN_CASE_ALL_ZSET_KEY1");
  delete_keys.emplace_back("GP6_SCAN_CASE_ALL_ZSET_KEY2");
  delete_keys.emplace_back("GP6_SCAN_CASE_ALL_ZSET_KEY3");

  cursor = 0;
  keys.clear();
  total_keys.clear();
  do {
    next_cursor = db.Scan(DataType::kAll, cursor, "*KEY1", 5, &keys);
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  } while (cursor != 0);
  ASSERT_EQ(total_keys.size(), 5);
  ASSERT_EQ(total_keys[0], "GP6_SCAN_CASE_ALL_STRING_KEY1");
  ASSERT_EQ(total_keys[1], "GP6_SCAN_CASE_ALL_HASH_KEY1");
  ASSERT_EQ(total_keys[2], "GP6_SCAN_CASE_ALL_SET_KEY1");
  ASSERT_EQ(total_keys[3], "GP6_SCAN_CASE_ALL_LIST_KEY1");
  ASSERT_EQ(total_keys[4], "GP6_SCAN_CASE_ALL_ZSET_KEY1");

  del_num = db.Del(delete_keys);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);

  // ***************** Group 7 Test *****************
  delete_keys.clear();
  // String
  s = db.Set("GP7_SCAN_CASE_ALL_STRING_KEY1", "GP7_SCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP7_SCAN_CASE_ALL_STRING_KEY2", "GP7_SCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP7_SCAN_CASE_ALL_STRING_KEY3", "GP7_SCAN_CASE_ALL_STRING_VALUE3");
  delete_keys.emplace_back("GP7_SCAN_CASE_ALL_STRING_KEY1");
  delete_keys.emplace_back("GP7_SCAN_CASE_ALL_STRING_KEY2");
  delete_keys.emplace_back("GP7_SCAN_CASE_ALL_STRING_KEY3");

  // Hash
  s = db.HSet("GP7_SCAN_CASE_ALL_HASH_KEY1", "GP7_SCAN_CASE_ALL_HASH_FIELD1", "GP7_SCAN_CASE_ALL_HASH_VALUE1",
              &int32_ret);
  s = db.HSet("GP7_SCAN_CASE_ALL_HASH_KEY2", "GP7_SCAN_CASE_ALL_HASH_FIELD2", "GP7_SCAN_CASE_ALL_HASH_VALUE2",
              &int32_ret);
  s = db.HSet("GP7_SCAN_CASE_ALL_HASH_KEY3", "GP7_SCAN_CASE_ALL_HASH_FIELD3", "GP7_SCAN_CASE_ALL_HASH_VALUE3",
              &int32_ret);
  delete_keys.emplace_back("GP7_SCAN_CASE_ALL_HASH_KEY1");
  delete_keys.emplace_back("GP7_SCAN_CASE_ALL_HASH_KEY2");
  delete_keys.emplace_back("GP7_SCAN_CASE_ALL_HASH_KEY3");

  // Set
  s = db.SAdd("GP7_SCAN_CASE_ALL_SET_KEY1", {"GP7_SCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP7_SCAN_CASE_ALL_SET_KEY2", {"GP7_SCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP7_SCAN_CASE_ALL_SET_KEY3", {"GP7_SCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);
  delete_keys.emplace_back("GP7_SCAN_CASE_ALL_SET_KEY1");
  delete_keys.emplace_back("GP7_SCAN_CASE_ALL_SET_KEY2");
  delete_keys.emplace_back("GP7_SCAN_CASE_ALL_SET_KEY3");

  // List
  s = db.LPush("GP7_SCAN_CASE_ALL_LIST_KEY1", {"GP7_SCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP7_SCAN_CASE_ALL_LIST_KEY2", {"GP7_SCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP7_SCAN_CASE_ALL_LIST_KEY3", {"GP7_SCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);
  delete_keys.emplace_back("GP7_SCAN_CASE_ALL_LIST_KEY1");
  delete_keys.emplace_back("GP7_SCAN_CASE_ALL_LIST_KEY2");
  delete_keys.emplace_back("GP7_SCAN_CASE_ALL_LIST_KEY3");

  // ZSet
  s = db.ZAdd("GP7_SCAN_CASE_ALL_ZSET_KEY1", {{1, "GP7_SCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP7_SCAN_CASE_ALL_ZSET_KEY2", {{1, "GP7_SCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP7_SCAN_CASE_ALL_ZSET_KEY3", {{1, "GP7_SCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.emplace_back("GP7_SCAN_CASE_ALL_ZSET_KEY1");
  delete_keys.emplace_back("GP7_SCAN_CASE_ALL_ZSET_KEY2");
  delete_keys.emplace_back("GP7_SCAN_CASE_ALL_ZSET_KEY3");

  cursor = 0;
  keys.clear();
  total_keys.clear();
  do {
    next_cursor = db.Scan(DataType::kAll, cursor, "*KEY2", 5, &keys);
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  } while (cursor != 0);
  ASSERT_EQ(total_keys.size(), 5);
  ASSERT_EQ(total_keys[0], "GP7_SCAN_CASE_ALL_STRING_KEY2");
  ASSERT_EQ(total_keys[1], "GP7_SCAN_CASE_ALL_HASH_KEY2");
  ASSERT_EQ(total_keys[2], "GP7_SCAN_CASE_ALL_SET_KEY2");
  ASSERT_EQ(total_keys[3], "GP7_SCAN_CASE_ALL_LIST_KEY2");
  ASSERT_EQ(total_keys[4], "GP7_SCAN_CASE_ALL_ZSET_KEY2");

  del_num = db.Del(delete_keys);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);

  // ***************** Group 8 Test *****************
  delete_keys.clear();
  // String
  s = db.Set("GP8_SCAN_CASE_ALL_STRING_KEY1", "GP8_SCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP8_SCAN_CASE_ALL_STRING_KEY2", "GP8_SCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP8_SCAN_CASE_ALL_STRING_KEY3", "GP8_SCAN_CASE_ALL_STRING_VALUE3");
  delete_keys.emplace_back("GP8_SCAN_CASE_ALL_STRING_KEY1");
  delete_keys.emplace_back("GP8_SCAN_CASE_ALL_STRING_KEY2");
  delete_keys.emplace_back("GP8_SCAN_CASE_ALL_STRING_KEY3");

  // Hash
  s = db.HSet("GP8_SCAN_CASE_ALL_HASH_KEY1", "GP8_SCAN_CASE_ALL_HASH_FIELD1", "GP8_SCAN_CASE_ALL_HASH_VALUE1",
              &int32_ret);
  s = db.HSet("GP8_SCAN_CASE_ALL_HASH_KEY2", "GP8_SCAN_CASE_ALL_HASH_FIELD2", "GP8_SCAN_CASE_ALL_HASH_VALUE2",
              &int32_ret);
  s = db.HSet("GP8_SCAN_CASE_ALL_HASH_KEY3", "GP8_SCAN_CASE_ALL_HASH_FIELD3", "GP8_SCAN_CASE_ALL_HASH_VALUE3",
              &int32_ret);
  delete_keys.emplace_back("GP8_SCAN_CASE_ALL_HASH_KEY1");
  delete_keys.emplace_back("GP8_SCAN_CASE_ALL_HASH_KEY2");
  delete_keys.emplace_back("GP8_SCAN_CASE_ALL_HASH_KEY3");

  // Set
  s = db.SAdd("GP8_SCAN_CASE_ALL_SET_KEY1", {"GP8_SCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP8_SCAN_CASE_ALL_SET_KEY2", {"GP8_SCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP8_SCAN_CASE_ALL_SET_KEY3", {"GP8_SCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);
  delete_keys.emplace_back("GP8_SCAN_CASE_ALL_SET_KEY1");
  delete_keys.emplace_back("GP8_SCAN_CASE_ALL_SET_KEY2");
  delete_keys.emplace_back("GP8_SCAN_CASE_ALL_SET_KEY3");

  // List
  s = db.LPush("GP8_SCAN_CASE_ALL_LIST_KEY1", {"GP8_SCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP8_SCAN_CASE_ALL_LIST_KEY2", {"GP8_SCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP8_SCAN_CASE_ALL_LIST_KEY3", {"GP8_SCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);
  delete_keys.emplace_back("GP8_SCAN_CASE_ALL_LIST_KEY1");
  delete_keys.emplace_back("GP8_SCAN_CASE_ALL_LIST_KEY2");
  delete_keys.emplace_back("GP8_SCAN_CASE_ALL_LIST_KEY3");

  // ZSet
  s = db.ZAdd("GP8_SCAN_CASE_ALL_ZSET_KEY1", {{1, "GP8_SCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP8_SCAN_CASE_ALL_ZSET_KEY2", {{1, "GP8_SCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP8_SCAN_CASE_ALL_ZSET_KEY3", {{1, "GP8_SCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.emplace_back("GP8_SCAN_CASE_ALL_ZSET_KEY1");
  delete_keys.emplace_back("GP8_SCAN_CASE_ALL_ZSET_KEY2");
  delete_keys.emplace_back("GP8_SCAN_CASE_ALL_ZSET_KEY3");

  cursor = 0;
  keys.clear();
  total_keys.clear();
  do {
    next_cursor = db.Scan(DataType::kAll, cursor, "*KEY3", 5, &keys);
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  } while (cursor != 0);
  ASSERT_EQ(total_keys.size(), 5);
  ASSERT_EQ(total_keys[0], "GP8_SCAN_CASE_ALL_STRING_KEY3");
  ASSERT_EQ(total_keys[1], "GP8_SCAN_CASE_ALL_HASH_KEY3");
  ASSERT_EQ(total_keys[2], "GP8_SCAN_CASE_ALL_SET_KEY3");
  ASSERT_EQ(total_keys[3], "GP8_SCAN_CASE_ALL_LIST_KEY3");
  ASSERT_EQ(total_keys[4], "GP8_SCAN_CASE_ALL_ZSET_KEY3");

  del_num = db.Del(delete_keys);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);

  // ***************** Group 9 Test *****************
  delete_keys.clear();
  // String
  s = db.Set("GP9_SCAN_CASE_ALL_STRING_KEY1", "GP9_SCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP9_SCAN_CASE_ALL_STRING_KEY2", "GP9_SCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP9_SCAN_CASE_ALL_STRING_KEY3", "GP9_SCAN_CASE_ALL_STRING_VALUE3");
  delete_keys.emplace_back("GP9_SCAN_CASE_ALL_STRING_KEY1");
  delete_keys.emplace_back("GP9_SCAN_CASE_ALL_STRING_KEY2");
  delete_keys.emplace_back("GP9_SCAN_CASE_ALL_STRING_KEY3");

  // Hash
  s = db.HSet("GP9_SCAN_CASE_ALL_HASH_KEY1", "GP9_SCAN_CASE_ALL_HASH_FIELD1", "GP9_SCAN_CASE_ALL_HASH_VALUE1",
              &int32_ret);
  s = db.HSet("GP9_SCAN_CASE_ALL_HASH_KEY2", "GP9_SCAN_CASE_ALL_HASH_FIELD2", "GP9_SCAN_CASE_ALL_HASH_VALUE2",
              &int32_ret);
  s = db.HSet("GP9_SCAN_CASE_ALL_HASH_KEY3", "GP9_SCAN_CASE_ALL_HASH_FIELD3", "GP9_SCAN_CASE_ALL_HASH_VALUE3",
              &int32_ret);
  delete_keys.emplace_back("GP9_SCAN_CASE_ALL_HASH_KEY1");
  delete_keys.emplace_back("GP9_SCAN_CASE_ALL_HASH_KEY2");
  delete_keys.emplace_back("GP9_SCAN_CASE_ALL_HASH_KEY3");

  // Set
  s = db.SAdd("GP9_SCAN_CASE_ALL_SET_KEY1", {"GP9_SCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP9_SCAN_CASE_ALL_SET_KEY2", {"GP9_SCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP9_SCAN_CASE_ALL_SET_KEY3", {"GP9_SCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);
  delete_keys.emplace_back("GP9_SCAN_CASE_ALL_SET_KEY1");
  delete_keys.emplace_back("GP9_SCAN_CASE_ALL_SET_KEY2");
  delete_keys.emplace_back("GP9_SCAN_CASE_ALL_SET_KEY3");

  // List
  s = db.LPush("GP9_SCAN_CASE_ALL_LIST_KEY1", {"GP9_SCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP9_SCAN_CASE_ALL_LIST_KEY2", {"GP9_SCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP9_SCAN_CASE_ALL_LIST_KEY3", {"GP9_SCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);
  delete_keys.emplace_back("GP9_SCAN_CASE_ALL_LIST_KEY1");
  delete_keys.emplace_back("GP9_SCAN_CASE_ALL_LIST_KEY2");
  delete_keys.emplace_back("GP9_SCAN_CASE_ALL_LIST_KEY3");

  // ZSet
  s = db.ZAdd("GP9_SCAN_CASE_ALL_ZSET_KEY1", {{1, "GP9_SCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP9_SCAN_CASE_ALL_ZSET_KEY2", {{1, "GP9_SCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP9_SCAN_CASE_ALL_ZSET_KEY3", {{1, "GP9_SCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.emplace_back("GP9_SCAN_CASE_ALL_ZSET_KEY1");
  delete_keys.emplace_back("GP9_SCAN_CASE_ALL_ZSET_KEY2");
  delete_keys.emplace_back("GP9_SCAN_CASE_ALL_ZSET_KEY3");

  cursor = 0;
  keys.clear();
  total_keys.clear();
  do {
    next_cursor = db.Scan(DataType::kAll, cursor, "GP9*", 5, &keys);
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  } while (cursor != 0);
  ASSERT_EQ(total_keys.size(), 15);
  ASSERT_EQ(total_keys[0], "GP9_SCAN_CASE_ALL_STRING_KEY1");
  ASSERT_EQ(total_keys[1], "GP9_SCAN_CASE_ALL_STRING_KEY2");
  ASSERT_EQ(total_keys[2], "GP9_SCAN_CASE_ALL_STRING_KEY3");
  ASSERT_EQ(total_keys[3], "GP9_SCAN_CASE_ALL_HASH_KEY1");
  ASSERT_EQ(total_keys[4], "GP9_SCAN_CASE_ALL_HASH_KEY2");
  ASSERT_EQ(total_keys[5], "GP9_SCAN_CASE_ALL_HASH_KEY3");
  ASSERT_EQ(total_keys[6], "GP9_SCAN_CASE_ALL_SET_KEY1");
  ASSERT_EQ(total_keys[7], "GP9_SCAN_CASE_ALL_SET_KEY2");
  ASSERT_EQ(total_keys[8], "GP9_SCAN_CASE_ALL_SET_KEY3");
  ASSERT_EQ(total_keys[9], "GP9_SCAN_CASE_ALL_LIST_KEY1");
  ASSERT_EQ(total_keys[10], "GP9_SCAN_CASE_ALL_LIST_KEY2");
  ASSERT_EQ(total_keys[11], "GP9_SCAN_CASE_ALL_LIST_KEY3");
  ASSERT_EQ(total_keys[12], "GP9_SCAN_CASE_ALL_ZSET_KEY1");
  ASSERT_EQ(total_keys[13], "GP9_SCAN_CASE_ALL_ZSET_KEY2");
  ASSERT_EQ(total_keys[14], "GP9_SCAN_CASE_ALL_ZSET_KEY3");

  del_num = db.Del(delete_keys);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);

  // ***************** Group 10 Test *****************
  delete_keys.clear();
  // String
  s = db.Set("GP10_SCAN_CASE_ALL_STRING_KEY1", "GP10_SCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP10_SCAN_CASE_ALL_STRING_KEY2", "GP10_SCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP10_SCAN_CASE_ALL_STRING_KEY3", "GP10_SCAN_CASE_ALL_STRING_VALUE3");
  delete_keys.emplace_back("GP10_SCAN_CASE_ALL_STRING_KEY1");
  delete_keys.emplace_back("GP10_SCAN_CASE_ALL_STRING_KEY2");
  delete_keys.emplace_back("GP10_SCAN_CASE_ALL_STRING_KEY3");

  // Hash
  s = db.HSet("GP10_SCAN_CASE_ALL_HASH_KEY1", "GP10_SCAN_CASE_ALL_HASH_FIELD1", "GP10_SCAN_CASE_ALL_HASH_VALUE1",
              &int32_ret);
  s = db.HSet("GP10_SCAN_CASE_ALL_HASH_KEY2", "GP10_SCAN_CASE_ALL_HASH_FIELD2", "GP10_SCAN_CASE_ALL_HASH_VALUE2",
              &int32_ret);
  s = db.HSet("GP10_SCAN_CASE_ALL_HASH_KEY3", "GP10_SCAN_CASE_ALL_HASH_FIELD3", "GP10_SCAN_CASE_ALL_HASH_VALUE3",
              &int32_ret);
  delete_keys.emplace_back("GP10_SCAN_CASE_ALL_HASH_KEY1");
  delete_keys.emplace_back("GP10_SCAN_CASE_ALL_HASH_KEY2");
  delete_keys.emplace_back("GP10_SCAN_CASE_ALL_HASH_KEY3");

  // Set
  s = db.SAdd("GP10_SCAN_CASE_ALL_SET_KEY1", {"GP10_SCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP10_SCAN_CASE_ALL_SET_KEY2", {"GP10_SCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP10_SCAN_CASE_ALL_SET_KEY3", {"GP10_SCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);
  delete_keys.emplace_back("GP10_SCAN_CASE_ALL_SET_KEY1");
  delete_keys.emplace_back("GP10_SCAN_CASE_ALL_SET_KEY2");
  delete_keys.emplace_back("GP10_SCAN_CASE_ALL_SET_KEY3");

  // List
  s = db.LPush("GP10_SCAN_CASE_ALL_LIST_KEY1", {"GP10_SCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP10_SCAN_CASE_ALL_LIST_KEY2", {"GP10_SCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP10_SCAN_CASE_ALL_LIST_KEY3", {"GP10_SCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);
  delete_keys.emplace_back("GP10_SCAN_CASE_ALL_LIST_KEY1");
  delete_keys.emplace_back("GP10_SCAN_CASE_ALL_LIST_KEY2");
  delete_keys.emplace_back("GP10_SCAN_CASE_ALL_LIST_KEY3");

  // ZSet
  s = db.ZAdd("GP10_SCAN_CASE_ALL_ZSET_KEY1", {{1, "GP10_SCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP10_SCAN_CASE_ALL_ZSET_KEY2", {{1, "GP10_SCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP10_SCAN_CASE_ALL_ZSET_KEY3", {{1, "GP10_SCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.emplace_back("GP10_SCAN_CASE_ALL_ZSET_KEY1");
  delete_keys.emplace_back("GP10_SCAN_CASE_ALL_ZSET_KEY2");
  delete_keys.emplace_back("GP10_SCAN_CASE_ALL_ZSET_KEY3");

  cursor = 0;
  keys.clear();
  total_keys.clear();
  do {
    next_cursor = db.Scan(DataType::kAll, cursor, "GP10_SCAN_CASE_ALL_STRING_*", 5, &keys);
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  } while (cursor != 0);
  ASSERT_EQ(total_keys.size(), 3);
  ASSERT_EQ(total_keys[0], "GP10_SCAN_CASE_ALL_STRING_KEY1");
  ASSERT_EQ(total_keys[1], "GP10_SCAN_CASE_ALL_STRING_KEY2");
  ASSERT_EQ(total_keys[2], "GP10_SCAN_CASE_ALL_STRING_KEY3");

  del_num = db.Del(delete_keys);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);

  // ***************** Group 11 Test *****************
  delete_keys.clear();
  // String
  s = db.Set("GP11_SCAN_CASE_ALL_STRING_KEY1", "GP11_SCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP11_SCAN_CASE_ALL_STRING_KEY2", "GP11_SCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP11_SCAN_CASE_ALL_STRING_KEY3", "GP11_SCAN_CASE_ALL_STRING_VALUE3");
  delete_keys.emplace_back("GP11_SCAN_CASE_ALL_STRING_KEY1");
  delete_keys.emplace_back("GP11_SCAN_CASE_ALL_STRING_KEY2");
  delete_keys.emplace_back("GP11_SCAN_CASE_ALL_STRING_KEY3");

  // Hash
  s = db.HSet("GP11_SCAN_CASE_ALL_HASH_KEY1", "GP11_SCAN_CASE_ALL_HASH_FIELD1", "GP11_SCAN_CASE_ALL_HASH_VALUE1",
              &int32_ret);
  s = db.HSet("GP11_SCAN_CASE_ALL_HASH_KEY2", "GP11_SCAN_CASE_ALL_HASH_FIELD2", "GP11_SCAN_CASE_ALL_HASH_VALUE2",
              &int32_ret);
  s = db.HSet("GP11_SCAN_CASE_ALL_HASH_KEY3", "GP11_SCAN_CASE_ALL_HASH_FIELD3", "GP11_SCAN_CASE_ALL_HASH_VALUE3",
              &int32_ret);
  delete_keys.emplace_back("GP11_SCAN_CASE_ALL_HASH_KEY1");
  delete_keys.emplace_back("GP11_SCAN_CASE_ALL_HASH_KEY2");
  delete_keys.emplace_back("GP11_SCAN_CASE_ALL_HASH_KEY3");

  // Set
  s = db.SAdd("GP11_SCAN_CASE_ALL_SET_KEY1", {"GP11_SCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP11_SCAN_CASE_ALL_SET_KEY2", {"GP11_SCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP11_SCAN_CASE_ALL_SET_KEY3", {"GP11_SCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);
  delete_keys.emplace_back("GP11_SCAN_CASE_ALL_SET_KEY1");
  delete_keys.emplace_back("GP11_SCAN_CASE_ALL_SET_KEY2");
  delete_keys.emplace_back("GP11_SCAN_CASE_ALL_SET_KEY3");

  // List
  s = db.LPush("GP11_SCAN_CASE_ALL_LIST_KEY1", {"GP11_SCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP11_SCAN_CASE_ALL_LIST_KEY2", {"GP11_SCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP11_SCAN_CASE_ALL_LIST_KEY3", {"GP11_SCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);
  delete_keys.emplace_back("GP11_SCAN_CASE_ALL_LIST_KEY1");
  delete_keys.emplace_back("GP11_SCAN_CASE_ALL_LIST_KEY2");
  delete_keys.emplace_back("GP11_SCAN_CASE_ALL_LIST_KEY3");

  // ZSet
  s = db.ZAdd("GP11_SCAN_CASE_ALL_ZSET_KEY1", {{1, "GP11_SCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP11_SCAN_CASE_ALL_ZSET_KEY2", {{1, "GP11_SCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP11_SCAN_CASE_ALL_ZSET_KEY3", {{1, "GP11_SCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.emplace_back("GP11_SCAN_CASE_ALL_ZSET_KEY1");
  delete_keys.emplace_back("GP11_SCAN_CASE_ALL_ZSET_KEY2");
  delete_keys.emplace_back("GP11_SCAN_CASE_ALL_ZSET_KEY3");

  cursor = 0;
  keys.clear();
  total_keys.clear();
  do {
    next_cursor = db.Scan(DataType::kAll, cursor, "GP11_SCAN_CASE_ALL_SET_*", 5, &keys);
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  } while (cursor != 0);
  ASSERT_EQ(total_keys.size(), 3);
  ASSERT_EQ(total_keys[0], "GP11_SCAN_CASE_ALL_SET_KEY1");
  ASSERT_EQ(total_keys[1], "GP11_SCAN_CASE_ALL_SET_KEY2");
  ASSERT_EQ(total_keys[2], "GP11_SCAN_CASE_ALL_SET_KEY3");

  del_num = db.Del(delete_keys);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);

  // ***************** Group 12 Test *****************
  delete_keys.clear();
  // String
  s = db.Set("GP12_SCAN_CASE_ALL_STRING_KEY1", "GP12_SCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP12_SCAN_CASE_ALL_STRING_KEY2", "GP12_SCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP12_SCAN_CASE_ALL_STRING_KEY3", "GP12_SCAN_CASE_ALL_STRING_VALUE3");
  delete_keys.emplace_back("GP12_SCAN_CASE_ALL_STRING_KEY1");
  delete_keys.emplace_back("GP12_SCAN_CASE_ALL_STRING_KEY2");
  delete_keys.emplace_back("GP12_SCAN_CASE_ALL_STRING_KEY3");

  // Hash
  s = db.HSet("GP12_SCAN_CASE_ALL_HASH_KEY1", "GP12_SCAN_CASE_ALL_HASH_FIELD1", "GP12_SCAN_CASE_ALL_HASH_VALUE1",
              &int32_ret);
  s = db.HSet("GP12_SCAN_CASE_ALL_HASH_KEY2", "GP12_SCAN_CASE_ALL_HASH_FIELD2", "GP12_SCAN_CASE_ALL_HASH_VALUE2",
              &int32_ret);
  s = db.HSet("GP12_SCAN_CASE_ALL_HASH_KEY3", "GP12_SCAN_CASE_ALL_HASH_FIELD3", "GP12_SCAN_CASE_ALL_HASH_VALUE3",
              &int32_ret);
  delete_keys.emplace_back("GP12_SCAN_CASE_ALL_HASH_KEY1");
  delete_keys.emplace_back("GP12_SCAN_CASE_ALL_HASH_KEY2");
  delete_keys.emplace_back("GP12_SCAN_CASE_ALL_HASH_KEY3");

  // Set
  s = db.SAdd("GP12_SCAN_CASE_ALL_SET_KEY1", {"GP12_SCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP12_SCAN_CASE_ALL_SET_KEY2", {"GP12_SCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP12_SCAN_CASE_ALL_SET_KEY3", {"GP12_SCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);
  delete_keys.emplace_back("GP12_SCAN_CASE_ALL_SET_KEY1");
  delete_keys.emplace_back("GP12_SCAN_CASE_ALL_SET_KEY2");
  delete_keys.emplace_back("GP12_SCAN_CASE_ALL_SET_KEY3");

  // List
  s = db.LPush("GP12_SCAN_CASE_ALL_LIST_KEY1", {"GP12_SCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP12_SCAN_CASE_ALL_LIST_KEY2", {"GP12_SCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP12_SCAN_CASE_ALL_LIST_KEY3", {"GP12_SCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);
  delete_keys.emplace_back("GP12_SCAN_CASE_ALL_LIST_KEY1");
  delete_keys.emplace_back("GP12_SCAN_CASE_ALL_LIST_KEY2");
  delete_keys.emplace_back("GP12_SCAN_CASE_ALL_LIST_KEY3");

  // ZSet
  s = db.ZAdd("GP12_SCAN_CASE_ALL_ZSET_KEY1", {{1, "GP12_SCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP12_SCAN_CASE_ALL_ZSET_KEY2", {{1, "GP12_SCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP12_SCAN_CASE_ALL_ZSET_KEY3", {{1, "GP12_SCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.emplace_back("GP12_SCAN_CASE_ALL_ZSET_KEY1");
  delete_keys.emplace_back("GP12_SCAN_CASE_ALL_ZSET_KEY2");
  delete_keys.emplace_back("GP12_SCAN_CASE_ALL_ZSET_KEY3");

  cursor = 0;
  keys.clear();
  total_keys.clear();
  do {
    next_cursor = db.Scan(DataType::kAll, cursor, "GP12_SCAN_CASE_ALL_ZSET_*", 5, &keys);
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  } while (cursor != 0);
  ASSERT_EQ(total_keys.size(), 3);
  ASSERT_EQ(total_keys[0], "GP12_SCAN_CASE_ALL_ZSET_KEY1");
  ASSERT_EQ(total_keys[1], "GP12_SCAN_CASE_ALL_ZSET_KEY2");
  ASSERT_EQ(total_keys[2], "GP12_SCAN_CASE_ALL_ZSET_KEY3");

  del_num = db.Del(delete_keys);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);

  // ***************** Group 13 Test *****************
  delete_keys.clear();
  // String
  s = db.Set("GP13_KEY1_SCAN_CASE_ALL_STRING", "GP13_SCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP13_KEY2_SCAN_CASE_ALL_STRING", "GP13_SCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP13_KEY3_SCAN_CASE_ALL_STRING", "GP13_SCAN_CASE_ALL_STRING_VALUE3");
  delete_keys.emplace_back("GP13_KEY1_SCAN_CASE_ALL_STRING");
  delete_keys.emplace_back("GP13_KEY2_SCAN_CASE_ALL_STRING");
  delete_keys.emplace_back("GP13_KEY3_SCAN_CASE_ALL_STRING");

  // Hash
  s = db.HSet("GP13_KEY1_SCAN_CASE_ALL_HASH", "GP13_SCAN_CASE_ALL_HASH_FIELD1", "GP13_SCAN_CASE_ALL_HASH_VALUE1",
              &int32_ret);
  s = db.HSet("GP13_KEY2_SCAN_CASE_ALL_HASH", "GP13_SCAN_CASE_ALL_HASH_FIELD2", "GP13_SCAN_CASE_ALL_HASH_VALUE2",
              &int32_ret);
  s = db.HSet("GP13_KEY3_SCAN_CASE_ALL_HASH", "GP13_SCAN_CASE_ALL_HASH_FIELD3", "GP13_SCAN_CASE_ALL_HASH_VALUE3",
              &int32_ret);
  delete_keys.emplace_back("GP13_KEY1_SCAN_CASE_ALL_HASH");
  delete_keys.emplace_back("GP13_KEY2_SCAN_CASE_ALL_HASH");
  delete_keys.emplace_back("GP13_KEY3_SCAN_CASE_ALL_HASH");

  // Set
  s = db.SAdd("GP13_KEY1_SCAN_CASE_ALL_SET", {"GP13_SCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP13_KEY2_SCAN_CASE_ALL_SET", {"GP13_SCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP13_KEY3_SCAN_CASE_ALL_SET", {"GP13_SCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);
  delete_keys.emplace_back("GP13_KEY1_SCAN_CASE_ALL_SET");
  delete_keys.emplace_back("GP13_KEY2_SCAN_CASE_ALL_SET");
  delete_keys.emplace_back("GP13_KEY3_SCAN_CASE_ALL_SET");

  // List
  s = db.LPush("GP13_KEY1_SCAN_CASE_ALL_LIST", {"GP13_SCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP13_KEY2_SCAN_CASE_ALL_LIST", {"GP13_SCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP13_KEY3_SCAN_CASE_ALL_LIST", {"GP13_SCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);
  delete_keys.emplace_back("GP13_KEY1_SCAN_CASE_ALL_LIST");
  delete_keys.emplace_back("GP13_KEY2_SCAN_CASE_ALL_LIST");
  delete_keys.emplace_back("GP13_KEY3_SCAN_CASE_ALL_LIST");

  // ZSet
  s = db.ZAdd("GP13_KEY1_SCAN_CASE_ALL_ZSET", {{1, "GP13_SCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP13_KEY2_SCAN_CASE_ALL_ZSET", {{1, "GP13_SCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP13_KEY3_SCAN_CASE_ALL_ZSET", {{1, "GP13_SCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.emplace_back("GP13_KEY1_SCAN_CASE_ALL_ZSET");
  delete_keys.emplace_back("GP13_KEY2_SCAN_CASE_ALL_ZSET");
  delete_keys.emplace_back("GP13_KEY3_SCAN_CASE_ALL_ZSET");

  cursor = 0;
  keys.clear();
  total_keys.clear();
  do {
    next_cursor = db.Scan(DataType::kAll, cursor, "GP13_KEY1_SCAN_CASE_ALL_*", 1, &keys);
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  } while (cursor != 0);
  ASSERT_EQ(total_keys.size(), 5);
  ASSERT_EQ(total_keys[0], "GP13_KEY1_SCAN_CASE_ALL_STRING");
  ASSERT_EQ(total_keys[1], "GP13_KEY1_SCAN_CASE_ALL_HASH");
  ASSERT_EQ(total_keys[2], "GP13_KEY1_SCAN_CASE_ALL_SET");
  ASSERT_EQ(total_keys[3], "GP13_KEY1_SCAN_CASE_ALL_LIST");
  ASSERT_EQ(total_keys[4], "GP13_KEY1_SCAN_CASE_ALL_ZSET");

  del_num = db.Del(delete_keys);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);

  // ***************** Group 14 Test *****************
  delete_keys.clear();
  // String
  s = db.Set("GP14_KEY1_SCAN_CASE_ALL_STRING", "GP14_SCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP14_KEY2_SCAN_CASE_ALL_STRING", "GP14_SCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP14_KEY3_SCAN_CASE_ALL_STRING", "GP14_SCAN_CASE_ALL_STRING_VALUE3");
  delete_keys.emplace_back("GP14_KEY1_SCAN_CASE_ALL_STRING");
  delete_keys.emplace_back("GP14_KEY2_SCAN_CASE_ALL_STRING");
  delete_keys.emplace_back("GP14_KEY3_SCAN_CASE_ALL_STRING");

  // Hash
  s = db.HSet("GP14_KEY1_SCAN_CASE_ALL_HASH", "GP14_SCAN_CASE_ALL_HASH_FIELD1", "GP14_SCAN_CASE_ALL_HASH_VALUE1",
              &int32_ret);
  s = db.HSet("GP14_KEY2_SCAN_CASE_ALL_HASH", "GP14_SCAN_CASE_ALL_HASH_FIELD2", "GP14_SCAN_CASE_ALL_HASH_VALUE2",
              &int32_ret);
  s = db.HSet("GP14_KEY3_SCAN_CASE_ALL_HASH", "GP14_SCAN_CASE_ALL_HASH_FIELD3", "GP14_SCAN_CASE_ALL_HASH_VALUE3",
              &int32_ret);
  delete_keys.emplace_back("GP14_KEY1_SCAN_CASE_ALL_HASH");
  delete_keys.emplace_back("GP14_KEY2_SCAN_CASE_ALL_HASH");
  delete_keys.emplace_back("GP14_KEY3_SCAN_CASE_ALL_HASH");

  // Set
  s = db.SAdd("GP14_KEY1_SCAN_CASE_ALL_SET", {"GP14_SCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP14_KEY2_SCAN_CASE_ALL_SET", {"GP14_SCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP14_KEY3_SCAN_CASE_ALL_SET", {"GP14_SCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);
  delete_keys.emplace_back("GP14_KEY1_SCAN_CASE_ALL_SET");
  delete_keys.emplace_back("GP14_KEY2_SCAN_CASE_ALL_SET");
  delete_keys.emplace_back("GP14_KEY3_SCAN_CASE_ALL_SET");

  // List
  s = db.LPush("GP14_KEY1_SCAN_CASE_ALL_LIST", {"GP14_SCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP14_KEY2_SCAN_CASE_ALL_LIST", {"GP14_SCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP14_KEY3_SCAN_CASE_ALL_LIST", {"GP14_SCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);
  delete_keys.emplace_back("GP14_KEY1_SCAN_CASE_ALL_LIST");
  delete_keys.emplace_back("GP14_KEY2_SCAN_CASE_ALL_LIST");
  delete_keys.emplace_back("GP14_KEY3_SCAN_CASE_ALL_LIST");

  // ZSet
  s = db.ZAdd("GP14_KEY1_SCAN_CASE_ALL_ZSET", {{1, "GP14_SCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP14_KEY2_SCAN_CASE_ALL_ZSET", {{1, "GP14_SCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP14_KEY3_SCAN_CASE_ALL_ZSET", {{1, "GP14_SCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.emplace_back("GP14_KEY1_SCAN_CASE_ALL_ZSET");
  delete_keys.emplace_back("GP14_KEY2_SCAN_CASE_ALL_ZSET");
  delete_keys.emplace_back("GP14_KEY3_SCAN_CASE_ALL_ZSET");

  cursor = 0;
  keys.clear();
  total_keys.clear();
  do {
    next_cursor = db.Scan(DataType::kAll, cursor, "GP14_KEY1_SCAN_CASE_ALL_*", 5, &keys);
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  } while (cursor != 0);
  ASSERT_EQ(total_keys.size(), 5);
  ASSERT_EQ(total_keys[0], "GP14_KEY1_SCAN_CASE_ALL_STRING");
  ASSERT_EQ(total_keys[1], "GP14_KEY1_SCAN_CASE_ALL_HASH");
  ASSERT_EQ(total_keys[2], "GP14_KEY1_SCAN_CASE_ALL_SET");
  ASSERT_EQ(total_keys[3], "GP14_KEY1_SCAN_CASE_ALL_LIST");
  ASSERT_EQ(total_keys[4], "GP14_KEY1_SCAN_CASE_ALL_ZSET");

  del_num = db.Del(delete_keys);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);

  // ***************** Group 15 Test *****************
  delete_keys.clear();
  // String
  s = db.Set("GP15_KEY1_SCAN_CASE_ALL_STRING", "GP15_SCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP15_KEY2_SCAN_CASE_ALL_STRING", "GP15_SCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP15_KEY3_SCAN_CASE_ALL_STRING", "GP15_SCAN_CASE_ALL_STRING_VALUE3");
  delete_keys.emplace_back("GP15_KEY1_SCAN_CASE_ALL_STRING");
  delete_keys.emplace_back("GP15_KEY2_SCAN_CASE_ALL_STRING");
  delete_keys.emplace_back("GP15_KEY3_SCAN_CASE_ALL_STRING");

  // Hash
  s = db.HSet("GP15_KEY1_SCAN_CASE_ALL_HASH", "GP15_SCAN_CASE_ALL_HASH_FIELD1", "GP15_SCAN_CASE_ALL_HASH_VALUE1",
              &int32_ret);
  s = db.HSet("GP15_KEY2_SCAN_CASE_ALL_HASH", "GP15_SCAN_CASE_ALL_HASH_FIELD2", "GP15_SCAN_CASE_ALL_HASH_VALUE2",
              &int32_ret);
  s = db.HSet("GP15_KEY3_SCAN_CASE_ALL_HASH", "GP15_SCAN_CASE_ALL_HASH_FIELD3", "GP15_SCAN_CASE_ALL_HASH_VALUE3",
              &int32_ret);
  delete_keys.emplace_back("GP15_KEY1_SCAN_CASE_ALL_HASH");
  delete_keys.emplace_back("GP15_KEY2_SCAN_CASE_ALL_HASH");
  delete_keys.emplace_back("GP15_KEY3_SCAN_CASE_ALL_HASH");

  // Set
  s = db.SAdd("GP15_KEY1_SCAN_CASE_ALL_SET", {"GP15_SCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP15_KEY2_SCAN_CASE_ALL_SET", {"GP15_SCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP15_KEY3_SCAN_CASE_ALL_SET", {"GP15_SCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);
  delete_keys.emplace_back("GP15_KEY1_SCAN_CASE_ALL_SET");
  delete_keys.emplace_back("GP15_KEY2_SCAN_CASE_ALL_SET");
  delete_keys.emplace_back("GP15_KEY3_SCAN_CASE_ALL_SET");

  // List
  s = db.LPush("GP15_KEY1_SCAN_CASE_ALL_LIST", {"GP15_SCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP15_KEY2_SCAN_CASE_ALL_LIST", {"GP15_SCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP15_KEY3_SCAN_CASE_ALL_LIST", {"GP15_SCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);
  delete_keys.emplace_back("GP15_KEY1_SCAN_CASE_ALL_LIST");
  delete_keys.emplace_back("GP15_KEY2_SCAN_CASE_ALL_LIST");
  delete_keys.emplace_back("GP15_KEY3_SCAN_CASE_ALL_LIST");

  // ZSet
  s = db.ZAdd("GP15_KEY1_SCAN_CASE_ALL_ZSET", {{1, "GP15_SCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP15_KEY2_SCAN_CASE_ALL_ZSET", {{1, "GP15_SCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP15_KEY3_SCAN_CASE_ALL_ZSET", {{1, "GP15_SCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.emplace_back("GP15_KEY1_SCAN_CASE_ALL_ZSET");
  delete_keys.emplace_back("GP15_KEY2_SCAN_CASE_ALL_ZSET");
  delete_keys.emplace_back("GP15_KEY3_SCAN_CASE_ALL_ZSET");

  cursor = 0;
  keys.clear();
  total_keys.clear();
  do {
    next_cursor = db.Scan(DataType::kAll, cursor, "GP15_KEY2_SCAN_CASE_ALL_*", 5, &keys);
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  } while (cursor != 0);
  ASSERT_EQ(total_keys.size(), 5);
  ASSERT_EQ(total_keys[0], "GP15_KEY2_SCAN_CASE_ALL_STRING");
  ASSERT_EQ(total_keys[1], "GP15_KEY2_SCAN_CASE_ALL_HASH");
  ASSERT_EQ(total_keys[2], "GP15_KEY2_SCAN_CASE_ALL_SET");
  ASSERT_EQ(total_keys[3], "GP15_KEY2_SCAN_CASE_ALL_LIST");
  ASSERT_EQ(total_keys[4], "GP15_KEY2_SCAN_CASE_ALL_ZSET");

  del_num = db.Del(delete_keys);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);

  // ***************** Group 16 Test *****************
  delete_keys.clear();
  // String
  s = db.Set("GP16_KEY1_SCAN_CASE_ALL_STRING", "GP16_SCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP16_KEY2_SCAN_CASE_ALL_STRING", "GP16_SCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP16_KEY3_SCAN_CASE_ALL_STRING", "GP16_SCAN_CASE_ALL_STRING_VALUE3");
  delete_keys.emplace_back("GP16_KEY1_SCAN_CASE_ALL_STRING");
  delete_keys.emplace_back("GP16_KEY2_SCAN_CASE_ALL_STRING");
  delete_keys.emplace_back("GP16_KEY3_SCAN_CASE_ALL_STRING");

  // Hash
  s = db.HSet("GP16_KEY1_SCAN_CASE_ALL_HASH", "GP16_SCAN_CASE_ALL_HASH_FIELD1", "GP16_SCAN_CASE_ALL_HASH_VALUE1",
              &int32_ret);
  s = db.HSet("GP16_KEY2_SCAN_CASE_ALL_HASH", "GP16_SCAN_CASE_ALL_HASH_FIELD2", "GP16_SCAN_CASE_ALL_HASH_VALUE2",
              &int32_ret);
  s = db.HSet("GP16_KEY3_SCAN_CASE_ALL_HASH", "GP16_SCAN_CASE_ALL_HASH_FIELD3", "GP16_SCAN_CASE_ALL_HASH_VALUE3",
              &int32_ret);
  delete_keys.emplace_back("GP16_KEY1_SCAN_CASE_ALL_HASH");
  delete_keys.emplace_back("GP16_KEY2_SCAN_CASE_ALL_HASH");
  delete_keys.emplace_back("GP16_KEY3_SCAN_CASE_ALL_HASH");

  // Set
  s = db.SAdd("GP16_KEY1_SCAN_CASE_ALL_SET", {"GP16_SCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP16_KEY2_SCAN_CASE_ALL_SET", {"GP16_SCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP16_KEY3_SCAN_CASE_ALL_SET", {"GP16_SCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);
  delete_keys.emplace_back("GP16_KEY1_SCAN_CASE_ALL_SET");
  delete_keys.emplace_back("GP16_KEY2_SCAN_CASE_ALL_SET");
  delete_keys.emplace_back("GP16_KEY3_SCAN_CASE_ALL_SET");

  // List
  s = db.LPush("GP16_KEY1_SCAN_CASE_ALL_LIST", {"GP16_SCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP16_KEY2_SCAN_CASE_ALL_LIST", {"GP16_SCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP16_KEY3_SCAN_CASE_ALL_LIST", {"GP16_SCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);
  delete_keys.emplace_back("GP16_KEY1_SCAN_CASE_ALL_LIST");
  delete_keys.emplace_back("GP16_KEY2_SCAN_CASE_ALL_LIST");
  delete_keys.emplace_back("GP16_KEY3_SCAN_CASE_ALL_LIST");

  // ZSet
  s = db.ZAdd("GP16_KEY1_SCAN_CASE_ALL_ZSET", {{1, "GP16_SCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP16_KEY2_SCAN_CASE_ALL_ZSET", {{1, "GP16_SCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP16_KEY3_SCAN_CASE_ALL_ZSET", {{1, "GP16_SCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.emplace_back("GP16_KEY1_SCAN_CASE_ALL_ZSET");
  delete_keys.emplace_back("GP16_KEY2_SCAN_CASE_ALL_ZSET");
  delete_keys.emplace_back("GP16_KEY3_SCAN_CASE_ALL_ZSET");

  cursor = 0;
  keys.clear();
  total_keys.clear();
  do {
    next_cursor = db.Scan(DataType::kAll, cursor, "GP16_KEY3_SCAN_CASE_ALL_*", 5, &keys);
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  } while (cursor != 0);
  ASSERT_EQ(total_keys.size(), 5);
  ASSERT_EQ(total_keys[0], "GP16_KEY3_SCAN_CASE_ALL_STRING");
  ASSERT_EQ(total_keys[1], "GP16_KEY3_SCAN_CASE_ALL_HASH");
  ASSERT_EQ(total_keys[2], "GP16_KEY3_SCAN_CASE_ALL_SET");
  ASSERT_EQ(total_keys[3], "GP16_KEY3_SCAN_CASE_ALL_LIST");
  ASSERT_EQ(total_keys[4], "GP16_KEY3_SCAN_CASE_ALL_ZSET");

  del_num = db.Del(delete_keys);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);
}

// Scan
// Note: This test needs to execute at first because all of the data is
// predetermined.
TEST_F(KeysTest, ScanCaseSingleTest) {  // NOLINT
  int64_t cursor;
  int64_t next_cursor;
  int64_t del_num;
  int32_t int32_ret;
  uint64_t uint64_ret;
  std::vector<std::string> keys;
  std::vector<std::string> total_keys;
  std::vector<std::string> delete_keys;
  std::map<storage::DataType, Status> type_status;

  // ***************** Group 1 Test *****************
  delete_keys.clear();
  // String
  s = db.Set("GP1_KEY1_SCAN_CASE_SINGLE_STRING", "GP1_SCAN_CASE_SINGLE_STRING_VALUE1");
  s = db.Set("GP1_KEY2_SCAN_CASE_SINGLE_STRING", "GP1_SCAN_CASE_SINGLE_STRING_VALUE2");
  s = db.Set("GP1_KEY3_SCAN_CASE_SINGLE_STRING", "GP1_SCAN_CASE_SINGLE_STRING_VALUE3");
  s = db.Set("GP1_KEY4_SCAN_CASE_SINGLE_STRING", "GP1_SCAN_CASE_SINGLE_STRING_VALUE4");
  s = db.Set("GP1_KEY5_SCAN_CASE_SINGLE_STRING", "GP1_SCAN_CASE_SINGLE_STRING_VALUE5");
  s = db.Set("GP1_KEY6_SCAN_CASE_SINGLE_STRING", "GP1_SCAN_CASE_SINGLE_STRING_VALUE6");
  delete_keys.emplace_back("GP1_KEY1_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP1_KEY2_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP1_KEY3_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP1_KEY4_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP1_KEY5_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP1_KEY6_SCAN_CASE_SINGLE_STRING");

  // Hash
  s = db.HSet("GP1_KEY1_SCAN_CASE_SINGLE_HASH", "GP1_SCAN_CASE_SINGLE_HASH_FIELD1", "GP1_SCAN_CASE_SINGLE_HASH_VALUE1",
              &int32_ret);
  s = db.HSet("GP1_KEY2_SCAN_CASE_SINGLE_HASH", "GP1_SCAN_CASE_SINGLE_HASH_FIELD2", "GP1_SCAN_CASE_SINGLE_HASH_VALUE2",
              &int32_ret);
  s = db.HSet("GP1_KEY3_SCAN_CASE_SINGLE_HASH", "GP1_SCAN_CASE_SINGLE_HASH_FIELD3", "GP1_SCAN_CASE_SINGLE_HASH_VALUE3",
              &int32_ret);
  s = db.HSet("GP1_KEY4_SCAN_CASE_SINGLE_HASH", "GP1_SCAN_CASE_SINGLE_HASH_FIELD4", "GP1_SCAN_CASE_SINGLE_HASH_VALUE4",
              &int32_ret);
  s = db.HSet("GP1_KEY5_SCAN_CASE_SINGLE_HASH", "GP1_SCAN_CASE_SINGLE_HASH_FIELD5", "GP1_SCAN_CASE_SINGLE_HASH_VALUE5",
              &int32_ret);
  s = db.HSet("GP1_KEY6_SCAN_CASE_SINGLE_HASH", "GP1_SCAN_CASE_SINGLE_HASH_FIELD6", "GP1_SCAN_CASE_SINGLE_HASH_VALUE6",
              &int32_ret);
  delete_keys.emplace_back("GP1_KEY1_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP1_KEY2_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP1_KEY3_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP1_KEY4_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP1_KEY5_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP1_KEY6_SCAN_CASE_SINGLE_HASH");

  // Set
  s = db.SAdd("GP1_KEY1_SCAN_CASE_SINGLE_SET", {"GP1_SCAN_CASE_SINGLE_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP1_KEY2_SCAN_CASE_SINGLE_SET", {"GP1_SCAN_CASE_SINGLE_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP1_KEY3_SCAN_CASE_SINGLE_SET", {"GP1_SCAN_CASE_SINGLE_SET_MEMBER3"}, &int32_ret);
  s = db.SAdd("GP1_KEY4_SCAN_CASE_SINGLE_SET", {"GP1_SCAN_CASE_SINGLE_SET_MEMBER4"}, &int32_ret);
  s = db.SAdd("GP1_KEY5_SCAN_CASE_SINGLE_SET", {"GP1_SCAN_CASE_SINGLE_SET_MEMBER5"}, &int32_ret);
  s = db.SAdd("GP1_KEY6_SCAN_CASE_SINGLE_SET", {"GP1_SCAN_CASE_SINGLE_SET_MEMBER6"}, &int32_ret);
  delete_keys.emplace_back("GP1_KEY1_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP1_KEY2_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP1_KEY3_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP1_KEY4_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP1_KEY5_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP1_KEY6_SCAN_CASE_SINGLE_SET");

  // List
  s = db.LPush("GP1_KEY1_SCAN_CASE_SINGLE_LIST", {"GP1_SCAN_CASE_SINGLE_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP1_KEY2_SCAN_CASE_SINGLE_LIST", {"GP1_SCAN_CASE_SINGLE_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP1_KEY3_SCAN_CASE_SINGLE_LIST", {"GP1_SCAN_CASE_SINGLE_LIST_NODE3"}, &uint64_ret);
  s = db.LPush("GP1_KEY4_SCAN_CASE_SINGLE_LIST", {"GP1_SCAN_CASE_SINGLE_LIST_NODE4"}, &uint64_ret);
  s = db.LPush("GP1_KEY5_SCAN_CASE_SINGLE_LIST", {"GP1_SCAN_CASE_SINGLE_LIST_NODE5"}, &uint64_ret);
  s = db.LPush("GP1_KEY6_SCAN_CASE_SINGLE_LIST", {"GP1_SCAN_CASE_SINGLE_LIST_NODE6"}, &uint64_ret);
  delete_keys.emplace_back("GP1_KEY1_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP1_KEY2_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP1_KEY3_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP1_KEY4_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP1_KEY5_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP1_KEY6_SCAN_CASE_SINGLE_LIST");

  // ZSet
  s = db.ZAdd("GP1_KEY1_SCAN_CASE_SINGLE_ZSET", {{1, "GP1_SCAN_CASE_SINGLE_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP1_KEY2_SCAN_CASE_SINGLE_ZSET", {{1, "GP1_SCAN_CASE_SINGLE_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP1_KEY3_SCAN_CASE_SINGLE_ZSET", {{1, "GP1_SCAN_CASE_SINGLE_LIST_MEMBER3"}}, &int32_ret);
  s = db.ZAdd("GP1_KEY4_SCAN_CASE_SINGLE_ZSET", {{1, "GP1_SCAN_CASE_SINGLE_LIST_MEMBER4"}}, &int32_ret);
  s = db.ZAdd("GP1_KEY5_SCAN_CASE_SINGLE_ZSET", {{1, "GP1_SCAN_CASE_SINGLE_LIST_MEMBER5"}}, &int32_ret);
  s = db.ZAdd("GP1_KEY6_SCAN_CASE_SINGLE_ZSET", {{1, "GP1_SCAN_CASE_SINGLE_LIST_MEMBER6"}}, &int32_ret);
  delete_keys.emplace_back("GP1_KEY1_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP1_KEY2_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP1_KEY3_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP1_KEY4_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP1_KEY5_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP1_KEY6_SCAN_CASE_SINGLE_ZSET");

  keys.clear();
  cursor = 0;
  cursor = db.Scan(DataType::kStrings, cursor, "*", 2, &keys);
  ASSERT_EQ(cursor, 2);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP1_KEY1_SCAN_CASE_SINGLE_STRING");
  ASSERT_EQ(keys[1], "GP1_KEY2_SCAN_CASE_SINGLE_STRING");

  keys.clear();
  cursor = db.Scan(DataType::kStrings, cursor, "*", 2, &keys);
  ASSERT_EQ(cursor, 4);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP1_KEY3_SCAN_CASE_SINGLE_STRING");
  ASSERT_EQ(keys[1], "GP1_KEY4_SCAN_CASE_SINGLE_STRING");

  keys.clear();
  cursor = db.Scan(DataType::kStrings, cursor, "*", 2, &keys);
  ASSERT_EQ(cursor, 0);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP1_KEY5_SCAN_CASE_SINGLE_STRING");
  ASSERT_EQ(keys[1], "GP1_KEY6_SCAN_CASE_SINGLE_STRING");

  del_num = db.Del(delete_keys);
  ASSERT_EQ(del_num, 30);
  sleep(2);
  db.Compact(DataType::kAll, true);

  // ***************** Group 2 Test *****************
  delete_keys.clear();
  // String
  s = db.Set("GP2_KEY1_SCAN_CASE_SINGLE_STRING", "GP2_SCAN_CASE_SINGLE_STRING_VALUE1");
  s = db.Set("GP2_KEY2_SCAN_CASE_SINGLE_STRING", "GP2_SCAN_CASE_SINGLE_STRING_VALUE2");
  s = db.Set("GP2_KEY3_SCAN_CASE_SINGLE_STRING", "GP2_SCAN_CASE_SINGLE_STRING_VALUE3");
  s = db.Set("GP2_KEY4_SCAN_CASE_SINGLE_STRING", "GP2_SCAN_CASE_SINGLE_STRING_VALUE4");
  s = db.Set("GP2_KEY5_SCAN_CASE_SINGLE_STRING", "GP2_SCAN_CASE_SINGLE_STRING_VALUE5");
  s = db.Set("GP2_KEY6_SCAN_CASE_SINGLE_STRING", "GP2_SCAN_CASE_SINGLE_STRING_VALUE6");
  delete_keys.emplace_back("GP2_KEY1_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP2_KEY2_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP2_KEY3_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP2_KEY4_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP2_KEY5_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP2_KEY6_SCAN_CASE_SINGLE_STRING");

  // Hash
  s = db.HSet("GP2_KEY1_SCAN_CASE_SINGLE_HASH", "GP2_SCAN_CASE_SINGLE_HASH_FIELD1", "GP2_SCAN_CASE_SINGLE_HASH_VALUE1",
              &int32_ret);
  s = db.HSet("GP2_KEY2_SCAN_CASE_SINGLE_HASH", "GP2_SCAN_CASE_SINGLE_HASH_FIELD2", "GP2_SCAN_CASE_SINGLE_HASH_VALUE2",
              &int32_ret);
  s = db.HSet("GP2_KEY3_SCAN_CASE_SINGLE_HASH", "GP2_SCAN_CASE_SINGLE_HASH_FIELD3", "GP2_SCAN_CASE_SINGLE_HASH_VALUE3",
              &int32_ret);
  s = db.HSet("GP2_KEY4_SCAN_CASE_SINGLE_HASH", "GP2_SCAN_CASE_SINGLE_HASH_FIELD4", "GP2_SCAN_CASE_SINGLE_HASH_VALUE4",
              &int32_ret);
  s = db.HSet("GP2_KEY5_SCAN_CASE_SINGLE_HASH", "GP2_SCAN_CASE_SINGLE_HASH_FIELD5", "GP2_SCAN_CASE_SINGLE_HASH_VALUE5",
              &int32_ret);
  s = db.HSet("GP2_KEY6_SCAN_CASE_SINGLE_HASH", "GP2_SCAN_CASE_SINGLE_HASH_FIELD6", "GP2_SCAN_CASE_SINGLE_HASH_VALUE6",
              &int32_ret);
  delete_keys.emplace_back("GP2_KEY1_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP2_KEY2_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP2_KEY3_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP2_KEY4_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP2_KEY5_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP2_KEY6_SCAN_CASE_SINGLE_HASH");

  // Set
  s = db.SAdd("GP2_KEY1_SCAN_CASE_SINGLE_SET", {"GP2_SCAN_CASE_SINGLE_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP2_KEY2_SCAN_CASE_SINGLE_SET", {"GP2_SCAN_CASE_SINGLE_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP2_KEY3_SCAN_CASE_SINGLE_SET", {"GP2_SCAN_CASE_SINGLE_SET_MEMBER3"}, &int32_ret);
  s = db.SAdd("GP2_KEY4_SCAN_CASE_SINGLE_SET", {"GP2_SCAN_CASE_SINGLE_SET_MEMBER4"}, &int32_ret);
  s = db.SAdd("GP2_KEY5_SCAN_CASE_SINGLE_SET", {"GP2_SCAN_CASE_SINGLE_SET_MEMBER5"}, &int32_ret);
  s = db.SAdd("GP2_KEY6_SCAN_CASE_SINGLE_SET", {"GP2_SCAN_CASE_SINGLE_SET_MEMBER6"}, &int32_ret);
  delete_keys.emplace_back("GP2_KEY1_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP2_KEY2_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP2_KEY3_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP2_KEY4_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP2_KEY5_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP2_KEY6_SCAN_CASE_SINGLE_SET");

  // List
  s = db.LPush("GP2_KEY1_SCAN_CASE_SINGLE_LIST", {"GP2_SCAN_CASE_SINGLE_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP2_KEY2_SCAN_CASE_SINGLE_LIST", {"GP2_SCAN_CASE_SINGLE_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP2_KEY3_SCAN_CASE_SINGLE_LIST", {"GP2_SCAN_CASE_SINGLE_LIST_NODE3"}, &uint64_ret);
  s = db.LPush("GP2_KEY4_SCAN_CASE_SINGLE_LIST", {"GP2_SCAN_CASE_SINGLE_LIST_NODE4"}, &uint64_ret);
  s = db.LPush("GP2_KEY5_SCAN_CASE_SINGLE_LIST", {"GP2_SCAN_CASE_SINGLE_LIST_NODE5"}, &uint64_ret);
  s = db.LPush("GP2_KEY6_SCAN_CASE_SINGLE_LIST", {"GP2_SCAN_CASE_SINGLE_LIST_NODE6"}, &uint64_ret);
  delete_keys.emplace_back("GP2_KEY1_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP2_KEY2_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP2_KEY3_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP2_KEY4_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP2_KEY5_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP2_KEY6_SCAN_CASE_SINGLE_LIST");

  // ZSet
  s = db.ZAdd("GP2_KEY1_SCAN_CASE_SINGLE_ZSET", {{1, "GP2_SCAN_CASE_SINGLE_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP2_KEY2_SCAN_CASE_SINGLE_ZSET", {{1, "GP2_SCAN_CASE_SINGLE_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP2_KEY3_SCAN_CASE_SINGLE_ZSET", {{1, "GP2_SCAN_CASE_SINGLE_LIST_MEMBER3"}}, &int32_ret);
  s = db.ZAdd("GP2_KEY4_SCAN_CASE_SINGLE_ZSET", {{1, "GP2_SCAN_CASE_SINGLE_LIST_MEMBER4"}}, &int32_ret);
  s = db.ZAdd("GP2_KEY5_SCAN_CASE_SINGLE_ZSET", {{1, "GP2_SCAN_CASE_SINGLE_LIST_MEMBER5"}}, &int32_ret);
  s = db.ZAdd("GP2_KEY6_SCAN_CASE_SINGLE_ZSET", {{1, "GP2_SCAN_CASE_SINGLE_LIST_MEMBER6"}}, &int32_ret);
  delete_keys.emplace_back("GP2_KEY1_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP2_KEY2_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP2_KEY3_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP2_KEY4_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP2_KEY5_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP2_KEY6_SCAN_CASE_SINGLE_ZSET");

  keys.clear();
  cursor = 0;
  cursor = db.Scan(DataType::kStrings, cursor, "*", 4, &keys);
  ASSERT_EQ(cursor, 4);
  ASSERT_EQ(keys.size(), 4);
  ASSERT_EQ(keys[0], "GP2_KEY1_SCAN_CASE_SINGLE_STRING");
  ASSERT_EQ(keys[1], "GP2_KEY2_SCAN_CASE_SINGLE_STRING");
  ASSERT_EQ(keys[2], "GP2_KEY3_SCAN_CASE_SINGLE_STRING");
  ASSERT_EQ(keys[3], "GP2_KEY4_SCAN_CASE_SINGLE_STRING");

  keys.clear();
  cursor = db.Scan(DataType::kStrings, cursor, "*", 4, &keys);
  ASSERT_EQ(cursor, 0);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP2_KEY5_SCAN_CASE_SINGLE_STRING");
  ASSERT_EQ(keys[1], "GP2_KEY6_SCAN_CASE_SINGLE_STRING");

  del_num = db.Del(delete_keys);
  ASSERT_EQ(del_num, 30);
  sleep(2);
  db.Compact(DataType::kAll, true);

  // ***************** Group 3 Test *****************
  delete_keys.clear();
  // String
  s = db.Set("GP3_KEY1_SCAN_CASE_SINGLE_STRING", "GP3_SCAN_CASE_SINGLE_STRING_VALUE1");
  s = db.Set("GP3_KEY2_SCAN_CASE_SINGLE_STRING", "GP3_SCAN_CASE_SINGLE_STRING_VALUE2");
  s = db.Set("GP3_KEY3_SCAN_CASE_SINGLE_STRING", "GP3_SCAN_CASE_SINGLE_STRING_VALUE3");
  s = db.Set("GP3_KEY4_SCAN_CASE_SINGLE_STRING", "GP3_SCAN_CASE_SINGLE_STRING_VALUE4");
  s = db.Set("GP3_KEY5_SCAN_CASE_SINGLE_STRING", "GP3_SCAN_CASE_SINGLE_STRING_VALUE5");
  s = db.Set("GP3_KEY6_SCAN_CASE_SINGLE_STRING", "GP3_SCAN_CASE_SINGLE_STRING_VALUE6");
  delete_keys.emplace_back("GP3_KEY1_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP3_KEY2_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP3_KEY3_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP3_KEY4_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP3_KEY5_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP3_KEY6_SCAN_CASE_SINGLE_STRING");

  // Hash
  s = db.HSet("GP3_KEY1_SCAN_CASE_SINGLE_HASH", "GP3_SCAN_CASE_SINGLE_HASH_FIELD1", "GP3_SCAN_CASE_SINGLE_HASH_VALUE1",
              &int32_ret);
  s = db.HSet("GP3_KEY2_SCAN_CASE_SINGLE_HASH", "GP3_SCAN_CASE_SINGLE_HASH_FIELD2", "GP3_SCAN_CASE_SINGLE_HASH_VALUE2",
              &int32_ret);
  s = db.HSet("GP3_KEY3_SCAN_CASE_SINGLE_HASH", "GP3_SCAN_CASE_SINGLE_HASH_FIELD3", "GP3_SCAN_CASE_SINGLE_HASH_VALUE3",
              &int32_ret);
  s = db.HSet("GP3_KEY4_SCAN_CASE_SINGLE_HASH", "GP3_SCAN_CASE_SINGLE_HASH_FIELD4", "GP3_SCAN_CASE_SINGLE_HASH_VALUE4",
              &int32_ret);
  s = db.HSet("GP3_KEY5_SCAN_CASE_SINGLE_HASH", "GP3_SCAN_CASE_SINGLE_HASH_FIELD5", "GP3_SCAN_CASE_SINGLE_HASH_VALUE5",
              &int32_ret);
  s = db.HSet("GP3_KEY6_SCAN_CASE_SINGLE_HASH", "GP3_SCAN_CASE_SINGLE_HASH_FIELD6", "GP3_SCAN_CASE_SINGLE_HASH_VALUE6",
              &int32_ret);
  delete_keys.emplace_back("GP3_KEY1_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP3_KEY2_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP3_KEY3_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP3_KEY4_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP3_KEY5_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP3_KEY6_SCAN_CASE_SINGLE_HASH");

  // Set
  s = db.SAdd("GP3_KEY1_SCAN_CASE_SINGLE_SET", {"GP3_SCAN_CASE_SINGLE_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP3_KEY2_SCAN_CASE_SINGLE_SET", {"GP3_SCAN_CASE_SINGLE_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP3_KEY3_SCAN_CASE_SINGLE_SET", {"GP3_SCAN_CASE_SINGLE_SET_MEMBER3"}, &int32_ret);
  s = db.SAdd("GP3_KEY4_SCAN_CASE_SINGLE_SET", {"GP3_SCAN_CASE_SINGLE_SET_MEMBER4"}, &int32_ret);
  s = db.SAdd("GP3_KEY5_SCAN_CASE_SINGLE_SET", {"GP3_SCAN_CASE_SINGLE_SET_MEMBER5"}, &int32_ret);
  s = db.SAdd("GP3_KEY6_SCAN_CASE_SINGLE_SET", {"GP3_SCAN_CASE_SINGLE_SET_MEMBER6"}, &int32_ret);
  delete_keys.emplace_back("GP3_KEY1_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP3_KEY2_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP3_KEY3_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP3_KEY4_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP3_KEY5_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP3_KEY6_SCAN_CASE_SINGLE_SET");

  // List
  s = db.LPush("GP3_KEY1_SCAN_CASE_SINGLE_LIST", {"GP3_SCAN_CASE_SINGLE_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP3_KEY2_SCAN_CASE_SINGLE_LIST", {"GP3_SCAN_CASE_SINGLE_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP3_KEY3_SCAN_CASE_SINGLE_LIST", {"GP3_SCAN_CASE_SINGLE_LIST_NODE3"}, &uint64_ret);
  s = db.LPush("GP3_KEY4_SCAN_CASE_SINGLE_LIST", {"GP3_SCAN_CASE_SINGLE_LIST_NODE4"}, &uint64_ret);
  s = db.LPush("GP3_KEY5_SCAN_CASE_SINGLE_LIST", {"GP3_SCAN_CASE_SINGLE_LIST_NODE5"}, &uint64_ret);
  s = db.LPush("GP3_KEY6_SCAN_CASE_SINGLE_LIST", {"GP3_SCAN_CASE_SINGLE_LIST_NODE6"}, &uint64_ret);
  delete_keys.emplace_back("GP3_KEY1_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP3_KEY2_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP3_KEY3_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP3_KEY4_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP3_KEY5_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP3_KEY6_SCAN_CASE_SINGLE_LIST");

  // ZSet
  s = db.ZAdd("GP3_KEY1_SCAN_CASE_SINGLE_ZSET", {{1, "GP3_SCAN_CASE_SINGLE_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP3_KEY2_SCAN_CASE_SINGLE_ZSET", {{1, "GP3_SCAN_CASE_SINGLE_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP3_KEY3_SCAN_CASE_SINGLE_ZSET", {{1, "GP3_SCAN_CASE_SINGLE_LIST_MEMBER3"}}, &int32_ret);
  s = db.ZAdd("GP3_KEY4_SCAN_CASE_SINGLE_ZSET", {{1, "GP3_SCAN_CASE_SINGLE_LIST_MEMBER4"}}, &int32_ret);
  s = db.ZAdd("GP3_KEY5_SCAN_CASE_SINGLE_ZSET", {{1, "GP3_SCAN_CASE_SINGLE_LIST_MEMBER5"}}, &int32_ret);
  s = db.ZAdd("GP3_KEY6_SCAN_CASE_SINGLE_ZSET", {{1, "GP3_SCAN_CASE_SINGLE_LIST_MEMBER6"}}, &int32_ret);
  delete_keys.emplace_back("GP3_KEY1_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP3_KEY2_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP3_KEY3_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP3_KEY4_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP3_KEY5_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP3_KEY6_SCAN_CASE_SINGLE_ZSET");

  keys.clear();
  cursor = 0;
  cursor = db.Scan(DataType::kStrings, cursor, "*", 6, &keys);
  ASSERT_EQ(cursor, 0);
  ASSERT_EQ(keys.size(), 6);
  ASSERT_EQ(keys[0], "GP3_KEY1_SCAN_CASE_SINGLE_STRING");
  ASSERT_EQ(keys[1], "GP3_KEY2_SCAN_CASE_SINGLE_STRING");
  ASSERT_EQ(keys[2], "GP3_KEY3_SCAN_CASE_SINGLE_STRING");
  ASSERT_EQ(keys[3], "GP3_KEY4_SCAN_CASE_SINGLE_STRING");
  ASSERT_EQ(keys[4], "GP3_KEY5_SCAN_CASE_SINGLE_STRING");
  ASSERT_EQ(keys[5], "GP3_KEY6_SCAN_CASE_SINGLE_STRING");

  del_num = db.Del(delete_keys);
  ASSERT_EQ(del_num, 30);
  sleep(2);
  db.Compact(DataType::kAll, true);

  // ***************** Group 4 Test *****************
  delete_keys.clear();
  // String
  s = db.Set("GP4_KEY1_SCAN_CASE_SINGLE_STRING", "GP4_SCAN_CASE_SINGLE_STRING_VALUE1");
  s = db.Set("GP4_KEY2_SCAN_CASE_SINGLE_STRING", "GP4_SCAN_CASE_SINGLE_STRING_VALUE2");
  s = db.Set("GP4_KEY3_SCAN_CASE_SINGLE_STRING", "GP4_SCAN_CASE_SINGLE_STRING_VALUE3");
  s = db.Set("GP4_KEY4_SCAN_CASE_SINGLE_STRING", "GP4_SCAN_CASE_SINGLE_STRING_VALUE4");
  s = db.Set("GP4_KEY5_SCAN_CASE_SINGLE_STRING", "GP4_SCAN_CASE_SINGLE_STRING_VALUE5");
  s = db.Set("GP4_KEY6_SCAN_CASE_SINGLE_STRING", "GP4_SCAN_CASE_SINGLE_STRING_VALUE6");
  delete_keys.emplace_back("GP4_KEY1_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP4_KEY2_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP4_KEY3_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP4_KEY4_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP4_KEY5_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP4_KEY6_SCAN_CASE_SINGLE_STRING");

  // Hash
  s = db.HSet("GP4_KEY1_SCAN_CASE_SINGLE_HASH", "GP4_SCAN_CASE_SINGLE_HASH_FIELD1", "GP4_SCAN_CASE_SINGLE_HASH_VALUE1",
              &int32_ret);
  s = db.HSet("GP4_KEY2_SCAN_CASE_SINGLE_HASH", "GP4_SCAN_CASE_SINGLE_HASH_FIELD2", "GP4_SCAN_CASE_SINGLE_HASH_VALUE2",
              &int32_ret);
  s = db.HSet("GP4_KEY3_SCAN_CASE_SINGLE_HASH", "GP4_SCAN_CASE_SINGLE_HASH_FIELD3", "GP4_SCAN_CASE_SINGLE_HASH_VALUE3",
              &int32_ret);
  s = db.HSet("GP4_KEY4_SCAN_CASE_SINGLE_HASH", "GP4_SCAN_CASE_SINGLE_HASH_FIELD4", "GP4_SCAN_CASE_SINGLE_HASH_VALUE4",
              &int32_ret);
  s = db.HSet("GP4_KEY5_SCAN_CASE_SINGLE_HASH", "GP4_SCAN_CASE_SINGLE_HASH_FIELD5", "GP4_SCAN_CASE_SINGLE_HASH_VALUE5",
              &int32_ret);
  s = db.HSet("GP4_KEY6_SCAN_CASE_SINGLE_HASH", "GP4_SCAN_CASE_SINGLE_HASH_FIELD6", "GP4_SCAN_CASE_SINGLE_HASH_VALUE6",
              &int32_ret);
  delete_keys.emplace_back("GP4_KEY1_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP4_KEY2_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP4_KEY3_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP4_KEY4_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP4_KEY5_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP4_KEY6_SCAN_CASE_SINGLE_HASH");

  // Set
  s = db.SAdd("GP4_KEY1_SCAN_CASE_SINGLE_SET", {"GP4_SCAN_CASE_SINGLE_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP4_KEY2_SCAN_CASE_SINGLE_SET", {"GP4_SCAN_CASE_SINGLE_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP4_KEY3_SCAN_CASE_SINGLE_SET", {"GP4_SCAN_CASE_SINGLE_SET_MEMBER3"}, &int32_ret);
  s = db.SAdd("GP4_KEY4_SCAN_CASE_SINGLE_SET", {"GP4_SCAN_CASE_SINGLE_SET_MEMBER4"}, &int32_ret);
  s = db.SAdd("GP4_KEY5_SCAN_CASE_SINGLE_SET", {"GP4_SCAN_CASE_SINGLE_SET_MEMBER5"}, &int32_ret);
  s = db.SAdd("GP4_KEY6_SCAN_CASE_SINGLE_SET", {"GP4_SCAN_CASE_SINGLE_SET_MEMBER6"}, &int32_ret);
  delete_keys.emplace_back("GP4_KEY1_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP4_KEY2_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP4_KEY3_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP4_KEY4_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP4_KEY5_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP4_KEY6_SCAN_CASE_SINGLE_SET");

  // List
  s = db.LPush("GP4_KEY1_SCAN_CASE_SINGLE_LIST", {"GP4_SCAN_CASE_SINGLE_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP4_KEY2_SCAN_CASE_SINGLE_LIST", {"GP4_SCAN_CASE_SINGLE_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP4_KEY3_SCAN_CASE_SINGLE_LIST", {"GP4_SCAN_CASE_SINGLE_LIST_NODE3"}, &uint64_ret);
  s = db.LPush("GP4_KEY4_SCAN_CASE_SINGLE_LIST", {"GP4_SCAN_CASE_SINGLE_LIST_NODE4"}, &uint64_ret);
  s = db.LPush("GP4_KEY5_SCAN_CASE_SINGLE_LIST", {"GP4_SCAN_CASE_SINGLE_LIST_NODE5"}, &uint64_ret);
  s = db.LPush("GP4_KEY6_SCAN_CASE_SINGLE_LIST", {"GP4_SCAN_CASE_SINGLE_LIST_NODE6"}, &uint64_ret);
  delete_keys.emplace_back("GP4_KEY1_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP4_KEY2_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP4_KEY3_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP4_KEY4_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP4_KEY5_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP4_KEY6_SCAN_CASE_SINGLE_LIST");

  // ZSet
  s = db.ZAdd("GP4_KEY1_SCAN_CASE_SINGLE_ZSET", {{1, "GP4_SCAN_CASE_SINGLE_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP4_KEY2_SCAN_CASE_SINGLE_ZSET", {{1, "GP4_SCAN_CASE_SINGLE_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP4_KEY3_SCAN_CASE_SINGLE_ZSET", {{1, "GP4_SCAN_CASE_SINGLE_LIST_MEMBER3"}}, &int32_ret);
  s = db.ZAdd("GP4_KEY4_SCAN_CASE_SINGLE_ZSET", {{1, "GP4_SCAN_CASE_SINGLE_LIST_MEMBER4"}}, &int32_ret);
  s = db.ZAdd("GP4_KEY5_SCAN_CASE_SINGLE_ZSET", {{1, "GP4_SCAN_CASE_SINGLE_LIST_MEMBER5"}}, &int32_ret);
  s = db.ZAdd("GP4_KEY6_SCAN_CASE_SINGLE_ZSET", {{1, "GP4_SCAN_CASE_SINGLE_LIST_MEMBER6"}}, &int32_ret);
  delete_keys.emplace_back("GP4_KEY1_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP4_KEY2_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP4_KEY3_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP4_KEY4_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP4_KEY5_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP4_KEY6_SCAN_CASE_SINGLE_ZSET");

  keys.clear();
  cursor = 0;
  cursor = db.Scan(DataType::kStrings, cursor, "*", 10, &keys);
  ASSERT_EQ(cursor, 0);
  ASSERT_EQ(keys.size(), 6);
  ASSERT_EQ(keys[0], "GP4_KEY1_SCAN_CASE_SINGLE_STRING");
  ASSERT_EQ(keys[1], "GP4_KEY2_SCAN_CASE_SINGLE_STRING");
  ASSERT_EQ(keys[2], "GP4_KEY3_SCAN_CASE_SINGLE_STRING");
  ASSERT_EQ(keys[3], "GP4_KEY4_SCAN_CASE_SINGLE_STRING");
  ASSERT_EQ(keys[4], "GP4_KEY5_SCAN_CASE_SINGLE_STRING");
  ASSERT_EQ(keys[5], "GP4_KEY6_SCAN_CASE_SINGLE_STRING");

  del_num = db.Del(delete_keys);
  ASSERT_EQ(del_num, 30);
  sleep(2);
  db.Compact(DataType::kAll, true);

  // ***************** Group 5 Test *****************
  delete_keys.clear();
  // String
  s = db.Set("GP5_KEY1_SCAN_CASE_SINGLE_STRING", "GP5_SCAN_CASE_SINGLE_STRING_VALUE1");
  s = db.Set("GP5_KEY2_SCAN_CASE_SINGLE_STRING", "GP5_SCAN_CASE_SINGLE_STRING_VALUE2");
  s = db.Set("GP5_KEY3_SCAN_CASE_SINGLE_STRING", "GP5_SCAN_CASE_SINGLE_STRING_VALUE3");
  s = db.Set("GP5_KEY4_SCAN_CASE_SINGLE_STRING", "GP5_SCAN_CASE_SINGLE_STRING_VALUE4");
  s = db.Set("GP5_KEY5_SCAN_CASE_SINGLE_STRING", "GP5_SCAN_CASE_SINGLE_STRING_VALUE5");
  s = db.Set("GP5_KEY6_SCAN_CASE_SINGLE_STRING", "GP5_SCAN_CASE_SINGLE_STRING_VALUE6");
  delete_keys.emplace_back("GP5_KEY1_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP5_KEY2_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP5_KEY3_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP5_KEY4_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP5_KEY5_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP5_KEY6_SCAN_CASE_SINGLE_STRING");

  // Hash
  s = db.HSet("GP5_KEY1_SCAN_CASE_SINGLE_HASH", "GP5_SCAN_CASE_SINGLE_HASH_FIELD1", "GP5_SCAN_CASE_SINGLE_HASH_VALUE1",
              &int32_ret);
  s = db.HSet("GP5_KEY2_SCAN_CASE_SINGLE_HASH", "GP5_SCAN_CASE_SINGLE_HASH_FIELD2", "GP5_SCAN_CASE_SINGLE_HASH_VALUE2",
              &int32_ret);
  s = db.HSet("GP5_KEY3_SCAN_CASE_SINGLE_HASH", "GP5_SCAN_CASE_SINGLE_HASH_FIELD3", "GP5_SCAN_CASE_SINGLE_HASH_VALUE3",
              &int32_ret);
  s = db.HSet("GP5_KEY4_SCAN_CASE_SINGLE_HASH", "GP5_SCAN_CASE_SINGLE_HASH_FIELD4", "GP5_SCAN_CASE_SINGLE_HASH_VALUE4",
              &int32_ret);
  s = db.HSet("GP5_KEY5_SCAN_CASE_SINGLE_HASH", "GP5_SCAN_CASE_SINGLE_HASH_FIELD5", "GP5_SCAN_CASE_SINGLE_HASH_VALUE5",
              &int32_ret);
  s = db.HSet("GP5_KEY6_SCAN_CASE_SINGLE_HASH", "GP5_SCAN_CASE_SINGLE_HASH_FIELD6", "GP5_SCAN_CASE_SINGLE_HASH_VALUE6",
              &int32_ret);
  delete_keys.emplace_back("GP5_KEY1_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP5_KEY2_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP5_KEY3_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP5_KEY4_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP5_KEY5_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP5_KEY6_SCAN_CASE_SINGLE_HASH");

  // Set
  s = db.SAdd("GP5_KEY1_SCAN_CASE_SINGLE_SET", {"GP5_SCAN_CASE_SINGLE_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP5_KEY2_SCAN_CASE_SINGLE_SET", {"GP5_SCAN_CASE_SINGLE_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP5_KEY3_SCAN_CASE_SINGLE_SET", {"GP5_SCAN_CASE_SINGLE_SET_MEMBER3"}, &int32_ret);
  s = db.SAdd("GP5_KEY4_SCAN_CASE_SINGLE_SET", {"GP5_SCAN_CASE_SINGLE_SET_MEMBER4"}, &int32_ret);
  s = db.SAdd("GP5_KEY5_SCAN_CASE_SINGLE_SET", {"GP5_SCAN_CASE_SINGLE_SET_MEMBER5"}, &int32_ret);
  s = db.SAdd("GP5_KEY6_SCAN_CASE_SINGLE_SET", {"GP5_SCAN_CASE_SINGLE_SET_MEMBER6"}, &int32_ret);
  delete_keys.emplace_back("GP5_KEY1_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP5_KEY2_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP5_KEY3_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP5_KEY4_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP5_KEY5_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP5_KEY6_SCAN_CASE_SINGLE_SET");

  // List
  s = db.LPush("GP5_KEY1_SCAN_CASE_SINGLE_LIST", {"GP5_SCAN_CASE_SINGLE_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP5_KEY2_SCAN_CASE_SINGLE_LIST", {"GP5_SCAN_CASE_SINGLE_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP5_KEY3_SCAN_CASE_SINGLE_LIST", {"GP5_SCAN_CASE_SINGLE_LIST_NODE3"}, &uint64_ret);
  s = db.LPush("GP5_KEY4_SCAN_CASE_SINGLE_LIST", {"GP5_SCAN_CASE_SINGLE_LIST_NODE4"}, &uint64_ret);
  s = db.LPush("GP5_KEY5_SCAN_CASE_SINGLE_LIST", {"GP5_SCAN_CASE_SINGLE_LIST_NODE5"}, &uint64_ret);
  s = db.LPush("GP5_KEY6_SCAN_CASE_SINGLE_LIST", {"GP5_SCAN_CASE_SINGLE_LIST_NODE6"}, &uint64_ret);
  delete_keys.emplace_back("GP5_KEY1_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP5_KEY2_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP5_KEY3_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP5_KEY4_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP5_KEY5_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP5_KEY6_SCAN_CASE_SINGLE_LIST");

  // ZSet
  s = db.ZAdd("GP5_KEY1_SCAN_CASE_SINGLE_ZSET", {{1, "GP5_SCAN_CASE_SINGLE_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP5_KEY2_SCAN_CASE_SINGLE_ZSET", {{1, "GP5_SCAN_CASE_SINGLE_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP5_KEY3_SCAN_CASE_SINGLE_ZSET", {{1, "GP5_SCAN_CASE_SINGLE_LIST_MEMBER3"}}, &int32_ret);
  s = db.ZAdd("GP5_KEY4_SCAN_CASE_SINGLE_ZSET", {{1, "GP5_SCAN_CASE_SINGLE_LIST_MEMBER4"}}, &int32_ret);
  s = db.ZAdd("GP5_KEY5_SCAN_CASE_SINGLE_ZSET", {{1, "GP5_SCAN_CASE_SINGLE_LIST_MEMBER5"}}, &int32_ret);
  s = db.ZAdd("GP5_KEY6_SCAN_CASE_SINGLE_ZSET", {{1, "GP5_SCAN_CASE_SINGLE_LIST_MEMBER6"}}, &int32_ret);
  delete_keys.emplace_back("GP5_KEY1_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP5_KEY2_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP5_KEY3_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP5_KEY4_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP5_KEY5_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP5_KEY6_SCAN_CASE_SINGLE_ZSET");

  keys.clear();
  cursor = 0;
  cursor = db.Scan(DataType::kSets, cursor, "*", 2, &keys);
  ASSERT_EQ(cursor, 2);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP5_KEY1_SCAN_CASE_SINGLE_SET");
  ASSERT_EQ(keys[1], "GP5_KEY2_SCAN_CASE_SINGLE_SET");

  keys.clear();
  cursor = db.Scan(DataType::kSets, cursor, "*", 2, &keys);
  ASSERT_EQ(cursor, 4);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP5_KEY3_SCAN_CASE_SINGLE_SET");
  ASSERT_EQ(keys[1], "GP5_KEY4_SCAN_CASE_SINGLE_SET");

  keys.clear();
  cursor = db.Scan(DataType::kSets, cursor, "*", 2, &keys);
  ASSERT_EQ(cursor, 0);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP5_KEY5_SCAN_CASE_SINGLE_SET");
  ASSERT_EQ(keys[1], "GP5_KEY6_SCAN_CASE_SINGLE_SET");

  del_num = db.Del(delete_keys);
  ASSERT_EQ(del_num, 30);
  sleep(2);
  db.Compact(DataType::kAll, true);

  // ***************** Group 6 Test *****************
  delete_keys.clear();
  // String
  s = db.Set("GP6_KEY1_SCAN_CASE_SINGLE_STRING", "GP6_SCAN_CASE_SINGLE_STRING_VALUE1");
  s = db.Set("GP6_KEY2_SCAN_CASE_SINGLE_STRING", "GP6_SCAN_CASE_SINGLE_STRING_VALUE2");
  s = db.Set("GP6_KEY3_SCAN_CASE_SINGLE_STRING", "GP6_SCAN_CASE_SINGLE_STRING_VALUE3");
  s = db.Set("GP6_KEY4_SCAN_CASE_SINGLE_STRING", "GP6_SCAN_CASE_SINGLE_STRING_VALUE4");
  s = db.Set("GP6_KEY5_SCAN_CASE_SINGLE_STRING", "GP6_SCAN_CASE_SINGLE_STRING_VALUE5");
  s = db.Set("GP6_KEY6_SCAN_CASE_SINGLE_STRING", "GP6_SCAN_CASE_SINGLE_STRING_VALUE6");
  delete_keys.emplace_back("GP6_KEY1_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP6_KEY2_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP6_KEY3_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP6_KEY4_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP6_KEY5_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP6_KEY6_SCAN_CASE_SINGLE_STRING");

  // Hash
  s = db.HSet("GP6_KEY1_SCAN_CASE_SINGLE_HASH", "GP6_SCAN_CASE_SINGLE_HASH_FIELD1", "GP6_SCAN_CASE_SINGLE_HASH_VALUE1",
              &int32_ret);
  s = db.HSet("GP6_KEY2_SCAN_CASE_SINGLE_HASH", "GP6_SCAN_CASE_SINGLE_HASH_FIELD2", "GP6_SCAN_CASE_SINGLE_HASH_VALUE2",
              &int32_ret);
  s = db.HSet("GP6_KEY3_SCAN_CASE_SINGLE_HASH", "GP6_SCAN_CASE_SINGLE_HASH_FIELD3", "GP6_SCAN_CASE_SINGLE_HASH_VALUE3",
              &int32_ret);
  s = db.HSet("GP6_KEY4_SCAN_CASE_SINGLE_HASH", "GP6_SCAN_CASE_SINGLE_HASH_FIELD4", "GP6_SCAN_CASE_SINGLE_HASH_VALUE4",
              &int32_ret);
  s = db.HSet("GP6_KEY5_SCAN_CASE_SINGLE_HASH", "GP6_SCAN_CASE_SINGLE_HASH_FIELD5", "GP6_SCAN_CASE_SINGLE_HASH_VALUE5",
              &int32_ret);
  s = db.HSet("GP6_KEY6_SCAN_CASE_SINGLE_HASH", "GP6_SCAN_CASE_SINGLE_HASH_FIELD6", "GP6_SCAN_CASE_SINGLE_HASH_VALUE6",
              &int32_ret);
  delete_keys.emplace_back("GP6_KEY1_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP6_KEY2_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP6_KEY3_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP6_KEY4_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP6_KEY5_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP6_KEY6_SCAN_CASE_SINGLE_HASH");

  // Set
  s = db.SAdd("GP6_KEY1_SCAN_CASE_SINGLE_SET", {"GP6_SCAN_CASE_SINGLE_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP6_KEY2_SCAN_CASE_SINGLE_SET", {"GP6_SCAN_CASE_SINGLE_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP6_KEY3_SCAN_CASE_SINGLE_SET", {"GP6_SCAN_CASE_SINGLE_SET_MEMBER3"}, &int32_ret);
  s = db.SAdd("GP6_KEY4_SCAN_CASE_SINGLE_SET", {"GP6_SCAN_CASE_SINGLE_SET_MEMBER4"}, &int32_ret);
  s = db.SAdd("GP6_KEY5_SCAN_CASE_SINGLE_SET", {"GP6_SCAN_CASE_SINGLE_SET_MEMBER5"}, &int32_ret);
  s = db.SAdd("GP6_KEY6_SCAN_CASE_SINGLE_SET", {"GP6_SCAN_CASE_SINGLE_SET_MEMBER6"}, &int32_ret);
  delete_keys.emplace_back("GP6_KEY1_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP6_KEY2_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP6_KEY3_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP6_KEY4_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP6_KEY5_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP6_KEY6_SCAN_CASE_SINGLE_SET");

  // List
  s = db.LPush("GP6_KEY1_SCAN_CASE_SINGLE_LIST", {"GP6_SCAN_CASE_SINGLE_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP6_KEY2_SCAN_CASE_SINGLE_LIST", {"GP6_SCAN_CASE_SINGLE_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP6_KEY3_SCAN_CASE_SINGLE_LIST", {"GP6_SCAN_CASE_SINGLE_LIST_NODE3"}, &uint64_ret);
  s = db.LPush("GP6_KEY4_SCAN_CASE_SINGLE_LIST", {"GP6_SCAN_CASE_SINGLE_LIST_NODE4"}, &uint64_ret);
  s = db.LPush("GP6_KEY5_SCAN_CASE_SINGLE_LIST", {"GP6_SCAN_CASE_SINGLE_LIST_NODE5"}, &uint64_ret);
  s = db.LPush("GP6_KEY6_SCAN_CASE_SINGLE_LIST", {"GP6_SCAN_CASE_SINGLE_LIST_NODE6"}, &uint64_ret);
  delete_keys.emplace_back("GP6_KEY1_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP6_KEY2_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP6_KEY3_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP6_KEY4_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP6_KEY5_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP6_KEY6_SCAN_CASE_SINGLE_LIST");

  // ZSet
  s = db.ZAdd("GP6_KEY1_SCAN_CASE_SINGLE_ZSET", {{1, "GP6_SCAN_CASE_SINGLE_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP6_KEY2_SCAN_CASE_SINGLE_ZSET", {{1, "GP6_SCAN_CASE_SINGLE_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP6_KEY3_SCAN_CASE_SINGLE_ZSET", {{1, "GP6_SCAN_CASE_SINGLE_LIST_MEMBER3"}}, &int32_ret);
  s = db.ZAdd("GP6_KEY4_SCAN_CASE_SINGLE_ZSET", {{1, "GP6_SCAN_CASE_SINGLE_LIST_MEMBER4"}}, &int32_ret);
  s = db.ZAdd("GP6_KEY5_SCAN_CASE_SINGLE_ZSET", {{1, "GP6_SCAN_CASE_SINGLE_LIST_MEMBER5"}}, &int32_ret);
  s = db.ZAdd("GP6_KEY6_SCAN_CASE_SINGLE_ZSET", {{1, "GP6_SCAN_CASE_SINGLE_LIST_MEMBER6"}}, &int32_ret);
  delete_keys.emplace_back("GP6_KEY1_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP6_KEY2_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP6_KEY3_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP6_KEY4_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP6_KEY5_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP6_KEY6_SCAN_CASE_SINGLE_ZSET");

  keys.clear();
  cursor = 0;
  cursor = db.Scan(DataType::kSets, cursor, "*", 4, &keys);
  ASSERT_EQ(cursor, 4);
  ASSERT_EQ(keys.size(), 4);
  ASSERT_EQ(keys[0], "GP6_KEY1_SCAN_CASE_SINGLE_SET");
  ASSERT_EQ(keys[1], "GP6_KEY2_SCAN_CASE_SINGLE_SET");
  ASSERT_EQ(keys[2], "GP6_KEY3_SCAN_CASE_SINGLE_SET");
  ASSERT_EQ(keys[3], "GP6_KEY4_SCAN_CASE_SINGLE_SET");

  keys.clear();
  cursor = db.Scan(DataType::kSets, cursor, "*", 4, &keys);
  ASSERT_EQ(cursor, 0);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP6_KEY5_SCAN_CASE_SINGLE_SET");
  ASSERT_EQ(keys[1], "GP6_KEY6_SCAN_CASE_SINGLE_SET");

  del_num = db.Del(delete_keys);
  ASSERT_EQ(del_num, 30);
  sleep(2);
  db.Compact(DataType::kAll, true);

  // ***************** Group 7 Test *****************
  delete_keys.clear();
  // String
  s = db.Set("GP7_KEY1_SCAN_CASE_SINGLE_STRING", "GP7_SCAN_CASE_SINGLE_STRING_VALUE1");
  s = db.Set("GP7_KEY2_SCAN_CASE_SINGLE_STRING", "GP7_SCAN_CASE_SINGLE_STRING_VALUE2");
  s = db.Set("GP7_KEY3_SCAN_CASE_SINGLE_STRING", "GP7_SCAN_CASE_SINGLE_STRING_VALUE3");
  s = db.Set("GP7_KEY4_SCAN_CASE_SINGLE_STRING", "GP7_SCAN_CASE_SINGLE_STRING_VALUE4");
  s = db.Set("GP7_KEY5_SCAN_CASE_SINGLE_STRING", "GP7_SCAN_CASE_SINGLE_STRING_VALUE5");
  s = db.Set("GP7_KEY6_SCAN_CASE_SINGLE_STRING", "GP7_SCAN_CASE_SINGLE_STRING_VALUE6");
  delete_keys.emplace_back("GP7_KEY1_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP7_KEY2_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP7_KEY3_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP7_KEY4_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP7_KEY5_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP7_KEY6_SCAN_CASE_SINGLE_STRING");

  // Hash
  s = db.HSet("GP7_KEY1_SCAN_CASE_SINGLE_HASH", "GP7_SCAN_CASE_SINGLE_HASH_FIELD1", "GP7_SCAN_CASE_SINGLE_HASH_VALUE1",
              &int32_ret);
  s = db.HSet("GP7_KEY2_SCAN_CASE_SINGLE_HASH", "GP7_SCAN_CASE_SINGLE_HASH_FIELD2", "GP7_SCAN_CASE_SINGLE_HASH_VALUE2",
              &int32_ret);
  s = db.HSet("GP7_KEY3_SCAN_CASE_SINGLE_HASH", "GP7_SCAN_CASE_SINGLE_HASH_FIELD3", "GP7_SCAN_CASE_SINGLE_HASH_VALUE3",
              &int32_ret);
  s = db.HSet("GP7_KEY4_SCAN_CASE_SINGLE_HASH", "GP7_SCAN_CASE_SINGLE_HASH_FIELD4", "GP7_SCAN_CASE_SINGLE_HASH_VALUE4",
              &int32_ret);
  s = db.HSet("GP7_KEY5_SCAN_CASE_SINGLE_HASH", "GP7_SCAN_CASE_SINGLE_HASH_FIELD5", "GP7_SCAN_CASE_SINGLE_HASH_VALUE5",
              &int32_ret);
  s = db.HSet("GP7_KEY6_SCAN_CASE_SINGLE_HASH", "GP7_SCAN_CASE_SINGLE_HASH_FIELD6", "GP7_SCAN_CASE_SINGLE_HASH_VALUE6",
              &int32_ret);
  delete_keys.emplace_back("GP7_KEY1_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP7_KEY2_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP7_KEY3_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP7_KEY4_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP7_KEY5_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP7_KEY6_SCAN_CASE_SINGLE_HASH");

  // Set
  s = db.SAdd("GP7_KEY1_SCAN_CASE_SINGLE_SET", {"GP7_SCAN_CASE_SINGLE_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP7_KEY2_SCAN_CASE_SINGLE_SET", {"GP7_SCAN_CASE_SINGLE_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP7_KEY3_SCAN_CASE_SINGLE_SET", {"GP7_SCAN_CASE_SINGLE_SET_MEMBER3"}, &int32_ret);
  s = db.SAdd("GP7_KEY4_SCAN_CASE_SINGLE_SET", {"GP7_SCAN_CASE_SINGLE_SET_MEMBER4"}, &int32_ret);
  s = db.SAdd("GP7_KEY5_SCAN_CASE_SINGLE_SET", {"GP7_SCAN_CASE_SINGLE_SET_MEMBER5"}, &int32_ret);
  s = db.SAdd("GP7_KEY6_SCAN_CASE_SINGLE_SET", {"GP7_SCAN_CASE_SINGLE_SET_MEMBER6"}, &int32_ret);
  delete_keys.emplace_back("GP7_KEY1_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP7_KEY2_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP7_KEY3_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP7_KEY4_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP7_KEY5_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP7_KEY6_SCAN_CASE_SINGLE_SET");

  // List
  s = db.LPush("GP7_KEY1_SCAN_CASE_SINGLE_LIST", {"GP7_SCAN_CASE_SINGLE_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP7_KEY2_SCAN_CASE_SINGLE_LIST", {"GP7_SCAN_CASE_SINGLE_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP7_KEY3_SCAN_CASE_SINGLE_LIST", {"GP7_SCAN_CASE_SINGLE_LIST_NODE3"}, &uint64_ret);
  s = db.LPush("GP7_KEY4_SCAN_CASE_SINGLE_LIST", {"GP7_SCAN_CASE_SINGLE_LIST_NODE4"}, &uint64_ret);
  s = db.LPush("GP7_KEY5_SCAN_CASE_SINGLE_LIST", {"GP7_SCAN_CASE_SINGLE_LIST_NODE5"}, &uint64_ret);
  s = db.LPush("GP7_KEY6_SCAN_CASE_SINGLE_LIST", {"GP7_SCAN_CASE_SINGLE_LIST_NODE6"}, &uint64_ret);
  delete_keys.emplace_back("GP7_KEY1_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP7_KEY2_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP7_KEY3_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP7_KEY4_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP7_KEY5_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP7_KEY6_SCAN_CASE_SINGLE_LIST");

  // ZSet
  s = db.ZAdd("GP7_KEY1_SCAN_CASE_SINGLE_ZSET", {{1, "GP7_SCAN_CASE_SINGLE_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP7_KEY2_SCAN_CASE_SINGLE_ZSET", {{1, "GP7_SCAN_CASE_SINGLE_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP7_KEY3_SCAN_CASE_SINGLE_ZSET", {{1, "GP7_SCAN_CASE_SINGLE_LIST_MEMBER3"}}, &int32_ret);
  s = db.ZAdd("GP7_KEY4_SCAN_CASE_SINGLE_ZSET", {{1, "GP7_SCAN_CASE_SINGLE_LIST_MEMBER4"}}, &int32_ret);
  s = db.ZAdd("GP7_KEY5_SCAN_CASE_SINGLE_ZSET", {{1, "GP7_SCAN_CASE_SINGLE_LIST_MEMBER5"}}, &int32_ret);
  s = db.ZAdd("GP7_KEY6_SCAN_CASE_SINGLE_ZSET", {{1, "GP7_SCAN_CASE_SINGLE_LIST_MEMBER6"}}, &int32_ret);
  delete_keys.emplace_back("GP7_KEY1_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP7_KEY2_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP7_KEY3_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP7_KEY4_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP7_KEY5_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP7_KEY6_SCAN_CASE_SINGLE_ZSET");

  keys.clear();
  cursor = 0;
  cursor = db.Scan(DataType::kSets, cursor, "*", 6, &keys);
  ASSERT_EQ(cursor, 0);
  ASSERT_EQ(keys.size(), 6);
  ASSERT_EQ(keys[0], "GP7_KEY1_SCAN_CASE_SINGLE_SET");
  ASSERT_EQ(keys[1], "GP7_KEY2_SCAN_CASE_SINGLE_SET");
  ASSERT_EQ(keys[2], "GP7_KEY3_SCAN_CASE_SINGLE_SET");
  ASSERT_EQ(keys[3], "GP7_KEY4_SCAN_CASE_SINGLE_SET");
  ASSERT_EQ(keys[4], "GP7_KEY5_SCAN_CASE_SINGLE_SET");
  ASSERT_EQ(keys[5], "GP7_KEY6_SCAN_CASE_SINGLE_SET");

  del_num = db.Del(delete_keys);
  ASSERT_EQ(del_num, 30);
  sleep(2);
  db.Compact(DataType::kAll, true);

  // ***************** Group 8 Test *****************
  delete_keys.clear();
  // String
  s = db.Set("GP8_KEY1_SCAN_CASE_SINGLE_STRING", "GP8_SCAN_CASE_SINGLE_STRING_VALUE1");
  s = db.Set("GP8_KEY2_SCAN_CASE_SINGLE_STRING", "GP8_SCAN_CASE_SINGLE_STRING_VALUE2");
  s = db.Set("GP8_KEY3_SCAN_CASE_SINGLE_STRING", "GP8_SCAN_CASE_SINGLE_STRING_VALUE3");
  s = db.Set("GP8_KEY4_SCAN_CASE_SINGLE_STRING", "GP8_SCAN_CASE_SINGLE_STRING_VALUE4");
  s = db.Set("GP8_KEY5_SCAN_CASE_SINGLE_STRING", "GP8_SCAN_CASE_SINGLE_STRING_VALUE5");
  s = db.Set("GP8_KEY6_SCAN_CASE_SINGLE_STRING", "GP8_SCAN_CASE_SINGLE_STRING_VALUE6");
  delete_keys.emplace_back("GP8_KEY1_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP8_KEY2_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP8_KEY3_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP8_KEY4_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP8_KEY5_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP8_KEY6_SCAN_CASE_SINGLE_STRING");

  // Hash
  s = db.HSet("GP8_KEY1_SCAN_CASE_SINGLE_HASH", "GP8_SCAN_CASE_SINGLE_HASH_FIELD1", "GP8_SCAN_CASE_SINGLE_HASH_VALUE1",
              &int32_ret);
  s = db.HSet("GP8_KEY2_SCAN_CASE_SINGLE_HASH", "GP8_SCAN_CASE_SINGLE_HASH_FIELD2", "GP8_SCAN_CASE_SINGLE_HASH_VALUE2",
              &int32_ret);
  s = db.HSet("GP8_KEY3_SCAN_CASE_SINGLE_HASH", "GP8_SCAN_CASE_SINGLE_HASH_FIELD3", "GP8_SCAN_CASE_SINGLE_HASH_VALUE3",
              &int32_ret);
  s = db.HSet("GP8_KEY4_SCAN_CASE_SINGLE_HASH", "GP8_SCAN_CASE_SINGLE_HASH_FIELD4", "GP8_SCAN_CASE_SINGLE_HASH_VALUE4",
              &int32_ret);
  s = db.HSet("GP8_KEY5_SCAN_CASE_SINGLE_HASH", "GP8_SCAN_CASE_SINGLE_HASH_FIELD5", "GP8_SCAN_CASE_SINGLE_HASH_VALUE5",
              &int32_ret);
  s = db.HSet("GP8_KEY6_SCAN_CASE_SINGLE_HASH", "GP8_SCAN_CASE_SINGLE_HASH_FIELD6", "GP8_SCAN_CASE_SINGLE_HASH_VALUE6",
              &int32_ret);
  delete_keys.emplace_back("GP8_KEY1_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP8_KEY2_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP8_KEY3_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP8_KEY4_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP8_KEY5_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP8_KEY6_SCAN_CASE_SINGLE_HASH");

  // Set
  s = db.SAdd("GP8_KEY1_SCAN_CASE_SINGLE_SET", {"GP8_SCAN_CASE_SINGLE_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP8_KEY2_SCAN_CASE_SINGLE_SET", {"GP8_SCAN_CASE_SINGLE_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP8_KEY3_SCAN_CASE_SINGLE_SET", {"GP8_SCAN_CASE_SINGLE_SET_MEMBER3"}, &int32_ret);
  s = db.SAdd("GP8_KEY4_SCAN_CASE_SINGLE_SET", {"GP8_SCAN_CASE_SINGLE_SET_MEMBER4"}, &int32_ret);
  s = db.SAdd("GP8_KEY5_SCAN_CASE_SINGLE_SET", {"GP8_SCAN_CASE_SINGLE_SET_MEMBER5"}, &int32_ret);
  s = db.SAdd("GP8_KEY6_SCAN_CASE_SINGLE_SET", {"GP8_SCAN_CASE_SINGLE_SET_MEMBER6"}, &int32_ret);
  delete_keys.emplace_back("GP8_KEY1_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP8_KEY2_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP8_KEY3_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP8_KEY4_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP8_KEY5_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP8_KEY6_SCAN_CASE_SINGLE_SET");

  // List
  s = db.LPush("GP8_KEY1_SCAN_CASE_SINGLE_LIST", {"GP8_SCAN_CASE_SINGLE_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP8_KEY2_SCAN_CASE_SINGLE_LIST", {"GP8_SCAN_CASE_SINGLE_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP8_KEY3_SCAN_CASE_SINGLE_LIST", {"GP8_SCAN_CASE_SINGLE_LIST_NODE3"}, &uint64_ret);
  s = db.LPush("GP8_KEY4_SCAN_CASE_SINGLE_LIST", {"GP8_SCAN_CASE_SINGLE_LIST_NODE4"}, &uint64_ret);
  s = db.LPush("GP8_KEY5_SCAN_CASE_SINGLE_LIST", {"GP8_SCAN_CASE_SINGLE_LIST_NODE5"}, &uint64_ret);
  s = db.LPush("GP8_KEY6_SCAN_CASE_SINGLE_LIST", {"GP8_SCAN_CASE_SINGLE_LIST_NODE6"}, &uint64_ret);
  delete_keys.emplace_back("GP8_KEY1_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP8_KEY2_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP8_KEY3_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP8_KEY4_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP8_KEY5_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP8_KEY6_SCAN_CASE_SINGLE_LIST");

  // ZSet
  s = db.ZAdd("GP8_KEY1_SCAN_CASE_SINGLE_ZSET", {{1, "GP8_SCAN_CASE_SINGLE_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP8_KEY2_SCAN_CASE_SINGLE_ZSET", {{1, "GP8_SCAN_CASE_SINGLE_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP8_KEY3_SCAN_CASE_SINGLE_ZSET", {{1, "GP8_SCAN_CASE_SINGLE_LIST_MEMBER3"}}, &int32_ret);
  s = db.ZAdd("GP8_KEY4_SCAN_CASE_SINGLE_ZSET", {{1, "GP8_SCAN_CASE_SINGLE_LIST_MEMBER4"}}, &int32_ret);
  s = db.ZAdd("GP8_KEY5_SCAN_CASE_SINGLE_ZSET", {{1, "GP8_SCAN_CASE_SINGLE_LIST_MEMBER5"}}, &int32_ret);
  s = db.ZAdd("GP8_KEY6_SCAN_CASE_SINGLE_ZSET", {{1, "GP8_SCAN_CASE_SINGLE_LIST_MEMBER6"}}, &int32_ret);
  delete_keys.emplace_back("GP8_KEY1_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP8_KEY2_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP8_KEY3_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP8_KEY4_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP8_KEY5_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP8_KEY6_SCAN_CASE_SINGLE_ZSET");

  keys.clear();
  cursor = 0;
  cursor = db.Scan(DataType::kSets, cursor, "*", 10, &keys);
  ASSERT_EQ(cursor, 0);
  ASSERT_EQ(keys.size(), 6);
  ASSERT_EQ(keys[0], "GP8_KEY1_SCAN_CASE_SINGLE_SET");
  ASSERT_EQ(keys[1], "GP8_KEY2_SCAN_CASE_SINGLE_SET");
  ASSERT_EQ(keys[2], "GP8_KEY3_SCAN_CASE_SINGLE_SET");
  ASSERT_EQ(keys[3], "GP8_KEY4_SCAN_CASE_SINGLE_SET");
  ASSERT_EQ(keys[4], "GP8_KEY5_SCAN_CASE_SINGLE_SET");
  ASSERT_EQ(keys[5], "GP8_KEY6_SCAN_CASE_SINGLE_SET");

  del_num = db.Del(delete_keys);
  ASSERT_EQ(del_num, 30);
  sleep(2);
  db.Compact(DataType::kAll, true);

  // ***************** Group 9 Test *****************
  delete_keys.clear();
  // String
  s = db.Set("GP9_KEY1_SCAN_CASE_SINGLE_STRING", "GP9_SCAN_CASE_SINGLE_STRING_VALUE1");
  s = db.Set("GP9_KEY2_SCAN_CASE_SINGLE_STRING", "GP9_SCAN_CASE_SINGLE_STRING_VALUE2");
  s = db.Set("GP9_KEY3_SCAN_CASE_SINGLE_STRING", "GP9_SCAN_CASE_SINGLE_STRING_VALUE3");
  s = db.Set("GP9_KEY4_SCAN_CASE_SINGLE_STRING", "GP9_SCAN_CASE_SINGLE_STRING_VALUE4");
  s = db.Set("GP9_KEY5_SCAN_CASE_SINGLE_STRING", "GP9_SCAN_CASE_SINGLE_STRING_VALUE5");
  s = db.Set("GP9_KEY6_SCAN_CASE_SINGLE_STRING", "GP9_SCAN_CASE_SINGLE_STRING_VALUE6");
  delete_keys.emplace_back("GP9_KEY1_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP9_KEY2_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP9_KEY3_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP9_KEY4_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP9_KEY5_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP9_KEY6_SCAN_CASE_SINGLE_STRING");

  // Hash
  s = db.HSet("GP9_KEY1_SCAN_CASE_SINGLE_HASH", "GP9_SCAN_CASE_SINGLE_HASH_FIELD1", "GP9_SCAN_CASE_SINGLE_HASH_VALUE1",
              &int32_ret);
  s = db.HSet("GP9_KEY2_SCAN_CASE_SINGLE_HASH", "GP9_SCAN_CASE_SINGLE_HASH_FIELD2", "GP9_SCAN_CASE_SINGLE_HASH_VALUE2",
              &int32_ret);
  s = db.HSet("GP9_KEY3_SCAN_CASE_SINGLE_HASH", "GP9_SCAN_CASE_SINGLE_HASH_FIELD3", "GP9_SCAN_CASE_SINGLE_HASH_VALUE3",
              &int32_ret);
  s = db.HSet("GP9_KEY4_SCAN_CASE_SINGLE_HASH", "GP9_SCAN_CASE_SINGLE_HASH_FIELD4", "GP9_SCAN_CASE_SINGLE_HASH_VALUE4",
              &int32_ret);
  s = db.HSet("GP9_KEY5_SCAN_CASE_SINGLE_HASH", "GP9_SCAN_CASE_SINGLE_HASH_FIELD5", "GP9_SCAN_CASE_SINGLE_HASH_VALUE5",
              &int32_ret);
  s = db.HSet("GP9_KEY6_SCAN_CASE_SINGLE_HASH", "GP9_SCAN_CASE_SINGLE_HASH_FIELD6", "GP9_SCAN_CASE_SINGLE_HASH_VALUE6",
              &int32_ret);
  delete_keys.emplace_back("GP9_KEY1_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP9_KEY2_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP9_KEY3_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP9_KEY4_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP9_KEY5_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP9_KEY6_SCAN_CASE_SINGLE_HASH");

  // Set
  s = db.SAdd("GP9_KEY1_SCAN_CASE_SINGLE_SET", {"GP9_SCAN_CASE_SINGLE_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP9_KEY2_SCAN_CASE_SINGLE_SET", {"GP9_SCAN_CASE_SINGLE_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP9_KEY3_SCAN_CASE_SINGLE_SET", {"GP9_SCAN_CASE_SINGLE_SET_MEMBER3"}, &int32_ret);
  s = db.SAdd("GP9_KEY4_SCAN_CASE_SINGLE_SET", {"GP9_SCAN_CASE_SINGLE_SET_MEMBER4"}, &int32_ret);
  s = db.SAdd("GP9_KEY5_SCAN_CASE_SINGLE_SET", {"GP9_SCAN_CASE_SINGLE_SET_MEMBER5"}, &int32_ret);
  s = db.SAdd("GP9_KEY6_SCAN_CASE_SINGLE_SET", {"GP9_SCAN_CASE_SINGLE_SET_MEMBER6"}, &int32_ret);
  delete_keys.emplace_back("GP9_KEY1_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP9_KEY2_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP9_KEY3_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP9_KEY4_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP9_KEY5_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP9_KEY6_SCAN_CASE_SINGLE_SET");

  // List
  s = db.LPush("GP9_KEY1_SCAN_CASE_SINGLE_LIST", {"GP9_SCAN_CASE_SINGLE_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP9_KEY2_SCAN_CASE_SINGLE_LIST", {"GP9_SCAN_CASE_SINGLE_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP9_KEY3_SCAN_CASE_SINGLE_LIST", {"GP9_SCAN_CASE_SINGLE_LIST_NODE3"}, &uint64_ret);
  s = db.LPush("GP9_KEY4_SCAN_CASE_SINGLE_LIST", {"GP9_SCAN_CASE_SINGLE_LIST_NODE4"}, &uint64_ret);
  s = db.LPush("GP9_KEY5_SCAN_CASE_SINGLE_LIST", {"GP9_SCAN_CASE_SINGLE_LIST_NODE5"}, &uint64_ret);
  s = db.LPush("GP9_KEY6_SCAN_CASE_SINGLE_LIST", {"GP9_SCAN_CASE_SINGLE_LIST_NODE6"}, &uint64_ret);
  delete_keys.emplace_back("GP9_KEY1_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP9_KEY2_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP9_KEY3_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP9_KEY4_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP9_KEY5_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP9_KEY6_SCAN_CASE_SINGLE_LIST");

  // ZSet
  s = db.ZAdd("GP9_KEY1_SCAN_CASE_SINGLE_ZSET", {{1, "GP9_SCAN_CASE_SINGLE_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP9_KEY2_SCAN_CASE_SINGLE_ZSET", {{1, "GP9_SCAN_CASE_SINGLE_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP9_KEY3_SCAN_CASE_SINGLE_ZSET", {{1, "GP9_SCAN_CASE_SINGLE_LIST_MEMBER3"}}, &int32_ret);
  s = db.ZAdd("GP9_KEY4_SCAN_CASE_SINGLE_ZSET", {{1, "GP9_SCAN_CASE_SINGLE_LIST_MEMBER4"}}, &int32_ret);
  s = db.ZAdd("GP9_KEY5_SCAN_CASE_SINGLE_ZSET", {{1, "GP9_SCAN_CASE_SINGLE_LIST_MEMBER5"}}, &int32_ret);
  s = db.ZAdd("GP9_KEY6_SCAN_CASE_SINGLE_ZSET", {{1, "GP9_SCAN_CASE_SINGLE_LIST_MEMBER6"}}, &int32_ret);
  delete_keys.emplace_back("GP9_KEY1_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP9_KEY2_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP9_KEY3_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP9_KEY4_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP9_KEY5_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP9_KEY6_SCAN_CASE_SINGLE_ZSET");

  keys.clear();
  cursor = 0;
  cursor = db.Scan(DataType::kZSets, cursor, "*", 2, &keys);
  ASSERT_EQ(cursor, 2);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP9_KEY1_SCAN_CASE_SINGLE_ZSET");
  ASSERT_EQ(keys[1], "GP9_KEY2_SCAN_CASE_SINGLE_ZSET");

  keys.clear();
  cursor = db.Scan(DataType::kZSets, cursor, "*", 2, &keys);
  ASSERT_EQ(cursor, 4);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP9_KEY3_SCAN_CASE_SINGLE_ZSET");
  ASSERT_EQ(keys[1], "GP9_KEY4_SCAN_CASE_SINGLE_ZSET");

  keys.clear();
  cursor = db.Scan(DataType::kZSets, cursor, "*", 2, &keys);
  ASSERT_EQ(cursor, 0);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP9_KEY5_SCAN_CASE_SINGLE_ZSET");
  ASSERT_EQ(keys[1], "GP9_KEY6_SCAN_CASE_SINGLE_ZSET");

  del_num = db.Del(delete_keys);
  ASSERT_EQ(del_num, 30);
  sleep(2);
  db.Compact(DataType::kAll, true);

  // ***************** Group 10 Test *****************
  delete_keys.clear();
  // String
  s = db.Set("GP10_KEY1_SCAN_CASE_SINGLE_STRING", "GP10_SCAN_CASE_SINGLE_STRING_VALUE1");
  s = db.Set("GP10_KEY2_SCAN_CASE_SINGLE_STRING", "GP10_SCAN_CASE_SINGLE_STRING_VALUE2");
  s = db.Set("GP10_KEY3_SCAN_CASE_SINGLE_STRING", "GP10_SCAN_CASE_SINGLE_STRING_VALUE3");
  s = db.Set("GP10_KEY4_SCAN_CASE_SINGLE_STRING", "GP10_SCAN_CASE_SINGLE_STRING_VALUE4");
  s = db.Set("GP10_KEY5_SCAN_CASE_SINGLE_STRING", "GP10_SCAN_CASE_SINGLE_STRING_VALUE5");
  s = db.Set("GP10_KEY6_SCAN_CASE_SINGLE_STRING", "GP10_SCAN_CASE_SINGLE_STRING_VALUE6");
  delete_keys.emplace_back("GP10_KEY1_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP10_KEY2_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP10_KEY3_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP10_KEY4_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP10_KEY5_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP10_KEY6_SCAN_CASE_SINGLE_STRING");

  // Hash
  s = db.HSet("GP10_KEY1_SCAN_CASE_SINGLE_HASH", "GP10_SCAN_CASE_SINGLE_HASH_FIELD1",
              "GP10_SCAN_CASE_SINGLE_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP10_KEY2_SCAN_CASE_SINGLE_HASH", "GP10_SCAN_CASE_SINGLE_HASH_FIELD2",
              "GP10_SCAN_CASE_SINGLE_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP10_KEY3_SCAN_CASE_SINGLE_HASH", "GP10_SCAN_CASE_SINGLE_HASH_FIELD3",
              "GP10_SCAN_CASE_SINGLE_HASH_VALUE3", &int32_ret);
  s = db.HSet("GP10_KEY4_SCAN_CASE_SINGLE_HASH", "GP10_SCAN_CASE_SINGLE_HASH_FIELD4",
              "GP10_SCAN_CASE_SINGLE_HASH_VALUE4", &int32_ret);
  s = db.HSet("GP10_KEY5_SCAN_CASE_SINGLE_HASH", "GP10_SCAN_CASE_SINGLE_HASH_FIELD5",
              "GP10_SCAN_CASE_SINGLE_HASH_VALUE5", &int32_ret);
  s = db.HSet("GP10_KEY6_SCAN_CASE_SINGLE_HASH", "GP10_SCAN_CASE_SINGLE_HASH_FIELD6",
              "GP10_SCAN_CASE_SINGLE_HASH_VALUE6", &int32_ret);
  delete_keys.emplace_back("GP10_KEY1_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP10_KEY2_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP10_KEY3_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP10_KEY4_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP10_KEY5_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP10_KEY6_SCAN_CASE_SINGLE_HASH");

  // Set
  s = db.SAdd("GP10_KEY1_SCAN_CASE_SINGLE_SET", {"GP10_SCAN_CASE_SINGLE_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP10_KEY2_SCAN_CASE_SINGLE_SET", {"GP10_SCAN_CASE_SINGLE_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP10_KEY3_SCAN_CASE_SINGLE_SET", {"GP10_SCAN_CASE_SINGLE_SET_MEMBER3"}, &int32_ret);
  s = db.SAdd("GP10_KEY4_SCAN_CASE_SINGLE_SET", {"GP10_SCAN_CASE_SINGLE_SET_MEMBER4"}, &int32_ret);
  s = db.SAdd("GP10_KEY5_SCAN_CASE_SINGLE_SET", {"GP10_SCAN_CASE_SINGLE_SET_MEMBER5"}, &int32_ret);
  s = db.SAdd("GP10_KEY6_SCAN_CASE_SINGLE_SET", {"GP10_SCAN_CASE_SINGLE_SET_MEMBER6"}, &int32_ret);
  delete_keys.emplace_back("GP10_KEY1_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP10_KEY2_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP10_KEY3_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP10_KEY4_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP10_KEY5_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP10_KEY6_SCAN_CASE_SINGLE_SET");

  // List
  s = db.LPush("GP10_KEY1_SCAN_CASE_SINGLE_LIST", {"GP10_SCAN_CASE_SINGLE_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP10_KEY2_SCAN_CASE_SINGLE_LIST", {"GP10_SCAN_CASE_SINGLE_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP10_KEY3_SCAN_CASE_SINGLE_LIST", {"GP10_SCAN_CASE_SINGLE_LIST_NODE3"}, &uint64_ret);
  s = db.LPush("GP10_KEY4_SCAN_CASE_SINGLE_LIST", {"GP10_SCAN_CASE_SINGLE_LIST_NODE4"}, &uint64_ret);
  s = db.LPush("GP10_KEY5_SCAN_CASE_SINGLE_LIST", {"GP10_SCAN_CASE_SINGLE_LIST_NODE5"}, &uint64_ret);
  s = db.LPush("GP10_KEY6_SCAN_CASE_SINGLE_LIST", {"GP10_SCAN_CASE_SINGLE_LIST_NODE6"}, &uint64_ret);
  delete_keys.emplace_back("GP10_KEY1_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP10_KEY2_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP10_KEY3_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP10_KEY4_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP10_KEY5_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP10_KEY6_SCAN_CASE_SINGLE_LIST");

  // ZSet
  s = db.ZAdd("GP10_KEY1_SCAN_CASE_SINGLE_ZSET", {{1, "GP10_SCAN_CASE_SINGLE_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP10_KEY2_SCAN_CASE_SINGLE_ZSET", {{1, "GP10_SCAN_CASE_SINGLE_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP10_KEY3_SCAN_CASE_SINGLE_ZSET", {{1, "GP10_SCAN_CASE_SINGLE_LIST_MEMBER3"}}, &int32_ret);
  s = db.ZAdd("GP10_KEY4_SCAN_CASE_SINGLE_ZSET", {{1, "GP10_SCAN_CASE_SINGLE_LIST_MEMBER4"}}, &int32_ret);
  s = db.ZAdd("GP10_KEY5_SCAN_CASE_SINGLE_ZSET", {{1, "GP10_SCAN_CASE_SINGLE_LIST_MEMBER5"}}, &int32_ret);
  s = db.ZAdd("GP10_KEY6_SCAN_CASE_SINGLE_ZSET", {{1, "GP10_SCAN_CASE_SINGLE_LIST_MEMBER6"}}, &int32_ret);
  delete_keys.emplace_back("GP10_KEY1_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP10_KEY2_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP10_KEY3_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP10_KEY4_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP10_KEY5_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP10_KEY6_SCAN_CASE_SINGLE_ZSET");

  keys.clear();
  cursor = 0;
  cursor = db.Scan(DataType::kZSets, cursor, "*", 4, &keys);
  ASSERT_EQ(cursor, 4);
  ASSERT_EQ(keys.size(), 4);
  ASSERT_EQ(keys[0], "GP10_KEY1_SCAN_CASE_SINGLE_ZSET");
  ASSERT_EQ(keys[1], "GP10_KEY2_SCAN_CASE_SINGLE_ZSET");
  ASSERT_EQ(keys[2], "GP10_KEY3_SCAN_CASE_SINGLE_ZSET");
  ASSERT_EQ(keys[3], "GP10_KEY4_SCAN_CASE_SINGLE_ZSET");

  keys.clear();
  cursor = db.Scan(DataType::kZSets, cursor, "*", 4, &keys);
  ASSERT_EQ(cursor, 0);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP10_KEY5_SCAN_CASE_SINGLE_ZSET");
  ASSERT_EQ(keys[1], "GP10_KEY6_SCAN_CASE_SINGLE_ZSET");

  del_num = db.Del(delete_keys);
  ASSERT_EQ(del_num, 30);
  sleep(2);
  db.Compact(DataType::kAll, true);

  // ***************** Group 11 Test *****************
  delete_keys.clear();
  // String
  s = db.Set("GP11_KEY1_SCAN_CASE_SINGLE_STRING", "GP11_SCAN_CASE_SINGLE_STRING_VALUE1");
  s = db.Set("GP11_KEY2_SCAN_CASE_SINGLE_STRING", "GP11_SCAN_CASE_SINGLE_STRING_VALUE2");
  s = db.Set("GP11_KEY3_SCAN_CASE_SINGLE_STRING", "GP11_SCAN_CASE_SINGLE_STRING_VALUE3");
  s = db.Set("GP11_KEY4_SCAN_CASE_SINGLE_STRING", "GP11_SCAN_CASE_SINGLE_STRING_VALUE4");
  s = db.Set("GP11_KEY5_SCAN_CASE_SINGLE_STRING", "GP11_SCAN_CASE_SINGLE_STRING_VALUE5");
  s = db.Set("GP11_KEY6_SCAN_CASE_SINGLE_STRING", "GP11_SCAN_CASE_SINGLE_STRING_VALUE6");
  delete_keys.emplace_back("GP11_KEY1_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP11_KEY2_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP11_KEY3_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP11_KEY4_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP11_KEY5_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP11_KEY6_SCAN_CASE_SINGLE_STRING");

  // Hash
  s = db.HSet("GP11_KEY1_SCAN_CASE_SINGLE_HASH", "GP11_SCAN_CASE_SINGLE_HASH_FIELD1",
              "GP11_SCAN_CASE_SINGLE_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP11_KEY2_SCAN_CASE_SINGLE_HASH", "GP11_SCAN_CASE_SINGLE_HASH_FIELD2",
              "GP11_SCAN_CASE_SINGLE_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP11_KEY3_SCAN_CASE_SINGLE_HASH", "GP11_SCAN_CASE_SINGLE_HASH_FIELD3",
              "GP11_SCAN_CASE_SINGLE_HASH_VALUE3", &int32_ret);
  s = db.HSet("GP11_KEY4_SCAN_CASE_SINGLE_HASH", "GP11_SCAN_CASE_SINGLE_HASH_FIELD4",
              "GP11_SCAN_CASE_SINGLE_HASH_VALUE4", &int32_ret);
  s = db.HSet("GP11_KEY5_SCAN_CASE_SINGLE_HASH", "GP11_SCAN_CASE_SINGLE_HASH_FIELD5",
              "GP11_SCAN_CASE_SINGLE_HASH_VALUE5", &int32_ret);
  s = db.HSet("GP11_KEY6_SCAN_CASE_SINGLE_HASH", "GP11_SCAN_CASE_SINGLE_HASH_FIELD6",
              "GP11_SCAN_CASE_SINGLE_HASH_VALUE6", &int32_ret);
  delete_keys.emplace_back("GP11_KEY1_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP11_KEY2_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP11_KEY3_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP11_KEY4_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP11_KEY5_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP11_KEY6_SCAN_CASE_SINGLE_HASH");

  // Set
  s = db.SAdd("GP11_KEY1_SCAN_CASE_SINGLE_SET", {"GP11_SCAN_CASE_SINGLE_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP11_KEY2_SCAN_CASE_SINGLE_SET", {"GP11_SCAN_CASE_SINGLE_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP11_KEY3_SCAN_CASE_SINGLE_SET", {"GP11_SCAN_CASE_SINGLE_SET_MEMBER3"}, &int32_ret);
  s = db.SAdd("GP11_KEY4_SCAN_CASE_SINGLE_SET", {"GP11_SCAN_CASE_SINGLE_SET_MEMBER4"}, &int32_ret);
  s = db.SAdd("GP11_KEY5_SCAN_CASE_SINGLE_SET", {"GP11_SCAN_CASE_SINGLE_SET_MEMBER5"}, &int32_ret);
  s = db.SAdd("GP11_KEY6_SCAN_CASE_SINGLE_SET", {"GP11_SCAN_CASE_SINGLE_SET_MEMBER6"}, &int32_ret);
  delete_keys.emplace_back("GP11_KEY1_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP11_KEY2_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP11_KEY3_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP11_KEY4_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP11_KEY5_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP11_KEY6_SCAN_CASE_SINGLE_SET");

  // List
  s = db.LPush("GP11_KEY1_SCAN_CASE_SINGLE_LIST", {"GP11_SCAN_CASE_SINGLE_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP11_KEY2_SCAN_CASE_SINGLE_LIST", {"GP11_SCAN_CASE_SINGLE_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP11_KEY3_SCAN_CASE_SINGLE_LIST", {"GP11_SCAN_CASE_SINGLE_LIST_NODE3"}, &uint64_ret);
  s = db.LPush("GP11_KEY4_SCAN_CASE_SINGLE_LIST", {"GP11_SCAN_CASE_SINGLE_LIST_NODE4"}, &uint64_ret);
  s = db.LPush("GP11_KEY5_SCAN_CASE_SINGLE_LIST", {"GP11_SCAN_CASE_SINGLE_LIST_NODE5"}, &uint64_ret);
  s = db.LPush("GP11_KEY6_SCAN_CASE_SINGLE_LIST", {"GP11_SCAN_CASE_SINGLE_LIST_NODE6"}, &uint64_ret);
  delete_keys.emplace_back("GP11_KEY1_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP11_KEY2_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP11_KEY3_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP11_KEY4_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP11_KEY5_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP11_KEY6_SCAN_CASE_SINGLE_LIST");

  // ZSet
  s = db.ZAdd("GP11_KEY1_SCAN_CASE_SINGLE_ZSET", {{1, "GP11_SCAN_CASE_SINGLE_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP11_KEY2_SCAN_CASE_SINGLE_ZSET", {{1, "GP11_SCAN_CASE_SINGLE_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP11_KEY3_SCAN_CASE_SINGLE_ZSET", {{1, "GP11_SCAN_CASE_SINGLE_LIST_MEMBER3"}}, &int32_ret);
  s = db.ZAdd("GP11_KEY4_SCAN_CASE_SINGLE_ZSET", {{1, "GP11_SCAN_CASE_SINGLE_LIST_MEMBER4"}}, &int32_ret);
  s = db.ZAdd("GP11_KEY5_SCAN_CASE_SINGLE_ZSET", {{1, "GP11_SCAN_CASE_SINGLE_LIST_MEMBER5"}}, &int32_ret);
  s = db.ZAdd("GP11_KEY6_SCAN_CASE_SINGLE_ZSET", {{1, "GP11_SCAN_CASE_SINGLE_LIST_MEMBER6"}}, &int32_ret);
  delete_keys.emplace_back("GP11_KEY1_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP11_KEY2_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP11_KEY3_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP11_KEY4_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP11_KEY5_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP11_KEY6_SCAN_CASE_SINGLE_ZSET");

  keys.clear();
  cursor = 0;
  cursor = db.Scan(DataType::kZSets, cursor, "*", 6, &keys);
  ASSERT_EQ(cursor, 0);
  ASSERT_EQ(keys.size(), 6);
  ASSERT_EQ(keys[0], "GP11_KEY1_SCAN_CASE_SINGLE_ZSET");
  ASSERT_EQ(keys[1], "GP11_KEY2_SCAN_CASE_SINGLE_ZSET");
  ASSERT_EQ(keys[2], "GP11_KEY3_SCAN_CASE_SINGLE_ZSET");
  ASSERT_EQ(keys[3], "GP11_KEY4_SCAN_CASE_SINGLE_ZSET");
  ASSERT_EQ(keys[4], "GP11_KEY5_SCAN_CASE_SINGLE_ZSET");
  ASSERT_EQ(keys[5], "GP11_KEY6_SCAN_CASE_SINGLE_ZSET");

  del_num = db.Del(delete_keys);
  ASSERT_EQ(del_num, 30);
  sleep(2);
  db.Compact(DataType::kAll, true);

  // ***************** Group 12 Test *****************
  delete_keys.clear();
  // String
  s = db.Set("GP12_KEY1_SCAN_CASE_SINGLE_STRING", "GP12_SCAN_CASE_SINGLE_STRING_VALUE1");
  s = db.Set("GP12_KEY2_SCAN_CASE_SINGLE_STRING", "GP12_SCAN_CASE_SINGLE_STRING_VALUE2");
  s = db.Set("GP12_KEY3_SCAN_CASE_SINGLE_STRING", "GP12_SCAN_CASE_SINGLE_STRING_VALUE3");
  s = db.Set("GP12_KEY4_SCAN_CASE_SINGLE_STRING", "GP12_SCAN_CASE_SINGLE_STRING_VALUE4");
  s = db.Set("GP12_KEY5_SCAN_CASE_SINGLE_STRING", "GP12_SCAN_CASE_SINGLE_STRING_VALUE5");
  s = db.Set("GP12_KEY6_SCAN_CASE_SINGLE_STRING", "GP12_SCAN_CASE_SINGLE_STRING_VALUE6");
  delete_keys.emplace_back("GP12_KEY1_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP12_KEY2_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP12_KEY3_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP12_KEY4_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP12_KEY5_SCAN_CASE_SINGLE_STRING");
  delete_keys.emplace_back("GP12_KEY6_SCAN_CASE_SINGLE_STRING");

  // Hash
  s = db.HSet("GP12_KEY1_SCAN_CASE_SINGLE_HASH", "GP12_SCAN_CASE_SINGLE_HASH_FIELD1",
              "GP12_SCAN_CASE_SINGLE_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP12_KEY2_SCAN_CASE_SINGLE_HASH", "GP12_SCAN_CASE_SINGLE_HASH_FIELD2",
              "GP12_SCAN_CASE_SINGLE_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP12_KEY3_SCAN_CASE_SINGLE_HASH", "GP12_SCAN_CASE_SINGLE_HASH_FIELD3",
              "GP12_SCAN_CASE_SINGLE_HASH_VALUE3", &int32_ret);
  s = db.HSet("GP12_KEY4_SCAN_CASE_SINGLE_HASH", "GP12_SCAN_CASE_SINGLE_HASH_FIELD4",
              "GP12_SCAN_CASE_SINGLE_HASH_VALUE4", &int32_ret);
  s = db.HSet("GP12_KEY5_SCAN_CASE_SINGLE_HASH", "GP12_SCAN_CASE_SINGLE_HASH_FIELD5",
              "GP12_SCAN_CASE_SINGLE_HASH_VALUE5", &int32_ret);
  s = db.HSet("GP12_KEY6_SCAN_CASE_SINGLE_HASH", "GP12_SCAN_CASE_SINGLE_HASH_FIELD6",
              "GP12_SCAN_CASE_SINGLE_HASH_VALUE6", &int32_ret);
  delete_keys.emplace_back("GP12_KEY1_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP12_KEY2_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP12_KEY3_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP12_KEY4_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP12_KEY5_SCAN_CASE_SINGLE_HASH");
  delete_keys.emplace_back("GP12_KEY6_SCAN_CASE_SINGLE_HASH");

  // Set
  s = db.SAdd("GP12_KEY1_SCAN_CASE_SINGLE_SET", {"GP12_SCAN_CASE_SINGLE_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP12_KEY2_SCAN_CASE_SINGLE_SET", {"GP12_SCAN_CASE_SINGLE_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP12_KEY3_SCAN_CASE_SINGLE_SET", {"GP12_SCAN_CASE_SINGLE_SET_MEMBER3"}, &int32_ret);
  s = db.SAdd("GP12_KEY4_SCAN_CASE_SINGLE_SET", {"GP12_SCAN_CASE_SINGLE_SET_MEMBER4"}, &int32_ret);
  s = db.SAdd("GP12_KEY5_SCAN_CASE_SINGLE_SET", {"GP12_SCAN_CASE_SINGLE_SET_MEMBER5"}, &int32_ret);
  s = db.SAdd("GP12_KEY6_SCAN_CASE_SINGLE_SET", {"GP12_SCAN_CASE_SINGLE_SET_MEMBER6"}, &int32_ret);
  delete_keys.emplace_back("GP12_KEY1_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP12_KEY2_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP12_KEY3_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP12_KEY4_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP12_KEY5_SCAN_CASE_SINGLE_SET");
  delete_keys.emplace_back("GP12_KEY6_SCAN_CASE_SINGLE_SET");

  // List
  s = db.LPush("GP12_KEY1_SCAN_CASE_SINGLE_LIST", {"GP12_SCAN_CASE_SINGLE_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP12_KEY2_SCAN_CASE_SINGLE_LIST", {"GP12_SCAN_CASE_SINGLE_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP12_KEY3_SCAN_CASE_SINGLE_LIST", {"GP12_SCAN_CASE_SINGLE_LIST_NODE3"}, &uint64_ret);
  s = db.LPush("GP12_KEY4_SCAN_CASE_SINGLE_LIST", {"GP12_SCAN_CASE_SINGLE_LIST_NODE4"}, &uint64_ret);
  s = db.LPush("GP12_KEY5_SCAN_CASE_SINGLE_LIST", {"GP12_SCAN_CASE_SINGLE_LIST_NODE5"}, &uint64_ret);
  s = db.LPush("GP12_KEY6_SCAN_CASE_SINGLE_LIST", {"GP12_SCAN_CASE_SINGLE_LIST_NODE6"}, &uint64_ret);
  delete_keys.emplace_back("GP12_KEY1_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP12_KEY2_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP12_KEY3_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP12_KEY4_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP12_KEY5_SCAN_CASE_SINGLE_LIST");
  delete_keys.emplace_back("GP12_KEY6_SCAN_CASE_SINGLE_LIST");

  // ZSet
  s = db.ZAdd("GP12_KEY1_SCAN_CASE_SINGLE_ZSET", {{1, "GP12_SCAN_CASE_SINGLE_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP12_KEY2_SCAN_CASE_SINGLE_ZSET", {{1, "GP12_SCAN_CASE_SINGLE_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP12_KEY3_SCAN_CASE_SINGLE_ZSET", {{1, "GP12_SCAN_CASE_SINGLE_LIST_MEMBER3"}}, &int32_ret);
  s = db.ZAdd("GP12_KEY4_SCAN_CASE_SINGLE_ZSET", {{1, "GP12_SCAN_CASE_SINGLE_LIST_MEMBER4"}}, &int32_ret);
  s = db.ZAdd("GP12_KEY5_SCAN_CASE_SINGLE_ZSET", {{1, "GP12_SCAN_CASE_SINGLE_LIST_MEMBER5"}}, &int32_ret);
  s = db.ZAdd("GP12_KEY6_SCAN_CASE_SINGLE_ZSET", {{1, "GP12_SCAN_CASE_SINGLE_LIST_MEMBER6"}}, &int32_ret);
  delete_keys.emplace_back("GP12_KEY1_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP12_KEY2_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP12_KEY3_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP12_KEY4_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP12_KEY5_SCAN_CASE_SINGLE_ZSET");
  delete_keys.emplace_back("GP12_KEY6_SCAN_CASE_SINGLE_ZSET");

  keys.clear();
  cursor = 0;
  cursor = db.Scan(DataType::kZSets, cursor, "*", 10, &keys);
  ASSERT_EQ(cursor, 0);
  ASSERT_EQ(keys.size(), 6);
  ASSERT_EQ(keys[0], "GP12_KEY1_SCAN_CASE_SINGLE_ZSET");
  ASSERT_EQ(keys[1], "GP12_KEY2_SCAN_CASE_SINGLE_ZSET");
  ASSERT_EQ(keys[2], "GP12_KEY3_SCAN_CASE_SINGLE_ZSET");
  ASSERT_EQ(keys[3], "GP12_KEY4_SCAN_CASE_SINGLE_ZSET");
  ASSERT_EQ(keys[4], "GP12_KEY5_SCAN_CASE_SINGLE_ZSET");
  ASSERT_EQ(keys[5], "GP12_KEY6_SCAN_CASE_SINGLE_ZSET");

  del_num = db.Del(delete_keys);
  ASSERT_EQ(del_num, 30);
  sleep(2);
  db.Compact(DataType::kAll, true);
}

// Expire
TEST_F(KeysTest, ExpireTest) {
  std::string value;
  std::map<storage::DataType, Status> type_status;
  int32_t ret;

  // ***************** Group 1 Test *****************
  // Strings
  s = db.Set("GP1_EXPIRE_KEY", "VALUE");
  ASSERT_TRUE(s.ok());
  // Hashes
  s = db.HSet("GP1_EXPIRE_HASH_KEY", "FIELD", "VALUE", &ret);
  ASSERT_TRUE(s.ok());

  // Sets
  s = db.SAdd("GP1_EXPIRE_SET_KEY", {"MEMBER"}, &ret);
  ASSERT_TRUE(s.ok());

  // Lists
  uint64_t llen;
  s = db.RPush("GP1_EXPIRE_LIST_KEY", {"NODE"}, &llen);
  ASSERT_TRUE(s.ok());

  // Zsets
  s = db.ZAdd("GP1_EXPIRE_ZSET_KEY", {{1, "MEMBER"}}, &ret);
  ASSERT_TRUE(s.ok());

  ret = db.Expire("GP1_EXPIRE_KEY", 1);
  ASSERT_EQ(ret, 1);
  ret = db.Expire("GP1_EXPIRE_HASH_KEY", 1);
  ASSERT_EQ(ret, 1);
  ret = db.Expire("GP1_EXPIRE_SET_KEY", 1);
  ASSERT_EQ(ret, 1);
  ret = db.Expire("GP1_EXPIRE_LIST_KEY", 1);
  ASSERT_EQ(ret, 1);
  ret = db.Expire("GP1_EXPIRE_ZSET_KEY", 1);
  ASSERT_EQ(ret, 1);
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  // Strings
  s = db.Get("GP1_EXPIRE_KEY", &value);
  ASSERT_TRUE(s.IsNotFound());
  // Hashes
  s = db.HGet("GP1_EXPIRE_HASH_KEY", "EXPIRE_FIELD", &value);
  ASSERT_TRUE(s.IsNotFound());

  // Sets
  s = db.SCard("GP1_EXPIRE_SET_KEY", &ret);
  ASSERT_TRUE(s.IsNotFound());

  // Lists
  s = db.LLen("GP1_EXPIRE_LIST_KEY", &llen);
  ASSERT_TRUE(s.IsNotFound());

  // ZSets
  s = db.ZCard("GP1_EXPIRE_ZSET_KEY", &ret);
  ASSERT_TRUE(s.IsNotFound());

  // ***************** Group 2 Test *****************
  // Strings
  s = db.Set("GP2_EXPIRE_STRING_KEY", "VALUE");
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(make_expired(&db, "GP2_EXPIRE_STRING_KEY"));

  type_status.clear();
  ret = db.Expire("GP2_EXPIRE_STRING_KEY", 1);
  ASSERT_EQ(ret, 0);
  // Hashes
  s = db.HSet("GP2_EXPIRE_HASHES_KEY", "FIELD", "VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(make_expired(&db, "GP2_EXPIRE_HASHES_KEY"));

  type_status.clear();
  ret = db.Expire("GP2_EXPIRE_HASHES_KEY", 1);
  ASSERT_EQ(ret, 0);

  // Sets
  s = db.SAdd("GP2_EXPIRE_SETS_KEY", {"MEMBER"}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(make_expired(&db, "GP2_EXPIRE_SETS_KEY"));

  type_status.clear();
  ret = db.Expire("GP2_EXPIRE_SETS_KEY", 1);
  ASSERT_EQ(ret, 0);

  // Lists
  s = db.RPush("GP2_EXPIRE_LISTS_KEY", {"NODE"}, &llen);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(make_expired(&db, "GP2_EXPIRE_LISTS_KEY"));

  type_status.clear();
  ret = db.Expire("GP2_EXPIRE_LISTS_KEY", 1);
  ASSERT_EQ(ret, 0);

  // Zsets
  s = db.ZAdd("GP2_EXPIRE_ZSETS_KEY", {{1, "MEMBER"}}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(make_expired(&db, "GP2_EXPIRE_ZSETS_KEY"));

  type_status.clear();
  ret = db.Expire("GP2_EXPIRE_ZSETS_KEY", 1);
  ASSERT_EQ(ret, 0);

  // ***************** Group 3 Test *****************
  // Strings
  s = db.Set("GP3_EXPIRE_STRING_KEY", "VALUE");
  ASSERT_TRUE(s.ok());
  int64_t res = 0;
  res = db.Del({"GP3_EXPIRE_STRING_KEY"});
  ASSERT_EQ(res, 1);

  type_status.clear();
  ret = db.Expire("GP3_EXPIRE_STRING_KEY", 1);
  ASSERT_EQ(ret, 0);
  // Hashes
  s = db.HSet("GP3_EXPIRE_HASHES_KEY", "FIELD", "VALUE", &ret);
  ASSERT_TRUE(s.ok());
  s = db.HDel("GP3_EXPIRE_HASHES_KEY", {"FIELD"}, &ret);
  ASSERT_TRUE(s.ok());
  type_status.clear();
  ret = db.Expire("GP3_EXPIRE_HASHES_KEY", 1);
  ASSERT_EQ(ret, 0);

  // Sets
  s = db.SAdd("GP3_EXPIRE_SETS_KEY", {"MEMBER"}, &ret);
  ASSERT_TRUE(s.ok());
  s = db.SRem("GP3_EXPIRE_SETS_KEY", {"MEMBER"}, &ret);
  ASSERT_TRUE(s.ok());

  type_status.clear();
  ret = db.Expire("GP3_EXPIRE_SETS_KEY", 1);
  ASSERT_EQ(ret, 0);

  // Lists
  s = db.RPush("GP3_EXPIRE_LISTS_KEY", {"NODE"}, &llen);
  ASSERT_TRUE(s.ok());
  std::vector<std::string> elements;
  s = db.LPop("GP3_EXPIRE_LISTS_KEY", 1,&elements);
  ASSERT_TRUE(s.ok());

  type_status.clear();
  ret = db.Expire("GP3_EXPIRE_LISTS_KEY", 1);
  ret = db.Expire("GP3_EXPIRE_LISTS_KEY", 1);
  LOG(WARNING) << "ret: " << ret;
  for (const auto& ts : type_status) {
    LOG(WARNING) << "type: " << storage::DataTypeStrings[static_cast<int>(ts.first)] << " status: " << ts.second.ToString();
  }
  ASSERT_EQ(ret, 0);

  // Zsets
  s = db.ZAdd("GP3_EXPIRE_ZSETS_KEY", {{1, "MEMBER"}}, &ret);
  ASSERT_TRUE(s.ok());
  s = db.ZRem("GP3_EXPIRE_ZSETS_KEY", {"MEMBER"}, &ret);
  ASSERT_TRUE(s.ok());

  type_status.clear();
  ret = db.Expire("GP3_EXPIRE_ZSETS_KEY", 1);
  ASSERT_EQ(ret, 0);
}

// Del
TEST_F(KeysTest, DelTest) {
  int32_t ret;
  std::string value;
  std::map<storage::DataType, Status> type_status;
  std::vector<std::string> keys{"DEL_KEY"};

  // Strings
  s = db.Set("DEL_KEY", "VALUE");
  ASSERT_TRUE(s.ok());
  ret = db.Del(keys);
  ASSERT_EQ(ret, 1);

  // Strings
  s = db.Get("DEL_KEY", &value);
  ASSERT_TRUE(s.IsNotFound());
}

// Exists
TEST_F(KeysTest, ExistsTest) {
  int32_t ret;
  uint64_t llen;
  std::map<storage::DataType, Status> type_status;
  std::vector<std::string> keys{"EXISTS_KEY"};

  // Strings
  s = db.Set("EXISTS_KEY", "VALUE");
  ASSERT_TRUE(s.ok());
  ret = db.Exists(keys);
  ASSERT_EQ(ret, 1);
}

// Expireat
TEST_F(KeysTest, ExpireatTest) {
  // If the key does not exist
  std::map<storage::DataType, Status> type_status;
  int32_t ret = db.Expireat("EXPIREAT_KEY", 0);
  ASSERT_EQ(ret, 0);

  // Strings
  std::string value;
  s = db.Set("EXPIREAT_KEY", "VALUE");
  ASSERT_TRUE(s.ok());

  pstd::TimeType unix_time = pstd::NowMillis();
  int64_t timestamp = unix_time + 1;
  ret = db.Expireat("EXPIREAT_KEY", timestamp);
  ASSERT_EQ(ret, 1);

  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  // Strings
  s = db.Get("EXPIREAT_KEY", &value);
  ASSERT_TRUE(s.IsNotFound());

  // Expireat key 0
  s = db.Set("EXPIREAT_KEY", "VALUE");
  ASSERT_TRUE(s.ok());

  ret = db.Expireat("EXPIREAT_KEY", 0);
  ASSERT_EQ(ret, 1);

  // Strings
  s = db.Get("EXPIREAT_KEY", &value);
  ASSERT_TRUE(s.IsNotFound());
}

// Persist
TEST_F(KeysTest, PersistTest) {
  // If the key does not exist
  std::map<storage::DataType, Status> type_status;
  int32_t ret = db.Persist("EXPIREAT_KEY");
  ASSERT_EQ(ret, 0);

  // If the key does not have an associated timeout
  // Strings
  std::string value;
  s = db.Set("PERSIST_KEY", "VALUE");
  ASSERT_TRUE(s.ok());

  ret = db.Persist("PERSIST_KEY");
  ASSERT_EQ(ret, 0);

  // If the timeout was set
  ret = db.Expire("PERSIST_KEY", 1000);
  ASSERT_EQ(ret, 1);
  ret = db.Persist("PERSIST_KEY");
  ASSERT_EQ(ret, 1);

  int64_t ttl_ret;
  ttl_ret = db.TTL("PERSIST_KEY");
}

// TTL
TEST_F(KeysTest, TTLTest) {
  // If the key does not exist
  std::map<storage::DataType, Status> type_status;
  int64_t ttl_ret;
  ttl_ret = db.TTL("TTL_KEY");

  // If the key does not have an associated timeout
  // Strings
  std::string value;
  int32_t ret = 0;
  s = db.Set("TTL_KEY", "VALUE");
  ASSERT_TRUE(s.ok());

  ttl_ret = db.TTL("TTL_KEY");

  // If the timeout was set
  ret = db.Expire("TTL_KEY", 10);
  ASSERT_EQ(ret, 1);
  ttl_ret = db.TTL("TTL_KEY");
}


int main(int argc, char** argv) {
  if (!pstd::FileExists("./log")) {
    pstd::CreatePath("./log");
  }
  FLAGS_log_dir = "./log";
  FLAGS_minloglevel = 0;
  FLAGS_max_log_size = 1800;
  FLAGS_logbufsecs = 0;
  ::google::InitGoogleLogging("keys_test");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
