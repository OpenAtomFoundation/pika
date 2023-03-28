//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <gtest/gtest.h>
#include <thread>
#include <iostream>

#include "storage/storage.h"

using namespace storage;

class KeysTest : public ::testing::Test {
 public:
  KeysTest() {
    std::string path = "./db/keys";
    if (access(path.c_str(), F_OK)) {
      mkdir(path.c_str(), 0755);
    }
    bw_options.options.create_if_missing = true;
    s = db.Open(bw_options, path);
  }
  virtual ~KeysTest() { }

  static void SetUpTestCase() { }
  static void TearDownTestCase() { }

  BlackwidowOptions bw_options;
  storage::BlackWidow db;
  storage::Status s;
};

static bool make_expired(storage::BlackWidow *const db,
                         const Slice& key) {
  std::map<storage::DataType, rocksdb::Status> type_status;
  int ret = db->Expire(key, 1, &type_status);
  if (!ret || !type_status[storage::DataType::kStrings].ok()) {
    return false;
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  return true;
}

static bool set_timeout(storage::BlackWidow *const db,
                        const Slice& key, int32_t ttl) {
  std::map<storage::DataType, rocksdb::Status> type_status;
  int ret = db->Expire(key, ttl, &type_status);
  if (!ret || !type_status[storage::DataType::kStrings].ok()) {
   return false;
  }
  return true;
}

static bool key_value_match(const std::vector<KeyValue>& key_value_out,
                            const std::vector<KeyValue>& expect_key_value) {
  if (key_value_out.size() != expect_key_value.size()) {
    return false;
  }
  for (int32_t idx = 0; idx < key_value_out.size(); ++idx) {
    if (key_value_out[idx].key != expect_key_value[idx].key
      || key_value_out[idx].value != expect_key_value[idx].value) {
      return false;
    }
  }
  return true;
}

static bool key_match(const std::vector<std::string>& keys_out,
                      const std::vector<std::string>& expect_keys) {
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
TEST_F(KeysTest, PKScanRangeTest) {
  int32_t ret;
  uint64_t ret_u64;
  std::string next_key;
  std::vector<std::string> keys_del;
  std::vector<std::string> keys_out;
  std::vector<std::string> expect_keys;
  std::map<DataType, Status> type_status;
  std::vector<storage::KeyValue> kvs_out;
  std::vector<storage::KeyValue> expect_kvs;
  std::vector<storage::KeyValue> kvs {{"PKSCANRANGE_A", "VALUE"},
                                         {"PKSCANRANGE_C", "VALUE"},
                                         {"PKSCANRANGE_E", "VALUE"},
                                         {"PKSCANRANGE_G", "VALUE"},
                                         {"PKSCANRANGE_I", "VALUE"},
                                         {"PKSCANRANGE_K", "VALUE"},
                                         {"PKSCANRANGE_M", "VALUE"},
                                         {"PKSCANRANGE_O", "VALUE"},
                                         {"PKSCANRANGE_Q", "VALUE"},
                                         {"PKSCANRANGE_S", "VALUE"}};
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
  for (const auto& kv : kvs) {
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
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKScanRange(DataType::kSets, "PKSCANRANGE_B", "", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 9; ++idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKScanRange(DataType::kSets, "", "PKSCANRANGE_R", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 0; idx <= 8; ++idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKScanRange(DataType::kSets, "PKSCANRANGE_D", "PKSCANRANGE_P", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 2; idx <= 7; ++idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKScanRange(DataType::kSets, "PKSCANRANGE_C", "PKSCANRANGE_Q", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 8; ++idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKScanRange(DataType::kSets, "PKSCANRANGE_I", "PKSCANRANGE_K", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 4; idx <= 5; ++idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKScanRange(DataType::kSets, "PKSCANRANGE_I", "PKSCANRANGE_I", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 4; idx <= 4; ++idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKScanRange(DataType::kSets, "PKSCANRANGE_K", "PKSCANRANGE_I", "*", 10, &keys_out, &kvs_out, &next_key);
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
  s = db.PKScanRange(DataType::kSets, "PKSCANRANGE_C", "PKSCANRANGE_Q", "*", 5, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 5; ++idx) {
    expect_keys.push_back(kvs[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKSCANRANGE_M");


  // ************************** Group 10 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^           ^                       ^     ^
  //         key_start   expire              next_key  key_end
  ASSERT_TRUE(make_expired(&db, "PKSCANRANGE_G"));
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kSets, "PKSCANRANGE_C", "PKSCANRANGE_Q", "*", 5, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 6; ++idx) {
    if (idx != 3) {
      expect_keys.push_back(kvs[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKSCANRANGE_O");


  // ************************** Group 11 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^           ^     ^                       ^
  //         key_start   expire next_key               key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kSets, "PKSCANRANGE_C", "PKSCANRANGE_Q", "*", 2, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 2; ++idx) {
    if (idx != 3) {
      expect_keys.push_back(kvs[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKSCANRANGE_I");


  // ************************** Group 12 Test **************************
  //      0     1     2     3      4        5     6     7     8     9
  //      A     C     E     G      I        K     M     O     Q     S
  //            ^           ^      ^        ^                 ^
  //         key_start   expire deleted next_key           key_end
  keys_out.clear();
  expect_keys.clear();
  db.Del({"PKSCANRANGE_I"}, &type_status);
  s = db.PKScanRange(DataType::kSets, "PKSCANRANGE_C", "PKSCANRANGE_Q", "*", 2, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 2; ++idx) {
    if (idx != 3) {
      expect_keys.push_back(kvs[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKSCANRANGE_K");


  //=============================== Hashes ===============================
  for (const auto& kv : kvs) {
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
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKScanRange(DataType::kHashes, "PKSCANRANGE_B", "", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 9; ++idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKScanRange(DataType::kHashes, "", "PKSCANRANGE_R", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 0; idx <= 8; ++idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKScanRange(DataType::kHashes, "PKSCANRANGE_D", "PKSCANRANGE_P", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 2; idx <= 7; ++idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKScanRange(DataType::kHashes, "PKSCANRANGE_C", "PKSCANRANGE_Q", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 8; ++idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKScanRange(DataType::kHashes, "PKSCANRANGE_I", "PKSCANRANGE_K", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 4; idx <= 5; ++idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKScanRange(DataType::kHashes, "PKSCANRANGE_I", "PKSCANRANGE_I", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 4; idx <= 4; ++idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKScanRange(DataType::kHashes, "PKSCANRANGE_K", "PKSCANRANGE_I", "*", 10, &keys_out, &kvs_out, &next_key);
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
  s = db.PKScanRange(DataType::kHashes, "PKSCANRANGE_C", "PKSCANRANGE_Q", "*", 5, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 5; ++idx) {
    expect_keys.push_back(kvs[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKSCANRANGE_M");


  // ************************** Group 10 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^           ^                       ^     ^
  //         key_start   expire              next_key  key_end
  ASSERT_TRUE(make_expired(&db, "PKSCANRANGE_G"));
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kHashes, "PKSCANRANGE_C", "PKSCANRANGE_Q", "*", 5, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 6; ++idx) {
    if (idx != 3) {
      expect_keys.push_back(kvs[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKSCANRANGE_O");


  // ************************** Group 11 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^           ^     ^                       ^
  //         key_start   expire next_key               key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kHashes, "PKSCANRANGE_C", "PKSCANRANGE_Q", "*", 2, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 2; ++idx) {
    expect_keys.push_back(kvs[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKSCANRANGE_I");


  // ************************** Group 12 Test **************************
  //      0     1     2     3      4        5     6     7     8     9
  //      A     C     E     G      I        K     M     O     Q     S
  //            ^           ^      ^        ^                 ^
  //         key_start   expire deleted next_key           key_end
  keys_out.clear();
  expect_keys.clear();
  db.Del({"PKSCANRANGE_I"}, &type_status);
  s = db.PKScanRange(DataType::kHashes, "PKSCANRANGE_C", "PKSCANRANGE_Q", "*", 2, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 2; ++idx) {
    expect_keys.push_back(kvs[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKSCANRANGE_K");


  //=============================== ZSets ===============================
  for (const auto& kv : kvs) {
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
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKScanRange(DataType::kZSets, "PKSCANRANGE_B", "", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 9; ++idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKScanRange(DataType::kZSets, "", "PKSCANRANGE_R", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 0; idx <= 8; ++idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKScanRange(DataType::kZSets, "PKSCANRANGE_D", "PKSCANRANGE_P", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 2; idx <= 7; ++idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKScanRange(DataType::kZSets, "PKSCANRANGE_C", "PKSCANRANGE_Q", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 8; ++idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKScanRange(DataType::kZSets, "PKSCANRANGE_I", "PKSCANRANGE_K", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 4; idx <= 5; ++idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKScanRange(DataType::kZSets, "PKSCANRANGE_I", "PKSCANRANGE_I", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 4; idx <= 4; ++idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKScanRange(DataType::kZSets, "PKSCANRANGE_K", "PKSCANRANGE_I", "*", 10, &keys_out, &kvs_out, &next_key);
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
  s = db.PKScanRange(DataType::kZSets, "PKSCANRANGE_C", "PKSCANRANGE_Q", "*", 5, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 5; ++idx) {
    expect_keys.push_back(kvs[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKSCANRANGE_M");


  // ************************** Group 10 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^           ^                       ^     ^
  //         key_start   expire              next_key  key_end
  ASSERT_TRUE(make_expired(&db, "PKSCANRANGE_G"));
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kZSets, "PKSCANRANGE_C", "PKSCANRANGE_Q", "*", 5, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 6; ++idx) {
    if (idx != 3) {
      expect_keys.push_back(kvs[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKSCANRANGE_O");


  // ************************** Group 11 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^           ^     ^                       ^
  //         key_start   expire next_key               key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kZSets, "PKSCANRANGE_C", "PKSCANRANGE_Q", "*", 2, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 2; ++idx) {
    expect_keys.push_back(kvs[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKSCANRANGE_I");


  // ************************** Group 12 Test **************************
  //      0     1     2     3      4        5     6     7     8     9
  //      A     C     E     G      I        K     M     O     Q     S
  //            ^           ^      ^        ^                 ^
  //         key_start   expire deleted next_key           key_end
  keys_out.clear();
  expect_keys.clear();
  db.Del({"PKSCANRANGE_I"}, &type_status);
  s = db.PKScanRange(DataType::kZSets, "PKSCANRANGE_C", "PKSCANRANGE_Q", "*", 2, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 2; ++idx) {
    if (idx != 3) {
      expect_keys.push_back(kvs[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKSCANRANGE_K");


  //=============================== Lists  ===============================
  for (const auto& kv : kvs) {
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
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKScanRange(DataType::kLists, "PKSCANRANGE_B", "", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 9; ++idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKScanRange(DataType::kLists, "", "PKSCANRANGE_R", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 0; idx <= 8; ++idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKScanRange(DataType::kLists, "PKSCANRANGE_D", "PKSCANRANGE_P", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 2; idx <= 7; ++idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKScanRange(DataType::kLists, "PKSCANRANGE_C", "PKSCANRANGE_Q", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 8; ++idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKScanRange(DataType::kLists, "PKSCANRANGE_I", "PKSCANRANGE_K", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 4; idx <= 5; ++idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKScanRange(DataType::kLists, "PKSCANRANGE_I", "PKSCANRANGE_I", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 4; idx <= 4; ++idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKScanRange(DataType::kLists, "PKSCANRANGE_K", "PKSCANRANGE_I", "*", 10, &keys_out, &kvs_out, &next_key);
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
  s = db.PKScanRange(DataType::kLists, "PKSCANRANGE_C", "PKSCANRANGE_Q", "*", 5, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 5; ++idx) {
    expect_keys.push_back(kvs[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKSCANRANGE_M");


  // ************************** Group 10 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^           ^                       ^     ^
  //         key_start   expire              next_key  key_end
  ASSERT_TRUE(make_expired(&db, "PKSCANRANGE_G"));
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kLists, "PKSCANRANGE_C", "PKSCANRANGE_Q", "*", 5, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 6; ++idx) {
    if (idx != 3) {
      expect_keys.push_back(kvs[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKSCANRANGE_O");


  // ************************** Group 11 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^           ^     ^                       ^
  //         key_start   expire next_key               key_end
  keys_out.clear();
  expect_keys.clear();
  s = db.PKScanRange(DataType::kLists, "PKSCANRANGE_C", "PKSCANRANGE_Q", "*", 2, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 2; ++idx) {
    expect_keys.push_back(kvs[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKSCANRANGE_I");


  // ************************** Group 12 Test **************************
  //      0     1     2     3      4        5     6     7     8     9
  //      A     C     E     G      I        K     M     O     Q     S
  //            ^           ^      ^        ^                 ^
  //         key_start   expire deleted next_key           key_end
  keys_out.clear();
  expect_keys.clear();
  db.Del({"PKSCANRANGE_I"}, &type_status);
  s = db.PKScanRange(DataType::kLists, "PKSCANRANGE_C", "PKSCANRANGE_Q", "*", 2, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 2; ++idx) {
    if (idx != 3) {
      expect_keys.push_back(kvs[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKSCANRANGE_K");

  type_status.clear();
  db.Del(keys_del, &type_status);
  sleep(2);
  db.Compact(DataType::kAll, true);
}

// PKRScanRange
// Note: This test needs to execute at first because all of the data is
// predetermined.
TEST_F(KeysTest, PKRScanRangeTest) {
  int32_t ret;
  uint64_t ret_u64;
  std::string next_key;
  std::vector<std::string> keys_del;
  std::vector<std::string> keys_out;
  std::vector<std::string> expect_keys;
  std::map<DataType, Status> type_status;
  std::vector<storage::KeyValue> kvs_out;
  std::vector<storage::KeyValue> expect_kvs;
  std::vector<storage::KeyValue> kvs {{"PKRSCANRANGE_A", "VALUE"},
                                         {"PKRSCANRANGE_C", "VALUE"},
                                         {"PKRSCANRANGE_E", "VALUE"},
                                         {"PKRSCANRANGE_G", "VALUE"},
                                         {"PKRSCANRANGE_I", "VALUE"},
                                         {"PKRSCANRANGE_K", "VALUE"},
                                         {"PKRSCANRANGE_M", "VALUE"},
                                         {"PKRSCANRANGE_O", "VALUE"},
                                         {"PKRSCANRANGE_Q", "VALUE"},
                                         {"PKRSCANRANGE_S", "VALUE"}};
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
  //key_end/next_key                                             key_start
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
  //key_end/next_key                                         key_start
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
  for (const auto& kv : kvs) {
    s = db.SAdd(kv.key, {"MEMBER"}, &ret);
  }

  // ************************** Group 1 Test **************************
  //         0     1     2     3     4     5     6     7     8     9
  //         A     C     E     G     I     K     M     O     Q     S
  //       ^                                                         ^
  //key_end/next_key                                             key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kSets, "", "", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 9; idx >= 0; --idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKRScanRange(DataType::kSets, "", "PKRSCANRANGE_B", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 9; idx >= 1; --idx) {
    expect_keys.push_back(kvs[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");


  // ************************** Group 3 Test **************************
  //         0     1     2     3     4     5     6     7     8     9
  //         A     C     E     G     I     K     M     O     Q     S
  //       ^                                                    ^
  //key_end/next_key                                         key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kSets, "PKRSCANRANGE_R", "", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 0; --idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKRScanRange(DataType::kSets, "PKRSCANRANGE_P", "PKRSCANRANGE_D", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 7; idx >= 2; --idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKRScanRange(DataType::kSets, "PKRSCANRANGE_Q", "PKRSCANRANGE_C", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 1; --idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKRScanRange(DataType::kSets, "PKRSCANRANGE_K", "PKRSCANRANGE_I", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 5; idx >= 4; --idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKRScanRange(DataType::kSets, "PKRSCANRANGE_I", "PKRSCANRANGE_I", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 4; idx >= 4; --idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKRScanRange(DataType::kSets, "PKRSCANRANGE_I", "PKRSCANRANGE_K", "*", 10, &keys_out, &kvs_out, &next_key);
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
  s = db.PKRScanRange(DataType::kSets, "PKRSCANRANGE_Q", "PKRSCANRANGE_C", "*", 5, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 4; --idx) {
    expect_keys.push_back(kvs[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_G");


  // ************************** Group 10 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^     ^                       ^           ^
  //         key_end  next_key              expire     key_start
  keys_out.clear();
  expect_keys.clear();
  ASSERT_TRUE(make_expired(&db, "PKRSCANRANGE_M"));
  s = db.PKRScanRange(DataType::kSets, "PKRSCANRANGE_Q", "PKRSCANRANGE_C", "*", 5, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 3; --idx) {
    if (idx != 6) {
      expect_keys.push_back(kvs[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_E");


  // ************************** Group 11 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^     ^           ^           ^           ^
  //         key_end  next_key  empty       expire     key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.SRem("PKRSCANRANGE_I", {"MEMBER"}, &ret);
  ASSERT_TRUE(s.ok());
  s = db.PKRScanRange(DataType::kSets, "PKRSCANRANGE_Q", "PKRSCANRANGE_C", "*", 4, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 3; --idx) {
    if (idx != 6 && idx != 4) {
      expect_keys.push_back(kvs[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_E");


  // ************************** Group 12 Test **************************
  //      0     1     2     3     4        5        6     7     8     9
  //      A     C     E     G     I        K        M     O     Q     S
  //            ^                 ^        ^        ^           ^
  //         key_end            empty  next_key   expire     key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kSets, "PKRSCANRANGE_Q", "PKRSCANRANGE_C", "*", 2, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 7; --idx) {
    expect_keys.push_back(kvs[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_K");


  // ************************** Group 13 Test **************************
  //      0     1     2       3       4     5     6     7     8     9
  //      A     C     E       G       I     K     M     O     Q     S
  //            ^             ^       ^           ^           ^
  //         key_end      next_key  empty       expire     key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kSets, "PKRSCANRANGE_Q", "PKRSCANRANGE_C", "*", 3, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 5; --idx) {
    if (idx != 6) {
      expect_keys.push_back(kvs[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_G");


  //=============================== Hashes ===============================
  for (const auto& kv : kvs) {
    s = db.HMSet(kv.key, {{"FIELD", "VALUE"}});
  }

  // ************************** Group 1 Test **************************
  //         0     1     2     3     4     5     6     7     8     9
  //         A     C     E     G     I     K     M     O     Q     S
  //       ^                                                         ^
  //key_end/next_key                                             key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kHashes, "", "", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 9; idx >= 0; --idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKRScanRange(DataType::kHashes, "", "PKRSCANRANGE_B", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 9; idx >= 1; --idx) {
    expect_keys.push_back(kvs[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");


  // ************************** Group 3 Test **************************
  //         0     1     2     3     4     5     6     7     8     9
  //         A     C     E     G     I     K     M     O     Q     S
  //       ^                                                    ^
  //key_end/next_key                                         key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kHashes, "PKRSCANRANGE_R", "", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 0; --idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKRScanRange(DataType::kHashes, "PKRSCANRANGE_P", "PKRSCANRANGE_D", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 7; idx >= 2; --idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKRScanRange(DataType::kHashes, "PKRSCANRANGE_Q", "PKRSCANRANGE_C", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 1; --idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKRScanRange(DataType::kHashes, "PKRSCANRANGE_K", "PKRSCANRANGE_I", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 5; idx >= 4; --idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKRScanRange(DataType::kHashes, "PKRSCANRANGE_I", "PKRSCANRANGE_I", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 4; idx >= 4; --idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKRScanRange(DataType::kHashes, "PKRSCANRANGE_I", "PKRSCANRANGE_K", "*", 10, &keys_out, &kvs_out, &next_key);
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
  s = db.PKRScanRange(DataType::kHashes, "PKRSCANRANGE_Q", "PKRSCANRANGE_C", "*", 5, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 4; --idx) {
    expect_keys.push_back(kvs[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_G");


  // ************************** Group 10 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^     ^                       ^           ^
  //         key_end  next_key              expire     key_start
  keys_out.clear();
  expect_keys.clear();
  ASSERT_TRUE(make_expired(&db, "PKRSCANRANGE_M"));
  s = db.PKRScanRange(DataType::kHashes, "PKRSCANRANGE_Q", "PKRSCANRANGE_C", "*", 5, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 3; --idx) {
    if (idx != 6) {
      expect_keys.push_back(kvs[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_E");


  // ************************** Group 11 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^     ^           ^           ^           ^
  //         key_end  next_key  empty       expire     key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.HDel("PKRSCANRANGE_I", {"FIELD"}, &ret);
  ASSERT_TRUE(s.ok());
  s = db.PKRScanRange(DataType::kHashes, "PKRSCANRANGE_Q", "PKRSCANRANGE_C", "*", 4, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 3; --idx) {
    if (idx != 6 && idx != 4) {
      expect_keys.push_back(kvs[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_E");


  // ************************** Group 12 Test **************************
  //      0     1     2     3     4        5        6     7     8     9
  //      A     C     E     G     I        K        M     O     Q     S
  //            ^                 ^        ^        ^           ^
  //         key_end            empty  next_key   expire     key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kHashes, "PKRSCANRANGE_Q", "PKRSCANRANGE_C", "*", 2, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 7; --idx) {
    expect_keys.push_back(kvs[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_K");


  // ************************** Group 13 Test **************************
  //      0     1     2       3       4     5     6     7     8     9
  //      A     C     E       G       I     K     M     O     Q     S
  //            ^             ^       ^           ^           ^
  //         key_end      next_key  empty       expire     key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kHashes, "PKRSCANRANGE_Q", "PKRSCANRANGE_C", "*", 3, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 5; --idx) {
    if (idx != 6) {
      expect_keys.push_back(kvs[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_G");


  //=============================== ZSets ===============================
  for (const auto& kv : kvs) {
    s = db.ZAdd(kv.key, {{1, "MEMBER"}}, &ret);
  }

  // ************************** Group 1 Test **************************
  //         0     1     2     3     4     5     6     7     8     9
  //         A     C     E     G     I     K     M     O     Q     S
  //       ^                                                         ^
  //key_end/next_key                                             key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kZSets, "", "", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 9; idx >= 0; --idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKRScanRange(DataType::kZSets, "", "PKRSCANRANGE_B", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 9; idx >= 1; --idx) {
    expect_keys.push_back(kvs[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");


  // ************************** Group 3 Test **************************
  //         0     1     2     3     4     5     6     7     8     9
  //         A     C     E     G     I     K     M     O     Q     S
  //       ^                                                    ^
  //key_end/next_key                                         key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kZSets, "PKRSCANRANGE_R", "", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 0; --idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKRScanRange(DataType::kZSets, "PKRSCANRANGE_P", "PKRSCANRANGE_D", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 7; idx >= 2; --idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKRScanRange(DataType::kZSets, "PKRSCANRANGE_Q", "PKRSCANRANGE_C", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 1; --idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKRScanRange(DataType::kZSets, "PKRSCANRANGE_K", "PKRSCANRANGE_I", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 5; idx >= 4; --idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKRScanRange(DataType::kZSets, "PKRSCANRANGE_I", "PKRSCANRANGE_I", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 4; idx >= 4; --idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKRScanRange(DataType::kZSets, "PKRSCANRANGE_I", "PKRSCANRANGE_K", "*", 10, &keys_out, &kvs_out, &next_key);
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
  s = db.PKRScanRange(DataType::kZSets, "PKRSCANRANGE_Q", "PKRSCANRANGE_C", "*", 5, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 4; --idx) {
    expect_keys.push_back(kvs[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_G");


  // ************************** Group 10 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^     ^                       ^           ^
  //         key_end  next_key              expire     key_start
  keys_out.clear();
  expect_keys.clear();
  ASSERT_TRUE(make_expired(&db, "PKRSCANRANGE_M"));
  s = db.PKRScanRange(DataType::kZSets, "PKRSCANRANGE_Q", "PKRSCANRANGE_C", "*", 5, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 3; --idx) {
    if (idx != 6) {
      expect_keys.push_back(kvs[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_E");


  // ************************** Group 11 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^     ^           ^           ^           ^
  //         key_end  next_key  empty       expire     key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.ZRem("PKRSCANRANGE_I", {"MEMBER"}, &ret);
  ASSERT_TRUE(s.ok());
  s = db.PKRScanRange(DataType::kZSets, "PKRSCANRANGE_Q", "PKRSCANRANGE_C", "*", 4, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 3; --idx) {
    if (idx != 6 && idx != 4) {
      expect_keys.push_back(kvs[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_E");


  // ************************** Group 12 Test **************************
  //      0     1     2     3     4        5        6     7     8     9
  //      A     C     E     G     I        K        M     O     Q     S
  //            ^                 ^        ^        ^           ^
  //         key_end            empty  next_key   expire     key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kZSets, "PKRSCANRANGE_Q", "PKRSCANRANGE_C", "*", 2, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 7; --idx) {
    expect_keys.push_back(kvs[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_K");


  // ************************** Group 13 Test **************************
  //      0     1     2       3       4     5     6     7     8     9
  //      A     C     E       G       I     K     M     O     Q     S
  //            ^             ^       ^           ^           ^
  //         key_end      next_key  empty       expire     key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kZSets, "PKRSCANRANGE_Q", "PKRSCANRANGE_C", "*", 3, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 5; --idx) {
    if (idx != 6) {
      expect_keys.push_back(kvs[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_G");


  //=============================== Lists ===============================
  for (const auto& kv : kvs) {
    s = db.LPush(kv.key, {"NODE"}, &ret_u64);
  }

  // ************************** Group 1 Test **************************
  //         0     1     2     3     4     5     6     7     8     9
  //         A     C     E     G     I     K     M     O     Q     S
  //       ^                                                         ^
  //key_end/next_key                                             key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kLists, "", "", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 9; idx >= 0; --idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKRScanRange(DataType::kLists, "", "PKRSCANRANGE_B", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 9; idx >= 1; --idx) {
    expect_keys.push_back(kvs[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "");


  // ************************** Group 3 Test **************************
  //         0     1     2     3     4     5     6     7     8     9
  //         A     C     E     G     I     K     M     O     Q     S
  //       ^                                                    ^
  //key_end/next_key                                         key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kLists, "PKRSCANRANGE_R", "", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 0; --idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKRScanRange(DataType::kLists, "PKRSCANRANGE_P", "PKRSCANRANGE_D", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 7; idx >= 2; --idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKRScanRange(DataType::kLists, "PKRSCANRANGE_Q", "PKRSCANRANGE_C", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 1; --idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKRScanRange(DataType::kLists, "PKRSCANRANGE_K", "PKRSCANRANGE_I", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 5; idx >= 4; --idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKRScanRange(DataType::kLists, "PKRSCANRANGE_I", "PKRSCANRANGE_I", "*", 10, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 4; idx >= 4; --idx) {
    expect_keys.push_back(kvs[idx].key);
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
  s = db.PKRScanRange(DataType::kLists, "PKRSCANRANGE_I", "PKRSCANRANGE_K", "*", 10, &keys_out, &kvs_out, &next_key);
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
  s = db.PKRScanRange(DataType::kLists, "PKRSCANRANGE_Q", "PKRSCANRANGE_C", "*", 5, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 4; --idx) {
    expect_keys.push_back(kvs[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_G");


  // ************************** Group 10 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^     ^                       ^           ^
  //         key_end  next_key              expire     key_start
  keys_out.clear();
  expect_keys.clear();
  ASSERT_TRUE(make_expired(&db, "PKRSCANRANGE_M"));
  s = db.PKRScanRange(DataType::kLists, "PKRSCANRANGE_Q", "PKRSCANRANGE_C", "*", 5, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 3; --idx) {
    if (idx != 6) {
      expect_keys.push_back(kvs[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_E");


  // ************************** Group 11 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      A     C     E     G     I     K     M     O     Q     S
  //            ^     ^           ^           ^           ^
  //         key_end  next_key  empty       expire     key_start
  keys_out.clear();
  expect_keys.clear();
  std::string element;
  s = db.LPop("PKRSCANRANGE_I",&element);
  ASSERT_TRUE(s.ok());
  s = db.PKRScanRange(DataType::kLists, "PKRSCANRANGE_Q", "PKRSCANRANGE_C", "*", 4, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 3; --idx) {
    if (idx != 6 && idx != 4) {
      expect_keys.push_back(kvs[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_E");


  // ************************** Group 12 Test **************************
  //      0     1     2     3     4        5        6     7     8     9
  //      A     C     E     G     I        K        M     O     Q     S
  //            ^                 ^        ^        ^           ^
  //         key_end            empty  next_key   expire     key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kLists, "PKRSCANRANGE_Q", "PKRSCANRANGE_C", "*", 2, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 7; --idx) {
    expect_keys.push_back(kvs[idx].key);
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_K");


  // ************************** Group 13 Test **************************
  //      0     1     2       3       4     5     6     7     8     9
  //      A     C     E       G       I     K     M     O     Q     S
  //            ^             ^       ^           ^           ^
  //         key_end      next_key  empty       expire     key_start
  keys_out.clear();
  expect_keys.clear();
  s = db.PKRScanRange(DataType::kLists, "PKRSCANRANGE_Q", "PKRSCANRANGE_C", "*", 3, &keys_out, &kvs_out, &next_key);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 5; --idx) {
    if (idx != 6) {
      expect_keys.push_back(kvs[idx].key);
    }
  }
  ASSERT_TRUE(key_match(keys_out, expect_keys));
  ASSERT_EQ(next_key, "PKRSCANRANGE_G");

  type_status.clear();
  db.Del(keys_del, &type_status);
  sleep(2);
  db.Compact(DataType::kAll, true);
}

TEST_F(KeysTest, PKPatternMatchDel) {
  int32_t ret;
  uint64_t ret64;
  int32_t delete_count;
  std::vector<std::string> keys;
  std::map<DataType, Status> type_status;

  //=============================== Strings ===============================

  // ***************** Group 1 Test *****************
  db.Set("GP1_PKPATTERNMATCHDEL_STRING_KEY1", "VALUE");
  db.Set("GP1_PKPATTERNMATCHDEL_STRING_KEY2", "VALUE");
  db.Set("GP1_PKPATTERNMATCHDEL_STRING_KEY3", "VALUE");
  db.Set("GP1_PKPATTERNMATCHDEL_STRING_KEY4", "VALUE");
  db.Set("GP1_PKPATTERNMATCHDEL_STRING_KEY5", "VALUE");
  db.Set("GP1_PKPATTERNMATCHDEL_STRING_KEY6", "VALUE");
  s = db.PKPatternMatchDel(DataType::kStrings, "*", &delete_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 6);
  keys.clear();
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
  s = db.PKPatternMatchDel(DataType::kStrings, "*", &delete_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 3);
  keys.clear();
  db.Keys(DataType::kStrings, "*", &keys);
  ASSERT_EQ(keys.size(), 0);


  // ***************** Group 3 Test *****************
  db.Set("GP3_PKPATTERNMATCHDEL_STRING_KEY1_0xxx0", "VALUE");
  db.Set("GP3_PKPATTERNMATCHDEL_STRING_KEY2_0ooo0", "VALUE");
  db.Set("GP3_PKPATTERNMATCHDEL_STRING_KEY3_0xxx0", "VALUE");
  db.Set("GP3_PKPATTERNMATCHDEL_STRING_KEY4_0ooo0", "VALUE");
  db.Set("GP3_PKPATTERNMATCHDEL_STRING_KEY5_0xxx0", "VALUE");
  db.Set("GP3_PKPATTERNMATCHDEL_STRING_KEY6_0ooo0", "VALUE");
  s = db.PKPatternMatchDel(DataType::kStrings, "*0xxx0", &delete_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 3);
  keys.clear();
  db.Keys(DataType::kStrings, "*", &keys);
  ASSERT_EQ(keys.size(), 3);
  ASSERT_EQ(keys[0], "GP3_PKPATTERNMATCHDEL_STRING_KEY2_0ooo0");
  ASSERT_EQ(keys[1], "GP3_PKPATTERNMATCHDEL_STRING_KEY4_0ooo0");
  ASSERT_EQ(keys[2], "GP3_PKPATTERNMATCHDEL_STRING_KEY6_0ooo0");
  type_status.clear();
  db.Del(keys, &type_status);


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
  s = db.PKPatternMatchDel(DataType::kStrings, "*0ooo0", &delete_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 3);
  keys.clear();
  db.Keys(DataType::kStrings, "*", &keys);
  ASSERT_EQ(keys.size(), 0);


  // ***************** Group 5 Test *****************
  size_t gp5_total_kv = 23333;
  for (size_t idx = 0; idx < gp5_total_kv; ++idx) {
    db.Set("GP5_PKPATTERNMATCHDEL_STRING_KEY" + std::to_string(idx), "VALUE");
  }
  s = db.PKPatternMatchDel(DataType::kStrings, "*", &delete_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, gp5_total_kv);
  keys.clear();
  db.Keys(DataType::kStrings, "*", &keys);
  ASSERT_EQ(keys.size(), 0);


  //=============================== Set ===============================

  // ***************** Group 1 Test *****************
  db.SAdd("GP1_PKPATTERNMATCHDEL_SET_KEY1", {"M1"}, &ret);
  db.SAdd("GP1_PKPATTERNMATCHDEL_SET_KEY2", {"M1"}, &ret);
  db.SAdd("GP1_PKPATTERNMATCHDEL_SET_KEY3", {"M1"}, &ret);
  db.SAdd("GP1_PKPATTERNMATCHDEL_SET_KEY4", {"M1"}, &ret);
  db.SAdd("GP1_PKPATTERNMATCHDEL_SET_KEY5", {"M1"}, &ret);
  db.SAdd("GP1_PKPATTERNMATCHDEL_SET_KEY6", {"M1"}, &ret);
  s = db.PKPatternMatchDel(DataType::kSets, "*", &delete_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 6);
  keys.clear();
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
  s = db.PKPatternMatchDel(DataType::kSets, "*", &delete_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 3);
  keys.clear();
  db.Keys(DataType::kSets, "*", &keys);
  ASSERT_EQ(keys.size(), 0);


  // ***************** Group 3 Test *****************
  db.SAdd("GP3_PKPATTERNMATCHDEL_SET_KEY1_0xxx0", {"M1"}, &ret);
  db.SAdd("GP3_PKPATTERNMATCHDEL_SET_KEY2_0ooo0", {"M1"}, &ret);
  db.SAdd("GP3_PKPATTERNMATCHDEL_SET_KEY3_0xxx0", {"M1"}, &ret);
  db.SAdd("GP3_PKPATTERNMATCHDEL_SET_KEY4_0ooo0", {"M1"}, &ret);
  db.SAdd("GP3_PKPATTERNMATCHDEL_SET_KEY5_0xxx0", {"M1"}, &ret);
  db.SAdd("GP3_PKPATTERNMATCHDEL_SET_KEY6_0ooo0", {"M1"}, &ret);
  s = db.PKPatternMatchDel(DataType::kSets, "*0ooo0", &delete_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 3);
  keys.clear();
  db.Keys(DataType::kSets, "*", &keys);
  ASSERT_EQ(keys.size(), 3);
  ASSERT_EQ("GP3_PKPATTERNMATCHDEL_SET_KEY1_0xxx0", keys[0]);
  ASSERT_EQ("GP3_PKPATTERNMATCHDEL_SET_KEY3_0xxx0", keys[1]);
  ASSERT_EQ("GP3_PKPATTERNMATCHDEL_SET_KEY5_0xxx0", keys[2]);
  type_status.clear();
  db.Del(keys, &type_status);


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
  s = db.PKPatternMatchDel(DataType::kSets, "*", &delete_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 3);
  keys.clear();
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
  s = db.PKPatternMatchDel(DataType::kSets, "*0ooo0", &delete_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 2);
  keys.clear();
  db.Keys(DataType::kSets, "*", &keys);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP5_PKPATTERNMATCHDEL_SET_KEY6_0xxx0");
  ASSERT_EQ(keys[1], "GP5_PKPATTERNMATCHDEL_SET_KEY8_0xxx0");
  type_status.clear();
  db.Del(keys, &type_status);


  // ***************** Group 6 Test *****************
  size_t gp6_total_set = 23333;
  for (size_t idx = 0; idx < gp6_total_set; ++idx) {
    db.SAdd("GP6_PKPATTERNMATCHDEL_SET_KEY" + std::to_string(idx), {"M1"}, &ret);
  }
  s = db.PKPatternMatchDel(DataType::kSets, "*", &delete_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, gp6_total_set);
  keys.clear();
  db.Keys(DataType::kSets, "*", &keys);
  ASSERT_EQ(keys.size(), 0);


  //=============================== Hashes ===============================

  // ***************** Group 1 Test *****************
  db.HSet("GP1_PKPATTERNMATCHDEL_HASH_KEY1", "FIELD", "VALUE", &ret);
  db.HSet("GP1_PKPATTERNMATCHDEL_HASH_KEY2", "FIELD", "VALUE", &ret);
  db.HSet("GP1_PKPATTERNMATCHDEL_HASH_KEY3", "FIELD", "VALUE", &ret);
  db.HSet("GP1_PKPATTERNMATCHDEL_HASH_KEY4", "FIELD", "VALUE", &ret);
  db.HSet("GP1_PKPATTERNMATCHDEL_HASH_KEY5", "FIELD", "VALUE", &ret);
  db.HSet("GP1_PKPATTERNMATCHDEL_HASH_KEY6", "FIELD", "VALUE", &ret);
  s = db.PKPatternMatchDel(DataType::kHashes, "*", &delete_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 6);
  keys.clear();
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
  s = db.PKPatternMatchDel(DataType::kHashes, "*", &delete_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 3);
  keys.clear();
  db.Keys(DataType::kHashes, "*", &keys);
  ASSERT_EQ(keys.size(), 0);


  // ***************** Group 3 Test *****************
  db.HSet("GP3_PKPATTERNMATCHDEL_HASH_KEY1_0xxx0", "FIELD", "VALUE", &ret);
  db.HSet("GP3_PKPATTERNMATCHDEL_HASH_KEY2_0ooo0", "FIELD", "VALUE", &ret);
  db.HSet("GP3_PKPATTERNMATCHDEL_HASH_KEY3_0xxx0", "FIELD", "VALUE", &ret);
  db.HSet("GP3_PKPATTERNMATCHDEL_HASH_KEY4_0ooo0", "FIELD", "VALUE", &ret);
  db.HSet("GP3_PKPATTERNMATCHDEL_HASH_KEY5_0xxx0", "FIELD", "VALUE", &ret);
  db.HSet("GP3_PKPATTERNMATCHDEL_HASH_KEY6_0ooo0", "FIELD", "VALUE", &ret);
  s = db.PKPatternMatchDel(DataType::kHashes, "*0ooo0", &delete_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 3);
  keys.clear();
  db.Keys(DataType::kHashes, "*", &keys);
  ASSERT_EQ(keys.size(), 3);
  ASSERT_EQ("GP3_PKPATTERNMATCHDEL_HASH_KEY1_0xxx0", keys[0]);
  ASSERT_EQ("GP3_PKPATTERNMATCHDEL_HASH_KEY3_0xxx0", keys[1]);
  ASSERT_EQ("GP3_PKPATTERNMATCHDEL_HASH_KEY5_0xxx0", keys[2]);
  type_status.clear();
  db.Del(keys, &type_status);


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
  s = db.PKPatternMatchDel(DataType::kHashes, "*", &delete_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 3);
  keys.clear();
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
  s = db.PKPatternMatchDel(DataType::kHashes, "*0ooo0", &delete_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 2);
  keys.clear();
  db.Keys(DataType::kHashes, "*", &keys);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP5_PKPATTERNMATCHDEL_HASH_KEY6_0xxx0");
  ASSERT_EQ(keys[1], "GP5_PKPATTERNMATCHDEL_HASH_KEY8_0xxx0");
  type_status.clear();
  db.Del(keys, &type_status);


  // ***************** Group 6 Test *****************
  size_t gp6_total_hash = 23333;
  for (size_t idx = 0; idx < gp6_total_hash; ++idx) {
    db.HSet("GP6_PKPATTERNMATCHDEL_HASH_KEY" + std::to_string(idx), "FIELD", "VALUE", &ret);
  }
  s = db.PKPatternMatchDel(DataType::kHashes, "*", &delete_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, gp6_total_hash);
  keys.clear();
  db.Keys(DataType::kHashes, "*", &keys);
  ASSERT_EQ(keys.size(), 0);


  //=============================== ZSets ===============================

  // ***************** Group 1 Test *****************
  db.ZAdd("GP1_PKPATTERNMATCHDEL_ZSET_KEY1", {{1, "M"}}, &ret);
  db.ZAdd("GP1_PKPATTERNMATCHDEL_ZSET_KEY2", {{1, "M"}}, &ret);
  db.ZAdd("GP1_PKPATTERNMATCHDEL_ZSET_KEY3", {{1, "M"}}, &ret);
  db.ZAdd("GP1_PKPATTERNMATCHDEL_ZSET_KEY4", {{1, "M"}}, &ret);
  db.ZAdd("GP1_PKPATTERNMATCHDEL_ZSET_KEY5", {{1, "M"}}, &ret);
  db.ZAdd("GP1_PKPATTERNMATCHDEL_ZSET_KEY6", {{1, "M"}}, &ret);
  s = db.PKPatternMatchDel(DataType::kZSets, "*", &delete_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 6);
  keys.clear();
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
  s = db.PKPatternMatchDel(DataType::kZSets, "*", &delete_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 3);
  keys.clear();
  db.Keys(DataType::kZSets, "*", &keys);
  ASSERT_EQ(keys.size(), 0);


  // ***************** Group 3 Test *****************
  db.ZAdd("GP3_PKPATTERNMATCHDEL_ZSET_KEY1_0xxx0", {{1, "M"}}, &ret);
  db.ZAdd("GP3_PKPATTERNMATCHDEL_ZSET_KEY2_0ooo0", {{1, "M"}}, &ret);
  db.ZAdd("GP3_PKPATTERNMATCHDEL_ZSET_KEY3_0xxx0", {{1, "M"}}, &ret);
  db.ZAdd("GP3_PKPATTERNMATCHDEL_ZSET_KEY4_0ooo0", {{1, "M"}}, &ret);
  db.ZAdd("GP3_PKPATTERNMATCHDEL_ZSET_KEY5_0xxx0", {{1, "M"}}, &ret);
  db.ZAdd("GP3_PKPATTERNMATCHDEL_ZSET_KEY6_0ooo0", {{1, "M"}}, &ret);
  s = db.PKPatternMatchDel(DataType::kZSets, "*0ooo0", &delete_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 3);
  keys.clear();
  db.Keys(DataType::kZSets, "*", &keys);
  ASSERT_EQ(keys.size(), 3);
  ASSERT_EQ("GP3_PKPATTERNMATCHDEL_ZSET_KEY1_0xxx0", keys[0]);
  ASSERT_EQ("GP3_PKPATTERNMATCHDEL_ZSET_KEY3_0xxx0", keys[1]);
  ASSERT_EQ("GP3_PKPATTERNMATCHDEL_ZSET_KEY5_0xxx0", keys[2]);
  type_status.clear();
  db.Del(keys, &type_status);


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
  s = db.PKPatternMatchDel(DataType::kZSets, "*", &delete_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 3);
  keys.clear();
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
  s = db.PKPatternMatchDel(DataType::kZSets, "*0ooo0", &delete_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 2);
  keys.clear();
  db.Keys(DataType::kZSets, "*", &keys);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP5_PKPATTERNMATCHDEL_ZSET_KEY6_0xxx0");
  ASSERT_EQ(keys[1], "GP5_PKPATTERNMATCHDEL_ZSET_KEY8_0xxx0");
  type_status.clear();
  db.Del(keys, &type_status);


  // ***************** Group 6 Test *****************
  size_t gp6_total_zset = 23333;
  for (size_t idx = 0; idx < gp6_total_zset; ++idx) {
    db.ZAdd("GP6_PKPATTERNMATCHDEL_ZSET_KEY" + std::to_string(idx), {{1, "M"}}, &ret);
  }
  s = db.PKPatternMatchDel(DataType::kZSets, "*", &delete_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, gp6_total_zset);
  keys.clear();
  db.Keys(DataType::kZSets, "*", &keys);
  ASSERT_EQ(keys.size(), 0);



  //=============================== List ===============================

  // ***************** Group 1 Test *****************
  db.LPush("GP1_PKPATTERNMATCHDEL_LIST_KEY1", {"VALUE"}, &ret64);
  db.LPush("GP1_PKPATTERNMATCHDEL_LIST_KEY2", {"VALUE"}, &ret64);
  db.LPush("GP1_PKPATTERNMATCHDEL_LIST_KEY3", {"VALUE"}, &ret64);
  db.LPush("GP1_PKPATTERNMATCHDEL_LIST_KEY4", {"VALUE"}, &ret64);
  db.LPush("GP1_PKPATTERNMATCHDEL_LIST_KEY5", {"VALUE"}, &ret64);
  db.LPush("GP1_PKPATTERNMATCHDEL_LIST_KEY6", {"VALUE"}, &ret64);
  s = db.PKPatternMatchDel(DataType::kLists, "*", &delete_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 6);
  keys.clear();
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
  s = db.PKPatternMatchDel(DataType::kLists, "*", &delete_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 3);
  keys.clear();
  db.Keys(DataType::kLists, "*", &keys);
  ASSERT_EQ(keys.size(), 0);


  // ***************** Group 3 Test *****************
  db.LPush("GP3_PKPATTERNMATCHDEL_LIST_KEY1_0xxx0", {"VALUE"}, &ret64);
  db.LPush("GP3_PKPATTERNMATCHDEL_LIST_KEY2_0ooo0", {"VALUE"}, &ret64);
  db.LPush("GP3_PKPATTERNMATCHDEL_LIST_KEY3_0xxx0", {"VALUE"}, &ret64);
  db.LPush("GP3_PKPATTERNMATCHDEL_LIST_KEY4_0ooo0", {"VALUE"}, &ret64);
  db.LPush("GP3_PKPATTERNMATCHDEL_LIST_KEY5_0xxx0", {"VALUE"}, &ret64);
  db.LPush("GP3_PKPATTERNMATCHDEL_LIST_KEY6_0ooo0", {"VALUE"}, &ret64);
  s = db.PKPatternMatchDel(DataType::kLists, "*0ooo0", &delete_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 3);
  keys.clear();
  db.Keys(DataType::kLists, "*", &keys);
  ASSERT_EQ(keys.size(), 3);
  ASSERT_EQ("GP3_PKPATTERNMATCHDEL_LIST_KEY1_0xxx0", keys[0]);
  ASSERT_EQ("GP3_PKPATTERNMATCHDEL_LIST_KEY3_0xxx0", keys[1]);
  ASSERT_EQ("GP3_PKPATTERNMATCHDEL_LIST_KEY5_0xxx0", keys[2]);
  type_status.clear();
  db.Del(keys, &type_status);


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
  s = db.PKPatternMatchDel(DataType::kLists, "*", &delete_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 3);
  keys.clear();
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
  s = db.PKPatternMatchDel(DataType::kLists, "*0ooo0", &delete_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, 2);
  keys.clear();
  db.Keys(DataType::kLists, "*", &keys);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP5_PKPATTERNMATCHDEL_LIST_KEY6_0xxx0");
  ASSERT_EQ(keys[1], "GP5_PKPATTERNMATCHDEL_LIST_KEY8_0xxx0");
  type_status.clear();
  db.Del(keys, &type_status);


  // ***************** Group 6 Test *****************
  size_t gp6_total_list = 23333;
  for (size_t idx = 0; idx < gp6_total_list; ++idx) {
    db.LPush("GP6_PKPATTERNMATCHDEL_LIST_KEY" + std::to_string(idx), {"VALUE"}, &ret64);
  }
  s = db.PKPatternMatchDel(DataType::kLists, "*", &delete_count);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(delete_count, gp6_total_hash);
  keys.clear();
  db.Keys(DataType::kLists, "*", &keys);
  ASSERT_EQ(keys.size(), 0);

  sleep(2);
  db.Compact(DataType::kAll, true);
}

// Scan
// Note: This test needs to execute at first because all of the data is
// predetermined.
TEST_F(KeysTest, ScanCaseAllTest) {

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
  //String
  s = db.Set("GP1_SCAN_CASE_ALL_STRING_KEY1", "GP1_SCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP1_SCAN_CASE_ALL_STRING_KEY2", "GP1_SCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP1_SCAN_CASE_ALL_STRING_KEY3", "GP1_SCAN_CASE_ALL_STRING_VALUE3");

  //Hash
  s = db.HSet("GP1_SCAN_CASE_ALL_HASH_KEY1", "GP1_SCAN_CASE_ALL_HASH_FIELD1", "GP1_SCAN_CASE_ALL_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP1_SCAN_CASE_ALL_HASH_KEY2", "GP1_SCAN_CASE_ALL_HASH_FIELD2", "GP1_SCAN_CASE_ALL_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP1_SCAN_CASE_ALL_HASH_KEY3", "GP1_SCAN_CASE_ALL_HASH_FIELD3", "GP1_SCAN_CASE_ALL_HASH_VALUE3", &int32_ret);

  //Set
  s = db.SAdd("GP1_SCAN_CASE_ALL_SET_KEY1", {"GP1_SCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP1_SCAN_CASE_ALL_SET_KEY2", {"GP1_SCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP1_SCAN_CASE_ALL_SET_KEY3", {"GP1_SCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);

  //List
  s = db.LPush("GP1_SCAN_CASE_ALL_LIST_KEY1", {"GP1_SCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP1_SCAN_CASE_ALL_LIST_KEY2", {"GP1_SCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP1_SCAN_CASE_ALL_LIST_KEY3", {"GP1_SCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);

  //ZSet
  s = db.ZAdd("GP1_SCAN_CASE_ALL_ZSET_KEY1", {{1, "GP1_SCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP1_SCAN_CASE_ALL_ZSET_KEY2", {{1, "GP1_SCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP1_SCAN_CASE_ALL_ZSET_KEY3", {{1, "GP1_SCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);

  //Scan
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

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 2 Test *****************
  //String
  s = db.Set("GP2_SCAN_CASE_ALL_STRING_KEY1", "GP2_SCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP2_SCAN_CASE_ALL_STRING_KEY2", "GP2_SCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP2_SCAN_CASE_ALL_STRING_KEY3", "GP2_SCAN_CASE_ALL_STRING_VALUE3");

  //Hash
  s = db.HSet("GP2_SCAN_CASE_ALL_HASH_KEY1", "GP2_SCAN_CASE_ALL_HASH_FIELD1", "GP2_SCAN_CASE_ALL_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP2_SCAN_CASE_ALL_HASH_KEY2", "GP2_SCAN_CASE_ALL_HASH_FIELD2", "GP2_SCAN_CASE_ALL_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP2_SCAN_CASE_ALL_HASH_KEY3", "GP2_SCAN_CASE_ALL_HASH_FIELD3", "GP2_SCAN_CASE_ALL_HASH_VALUE3", &int32_ret);

  //Set
  s = db.SAdd("GP2_SCAN_CASE_ALL_SET_KEY1", {"GP2_SCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP2_SCAN_CASE_ALL_SET_KEY2", {"GP2_SCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP2_SCAN_CASE_ALL_SET_KEY3", {"GP2_SCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);

  //List
  s = db.LPush("GP2_SCAN_CASE_ALL_LIST_KEY1", {"GP2_SCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP2_SCAN_CASE_ALL_LIST_KEY2", {"GP2_SCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP2_SCAN_CASE_ALL_LIST_KEY3", {"GP2_SCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);

  //ZSet
  s = db.ZAdd("GP2_SCAN_CASE_ALL_ZSET_KEY1", {{1, "GP2_SCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP2_SCAN_CASE_ALL_ZSET_KEY2", {{1, "GP2_SCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP2_SCAN_CASE_ALL_ZSET_KEY3", {{1, "GP2_SCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);

  //Scan
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

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 3 Test *****************
  //String
  s = db.Set("GP3_SCAN_CASE_ALL_STRING_KEY1", "GP3_SCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP3_SCAN_CASE_ALL_STRING_KEY2", "GP3_SCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP3_SCAN_CASE_ALL_STRING_KEY3", "GP3_SCAN_CASE_ALL_STRING_VALUE3");

  //Hash
  s = db.HSet("GP3_SCAN_CASE_ALL_HASH_KEY1", "GP3_SCAN_CASE_ALL_HASH_FIELD1", "GP3_SCAN_CASE_ALL_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP3_SCAN_CASE_ALL_HASH_KEY2", "GP3_SCAN_CASE_ALL_HASH_FIELD2", "GP3_SCAN_CASE_ALL_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP3_SCAN_CASE_ALL_HASH_KEY3", "GP3_SCAN_CASE_ALL_HASH_FIELD3", "GP3_SCAN_CASE_ALL_HASH_VALUE3", &int32_ret);

  //Set
  s = db.SAdd("GP3_SCAN_CASE_ALL_SET_KEY1", {"GP3_SCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP3_SCAN_CASE_ALL_SET_KEY2", {"GP3_SCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP3_SCAN_CASE_ALL_SET_KEY3", {"GP3_SCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);

  //List
  s = db.LPush("GP3_SCAN_CASE_ALL_LIST_KEY1", {"GP3_SCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP3_SCAN_CASE_ALL_LIST_KEY2", {"GP3_SCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP3_SCAN_CASE_ALL_LIST_KEY3", {"GP3_SCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);

  //ZSet
  s = db.ZAdd("GP3_SCAN_CASE_ALL_ZSET_KEY1", {{1, "GP3_SCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP3_SCAN_CASE_ALL_ZSET_KEY2", {{1, "GP3_SCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP3_SCAN_CASE_ALL_ZSET_KEY3", {{1, "GP3_SCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);

  //Scan
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

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 4 Test *****************
  //String
  s = db.Set("GP4_SCAN_CASE_ALL_STRING_KEY1", "GP4_SCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP4_SCAN_CASE_ALL_STRING_KEY2", "GP4_SCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP4_SCAN_CASE_ALL_STRING_KEY3", "GP4_SCAN_CASE_ALL_STRING_VALUE3");

  //Hash
  s = db.HSet("GP4_SCAN_CASE_ALL_HASH_KEY1", "GP4_SCAN_CASE_ALL_HASH_FIELD1", "GP4_SCAN_CASE_ALL_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP4_SCAN_CASE_ALL_HASH_KEY2", "GP4_SCAN_CASE_ALL_HASH_FIELD2", "GP4_SCAN_CASE_ALL_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP4_SCAN_CASE_ALL_HASH_KEY3", "GP4_SCAN_CASE_ALL_HASH_FIELD3", "GP4_SCAN_CASE_ALL_HASH_VALUE3", &int32_ret);

  //Set
  s = db.SAdd("GP4_SCAN_CASE_ALL_SET_KEY1", {"GP4_SCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP4_SCAN_CASE_ALL_SET_KEY2", {"GP4_SCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP4_SCAN_CASE_ALL_SET_KEY3", {"GP4_SCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);

  //List
  s = db.LPush("GP4_SCAN_CASE_ALL_LIST_KEY1", {"GP4_SCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP4_SCAN_CASE_ALL_LIST_KEY2", {"GP4_SCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP4_SCAN_CASE_ALL_LIST_KEY3", {"GP4_SCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);

  //ZSet
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

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 5 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP5_SCAN_CASE_ALL_STRING_KEY1", "GP5_SCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP5_SCAN_CASE_ALL_STRING_KEY2", "GP5_SCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP5_SCAN_CASE_ALL_STRING_KEY3", "GP5_SCAN_CASE_ALL_STRING_VALUE3");
  delete_keys.push_back("GP5_SCAN_CASE_ALL_STRING_KEY1");
  delete_keys.push_back("GP5_SCAN_CASE_ALL_STRING_KEY2");
  delete_keys.push_back("GP5_SCAN_CASE_ALL_STRING_KEY3");

  //Hash
  s = db.HSet("GP5_SCAN_CASE_ALL_HASH_KEY1", "GP5_SCAN_CASE_ALL_HASH_FIELD1", "GP5_SCAN_CASE_ALL_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP5_SCAN_CASE_ALL_HASH_KEY2", "GP5_SCAN_CASE_ALL_HASH_FIELD2", "GP5_SCAN_CASE_ALL_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP5_SCAN_CASE_ALL_HASH_KEY3", "GP5_SCAN_CASE_ALL_HASH_FIELD3", "GP5_SCAN_CASE_ALL_HASH_VALUE3", &int32_ret);
  delete_keys.push_back("GP5_SCAN_CASE_ALL_HASH_KEY1");
  delete_keys.push_back("GP5_SCAN_CASE_ALL_HASH_KEY2");
  delete_keys.push_back("GP5_SCAN_CASE_ALL_HASH_KEY3");

  //Set
  s = db.SAdd("GP5_SCAN_CASE_ALL_SET_KEY1", {"GP5_SCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP5_SCAN_CASE_ALL_SET_KEY2", {"GP5_SCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP5_SCAN_CASE_ALL_SET_KEY3", {"GP5_SCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);
  delete_keys.push_back("GP5_SCAN_CASE_ALL_SET_KEY1");
  delete_keys.push_back("GP5_SCAN_CASE_ALL_SET_KEY2");
  delete_keys.push_back("GP5_SCAN_CASE_ALL_SET_KEY3");

  //List
  s = db.LPush("GP5_SCAN_CASE_ALL_LIST_KEY1", {"GP5_SCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP5_SCAN_CASE_ALL_LIST_KEY2", {"GP5_SCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP5_SCAN_CASE_ALL_LIST_KEY3", {"GP5_SCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);
  delete_keys.push_back("GP5_SCAN_CASE_ALL_LIST_KEY1");
  delete_keys.push_back("GP5_SCAN_CASE_ALL_LIST_KEY2");
  delete_keys.push_back("GP5_SCAN_CASE_ALL_LIST_KEY3");

  //ZSet
  s = db.ZAdd("GP5_SCAN_CASE_ALL_ZSET_KEY1", {{1, "GP5_SCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP5_SCAN_CASE_ALL_ZSET_KEY2", {{1, "GP5_SCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP5_SCAN_CASE_ALL_ZSET_KEY3", {{1, "GP5_SCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.push_back("GP5_SCAN_CASE_ALL_ZSET_KEY1");
  delete_keys.push_back("GP5_SCAN_CASE_ALL_ZSET_KEY2");
  delete_keys.push_back("GP5_SCAN_CASE_ALL_ZSET_KEY3");

  cursor = 0;
  keys.clear();
  total_keys.clear();
  do {
    next_cursor = db.Scan(DataType::kAll, cursor, "*_SET_*", 5, &keys);
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  } while (cursor);
  ASSERT_EQ(total_keys.size(), 3);
  ASSERT_EQ(total_keys[0], "GP5_SCAN_CASE_ALL_SET_KEY1");
  ASSERT_EQ(total_keys[1], "GP5_SCAN_CASE_ALL_SET_KEY2");
  ASSERT_EQ(total_keys[2], "GP5_SCAN_CASE_ALL_SET_KEY3");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 6 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP6_SCAN_CASE_ALL_STRING_KEY1", "GP6_SCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP6_SCAN_CASE_ALL_STRING_KEY2", "GP6_SCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP6_SCAN_CASE_ALL_STRING_KEY3", "GP6_SCAN_CASE_ALL_STRING_VALUE3");
  delete_keys.push_back("GP6_SCAN_CASE_ALL_STRING_KEY1");
  delete_keys.push_back("GP6_SCAN_CASE_ALL_STRING_KEY2");
  delete_keys.push_back("GP6_SCAN_CASE_ALL_STRING_KEY3");

  //Hash
  s = db.HSet("GP6_SCAN_CASE_ALL_HASH_KEY1", "GP6_SCAN_CASE_ALL_HASH_FIELD1", "GP6_SCAN_CASE_ALL_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP6_SCAN_CASE_ALL_HASH_KEY2", "GP6_SCAN_CASE_ALL_HASH_FIELD2", "GP6_SCAN_CASE_ALL_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP6_SCAN_CASE_ALL_HASH_KEY3", "GP6_SCAN_CASE_ALL_HASH_FIELD3", "GP6_SCAN_CASE_ALL_HASH_VALUE3", &int32_ret);
  delete_keys.push_back("GP6_SCAN_CASE_ALL_HASH_KEY1");
  delete_keys.push_back("GP6_SCAN_CASE_ALL_HASH_KEY2");
  delete_keys.push_back("GP6_SCAN_CASE_ALL_HASH_KEY3");

  //Set
  s = db.SAdd("GP6_SCAN_CASE_ALL_SET_KEY1", {"GP6_SCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP6_SCAN_CASE_ALL_SET_KEY2", {"GP6_SCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP6_SCAN_CASE_ALL_SET_KEY3", {"GP6_SCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);
  delete_keys.push_back("GP6_SCAN_CASE_ALL_SET_KEY1");
  delete_keys.push_back("GP6_SCAN_CASE_ALL_SET_KEY2");
  delete_keys.push_back("GP6_SCAN_CASE_ALL_SET_KEY3");

  //List
  s = db.LPush("GP6_SCAN_CASE_ALL_LIST_KEY1", {"GP6_SCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP6_SCAN_CASE_ALL_LIST_KEY2", {"GP6_SCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP6_SCAN_CASE_ALL_LIST_KEY3", {"GP6_SCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);
  delete_keys.push_back("GP6_SCAN_CASE_ALL_LIST_KEY1");
  delete_keys.push_back("GP6_SCAN_CASE_ALL_LIST_KEY2");
  delete_keys.push_back("GP6_SCAN_CASE_ALL_LIST_KEY3");

  //ZSet
  s = db.ZAdd("GP6_SCAN_CASE_ALL_ZSET_KEY1", {{1, "GP6_SCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP6_SCAN_CASE_ALL_ZSET_KEY2", {{1, "GP6_SCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP6_SCAN_CASE_ALL_ZSET_KEY3", {{1, "GP6_SCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.push_back("GP6_SCAN_CASE_ALL_ZSET_KEY1");
  delete_keys.push_back("GP6_SCAN_CASE_ALL_ZSET_KEY2");
  delete_keys.push_back("GP6_SCAN_CASE_ALL_ZSET_KEY3");

  cursor = 0;
  keys.clear();
  total_keys.clear();
  do {
    next_cursor = db.Scan(DataType::kAll, cursor, "*KEY1", 5, &keys);
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  } while (cursor);
  ASSERT_EQ(total_keys.size(), 5);
  ASSERT_EQ(total_keys[0], "GP6_SCAN_CASE_ALL_STRING_KEY1");
  ASSERT_EQ(total_keys[1], "GP6_SCAN_CASE_ALL_HASH_KEY1");
  ASSERT_EQ(total_keys[2], "GP6_SCAN_CASE_ALL_SET_KEY1");
  ASSERT_EQ(total_keys[3], "GP6_SCAN_CASE_ALL_LIST_KEY1");
  ASSERT_EQ(total_keys[4], "GP6_SCAN_CASE_ALL_ZSET_KEY1");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 7 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP7_SCAN_CASE_ALL_STRING_KEY1", "GP7_SCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP7_SCAN_CASE_ALL_STRING_KEY2", "GP7_SCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP7_SCAN_CASE_ALL_STRING_KEY3", "GP7_SCAN_CASE_ALL_STRING_VALUE3");
  delete_keys.push_back("GP7_SCAN_CASE_ALL_STRING_KEY1");
  delete_keys.push_back("GP7_SCAN_CASE_ALL_STRING_KEY2");
  delete_keys.push_back("GP7_SCAN_CASE_ALL_STRING_KEY3");

  //Hash
  s = db.HSet("GP7_SCAN_CASE_ALL_HASH_KEY1", "GP7_SCAN_CASE_ALL_HASH_FIELD1", "GP7_SCAN_CASE_ALL_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP7_SCAN_CASE_ALL_HASH_KEY2", "GP7_SCAN_CASE_ALL_HASH_FIELD2", "GP7_SCAN_CASE_ALL_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP7_SCAN_CASE_ALL_HASH_KEY3", "GP7_SCAN_CASE_ALL_HASH_FIELD3", "GP7_SCAN_CASE_ALL_HASH_VALUE3", &int32_ret);
  delete_keys.push_back("GP7_SCAN_CASE_ALL_HASH_KEY1");
  delete_keys.push_back("GP7_SCAN_CASE_ALL_HASH_KEY2");
  delete_keys.push_back("GP7_SCAN_CASE_ALL_HASH_KEY3");

  //Set
  s = db.SAdd("GP7_SCAN_CASE_ALL_SET_KEY1", {"GP7_SCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP7_SCAN_CASE_ALL_SET_KEY2", {"GP7_SCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP7_SCAN_CASE_ALL_SET_KEY3", {"GP7_SCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);
  delete_keys.push_back("GP7_SCAN_CASE_ALL_SET_KEY1");
  delete_keys.push_back("GP7_SCAN_CASE_ALL_SET_KEY2");
  delete_keys.push_back("GP7_SCAN_CASE_ALL_SET_KEY3");

  //List
  s = db.LPush("GP7_SCAN_CASE_ALL_LIST_KEY1", {"GP7_SCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP7_SCAN_CASE_ALL_LIST_KEY2", {"GP7_SCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP7_SCAN_CASE_ALL_LIST_KEY3", {"GP7_SCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);
  delete_keys.push_back("GP7_SCAN_CASE_ALL_LIST_KEY1");
  delete_keys.push_back("GP7_SCAN_CASE_ALL_LIST_KEY2");
  delete_keys.push_back("GP7_SCAN_CASE_ALL_LIST_KEY3");

  //ZSet
  s = db.ZAdd("GP7_SCAN_CASE_ALL_ZSET_KEY1", {{1, "GP7_SCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP7_SCAN_CASE_ALL_ZSET_KEY2", {{1, "GP7_SCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP7_SCAN_CASE_ALL_ZSET_KEY3", {{1, "GP7_SCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.push_back("GP7_SCAN_CASE_ALL_ZSET_KEY1");
  delete_keys.push_back("GP7_SCAN_CASE_ALL_ZSET_KEY2");
  delete_keys.push_back("GP7_SCAN_CASE_ALL_ZSET_KEY3");

  cursor = 0;
  keys.clear();
  total_keys.clear();
  do {
    next_cursor = db.Scan(DataType::kAll, cursor, "*KEY2", 5, &keys);
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  } while (cursor);
  ASSERT_EQ(total_keys.size(), 5);
  ASSERT_EQ(total_keys[0], "GP7_SCAN_CASE_ALL_STRING_KEY2");
  ASSERT_EQ(total_keys[1], "GP7_SCAN_CASE_ALL_HASH_KEY2");
  ASSERT_EQ(total_keys[2], "GP7_SCAN_CASE_ALL_SET_KEY2");
  ASSERT_EQ(total_keys[3], "GP7_SCAN_CASE_ALL_LIST_KEY2");
  ASSERT_EQ(total_keys[4], "GP7_SCAN_CASE_ALL_ZSET_KEY2");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 8 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP8_SCAN_CASE_ALL_STRING_KEY1", "GP8_SCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP8_SCAN_CASE_ALL_STRING_KEY2", "GP8_SCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP8_SCAN_CASE_ALL_STRING_KEY3", "GP8_SCAN_CASE_ALL_STRING_VALUE3");
  delete_keys.push_back("GP8_SCAN_CASE_ALL_STRING_KEY1");
  delete_keys.push_back("GP8_SCAN_CASE_ALL_STRING_KEY2");
  delete_keys.push_back("GP8_SCAN_CASE_ALL_STRING_KEY3");

  //Hash
  s = db.HSet("GP8_SCAN_CASE_ALL_HASH_KEY1", "GP8_SCAN_CASE_ALL_HASH_FIELD1", "GP8_SCAN_CASE_ALL_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP8_SCAN_CASE_ALL_HASH_KEY2", "GP8_SCAN_CASE_ALL_HASH_FIELD2", "GP8_SCAN_CASE_ALL_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP8_SCAN_CASE_ALL_HASH_KEY3", "GP8_SCAN_CASE_ALL_HASH_FIELD3", "GP8_SCAN_CASE_ALL_HASH_VALUE3", &int32_ret);
  delete_keys.push_back("GP8_SCAN_CASE_ALL_HASH_KEY1");
  delete_keys.push_back("GP8_SCAN_CASE_ALL_HASH_KEY2");
  delete_keys.push_back("GP8_SCAN_CASE_ALL_HASH_KEY3");

  //Set
  s = db.SAdd("GP8_SCAN_CASE_ALL_SET_KEY1", {"GP8_SCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP8_SCAN_CASE_ALL_SET_KEY2", {"GP8_SCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP8_SCAN_CASE_ALL_SET_KEY3", {"GP8_SCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);
  delete_keys.push_back("GP8_SCAN_CASE_ALL_SET_KEY1");
  delete_keys.push_back("GP8_SCAN_CASE_ALL_SET_KEY2");
  delete_keys.push_back("GP8_SCAN_CASE_ALL_SET_KEY3");

  //List
  s = db.LPush("GP8_SCAN_CASE_ALL_LIST_KEY1", {"GP8_SCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP8_SCAN_CASE_ALL_LIST_KEY2", {"GP8_SCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP8_SCAN_CASE_ALL_LIST_KEY3", {"GP8_SCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);
  delete_keys.push_back("GP8_SCAN_CASE_ALL_LIST_KEY1");
  delete_keys.push_back("GP8_SCAN_CASE_ALL_LIST_KEY2");
  delete_keys.push_back("GP8_SCAN_CASE_ALL_LIST_KEY3");

  //ZSet
  s = db.ZAdd("GP8_SCAN_CASE_ALL_ZSET_KEY1", {{1, "GP8_SCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP8_SCAN_CASE_ALL_ZSET_KEY2", {{1, "GP8_SCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP8_SCAN_CASE_ALL_ZSET_KEY3", {{1, "GP8_SCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.push_back("GP8_SCAN_CASE_ALL_ZSET_KEY1");
  delete_keys.push_back("GP8_SCAN_CASE_ALL_ZSET_KEY2");
  delete_keys.push_back("GP8_SCAN_CASE_ALL_ZSET_KEY3");

  cursor = 0;
  keys.clear();
  total_keys.clear();
  do {
    next_cursor = db.Scan(DataType::kAll, cursor, "*KEY3", 5, &keys);
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  } while (cursor);
  ASSERT_EQ(total_keys.size(), 5);
  ASSERT_EQ(total_keys[0], "GP8_SCAN_CASE_ALL_STRING_KEY3");
  ASSERT_EQ(total_keys[1], "GP8_SCAN_CASE_ALL_HASH_KEY3");
  ASSERT_EQ(total_keys[2], "GP8_SCAN_CASE_ALL_SET_KEY3");
  ASSERT_EQ(total_keys[3], "GP8_SCAN_CASE_ALL_LIST_KEY3");
  ASSERT_EQ(total_keys[4], "GP8_SCAN_CASE_ALL_ZSET_KEY3");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 9 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP9_SCAN_CASE_ALL_STRING_KEY1", "GP9_SCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP9_SCAN_CASE_ALL_STRING_KEY2", "GP9_SCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP9_SCAN_CASE_ALL_STRING_KEY3", "GP9_SCAN_CASE_ALL_STRING_VALUE3");
  delete_keys.push_back("GP9_SCAN_CASE_ALL_STRING_KEY1");
  delete_keys.push_back("GP9_SCAN_CASE_ALL_STRING_KEY2");
  delete_keys.push_back("GP9_SCAN_CASE_ALL_STRING_KEY3");

  //Hash
  s = db.HSet("GP9_SCAN_CASE_ALL_HASH_KEY1", "GP9_SCAN_CASE_ALL_HASH_FIELD1", "GP9_SCAN_CASE_ALL_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP9_SCAN_CASE_ALL_HASH_KEY2", "GP9_SCAN_CASE_ALL_HASH_FIELD2", "GP9_SCAN_CASE_ALL_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP9_SCAN_CASE_ALL_HASH_KEY3", "GP9_SCAN_CASE_ALL_HASH_FIELD3", "GP9_SCAN_CASE_ALL_HASH_VALUE3", &int32_ret);
  delete_keys.push_back("GP9_SCAN_CASE_ALL_HASH_KEY1");
  delete_keys.push_back("GP9_SCAN_CASE_ALL_HASH_KEY2");
  delete_keys.push_back("GP9_SCAN_CASE_ALL_HASH_KEY3");

  //Set
  s = db.SAdd("GP9_SCAN_CASE_ALL_SET_KEY1", {"GP9_SCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP9_SCAN_CASE_ALL_SET_KEY2", {"GP9_SCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP9_SCAN_CASE_ALL_SET_KEY3", {"GP9_SCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);
  delete_keys.push_back("GP9_SCAN_CASE_ALL_SET_KEY1");
  delete_keys.push_back("GP9_SCAN_CASE_ALL_SET_KEY2");
  delete_keys.push_back("GP9_SCAN_CASE_ALL_SET_KEY3");

  //List
  s = db.LPush("GP9_SCAN_CASE_ALL_LIST_KEY1", {"GP9_SCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP9_SCAN_CASE_ALL_LIST_KEY2", {"GP9_SCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP9_SCAN_CASE_ALL_LIST_KEY3", {"GP9_SCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);
  delete_keys.push_back("GP9_SCAN_CASE_ALL_LIST_KEY1");
  delete_keys.push_back("GP9_SCAN_CASE_ALL_LIST_KEY2");
  delete_keys.push_back("GP9_SCAN_CASE_ALL_LIST_KEY3");

  //ZSet
  s = db.ZAdd("GP9_SCAN_CASE_ALL_ZSET_KEY1", {{1, "GP9_SCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP9_SCAN_CASE_ALL_ZSET_KEY2", {{1, "GP9_SCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP9_SCAN_CASE_ALL_ZSET_KEY3", {{1, "GP9_SCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.push_back("GP9_SCAN_CASE_ALL_ZSET_KEY1");
  delete_keys.push_back("GP9_SCAN_CASE_ALL_ZSET_KEY2");
  delete_keys.push_back("GP9_SCAN_CASE_ALL_ZSET_KEY3");

  cursor = 0;
  keys.clear();
  total_keys.clear();
  do {
    next_cursor = db.Scan(DataType::kAll, cursor, "GP9*", 5, &keys);
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  } while (cursor);
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

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 10 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP10_SCAN_CASE_ALL_STRING_KEY1", "GP10_SCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP10_SCAN_CASE_ALL_STRING_KEY2", "GP10_SCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP10_SCAN_CASE_ALL_STRING_KEY3", "GP10_SCAN_CASE_ALL_STRING_VALUE3");
  delete_keys.push_back("GP10_SCAN_CASE_ALL_STRING_KEY1");
  delete_keys.push_back("GP10_SCAN_CASE_ALL_STRING_KEY2");
  delete_keys.push_back("GP10_SCAN_CASE_ALL_STRING_KEY3");

  //Hash
  s = db.HSet("GP10_SCAN_CASE_ALL_HASH_KEY1", "GP10_SCAN_CASE_ALL_HASH_FIELD1", "GP10_SCAN_CASE_ALL_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP10_SCAN_CASE_ALL_HASH_KEY2", "GP10_SCAN_CASE_ALL_HASH_FIELD2", "GP10_SCAN_CASE_ALL_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP10_SCAN_CASE_ALL_HASH_KEY3", "GP10_SCAN_CASE_ALL_HASH_FIELD3", "GP10_SCAN_CASE_ALL_HASH_VALUE3", &int32_ret);
  delete_keys.push_back("GP10_SCAN_CASE_ALL_HASH_KEY1");
  delete_keys.push_back("GP10_SCAN_CASE_ALL_HASH_KEY2");
  delete_keys.push_back("GP10_SCAN_CASE_ALL_HASH_KEY3");

  //Set
  s = db.SAdd("GP10_SCAN_CASE_ALL_SET_KEY1", {"GP10_SCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP10_SCAN_CASE_ALL_SET_KEY2", {"GP10_SCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP10_SCAN_CASE_ALL_SET_KEY3", {"GP10_SCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);
  delete_keys.push_back("GP10_SCAN_CASE_ALL_SET_KEY1");
  delete_keys.push_back("GP10_SCAN_CASE_ALL_SET_KEY2");
  delete_keys.push_back("GP10_SCAN_CASE_ALL_SET_KEY3");

  //List
  s = db.LPush("GP10_SCAN_CASE_ALL_LIST_KEY1", {"GP10_SCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP10_SCAN_CASE_ALL_LIST_KEY2", {"GP10_SCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP10_SCAN_CASE_ALL_LIST_KEY3", {"GP10_SCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);
  delete_keys.push_back("GP10_SCAN_CASE_ALL_LIST_KEY1");
  delete_keys.push_back("GP10_SCAN_CASE_ALL_LIST_KEY2");
  delete_keys.push_back("GP10_SCAN_CASE_ALL_LIST_KEY3");

  //ZSet
  s = db.ZAdd("GP10_SCAN_CASE_ALL_ZSET_KEY1", {{1, "GP10_SCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP10_SCAN_CASE_ALL_ZSET_KEY2", {{1, "GP10_SCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP10_SCAN_CASE_ALL_ZSET_KEY3", {{1, "GP10_SCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.push_back("GP10_SCAN_CASE_ALL_ZSET_KEY1");
  delete_keys.push_back("GP10_SCAN_CASE_ALL_ZSET_KEY2");
  delete_keys.push_back("GP10_SCAN_CASE_ALL_ZSET_KEY3");

  cursor = 0;
  keys.clear();
  total_keys.clear();
  do {
    next_cursor = db.Scan(DataType::kAll, cursor, "GP10_SCAN_CASE_ALL_STRING_*", 5, &keys);
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  } while (cursor);
  ASSERT_EQ(total_keys.size(), 3);
  ASSERT_EQ(total_keys[0], "GP10_SCAN_CASE_ALL_STRING_KEY1");
  ASSERT_EQ(total_keys[1], "GP10_SCAN_CASE_ALL_STRING_KEY2");
  ASSERT_EQ(total_keys[2], "GP10_SCAN_CASE_ALL_STRING_KEY3");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 11 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP11_SCAN_CASE_ALL_STRING_KEY1", "GP11_SCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP11_SCAN_CASE_ALL_STRING_KEY2", "GP11_SCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP11_SCAN_CASE_ALL_STRING_KEY3", "GP11_SCAN_CASE_ALL_STRING_VALUE3");
  delete_keys.push_back("GP11_SCAN_CASE_ALL_STRING_KEY1");
  delete_keys.push_back("GP11_SCAN_CASE_ALL_STRING_KEY2");
  delete_keys.push_back("GP11_SCAN_CASE_ALL_STRING_KEY3");

  //Hash
  s = db.HSet("GP11_SCAN_CASE_ALL_HASH_KEY1", "GP11_SCAN_CASE_ALL_HASH_FIELD1", "GP11_SCAN_CASE_ALL_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP11_SCAN_CASE_ALL_HASH_KEY2", "GP11_SCAN_CASE_ALL_HASH_FIELD2", "GP11_SCAN_CASE_ALL_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP11_SCAN_CASE_ALL_HASH_KEY3", "GP11_SCAN_CASE_ALL_HASH_FIELD3", "GP11_SCAN_CASE_ALL_HASH_VALUE3", &int32_ret);
  delete_keys.push_back("GP11_SCAN_CASE_ALL_HASH_KEY1");
  delete_keys.push_back("GP11_SCAN_CASE_ALL_HASH_KEY2");
  delete_keys.push_back("GP11_SCAN_CASE_ALL_HASH_KEY3");

  //Set
  s = db.SAdd("GP11_SCAN_CASE_ALL_SET_KEY1", {"GP11_SCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP11_SCAN_CASE_ALL_SET_KEY2", {"GP11_SCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP11_SCAN_CASE_ALL_SET_KEY3", {"GP11_SCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);
  delete_keys.push_back("GP11_SCAN_CASE_ALL_SET_KEY1");
  delete_keys.push_back("GP11_SCAN_CASE_ALL_SET_KEY2");
  delete_keys.push_back("GP11_SCAN_CASE_ALL_SET_KEY3");

  //List
  s = db.LPush("GP11_SCAN_CASE_ALL_LIST_KEY1", {"GP11_SCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP11_SCAN_CASE_ALL_LIST_KEY2", {"GP11_SCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP11_SCAN_CASE_ALL_LIST_KEY3", {"GP11_SCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);
  delete_keys.push_back("GP11_SCAN_CASE_ALL_LIST_KEY1");
  delete_keys.push_back("GP11_SCAN_CASE_ALL_LIST_KEY2");
  delete_keys.push_back("GP11_SCAN_CASE_ALL_LIST_KEY3");

  //ZSet
  s = db.ZAdd("GP11_SCAN_CASE_ALL_ZSET_KEY1", {{1, "GP11_SCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP11_SCAN_CASE_ALL_ZSET_KEY2", {{1, "GP11_SCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP11_SCAN_CASE_ALL_ZSET_KEY3", {{1, "GP11_SCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.push_back("GP11_SCAN_CASE_ALL_ZSET_KEY1");
  delete_keys.push_back("GP11_SCAN_CASE_ALL_ZSET_KEY2");
  delete_keys.push_back("GP11_SCAN_CASE_ALL_ZSET_KEY3");

  cursor = 0;
  keys.clear();
  total_keys.clear();
  do {
    next_cursor = db.Scan(DataType::kAll, cursor, "GP11_SCAN_CASE_ALL_SET_*", 5, &keys);
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  } while (cursor);
  ASSERT_EQ(total_keys.size(), 3);
  ASSERT_EQ(total_keys[0], "GP11_SCAN_CASE_ALL_SET_KEY1");
  ASSERT_EQ(total_keys[1], "GP11_SCAN_CASE_ALL_SET_KEY2");
  ASSERT_EQ(total_keys[2], "GP11_SCAN_CASE_ALL_SET_KEY3");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 12 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP12_SCAN_CASE_ALL_STRING_KEY1", "GP12_SCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP12_SCAN_CASE_ALL_STRING_KEY2", "GP12_SCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP12_SCAN_CASE_ALL_STRING_KEY3", "GP12_SCAN_CASE_ALL_STRING_VALUE3");
  delete_keys.push_back("GP12_SCAN_CASE_ALL_STRING_KEY1");
  delete_keys.push_back("GP12_SCAN_CASE_ALL_STRING_KEY2");
  delete_keys.push_back("GP12_SCAN_CASE_ALL_STRING_KEY3");

  //Hash
  s = db.HSet("GP12_SCAN_CASE_ALL_HASH_KEY1", "GP12_SCAN_CASE_ALL_HASH_FIELD1", "GP12_SCAN_CASE_ALL_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP12_SCAN_CASE_ALL_HASH_KEY2", "GP12_SCAN_CASE_ALL_HASH_FIELD2", "GP12_SCAN_CASE_ALL_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP12_SCAN_CASE_ALL_HASH_KEY3", "GP12_SCAN_CASE_ALL_HASH_FIELD3", "GP12_SCAN_CASE_ALL_HASH_VALUE3", &int32_ret);
  delete_keys.push_back("GP12_SCAN_CASE_ALL_HASH_KEY1");
  delete_keys.push_back("GP12_SCAN_CASE_ALL_HASH_KEY2");
  delete_keys.push_back("GP12_SCAN_CASE_ALL_HASH_KEY3");

  //Set
  s = db.SAdd("GP12_SCAN_CASE_ALL_SET_KEY1", {"GP12_SCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP12_SCAN_CASE_ALL_SET_KEY2", {"GP12_SCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP12_SCAN_CASE_ALL_SET_KEY3", {"GP12_SCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);
  delete_keys.push_back("GP12_SCAN_CASE_ALL_SET_KEY1");
  delete_keys.push_back("GP12_SCAN_CASE_ALL_SET_KEY2");
  delete_keys.push_back("GP12_SCAN_CASE_ALL_SET_KEY3");

  //List
  s = db.LPush("GP12_SCAN_CASE_ALL_LIST_KEY1", {"GP12_SCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP12_SCAN_CASE_ALL_LIST_KEY2", {"GP12_SCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP12_SCAN_CASE_ALL_LIST_KEY3", {"GP12_SCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);
  delete_keys.push_back("GP12_SCAN_CASE_ALL_LIST_KEY1");
  delete_keys.push_back("GP12_SCAN_CASE_ALL_LIST_KEY2");
  delete_keys.push_back("GP12_SCAN_CASE_ALL_LIST_KEY3");

  //ZSet
  s = db.ZAdd("GP12_SCAN_CASE_ALL_ZSET_KEY1", {{1, "GP12_SCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP12_SCAN_CASE_ALL_ZSET_KEY2", {{1, "GP12_SCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP12_SCAN_CASE_ALL_ZSET_KEY3", {{1, "GP12_SCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.push_back("GP12_SCAN_CASE_ALL_ZSET_KEY1");
  delete_keys.push_back("GP12_SCAN_CASE_ALL_ZSET_KEY2");
  delete_keys.push_back("GP12_SCAN_CASE_ALL_ZSET_KEY3");

  cursor = 0;
  keys.clear();
  total_keys.clear();
  do {
    next_cursor = db.Scan(DataType::kAll, cursor, "GP12_SCAN_CASE_ALL_ZSET_*", 5, &keys);
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  } while (cursor);
  ASSERT_EQ(total_keys.size(), 3);
  ASSERT_EQ(total_keys[0], "GP12_SCAN_CASE_ALL_ZSET_KEY1");
  ASSERT_EQ(total_keys[1], "GP12_SCAN_CASE_ALL_ZSET_KEY2");
  ASSERT_EQ(total_keys[2], "GP12_SCAN_CASE_ALL_ZSET_KEY3");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 13 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP13_KEY1_SCAN_CASE_ALL_STRING", "GP13_SCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP13_KEY2_SCAN_CASE_ALL_STRING", "GP13_SCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP13_KEY3_SCAN_CASE_ALL_STRING", "GP13_SCAN_CASE_ALL_STRING_VALUE3");
  delete_keys.push_back("GP13_KEY1_SCAN_CASE_ALL_STRING");
  delete_keys.push_back("GP13_KEY2_SCAN_CASE_ALL_STRING");
  delete_keys.push_back("GP13_KEY3_SCAN_CASE_ALL_STRING");

  //Hash
  s = db.HSet("GP13_KEY1_SCAN_CASE_ALL_HASH", "GP13_SCAN_CASE_ALL_HASH_FIELD1", "GP13_SCAN_CASE_ALL_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP13_KEY2_SCAN_CASE_ALL_HASH", "GP13_SCAN_CASE_ALL_HASH_FIELD2", "GP13_SCAN_CASE_ALL_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP13_KEY3_SCAN_CASE_ALL_HASH", "GP13_SCAN_CASE_ALL_HASH_FIELD3", "GP13_SCAN_CASE_ALL_HASH_VALUE3", &int32_ret);
  delete_keys.push_back("GP13_KEY1_SCAN_CASE_ALL_HASH");
  delete_keys.push_back("GP13_KEY2_SCAN_CASE_ALL_HASH");
  delete_keys.push_back("GP13_KEY3_SCAN_CASE_ALL_HASH");

  //Set
  s = db.SAdd("GP13_KEY1_SCAN_CASE_ALL_SET", {"GP13_SCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP13_KEY2_SCAN_CASE_ALL_SET", {"GP13_SCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP13_KEY3_SCAN_CASE_ALL_SET", {"GP13_SCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);
  delete_keys.push_back("GP13_KEY1_SCAN_CASE_ALL_SET");
  delete_keys.push_back("GP13_KEY2_SCAN_CASE_ALL_SET");
  delete_keys.push_back("GP13_KEY3_SCAN_CASE_ALL_SET");

  //List
  s = db.LPush("GP13_KEY1_SCAN_CASE_ALL_LIST", {"GP13_SCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP13_KEY2_SCAN_CASE_ALL_LIST", {"GP13_SCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP13_KEY3_SCAN_CASE_ALL_LIST", {"GP13_SCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);
  delete_keys.push_back("GP13_KEY1_SCAN_CASE_ALL_LIST");
  delete_keys.push_back("GP13_KEY2_SCAN_CASE_ALL_LIST");
  delete_keys.push_back("GP13_KEY3_SCAN_CASE_ALL_LIST");

  //ZSet
  s = db.ZAdd("GP13_KEY1_SCAN_CASE_ALL_ZSET", {{1, "GP13_SCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP13_KEY2_SCAN_CASE_ALL_ZSET", {{1, "GP13_SCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP13_KEY3_SCAN_CASE_ALL_ZSET", {{1, "GP13_SCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.push_back("GP13_KEY1_SCAN_CASE_ALL_ZSET");
  delete_keys.push_back("GP13_KEY2_SCAN_CASE_ALL_ZSET");
  delete_keys.push_back("GP13_KEY3_SCAN_CASE_ALL_ZSET");

  cursor = 0;
  keys.clear();
  total_keys.clear();
  do {
    next_cursor = db.Scan(DataType::kAll, cursor, "GP13_KEY1_SCAN_CASE_ALL_*", 1, &keys);
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  } while (cursor);
  ASSERT_EQ(total_keys.size(), 5);
  ASSERT_EQ(total_keys[0], "GP13_KEY1_SCAN_CASE_ALL_STRING");
  ASSERT_EQ(total_keys[1], "GP13_KEY1_SCAN_CASE_ALL_HASH");
  ASSERT_EQ(total_keys[2], "GP13_KEY1_SCAN_CASE_ALL_SET");
  ASSERT_EQ(total_keys[3], "GP13_KEY1_SCAN_CASE_ALL_LIST");
  ASSERT_EQ(total_keys[4], "GP13_KEY1_SCAN_CASE_ALL_ZSET");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 14 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP14_KEY1_SCAN_CASE_ALL_STRING", "GP14_SCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP14_KEY2_SCAN_CASE_ALL_STRING", "GP14_SCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP14_KEY3_SCAN_CASE_ALL_STRING", "GP14_SCAN_CASE_ALL_STRING_VALUE3");
  delete_keys.push_back("GP14_KEY1_SCAN_CASE_ALL_STRING");
  delete_keys.push_back("GP14_KEY2_SCAN_CASE_ALL_STRING");
  delete_keys.push_back("GP14_KEY3_SCAN_CASE_ALL_STRING");

  //Hash
  s = db.HSet("GP14_KEY1_SCAN_CASE_ALL_HASH", "GP14_SCAN_CASE_ALL_HASH_FIELD1", "GP14_SCAN_CASE_ALL_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP14_KEY2_SCAN_CASE_ALL_HASH", "GP14_SCAN_CASE_ALL_HASH_FIELD2", "GP14_SCAN_CASE_ALL_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP14_KEY3_SCAN_CASE_ALL_HASH", "GP14_SCAN_CASE_ALL_HASH_FIELD3", "GP14_SCAN_CASE_ALL_HASH_VALUE3", &int32_ret);
  delete_keys.push_back("GP14_KEY1_SCAN_CASE_ALL_HASH");
  delete_keys.push_back("GP14_KEY2_SCAN_CASE_ALL_HASH");
  delete_keys.push_back("GP14_KEY3_SCAN_CASE_ALL_HASH");

  //Set
  s = db.SAdd("GP14_KEY1_SCAN_CASE_ALL_SET", {"GP14_SCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP14_KEY2_SCAN_CASE_ALL_SET", {"GP14_SCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP14_KEY3_SCAN_CASE_ALL_SET", {"GP14_SCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);
  delete_keys.push_back("GP14_KEY1_SCAN_CASE_ALL_SET");
  delete_keys.push_back("GP14_KEY2_SCAN_CASE_ALL_SET");
  delete_keys.push_back("GP14_KEY3_SCAN_CASE_ALL_SET");

  //List
  s = db.LPush("GP14_KEY1_SCAN_CASE_ALL_LIST", {"GP14_SCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP14_KEY2_SCAN_CASE_ALL_LIST", {"GP14_SCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP14_KEY3_SCAN_CASE_ALL_LIST", {"GP14_SCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);
  delete_keys.push_back("GP14_KEY1_SCAN_CASE_ALL_LIST");
  delete_keys.push_back("GP14_KEY2_SCAN_CASE_ALL_LIST");
  delete_keys.push_back("GP14_KEY3_SCAN_CASE_ALL_LIST");

  //ZSet
  s = db.ZAdd("GP14_KEY1_SCAN_CASE_ALL_ZSET", {{1, "GP14_SCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP14_KEY2_SCAN_CASE_ALL_ZSET", {{1, "GP14_SCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP14_KEY3_SCAN_CASE_ALL_ZSET", {{1, "GP14_SCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.push_back("GP14_KEY1_SCAN_CASE_ALL_ZSET");
  delete_keys.push_back("GP14_KEY2_SCAN_CASE_ALL_ZSET");
  delete_keys.push_back("GP14_KEY3_SCAN_CASE_ALL_ZSET");

  cursor = 0;
  keys.clear();
  total_keys.clear();
  do {
    next_cursor = db.Scan(DataType::kAll, cursor, "GP14_KEY1_SCAN_CASE_ALL_*", 5, &keys);
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  } while (cursor);
  ASSERT_EQ(total_keys.size(), 5);
  ASSERT_EQ(total_keys[0], "GP14_KEY1_SCAN_CASE_ALL_STRING");
  ASSERT_EQ(total_keys[1], "GP14_KEY1_SCAN_CASE_ALL_HASH");
  ASSERT_EQ(total_keys[2], "GP14_KEY1_SCAN_CASE_ALL_SET");
  ASSERT_EQ(total_keys[3], "GP14_KEY1_SCAN_CASE_ALL_LIST");
  ASSERT_EQ(total_keys[4], "GP14_KEY1_SCAN_CASE_ALL_ZSET");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 15 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP15_KEY1_SCAN_CASE_ALL_STRING", "GP15_SCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP15_KEY2_SCAN_CASE_ALL_STRING", "GP15_SCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP15_KEY3_SCAN_CASE_ALL_STRING", "GP15_SCAN_CASE_ALL_STRING_VALUE3");
  delete_keys.push_back("GP15_KEY1_SCAN_CASE_ALL_STRING");
  delete_keys.push_back("GP15_KEY2_SCAN_CASE_ALL_STRING");
  delete_keys.push_back("GP15_KEY3_SCAN_CASE_ALL_STRING");

  //Hash
  s = db.HSet("GP15_KEY1_SCAN_CASE_ALL_HASH", "GP15_SCAN_CASE_ALL_HASH_FIELD1", "GP15_SCAN_CASE_ALL_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP15_KEY2_SCAN_CASE_ALL_HASH", "GP15_SCAN_CASE_ALL_HASH_FIELD2", "GP15_SCAN_CASE_ALL_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP15_KEY3_SCAN_CASE_ALL_HASH", "GP15_SCAN_CASE_ALL_HASH_FIELD3", "GP15_SCAN_CASE_ALL_HASH_VALUE3", &int32_ret);
  delete_keys.push_back("GP15_KEY1_SCAN_CASE_ALL_HASH");
  delete_keys.push_back("GP15_KEY2_SCAN_CASE_ALL_HASH");
  delete_keys.push_back("GP15_KEY3_SCAN_CASE_ALL_HASH");

  //Set
  s = db.SAdd("GP15_KEY1_SCAN_CASE_ALL_SET", {"GP15_SCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP15_KEY2_SCAN_CASE_ALL_SET", {"GP15_SCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP15_KEY3_SCAN_CASE_ALL_SET", {"GP15_SCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);
  delete_keys.push_back("GP15_KEY1_SCAN_CASE_ALL_SET");
  delete_keys.push_back("GP15_KEY2_SCAN_CASE_ALL_SET");
  delete_keys.push_back("GP15_KEY3_SCAN_CASE_ALL_SET");

  //List
  s = db.LPush("GP15_KEY1_SCAN_CASE_ALL_LIST", {"GP15_SCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP15_KEY2_SCAN_CASE_ALL_LIST", {"GP15_SCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP15_KEY3_SCAN_CASE_ALL_LIST", {"GP15_SCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);
  delete_keys.push_back("GP15_KEY1_SCAN_CASE_ALL_LIST");
  delete_keys.push_back("GP15_KEY2_SCAN_CASE_ALL_LIST");
  delete_keys.push_back("GP15_KEY3_SCAN_CASE_ALL_LIST");

  //ZSet
  s = db.ZAdd("GP15_KEY1_SCAN_CASE_ALL_ZSET", {{1, "GP15_SCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP15_KEY2_SCAN_CASE_ALL_ZSET", {{1, "GP15_SCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP15_KEY3_SCAN_CASE_ALL_ZSET", {{1, "GP15_SCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.push_back("GP15_KEY1_SCAN_CASE_ALL_ZSET");
  delete_keys.push_back("GP15_KEY2_SCAN_CASE_ALL_ZSET");
  delete_keys.push_back("GP15_KEY3_SCAN_CASE_ALL_ZSET");

  cursor = 0;
  keys.clear();
  total_keys.clear();
  do {
    next_cursor = db.Scan(DataType::kAll, cursor, "GP15_KEY2_SCAN_CASE_ALL_*", 5, &keys);
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  } while (cursor);
  ASSERT_EQ(total_keys.size(), 5);
  ASSERT_EQ(total_keys[0], "GP15_KEY2_SCAN_CASE_ALL_STRING");
  ASSERT_EQ(total_keys[1], "GP15_KEY2_SCAN_CASE_ALL_HASH");
  ASSERT_EQ(total_keys[2], "GP15_KEY2_SCAN_CASE_ALL_SET");
  ASSERT_EQ(total_keys[3], "GP15_KEY2_SCAN_CASE_ALL_LIST");
  ASSERT_EQ(total_keys[4], "GP15_KEY2_SCAN_CASE_ALL_ZSET");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 16 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP16_KEY1_SCAN_CASE_ALL_STRING", "GP16_SCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP16_KEY2_SCAN_CASE_ALL_STRING", "GP16_SCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP16_KEY3_SCAN_CASE_ALL_STRING", "GP16_SCAN_CASE_ALL_STRING_VALUE3");
  delete_keys.push_back("GP16_KEY1_SCAN_CASE_ALL_STRING");
  delete_keys.push_back("GP16_KEY2_SCAN_CASE_ALL_STRING");
  delete_keys.push_back("GP16_KEY3_SCAN_CASE_ALL_STRING");

  //Hash
  s = db.HSet("GP16_KEY1_SCAN_CASE_ALL_HASH", "GP16_SCAN_CASE_ALL_HASH_FIELD1", "GP16_SCAN_CASE_ALL_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP16_KEY2_SCAN_CASE_ALL_HASH", "GP16_SCAN_CASE_ALL_HASH_FIELD2", "GP16_SCAN_CASE_ALL_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP16_KEY3_SCAN_CASE_ALL_HASH", "GP16_SCAN_CASE_ALL_HASH_FIELD3", "GP16_SCAN_CASE_ALL_HASH_VALUE3", &int32_ret);
  delete_keys.push_back("GP16_KEY1_SCAN_CASE_ALL_HASH");
  delete_keys.push_back("GP16_KEY2_SCAN_CASE_ALL_HASH");
  delete_keys.push_back("GP16_KEY3_SCAN_CASE_ALL_HASH");

  //Set
  s = db.SAdd("GP16_KEY1_SCAN_CASE_ALL_SET", {"GP16_SCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP16_KEY2_SCAN_CASE_ALL_SET", {"GP16_SCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP16_KEY3_SCAN_CASE_ALL_SET", {"GP16_SCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);
  delete_keys.push_back("GP16_KEY1_SCAN_CASE_ALL_SET");
  delete_keys.push_back("GP16_KEY2_SCAN_CASE_ALL_SET");
  delete_keys.push_back("GP16_KEY3_SCAN_CASE_ALL_SET");

  //List
  s = db.LPush("GP16_KEY1_SCAN_CASE_ALL_LIST", {"GP16_SCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP16_KEY2_SCAN_CASE_ALL_LIST", {"GP16_SCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP16_KEY3_SCAN_CASE_ALL_LIST", {"GP16_SCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);
  delete_keys.push_back("GP16_KEY1_SCAN_CASE_ALL_LIST");
  delete_keys.push_back("GP16_KEY2_SCAN_CASE_ALL_LIST");
  delete_keys.push_back("GP16_KEY3_SCAN_CASE_ALL_LIST");

  //ZSet
  s = db.ZAdd("GP16_KEY1_SCAN_CASE_ALL_ZSET", {{1, "GP16_SCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP16_KEY2_SCAN_CASE_ALL_ZSET", {{1, "GP16_SCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP16_KEY3_SCAN_CASE_ALL_ZSET", {{1, "GP16_SCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.push_back("GP16_KEY1_SCAN_CASE_ALL_ZSET");
  delete_keys.push_back("GP16_KEY2_SCAN_CASE_ALL_ZSET");
  delete_keys.push_back("GP16_KEY3_SCAN_CASE_ALL_ZSET");

  cursor = 0;
  keys.clear();
  total_keys.clear();
  do {
    next_cursor = db.Scan(DataType::kAll, cursor, "GP16_KEY3_SCAN_CASE_ALL_*", 5, &keys);
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  } while (cursor);
  ASSERT_EQ(total_keys.size(), 5);
  ASSERT_EQ(total_keys[0], "GP16_KEY3_SCAN_CASE_ALL_STRING");
  ASSERT_EQ(total_keys[1], "GP16_KEY3_SCAN_CASE_ALL_HASH");
  ASSERT_EQ(total_keys[2], "GP16_KEY3_SCAN_CASE_ALL_SET");
  ASSERT_EQ(total_keys[3], "GP16_KEY3_SCAN_CASE_ALL_LIST");
  ASSERT_EQ(total_keys[4], "GP16_KEY3_SCAN_CASE_ALL_ZSET");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);
}

// Scan
// Note: This test needs to execute at first because all of the data is
// predetermined.
TEST_F(KeysTest, ScanCaseSingleTest) {

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
  //String
  s = db.Set("GP1_KEY1_SCAN_CASE_SINGLE_STRING", "GP1_SCAN_CASE_SINGLE_STRING_VALUE1");
  s = db.Set("GP1_KEY2_SCAN_CASE_SINGLE_STRING", "GP1_SCAN_CASE_SINGLE_STRING_VALUE2");
  s = db.Set("GP1_KEY3_SCAN_CASE_SINGLE_STRING", "GP1_SCAN_CASE_SINGLE_STRING_VALUE3");
  s = db.Set("GP1_KEY4_SCAN_CASE_SINGLE_STRING", "GP1_SCAN_CASE_SINGLE_STRING_VALUE4");
  s = db.Set("GP1_KEY5_SCAN_CASE_SINGLE_STRING", "GP1_SCAN_CASE_SINGLE_STRING_VALUE5");
  s = db.Set("GP1_KEY6_SCAN_CASE_SINGLE_STRING", "GP1_SCAN_CASE_SINGLE_STRING_VALUE6");
  delete_keys.push_back("GP1_KEY1_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP1_KEY2_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP1_KEY3_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP1_KEY4_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP1_KEY5_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP1_KEY6_SCAN_CASE_SINGLE_STRING");

  //Hash
  s = db.HSet("GP1_KEY1_SCAN_CASE_SINGLE_HASH", "GP1_SCAN_CASE_SINGLE_HASH_FIELD1", "GP1_SCAN_CASE_SINGLE_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP1_KEY2_SCAN_CASE_SINGLE_HASH", "GP1_SCAN_CASE_SINGLE_HASH_FIELD2", "GP1_SCAN_CASE_SINGLE_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP1_KEY3_SCAN_CASE_SINGLE_HASH", "GP1_SCAN_CASE_SINGLE_HASH_FIELD3", "GP1_SCAN_CASE_SINGLE_HASH_VALUE3", &int32_ret);
  s = db.HSet("GP1_KEY4_SCAN_CASE_SINGLE_HASH", "GP1_SCAN_CASE_SINGLE_HASH_FIELD4", "GP1_SCAN_CASE_SINGLE_HASH_VALUE4", &int32_ret);
  s = db.HSet("GP1_KEY5_SCAN_CASE_SINGLE_HASH", "GP1_SCAN_CASE_SINGLE_HASH_FIELD5", "GP1_SCAN_CASE_SINGLE_HASH_VALUE5", &int32_ret);
  s = db.HSet("GP1_KEY6_SCAN_CASE_SINGLE_HASH", "GP1_SCAN_CASE_SINGLE_HASH_FIELD6", "GP1_SCAN_CASE_SINGLE_HASH_VALUE6", &int32_ret);
  delete_keys.push_back("GP1_KEY1_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP1_KEY2_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP1_KEY3_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP1_KEY4_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP1_KEY5_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP1_KEY6_SCAN_CASE_SINGLE_HASH");

  //Set
  s = db.SAdd("GP1_KEY1_SCAN_CASE_SINGLE_SET", {"GP1_SCAN_CASE_SINGLE_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP1_KEY2_SCAN_CASE_SINGLE_SET", {"GP1_SCAN_CASE_SINGLE_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP1_KEY3_SCAN_CASE_SINGLE_SET", {"GP1_SCAN_CASE_SINGLE_SET_MEMBER3"}, &int32_ret);
  s = db.SAdd("GP1_KEY4_SCAN_CASE_SINGLE_SET", {"GP1_SCAN_CASE_SINGLE_SET_MEMBER4"}, &int32_ret);
  s = db.SAdd("GP1_KEY5_SCAN_CASE_SINGLE_SET", {"GP1_SCAN_CASE_SINGLE_SET_MEMBER5"}, &int32_ret);
  s = db.SAdd("GP1_KEY6_SCAN_CASE_SINGLE_SET", {"GP1_SCAN_CASE_SINGLE_SET_MEMBER6"}, &int32_ret);
  delete_keys.push_back("GP1_KEY1_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP1_KEY2_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP1_KEY3_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP1_KEY4_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP1_KEY5_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP1_KEY6_SCAN_CASE_SINGLE_SET");

  //List
  s = db.LPush("GP1_KEY1_SCAN_CASE_SINGLE_LIST", {"GP1_SCAN_CASE_SINGLE_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP1_KEY2_SCAN_CASE_SINGLE_LIST", {"GP1_SCAN_CASE_SINGLE_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP1_KEY3_SCAN_CASE_SINGLE_LIST", {"GP1_SCAN_CASE_SINGLE_LIST_NODE3"}, &uint64_ret);
  s = db.LPush("GP1_KEY4_SCAN_CASE_SINGLE_LIST", {"GP1_SCAN_CASE_SINGLE_LIST_NODE4"}, &uint64_ret);
  s = db.LPush("GP1_KEY5_SCAN_CASE_SINGLE_LIST", {"GP1_SCAN_CASE_SINGLE_LIST_NODE5"}, &uint64_ret);
  s = db.LPush("GP1_KEY6_SCAN_CASE_SINGLE_LIST", {"GP1_SCAN_CASE_SINGLE_LIST_NODE6"}, &uint64_ret);
  delete_keys.push_back("GP1_KEY1_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP1_KEY2_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP1_KEY3_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP1_KEY4_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP1_KEY5_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP1_KEY6_SCAN_CASE_SINGLE_LIST");

  //ZSet
  s = db.ZAdd("GP1_KEY1_SCAN_CASE_SINGLE_ZSET", {{1, "GP1_SCAN_CASE_SINGLE_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP1_KEY2_SCAN_CASE_SINGLE_ZSET", {{1, "GP1_SCAN_CASE_SINGLE_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP1_KEY3_SCAN_CASE_SINGLE_ZSET", {{1, "GP1_SCAN_CASE_SINGLE_LIST_MEMBER3"}}, &int32_ret);
  s = db.ZAdd("GP1_KEY4_SCAN_CASE_SINGLE_ZSET", {{1, "GP1_SCAN_CASE_SINGLE_LIST_MEMBER4"}}, &int32_ret);
  s = db.ZAdd("GP1_KEY5_SCAN_CASE_SINGLE_ZSET", {{1, "GP1_SCAN_CASE_SINGLE_LIST_MEMBER5"}}, &int32_ret);
  s = db.ZAdd("GP1_KEY6_SCAN_CASE_SINGLE_ZSET", {{1, "GP1_SCAN_CASE_SINGLE_LIST_MEMBER6"}}, &int32_ret);
  delete_keys.push_back("GP1_KEY1_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP1_KEY2_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP1_KEY3_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP1_KEY4_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP1_KEY5_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP1_KEY6_SCAN_CASE_SINGLE_ZSET");

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

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 30);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 2 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP2_KEY1_SCAN_CASE_SINGLE_STRING", "GP2_SCAN_CASE_SINGLE_STRING_VALUE1");
  s = db.Set("GP2_KEY2_SCAN_CASE_SINGLE_STRING", "GP2_SCAN_CASE_SINGLE_STRING_VALUE2");
  s = db.Set("GP2_KEY3_SCAN_CASE_SINGLE_STRING", "GP2_SCAN_CASE_SINGLE_STRING_VALUE3");
  s = db.Set("GP2_KEY4_SCAN_CASE_SINGLE_STRING", "GP2_SCAN_CASE_SINGLE_STRING_VALUE4");
  s = db.Set("GP2_KEY5_SCAN_CASE_SINGLE_STRING", "GP2_SCAN_CASE_SINGLE_STRING_VALUE5");
  s = db.Set("GP2_KEY6_SCAN_CASE_SINGLE_STRING", "GP2_SCAN_CASE_SINGLE_STRING_VALUE6");
  delete_keys.push_back("GP2_KEY1_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP2_KEY2_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP2_KEY3_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP2_KEY4_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP2_KEY5_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP2_KEY6_SCAN_CASE_SINGLE_STRING");

  //Hash
  s = db.HSet("GP2_KEY1_SCAN_CASE_SINGLE_HASH", "GP2_SCAN_CASE_SINGLE_HASH_FIELD1", "GP2_SCAN_CASE_SINGLE_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP2_KEY2_SCAN_CASE_SINGLE_HASH", "GP2_SCAN_CASE_SINGLE_HASH_FIELD2", "GP2_SCAN_CASE_SINGLE_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP2_KEY3_SCAN_CASE_SINGLE_HASH", "GP2_SCAN_CASE_SINGLE_HASH_FIELD3", "GP2_SCAN_CASE_SINGLE_HASH_VALUE3", &int32_ret);
  s = db.HSet("GP2_KEY4_SCAN_CASE_SINGLE_HASH", "GP2_SCAN_CASE_SINGLE_HASH_FIELD4", "GP2_SCAN_CASE_SINGLE_HASH_VALUE4", &int32_ret);
  s = db.HSet("GP2_KEY5_SCAN_CASE_SINGLE_HASH", "GP2_SCAN_CASE_SINGLE_HASH_FIELD5", "GP2_SCAN_CASE_SINGLE_HASH_VALUE5", &int32_ret);
  s = db.HSet("GP2_KEY6_SCAN_CASE_SINGLE_HASH", "GP2_SCAN_CASE_SINGLE_HASH_FIELD6", "GP2_SCAN_CASE_SINGLE_HASH_VALUE6", &int32_ret);
  delete_keys.push_back("GP2_KEY1_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP2_KEY2_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP2_KEY3_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP2_KEY4_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP2_KEY5_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP2_KEY6_SCAN_CASE_SINGLE_HASH");

  //Set
  s = db.SAdd("GP2_KEY1_SCAN_CASE_SINGLE_SET", {"GP2_SCAN_CASE_SINGLE_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP2_KEY2_SCAN_CASE_SINGLE_SET", {"GP2_SCAN_CASE_SINGLE_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP2_KEY3_SCAN_CASE_SINGLE_SET", {"GP2_SCAN_CASE_SINGLE_SET_MEMBER3"}, &int32_ret);
  s = db.SAdd("GP2_KEY4_SCAN_CASE_SINGLE_SET", {"GP2_SCAN_CASE_SINGLE_SET_MEMBER4"}, &int32_ret);
  s = db.SAdd("GP2_KEY5_SCAN_CASE_SINGLE_SET", {"GP2_SCAN_CASE_SINGLE_SET_MEMBER5"}, &int32_ret);
  s = db.SAdd("GP2_KEY6_SCAN_CASE_SINGLE_SET", {"GP2_SCAN_CASE_SINGLE_SET_MEMBER6"}, &int32_ret);
  delete_keys.push_back("GP2_KEY1_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP2_KEY2_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP2_KEY3_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP2_KEY4_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP2_KEY5_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP2_KEY6_SCAN_CASE_SINGLE_SET");

  //List
  s = db.LPush("GP2_KEY1_SCAN_CASE_SINGLE_LIST", {"GP2_SCAN_CASE_SINGLE_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP2_KEY2_SCAN_CASE_SINGLE_LIST", {"GP2_SCAN_CASE_SINGLE_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP2_KEY3_SCAN_CASE_SINGLE_LIST", {"GP2_SCAN_CASE_SINGLE_LIST_NODE3"}, &uint64_ret);
  s = db.LPush("GP2_KEY4_SCAN_CASE_SINGLE_LIST", {"GP2_SCAN_CASE_SINGLE_LIST_NODE4"}, &uint64_ret);
  s = db.LPush("GP2_KEY5_SCAN_CASE_SINGLE_LIST", {"GP2_SCAN_CASE_SINGLE_LIST_NODE5"}, &uint64_ret);
  s = db.LPush("GP2_KEY6_SCAN_CASE_SINGLE_LIST", {"GP2_SCAN_CASE_SINGLE_LIST_NODE6"}, &uint64_ret);
  delete_keys.push_back("GP2_KEY1_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP2_KEY2_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP2_KEY3_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP2_KEY4_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP2_KEY5_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP2_KEY6_SCAN_CASE_SINGLE_LIST");

  //ZSet
  s = db.ZAdd("GP2_KEY1_SCAN_CASE_SINGLE_ZSET", {{1, "GP2_SCAN_CASE_SINGLE_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP2_KEY2_SCAN_CASE_SINGLE_ZSET", {{1, "GP2_SCAN_CASE_SINGLE_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP2_KEY3_SCAN_CASE_SINGLE_ZSET", {{1, "GP2_SCAN_CASE_SINGLE_LIST_MEMBER3"}}, &int32_ret);
  s = db.ZAdd("GP2_KEY4_SCAN_CASE_SINGLE_ZSET", {{1, "GP2_SCAN_CASE_SINGLE_LIST_MEMBER4"}}, &int32_ret);
  s = db.ZAdd("GP2_KEY5_SCAN_CASE_SINGLE_ZSET", {{1, "GP2_SCAN_CASE_SINGLE_LIST_MEMBER5"}}, &int32_ret);
  s = db.ZAdd("GP2_KEY6_SCAN_CASE_SINGLE_ZSET", {{1, "GP2_SCAN_CASE_SINGLE_LIST_MEMBER6"}}, &int32_ret);
  delete_keys.push_back("GP2_KEY1_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP2_KEY2_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP2_KEY3_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP2_KEY4_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP2_KEY5_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP2_KEY6_SCAN_CASE_SINGLE_ZSET");

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

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 30);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 3 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP3_KEY1_SCAN_CASE_SINGLE_STRING", "GP3_SCAN_CASE_SINGLE_STRING_VALUE1");
  s = db.Set("GP3_KEY2_SCAN_CASE_SINGLE_STRING", "GP3_SCAN_CASE_SINGLE_STRING_VALUE2");
  s = db.Set("GP3_KEY3_SCAN_CASE_SINGLE_STRING", "GP3_SCAN_CASE_SINGLE_STRING_VALUE3");
  s = db.Set("GP3_KEY4_SCAN_CASE_SINGLE_STRING", "GP3_SCAN_CASE_SINGLE_STRING_VALUE4");
  s = db.Set("GP3_KEY5_SCAN_CASE_SINGLE_STRING", "GP3_SCAN_CASE_SINGLE_STRING_VALUE5");
  s = db.Set("GP3_KEY6_SCAN_CASE_SINGLE_STRING", "GP3_SCAN_CASE_SINGLE_STRING_VALUE6");
  delete_keys.push_back("GP3_KEY1_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP3_KEY2_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP3_KEY3_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP3_KEY4_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP3_KEY5_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP3_KEY6_SCAN_CASE_SINGLE_STRING");

  //Hash
  s = db.HSet("GP3_KEY1_SCAN_CASE_SINGLE_HASH", "GP3_SCAN_CASE_SINGLE_HASH_FIELD1", "GP3_SCAN_CASE_SINGLE_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP3_KEY2_SCAN_CASE_SINGLE_HASH", "GP3_SCAN_CASE_SINGLE_HASH_FIELD2", "GP3_SCAN_CASE_SINGLE_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP3_KEY3_SCAN_CASE_SINGLE_HASH", "GP3_SCAN_CASE_SINGLE_HASH_FIELD3", "GP3_SCAN_CASE_SINGLE_HASH_VALUE3", &int32_ret);
  s = db.HSet("GP3_KEY4_SCAN_CASE_SINGLE_HASH", "GP3_SCAN_CASE_SINGLE_HASH_FIELD4", "GP3_SCAN_CASE_SINGLE_HASH_VALUE4", &int32_ret);
  s = db.HSet("GP3_KEY5_SCAN_CASE_SINGLE_HASH", "GP3_SCAN_CASE_SINGLE_HASH_FIELD5", "GP3_SCAN_CASE_SINGLE_HASH_VALUE5", &int32_ret);
  s = db.HSet("GP3_KEY6_SCAN_CASE_SINGLE_HASH", "GP3_SCAN_CASE_SINGLE_HASH_FIELD6", "GP3_SCAN_CASE_SINGLE_HASH_VALUE6", &int32_ret);
  delete_keys.push_back("GP3_KEY1_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP3_KEY2_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP3_KEY3_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP3_KEY4_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP3_KEY5_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP3_KEY6_SCAN_CASE_SINGLE_HASH");

  //Set
  s = db.SAdd("GP3_KEY1_SCAN_CASE_SINGLE_SET", {"GP3_SCAN_CASE_SINGLE_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP3_KEY2_SCAN_CASE_SINGLE_SET", {"GP3_SCAN_CASE_SINGLE_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP3_KEY3_SCAN_CASE_SINGLE_SET", {"GP3_SCAN_CASE_SINGLE_SET_MEMBER3"}, &int32_ret);
  s = db.SAdd("GP3_KEY4_SCAN_CASE_SINGLE_SET", {"GP3_SCAN_CASE_SINGLE_SET_MEMBER4"}, &int32_ret);
  s = db.SAdd("GP3_KEY5_SCAN_CASE_SINGLE_SET", {"GP3_SCAN_CASE_SINGLE_SET_MEMBER5"}, &int32_ret);
  s = db.SAdd("GP3_KEY6_SCAN_CASE_SINGLE_SET", {"GP3_SCAN_CASE_SINGLE_SET_MEMBER6"}, &int32_ret);
  delete_keys.push_back("GP3_KEY1_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP3_KEY2_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP3_KEY3_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP3_KEY4_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP3_KEY5_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP3_KEY6_SCAN_CASE_SINGLE_SET");

  //List
  s = db.LPush("GP3_KEY1_SCAN_CASE_SINGLE_LIST", {"GP3_SCAN_CASE_SINGLE_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP3_KEY2_SCAN_CASE_SINGLE_LIST", {"GP3_SCAN_CASE_SINGLE_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP3_KEY3_SCAN_CASE_SINGLE_LIST", {"GP3_SCAN_CASE_SINGLE_LIST_NODE3"}, &uint64_ret);
  s = db.LPush("GP3_KEY4_SCAN_CASE_SINGLE_LIST", {"GP3_SCAN_CASE_SINGLE_LIST_NODE4"}, &uint64_ret);
  s = db.LPush("GP3_KEY5_SCAN_CASE_SINGLE_LIST", {"GP3_SCAN_CASE_SINGLE_LIST_NODE5"}, &uint64_ret);
  s = db.LPush("GP3_KEY6_SCAN_CASE_SINGLE_LIST", {"GP3_SCAN_CASE_SINGLE_LIST_NODE6"}, &uint64_ret);
  delete_keys.push_back("GP3_KEY1_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP3_KEY2_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP3_KEY3_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP3_KEY4_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP3_KEY5_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP3_KEY6_SCAN_CASE_SINGLE_LIST");

  //ZSet
  s = db.ZAdd("GP3_KEY1_SCAN_CASE_SINGLE_ZSET", {{1, "GP3_SCAN_CASE_SINGLE_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP3_KEY2_SCAN_CASE_SINGLE_ZSET", {{1, "GP3_SCAN_CASE_SINGLE_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP3_KEY3_SCAN_CASE_SINGLE_ZSET", {{1, "GP3_SCAN_CASE_SINGLE_LIST_MEMBER3"}}, &int32_ret);
  s = db.ZAdd("GP3_KEY4_SCAN_CASE_SINGLE_ZSET", {{1, "GP3_SCAN_CASE_SINGLE_LIST_MEMBER4"}}, &int32_ret);
  s = db.ZAdd("GP3_KEY5_SCAN_CASE_SINGLE_ZSET", {{1, "GP3_SCAN_CASE_SINGLE_LIST_MEMBER5"}}, &int32_ret);
  s = db.ZAdd("GP3_KEY6_SCAN_CASE_SINGLE_ZSET", {{1, "GP3_SCAN_CASE_SINGLE_LIST_MEMBER6"}}, &int32_ret);
  delete_keys.push_back("GP3_KEY1_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP3_KEY2_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP3_KEY3_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP3_KEY4_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP3_KEY5_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP3_KEY6_SCAN_CASE_SINGLE_ZSET");

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

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 30);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 4 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP4_KEY1_SCAN_CASE_SINGLE_STRING", "GP4_SCAN_CASE_SINGLE_STRING_VALUE1");
  s = db.Set("GP4_KEY2_SCAN_CASE_SINGLE_STRING", "GP4_SCAN_CASE_SINGLE_STRING_VALUE2");
  s = db.Set("GP4_KEY3_SCAN_CASE_SINGLE_STRING", "GP4_SCAN_CASE_SINGLE_STRING_VALUE3");
  s = db.Set("GP4_KEY4_SCAN_CASE_SINGLE_STRING", "GP4_SCAN_CASE_SINGLE_STRING_VALUE4");
  s = db.Set("GP4_KEY5_SCAN_CASE_SINGLE_STRING", "GP4_SCAN_CASE_SINGLE_STRING_VALUE5");
  s = db.Set("GP4_KEY6_SCAN_CASE_SINGLE_STRING", "GP4_SCAN_CASE_SINGLE_STRING_VALUE6");
  delete_keys.push_back("GP4_KEY1_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP4_KEY2_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP4_KEY3_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP4_KEY4_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP4_KEY5_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP4_KEY6_SCAN_CASE_SINGLE_STRING");

  //Hash
  s = db.HSet("GP4_KEY1_SCAN_CASE_SINGLE_HASH", "GP4_SCAN_CASE_SINGLE_HASH_FIELD1", "GP4_SCAN_CASE_SINGLE_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP4_KEY2_SCAN_CASE_SINGLE_HASH", "GP4_SCAN_CASE_SINGLE_HASH_FIELD2", "GP4_SCAN_CASE_SINGLE_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP4_KEY3_SCAN_CASE_SINGLE_HASH", "GP4_SCAN_CASE_SINGLE_HASH_FIELD3", "GP4_SCAN_CASE_SINGLE_HASH_VALUE3", &int32_ret);
  s = db.HSet("GP4_KEY4_SCAN_CASE_SINGLE_HASH", "GP4_SCAN_CASE_SINGLE_HASH_FIELD4", "GP4_SCAN_CASE_SINGLE_HASH_VALUE4", &int32_ret);
  s = db.HSet("GP4_KEY5_SCAN_CASE_SINGLE_HASH", "GP4_SCAN_CASE_SINGLE_HASH_FIELD5", "GP4_SCAN_CASE_SINGLE_HASH_VALUE5", &int32_ret);
  s = db.HSet("GP4_KEY6_SCAN_CASE_SINGLE_HASH", "GP4_SCAN_CASE_SINGLE_HASH_FIELD6", "GP4_SCAN_CASE_SINGLE_HASH_VALUE6", &int32_ret);
  delete_keys.push_back("GP4_KEY1_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP4_KEY2_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP4_KEY3_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP4_KEY4_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP4_KEY5_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP4_KEY6_SCAN_CASE_SINGLE_HASH");

  //Set
  s = db.SAdd("GP4_KEY1_SCAN_CASE_SINGLE_SET", {"GP4_SCAN_CASE_SINGLE_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP4_KEY2_SCAN_CASE_SINGLE_SET", {"GP4_SCAN_CASE_SINGLE_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP4_KEY3_SCAN_CASE_SINGLE_SET", {"GP4_SCAN_CASE_SINGLE_SET_MEMBER3"}, &int32_ret);
  s = db.SAdd("GP4_KEY4_SCAN_CASE_SINGLE_SET", {"GP4_SCAN_CASE_SINGLE_SET_MEMBER4"}, &int32_ret);
  s = db.SAdd("GP4_KEY5_SCAN_CASE_SINGLE_SET", {"GP4_SCAN_CASE_SINGLE_SET_MEMBER5"}, &int32_ret);
  s = db.SAdd("GP4_KEY6_SCAN_CASE_SINGLE_SET", {"GP4_SCAN_CASE_SINGLE_SET_MEMBER6"}, &int32_ret);
  delete_keys.push_back("GP4_KEY1_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP4_KEY2_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP4_KEY3_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP4_KEY4_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP4_KEY5_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP4_KEY6_SCAN_CASE_SINGLE_SET");

  //List
  s = db.LPush("GP4_KEY1_SCAN_CASE_SINGLE_LIST", {"GP4_SCAN_CASE_SINGLE_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP4_KEY2_SCAN_CASE_SINGLE_LIST", {"GP4_SCAN_CASE_SINGLE_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP4_KEY3_SCAN_CASE_SINGLE_LIST", {"GP4_SCAN_CASE_SINGLE_LIST_NODE3"}, &uint64_ret);
  s = db.LPush("GP4_KEY4_SCAN_CASE_SINGLE_LIST", {"GP4_SCAN_CASE_SINGLE_LIST_NODE4"}, &uint64_ret);
  s = db.LPush("GP4_KEY5_SCAN_CASE_SINGLE_LIST", {"GP4_SCAN_CASE_SINGLE_LIST_NODE5"}, &uint64_ret);
  s = db.LPush("GP4_KEY6_SCAN_CASE_SINGLE_LIST", {"GP4_SCAN_CASE_SINGLE_LIST_NODE6"}, &uint64_ret);
  delete_keys.push_back("GP4_KEY1_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP4_KEY2_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP4_KEY3_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP4_KEY4_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP4_KEY5_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP4_KEY6_SCAN_CASE_SINGLE_LIST");

  //ZSet
  s = db.ZAdd("GP4_KEY1_SCAN_CASE_SINGLE_ZSET", {{1, "GP4_SCAN_CASE_SINGLE_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP4_KEY2_SCAN_CASE_SINGLE_ZSET", {{1, "GP4_SCAN_CASE_SINGLE_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP4_KEY3_SCAN_CASE_SINGLE_ZSET", {{1, "GP4_SCAN_CASE_SINGLE_LIST_MEMBER3"}}, &int32_ret);
  s = db.ZAdd("GP4_KEY4_SCAN_CASE_SINGLE_ZSET", {{1, "GP4_SCAN_CASE_SINGLE_LIST_MEMBER4"}}, &int32_ret);
  s = db.ZAdd("GP4_KEY5_SCAN_CASE_SINGLE_ZSET", {{1, "GP4_SCAN_CASE_SINGLE_LIST_MEMBER5"}}, &int32_ret);
  s = db.ZAdd("GP4_KEY6_SCAN_CASE_SINGLE_ZSET", {{1, "GP4_SCAN_CASE_SINGLE_LIST_MEMBER6"}}, &int32_ret);
  delete_keys.push_back("GP4_KEY1_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP4_KEY2_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP4_KEY3_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP4_KEY4_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP4_KEY5_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP4_KEY6_SCAN_CASE_SINGLE_ZSET");

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

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 30);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 5 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP5_KEY1_SCAN_CASE_SINGLE_STRING", "GP5_SCAN_CASE_SINGLE_STRING_VALUE1");
  s = db.Set("GP5_KEY2_SCAN_CASE_SINGLE_STRING", "GP5_SCAN_CASE_SINGLE_STRING_VALUE2");
  s = db.Set("GP5_KEY3_SCAN_CASE_SINGLE_STRING", "GP5_SCAN_CASE_SINGLE_STRING_VALUE3");
  s = db.Set("GP5_KEY4_SCAN_CASE_SINGLE_STRING", "GP5_SCAN_CASE_SINGLE_STRING_VALUE4");
  s = db.Set("GP5_KEY5_SCAN_CASE_SINGLE_STRING", "GP5_SCAN_CASE_SINGLE_STRING_VALUE5");
  s = db.Set("GP5_KEY6_SCAN_CASE_SINGLE_STRING", "GP5_SCAN_CASE_SINGLE_STRING_VALUE6");
  delete_keys.push_back("GP5_KEY1_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP5_KEY2_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP5_KEY3_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP5_KEY4_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP5_KEY5_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP5_KEY6_SCAN_CASE_SINGLE_STRING");

  //Hash
  s = db.HSet("GP5_KEY1_SCAN_CASE_SINGLE_HASH", "GP5_SCAN_CASE_SINGLE_HASH_FIELD1", "GP5_SCAN_CASE_SINGLE_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP5_KEY2_SCAN_CASE_SINGLE_HASH", "GP5_SCAN_CASE_SINGLE_HASH_FIELD2", "GP5_SCAN_CASE_SINGLE_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP5_KEY3_SCAN_CASE_SINGLE_HASH", "GP5_SCAN_CASE_SINGLE_HASH_FIELD3", "GP5_SCAN_CASE_SINGLE_HASH_VALUE3", &int32_ret);
  s = db.HSet("GP5_KEY4_SCAN_CASE_SINGLE_HASH", "GP5_SCAN_CASE_SINGLE_HASH_FIELD4", "GP5_SCAN_CASE_SINGLE_HASH_VALUE4", &int32_ret);
  s = db.HSet("GP5_KEY5_SCAN_CASE_SINGLE_HASH", "GP5_SCAN_CASE_SINGLE_HASH_FIELD5", "GP5_SCAN_CASE_SINGLE_HASH_VALUE5", &int32_ret);
  s = db.HSet("GP5_KEY6_SCAN_CASE_SINGLE_HASH", "GP5_SCAN_CASE_SINGLE_HASH_FIELD6", "GP5_SCAN_CASE_SINGLE_HASH_VALUE6", &int32_ret);
  delete_keys.push_back("GP5_KEY1_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP5_KEY2_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP5_KEY3_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP5_KEY4_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP5_KEY5_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP5_KEY6_SCAN_CASE_SINGLE_HASH");

  //Set
  s = db.SAdd("GP5_KEY1_SCAN_CASE_SINGLE_SET", {"GP5_SCAN_CASE_SINGLE_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP5_KEY2_SCAN_CASE_SINGLE_SET", {"GP5_SCAN_CASE_SINGLE_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP5_KEY3_SCAN_CASE_SINGLE_SET", {"GP5_SCAN_CASE_SINGLE_SET_MEMBER3"}, &int32_ret);
  s = db.SAdd("GP5_KEY4_SCAN_CASE_SINGLE_SET", {"GP5_SCAN_CASE_SINGLE_SET_MEMBER4"}, &int32_ret);
  s = db.SAdd("GP5_KEY5_SCAN_CASE_SINGLE_SET", {"GP5_SCAN_CASE_SINGLE_SET_MEMBER5"}, &int32_ret);
  s = db.SAdd("GP5_KEY6_SCAN_CASE_SINGLE_SET", {"GP5_SCAN_CASE_SINGLE_SET_MEMBER6"}, &int32_ret);
  delete_keys.push_back("GP5_KEY1_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP5_KEY2_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP5_KEY3_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP5_KEY4_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP5_KEY5_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP5_KEY6_SCAN_CASE_SINGLE_SET");

  //List
  s = db.LPush("GP5_KEY1_SCAN_CASE_SINGLE_LIST", {"GP5_SCAN_CASE_SINGLE_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP5_KEY2_SCAN_CASE_SINGLE_LIST", {"GP5_SCAN_CASE_SINGLE_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP5_KEY3_SCAN_CASE_SINGLE_LIST", {"GP5_SCAN_CASE_SINGLE_LIST_NODE3"}, &uint64_ret);
  s = db.LPush("GP5_KEY4_SCAN_CASE_SINGLE_LIST", {"GP5_SCAN_CASE_SINGLE_LIST_NODE4"}, &uint64_ret);
  s = db.LPush("GP5_KEY5_SCAN_CASE_SINGLE_LIST", {"GP5_SCAN_CASE_SINGLE_LIST_NODE5"}, &uint64_ret);
  s = db.LPush("GP5_KEY6_SCAN_CASE_SINGLE_LIST", {"GP5_SCAN_CASE_SINGLE_LIST_NODE6"}, &uint64_ret);
  delete_keys.push_back("GP5_KEY1_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP5_KEY2_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP5_KEY3_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP5_KEY4_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP5_KEY5_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP5_KEY6_SCAN_CASE_SINGLE_LIST");

  //ZSet
  s = db.ZAdd("GP5_KEY1_SCAN_CASE_SINGLE_ZSET", {{1, "GP5_SCAN_CASE_SINGLE_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP5_KEY2_SCAN_CASE_SINGLE_ZSET", {{1, "GP5_SCAN_CASE_SINGLE_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP5_KEY3_SCAN_CASE_SINGLE_ZSET", {{1, "GP5_SCAN_CASE_SINGLE_LIST_MEMBER3"}}, &int32_ret);
  s = db.ZAdd("GP5_KEY4_SCAN_CASE_SINGLE_ZSET", {{1, "GP5_SCAN_CASE_SINGLE_LIST_MEMBER4"}}, &int32_ret);
  s = db.ZAdd("GP5_KEY5_SCAN_CASE_SINGLE_ZSET", {{1, "GP5_SCAN_CASE_SINGLE_LIST_MEMBER5"}}, &int32_ret);
  s = db.ZAdd("GP5_KEY6_SCAN_CASE_SINGLE_ZSET", {{1, "GP5_SCAN_CASE_SINGLE_LIST_MEMBER6"}}, &int32_ret);
  delete_keys.push_back("GP5_KEY1_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP5_KEY2_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP5_KEY3_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP5_KEY4_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP5_KEY5_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP5_KEY6_SCAN_CASE_SINGLE_ZSET");

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

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 30);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 6 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP6_KEY1_SCAN_CASE_SINGLE_STRING", "GP6_SCAN_CASE_SINGLE_STRING_VALUE1");
  s = db.Set("GP6_KEY2_SCAN_CASE_SINGLE_STRING", "GP6_SCAN_CASE_SINGLE_STRING_VALUE2");
  s = db.Set("GP6_KEY3_SCAN_CASE_SINGLE_STRING", "GP6_SCAN_CASE_SINGLE_STRING_VALUE3");
  s = db.Set("GP6_KEY4_SCAN_CASE_SINGLE_STRING", "GP6_SCAN_CASE_SINGLE_STRING_VALUE4");
  s = db.Set("GP6_KEY5_SCAN_CASE_SINGLE_STRING", "GP6_SCAN_CASE_SINGLE_STRING_VALUE5");
  s = db.Set("GP6_KEY6_SCAN_CASE_SINGLE_STRING", "GP6_SCAN_CASE_SINGLE_STRING_VALUE6");
  delete_keys.push_back("GP6_KEY1_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP6_KEY2_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP6_KEY3_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP6_KEY4_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP6_KEY5_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP6_KEY6_SCAN_CASE_SINGLE_STRING");

  //Hash
  s = db.HSet("GP6_KEY1_SCAN_CASE_SINGLE_HASH", "GP6_SCAN_CASE_SINGLE_HASH_FIELD1", "GP6_SCAN_CASE_SINGLE_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP6_KEY2_SCAN_CASE_SINGLE_HASH", "GP6_SCAN_CASE_SINGLE_HASH_FIELD2", "GP6_SCAN_CASE_SINGLE_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP6_KEY3_SCAN_CASE_SINGLE_HASH", "GP6_SCAN_CASE_SINGLE_HASH_FIELD3", "GP6_SCAN_CASE_SINGLE_HASH_VALUE3", &int32_ret);
  s = db.HSet("GP6_KEY4_SCAN_CASE_SINGLE_HASH", "GP6_SCAN_CASE_SINGLE_HASH_FIELD4", "GP6_SCAN_CASE_SINGLE_HASH_VALUE4", &int32_ret);
  s = db.HSet("GP6_KEY5_SCAN_CASE_SINGLE_HASH", "GP6_SCAN_CASE_SINGLE_HASH_FIELD5", "GP6_SCAN_CASE_SINGLE_HASH_VALUE5", &int32_ret);
  s = db.HSet("GP6_KEY6_SCAN_CASE_SINGLE_HASH", "GP6_SCAN_CASE_SINGLE_HASH_FIELD6", "GP6_SCAN_CASE_SINGLE_HASH_VALUE6", &int32_ret);
  delete_keys.push_back("GP6_KEY1_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP6_KEY2_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP6_KEY3_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP6_KEY4_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP6_KEY5_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP6_KEY6_SCAN_CASE_SINGLE_HASH");

  //Set
  s = db.SAdd("GP6_KEY1_SCAN_CASE_SINGLE_SET", {"GP6_SCAN_CASE_SINGLE_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP6_KEY2_SCAN_CASE_SINGLE_SET", {"GP6_SCAN_CASE_SINGLE_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP6_KEY3_SCAN_CASE_SINGLE_SET", {"GP6_SCAN_CASE_SINGLE_SET_MEMBER3"}, &int32_ret);
  s = db.SAdd("GP6_KEY4_SCAN_CASE_SINGLE_SET", {"GP6_SCAN_CASE_SINGLE_SET_MEMBER4"}, &int32_ret);
  s = db.SAdd("GP6_KEY5_SCAN_CASE_SINGLE_SET", {"GP6_SCAN_CASE_SINGLE_SET_MEMBER5"}, &int32_ret);
  s = db.SAdd("GP6_KEY6_SCAN_CASE_SINGLE_SET", {"GP6_SCAN_CASE_SINGLE_SET_MEMBER6"}, &int32_ret);
  delete_keys.push_back("GP6_KEY1_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP6_KEY2_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP6_KEY3_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP6_KEY4_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP6_KEY5_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP6_KEY6_SCAN_CASE_SINGLE_SET");

  //List
  s = db.LPush("GP6_KEY1_SCAN_CASE_SINGLE_LIST", {"GP6_SCAN_CASE_SINGLE_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP6_KEY2_SCAN_CASE_SINGLE_LIST", {"GP6_SCAN_CASE_SINGLE_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP6_KEY3_SCAN_CASE_SINGLE_LIST", {"GP6_SCAN_CASE_SINGLE_LIST_NODE3"}, &uint64_ret);
  s = db.LPush("GP6_KEY4_SCAN_CASE_SINGLE_LIST", {"GP6_SCAN_CASE_SINGLE_LIST_NODE4"}, &uint64_ret);
  s = db.LPush("GP6_KEY5_SCAN_CASE_SINGLE_LIST", {"GP6_SCAN_CASE_SINGLE_LIST_NODE5"}, &uint64_ret);
  s = db.LPush("GP6_KEY6_SCAN_CASE_SINGLE_LIST", {"GP6_SCAN_CASE_SINGLE_LIST_NODE6"}, &uint64_ret);
  delete_keys.push_back("GP6_KEY1_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP6_KEY2_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP6_KEY3_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP6_KEY4_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP6_KEY5_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP6_KEY6_SCAN_CASE_SINGLE_LIST");

  //ZSet
  s = db.ZAdd("GP6_KEY1_SCAN_CASE_SINGLE_ZSET", {{1, "GP6_SCAN_CASE_SINGLE_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP6_KEY2_SCAN_CASE_SINGLE_ZSET", {{1, "GP6_SCAN_CASE_SINGLE_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP6_KEY3_SCAN_CASE_SINGLE_ZSET", {{1, "GP6_SCAN_CASE_SINGLE_LIST_MEMBER3"}}, &int32_ret);
  s = db.ZAdd("GP6_KEY4_SCAN_CASE_SINGLE_ZSET", {{1, "GP6_SCAN_CASE_SINGLE_LIST_MEMBER4"}}, &int32_ret);
  s = db.ZAdd("GP6_KEY5_SCAN_CASE_SINGLE_ZSET", {{1, "GP6_SCAN_CASE_SINGLE_LIST_MEMBER5"}}, &int32_ret);
  s = db.ZAdd("GP6_KEY6_SCAN_CASE_SINGLE_ZSET", {{1, "GP6_SCAN_CASE_SINGLE_LIST_MEMBER6"}}, &int32_ret);
  delete_keys.push_back("GP6_KEY1_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP6_KEY2_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP6_KEY3_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP6_KEY4_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP6_KEY5_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP6_KEY6_SCAN_CASE_SINGLE_ZSET");

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

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 30);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 7 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP7_KEY1_SCAN_CASE_SINGLE_STRING", "GP7_SCAN_CASE_SINGLE_STRING_VALUE1");
  s = db.Set("GP7_KEY2_SCAN_CASE_SINGLE_STRING", "GP7_SCAN_CASE_SINGLE_STRING_VALUE2");
  s = db.Set("GP7_KEY3_SCAN_CASE_SINGLE_STRING", "GP7_SCAN_CASE_SINGLE_STRING_VALUE3");
  s = db.Set("GP7_KEY4_SCAN_CASE_SINGLE_STRING", "GP7_SCAN_CASE_SINGLE_STRING_VALUE4");
  s = db.Set("GP7_KEY5_SCAN_CASE_SINGLE_STRING", "GP7_SCAN_CASE_SINGLE_STRING_VALUE5");
  s = db.Set("GP7_KEY6_SCAN_CASE_SINGLE_STRING", "GP7_SCAN_CASE_SINGLE_STRING_VALUE6");
  delete_keys.push_back("GP7_KEY1_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP7_KEY2_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP7_KEY3_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP7_KEY4_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP7_KEY5_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP7_KEY6_SCAN_CASE_SINGLE_STRING");

  //Hash
  s = db.HSet("GP7_KEY1_SCAN_CASE_SINGLE_HASH", "GP7_SCAN_CASE_SINGLE_HASH_FIELD1", "GP7_SCAN_CASE_SINGLE_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP7_KEY2_SCAN_CASE_SINGLE_HASH", "GP7_SCAN_CASE_SINGLE_HASH_FIELD2", "GP7_SCAN_CASE_SINGLE_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP7_KEY3_SCAN_CASE_SINGLE_HASH", "GP7_SCAN_CASE_SINGLE_HASH_FIELD3", "GP7_SCAN_CASE_SINGLE_HASH_VALUE3", &int32_ret);
  s = db.HSet("GP7_KEY4_SCAN_CASE_SINGLE_HASH", "GP7_SCAN_CASE_SINGLE_HASH_FIELD4", "GP7_SCAN_CASE_SINGLE_HASH_VALUE4", &int32_ret);
  s = db.HSet("GP7_KEY5_SCAN_CASE_SINGLE_HASH", "GP7_SCAN_CASE_SINGLE_HASH_FIELD5", "GP7_SCAN_CASE_SINGLE_HASH_VALUE5", &int32_ret);
  s = db.HSet("GP7_KEY6_SCAN_CASE_SINGLE_HASH", "GP7_SCAN_CASE_SINGLE_HASH_FIELD6", "GP7_SCAN_CASE_SINGLE_HASH_VALUE6", &int32_ret);
  delete_keys.push_back("GP7_KEY1_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP7_KEY2_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP7_KEY3_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP7_KEY4_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP7_KEY5_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP7_KEY6_SCAN_CASE_SINGLE_HASH");

  //Set
  s = db.SAdd("GP7_KEY1_SCAN_CASE_SINGLE_SET", {"GP7_SCAN_CASE_SINGLE_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP7_KEY2_SCAN_CASE_SINGLE_SET", {"GP7_SCAN_CASE_SINGLE_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP7_KEY3_SCAN_CASE_SINGLE_SET", {"GP7_SCAN_CASE_SINGLE_SET_MEMBER3"}, &int32_ret);
  s = db.SAdd("GP7_KEY4_SCAN_CASE_SINGLE_SET", {"GP7_SCAN_CASE_SINGLE_SET_MEMBER4"}, &int32_ret);
  s = db.SAdd("GP7_KEY5_SCAN_CASE_SINGLE_SET", {"GP7_SCAN_CASE_SINGLE_SET_MEMBER5"}, &int32_ret);
  s = db.SAdd("GP7_KEY6_SCAN_CASE_SINGLE_SET", {"GP7_SCAN_CASE_SINGLE_SET_MEMBER6"}, &int32_ret);
  delete_keys.push_back("GP7_KEY1_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP7_KEY2_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP7_KEY3_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP7_KEY4_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP7_KEY5_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP7_KEY6_SCAN_CASE_SINGLE_SET");

  //List
  s = db.LPush("GP7_KEY1_SCAN_CASE_SINGLE_LIST", {"GP7_SCAN_CASE_SINGLE_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP7_KEY2_SCAN_CASE_SINGLE_LIST", {"GP7_SCAN_CASE_SINGLE_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP7_KEY3_SCAN_CASE_SINGLE_LIST", {"GP7_SCAN_CASE_SINGLE_LIST_NODE3"}, &uint64_ret);
  s = db.LPush("GP7_KEY4_SCAN_CASE_SINGLE_LIST", {"GP7_SCAN_CASE_SINGLE_LIST_NODE4"}, &uint64_ret);
  s = db.LPush("GP7_KEY5_SCAN_CASE_SINGLE_LIST", {"GP7_SCAN_CASE_SINGLE_LIST_NODE5"}, &uint64_ret);
  s = db.LPush("GP7_KEY6_SCAN_CASE_SINGLE_LIST", {"GP7_SCAN_CASE_SINGLE_LIST_NODE6"}, &uint64_ret);
  delete_keys.push_back("GP7_KEY1_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP7_KEY2_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP7_KEY3_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP7_KEY4_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP7_KEY5_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP7_KEY6_SCAN_CASE_SINGLE_LIST");

  //ZSet
  s = db.ZAdd("GP7_KEY1_SCAN_CASE_SINGLE_ZSET", {{1, "GP7_SCAN_CASE_SINGLE_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP7_KEY2_SCAN_CASE_SINGLE_ZSET", {{1, "GP7_SCAN_CASE_SINGLE_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP7_KEY3_SCAN_CASE_SINGLE_ZSET", {{1, "GP7_SCAN_CASE_SINGLE_LIST_MEMBER3"}}, &int32_ret);
  s = db.ZAdd("GP7_KEY4_SCAN_CASE_SINGLE_ZSET", {{1, "GP7_SCAN_CASE_SINGLE_LIST_MEMBER4"}}, &int32_ret);
  s = db.ZAdd("GP7_KEY5_SCAN_CASE_SINGLE_ZSET", {{1, "GP7_SCAN_CASE_SINGLE_LIST_MEMBER5"}}, &int32_ret);
  s = db.ZAdd("GP7_KEY6_SCAN_CASE_SINGLE_ZSET", {{1, "GP7_SCAN_CASE_SINGLE_LIST_MEMBER6"}}, &int32_ret);
  delete_keys.push_back("GP7_KEY1_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP7_KEY2_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP7_KEY3_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP7_KEY4_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP7_KEY5_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP7_KEY6_SCAN_CASE_SINGLE_ZSET");

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

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 30);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 8 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP8_KEY1_SCAN_CASE_SINGLE_STRING", "GP8_SCAN_CASE_SINGLE_STRING_VALUE1");
  s = db.Set("GP8_KEY2_SCAN_CASE_SINGLE_STRING", "GP8_SCAN_CASE_SINGLE_STRING_VALUE2");
  s = db.Set("GP8_KEY3_SCAN_CASE_SINGLE_STRING", "GP8_SCAN_CASE_SINGLE_STRING_VALUE3");
  s = db.Set("GP8_KEY4_SCAN_CASE_SINGLE_STRING", "GP8_SCAN_CASE_SINGLE_STRING_VALUE4");
  s = db.Set("GP8_KEY5_SCAN_CASE_SINGLE_STRING", "GP8_SCAN_CASE_SINGLE_STRING_VALUE5");
  s = db.Set("GP8_KEY6_SCAN_CASE_SINGLE_STRING", "GP8_SCAN_CASE_SINGLE_STRING_VALUE6");
  delete_keys.push_back("GP8_KEY1_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP8_KEY2_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP8_KEY3_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP8_KEY4_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP8_KEY5_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP8_KEY6_SCAN_CASE_SINGLE_STRING");

  //Hash
  s = db.HSet("GP8_KEY1_SCAN_CASE_SINGLE_HASH", "GP8_SCAN_CASE_SINGLE_HASH_FIELD1", "GP8_SCAN_CASE_SINGLE_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP8_KEY2_SCAN_CASE_SINGLE_HASH", "GP8_SCAN_CASE_SINGLE_HASH_FIELD2", "GP8_SCAN_CASE_SINGLE_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP8_KEY3_SCAN_CASE_SINGLE_HASH", "GP8_SCAN_CASE_SINGLE_HASH_FIELD3", "GP8_SCAN_CASE_SINGLE_HASH_VALUE3", &int32_ret);
  s = db.HSet("GP8_KEY4_SCAN_CASE_SINGLE_HASH", "GP8_SCAN_CASE_SINGLE_HASH_FIELD4", "GP8_SCAN_CASE_SINGLE_HASH_VALUE4", &int32_ret);
  s = db.HSet("GP8_KEY5_SCAN_CASE_SINGLE_HASH", "GP8_SCAN_CASE_SINGLE_HASH_FIELD5", "GP8_SCAN_CASE_SINGLE_HASH_VALUE5", &int32_ret);
  s = db.HSet("GP8_KEY6_SCAN_CASE_SINGLE_HASH", "GP8_SCAN_CASE_SINGLE_HASH_FIELD6", "GP8_SCAN_CASE_SINGLE_HASH_VALUE6", &int32_ret);
  delete_keys.push_back("GP8_KEY1_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP8_KEY2_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP8_KEY3_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP8_KEY4_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP8_KEY5_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP8_KEY6_SCAN_CASE_SINGLE_HASH");

  //Set
  s = db.SAdd("GP8_KEY1_SCAN_CASE_SINGLE_SET", {"GP8_SCAN_CASE_SINGLE_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP8_KEY2_SCAN_CASE_SINGLE_SET", {"GP8_SCAN_CASE_SINGLE_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP8_KEY3_SCAN_CASE_SINGLE_SET", {"GP8_SCAN_CASE_SINGLE_SET_MEMBER3"}, &int32_ret);
  s = db.SAdd("GP8_KEY4_SCAN_CASE_SINGLE_SET", {"GP8_SCAN_CASE_SINGLE_SET_MEMBER4"}, &int32_ret);
  s = db.SAdd("GP8_KEY5_SCAN_CASE_SINGLE_SET", {"GP8_SCAN_CASE_SINGLE_SET_MEMBER5"}, &int32_ret);
  s = db.SAdd("GP8_KEY6_SCAN_CASE_SINGLE_SET", {"GP8_SCAN_CASE_SINGLE_SET_MEMBER6"}, &int32_ret);
  delete_keys.push_back("GP8_KEY1_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP8_KEY2_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP8_KEY3_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP8_KEY4_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP8_KEY5_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP8_KEY6_SCAN_CASE_SINGLE_SET");

  //List
  s = db.LPush("GP8_KEY1_SCAN_CASE_SINGLE_LIST", {"GP8_SCAN_CASE_SINGLE_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP8_KEY2_SCAN_CASE_SINGLE_LIST", {"GP8_SCAN_CASE_SINGLE_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP8_KEY3_SCAN_CASE_SINGLE_LIST", {"GP8_SCAN_CASE_SINGLE_LIST_NODE3"}, &uint64_ret);
  s = db.LPush("GP8_KEY4_SCAN_CASE_SINGLE_LIST", {"GP8_SCAN_CASE_SINGLE_LIST_NODE4"}, &uint64_ret);
  s = db.LPush("GP8_KEY5_SCAN_CASE_SINGLE_LIST", {"GP8_SCAN_CASE_SINGLE_LIST_NODE5"}, &uint64_ret);
  s = db.LPush("GP8_KEY6_SCAN_CASE_SINGLE_LIST", {"GP8_SCAN_CASE_SINGLE_LIST_NODE6"}, &uint64_ret);
  delete_keys.push_back("GP8_KEY1_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP8_KEY2_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP8_KEY3_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP8_KEY4_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP8_KEY5_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP8_KEY6_SCAN_CASE_SINGLE_LIST");

  //ZSet
  s = db.ZAdd("GP8_KEY1_SCAN_CASE_SINGLE_ZSET", {{1, "GP8_SCAN_CASE_SINGLE_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP8_KEY2_SCAN_CASE_SINGLE_ZSET", {{1, "GP8_SCAN_CASE_SINGLE_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP8_KEY3_SCAN_CASE_SINGLE_ZSET", {{1, "GP8_SCAN_CASE_SINGLE_LIST_MEMBER3"}}, &int32_ret);
  s = db.ZAdd("GP8_KEY4_SCAN_CASE_SINGLE_ZSET", {{1, "GP8_SCAN_CASE_SINGLE_LIST_MEMBER4"}}, &int32_ret);
  s = db.ZAdd("GP8_KEY5_SCAN_CASE_SINGLE_ZSET", {{1, "GP8_SCAN_CASE_SINGLE_LIST_MEMBER5"}}, &int32_ret);
  s = db.ZAdd("GP8_KEY6_SCAN_CASE_SINGLE_ZSET", {{1, "GP8_SCAN_CASE_SINGLE_LIST_MEMBER6"}}, &int32_ret);
  delete_keys.push_back("GP8_KEY1_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP8_KEY2_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP8_KEY3_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP8_KEY4_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP8_KEY5_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP8_KEY6_SCAN_CASE_SINGLE_ZSET");

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

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 30);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 9 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP9_KEY1_SCAN_CASE_SINGLE_STRING", "GP9_SCAN_CASE_SINGLE_STRING_VALUE1");
  s = db.Set("GP9_KEY2_SCAN_CASE_SINGLE_STRING", "GP9_SCAN_CASE_SINGLE_STRING_VALUE2");
  s = db.Set("GP9_KEY3_SCAN_CASE_SINGLE_STRING", "GP9_SCAN_CASE_SINGLE_STRING_VALUE3");
  s = db.Set("GP9_KEY4_SCAN_CASE_SINGLE_STRING", "GP9_SCAN_CASE_SINGLE_STRING_VALUE4");
  s = db.Set("GP9_KEY5_SCAN_CASE_SINGLE_STRING", "GP9_SCAN_CASE_SINGLE_STRING_VALUE5");
  s = db.Set("GP9_KEY6_SCAN_CASE_SINGLE_STRING", "GP9_SCAN_CASE_SINGLE_STRING_VALUE6");
  delete_keys.push_back("GP9_KEY1_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP9_KEY2_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP9_KEY3_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP9_KEY4_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP9_KEY5_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP9_KEY6_SCAN_CASE_SINGLE_STRING");

  //Hash
  s = db.HSet("GP9_KEY1_SCAN_CASE_SINGLE_HASH", "GP9_SCAN_CASE_SINGLE_HASH_FIELD1", "GP9_SCAN_CASE_SINGLE_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP9_KEY2_SCAN_CASE_SINGLE_HASH", "GP9_SCAN_CASE_SINGLE_HASH_FIELD2", "GP9_SCAN_CASE_SINGLE_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP9_KEY3_SCAN_CASE_SINGLE_HASH", "GP9_SCAN_CASE_SINGLE_HASH_FIELD3", "GP9_SCAN_CASE_SINGLE_HASH_VALUE3", &int32_ret);
  s = db.HSet("GP9_KEY4_SCAN_CASE_SINGLE_HASH", "GP9_SCAN_CASE_SINGLE_HASH_FIELD4", "GP9_SCAN_CASE_SINGLE_HASH_VALUE4", &int32_ret);
  s = db.HSet("GP9_KEY5_SCAN_CASE_SINGLE_HASH", "GP9_SCAN_CASE_SINGLE_HASH_FIELD5", "GP9_SCAN_CASE_SINGLE_HASH_VALUE5", &int32_ret);
  s = db.HSet("GP9_KEY6_SCAN_CASE_SINGLE_HASH", "GP9_SCAN_CASE_SINGLE_HASH_FIELD6", "GP9_SCAN_CASE_SINGLE_HASH_VALUE6", &int32_ret);
  delete_keys.push_back("GP9_KEY1_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP9_KEY2_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP9_KEY3_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP9_KEY4_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP9_KEY5_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP9_KEY6_SCAN_CASE_SINGLE_HASH");

  //Set
  s = db.SAdd("GP9_KEY1_SCAN_CASE_SINGLE_SET", {"GP9_SCAN_CASE_SINGLE_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP9_KEY2_SCAN_CASE_SINGLE_SET", {"GP9_SCAN_CASE_SINGLE_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP9_KEY3_SCAN_CASE_SINGLE_SET", {"GP9_SCAN_CASE_SINGLE_SET_MEMBER3"}, &int32_ret);
  s = db.SAdd("GP9_KEY4_SCAN_CASE_SINGLE_SET", {"GP9_SCAN_CASE_SINGLE_SET_MEMBER4"}, &int32_ret);
  s = db.SAdd("GP9_KEY5_SCAN_CASE_SINGLE_SET", {"GP9_SCAN_CASE_SINGLE_SET_MEMBER5"}, &int32_ret);
  s = db.SAdd("GP9_KEY6_SCAN_CASE_SINGLE_SET", {"GP9_SCAN_CASE_SINGLE_SET_MEMBER6"}, &int32_ret);
  delete_keys.push_back("GP9_KEY1_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP9_KEY2_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP9_KEY3_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP9_KEY4_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP9_KEY5_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP9_KEY6_SCAN_CASE_SINGLE_SET");

  //List
  s = db.LPush("GP9_KEY1_SCAN_CASE_SINGLE_LIST", {"GP9_SCAN_CASE_SINGLE_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP9_KEY2_SCAN_CASE_SINGLE_LIST", {"GP9_SCAN_CASE_SINGLE_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP9_KEY3_SCAN_CASE_SINGLE_LIST", {"GP9_SCAN_CASE_SINGLE_LIST_NODE3"}, &uint64_ret);
  s = db.LPush("GP9_KEY4_SCAN_CASE_SINGLE_LIST", {"GP9_SCAN_CASE_SINGLE_LIST_NODE4"}, &uint64_ret);
  s = db.LPush("GP9_KEY5_SCAN_CASE_SINGLE_LIST", {"GP9_SCAN_CASE_SINGLE_LIST_NODE5"}, &uint64_ret);
  s = db.LPush("GP9_KEY6_SCAN_CASE_SINGLE_LIST", {"GP9_SCAN_CASE_SINGLE_LIST_NODE6"}, &uint64_ret);
  delete_keys.push_back("GP9_KEY1_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP9_KEY2_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP9_KEY3_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP9_KEY4_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP9_KEY5_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP9_KEY6_SCAN_CASE_SINGLE_LIST");

  //ZSet
  s = db.ZAdd("GP9_KEY1_SCAN_CASE_SINGLE_ZSET", {{1, "GP9_SCAN_CASE_SINGLE_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP9_KEY2_SCAN_CASE_SINGLE_ZSET", {{1, "GP9_SCAN_CASE_SINGLE_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP9_KEY3_SCAN_CASE_SINGLE_ZSET", {{1, "GP9_SCAN_CASE_SINGLE_LIST_MEMBER3"}}, &int32_ret);
  s = db.ZAdd("GP9_KEY4_SCAN_CASE_SINGLE_ZSET", {{1, "GP9_SCAN_CASE_SINGLE_LIST_MEMBER4"}}, &int32_ret);
  s = db.ZAdd("GP9_KEY5_SCAN_CASE_SINGLE_ZSET", {{1, "GP9_SCAN_CASE_SINGLE_LIST_MEMBER5"}}, &int32_ret);
  s = db.ZAdd("GP9_KEY6_SCAN_CASE_SINGLE_ZSET", {{1, "GP9_SCAN_CASE_SINGLE_LIST_MEMBER6"}}, &int32_ret);
  delete_keys.push_back("GP9_KEY1_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP9_KEY2_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP9_KEY3_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP9_KEY4_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP9_KEY5_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP9_KEY6_SCAN_CASE_SINGLE_ZSET");

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

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 30);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 10 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP10_KEY1_SCAN_CASE_SINGLE_STRING", "GP10_SCAN_CASE_SINGLE_STRING_VALUE1");
  s = db.Set("GP10_KEY2_SCAN_CASE_SINGLE_STRING", "GP10_SCAN_CASE_SINGLE_STRING_VALUE2");
  s = db.Set("GP10_KEY3_SCAN_CASE_SINGLE_STRING", "GP10_SCAN_CASE_SINGLE_STRING_VALUE3");
  s = db.Set("GP10_KEY4_SCAN_CASE_SINGLE_STRING", "GP10_SCAN_CASE_SINGLE_STRING_VALUE4");
  s = db.Set("GP10_KEY5_SCAN_CASE_SINGLE_STRING", "GP10_SCAN_CASE_SINGLE_STRING_VALUE5");
  s = db.Set("GP10_KEY6_SCAN_CASE_SINGLE_STRING", "GP10_SCAN_CASE_SINGLE_STRING_VALUE6");
  delete_keys.push_back("GP10_KEY1_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP10_KEY2_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP10_KEY3_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP10_KEY4_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP10_KEY5_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP10_KEY6_SCAN_CASE_SINGLE_STRING");

  //Hash
  s = db.HSet("GP10_KEY1_SCAN_CASE_SINGLE_HASH", "GP10_SCAN_CASE_SINGLE_HASH_FIELD1", "GP10_SCAN_CASE_SINGLE_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP10_KEY2_SCAN_CASE_SINGLE_HASH", "GP10_SCAN_CASE_SINGLE_HASH_FIELD2", "GP10_SCAN_CASE_SINGLE_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP10_KEY3_SCAN_CASE_SINGLE_HASH", "GP10_SCAN_CASE_SINGLE_HASH_FIELD3", "GP10_SCAN_CASE_SINGLE_HASH_VALUE3", &int32_ret);
  s = db.HSet("GP10_KEY4_SCAN_CASE_SINGLE_HASH", "GP10_SCAN_CASE_SINGLE_HASH_FIELD4", "GP10_SCAN_CASE_SINGLE_HASH_VALUE4", &int32_ret);
  s = db.HSet("GP10_KEY5_SCAN_CASE_SINGLE_HASH", "GP10_SCAN_CASE_SINGLE_HASH_FIELD5", "GP10_SCAN_CASE_SINGLE_HASH_VALUE5", &int32_ret);
  s = db.HSet("GP10_KEY6_SCAN_CASE_SINGLE_HASH", "GP10_SCAN_CASE_SINGLE_HASH_FIELD6", "GP10_SCAN_CASE_SINGLE_HASH_VALUE6", &int32_ret);
  delete_keys.push_back("GP10_KEY1_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP10_KEY2_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP10_KEY3_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP10_KEY4_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP10_KEY5_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP10_KEY6_SCAN_CASE_SINGLE_HASH");

  //Set
  s = db.SAdd("GP10_KEY1_SCAN_CASE_SINGLE_SET", {"GP10_SCAN_CASE_SINGLE_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP10_KEY2_SCAN_CASE_SINGLE_SET", {"GP10_SCAN_CASE_SINGLE_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP10_KEY3_SCAN_CASE_SINGLE_SET", {"GP10_SCAN_CASE_SINGLE_SET_MEMBER3"}, &int32_ret);
  s = db.SAdd("GP10_KEY4_SCAN_CASE_SINGLE_SET", {"GP10_SCAN_CASE_SINGLE_SET_MEMBER4"}, &int32_ret);
  s = db.SAdd("GP10_KEY5_SCAN_CASE_SINGLE_SET", {"GP10_SCAN_CASE_SINGLE_SET_MEMBER5"}, &int32_ret);
  s = db.SAdd("GP10_KEY6_SCAN_CASE_SINGLE_SET", {"GP10_SCAN_CASE_SINGLE_SET_MEMBER6"}, &int32_ret);
  delete_keys.push_back("GP10_KEY1_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP10_KEY2_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP10_KEY3_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP10_KEY4_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP10_KEY5_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP10_KEY6_SCAN_CASE_SINGLE_SET");

  //List
  s = db.LPush("GP10_KEY1_SCAN_CASE_SINGLE_LIST", {"GP10_SCAN_CASE_SINGLE_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP10_KEY2_SCAN_CASE_SINGLE_LIST", {"GP10_SCAN_CASE_SINGLE_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP10_KEY3_SCAN_CASE_SINGLE_LIST", {"GP10_SCAN_CASE_SINGLE_LIST_NODE3"}, &uint64_ret);
  s = db.LPush("GP10_KEY4_SCAN_CASE_SINGLE_LIST", {"GP10_SCAN_CASE_SINGLE_LIST_NODE4"}, &uint64_ret);
  s = db.LPush("GP10_KEY5_SCAN_CASE_SINGLE_LIST", {"GP10_SCAN_CASE_SINGLE_LIST_NODE5"}, &uint64_ret);
  s = db.LPush("GP10_KEY6_SCAN_CASE_SINGLE_LIST", {"GP10_SCAN_CASE_SINGLE_LIST_NODE6"}, &uint64_ret);
  delete_keys.push_back("GP10_KEY1_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP10_KEY2_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP10_KEY3_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP10_KEY4_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP10_KEY5_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP10_KEY6_SCAN_CASE_SINGLE_LIST");

  //ZSet
  s = db.ZAdd("GP10_KEY1_SCAN_CASE_SINGLE_ZSET", {{1, "GP10_SCAN_CASE_SINGLE_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP10_KEY2_SCAN_CASE_SINGLE_ZSET", {{1, "GP10_SCAN_CASE_SINGLE_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP10_KEY3_SCAN_CASE_SINGLE_ZSET", {{1, "GP10_SCAN_CASE_SINGLE_LIST_MEMBER3"}}, &int32_ret);
  s = db.ZAdd("GP10_KEY4_SCAN_CASE_SINGLE_ZSET", {{1, "GP10_SCAN_CASE_SINGLE_LIST_MEMBER4"}}, &int32_ret);
  s = db.ZAdd("GP10_KEY5_SCAN_CASE_SINGLE_ZSET", {{1, "GP10_SCAN_CASE_SINGLE_LIST_MEMBER5"}}, &int32_ret);
  s = db.ZAdd("GP10_KEY6_SCAN_CASE_SINGLE_ZSET", {{1, "GP10_SCAN_CASE_SINGLE_LIST_MEMBER6"}}, &int32_ret);
  delete_keys.push_back("GP10_KEY1_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP10_KEY2_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP10_KEY3_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP10_KEY4_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP10_KEY5_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP10_KEY6_SCAN_CASE_SINGLE_ZSET");

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

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 30);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 11 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP11_KEY1_SCAN_CASE_SINGLE_STRING", "GP11_SCAN_CASE_SINGLE_STRING_VALUE1");
  s = db.Set("GP11_KEY2_SCAN_CASE_SINGLE_STRING", "GP11_SCAN_CASE_SINGLE_STRING_VALUE2");
  s = db.Set("GP11_KEY3_SCAN_CASE_SINGLE_STRING", "GP11_SCAN_CASE_SINGLE_STRING_VALUE3");
  s = db.Set("GP11_KEY4_SCAN_CASE_SINGLE_STRING", "GP11_SCAN_CASE_SINGLE_STRING_VALUE4");
  s = db.Set("GP11_KEY5_SCAN_CASE_SINGLE_STRING", "GP11_SCAN_CASE_SINGLE_STRING_VALUE5");
  s = db.Set("GP11_KEY6_SCAN_CASE_SINGLE_STRING", "GP11_SCAN_CASE_SINGLE_STRING_VALUE6");
  delete_keys.push_back("GP11_KEY1_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP11_KEY2_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP11_KEY3_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP11_KEY4_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP11_KEY5_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP11_KEY6_SCAN_CASE_SINGLE_STRING");

  //Hash
  s = db.HSet("GP11_KEY1_SCAN_CASE_SINGLE_HASH", "GP11_SCAN_CASE_SINGLE_HASH_FIELD1", "GP11_SCAN_CASE_SINGLE_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP11_KEY2_SCAN_CASE_SINGLE_HASH", "GP11_SCAN_CASE_SINGLE_HASH_FIELD2", "GP11_SCAN_CASE_SINGLE_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP11_KEY3_SCAN_CASE_SINGLE_HASH", "GP11_SCAN_CASE_SINGLE_HASH_FIELD3", "GP11_SCAN_CASE_SINGLE_HASH_VALUE3", &int32_ret);
  s = db.HSet("GP11_KEY4_SCAN_CASE_SINGLE_HASH", "GP11_SCAN_CASE_SINGLE_HASH_FIELD4", "GP11_SCAN_CASE_SINGLE_HASH_VALUE4", &int32_ret);
  s = db.HSet("GP11_KEY5_SCAN_CASE_SINGLE_HASH", "GP11_SCAN_CASE_SINGLE_HASH_FIELD5", "GP11_SCAN_CASE_SINGLE_HASH_VALUE5", &int32_ret);
  s = db.HSet("GP11_KEY6_SCAN_CASE_SINGLE_HASH", "GP11_SCAN_CASE_SINGLE_HASH_FIELD6", "GP11_SCAN_CASE_SINGLE_HASH_VALUE6", &int32_ret);
  delete_keys.push_back("GP11_KEY1_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP11_KEY2_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP11_KEY3_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP11_KEY4_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP11_KEY5_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP11_KEY6_SCAN_CASE_SINGLE_HASH");

  //Set
  s = db.SAdd("GP11_KEY1_SCAN_CASE_SINGLE_SET", {"GP11_SCAN_CASE_SINGLE_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP11_KEY2_SCAN_CASE_SINGLE_SET", {"GP11_SCAN_CASE_SINGLE_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP11_KEY3_SCAN_CASE_SINGLE_SET", {"GP11_SCAN_CASE_SINGLE_SET_MEMBER3"}, &int32_ret);
  s = db.SAdd("GP11_KEY4_SCAN_CASE_SINGLE_SET", {"GP11_SCAN_CASE_SINGLE_SET_MEMBER4"}, &int32_ret);
  s = db.SAdd("GP11_KEY5_SCAN_CASE_SINGLE_SET", {"GP11_SCAN_CASE_SINGLE_SET_MEMBER5"}, &int32_ret);
  s = db.SAdd("GP11_KEY6_SCAN_CASE_SINGLE_SET", {"GP11_SCAN_CASE_SINGLE_SET_MEMBER6"}, &int32_ret);
  delete_keys.push_back("GP11_KEY1_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP11_KEY2_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP11_KEY3_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP11_KEY4_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP11_KEY5_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP11_KEY6_SCAN_CASE_SINGLE_SET");

  //List
  s = db.LPush("GP11_KEY1_SCAN_CASE_SINGLE_LIST", {"GP11_SCAN_CASE_SINGLE_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP11_KEY2_SCAN_CASE_SINGLE_LIST", {"GP11_SCAN_CASE_SINGLE_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP11_KEY3_SCAN_CASE_SINGLE_LIST", {"GP11_SCAN_CASE_SINGLE_LIST_NODE3"}, &uint64_ret);
  s = db.LPush("GP11_KEY4_SCAN_CASE_SINGLE_LIST", {"GP11_SCAN_CASE_SINGLE_LIST_NODE4"}, &uint64_ret);
  s = db.LPush("GP11_KEY5_SCAN_CASE_SINGLE_LIST", {"GP11_SCAN_CASE_SINGLE_LIST_NODE5"}, &uint64_ret);
  s = db.LPush("GP11_KEY6_SCAN_CASE_SINGLE_LIST", {"GP11_SCAN_CASE_SINGLE_LIST_NODE6"}, &uint64_ret);
  delete_keys.push_back("GP11_KEY1_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP11_KEY2_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP11_KEY3_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP11_KEY4_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP11_KEY5_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP11_KEY6_SCAN_CASE_SINGLE_LIST");

  //ZSet
  s = db.ZAdd("GP11_KEY1_SCAN_CASE_SINGLE_ZSET", {{1, "GP11_SCAN_CASE_SINGLE_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP11_KEY2_SCAN_CASE_SINGLE_ZSET", {{1, "GP11_SCAN_CASE_SINGLE_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP11_KEY3_SCAN_CASE_SINGLE_ZSET", {{1, "GP11_SCAN_CASE_SINGLE_LIST_MEMBER3"}}, &int32_ret);
  s = db.ZAdd("GP11_KEY4_SCAN_CASE_SINGLE_ZSET", {{1, "GP11_SCAN_CASE_SINGLE_LIST_MEMBER4"}}, &int32_ret);
  s = db.ZAdd("GP11_KEY5_SCAN_CASE_SINGLE_ZSET", {{1, "GP11_SCAN_CASE_SINGLE_LIST_MEMBER5"}}, &int32_ret);
  s = db.ZAdd("GP11_KEY6_SCAN_CASE_SINGLE_ZSET", {{1, "GP11_SCAN_CASE_SINGLE_LIST_MEMBER6"}}, &int32_ret);
  delete_keys.push_back("GP11_KEY1_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP11_KEY2_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP11_KEY3_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP11_KEY4_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP11_KEY5_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP11_KEY6_SCAN_CASE_SINGLE_ZSET");

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

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 30);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 12 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP12_KEY1_SCAN_CASE_SINGLE_STRING", "GP12_SCAN_CASE_SINGLE_STRING_VALUE1");
  s = db.Set("GP12_KEY2_SCAN_CASE_SINGLE_STRING", "GP12_SCAN_CASE_SINGLE_STRING_VALUE2");
  s = db.Set("GP12_KEY3_SCAN_CASE_SINGLE_STRING", "GP12_SCAN_CASE_SINGLE_STRING_VALUE3");
  s = db.Set("GP12_KEY4_SCAN_CASE_SINGLE_STRING", "GP12_SCAN_CASE_SINGLE_STRING_VALUE4");
  s = db.Set("GP12_KEY5_SCAN_CASE_SINGLE_STRING", "GP12_SCAN_CASE_SINGLE_STRING_VALUE5");
  s = db.Set("GP12_KEY6_SCAN_CASE_SINGLE_STRING", "GP12_SCAN_CASE_SINGLE_STRING_VALUE6");
  delete_keys.push_back("GP12_KEY1_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP12_KEY2_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP12_KEY3_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP12_KEY4_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP12_KEY5_SCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP12_KEY6_SCAN_CASE_SINGLE_STRING");

  //Hash
  s = db.HSet("GP12_KEY1_SCAN_CASE_SINGLE_HASH", "GP12_SCAN_CASE_SINGLE_HASH_FIELD1", "GP12_SCAN_CASE_SINGLE_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP12_KEY2_SCAN_CASE_SINGLE_HASH", "GP12_SCAN_CASE_SINGLE_HASH_FIELD2", "GP12_SCAN_CASE_SINGLE_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP12_KEY3_SCAN_CASE_SINGLE_HASH", "GP12_SCAN_CASE_SINGLE_HASH_FIELD3", "GP12_SCAN_CASE_SINGLE_HASH_VALUE3", &int32_ret);
  s = db.HSet("GP12_KEY4_SCAN_CASE_SINGLE_HASH", "GP12_SCAN_CASE_SINGLE_HASH_FIELD4", "GP12_SCAN_CASE_SINGLE_HASH_VALUE4", &int32_ret);
  s = db.HSet("GP12_KEY5_SCAN_CASE_SINGLE_HASH", "GP12_SCAN_CASE_SINGLE_HASH_FIELD5", "GP12_SCAN_CASE_SINGLE_HASH_VALUE5", &int32_ret);
  s = db.HSet("GP12_KEY6_SCAN_CASE_SINGLE_HASH", "GP12_SCAN_CASE_SINGLE_HASH_FIELD6", "GP12_SCAN_CASE_SINGLE_HASH_VALUE6", &int32_ret);
  delete_keys.push_back("GP12_KEY1_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP12_KEY2_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP12_KEY3_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP12_KEY4_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP12_KEY5_SCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP12_KEY6_SCAN_CASE_SINGLE_HASH");

  //Set
  s = db.SAdd("GP12_KEY1_SCAN_CASE_SINGLE_SET", {"GP12_SCAN_CASE_SINGLE_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP12_KEY2_SCAN_CASE_SINGLE_SET", {"GP12_SCAN_CASE_SINGLE_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP12_KEY3_SCAN_CASE_SINGLE_SET", {"GP12_SCAN_CASE_SINGLE_SET_MEMBER3"}, &int32_ret);
  s = db.SAdd("GP12_KEY4_SCAN_CASE_SINGLE_SET", {"GP12_SCAN_CASE_SINGLE_SET_MEMBER4"}, &int32_ret);
  s = db.SAdd("GP12_KEY5_SCAN_CASE_SINGLE_SET", {"GP12_SCAN_CASE_SINGLE_SET_MEMBER5"}, &int32_ret);
  s = db.SAdd("GP12_KEY6_SCAN_CASE_SINGLE_SET", {"GP12_SCAN_CASE_SINGLE_SET_MEMBER6"}, &int32_ret);
  delete_keys.push_back("GP12_KEY1_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP12_KEY2_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP12_KEY3_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP12_KEY4_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP12_KEY5_SCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP12_KEY6_SCAN_CASE_SINGLE_SET");

  //List
  s = db.LPush("GP12_KEY1_SCAN_CASE_SINGLE_LIST", {"GP12_SCAN_CASE_SINGLE_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP12_KEY2_SCAN_CASE_SINGLE_LIST", {"GP12_SCAN_CASE_SINGLE_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP12_KEY3_SCAN_CASE_SINGLE_LIST", {"GP12_SCAN_CASE_SINGLE_LIST_NODE3"}, &uint64_ret);
  s = db.LPush("GP12_KEY4_SCAN_CASE_SINGLE_LIST", {"GP12_SCAN_CASE_SINGLE_LIST_NODE4"}, &uint64_ret);
  s = db.LPush("GP12_KEY5_SCAN_CASE_SINGLE_LIST", {"GP12_SCAN_CASE_SINGLE_LIST_NODE5"}, &uint64_ret);
  s = db.LPush("GP12_KEY6_SCAN_CASE_SINGLE_LIST", {"GP12_SCAN_CASE_SINGLE_LIST_NODE6"}, &uint64_ret);
  delete_keys.push_back("GP12_KEY1_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP12_KEY2_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP12_KEY3_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP12_KEY4_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP12_KEY5_SCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP12_KEY6_SCAN_CASE_SINGLE_LIST");

  //ZSet
  s = db.ZAdd("GP12_KEY1_SCAN_CASE_SINGLE_ZSET", {{1, "GP12_SCAN_CASE_SINGLE_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP12_KEY2_SCAN_CASE_SINGLE_ZSET", {{1, "GP12_SCAN_CASE_SINGLE_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP12_KEY3_SCAN_CASE_SINGLE_ZSET", {{1, "GP12_SCAN_CASE_SINGLE_LIST_MEMBER3"}}, &int32_ret);
  s = db.ZAdd("GP12_KEY4_SCAN_CASE_SINGLE_ZSET", {{1, "GP12_SCAN_CASE_SINGLE_LIST_MEMBER4"}}, &int32_ret);
  s = db.ZAdd("GP12_KEY5_SCAN_CASE_SINGLE_ZSET", {{1, "GP12_SCAN_CASE_SINGLE_LIST_MEMBER5"}}, &int32_ret);
  s = db.ZAdd("GP12_KEY6_SCAN_CASE_SINGLE_ZSET", {{1, "GP12_SCAN_CASE_SINGLE_LIST_MEMBER6"}}, &int32_ret);
  delete_keys.push_back("GP12_KEY1_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP12_KEY2_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP12_KEY3_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP12_KEY4_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP12_KEY5_SCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP12_KEY6_SCAN_CASE_SINGLE_ZSET");

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

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 30);
  sleep(2);
  db.Compact(DataType::kAll, true);
}

TEST_F(KeysTest, PKExpireScanCaseAllTest) {

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
  //String
  s = db.Set("GP1_PKEXPIRESCAN_CASE_ALL_STRING_KEY1", "GP1_PKEXPIRESCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP1_PKEXPIRESCAN_CASE_ALL_STRING_KEY2", "GP1_PKEXPIRESCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP1_PKEXPIRESCAN_CASE_ALL_STRING_KEY3", "GP1_PKEXPIRESCAN_CASE_ALL_STRING_VALUE3");

  //Hash
  s = db.HSet("GP1_PKEXPIRESCAN_CASE_ALL_HASH_KEY1", "GP1_PKEXPIRESCAN_CASE_ALL_HASH_FIELD1", "GP1_PKEXPIRESCAN_CASE_ALL_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP1_PKEXPIRESCAN_CASE_ALL_HASH_KEY2", "GP1_PKEXPIRESCAN_CASE_ALL_HASH_FIELD2", "GP1_PKEXPIRESCAN_CASE_ALL_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP1_PKEXPIRESCAN_CASE_ALL_HASH_KEY3", "GP1_PKEXPIRESCAN_CASE_ALL_HASH_FIELD3", "GP1_PKEXPIRESCAN_CASE_ALL_HASH_VALUE3", &int32_ret);

  //Set
  s = db.SAdd("GP1_PKEXPIRESCAN_CASE_ALL_SET_KEY1", {"GP1_PKEXPIRESCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP1_PKEXPIRESCAN_CASE_ALL_SET_KEY2", {"GP1_PKEXPIRESCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP1_PKEXPIRESCAN_CASE_ALL_SET_KEY3", {"GP1_PKEXPIRESCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);

  //List
  s = db.LPush("GP1_PKEXPIRESCAN_CASE_ALL_LIST_KEY1", {"GP1_PKEXPIRESCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP1_PKEXPIRESCAN_CASE_ALL_LIST_KEY2", {"GP1_PKEXPIRESCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP1_PKEXPIRESCAN_CASE_ALL_LIST_KEY3", {"GP1_PKEXPIRESCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);

  //ZSet
  s = db.ZAdd("GP1_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1", {{1, "GP1_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP1_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2", {{1, "GP1_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP1_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3", {{1, "GP1_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);

  ASSERT_TRUE(set_timeout(&db, "GP1_PKEXPIRESCAN_CASE_ALL_STRING_KEY1", 2));
  ASSERT_TRUE(set_timeout(&db, "GP1_PKEXPIRESCAN_CASE_ALL_STRING_KEY2", 4));
  ASSERT_TRUE(set_timeout(&db, "GP1_PKEXPIRESCAN_CASE_ALL_STRING_KEY3", 6));

  ASSERT_TRUE(set_timeout(&db, "GP1_PKEXPIRESCAN_CASE_ALL_HASH_KEY1", 8));
  ASSERT_TRUE(set_timeout(&db, "GP1_PKEXPIRESCAN_CASE_ALL_HASH_KEY2", 10));
  ASSERT_TRUE(set_timeout(&db, "GP1_PKEXPIRESCAN_CASE_ALL_HASH_KEY3", 12));

  ASSERT_TRUE(set_timeout(&db, "GP1_PKEXPIRESCAN_CASE_ALL_SET_KEY1", 14));
  ASSERT_TRUE(set_timeout(&db, "GP1_PKEXPIRESCAN_CASE_ALL_SET_KEY2", 16));
  ASSERT_TRUE(set_timeout(&db, "GP1_PKEXPIRESCAN_CASE_ALL_SET_KEY3", 18));

  ASSERT_TRUE(set_timeout(&db, "GP1_PKEXPIRESCAN_CASE_ALL_LIST_KEY1", 20));
  ASSERT_TRUE(set_timeout(&db, "GP1_PKEXPIRESCAN_CASE_ALL_LIST_KEY2", 22));
  ASSERT_TRUE(set_timeout(&db, "GP1_PKEXPIRESCAN_CASE_ALL_LIST_KEY3", 24));

  ASSERT_TRUE(set_timeout(&db, "GP1_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1", 26));
  ASSERT_TRUE(set_timeout(&db, "GP1_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2", 28));
  ASSERT_TRUE(set_timeout(&db, "GP1_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3", 30));

  //PKExpireScan
  delete_keys.clear();
  keys.clear();
  cursor = db.PKExpireScan(DataType::kAll, 0, 0, 100, 3, &keys);
  ASSERT_EQ(cursor, 3);
  ASSERT_EQ(keys.size(), 3);
  ASSERT_EQ(keys[0], "GP1_PKEXPIRESCAN_CASE_ALL_STRING_KEY1");
  ASSERT_EQ(keys[1], "GP1_PKEXPIRESCAN_CASE_ALL_STRING_KEY2");
  ASSERT_EQ(keys[2], "GP1_PKEXPIRESCAN_CASE_ALL_STRING_KEY3");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.PKExpireScan(DataType::kAll, 3, 0, 100, 3, &keys);
  ASSERT_EQ(cursor, 6);
  ASSERT_EQ(keys.size(), 3);
  ASSERT_EQ(keys[0], "GP1_PKEXPIRESCAN_CASE_ALL_HASH_KEY1");
  ASSERT_EQ(keys[1], "GP1_PKEXPIRESCAN_CASE_ALL_HASH_KEY2");
  ASSERT_EQ(keys[2], "GP1_PKEXPIRESCAN_CASE_ALL_HASH_KEY3");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.PKExpireScan(DataType::kAll, 6, 0, 100, 3, &keys);
  ASSERT_EQ(cursor, 9);
  ASSERT_EQ(keys.size(), 3);
  ASSERT_EQ(keys[0], "GP1_PKEXPIRESCAN_CASE_ALL_SET_KEY1");
  ASSERT_EQ(keys[1], "GP1_PKEXPIRESCAN_CASE_ALL_SET_KEY2");
  ASSERT_EQ(keys[2], "GP1_PKEXPIRESCAN_CASE_ALL_SET_KEY3");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.PKExpireScan(DataType::kAll, 9, 0, 100, 3, &keys);
  ASSERT_EQ(cursor, 12);
  ASSERT_EQ(keys.size(), 3);
  ASSERT_EQ(keys[0], "GP1_PKEXPIRESCAN_CASE_ALL_LIST_KEY1");
  ASSERT_EQ(keys[1], "GP1_PKEXPIRESCAN_CASE_ALL_LIST_KEY2");
  ASSERT_EQ(keys[2], "GP1_PKEXPIRESCAN_CASE_ALL_LIST_KEY3");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.PKExpireScan(DataType::kAll, 12, 0, 100, 3, &keys);
  ASSERT_EQ(cursor, 0);
  ASSERT_EQ(keys.size(), 3);
  ASSERT_EQ(keys[0], "GP1_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1");
  ASSERT_EQ(keys[1], "GP1_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2");
  ASSERT_EQ(keys[2], "GP1_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 2 Test *****************
  //String
  s = db.Set("GP2_PKEXPIRESCAN_CASE_ALL_STRING_KEY1", "GP2_PKEXPIRESCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP2_PKEXPIRESCAN_CASE_ALL_STRING_KEY2", "GP2_PKEXPIRESCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP2_PKEXPIRESCAN_CASE_ALL_STRING_KEY3", "GP2_PKEXPIRESCAN_CASE_ALL_STRING_VALUE3");

  //Hash
  s = db.HSet("GP2_PKEXPIRESCAN_CASE_ALL_HASH_KEY1", "GP2_PKEXPIRESCAN_CASE_ALL_HASH_FIELD1", "GP2_PKEXPIRESCAN_CASE_ALL_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP2_PKEXPIRESCAN_CASE_ALL_HASH_KEY2", "GP2_PKEXPIRESCAN_CASE_ALL_HASH_FIELD2", "GP2_PKEXPIRESCAN_CASE_ALL_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP2_PKEXPIRESCAN_CASE_ALL_HASH_KEY3", "GP2_PKEXPIRESCAN_CASE_ALL_HASH_FIELD3", "GP2_PKEXPIRESCAN_CASE_ALL_HASH_VALUE3", &int32_ret);

  //Set
  s = db.SAdd("GP2_PKEXPIRESCAN_CASE_ALL_SET_KEY1", {"GP2_PKEXPIRESCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP2_PKEXPIRESCAN_CASE_ALL_SET_KEY2", {"GP2_PKEXPIRESCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP2_PKEXPIRESCAN_CASE_ALL_SET_KEY3", {"GP2_PKEXPIRESCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);

  //List
  s = db.LPush("GP2_PKEXPIRESCAN_CASE_ALL_LIST_KEY1", {"GP2_PKEXPIRESCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP2_PKEXPIRESCAN_CASE_ALL_LIST_KEY2", {"GP2_PKEXPIRESCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP2_PKEXPIRESCAN_CASE_ALL_LIST_KEY3", {"GP2_PKEXPIRESCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);

  //ZSet
  s = db.ZAdd("GP2_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1", {{1, "GP2_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP2_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2", {{1, "GP2_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP2_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3", {{1, "GP2_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);

  ASSERT_TRUE(set_timeout(&db, "GP2_PKEXPIRESCAN_CASE_ALL_STRING_KEY1", 2));
  ASSERT_TRUE(set_timeout(&db, "GP2_PKEXPIRESCAN_CASE_ALL_STRING_KEY2", 4));
  ASSERT_TRUE(set_timeout(&db, "GP2_PKEXPIRESCAN_CASE_ALL_STRING_KEY3", 6));

  ASSERT_TRUE(set_timeout(&db, "GP2_PKEXPIRESCAN_CASE_ALL_HASH_KEY1", 8));
  ASSERT_TRUE(set_timeout(&db, "GP2_PKEXPIRESCAN_CASE_ALL_HASH_KEY2", 10));
  ASSERT_TRUE(set_timeout(&db, "GP2_PKEXPIRESCAN_CASE_ALL_HASH_KEY3", 12));

  ASSERT_TRUE(set_timeout(&db, "GP2_PKEXPIRESCAN_CASE_ALL_SET_KEY1", 14));
  ASSERT_TRUE(set_timeout(&db, "GP2_PKEXPIRESCAN_CASE_ALL_SET_KEY2", 16));
  ASSERT_TRUE(set_timeout(&db, "GP2_PKEXPIRESCAN_CASE_ALL_SET_KEY3", 18));

  ASSERT_TRUE(set_timeout(&db, "GP2_PKEXPIRESCAN_CASE_ALL_LIST_KEY1", 20));
  ASSERT_TRUE(set_timeout(&db, "GP2_PKEXPIRESCAN_CASE_ALL_LIST_KEY2", 22));
  ASSERT_TRUE(set_timeout(&db, "GP2_PKEXPIRESCAN_CASE_ALL_LIST_KEY3", 24));

  ASSERT_TRUE(set_timeout(&db, "GP2_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1", 26));
  ASSERT_TRUE(set_timeout(&db, "GP2_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2", 28));
  ASSERT_TRUE(set_timeout(&db, "GP2_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3", 30));

  //PKExpireScan
  delete_keys.clear();
  keys.clear();
  cursor = db.PKExpireScan(DataType::kAll, 0, 0, 100, 2, &keys);
  ASSERT_EQ(cursor, 2);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP2_PKEXPIRESCAN_CASE_ALL_STRING_KEY1");
  ASSERT_EQ(keys[1], "GP2_PKEXPIRESCAN_CASE_ALL_STRING_KEY2");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.PKExpireScan(DataType::kAll, 2, 0, 100, 2, &keys);
  ASSERT_EQ(cursor, 4);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP2_PKEXPIRESCAN_CASE_ALL_STRING_KEY3");
  ASSERT_EQ(keys[1], "GP2_PKEXPIRESCAN_CASE_ALL_HASH_KEY1");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.PKExpireScan(DataType::kAll, 4, 0, 100, 2, &keys);
  ASSERT_EQ(cursor, 6);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP2_PKEXPIRESCAN_CASE_ALL_HASH_KEY2");
  ASSERT_EQ(keys[1], "GP2_PKEXPIRESCAN_CASE_ALL_HASH_KEY3");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.PKExpireScan(DataType::kAll, 6, 0, 100, 2, &keys);
  ASSERT_EQ(cursor, 8);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP2_PKEXPIRESCAN_CASE_ALL_SET_KEY1");
  ASSERT_EQ(keys[1], "GP2_PKEXPIRESCAN_CASE_ALL_SET_KEY2");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.PKExpireScan(DataType::kAll, 8, 0, 100, 2, &keys);
  ASSERT_EQ(cursor, 10);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP2_PKEXPIRESCAN_CASE_ALL_SET_KEY3");
  ASSERT_EQ(keys[1], "GP2_PKEXPIRESCAN_CASE_ALL_LIST_KEY1");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.PKExpireScan(DataType::kAll, 10, 0, 100, 2, &keys);
  ASSERT_EQ(cursor, 12);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP2_PKEXPIRESCAN_CASE_ALL_LIST_KEY2");
  ASSERT_EQ(keys[1], "GP2_PKEXPIRESCAN_CASE_ALL_LIST_KEY3");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.PKExpireScan(DataType::kAll, 12, 0, 100, 2, &keys);
  ASSERT_EQ(cursor, 14);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP2_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1");
  ASSERT_EQ(keys[1], "GP2_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.PKExpireScan(DataType::kAll, 14, 0, 100, 2, &keys);
  ASSERT_EQ(cursor, 0);
  ASSERT_EQ(keys.size(), 1);
  ASSERT_EQ(keys[0], "GP2_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 3 Test *****************
  //String
  s = db.Set("GP3_PKEXPIRESCAN_CASE_ALL_STRING_KEY1", "GP3_PKEXPIRESCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP3_PKEXPIRESCAN_CASE_ALL_STRING_KEY2", "GP3_PKEXPIRESCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP3_PKEXPIRESCAN_CASE_ALL_STRING_KEY3", "GP3_PKEXPIRESCAN_CASE_ALL_STRING_VALUE3");

  //Hash
  s = db.HSet("GP3_PKEXPIRESCAN_CASE_ALL_HASH_KEY1", "GP3_PKEXPIRESCAN_CASE_ALL_HASH_FIELD1", "GP3_PKEXPIRESCAN_CASE_ALL_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP3_PKEXPIRESCAN_CASE_ALL_HASH_KEY2", "GP3_PKEXPIRESCAN_CASE_ALL_HASH_FIELD2", "GP3_PKEXPIRESCAN_CASE_ALL_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP3_PKEXPIRESCAN_CASE_ALL_HASH_KEY3", "GP3_PKEXPIRESCAN_CASE_ALL_HASH_FIELD3", "GP3_PKEXPIRESCAN_CASE_ALL_HASH_VALUE3", &int32_ret);

  //Set
  s = db.SAdd("GP3_PKEXPIRESCAN_CASE_ALL_SET_KEY1", {"GP3_PKEXPIRESCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP3_PKEXPIRESCAN_CASE_ALL_SET_KEY2", {"GP3_PKEXPIRESCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP3_PKEXPIRESCAN_CASE_ALL_SET_KEY3", {"GP3_PKEXPIRESCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);

  //List
  s = db.LPush("GP3_PKEXPIRESCAN_CASE_ALL_LIST_KEY1", {"GP3_PKEXPIRESCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP3_PKEXPIRESCAN_CASE_ALL_LIST_KEY2", {"GP3_PKEXPIRESCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP3_PKEXPIRESCAN_CASE_ALL_LIST_KEY3", {"GP3_PKEXPIRESCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);

  //ZSet
  s = db.ZAdd("GP3_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1", {{1, "GP3_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP3_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2", {{1, "GP3_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP3_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3", {{1, "GP3_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);

  ASSERT_TRUE(set_timeout(&db, "GP3_PKEXPIRESCAN_CASE_ALL_STRING_KEY1", 2));
  ASSERT_TRUE(set_timeout(&db, "GP3_PKEXPIRESCAN_CASE_ALL_STRING_KEY2", 4));
  ASSERT_TRUE(set_timeout(&db, "GP3_PKEXPIRESCAN_CASE_ALL_STRING_KEY3", 6));

  ASSERT_TRUE(set_timeout(&db, "GP3_PKEXPIRESCAN_CASE_ALL_HASH_KEY1", 8));
  ASSERT_TRUE(set_timeout(&db, "GP3_PKEXPIRESCAN_CASE_ALL_HASH_KEY2", 10));
  ASSERT_TRUE(set_timeout(&db, "GP3_PKEXPIRESCAN_CASE_ALL_HASH_KEY3", 12));

  ASSERT_TRUE(set_timeout(&db, "GP3_PKEXPIRESCAN_CASE_ALL_SET_KEY1", 14));
  ASSERT_TRUE(set_timeout(&db, "GP3_PKEXPIRESCAN_CASE_ALL_SET_KEY2", 16));
  ASSERT_TRUE(set_timeout(&db, "GP3_PKEXPIRESCAN_CASE_ALL_SET_KEY3", 18));

  ASSERT_TRUE(set_timeout(&db, "GP3_PKEXPIRESCAN_CASE_ALL_LIST_KEY1", 20));
  ASSERT_TRUE(set_timeout(&db, "GP3_PKEXPIRESCAN_CASE_ALL_LIST_KEY2", 22));
  ASSERT_TRUE(set_timeout(&db, "GP3_PKEXPIRESCAN_CASE_ALL_LIST_KEY3", 24));

  ASSERT_TRUE(set_timeout(&db, "GP3_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1", 26));
  ASSERT_TRUE(set_timeout(&db, "GP3_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2", 28));
  ASSERT_TRUE(set_timeout(&db, "GP3_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3", 30));

  //PKExpireScan
  delete_keys.clear();
  keys.clear();
  cursor = db.PKExpireScan(DataType::kAll, 0, 0, 100, 5, &keys);
  ASSERT_EQ(cursor, 5);
  ASSERT_EQ(keys.size(), 5);
  ASSERT_EQ(keys[0], "GP3_PKEXPIRESCAN_CASE_ALL_STRING_KEY1");
  ASSERT_EQ(keys[1], "GP3_PKEXPIRESCAN_CASE_ALL_STRING_KEY2");
  ASSERT_EQ(keys[2], "GP3_PKEXPIRESCAN_CASE_ALL_STRING_KEY3");
  ASSERT_EQ(keys[3], "GP3_PKEXPIRESCAN_CASE_ALL_HASH_KEY1");
  ASSERT_EQ(keys[4], "GP3_PKEXPIRESCAN_CASE_ALL_HASH_KEY2");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.PKExpireScan(DataType::kAll, 5, 0, 100, 5, &keys);
  ASSERT_EQ(cursor, 10);
  ASSERT_EQ(keys.size(), 5);
  ASSERT_EQ(keys[0], "GP3_PKEXPIRESCAN_CASE_ALL_HASH_KEY3");
  ASSERT_EQ(keys[1], "GP3_PKEXPIRESCAN_CASE_ALL_SET_KEY1");
  ASSERT_EQ(keys[2], "GP3_PKEXPIRESCAN_CASE_ALL_SET_KEY2");
  ASSERT_EQ(keys[3], "GP3_PKEXPIRESCAN_CASE_ALL_SET_KEY3");
  ASSERT_EQ(keys[4], "GP3_PKEXPIRESCAN_CASE_ALL_LIST_KEY1");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.PKExpireScan(DataType::kAll, 10, 0, 100, 5, &keys);
  ASSERT_EQ(cursor, 0);
  ASSERT_EQ(keys.size(), 5);
  ASSERT_EQ(keys[0], "GP3_PKEXPIRESCAN_CASE_ALL_LIST_KEY2");
  ASSERT_EQ(keys[1], "GP3_PKEXPIRESCAN_CASE_ALL_LIST_KEY3");
  ASSERT_EQ(keys[2], "GP3_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1");
  ASSERT_EQ(keys[3], "GP3_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2");
  ASSERT_EQ(keys[4], "GP3_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 4 Test *****************
  //String
  s = db.Set("GP4_PKEXPIRESCAN_CASE_ALL_STRING_KEY1", "GP4_PKEXPIRESCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP4_PKEXPIRESCAN_CASE_ALL_STRING_KEY2", "GP4_PKEXPIRESCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP4_PKEXPIRESCAN_CASE_ALL_STRING_KEY3", "GP4_PKEXPIRESCAN_CASE_ALL_STRING_VALUE3");

  //Hash
  s = db.HSet("GP4_PKEXPIRESCAN_CASE_ALL_HASH_KEY1", "GP4_PKEXPIRESCAN_CASE_ALL_HASH_FIELD1", "GP4_PKEXPIRESCAN_CASE_ALL_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP4_PKEXPIRESCAN_CASE_ALL_HASH_KEY2", "GP4_PKEXPIRESCAN_CASE_ALL_HASH_FIELD2", "GP4_PKEXPIRESCAN_CASE_ALL_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP4_PKEXPIRESCAN_CASE_ALL_HASH_KEY3", "GP4_PKEXPIRESCAN_CASE_ALL_HASH_FIELD3", "GP4_PKEXPIRESCAN_CASE_ALL_HASH_VALUE3", &int32_ret);

  //Set
  s = db.SAdd("GP4_PKEXPIRESCAN_CASE_ALL_SET_KEY1", {"GP4_PKEXPIRESCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP4_PKEXPIRESCAN_CASE_ALL_SET_KEY2", {"GP4_PKEXPIRESCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP4_PKEXPIRESCAN_CASE_ALL_SET_KEY3", {"GP4_PKEXPIRESCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);

  //List
  s = db.LPush("GP4_PKEXPIRESCAN_CASE_ALL_LIST_KEY1", {"GP4_PKEXPIRESCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP4_PKEXPIRESCAN_CASE_ALL_LIST_KEY2", {"GP4_PKEXPIRESCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP4_PKEXPIRESCAN_CASE_ALL_LIST_KEY3", {"GP4_PKEXPIRESCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);

  //ZSet
  s = db.ZAdd("GP4_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1", {{1, "GP4_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP4_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2", {{1, "GP4_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP4_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3", {{1, "GP4_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);

  ASSERT_TRUE(set_timeout(&db, "GP4_PKEXPIRESCAN_CASE_ALL_STRING_KEY1", 2));
  ASSERT_TRUE(set_timeout(&db, "GP4_PKEXPIRESCAN_CASE_ALL_STRING_KEY2", 4));
  ASSERT_TRUE(set_timeout(&db, "GP4_PKEXPIRESCAN_CASE_ALL_STRING_KEY3", 6));

  ASSERT_TRUE(set_timeout(&db, "GP4_PKEXPIRESCAN_CASE_ALL_HASH_KEY1", 8));
  ASSERT_TRUE(set_timeout(&db, "GP4_PKEXPIRESCAN_CASE_ALL_HASH_KEY2", 10));
  ASSERT_TRUE(set_timeout(&db, "GP4_PKEXPIRESCAN_CASE_ALL_HASH_KEY3", 12));

  ASSERT_TRUE(set_timeout(&db, "GP4_PKEXPIRESCAN_CASE_ALL_SET_KEY1", 14));
  ASSERT_TRUE(set_timeout(&db, "GP4_PKEXPIRESCAN_CASE_ALL_SET_KEY2", 16));
  ASSERT_TRUE(set_timeout(&db, "GP4_PKEXPIRESCAN_CASE_ALL_SET_KEY3", 18));

  ASSERT_TRUE(set_timeout(&db, "GP4_PKEXPIRESCAN_CASE_ALL_LIST_KEY1", 20));
  ASSERT_TRUE(set_timeout(&db, "GP4_PKEXPIRESCAN_CASE_ALL_LIST_KEY2", 22));
  ASSERT_TRUE(set_timeout(&db, "GP4_PKEXPIRESCAN_CASE_ALL_LIST_KEY3", 24));

  ASSERT_TRUE(set_timeout(&db, "GP4_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1", 26));
  ASSERT_TRUE(set_timeout(&db, "GP4_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2", 28));
  ASSERT_TRUE(set_timeout(&db, "GP4_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3", 30));

  delete_keys.clear();
  keys.clear();
  cursor = db.PKExpireScan(DataType::kAll, 0, 0, 100, 15, &keys);
  ASSERT_EQ(cursor, 0);
  ASSERT_EQ(keys.size(), 15);
  ASSERT_EQ(keys[0], "GP4_PKEXPIRESCAN_CASE_ALL_STRING_KEY1");
  ASSERT_EQ(keys[1], "GP4_PKEXPIRESCAN_CASE_ALL_STRING_KEY2");
  ASSERT_EQ(keys[2], "GP4_PKEXPIRESCAN_CASE_ALL_STRING_KEY3");
  ASSERT_EQ(keys[3], "GP4_PKEXPIRESCAN_CASE_ALL_HASH_KEY1");
  ASSERT_EQ(keys[4], "GP4_PKEXPIRESCAN_CASE_ALL_HASH_KEY2");
  ASSERT_EQ(keys[5], "GP4_PKEXPIRESCAN_CASE_ALL_HASH_KEY3");
  ASSERT_EQ(keys[6], "GP4_PKEXPIRESCAN_CASE_ALL_SET_KEY1");
  ASSERT_EQ(keys[7], "GP4_PKEXPIRESCAN_CASE_ALL_SET_KEY2");
  ASSERT_EQ(keys[8], "GP4_PKEXPIRESCAN_CASE_ALL_SET_KEY3");
  ASSERT_EQ(keys[9], "GP4_PKEXPIRESCAN_CASE_ALL_LIST_KEY1");
  ASSERT_EQ(keys[10], "GP4_PKEXPIRESCAN_CASE_ALL_LIST_KEY2");
  ASSERT_EQ(keys[11], "GP4_PKEXPIRESCAN_CASE_ALL_LIST_KEY3");
  ASSERT_EQ(keys[12], "GP4_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1");
  ASSERT_EQ(keys[13], "GP4_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2");
  ASSERT_EQ(keys[14], "GP4_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 5 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP5_PKEXPIRESCAN_CASE_ALL_STRING_KEY1", "GP5_PKEXPIRESCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP5_PKEXPIRESCAN_CASE_ALL_STRING_KEY2", "GP5_PKEXPIRESCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP5_PKEXPIRESCAN_CASE_ALL_STRING_KEY3", "GP5_PKEXPIRESCAN_CASE_ALL_STRING_VALUE3");
  delete_keys.push_back("GP5_PKEXPIRESCAN_CASE_ALL_STRING_KEY1");
  delete_keys.push_back("GP5_PKEXPIRESCAN_CASE_ALL_STRING_KEY2");
  delete_keys.push_back("GP5_PKEXPIRESCAN_CASE_ALL_STRING_KEY3");

  //Hash
  s = db.HSet("GP5_PKEXPIRESCAN_CASE_ALL_HASH_KEY1", "GP5_PKEXPIRESCAN_CASE_ALL_HASH_FIELD1", "GP5_PKEXPIRESCAN_CASE_ALL_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP5_PKEXPIRESCAN_CASE_ALL_HASH_KEY2", "GP5_PKEXPIRESCAN_CASE_ALL_HASH_FIELD2", "GP5_PKEXPIRESCAN_CASE_ALL_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP5_PKEXPIRESCAN_CASE_ALL_HASH_KEY3", "GP5_PKEXPIRESCAN_CASE_ALL_HASH_FIELD3", "GP5_PKEXPIRESCAN_CASE_ALL_HASH_VALUE3", &int32_ret);
  delete_keys.push_back("GP5_PKEXPIRESCAN_CASE_ALL_HASH_KEY1");
  delete_keys.push_back("GP5_PKEXPIRESCAN_CASE_ALL_HASH_KEY2");
  delete_keys.push_back("GP5_PKEXPIRESCAN_CASE_ALL_HASH_KEY3");

  //Set
  s = db.SAdd("GP5_PKEXPIRESCAN_CASE_ALL_SET_KEY1", {"GP5_PKEXPIRESCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP5_PKEXPIRESCAN_CASE_ALL_SET_KEY2", {"GP5_PKEXPIRESCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP5_PKEXPIRESCAN_CASE_ALL_SET_KEY3", {"GP5_PKEXPIRESCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);
  delete_keys.push_back("GP5_PKEXPIRESCAN_CASE_ALL_SET_KEY1");
  delete_keys.push_back("GP5_PKEXPIRESCAN_CASE_ALL_SET_KEY2");
  delete_keys.push_back("GP5_PKEXPIRESCAN_CASE_ALL_SET_KEY3");

  //List
  s = db.LPush("GP5_PKEXPIRESCAN_CASE_ALL_LIST_KEY1", {"GP5_PKEXPIRESCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP5_PKEXPIRESCAN_CASE_ALL_LIST_KEY2", {"GP5_PKEXPIRESCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP5_PKEXPIRESCAN_CASE_ALL_LIST_KEY3", {"GP5_PKEXPIRESCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);
  delete_keys.push_back("GP5_PKEXPIRESCAN_CASE_ALL_LIST_KEY1");
  delete_keys.push_back("GP5_PKEXPIRESCAN_CASE_ALL_LIST_KEY2");
  delete_keys.push_back("GP5_PKEXPIRESCAN_CASE_ALL_LIST_KEY3");

  //ZSet
  s = db.ZAdd("GP5_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1", {{1, "GP5_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP5_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2", {{1, "GP5_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP5_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3", {{1, "GP5_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.push_back("GP5_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1");
  delete_keys.push_back("GP5_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2");
  delete_keys.push_back("GP5_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3");

  ASSERT_TRUE(set_timeout(&db, "GP5_PKEXPIRESCAN_CASE_ALL_STRING_KEY1", 5));
  ASSERT_TRUE(set_timeout(&db, "GP5_PKEXPIRESCAN_CASE_ALL_STRING_KEY2", 5));
  ASSERT_TRUE(set_timeout(&db, "GP5_PKEXPIRESCAN_CASE_ALL_STRING_KEY3", 5));

  ASSERT_TRUE(set_timeout(&db, "GP5_PKEXPIRESCAN_CASE_ALL_HASH_KEY1", 5));
  ASSERT_TRUE(set_timeout(&db, "GP5_PKEXPIRESCAN_CASE_ALL_HASH_KEY2", 5));
  ASSERT_TRUE(set_timeout(&db, "GP5_PKEXPIRESCAN_CASE_ALL_HASH_KEY3", 5));

  ASSERT_TRUE(set_timeout(&db, "GP5_PKEXPIRESCAN_CASE_ALL_SET_KEY1", 15));
  ASSERT_TRUE(set_timeout(&db, "GP5_PKEXPIRESCAN_CASE_ALL_SET_KEY2", 15));
  ASSERT_TRUE(set_timeout(&db, "GP5_PKEXPIRESCAN_CASE_ALL_SET_KEY3", 15));

  ASSERT_TRUE(set_timeout(&db, "GP5_PKEXPIRESCAN_CASE_ALL_LIST_KEY1", 25));
  ASSERT_TRUE(set_timeout(&db, "GP5_PKEXPIRESCAN_CASE_ALL_LIST_KEY2", 25));
  ASSERT_TRUE(set_timeout(&db, "GP5_PKEXPIRESCAN_CASE_ALL_LIST_KEY3", 25));

  ASSERT_TRUE(set_timeout(&db, "GP5_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1", 25));
  ASSERT_TRUE(set_timeout(&db, "GP5_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2", 25));
  ASSERT_TRUE(set_timeout(&db, "GP5_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3", 25));

  cursor = 0;
  keys.clear();
  total_keys.clear();
  do {
    next_cursor = db.PKExpireScan(DataType::kAll, cursor, 10, 20, 5, &keys);
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  } while (cursor);
  ASSERT_EQ(total_keys.size(), 3);
  ASSERT_EQ(total_keys[0], "GP5_PKEXPIRESCAN_CASE_ALL_SET_KEY1");
  ASSERT_EQ(total_keys[1], "GP5_PKEXPIRESCAN_CASE_ALL_SET_KEY2");
  ASSERT_EQ(total_keys[2], "GP5_PKEXPIRESCAN_CASE_ALL_SET_KEY3");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 6 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP6_PKEXPIRESCAN_CASE_ALL_STRING_KEY1", "GP6_PKEXPIRESCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP6_PKEXPIRESCAN_CASE_ALL_STRING_KEY2", "GP6_PKEXPIRESCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP6_PKEXPIRESCAN_CASE_ALL_STRING_KEY3", "GP6_PKEXPIRESCAN_CASE_ALL_STRING_VALUE3");
  delete_keys.push_back("GP6_PKEXPIRESCAN_CASE_ALL_STRING_KEY1");
  delete_keys.push_back("GP6_PKEXPIRESCAN_CASE_ALL_STRING_KEY2");
  delete_keys.push_back("GP6_PKEXPIRESCAN_CASE_ALL_STRING_KEY3");

  //Hash
  s = db.HSet("GP6_PKEXPIRESCAN_CASE_ALL_HASH_KEY1", "GP6_PKEXPIRESCAN_CASE_ALL_HASH_FIELD1", "GP6_PKEXPIRESCAN_CASE_ALL_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP6_PKEXPIRESCAN_CASE_ALL_HASH_KEY2", "GP6_PKEXPIRESCAN_CASE_ALL_HASH_FIELD2", "GP6_PKEXPIRESCAN_CASE_ALL_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP6_PKEXPIRESCAN_CASE_ALL_HASH_KEY3", "GP6_PKEXPIRESCAN_CASE_ALL_HASH_FIELD3", "GP6_PKEXPIRESCAN_CASE_ALL_HASH_VALUE3", &int32_ret);
  delete_keys.push_back("GP6_PKEXPIRESCAN_CASE_ALL_HASH_KEY1");
  delete_keys.push_back("GP6_PKEXPIRESCAN_CASE_ALL_HASH_KEY2");
  delete_keys.push_back("GP6_PKEXPIRESCAN_CASE_ALL_HASH_KEY3");

  //Set
  s = db.SAdd("GP6_PKEXPIRESCAN_CASE_ALL_SET_KEY1", {"GP6_PKEXPIRESCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP6_PKEXPIRESCAN_CASE_ALL_SET_KEY2", {"GP6_PKEXPIRESCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP6_PKEXPIRESCAN_CASE_ALL_SET_KEY3", {"GP6_PKEXPIRESCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);
  delete_keys.push_back("GP6_PKEXPIRESCAN_CASE_ALL_SET_KEY1");
  delete_keys.push_back("GP6_PKEXPIRESCAN_CASE_ALL_SET_KEY2");
  delete_keys.push_back("GP6_PKEXPIRESCAN_CASE_ALL_SET_KEY3");

  //List
  s = db.LPush("GP6_PKEXPIRESCAN_CASE_ALL_LIST_KEY1", {"GP6_PKEXPIRESCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP6_PKEXPIRESCAN_CASE_ALL_LIST_KEY2", {"GP6_PKEXPIRESCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP6_PKEXPIRESCAN_CASE_ALL_LIST_KEY3", {"GP6_PKEXPIRESCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);
  delete_keys.push_back("GP6_PKEXPIRESCAN_CASE_ALL_LIST_KEY1");
  delete_keys.push_back("GP6_PKEXPIRESCAN_CASE_ALL_LIST_KEY2");
  delete_keys.push_back("GP6_PKEXPIRESCAN_CASE_ALL_LIST_KEY3");

  //ZSet
  s = db.ZAdd("GP6_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1", {{1, "GP6_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP6_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2", {{1, "GP6_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP6_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3", {{1, "GP6_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.push_back("GP6_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1");
  delete_keys.push_back("GP6_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2");
  delete_keys.push_back("GP6_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3");

  ASSERT_TRUE(set_timeout(&db, "GP6_PKEXPIRESCAN_CASE_ALL_STRING_KEY1", 15));
  ASSERT_TRUE(set_timeout(&db, "GP6_PKEXPIRESCAN_CASE_ALL_STRING_KEY2", 5));
  ASSERT_TRUE(set_timeout(&db, "GP6_PKEXPIRESCAN_CASE_ALL_STRING_KEY3", 25));

  ASSERT_TRUE(set_timeout(&db, "GP6_PKEXPIRESCAN_CASE_ALL_HASH_KEY1", 15));
  ASSERT_TRUE(set_timeout(&db, "GP6_PKEXPIRESCAN_CASE_ALL_HASH_KEY2", 5));
  ASSERT_TRUE(set_timeout(&db, "GP6_PKEXPIRESCAN_CASE_ALL_HASH_KEY3", 25));

  ASSERT_TRUE(set_timeout(&db, "GP6_PKEXPIRESCAN_CASE_ALL_SET_KEY1", 15));
  ASSERT_TRUE(set_timeout(&db, "GP6_PKEXPIRESCAN_CASE_ALL_SET_KEY2", 5));
  ASSERT_TRUE(set_timeout(&db, "GP6_PKEXPIRESCAN_CASE_ALL_SET_KEY3", 25));

  ASSERT_TRUE(set_timeout(&db, "GP6_PKEXPIRESCAN_CASE_ALL_LIST_KEY1", 15));
  ASSERT_TRUE(set_timeout(&db, "GP6_PKEXPIRESCAN_CASE_ALL_LIST_KEY2", 5));
  ASSERT_TRUE(set_timeout(&db, "GP6_PKEXPIRESCAN_CASE_ALL_LIST_KEY3", 25));

  ASSERT_TRUE(set_timeout(&db, "GP6_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1", 15));
  ASSERT_TRUE(set_timeout(&db, "GP6_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2", 5));
  ASSERT_TRUE(set_timeout(&db, "GP6_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3", 25));

  cursor = 0;
  keys.clear();
  total_keys.clear();
  do {
    next_cursor = db.PKExpireScan(DataType::kAll, cursor, 10, 20, 5, &keys);
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  } while (cursor);
  ASSERT_EQ(total_keys.size(), 5);
  ASSERT_EQ(total_keys[0], "GP6_PKEXPIRESCAN_CASE_ALL_STRING_KEY1");
  ASSERT_EQ(total_keys[1], "GP6_PKEXPIRESCAN_CASE_ALL_HASH_KEY1");
  ASSERT_EQ(total_keys[2], "GP6_PKEXPIRESCAN_CASE_ALL_SET_KEY1");
  ASSERT_EQ(total_keys[3], "GP6_PKEXPIRESCAN_CASE_ALL_LIST_KEY1");
  ASSERT_EQ(total_keys[4], "GP6_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 7 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP7_PKEXPIRESCAN_CASE_ALL_STRING_KEY1", "GP7_PKEXPIRESCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP7_PKEXPIRESCAN_CASE_ALL_STRING_KEY2", "GP7_PKEXPIRESCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP7_PKEXPIRESCAN_CASE_ALL_STRING_KEY3", "GP7_PKEXPIRESCAN_CASE_ALL_STRING_VALUE3");
  delete_keys.push_back("GP7_PKEXPIRESCAN_CASE_ALL_STRING_KEY1");
  delete_keys.push_back("GP7_PKEXPIRESCAN_CASE_ALL_STRING_KEY2");
  delete_keys.push_back("GP7_PKEXPIRESCAN_CASE_ALL_STRING_KEY3");

  //Hash
  s = db.HSet("GP7_PKEXPIRESCAN_CASE_ALL_HASH_KEY1", "GP7_PKEXPIRESCAN_CASE_ALL_HASH_FIELD1", "GP7_PKEXPIRESCAN_CASE_ALL_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP7_PKEXPIRESCAN_CASE_ALL_HASH_KEY2", "GP7_PKEXPIRESCAN_CASE_ALL_HASH_FIELD2", "GP7_PKEXPIRESCAN_CASE_ALL_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP7_PKEXPIRESCAN_CASE_ALL_HASH_KEY3", "GP7_PKEXPIRESCAN_CASE_ALL_HASH_FIELD3", "GP7_PKEXPIRESCAN_CASE_ALL_HASH_VALUE3", &int32_ret);
  delete_keys.push_back("GP7_PKEXPIRESCAN_CASE_ALL_HASH_KEY1");
  delete_keys.push_back("GP7_PKEXPIRESCAN_CASE_ALL_HASH_KEY2");
  delete_keys.push_back("GP7_PKEXPIRESCAN_CASE_ALL_HASH_KEY3");

  //Set
  s = db.SAdd("GP7_PKEXPIRESCAN_CASE_ALL_SET_KEY1", {"GP7_PKEXPIRESCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP7_PKEXPIRESCAN_CASE_ALL_SET_KEY2", {"GP7_PKEXPIRESCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP7_PKEXPIRESCAN_CASE_ALL_SET_KEY3", {"GP7_PKEXPIRESCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);
  delete_keys.push_back("GP7_PKEXPIRESCAN_CASE_ALL_SET_KEY1");
  delete_keys.push_back("GP7_PKEXPIRESCAN_CASE_ALL_SET_KEY2");
  delete_keys.push_back("GP7_PKEXPIRESCAN_CASE_ALL_SET_KEY3");

  //List
  s = db.LPush("GP7_PKEXPIRESCAN_CASE_ALL_LIST_KEY1", {"GP7_PKEXPIRESCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP7_PKEXPIRESCAN_CASE_ALL_LIST_KEY2", {"GP7_PKEXPIRESCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP7_PKEXPIRESCAN_CASE_ALL_LIST_KEY3", {"GP7_PKEXPIRESCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);
  delete_keys.push_back("GP7_PKEXPIRESCAN_CASE_ALL_LIST_KEY1");
  delete_keys.push_back("GP7_PKEXPIRESCAN_CASE_ALL_LIST_KEY2");
  delete_keys.push_back("GP7_PKEXPIRESCAN_CASE_ALL_LIST_KEY3");

  //ZSet
  s = db.ZAdd("GP7_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1", {{1, "GP7_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP7_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2", {{1, "GP7_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP7_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3", {{1, "GP7_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.push_back("GP7_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1");
  delete_keys.push_back("GP7_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2");
  delete_keys.push_back("GP7_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3");

  ASSERT_TRUE(set_timeout(&db, "GP7_PKEXPIRESCAN_CASE_ALL_STRING_KEY1", 5));
  ASSERT_TRUE(set_timeout(&db, "GP7_PKEXPIRESCAN_CASE_ALL_STRING_KEY2", 15));
  ASSERT_TRUE(set_timeout(&db, "GP7_PKEXPIRESCAN_CASE_ALL_STRING_KEY3", 25));

  ASSERT_TRUE(set_timeout(&db, "GP7_PKEXPIRESCAN_CASE_ALL_HASH_KEY1", 5));
  ASSERT_TRUE(set_timeout(&db, "GP7_PKEXPIRESCAN_CASE_ALL_HASH_KEY2", 15));
  ASSERT_TRUE(set_timeout(&db, "GP7_PKEXPIRESCAN_CASE_ALL_HASH_KEY3", 25));

  ASSERT_TRUE(set_timeout(&db, "GP7_PKEXPIRESCAN_CASE_ALL_SET_KEY1", 5));
  ASSERT_TRUE(set_timeout(&db, "GP7_PKEXPIRESCAN_CASE_ALL_SET_KEY2", 15));
  ASSERT_TRUE(set_timeout(&db, "GP7_PKEXPIRESCAN_CASE_ALL_SET_KEY3", 25));

  ASSERT_TRUE(set_timeout(&db, "GP7_PKEXPIRESCAN_CASE_ALL_LIST_KEY1", 5));
  ASSERT_TRUE(set_timeout(&db, "GP7_PKEXPIRESCAN_CASE_ALL_LIST_KEY2", 15));
  ASSERT_TRUE(set_timeout(&db, "GP7_PKEXPIRESCAN_CASE_ALL_LIST_KEY3", 25));

  ASSERT_TRUE(set_timeout(&db, "GP7_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1", 5));
  ASSERT_TRUE(set_timeout(&db, "GP7_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2", 15));
  ASSERT_TRUE(set_timeout(&db, "GP7_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3", 25));

  cursor = 0;
  keys.clear();
  total_keys.clear();
  do {
    next_cursor = db.PKExpireScan(DataType::kAll, cursor, 10, 20, 5, &keys);
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  } while (cursor);
  ASSERT_EQ(total_keys.size(), 5);
  ASSERT_EQ(total_keys[0], "GP7_PKEXPIRESCAN_CASE_ALL_STRING_KEY2");
  ASSERT_EQ(total_keys[1], "GP7_PKEXPIRESCAN_CASE_ALL_HASH_KEY2");
  ASSERT_EQ(total_keys[2], "GP7_PKEXPIRESCAN_CASE_ALL_SET_KEY2");
  ASSERT_EQ(total_keys[3], "GP7_PKEXPIRESCAN_CASE_ALL_LIST_KEY2");
  ASSERT_EQ(total_keys[4], "GP7_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 8 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP8_PKEXPIRESCAN_CASE_ALL_STRING_KEY1", "GP8_PKEXPIRESCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP8_PKEXPIRESCAN_CASE_ALL_STRING_KEY2", "GP8_PKEXPIRESCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP8_PKEXPIRESCAN_CASE_ALL_STRING_KEY3", "GP8_PKEXPIRESCAN_CASE_ALL_STRING_VALUE3");
  delete_keys.push_back("GP8_PKEXPIRESCAN_CASE_ALL_STRING_KEY1");
  delete_keys.push_back("GP8_PKEXPIRESCAN_CASE_ALL_STRING_KEY2");
  delete_keys.push_back("GP8_PKEXPIRESCAN_CASE_ALL_STRING_KEY3");

  //Hash
  s = db.HSet("GP8_PKEXPIRESCAN_CASE_ALL_HASH_KEY1", "GP8_PKEXPIRESCAN_CASE_ALL_HASH_FIELD1", "GP8_PKEXPIRESCAN_CASE_ALL_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP8_PKEXPIRESCAN_CASE_ALL_HASH_KEY2", "GP8_PKEXPIRESCAN_CASE_ALL_HASH_FIELD2", "GP8_PKEXPIRESCAN_CASE_ALL_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP8_PKEXPIRESCAN_CASE_ALL_HASH_KEY3", "GP8_PKEXPIRESCAN_CASE_ALL_HASH_FIELD3", "GP8_PKEXPIRESCAN_CASE_ALL_HASH_VALUE3", &int32_ret);
  delete_keys.push_back("GP8_PKEXPIRESCAN_CASE_ALL_HASH_KEY1");
  delete_keys.push_back("GP8_PKEXPIRESCAN_CASE_ALL_HASH_KEY2");
  delete_keys.push_back("GP8_PKEXPIRESCAN_CASE_ALL_HASH_KEY3");

  //Set
  s = db.SAdd("GP8_PKEXPIRESCAN_CASE_ALL_SET_KEY1", {"GP8_PKEXPIRESCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP8_PKEXPIRESCAN_CASE_ALL_SET_KEY2", {"GP8_PKEXPIRESCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP8_PKEXPIRESCAN_CASE_ALL_SET_KEY3", {"GP8_PKEXPIRESCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);
  delete_keys.push_back("GP8_PKEXPIRESCAN_CASE_ALL_SET_KEY1");
  delete_keys.push_back("GP8_PKEXPIRESCAN_CASE_ALL_SET_KEY2");
  delete_keys.push_back("GP8_PKEXPIRESCAN_CASE_ALL_SET_KEY3");

  //List
  s = db.LPush("GP8_PKEXPIRESCAN_CASE_ALL_LIST_KEY1", {"GP8_PKEXPIRESCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP8_PKEXPIRESCAN_CASE_ALL_LIST_KEY2", {"GP8_PKEXPIRESCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP8_PKEXPIRESCAN_CASE_ALL_LIST_KEY3", {"GP8_PKEXPIRESCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);
  delete_keys.push_back("GP8_PKEXPIRESCAN_CASE_ALL_LIST_KEY1");
  delete_keys.push_back("GP8_PKEXPIRESCAN_CASE_ALL_LIST_KEY2");
  delete_keys.push_back("GP8_PKEXPIRESCAN_CASE_ALL_LIST_KEY3");

  //ZSet
  s = db.ZAdd("GP8_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1", {{1, "GP8_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP8_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2", {{1, "GP8_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP8_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3", {{1, "GP8_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.push_back("GP8_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1");
  delete_keys.push_back("GP8_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2");
  delete_keys.push_back("GP8_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3");

  ASSERT_TRUE(set_timeout(&db, "GP8_PKEXPIRESCAN_CASE_ALL_STRING_KEY1", 5));
  ASSERT_TRUE(set_timeout(&db, "GP8_PKEXPIRESCAN_CASE_ALL_STRING_KEY2", 25));
  ASSERT_TRUE(set_timeout(&db, "GP8_PKEXPIRESCAN_CASE_ALL_STRING_KEY3", 15));

  ASSERT_TRUE(set_timeout(&db, "GP8_PKEXPIRESCAN_CASE_ALL_HASH_KEY1", 5));
  ASSERT_TRUE(set_timeout(&db, "GP8_PKEXPIRESCAN_CASE_ALL_HASH_KEY2", 25));
  ASSERT_TRUE(set_timeout(&db, "GP8_PKEXPIRESCAN_CASE_ALL_HASH_KEY3", 15));

  ASSERT_TRUE(set_timeout(&db, "GP8_PKEXPIRESCAN_CASE_ALL_SET_KEY1", 5));
  ASSERT_TRUE(set_timeout(&db, "GP8_PKEXPIRESCAN_CASE_ALL_SET_KEY2", 25));
  ASSERT_TRUE(set_timeout(&db, "GP8_PKEXPIRESCAN_CASE_ALL_SET_KEY3", 15));

  ASSERT_TRUE(set_timeout(&db, "GP8_PKEXPIRESCAN_CASE_ALL_LIST_KEY1", 5));
  ASSERT_TRUE(set_timeout(&db, "GP8_PKEXPIRESCAN_CASE_ALL_LIST_KEY2", 25));
  ASSERT_TRUE(set_timeout(&db, "GP8_PKEXPIRESCAN_CASE_ALL_LIST_KEY3", 15));

  ASSERT_TRUE(set_timeout(&db, "GP8_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1", 5));
  ASSERT_TRUE(set_timeout(&db, "GP8_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2", 25));
  ASSERT_TRUE(set_timeout(&db, "GP8_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3", 15));

  cursor = 0;
  keys.clear();
  total_keys.clear();
  do {
    next_cursor = db.PKExpireScan(DataType::kAll, cursor, 10, 20, 5, &keys);
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  } while (cursor);
  ASSERT_EQ(total_keys.size(), 5);
  ASSERT_EQ(total_keys[0], "GP8_PKEXPIRESCAN_CASE_ALL_STRING_KEY3");
  ASSERT_EQ(total_keys[1], "GP8_PKEXPIRESCAN_CASE_ALL_HASH_KEY3");
  ASSERT_EQ(total_keys[2], "GP8_PKEXPIRESCAN_CASE_ALL_SET_KEY3");
  ASSERT_EQ(total_keys[3], "GP8_PKEXPIRESCAN_CASE_ALL_LIST_KEY3");
  ASSERT_EQ(total_keys[4], "GP8_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 9 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP9_PKEXPIRESCAN_CASE_ALL_STRING_KEY1", "GP9_PKEXPIRESCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP9_PKEXPIRESCAN_CASE_ALL_STRING_KEY2", "GP9_PKEXPIRESCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP9_PKEXPIRESCAN_CASE_ALL_STRING_KEY3", "GP9_PKEXPIRESCAN_CASE_ALL_STRING_VALUE3");
  delete_keys.push_back("GP9_PKEXPIRESCAN_CASE_ALL_STRING_KEY1");
  delete_keys.push_back("GP9_PKEXPIRESCAN_CASE_ALL_STRING_KEY2");
  delete_keys.push_back("GP9_PKEXPIRESCAN_CASE_ALL_STRING_KEY3");

  //Hash
  s = db.HSet("GP9_PKEXPIRESCAN_CASE_ALL_HASH_KEY1", "GP9_PKEXPIRESCAN_CASE_ALL_HASH_FIELD1", "GP9_PKEXPIRESCAN_CASE_ALL_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP9_PKEXPIRESCAN_CASE_ALL_HASH_KEY2", "GP9_PKEXPIRESCAN_CASE_ALL_HASH_FIELD2", "GP9_PKEXPIRESCAN_CASE_ALL_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP9_PKEXPIRESCAN_CASE_ALL_HASH_KEY3", "GP9_PKEXPIRESCAN_CASE_ALL_HASH_FIELD3", "GP9_PKEXPIRESCAN_CASE_ALL_HASH_VALUE3", &int32_ret);
  delete_keys.push_back("GP9_PKEXPIRESCAN_CASE_ALL_HASH_KEY1");
  delete_keys.push_back("GP9_PKEXPIRESCAN_CASE_ALL_HASH_KEY2");
  delete_keys.push_back("GP9_PKEXPIRESCAN_CASE_ALL_HASH_KEY3");

  //Set
  s = db.SAdd("GP9_PKEXPIRESCAN_CASE_ALL_SET_KEY1", {"GP9_PKEXPIRESCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP9_PKEXPIRESCAN_CASE_ALL_SET_KEY2", {"GP9_PKEXPIRESCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP9_PKEXPIRESCAN_CASE_ALL_SET_KEY3", {"GP9_PKEXPIRESCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);
  delete_keys.push_back("GP9_PKEXPIRESCAN_CASE_ALL_SET_KEY1");
  delete_keys.push_back("GP9_PKEXPIRESCAN_CASE_ALL_SET_KEY2");
  delete_keys.push_back("GP9_PKEXPIRESCAN_CASE_ALL_SET_KEY3");

  //List
  s = db.LPush("GP9_PKEXPIRESCAN_CASE_ALL_LIST_KEY1", {"GP9_PKEXPIRESCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP9_PKEXPIRESCAN_CASE_ALL_LIST_KEY2", {"GP9_PKEXPIRESCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP9_PKEXPIRESCAN_CASE_ALL_LIST_KEY3", {"GP9_PKEXPIRESCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);
  delete_keys.push_back("GP9_PKEXPIRESCAN_CASE_ALL_LIST_KEY1");
  delete_keys.push_back("GP9_PKEXPIRESCAN_CASE_ALL_LIST_KEY2");
  delete_keys.push_back("GP9_PKEXPIRESCAN_CASE_ALL_LIST_KEY3");

  //ZSet
  s = db.ZAdd("GP9_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1", {{1, "GP9_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP9_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2", {{1, "GP9_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP9_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3", {{1, "GP9_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.push_back("GP9_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1");
  delete_keys.push_back("GP9_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2");
  delete_keys.push_back("GP9_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3");

  ASSERT_TRUE(set_timeout(&db, "GP9_PKEXPIRESCAN_CASE_ALL_STRING_KEY1", 5));
  ASSERT_TRUE(set_timeout(&db, "GP9_PKEXPIRESCAN_CASE_ALL_STRING_KEY2", 25));
  ASSERT_TRUE(set_timeout(&db, "GP9_PKEXPIRESCAN_CASE_ALL_STRING_KEY3", 15));

  ASSERT_TRUE(set_timeout(&db, "GP9_PKEXPIRESCAN_CASE_ALL_HASH_KEY1", 6));
  ASSERT_TRUE(set_timeout(&db, "GP9_PKEXPIRESCAN_CASE_ALL_HASH_KEY2", 16));
  ASSERT_TRUE(set_timeout(&db, "GP9_PKEXPIRESCAN_CASE_ALL_HASH_KEY3", 26));

  ASSERT_TRUE(set_timeout(&db, "GP9_PKEXPIRESCAN_CASE_ALL_SET_KEY1", 7));
  ASSERT_TRUE(set_timeout(&db, "GP9_PKEXPIRESCAN_CASE_ALL_SET_KEY2", 17));
  ASSERT_TRUE(set_timeout(&db, "GP9_PKEXPIRESCAN_CASE_ALL_SET_KEY3", 27));

  ASSERT_TRUE(set_timeout(&db, "GP9_PKEXPIRESCAN_CASE_ALL_LIST_KEY1", 8));
  ASSERT_TRUE(set_timeout(&db, "GP9_PKEXPIRESCAN_CASE_ALL_LIST_KEY2", 18));
  ASSERT_TRUE(set_timeout(&db, "GP9_PKEXPIRESCAN_CASE_ALL_LIST_KEY3", 28));

  ASSERT_TRUE(set_timeout(&db, "GP9_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1", 9));
  ASSERT_TRUE(set_timeout(&db, "GP9_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2", 19));
  ASSERT_TRUE(set_timeout(&db, "GP9_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3", 29));

  cursor = 0;
  keys.clear();
  total_keys.clear();
  do {
    next_cursor = db.PKExpireScan(DataType::kAll, cursor, 0, 30, 5, &keys);
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  } while (cursor);
  ASSERT_EQ(total_keys.size(), 15);
  ASSERT_EQ(total_keys[0],  "GP9_PKEXPIRESCAN_CASE_ALL_STRING_KEY1");
  ASSERT_EQ(total_keys[1],  "GP9_PKEXPIRESCAN_CASE_ALL_STRING_KEY2");
  ASSERT_EQ(total_keys[2],  "GP9_PKEXPIRESCAN_CASE_ALL_STRING_KEY3");
  ASSERT_EQ(total_keys[3],  "GP9_PKEXPIRESCAN_CASE_ALL_HASH_KEY1");
  ASSERT_EQ(total_keys[4],  "GP9_PKEXPIRESCAN_CASE_ALL_HASH_KEY2");
  ASSERT_EQ(total_keys[5],  "GP9_PKEXPIRESCAN_CASE_ALL_HASH_KEY3");
  ASSERT_EQ(total_keys[6],  "GP9_PKEXPIRESCAN_CASE_ALL_SET_KEY1");
  ASSERT_EQ(total_keys[7],  "GP9_PKEXPIRESCAN_CASE_ALL_SET_KEY2");
  ASSERT_EQ(total_keys[8],  "GP9_PKEXPIRESCAN_CASE_ALL_SET_KEY3");
  ASSERT_EQ(total_keys[9],  "GP9_PKEXPIRESCAN_CASE_ALL_LIST_KEY1");
  ASSERT_EQ(total_keys[10], "GP9_PKEXPIRESCAN_CASE_ALL_LIST_KEY2");
  ASSERT_EQ(total_keys[11], "GP9_PKEXPIRESCAN_CASE_ALL_LIST_KEY3");
  ASSERT_EQ(total_keys[12], "GP9_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1");
  ASSERT_EQ(total_keys[13], "GP9_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2");
  ASSERT_EQ(total_keys[14], "GP9_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 10 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP10_PKEXPIRESCAN_CASE_ALL_STRING_KEY1", "GP10_PKEXPIRESCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP10_PKEXPIRESCAN_CASE_ALL_STRING_KEY2", "GP10_PKEXPIRESCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP10_PKEXPIRESCAN_CASE_ALL_STRING_KEY3", "GP10_PKEXPIRESCAN_CASE_ALL_STRING_VALUE3");
  delete_keys.push_back("GP10_PKEXPIRESCAN_CASE_ALL_STRING_KEY1");
  delete_keys.push_back("GP10_PKEXPIRESCAN_CASE_ALL_STRING_KEY2");
  delete_keys.push_back("GP10_PKEXPIRESCAN_CASE_ALL_STRING_KEY3");

  //Hash
  s = db.HSet("GP10_PKEXPIRESCAN_CASE_ALL_HASH_KEY1", "GP10_PKEXPIRESCAN_CASE_ALL_HASH_FIELD1", "GP10_PKEXPIRESCAN_CASE_ALL_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP10_PKEXPIRESCAN_CASE_ALL_HASH_KEY2", "GP10_PKEXPIRESCAN_CASE_ALL_HASH_FIELD2", "GP10_PKEXPIRESCAN_CASE_ALL_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP10_PKEXPIRESCAN_CASE_ALL_HASH_KEY3", "GP10_PKEXPIRESCAN_CASE_ALL_HASH_FIELD3", "GP10_PKEXPIRESCAN_CASE_ALL_HASH_VALUE3", &int32_ret);
  delete_keys.push_back("GP10_PKEXPIRESCAN_CASE_ALL_HASH_KEY1");
  delete_keys.push_back("GP10_PKEXPIRESCAN_CASE_ALL_HASH_KEY2");
  delete_keys.push_back("GP10_PKEXPIRESCAN_CASE_ALL_HASH_KEY3");

  //Set
  s = db.SAdd("GP10_PKEXPIRESCAN_CASE_ALL_SET_KEY1", {"GP10_PKEXPIRESCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP10_PKEXPIRESCAN_CASE_ALL_SET_KEY2", {"GP10_PKEXPIRESCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP10_PKEXPIRESCAN_CASE_ALL_SET_KEY3", {"GP10_PKEXPIRESCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);
  delete_keys.push_back("GP10_PKEXPIRESCAN_CASE_ALL_SET_KEY1");
  delete_keys.push_back("GP10_PKEXPIRESCAN_CASE_ALL_SET_KEY2");
  delete_keys.push_back("GP10_PKEXPIRESCAN_CASE_ALL_SET_KEY3");

  //List
  s = db.LPush("GP10_PKEXPIRESCAN_CASE_ALL_LIST_KEY1", {"GP10_PKEXPIRESCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP10_PKEXPIRESCAN_CASE_ALL_LIST_KEY2", {"GP10_PKEXPIRESCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP10_PKEXPIRESCAN_CASE_ALL_LIST_KEY3", {"GP10_PKEXPIRESCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);
  delete_keys.push_back("GP10_PKEXPIRESCAN_CASE_ALL_LIST_KEY1");
  delete_keys.push_back("GP10_PKEXPIRESCAN_CASE_ALL_LIST_KEY2");
  delete_keys.push_back("GP10_PKEXPIRESCAN_CASE_ALL_LIST_KEY3");

  //ZSet
  s = db.ZAdd("GP10_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1", {{1, "GP10_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP10_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2", {{1, "GP10_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP10_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3", {{1, "GP10_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.push_back("GP10_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1");
  delete_keys.push_back("GP10_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2");
  delete_keys.push_back("GP10_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3");

  ASSERT_TRUE(set_timeout(&db, "GP10_PKEXPIRESCAN_CASE_ALL_STRING_KEY1", 15));
  ASSERT_TRUE(set_timeout(&db, "GP10_PKEXPIRESCAN_CASE_ALL_STRING_KEY2", 15));
  ASSERT_TRUE(set_timeout(&db, "GP10_PKEXPIRESCAN_CASE_ALL_STRING_KEY3", 15));

  ASSERT_TRUE(set_timeout(&db, "GP10_PKEXPIRESCAN_CASE_ALL_HASH_KEY1", 5));
  ASSERT_TRUE(set_timeout(&db, "GP10_PKEXPIRESCAN_CASE_ALL_HASH_KEY2", 5));
  ASSERT_TRUE(set_timeout(&db, "GP10_PKEXPIRESCAN_CASE_ALL_HASH_KEY3", 5));

  ASSERT_TRUE(set_timeout(&db, "GP10_PKEXPIRESCAN_CASE_ALL_SET_KEY1", 5));
  ASSERT_TRUE(set_timeout(&db, "GP10_PKEXPIRESCAN_CASE_ALL_SET_KEY2", 5));
  ASSERT_TRUE(set_timeout(&db, "GP10_PKEXPIRESCAN_CASE_ALL_SET_KEY3", 5));

  ASSERT_TRUE(set_timeout(&db, "GP10_PKEXPIRESCAN_CASE_ALL_LIST_KEY1", 25));
  ASSERT_TRUE(set_timeout(&db, "GP10_PKEXPIRESCAN_CASE_ALL_LIST_KEY2", 25));
  ASSERT_TRUE(set_timeout(&db, "GP10_PKEXPIRESCAN_CASE_ALL_LIST_KEY3", 25));

  ASSERT_TRUE(set_timeout(&db, "GP10_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1", 25));
  ASSERT_TRUE(set_timeout(&db, "GP10_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2", 25));
  ASSERT_TRUE(set_timeout(&db, "GP10_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3", 25));

  cursor = 0;
  keys.clear();
  total_keys.clear();
  do {
    next_cursor = db.PKExpireScan(DataType::kAll, cursor, 10, 20, 5, &keys);
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  } while (cursor);
  ASSERT_EQ(total_keys.size(), 3);
  ASSERT_EQ(total_keys[0], "GP10_PKEXPIRESCAN_CASE_ALL_STRING_KEY1");
  ASSERT_EQ(total_keys[1], "GP10_PKEXPIRESCAN_CASE_ALL_STRING_KEY2");
  ASSERT_EQ(total_keys[2], "GP10_PKEXPIRESCAN_CASE_ALL_STRING_KEY3");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 11 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP11_PKEXPIRESCAN_CASE_ALL_STRING_KEY1", "GP11_PKEXPIRESCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP11_PKEXPIRESCAN_CASE_ALL_STRING_KEY2", "GP11_PKEXPIRESCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP11_PKEXPIRESCAN_CASE_ALL_STRING_KEY3", "GP11_PKEXPIRESCAN_CASE_ALL_STRING_VALUE3");
  delete_keys.push_back("GP11_PKEXPIRESCAN_CASE_ALL_STRING_KEY1");
  delete_keys.push_back("GP11_PKEXPIRESCAN_CASE_ALL_STRING_KEY2");
  delete_keys.push_back("GP11_PKEXPIRESCAN_CASE_ALL_STRING_KEY3");

  //Hash
  s = db.HSet("GP11_PKEXPIRESCAN_CASE_ALL_HASH_KEY1", "GP11_PKEXPIRESCAN_CASE_ALL_HASH_FIELD1", "GP11_PKEXPIRESCAN_CASE_ALL_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP11_PKEXPIRESCAN_CASE_ALL_HASH_KEY2", "GP11_PKEXPIRESCAN_CASE_ALL_HASH_FIELD2", "GP11_PKEXPIRESCAN_CASE_ALL_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP11_PKEXPIRESCAN_CASE_ALL_HASH_KEY3", "GP11_PKEXPIRESCAN_CASE_ALL_HASH_FIELD3", "GP11_PKEXPIRESCAN_CASE_ALL_HASH_VALUE3", &int32_ret);
  delete_keys.push_back("GP11_PKEXPIRESCAN_CASE_ALL_HASH_KEY1");
  delete_keys.push_back("GP11_PKEXPIRESCAN_CASE_ALL_HASH_KEY2");
  delete_keys.push_back("GP11_PKEXPIRESCAN_CASE_ALL_HASH_KEY3");

  //Set
  s = db.SAdd("GP11_PKEXPIRESCAN_CASE_ALL_SET_KEY1", {"GP11_PKEXPIRESCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP11_PKEXPIRESCAN_CASE_ALL_SET_KEY2", {"GP11_PKEXPIRESCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP11_PKEXPIRESCAN_CASE_ALL_SET_KEY3", {"GP11_PKEXPIRESCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);
  delete_keys.push_back("GP11_PKEXPIRESCAN_CASE_ALL_SET_KEY1");
  delete_keys.push_back("GP11_PKEXPIRESCAN_CASE_ALL_SET_KEY2");
  delete_keys.push_back("GP11_PKEXPIRESCAN_CASE_ALL_SET_KEY3");

  //List
  s = db.LPush("GP11_PKEXPIRESCAN_CASE_ALL_LIST_KEY1", {"GP11_PKEXPIRESCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP11_PKEXPIRESCAN_CASE_ALL_LIST_KEY2", {"GP11_PKEXPIRESCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP11_PKEXPIRESCAN_CASE_ALL_LIST_KEY3", {"GP11_PKEXPIRESCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);
  delete_keys.push_back("GP11_PKEXPIRESCAN_CASE_ALL_LIST_KEY1");
  delete_keys.push_back("GP11_PKEXPIRESCAN_CASE_ALL_LIST_KEY2");
  delete_keys.push_back("GP11_PKEXPIRESCAN_CASE_ALL_LIST_KEY3");

  //ZSet
  s = db.ZAdd("GP11_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1", {{1, "GP11_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP11_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2", {{1, "GP11_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP11_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3", {{1, "GP11_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.push_back("GP11_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1");
  delete_keys.push_back("GP11_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2");
  delete_keys.push_back("GP11_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3");

  ASSERT_TRUE(set_timeout(&db, "GP11_PKEXPIRESCAN_CASE_ALL_STRING_KEY1", 5));
  ASSERT_TRUE(set_timeout(&db, "GP11_PKEXPIRESCAN_CASE_ALL_STRING_KEY2", 5));
  ASSERT_TRUE(set_timeout(&db, "GP11_PKEXPIRESCAN_CASE_ALL_STRING_KEY3", 5));

  ASSERT_TRUE(set_timeout(&db, "GP11_PKEXPIRESCAN_CASE_ALL_HASH_KEY1", 5));
  ASSERT_TRUE(set_timeout(&db, "GP11_PKEXPIRESCAN_CASE_ALL_HASH_KEY2", 5));
  ASSERT_TRUE(set_timeout(&db, "GP11_PKEXPIRESCAN_CASE_ALL_HASH_KEY3", 5));

  ASSERT_TRUE(set_timeout(&db, "GP11_PKEXPIRESCAN_CASE_ALL_SET_KEY1", 15));
  ASSERT_TRUE(set_timeout(&db, "GP11_PKEXPIRESCAN_CASE_ALL_SET_KEY2", 15));
  ASSERT_TRUE(set_timeout(&db, "GP11_PKEXPIRESCAN_CASE_ALL_SET_KEY3", 15));

  ASSERT_TRUE(set_timeout(&db, "GP11_PKEXPIRESCAN_CASE_ALL_LIST_KEY1", 25));
  ASSERT_TRUE(set_timeout(&db, "GP11_PKEXPIRESCAN_CASE_ALL_LIST_KEY2", 25));
  ASSERT_TRUE(set_timeout(&db, "GP11_PKEXPIRESCAN_CASE_ALL_LIST_KEY3", 25));

  ASSERT_TRUE(set_timeout(&db, "GP11_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1", 25));
  ASSERT_TRUE(set_timeout(&db, "GP11_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2", 25));
  ASSERT_TRUE(set_timeout(&db, "GP11_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3", 25));

  cursor = 0;
  keys.clear();
  total_keys.clear();
  do {
    next_cursor = db.PKExpireScan(DataType::kAll, cursor, 10, 20, 5, &keys);
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  } while (cursor);
  ASSERT_EQ(total_keys.size(), 3);
  ASSERT_EQ(total_keys[0], "GP11_PKEXPIRESCAN_CASE_ALL_SET_KEY1");
  ASSERT_EQ(total_keys[1], "GP11_PKEXPIRESCAN_CASE_ALL_SET_KEY2");
  ASSERT_EQ(total_keys[2], "GP11_PKEXPIRESCAN_CASE_ALL_SET_KEY3");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 12 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP12_PKEXPIRESCAN_CASE_ALL_STRING_KEY1", "GP12_PKEXPIRESCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP12_PKEXPIRESCAN_CASE_ALL_STRING_KEY2", "GP12_PKEXPIRESCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP12_PKEXPIRESCAN_CASE_ALL_STRING_KEY3", "GP12_PKEXPIRESCAN_CASE_ALL_STRING_VALUE3");
  delete_keys.push_back("GP12_PKEXPIRESCAN_CASE_ALL_STRING_KEY1");
  delete_keys.push_back("GP12_PKEXPIRESCAN_CASE_ALL_STRING_KEY2");
  delete_keys.push_back("GP12_PKEXPIRESCAN_CASE_ALL_STRING_KEY3");

  //Hash
  s = db.HSet("GP12_PKEXPIRESCAN_CASE_ALL_HASH_KEY1", "GP12_PKEXPIRESCAN_CASE_ALL_HASH_FIELD1", "GP12_PKEXPIRESCAN_CASE_ALL_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP12_PKEXPIRESCAN_CASE_ALL_HASH_KEY2", "GP12_PKEXPIRESCAN_CASE_ALL_HASH_FIELD2", "GP12_PKEXPIRESCAN_CASE_ALL_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP12_PKEXPIRESCAN_CASE_ALL_HASH_KEY3", "GP12_PKEXPIRESCAN_CASE_ALL_HASH_FIELD3", "GP12_PKEXPIRESCAN_CASE_ALL_HASH_VALUE3", &int32_ret);
  delete_keys.push_back("GP12_PKEXPIRESCAN_CASE_ALL_HASH_KEY1");
  delete_keys.push_back("GP12_PKEXPIRESCAN_CASE_ALL_HASH_KEY2");
  delete_keys.push_back("GP12_PKEXPIRESCAN_CASE_ALL_HASH_KEY3");

  //Set
  s = db.SAdd("GP12_PKEXPIRESCAN_CASE_ALL_SET_KEY1", {"GP12_PKEXPIRESCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP12_PKEXPIRESCAN_CASE_ALL_SET_KEY2", {"GP12_PKEXPIRESCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP12_PKEXPIRESCAN_CASE_ALL_SET_KEY3", {"GP12_PKEXPIRESCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);
  delete_keys.push_back("GP12_PKEXPIRESCAN_CASE_ALL_SET_KEY1");
  delete_keys.push_back("GP12_PKEXPIRESCAN_CASE_ALL_SET_KEY2");
  delete_keys.push_back("GP12_PKEXPIRESCAN_CASE_ALL_SET_KEY3");

  //List
  s = db.LPush("GP12_PKEXPIRESCAN_CASE_ALL_LIST_KEY1", {"GP12_PKEXPIRESCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP12_PKEXPIRESCAN_CASE_ALL_LIST_KEY2", {"GP12_PKEXPIRESCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP12_PKEXPIRESCAN_CASE_ALL_LIST_KEY3", {"GP12_PKEXPIRESCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);
  delete_keys.push_back("GP12_PKEXPIRESCAN_CASE_ALL_LIST_KEY1");
  delete_keys.push_back("GP12_PKEXPIRESCAN_CASE_ALL_LIST_KEY2");
  delete_keys.push_back("GP12_PKEXPIRESCAN_CASE_ALL_LIST_KEY3");

  //ZSet
  s = db.ZAdd("GP12_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1", {{1, "GP12_PKEXPIRESCAN_CASE_ALL_ZSET_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP12_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2", {{1, "GP12_PKEXPIRESCAN_CASE_ALL_ZSET_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP12_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3", {{1, "GP12_PKEXPIRESCAN_CASE_ALL_ZSET_MEMBER3"}}, &int32_ret);
  delete_keys.push_back("GP12_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1");
  delete_keys.push_back("GP12_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2");
  delete_keys.push_back("GP12_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3");

  ASSERT_TRUE(set_timeout(&db, "GP12_PKEXPIRESCAN_CASE_ALL_STRING_KEY1", 5));
  ASSERT_TRUE(set_timeout(&db, "GP12_PKEXPIRESCAN_CASE_ALL_STRING_KEY2", 5));
  ASSERT_TRUE(set_timeout(&db, "GP12_PKEXPIRESCAN_CASE_ALL_STRING_KEY3", 5));

  ASSERT_TRUE(set_timeout(&db, "GP12_PKEXPIRESCAN_CASE_ALL_HASH_KEY1", 5));
  ASSERT_TRUE(set_timeout(&db, "GP12_PKEXPIRESCAN_CASE_ALL_HASH_KEY2", 5));
  ASSERT_TRUE(set_timeout(&db, "GP12_PKEXPIRESCAN_CASE_ALL_HASH_KEY3", 5));

  ASSERT_TRUE(set_timeout(&db, "GP12_PKEXPIRESCAN_CASE_ALL_SET_KEY1", 25));
  ASSERT_TRUE(set_timeout(&db, "GP12_PKEXPIRESCAN_CASE_ALL_SET_KEY2", 25));
  ASSERT_TRUE(set_timeout(&db, "GP12_PKEXPIRESCAN_CASE_ALL_SET_KEY3", 25));

  ASSERT_TRUE(set_timeout(&db, "GP12_PKEXPIRESCAN_CASE_ALL_LIST_KEY1", 25));
  ASSERT_TRUE(set_timeout(&db, "GP12_PKEXPIRESCAN_CASE_ALL_LIST_KEY2", 25));
  ASSERT_TRUE(set_timeout(&db, "GP12_PKEXPIRESCAN_CASE_ALL_LIST_KEY3", 25));

  ASSERT_TRUE(set_timeout(&db, "GP12_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1", 15));
  ASSERT_TRUE(set_timeout(&db, "GP12_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2", 15));
  ASSERT_TRUE(set_timeout(&db, "GP12_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3", 15));

  cursor = 0;
  keys.clear();
  total_keys.clear();
  do {
    next_cursor = db.PKExpireScan(DataType::kAll, cursor, 10, 20, 5, &keys);
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  } while (cursor);
  ASSERT_EQ(total_keys.size(), 3);
  ASSERT_EQ(total_keys[0], "GP12_PKEXPIRESCAN_CASE_ALL_ZSET_KEY1");
  ASSERT_EQ(total_keys[1], "GP12_PKEXPIRESCAN_CASE_ALL_ZSET_KEY2");
  ASSERT_EQ(total_keys[2], "GP12_PKEXPIRESCAN_CASE_ALL_ZSET_KEY3");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 13 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP13_KEY1_PKEXPIRESCAN_CASE_ALL_STRING", "GP13_PKEXPIRESCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP13_KEY2_PKEXPIRESCAN_CASE_ALL_STRING", "GP13_PKEXPIRESCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP13_KEY3_PKEXPIRESCAN_CASE_ALL_STRING", "GP13_PKEXPIRESCAN_CASE_ALL_STRING_VALUE3");
  delete_keys.push_back("GP13_KEY1_PKEXPIRESCAN_CASE_ALL_STRING");
  delete_keys.push_back("GP13_KEY2_PKEXPIRESCAN_CASE_ALL_STRING");
  delete_keys.push_back("GP13_KEY3_PKEXPIRESCAN_CASE_ALL_STRING");

  //Hash
  s = db.HSet("GP13_KEY1_PKEXPIRESCAN_CASE_ALL_HASH", "GP13_PKEXPIRESCAN_CASE_ALL_HASH_FIELD1", "GP13_PKEXPIRESCAN_CASE_ALL_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP13_KEY2_PKEXPIRESCAN_CASE_ALL_HASH", "GP13_PKEXPIRESCAN_CASE_ALL_HASH_FIELD2", "GP13_PKEXPIRESCAN_CASE_ALL_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP13_KEY3_PKEXPIRESCAN_CASE_ALL_HASH", "GP13_PKEXPIRESCAN_CASE_ALL_HASH_FIELD3", "GP13_PKEXPIRESCAN_CASE_ALL_HASH_VALUE3", &int32_ret);
  delete_keys.push_back("GP13_KEY1_PKEXPIRESCAN_CASE_ALL_HASH");
  delete_keys.push_back("GP13_KEY2_PKEXPIRESCAN_CASE_ALL_HASH");
  delete_keys.push_back("GP13_KEY3_PKEXPIRESCAN_CASE_ALL_HASH");

  //Set
  s = db.SAdd("GP13_KEY1_PKEXPIRESCAN_CASE_ALL_SET", {"GP13_PKEXPIRESCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP13_KEY2_PKEXPIRESCAN_CASE_ALL_SET", {"GP13_PKEXPIRESCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP13_KEY3_PKEXPIRESCAN_CASE_ALL_SET", {"GP13_PKEXPIRESCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);
  delete_keys.push_back("GP13_KEY1_PKEXPIRESCAN_CASE_ALL_SET");
  delete_keys.push_back("GP13_KEY2_PKEXPIRESCAN_CASE_ALL_SET");
  delete_keys.push_back("GP13_KEY3_PKEXPIRESCAN_CASE_ALL_SET");

  //List
  s = db.LPush("GP13_KEY1_PKEXPIRESCAN_CASE_ALL_LIST", {"GP13_PKEXPIRESCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP13_KEY2_PKEXPIRESCAN_CASE_ALL_LIST", {"GP13_PKEXPIRESCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP13_KEY3_PKEXPIRESCAN_CASE_ALL_LIST", {"GP13_PKEXPIRESCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);
  delete_keys.push_back("GP13_KEY1_PKEXPIRESCAN_CASE_ALL_LIST");
  delete_keys.push_back("GP13_KEY2_PKEXPIRESCAN_CASE_ALL_LIST");
  delete_keys.push_back("GP13_KEY3_PKEXPIRESCAN_CASE_ALL_LIST");

  //ZSet
  s = db.ZAdd("GP13_KEY1_PKEXPIRESCAN_CASE_ALL_ZSET", {{1, "GP13_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP13_KEY2_PKEXPIRESCAN_CASE_ALL_ZSET", {{1, "GP13_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP13_KEY3_PKEXPIRESCAN_CASE_ALL_ZSET", {{1, "GP13_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.push_back("GP13_KEY1_PKEXPIRESCAN_CASE_ALL_ZSET");
  delete_keys.push_back("GP13_KEY2_PKEXPIRESCAN_CASE_ALL_ZSET");
  delete_keys.push_back("GP13_KEY3_PKEXPIRESCAN_CASE_ALL_ZSET");

  ASSERT_TRUE(set_timeout(&db, "GP13_KEY1_PKEXPIRESCAN_CASE_ALL_STRING", 15));
  ASSERT_TRUE(set_timeout(&db, "GP13_KEY2_PKEXPIRESCAN_CASE_ALL_STRING", 5));
  ASSERT_TRUE(set_timeout(&db, "GP13_KEY3_PKEXPIRESCAN_CASE_ALL_STRING", 25));

  ASSERT_TRUE(set_timeout(&db, "GP13_KEY1_PKEXPIRESCAN_CASE_ALL_HASH", 15));
  ASSERT_TRUE(set_timeout(&db, "GP13_KEY2_PKEXPIRESCAN_CASE_ALL_HASH", 5));
  ASSERT_TRUE(set_timeout(&db, "GP13_KEY3_PKEXPIRESCAN_CASE_ALL_HASH", 25));

  ASSERT_TRUE(set_timeout(&db, "GP13_KEY1_PKEXPIRESCAN_CASE_ALL_SET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP13_KEY2_PKEXPIRESCAN_CASE_ALL_SET", 5));
  ASSERT_TRUE(set_timeout(&db, "GP13_KEY3_PKEXPIRESCAN_CASE_ALL_SET", 25));

  ASSERT_TRUE(set_timeout(&db, "GP13_KEY1_PKEXPIRESCAN_CASE_ALL_LIST", 15));
  ASSERT_TRUE(set_timeout(&db, "GP13_KEY2_PKEXPIRESCAN_CASE_ALL_LIST", 5));
  ASSERT_TRUE(set_timeout(&db, "GP13_KEY3_PKEXPIRESCAN_CASE_ALL_LIST", 25));

  ASSERT_TRUE(set_timeout(&db, "GP13_KEY1_PKEXPIRESCAN_CASE_ALL_ZSET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP13_KEY2_PKEXPIRESCAN_CASE_ALL_ZSET", 5));
  ASSERT_TRUE(set_timeout(&db, "GP13_KEY3_PKEXPIRESCAN_CASE_ALL_ZSET", 25));

  cursor = 0;
  keys.clear();
  total_keys.clear();
  do {
    next_cursor = db.PKExpireScan(DataType::kAll, cursor, 10, 20, 1, &keys);
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  } while (cursor);
  ASSERT_EQ(total_keys.size(), 5);
  ASSERT_EQ(total_keys[0], "GP13_KEY1_PKEXPIRESCAN_CASE_ALL_STRING");
  ASSERT_EQ(total_keys[1], "GP13_KEY1_PKEXPIRESCAN_CASE_ALL_HASH");
  ASSERT_EQ(total_keys[2], "GP13_KEY1_PKEXPIRESCAN_CASE_ALL_SET");
  ASSERT_EQ(total_keys[3], "GP13_KEY1_PKEXPIRESCAN_CASE_ALL_LIST");
  ASSERT_EQ(total_keys[4], "GP13_KEY1_PKEXPIRESCAN_CASE_ALL_ZSET");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 14 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP14_KEY1_PKEXPIRESCAN_CASE_ALL_STRING", "GP14_PKEXPIRESCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP14_KEY2_PKEXPIRESCAN_CASE_ALL_STRING", "GP14_PKEXPIRESCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP14_KEY3_PKEXPIRESCAN_CASE_ALL_STRING", "GP14_PKEXPIRESCAN_CASE_ALL_STRING_VALUE3");
  delete_keys.push_back("GP14_KEY1_PKEXPIRESCAN_CASE_ALL_STRING");
  delete_keys.push_back("GP14_KEY2_PKEXPIRESCAN_CASE_ALL_STRING");
  delete_keys.push_back("GP14_KEY3_PKEXPIRESCAN_CASE_ALL_STRING");

  //Hash
  s = db.HSet("GP14_KEY1_PKEXPIRESCAN_CASE_ALL_HASH", "GP14_PKEXPIRESCAN_CASE_ALL_HASH_FIELD1", "GP14_PKEXPIRESCAN_CASE_ALL_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP14_KEY2_PKEXPIRESCAN_CASE_ALL_HASH", "GP14_PKEXPIRESCAN_CASE_ALL_HASH_FIELD2", "GP14_PKEXPIRESCAN_CASE_ALL_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP14_KEY3_PKEXPIRESCAN_CASE_ALL_HASH", "GP14_PKEXPIRESCAN_CASE_ALL_HASH_FIELD3", "GP14_PKEXPIRESCAN_CASE_ALL_HASH_VALUE3", &int32_ret);
  delete_keys.push_back("GP14_KEY1_PKEXPIRESCAN_CASE_ALL_HASH");
  delete_keys.push_back("GP14_KEY2_PKEXPIRESCAN_CASE_ALL_HASH");
  delete_keys.push_back("GP14_KEY3_PKEXPIRESCAN_CASE_ALL_HASH");

  //Set
  s = db.SAdd("GP14_KEY1_PKEXPIRESCAN_CASE_ALL_SET", {"GP14_PKEXPIRESCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP14_KEY2_PKEXPIRESCAN_CASE_ALL_SET", {"GP14_PKEXPIRESCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP14_KEY3_PKEXPIRESCAN_CASE_ALL_SET", {"GP14_PKEXPIRESCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);
  delete_keys.push_back("GP14_KEY1_PKEXPIRESCAN_CASE_ALL_SET");
  delete_keys.push_back("GP14_KEY2_PKEXPIRESCAN_CASE_ALL_SET");
  delete_keys.push_back("GP14_KEY3_PKEXPIRESCAN_CASE_ALL_SET");

  //List
  s = db.LPush("GP14_KEY1_PKEXPIRESCAN_CASE_ALL_LIST", {"GP14_PKEXPIRESCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP14_KEY2_PKEXPIRESCAN_CASE_ALL_LIST", {"GP14_PKEXPIRESCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP14_KEY3_PKEXPIRESCAN_CASE_ALL_LIST", {"GP14_PKEXPIRESCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);
  delete_keys.push_back("GP14_KEY1_PKEXPIRESCAN_CASE_ALL_LIST");
  delete_keys.push_back("GP14_KEY2_PKEXPIRESCAN_CASE_ALL_LIST");
  delete_keys.push_back("GP14_KEY3_PKEXPIRESCAN_CASE_ALL_LIST");

  //ZSet
  s = db.ZAdd("GP14_KEY1_PKEXPIRESCAN_CASE_ALL_ZSET", {{1, "GP14_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP14_KEY2_PKEXPIRESCAN_CASE_ALL_ZSET", {{1, "GP14_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP14_KEY3_PKEXPIRESCAN_CASE_ALL_ZSET", {{1, "GP14_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.push_back("GP14_KEY1_PKEXPIRESCAN_CASE_ALL_ZSET");
  delete_keys.push_back("GP14_KEY2_PKEXPIRESCAN_CASE_ALL_ZSET");
  delete_keys.push_back("GP14_KEY3_PKEXPIRESCAN_CASE_ALL_ZSET");

  ASSERT_TRUE(set_timeout(&db, "GP14_KEY1_PKEXPIRESCAN_CASE_ALL_STRING", 15));
  ASSERT_TRUE(set_timeout(&db, "GP14_KEY2_PKEXPIRESCAN_CASE_ALL_STRING", 5));
  ASSERT_TRUE(set_timeout(&db, "GP14_KEY3_PKEXPIRESCAN_CASE_ALL_STRING", 25));

  ASSERT_TRUE(set_timeout(&db, "GP14_KEY1_PKEXPIRESCAN_CASE_ALL_HASH", 15));
  ASSERT_TRUE(set_timeout(&db, "GP14_KEY2_PKEXPIRESCAN_CASE_ALL_HASH", 5));
  ASSERT_TRUE(set_timeout(&db, "GP14_KEY3_PKEXPIRESCAN_CASE_ALL_HASH", 25));

  ASSERT_TRUE(set_timeout(&db, "GP14_KEY1_PKEXPIRESCAN_CASE_ALL_SET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP14_KEY2_PKEXPIRESCAN_CASE_ALL_SET", 5));
  ASSERT_TRUE(set_timeout(&db, "GP14_KEY3_PKEXPIRESCAN_CASE_ALL_SET", 25));

  ASSERT_TRUE(set_timeout(&db, "GP14_KEY1_PKEXPIRESCAN_CASE_ALL_LIST", 15));
  ASSERT_TRUE(set_timeout(&db, "GP14_KEY2_PKEXPIRESCAN_CASE_ALL_LIST", 5));
  ASSERT_TRUE(set_timeout(&db, "GP14_KEY3_PKEXPIRESCAN_CASE_ALL_LIST", 25));

  ASSERT_TRUE(set_timeout(&db, "GP14_KEY1_PKEXPIRESCAN_CASE_ALL_ZSET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP14_KEY2_PKEXPIRESCAN_CASE_ALL_ZSET", 5));
  ASSERT_TRUE(set_timeout(&db, "GP14_KEY3_PKEXPIRESCAN_CASE_ALL_ZSET", 25));

  cursor = 0;
  keys.clear();
  total_keys.clear();
  do {
    next_cursor = db.PKExpireScan(DataType::kAll, cursor, 10, 20, 5, &keys);
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  } while (cursor);
  ASSERT_EQ(total_keys.size(), 5);
  ASSERT_EQ(total_keys[0], "GP14_KEY1_PKEXPIRESCAN_CASE_ALL_STRING");
  ASSERT_EQ(total_keys[1], "GP14_KEY1_PKEXPIRESCAN_CASE_ALL_HASH");
  ASSERT_EQ(total_keys[2], "GP14_KEY1_PKEXPIRESCAN_CASE_ALL_SET");
  ASSERT_EQ(total_keys[3], "GP14_KEY1_PKEXPIRESCAN_CASE_ALL_LIST");
  ASSERT_EQ(total_keys[4], "GP14_KEY1_PKEXPIRESCAN_CASE_ALL_ZSET");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 15 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP15_KEY1_PKEXPIRESCAN_CASE_ALL_STRING", "GP15_PKEXPIRESCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP15_KEY2_PKEXPIRESCAN_CASE_ALL_STRING", "GP15_PKEXPIRESCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP15_KEY3_PKEXPIRESCAN_CASE_ALL_STRING", "GP15_PKEXPIRESCAN_CASE_ALL_STRING_VALUE3");
  delete_keys.push_back("GP15_KEY1_PKEXPIRESCAN_CASE_ALL_STRING");
  delete_keys.push_back("GP15_KEY2_PKEXPIRESCAN_CASE_ALL_STRING");
  delete_keys.push_back("GP15_KEY3_PKEXPIRESCAN_CASE_ALL_STRING");

  //Hash
  s = db.HSet("GP15_KEY1_PKEXPIRESCAN_CASE_ALL_HASH", "GP15_PKEXPIRESCAN_CASE_ALL_HASH_FIELD1", "GP15_PKEXPIRESCAN_CASE_ALL_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP15_KEY2_PKEXPIRESCAN_CASE_ALL_HASH", "GP15_PKEXPIRESCAN_CASE_ALL_HASH_FIELD2", "GP15_PKEXPIRESCAN_CASE_ALL_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP15_KEY3_PKEXPIRESCAN_CASE_ALL_HASH", "GP15_PKEXPIRESCAN_CASE_ALL_HASH_FIELD3", "GP15_PKEXPIRESCAN_CASE_ALL_HASH_VALUE3", &int32_ret);
  delete_keys.push_back("GP15_KEY1_PKEXPIRESCAN_CASE_ALL_HASH");
  delete_keys.push_back("GP15_KEY2_PKEXPIRESCAN_CASE_ALL_HASH");
  delete_keys.push_back("GP15_KEY3_PKEXPIRESCAN_CASE_ALL_HASH");

  //Set
  s = db.SAdd("GP15_KEY1_PKEXPIRESCAN_CASE_ALL_SET", {"GP15_PKEXPIRESCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP15_KEY2_PKEXPIRESCAN_CASE_ALL_SET", {"GP15_PKEXPIRESCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP15_KEY3_PKEXPIRESCAN_CASE_ALL_SET", {"GP15_PKEXPIRESCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);
  delete_keys.push_back("GP15_KEY1_PKEXPIRESCAN_CASE_ALL_SET");
  delete_keys.push_back("GP15_KEY2_PKEXPIRESCAN_CASE_ALL_SET");
  delete_keys.push_back("GP15_KEY3_PKEXPIRESCAN_CASE_ALL_SET");

  //List
  s = db.LPush("GP15_KEY1_PKEXPIRESCAN_CASE_ALL_LIST", {"GP15_PKEXPIRESCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP15_KEY2_PKEXPIRESCAN_CASE_ALL_LIST", {"GP15_PKEXPIRESCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP15_KEY3_PKEXPIRESCAN_CASE_ALL_LIST", {"GP15_PKEXPIRESCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);
  delete_keys.push_back("GP15_KEY1_PKEXPIRESCAN_CASE_ALL_LIST");
  delete_keys.push_back("GP15_KEY2_PKEXPIRESCAN_CASE_ALL_LIST");
  delete_keys.push_back("GP15_KEY3_PKEXPIRESCAN_CASE_ALL_LIST");

  //ZSet
  s = db.ZAdd("GP15_KEY1_PKEXPIRESCAN_CASE_ALL_ZSET", {{1, "GP15_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP15_KEY2_PKEXPIRESCAN_CASE_ALL_ZSET", {{1, "GP15_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP15_KEY3_PKEXPIRESCAN_CASE_ALL_ZSET", {{1, "GP15_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.push_back("GP15_KEY1_PKEXPIRESCAN_CASE_ALL_ZSET");
  delete_keys.push_back("GP15_KEY2_PKEXPIRESCAN_CASE_ALL_ZSET");
  delete_keys.push_back("GP15_KEY3_PKEXPIRESCAN_CASE_ALL_ZSET");

  ASSERT_TRUE(set_timeout(&db, "GP15_KEY1_PKEXPIRESCAN_CASE_ALL_STRING", 5));
  ASSERT_TRUE(set_timeout(&db, "GP15_KEY2_PKEXPIRESCAN_CASE_ALL_STRING", 15));
  ASSERT_TRUE(set_timeout(&db, "GP15_KEY3_PKEXPIRESCAN_CASE_ALL_STRING", 25));

  ASSERT_TRUE(set_timeout(&db, "GP15_KEY1_PKEXPIRESCAN_CASE_ALL_HASH", 5));
  ASSERT_TRUE(set_timeout(&db, "GP15_KEY2_PKEXPIRESCAN_CASE_ALL_HASH", 15));
  ASSERT_TRUE(set_timeout(&db, "GP15_KEY3_PKEXPIRESCAN_CASE_ALL_HASH", 25));

  ASSERT_TRUE(set_timeout(&db, "GP15_KEY1_PKEXPIRESCAN_CASE_ALL_SET", 5));
  ASSERT_TRUE(set_timeout(&db, "GP15_KEY2_PKEXPIRESCAN_CASE_ALL_SET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP15_KEY3_PKEXPIRESCAN_CASE_ALL_SET", 25));

  ASSERT_TRUE(set_timeout(&db, "GP15_KEY1_PKEXPIRESCAN_CASE_ALL_LIST", 5));
  ASSERT_TRUE(set_timeout(&db, "GP15_KEY2_PKEXPIRESCAN_CASE_ALL_LIST", 15));
  ASSERT_TRUE(set_timeout(&db, "GP15_KEY3_PKEXPIRESCAN_CASE_ALL_LIST", 25));

  ASSERT_TRUE(set_timeout(&db, "GP15_KEY1_PKEXPIRESCAN_CASE_ALL_ZSET", 5));
  ASSERT_TRUE(set_timeout(&db, "GP15_KEY2_PKEXPIRESCAN_CASE_ALL_ZSET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP15_KEY3_PKEXPIRESCAN_CASE_ALL_ZSET", 25));

  cursor = 0;
  keys.clear();
  total_keys.clear();
  do {
    next_cursor = db.PKExpireScan(DataType::kAll, cursor, 10, 20, 5, &keys);
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  } while (cursor);
  ASSERT_EQ(total_keys.size(), 5);
  ASSERT_EQ(total_keys[0], "GP15_KEY2_PKEXPIRESCAN_CASE_ALL_STRING");
  ASSERT_EQ(total_keys[1], "GP15_KEY2_PKEXPIRESCAN_CASE_ALL_HASH");
  ASSERT_EQ(total_keys[2], "GP15_KEY2_PKEXPIRESCAN_CASE_ALL_SET");
  ASSERT_EQ(total_keys[3], "GP15_KEY2_PKEXPIRESCAN_CASE_ALL_LIST");
  ASSERT_EQ(total_keys[4], "GP15_KEY2_PKEXPIRESCAN_CASE_ALL_ZSET");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 16 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP16_KEY1_PKEXPIRESCAN_CASE_ALL_STRING", "GP16_PKEXPIRESCAN_CASE_ALL_STRING_VALUE1");
  s = db.Set("GP16_KEY2_PKEXPIRESCAN_CASE_ALL_STRING", "GP16_PKEXPIRESCAN_CASE_ALL_STRING_VALUE2");
  s = db.Set("GP16_KEY3_PKEXPIRESCAN_CASE_ALL_STRING", "GP16_PKEXPIRESCAN_CASE_ALL_STRING_VALUE3");
  delete_keys.push_back("GP16_KEY1_PKEXPIRESCAN_CASE_ALL_STRING");
  delete_keys.push_back("GP16_KEY2_PKEXPIRESCAN_CASE_ALL_STRING");
  delete_keys.push_back("GP16_KEY3_PKEXPIRESCAN_CASE_ALL_STRING");

  //Hash
  s = db.HSet("GP16_KEY1_PKEXPIRESCAN_CASE_ALL_HASH", "GP16_PKEXPIRESCAN_CASE_ALL_HASH_FIELD1", "GP16_PKEXPIRESCAN_CASE_ALL_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP16_KEY2_PKEXPIRESCAN_CASE_ALL_HASH", "GP16_PKEXPIRESCAN_CASE_ALL_HASH_FIELD2", "GP16_PKEXPIRESCAN_CASE_ALL_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP16_KEY3_PKEXPIRESCAN_CASE_ALL_HASH", "GP16_PKEXPIRESCAN_CASE_ALL_HASH_FIELD3", "GP16_PKEXPIRESCAN_CASE_ALL_HASH_VALUE3", &int32_ret);
  delete_keys.push_back("GP16_KEY1_PKEXPIRESCAN_CASE_ALL_HASH");
  delete_keys.push_back("GP16_KEY2_PKEXPIRESCAN_CASE_ALL_HASH");
  delete_keys.push_back("GP16_KEY3_PKEXPIRESCAN_CASE_ALL_HASH");

  //Set
  s = db.SAdd("GP16_KEY1_PKEXPIRESCAN_CASE_ALL_SET", {"GP16_PKEXPIRESCAN_CASE_ALL_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP16_KEY2_PKEXPIRESCAN_CASE_ALL_SET", {"GP16_PKEXPIRESCAN_CASE_ALL_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP16_KEY3_PKEXPIRESCAN_CASE_ALL_SET", {"GP16_PKEXPIRESCAN_CASE_ALL_SET_MEMBER3"}, &int32_ret);
  delete_keys.push_back("GP16_KEY1_PKEXPIRESCAN_CASE_ALL_SET");
  delete_keys.push_back("GP16_KEY2_PKEXPIRESCAN_CASE_ALL_SET");
  delete_keys.push_back("GP16_KEY3_PKEXPIRESCAN_CASE_ALL_SET");

  //List
  s = db.LPush("GP16_KEY1_PKEXPIRESCAN_CASE_ALL_LIST", {"GP16_PKEXPIRESCAN_CASE_ALL_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP16_KEY2_PKEXPIRESCAN_CASE_ALL_LIST", {"GP16_PKEXPIRESCAN_CASE_ALL_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP16_KEY3_PKEXPIRESCAN_CASE_ALL_LIST", {"GP16_PKEXPIRESCAN_CASE_ALL_LIST_NODE3"}, &uint64_ret);
  delete_keys.push_back("GP16_KEY1_PKEXPIRESCAN_CASE_ALL_LIST");
  delete_keys.push_back("GP16_KEY2_PKEXPIRESCAN_CASE_ALL_LIST");
  delete_keys.push_back("GP16_KEY3_PKEXPIRESCAN_CASE_ALL_LIST");

  //ZSet
  s = db.ZAdd("GP16_KEY1_PKEXPIRESCAN_CASE_ALL_ZSET", {{1, "GP16_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP16_KEY2_PKEXPIRESCAN_CASE_ALL_ZSET", {{1, "GP16_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP16_KEY3_PKEXPIRESCAN_CASE_ALL_ZSET", {{1, "GP16_PKEXPIRESCAN_CASE_ALL_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.push_back("GP16_KEY1_PKEXPIRESCAN_CASE_ALL_ZSET");
  delete_keys.push_back("GP16_KEY2_PKEXPIRESCAN_CASE_ALL_ZSET");
  delete_keys.push_back("GP16_KEY3_PKEXPIRESCAN_CASE_ALL_ZSET");

  ASSERT_TRUE(set_timeout(&db, "GP16_KEY1_PKEXPIRESCAN_CASE_ALL_STRING", 5));
  ASSERT_TRUE(set_timeout(&db, "GP16_KEY2_PKEXPIRESCAN_CASE_ALL_STRING", 25));
  ASSERT_TRUE(set_timeout(&db, "GP16_KEY3_PKEXPIRESCAN_CASE_ALL_STRING", 15));

  ASSERT_TRUE(set_timeout(&db, "GP16_KEY1_PKEXPIRESCAN_CASE_ALL_HASH", 5));
  ASSERT_TRUE(set_timeout(&db, "GP16_KEY2_PKEXPIRESCAN_CASE_ALL_HASH", 25));
  ASSERT_TRUE(set_timeout(&db, "GP16_KEY3_PKEXPIRESCAN_CASE_ALL_HASH", 15));

  ASSERT_TRUE(set_timeout(&db, "GP16_KEY1_PKEXPIRESCAN_CASE_ALL_SET", 5));
  ASSERT_TRUE(set_timeout(&db, "GP16_KEY2_PKEXPIRESCAN_CASE_ALL_SET", 25));
  ASSERT_TRUE(set_timeout(&db, "GP16_KEY3_PKEXPIRESCAN_CASE_ALL_SET", 15));

  ASSERT_TRUE(set_timeout(&db, "GP16_KEY1_PKEXPIRESCAN_CASE_ALL_LIST", 5));
  ASSERT_TRUE(set_timeout(&db, "GP16_KEY2_PKEXPIRESCAN_CASE_ALL_LIST", 25));
  ASSERT_TRUE(set_timeout(&db, "GP16_KEY3_PKEXPIRESCAN_CASE_ALL_LIST", 15));

  ASSERT_TRUE(set_timeout(&db, "GP16_KEY1_PKEXPIRESCAN_CASE_ALL_ZSET", 5));
  ASSERT_TRUE(set_timeout(&db, "GP16_KEY2_PKEXPIRESCAN_CASE_ALL_ZSET", 25));
  ASSERT_TRUE(set_timeout(&db, "GP16_KEY3_PKEXPIRESCAN_CASE_ALL_ZSET", 15));

  cursor = 0;
  keys.clear();
  total_keys.clear();
  do {
    next_cursor = db.PKExpireScan(DataType::kAll, cursor, 10, 20, 5, &keys);
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  } while (cursor);
  ASSERT_EQ(total_keys.size(), 5);
  ASSERT_EQ(total_keys[0], "GP16_KEY3_PKEXPIRESCAN_CASE_ALL_STRING");
  ASSERT_EQ(total_keys[1], "GP16_KEY3_PKEXPIRESCAN_CASE_ALL_HASH");
  ASSERT_EQ(total_keys[2], "GP16_KEY3_PKEXPIRESCAN_CASE_ALL_SET");
  ASSERT_EQ(total_keys[3], "GP16_KEY3_PKEXPIRESCAN_CASE_ALL_LIST");
  ASSERT_EQ(total_keys[4], "GP16_KEY3_PKEXPIRESCAN_CASE_ALL_ZSET");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);
  sleep(2);
  db.Compact(DataType::kAll, true);
}

TEST_F(KeysTest, PKExpireScanCaseSingleTest) {

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
  //String
  s = db.Set("GP1_KEY1_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP1_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE1");
  s = db.Set("GP1_KEY2_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP1_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE2");
  s = db.Set("GP1_KEY3_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP1_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE3");
  s = db.Set("GP1_KEY4_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP1_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE4");
  s = db.Set("GP1_KEY5_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP1_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE5");
  s = db.Set("GP1_KEY6_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP1_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE6");
  delete_keys.push_back("GP1_KEY1_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP1_KEY2_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP1_KEY3_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP1_KEY4_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP1_KEY5_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP1_KEY6_PKEXPIRESCAN_CASE_SINGLE_STRING");

  //Hash
  s = db.HSet("GP1_KEY1_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP1_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD1", "GP1_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP1_KEY2_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP1_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD2", "GP1_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP1_KEY3_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP1_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD3", "GP1_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE3", &int32_ret);
  s = db.HSet("GP1_KEY4_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP1_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD4", "GP1_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE4", &int32_ret);
  s = db.HSet("GP1_KEY5_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP1_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD5", "GP1_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE5", &int32_ret);
  s = db.HSet("GP1_KEY6_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP1_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD6", "GP1_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE6", &int32_ret);
  delete_keys.push_back("GP1_KEY1_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP1_KEY2_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP1_KEY3_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP1_KEY4_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP1_KEY5_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP1_KEY6_PKEXPIRESCAN_CASE_SINGLE_HASH");

  //Set
  s = db.SAdd("GP1_KEY1_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP1_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP1_KEY2_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP1_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP1_KEY3_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP1_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER3"}, &int32_ret);
  s = db.SAdd("GP1_KEY4_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP1_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER4"}, &int32_ret);
  s = db.SAdd("GP1_KEY5_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP1_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER5"}, &int32_ret);
  s = db.SAdd("GP1_KEY6_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP1_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER6"}, &int32_ret);
  delete_keys.push_back("GP1_KEY1_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP1_KEY2_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP1_KEY3_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP1_KEY4_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP1_KEY5_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP1_KEY6_PKEXPIRESCAN_CASE_SINGLE_SET");

  //List
  s = db.LPush("GP1_KEY1_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP1_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP1_KEY2_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP1_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP1_KEY3_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP1_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE3"}, &uint64_ret);
  s = db.LPush("GP1_KEY4_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP1_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE4"}, &uint64_ret);
  s = db.LPush("GP1_KEY5_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP1_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE5"}, &uint64_ret);
  s = db.LPush("GP1_KEY6_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP1_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE6"}, &uint64_ret);
  delete_keys.push_back("GP1_KEY1_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP1_KEY2_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP1_KEY3_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP1_KEY4_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP1_KEY5_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP1_KEY6_PKEXPIRESCAN_CASE_SINGLE_LIST");

  //ZSet
  s = db.ZAdd("GP1_KEY1_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP1_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP1_KEY2_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP1_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP1_KEY3_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP1_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER3"}}, &int32_ret);
  s = db.ZAdd("GP1_KEY4_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP1_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER4"}}, &int32_ret);
  s = db.ZAdd("GP1_KEY5_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP1_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER5"}}, &int32_ret);
  s = db.ZAdd("GP1_KEY6_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP1_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER6"}}, &int32_ret);
  delete_keys.push_back("GP1_KEY1_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP1_KEY2_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP1_KEY3_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP1_KEY4_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP1_KEY5_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP1_KEY6_PKEXPIRESCAN_CASE_SINGLE_ZSET");

  ASSERT_TRUE(set_timeout(&db, "GP1_KEY1_PKEXPIRESCAN_CASE_SINGLE_STRING", 15));
  ASSERT_TRUE(set_timeout(&db, "GP1_KEY2_PKEXPIRESCAN_CASE_SINGLE_STRING", 15));
  ASSERT_TRUE(set_timeout(&db, "GP1_KEY3_PKEXPIRESCAN_CASE_SINGLE_STRING", 15));
  ASSERT_TRUE(set_timeout(&db, "GP1_KEY4_PKEXPIRESCAN_CASE_SINGLE_STRING", 15));
  ASSERT_TRUE(set_timeout(&db, "GP1_KEY5_PKEXPIRESCAN_CASE_SINGLE_STRING", 15));
  ASSERT_TRUE(set_timeout(&db, "GP1_KEY6_PKEXPIRESCAN_CASE_SINGLE_STRING", 15));

  keys.clear();
  cursor = 0;
  cursor = db.PKExpireScan(DataType::kStrings, cursor, 10, 20, 2, &keys);
  ASSERT_EQ(cursor, 2);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP1_KEY1_PKEXPIRESCAN_CASE_SINGLE_STRING");
  ASSERT_EQ(keys[1], "GP1_KEY2_PKEXPIRESCAN_CASE_SINGLE_STRING");

  keys.clear();
  cursor = db.PKExpireScan(DataType::kStrings, cursor, 10, 20, 2, &keys);
  ASSERT_EQ(cursor, 4);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP1_KEY3_PKEXPIRESCAN_CASE_SINGLE_STRING");
  ASSERT_EQ(keys[1], "GP1_KEY4_PKEXPIRESCAN_CASE_SINGLE_STRING");

  keys.clear();
  cursor = db.PKExpireScan(DataType::kStrings, cursor, 10, 20, 2, &keys);
  ASSERT_EQ(cursor, 0);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP1_KEY5_PKEXPIRESCAN_CASE_SINGLE_STRING");
  ASSERT_EQ(keys[1], "GP1_KEY6_PKEXPIRESCAN_CASE_SINGLE_STRING");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 30);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 2 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP2_KEY1_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP2_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE1");
  s = db.Set("GP2_KEY2_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP2_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE2");
  s = db.Set("GP2_KEY3_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP2_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE3");
  s = db.Set("GP2_KEY4_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP2_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE4");
  s = db.Set("GP2_KEY5_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP2_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE5");
  s = db.Set("GP2_KEY6_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP2_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE6");
  delete_keys.push_back("GP2_KEY1_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP2_KEY2_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP2_KEY3_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP2_KEY4_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP2_KEY5_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP2_KEY6_PKEXPIRESCAN_CASE_SINGLE_STRING");

  //Hash
  s = db.HSet("GP2_KEY1_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP2_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD1", "GP2_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP2_KEY2_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP2_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD2", "GP2_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP2_KEY3_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP2_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD3", "GP2_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE3", &int32_ret);
  s = db.HSet("GP2_KEY4_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP2_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD4", "GP2_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE4", &int32_ret);
  s = db.HSet("GP2_KEY5_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP2_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD5", "GP2_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE5", &int32_ret);
  s = db.HSet("GP2_KEY6_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP2_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD6", "GP2_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE6", &int32_ret);
  delete_keys.push_back("GP2_KEY1_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP2_KEY2_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP2_KEY3_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP2_KEY4_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP2_KEY5_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP2_KEY6_PKEXPIRESCAN_CASE_SINGLE_HASH");

  //Set
  s = db.SAdd("GP2_KEY1_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP2_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP2_KEY2_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP2_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP2_KEY3_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP2_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER3"}, &int32_ret);
  s = db.SAdd("GP2_KEY4_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP2_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER4"}, &int32_ret);
  s = db.SAdd("GP2_KEY5_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP2_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER5"}, &int32_ret);
  s = db.SAdd("GP2_KEY6_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP2_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER6"}, &int32_ret);
  delete_keys.push_back("GP2_KEY1_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP2_KEY2_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP2_KEY3_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP2_KEY4_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP2_KEY5_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP2_KEY6_PKEXPIRESCAN_CASE_SINGLE_SET");

  //List
  s = db.LPush("GP2_KEY1_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP2_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP2_KEY2_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP2_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP2_KEY3_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP2_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE3"}, &uint64_ret);
  s = db.LPush("GP2_KEY4_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP2_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE4"}, &uint64_ret);
  s = db.LPush("GP2_KEY5_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP2_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE5"}, &uint64_ret);
  s = db.LPush("GP2_KEY6_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP2_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE6"}, &uint64_ret);
  delete_keys.push_back("GP2_KEY1_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP2_KEY2_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP2_KEY3_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP2_KEY4_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP2_KEY5_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP2_KEY6_PKEXPIRESCAN_CASE_SINGLE_LIST");

  //ZSet
  s = db.ZAdd("GP2_KEY1_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP2_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP2_KEY2_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP2_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP2_KEY3_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP2_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER3"}}, &int32_ret);
  s = db.ZAdd("GP2_KEY4_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP2_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER4"}}, &int32_ret);
  s = db.ZAdd("GP2_KEY5_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP2_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER5"}}, &int32_ret);
  s = db.ZAdd("GP2_KEY6_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP2_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER6"}}, &int32_ret);
  delete_keys.push_back("GP2_KEY1_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP2_KEY2_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP2_KEY3_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP2_KEY4_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP2_KEY5_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP2_KEY6_PKEXPIRESCAN_CASE_SINGLE_ZSET");

  ASSERT_TRUE(set_timeout(&db, "GP2_KEY1_PKEXPIRESCAN_CASE_SINGLE_STRING", 15));
  ASSERT_TRUE(set_timeout(&db, "GP2_KEY2_PKEXPIRESCAN_CASE_SINGLE_STRING", 15));
  ASSERT_TRUE(set_timeout(&db, "GP2_KEY3_PKEXPIRESCAN_CASE_SINGLE_STRING", 15));
  ASSERT_TRUE(set_timeout(&db, "GP2_KEY4_PKEXPIRESCAN_CASE_SINGLE_STRING", 15));
  ASSERT_TRUE(set_timeout(&db, "GP2_KEY5_PKEXPIRESCAN_CASE_SINGLE_STRING", 15));
  ASSERT_TRUE(set_timeout(&db, "GP2_KEY6_PKEXPIRESCAN_CASE_SINGLE_STRING", 15));

  keys.clear();
  cursor = 0;
  cursor = db.PKExpireScan(DataType::kStrings, cursor, 10, 20, 4, &keys);
  ASSERT_EQ(cursor, 4);
  ASSERT_EQ(keys.size(), 4);
  ASSERT_EQ(keys[0], "GP2_KEY1_PKEXPIRESCAN_CASE_SINGLE_STRING");
  ASSERT_EQ(keys[1], "GP2_KEY2_PKEXPIRESCAN_CASE_SINGLE_STRING");
  ASSERT_EQ(keys[2], "GP2_KEY3_PKEXPIRESCAN_CASE_SINGLE_STRING");
  ASSERT_EQ(keys[3], "GP2_KEY4_PKEXPIRESCAN_CASE_SINGLE_STRING");

  keys.clear();
  cursor = db.PKExpireScan(DataType::kStrings, cursor, 10, 20, 4, &keys);
  ASSERT_EQ(cursor, 0);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP2_KEY5_PKEXPIRESCAN_CASE_SINGLE_STRING");
  ASSERT_EQ(keys[1], "GP2_KEY6_PKEXPIRESCAN_CASE_SINGLE_STRING");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 30);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 3 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP3_KEY1_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP3_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE1");
  s = db.Set("GP3_KEY2_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP3_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE2");
  s = db.Set("GP3_KEY3_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP3_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE3");
  s = db.Set("GP3_KEY4_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP3_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE4");
  s = db.Set("GP3_KEY5_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP3_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE5");
  s = db.Set("GP3_KEY6_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP3_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE6");
  delete_keys.push_back("GP3_KEY1_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP3_KEY2_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP3_KEY3_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP3_KEY4_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP3_KEY5_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP3_KEY6_PKEXPIRESCAN_CASE_SINGLE_STRING");

  //Hash
  s = db.HSet("GP3_KEY1_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP3_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD1", "GP3_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP3_KEY2_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP3_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD2", "GP3_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP3_KEY3_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP3_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD3", "GP3_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE3", &int32_ret);
  s = db.HSet("GP3_KEY4_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP3_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD4", "GP3_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE4", &int32_ret);
  s = db.HSet("GP3_KEY5_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP3_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD5", "GP3_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE5", &int32_ret);
  s = db.HSet("GP3_KEY6_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP3_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD6", "GP3_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE6", &int32_ret);
  delete_keys.push_back("GP3_KEY1_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP3_KEY2_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP3_KEY3_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP3_KEY4_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP3_KEY5_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP3_KEY6_PKEXPIRESCAN_CASE_SINGLE_HASH");

  //Set
  s = db.SAdd("GP3_KEY1_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP3_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP3_KEY2_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP3_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP3_KEY3_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP3_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER3"}, &int32_ret);
  s = db.SAdd("GP3_KEY4_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP3_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER4"}, &int32_ret);
  s = db.SAdd("GP3_KEY5_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP3_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER5"}, &int32_ret);
  s = db.SAdd("GP3_KEY6_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP3_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER6"}, &int32_ret);
  delete_keys.push_back("GP3_KEY1_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP3_KEY2_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP3_KEY3_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP3_KEY4_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP3_KEY5_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP3_KEY6_PKEXPIRESCAN_CASE_SINGLE_SET");

  //List
  s = db.LPush("GP3_KEY1_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP3_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP3_KEY2_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP3_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP3_KEY3_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP3_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE3"}, &uint64_ret);
  s = db.LPush("GP3_KEY4_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP3_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE4"}, &uint64_ret);
  s = db.LPush("GP3_KEY5_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP3_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE5"}, &uint64_ret);
  s = db.LPush("GP3_KEY6_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP3_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE6"}, &uint64_ret);
  delete_keys.push_back("GP3_KEY1_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP3_KEY2_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP3_KEY3_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP3_KEY4_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP3_KEY5_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP3_KEY6_PKEXPIRESCAN_CASE_SINGLE_LIST");

  //ZSet
  s = db.ZAdd("GP3_KEY1_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP3_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP3_KEY2_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP3_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP3_KEY3_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP3_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER3"}}, &int32_ret);
  s = db.ZAdd("GP3_KEY4_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP3_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER4"}}, &int32_ret);
  s = db.ZAdd("GP3_KEY5_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP3_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER5"}}, &int32_ret);
  s = db.ZAdd("GP3_KEY6_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP3_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER6"}}, &int32_ret);
  delete_keys.push_back("GP3_KEY1_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP3_KEY2_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP3_KEY3_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP3_KEY4_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP3_KEY5_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP3_KEY6_PKEXPIRESCAN_CASE_SINGLE_ZSET");

  ASSERT_TRUE(set_timeout(&db, "GP3_KEY1_PKEXPIRESCAN_CASE_SINGLE_STRING", 15));
  ASSERT_TRUE(set_timeout(&db, "GP3_KEY2_PKEXPIRESCAN_CASE_SINGLE_STRING", 15));
  ASSERT_TRUE(set_timeout(&db, "GP3_KEY3_PKEXPIRESCAN_CASE_SINGLE_STRING", 15));
  ASSERT_TRUE(set_timeout(&db, "GP3_KEY4_PKEXPIRESCAN_CASE_SINGLE_STRING", 15));
  ASSERT_TRUE(set_timeout(&db, "GP3_KEY5_PKEXPIRESCAN_CASE_SINGLE_STRING", 15));
  ASSERT_TRUE(set_timeout(&db, "GP3_KEY6_PKEXPIRESCAN_CASE_SINGLE_STRING", 15));

  keys.clear();
  cursor = 0;
  cursor = db.PKExpireScan(DataType::kStrings, cursor, 10, 20, 6, &keys);
  ASSERT_EQ(cursor, 0);
  ASSERT_EQ(keys.size(), 6);
  ASSERT_EQ(keys[0], "GP3_KEY1_PKEXPIRESCAN_CASE_SINGLE_STRING");
  ASSERT_EQ(keys[1], "GP3_KEY2_PKEXPIRESCAN_CASE_SINGLE_STRING");
  ASSERT_EQ(keys[2], "GP3_KEY3_PKEXPIRESCAN_CASE_SINGLE_STRING");
  ASSERT_EQ(keys[3], "GP3_KEY4_PKEXPIRESCAN_CASE_SINGLE_STRING");
  ASSERT_EQ(keys[4], "GP3_KEY5_PKEXPIRESCAN_CASE_SINGLE_STRING");
  ASSERT_EQ(keys[5], "GP3_KEY6_PKEXPIRESCAN_CASE_SINGLE_STRING");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 30);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 4 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP4_KEY1_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP4_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE1");
  s = db.Set("GP4_KEY2_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP4_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE2");
  s = db.Set("GP4_KEY3_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP4_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE3");
  s = db.Set("GP4_KEY4_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP4_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE4");
  s = db.Set("GP4_KEY5_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP4_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE5");
  s = db.Set("GP4_KEY6_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP4_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE6");
  delete_keys.push_back("GP4_KEY1_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP4_KEY2_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP4_KEY3_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP4_KEY4_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP4_KEY5_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP4_KEY6_PKEXPIRESCAN_CASE_SINGLE_STRING");

  //Hash
  s = db.HSet("GP4_KEY1_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP4_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD1", "GP4_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP4_KEY2_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP4_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD2", "GP4_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP4_KEY3_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP4_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD3", "GP4_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE3", &int32_ret);
  s = db.HSet("GP4_KEY4_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP4_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD4", "GP4_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE4", &int32_ret);
  s = db.HSet("GP4_KEY5_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP4_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD5", "GP4_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE5", &int32_ret);
  s = db.HSet("GP4_KEY6_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP4_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD6", "GP4_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE6", &int32_ret);
  delete_keys.push_back("GP4_KEY1_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP4_KEY2_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP4_KEY3_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP4_KEY4_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP4_KEY5_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP4_KEY6_PKEXPIRESCAN_CASE_SINGLE_HASH");

  //Set
  s = db.SAdd("GP4_KEY1_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP4_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP4_KEY2_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP4_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP4_KEY3_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP4_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER3"}, &int32_ret);
  s = db.SAdd("GP4_KEY4_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP4_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER4"}, &int32_ret);
  s = db.SAdd("GP4_KEY5_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP4_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER5"}, &int32_ret);
  s = db.SAdd("GP4_KEY6_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP4_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER6"}, &int32_ret);
  delete_keys.push_back("GP4_KEY1_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP4_KEY2_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP4_KEY3_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP4_KEY4_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP4_KEY5_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP4_KEY6_PKEXPIRESCAN_CASE_SINGLE_SET");

  //List
  s = db.LPush("GP4_KEY1_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP4_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP4_KEY2_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP4_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP4_KEY3_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP4_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE3"}, &uint64_ret);
  s = db.LPush("GP4_KEY4_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP4_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE4"}, &uint64_ret);
  s = db.LPush("GP4_KEY5_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP4_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE5"}, &uint64_ret);
  s = db.LPush("GP4_KEY6_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP4_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE6"}, &uint64_ret);
  delete_keys.push_back("GP4_KEY1_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP4_KEY2_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP4_KEY3_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP4_KEY4_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP4_KEY5_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP4_KEY6_PKEXPIRESCAN_CASE_SINGLE_LIST");

  //ZSet
  s = db.ZAdd("GP4_KEY1_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP4_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP4_KEY2_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP4_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP4_KEY3_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP4_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER3"}}, &int32_ret);
  s = db.ZAdd("GP4_KEY4_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP4_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER4"}}, &int32_ret);
  s = db.ZAdd("GP4_KEY5_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP4_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER5"}}, &int32_ret);
  s = db.ZAdd("GP4_KEY6_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP4_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER6"}}, &int32_ret);
  delete_keys.push_back("GP4_KEY1_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP4_KEY2_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP4_KEY3_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP4_KEY4_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP4_KEY5_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP4_KEY6_PKEXPIRESCAN_CASE_SINGLE_ZSET");

  ASSERT_TRUE(set_timeout(&db, "GP4_KEY1_PKEXPIRESCAN_CASE_SINGLE_STRING", 15));
  ASSERT_TRUE(set_timeout(&db, "GP4_KEY2_PKEXPIRESCAN_CASE_SINGLE_STRING", 15));
  ASSERT_TRUE(set_timeout(&db, "GP4_KEY3_PKEXPIRESCAN_CASE_SINGLE_STRING", 15));
  ASSERT_TRUE(set_timeout(&db, "GP4_KEY4_PKEXPIRESCAN_CASE_SINGLE_STRING", 15));
  ASSERT_TRUE(set_timeout(&db, "GP4_KEY5_PKEXPIRESCAN_CASE_SINGLE_STRING", 15));
  ASSERT_TRUE(set_timeout(&db, "GP4_KEY6_PKEXPIRESCAN_CASE_SINGLE_STRING", 15));

  keys.clear();
  cursor = 0;
  cursor = db.PKExpireScan(DataType::kStrings, cursor, 10, 20, 10, &keys);
  ASSERT_EQ(cursor, 0);
  ASSERT_EQ(keys.size(), 6);
  ASSERT_EQ(keys[0], "GP4_KEY1_PKEXPIRESCAN_CASE_SINGLE_STRING");
  ASSERT_EQ(keys[1], "GP4_KEY2_PKEXPIRESCAN_CASE_SINGLE_STRING");
  ASSERT_EQ(keys[2], "GP4_KEY3_PKEXPIRESCAN_CASE_SINGLE_STRING");
  ASSERT_EQ(keys[3], "GP4_KEY4_PKEXPIRESCAN_CASE_SINGLE_STRING");
  ASSERT_EQ(keys[4], "GP4_KEY5_PKEXPIRESCAN_CASE_SINGLE_STRING");
  ASSERT_EQ(keys[5], "GP4_KEY6_PKEXPIRESCAN_CASE_SINGLE_STRING");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 30);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 5 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP5_KEY1_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP5_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE1");
  s = db.Set("GP5_KEY2_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP5_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE2");
  s = db.Set("GP5_KEY3_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP5_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE3");
  s = db.Set("GP5_KEY4_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP5_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE4");
  s = db.Set("GP5_KEY5_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP5_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE5");
  s = db.Set("GP5_KEY6_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP5_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE6");
  delete_keys.push_back("GP5_KEY1_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP5_KEY2_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP5_KEY3_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP5_KEY4_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP5_KEY5_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP5_KEY6_PKEXPIRESCAN_CASE_SINGLE_STRING");

  //Hash
  s = db.HSet("GP5_KEY1_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP5_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD1", "GP5_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP5_KEY2_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP5_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD2", "GP5_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP5_KEY3_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP5_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD3", "GP5_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE3", &int32_ret);
  s = db.HSet("GP5_KEY4_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP5_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD4", "GP5_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE4", &int32_ret);
  s = db.HSet("GP5_KEY5_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP5_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD5", "GP5_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE5", &int32_ret);
  s = db.HSet("GP5_KEY6_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP5_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD6", "GP5_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE6", &int32_ret);
  delete_keys.push_back("GP5_KEY1_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP5_KEY2_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP5_KEY3_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP5_KEY4_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP5_KEY5_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP5_KEY6_PKEXPIRESCAN_CASE_SINGLE_HASH");

  //Set
  s = db.SAdd("GP5_KEY1_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP5_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP5_KEY2_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP5_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP5_KEY3_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP5_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER3"}, &int32_ret);
  s = db.SAdd("GP5_KEY4_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP5_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER4"}, &int32_ret);
  s = db.SAdd("GP5_KEY5_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP5_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER5"}, &int32_ret);
  s = db.SAdd("GP5_KEY6_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP5_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER6"}, &int32_ret);
  delete_keys.push_back("GP5_KEY1_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP5_KEY2_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP5_KEY3_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP5_KEY4_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP5_KEY5_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP5_KEY6_PKEXPIRESCAN_CASE_SINGLE_SET");

  //List
  s = db.LPush("GP5_KEY1_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP5_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP5_KEY2_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP5_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP5_KEY3_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP5_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE3"}, &uint64_ret);
  s = db.LPush("GP5_KEY4_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP5_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE4"}, &uint64_ret);
  s = db.LPush("GP5_KEY5_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP5_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE5"}, &uint64_ret);
  s = db.LPush("GP5_KEY6_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP5_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE6"}, &uint64_ret);
  delete_keys.push_back("GP5_KEY1_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP5_KEY2_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP5_KEY3_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP5_KEY4_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP5_KEY5_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP5_KEY6_PKEXPIRESCAN_CASE_SINGLE_LIST");

  //ZSet
  s = db.ZAdd("GP5_KEY1_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP5_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP5_KEY2_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP5_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP5_KEY3_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP5_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER3"}}, &int32_ret);
  s = db.ZAdd("GP5_KEY4_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP5_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER4"}}, &int32_ret);
  s = db.ZAdd("GP5_KEY5_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP5_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER5"}}, &int32_ret);
  s = db.ZAdd("GP5_KEY6_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP5_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER6"}}, &int32_ret);
  delete_keys.push_back("GP5_KEY1_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP5_KEY2_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP5_KEY3_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP5_KEY4_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP5_KEY5_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP5_KEY6_PKEXPIRESCAN_CASE_SINGLE_ZSET");

  ASSERT_TRUE(set_timeout(&db, "GP5_KEY1_PKEXPIRESCAN_CASE_SINGLE_SET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP5_KEY2_PKEXPIRESCAN_CASE_SINGLE_SET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP5_KEY3_PKEXPIRESCAN_CASE_SINGLE_SET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP5_KEY4_PKEXPIRESCAN_CASE_SINGLE_SET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP5_KEY5_PKEXPIRESCAN_CASE_SINGLE_SET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP5_KEY6_PKEXPIRESCAN_CASE_SINGLE_SET", 15));

  keys.clear();
  cursor = 0;
  cursor = db.PKExpireScan(DataType::kSets, cursor, 10, 20, 2, &keys);
  ASSERT_EQ(cursor, 2);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP5_KEY1_PKEXPIRESCAN_CASE_SINGLE_SET");
  ASSERT_EQ(keys[1], "GP5_KEY2_PKEXPIRESCAN_CASE_SINGLE_SET");

  keys.clear();
  cursor = db.PKExpireScan(DataType::kSets, cursor, 10, 20, 2, &keys);
  ASSERT_EQ(cursor, 4);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP5_KEY3_PKEXPIRESCAN_CASE_SINGLE_SET");
  ASSERT_EQ(keys[1], "GP5_KEY4_PKEXPIRESCAN_CASE_SINGLE_SET");

  keys.clear();
  cursor = db.PKExpireScan(DataType::kSets, cursor, 10, 20, 2, &keys);
  ASSERT_EQ(cursor, 0);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP5_KEY5_PKEXPIRESCAN_CASE_SINGLE_SET");
  ASSERT_EQ(keys[1], "GP5_KEY6_PKEXPIRESCAN_CASE_SINGLE_SET");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 30);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 6 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP6_KEY1_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP6_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE1");
  s = db.Set("GP6_KEY2_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP6_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE2");
  s = db.Set("GP6_KEY3_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP6_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE3");
  s = db.Set("GP6_KEY4_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP6_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE4");
  s = db.Set("GP6_KEY5_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP6_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE5");
  s = db.Set("GP6_KEY6_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP6_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE6");
  delete_keys.push_back("GP6_KEY1_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP6_KEY2_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP6_KEY3_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP6_KEY4_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP6_KEY5_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP6_KEY6_PKEXPIRESCAN_CASE_SINGLE_STRING");

  //Hash
  s = db.HSet("GP6_KEY1_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP6_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD1", "GP6_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP6_KEY2_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP6_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD2", "GP6_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP6_KEY3_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP6_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD3", "GP6_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE3", &int32_ret);
  s = db.HSet("GP6_KEY4_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP6_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD4", "GP6_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE4", &int32_ret);
  s = db.HSet("GP6_KEY5_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP6_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD5", "GP6_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE5", &int32_ret);
  s = db.HSet("GP6_KEY6_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP6_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD6", "GP6_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE6", &int32_ret);
  delete_keys.push_back("GP6_KEY1_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP6_KEY2_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP6_KEY3_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP6_KEY4_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP6_KEY5_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP6_KEY6_PKEXPIRESCAN_CASE_SINGLE_HASH");

  //Set
  s = db.SAdd("GP6_KEY1_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP6_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP6_KEY2_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP6_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP6_KEY3_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP6_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER3"}, &int32_ret);
  s = db.SAdd("GP6_KEY4_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP6_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER4"}, &int32_ret);
  s = db.SAdd("GP6_KEY5_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP6_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER5"}, &int32_ret);
  s = db.SAdd("GP6_KEY6_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP6_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER6"}, &int32_ret);
  delete_keys.push_back("GP6_KEY1_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP6_KEY2_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP6_KEY3_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP6_KEY4_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP6_KEY5_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP6_KEY6_PKEXPIRESCAN_CASE_SINGLE_SET");

  //List
  s = db.LPush("GP6_KEY1_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP6_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP6_KEY2_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP6_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP6_KEY3_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP6_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE3"}, &uint64_ret);
  s = db.LPush("GP6_KEY4_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP6_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE4"}, &uint64_ret);
  s = db.LPush("GP6_KEY5_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP6_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE5"}, &uint64_ret);
  s = db.LPush("GP6_KEY6_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP6_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE6"}, &uint64_ret);
  delete_keys.push_back("GP6_KEY1_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP6_KEY2_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP6_KEY3_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP6_KEY4_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP6_KEY5_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP6_KEY6_PKEXPIRESCAN_CASE_SINGLE_LIST");

  //ZSet
  s = db.ZAdd("GP6_KEY1_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP6_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP6_KEY2_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP6_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP6_KEY3_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP6_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER3"}}, &int32_ret);
  s = db.ZAdd("GP6_KEY4_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP6_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER4"}}, &int32_ret);
  s = db.ZAdd("GP6_KEY5_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP6_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER5"}}, &int32_ret);
  s = db.ZAdd("GP6_KEY6_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP6_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER6"}}, &int32_ret);
  delete_keys.push_back("GP6_KEY1_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP6_KEY2_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP6_KEY3_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP6_KEY4_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP6_KEY5_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP6_KEY6_PKEXPIRESCAN_CASE_SINGLE_ZSET");

  ASSERT_TRUE(set_timeout(&db, "GP6_KEY1_PKEXPIRESCAN_CASE_SINGLE_SET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP6_KEY2_PKEXPIRESCAN_CASE_SINGLE_SET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP6_KEY3_PKEXPIRESCAN_CASE_SINGLE_SET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP6_KEY4_PKEXPIRESCAN_CASE_SINGLE_SET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP6_KEY5_PKEXPIRESCAN_CASE_SINGLE_SET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP6_KEY6_PKEXPIRESCAN_CASE_SINGLE_SET", 15));

  keys.clear();
  cursor = 0;
  cursor = db.PKExpireScan(DataType::kSets, cursor, 10, 20, 4, &keys);
  ASSERT_EQ(cursor, 4);
  ASSERT_EQ(keys.size(), 4);
  ASSERT_EQ(keys[0], "GP6_KEY1_PKEXPIRESCAN_CASE_SINGLE_SET");
  ASSERT_EQ(keys[1], "GP6_KEY2_PKEXPIRESCAN_CASE_SINGLE_SET");
  ASSERT_EQ(keys[2], "GP6_KEY3_PKEXPIRESCAN_CASE_SINGLE_SET");
  ASSERT_EQ(keys[3], "GP6_KEY4_PKEXPIRESCAN_CASE_SINGLE_SET");

  keys.clear();
  cursor = db.PKExpireScan(DataType::kSets, cursor, 10, 20, 4, &keys);
  ASSERT_EQ(cursor, 0);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP6_KEY5_PKEXPIRESCAN_CASE_SINGLE_SET");
  ASSERT_EQ(keys[1], "GP6_KEY6_PKEXPIRESCAN_CASE_SINGLE_SET");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 30);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 7 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP7_KEY1_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP7_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE1");
  s = db.Set("GP7_KEY2_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP7_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE2");
  s = db.Set("GP7_KEY3_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP7_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE3");
  s = db.Set("GP7_KEY4_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP7_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE4");
  s = db.Set("GP7_KEY5_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP7_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE5");
  s = db.Set("GP7_KEY6_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP7_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE6");
  delete_keys.push_back("GP7_KEY1_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP7_KEY2_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP7_KEY3_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP7_KEY4_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP7_KEY5_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP7_KEY6_PKEXPIRESCAN_CASE_SINGLE_STRING");

  //Hash
  s = db.HSet("GP7_KEY1_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP7_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD1", "GP7_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP7_KEY2_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP7_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD2", "GP7_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP7_KEY3_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP7_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD3", "GP7_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE3", &int32_ret);
  s = db.HSet("GP7_KEY4_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP7_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD4", "GP7_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE4", &int32_ret);
  s = db.HSet("GP7_KEY5_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP7_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD5", "GP7_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE5", &int32_ret);
  s = db.HSet("GP7_KEY6_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP7_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD6", "GP7_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE6", &int32_ret);
  delete_keys.push_back("GP7_KEY1_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP7_KEY2_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP7_KEY3_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP7_KEY4_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP7_KEY5_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP7_KEY6_PKEXPIRESCAN_CASE_SINGLE_HASH");

  //Set
  s = db.SAdd("GP7_KEY1_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP7_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP7_KEY2_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP7_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP7_KEY3_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP7_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER3"}, &int32_ret);
  s = db.SAdd("GP7_KEY4_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP7_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER4"}, &int32_ret);
  s = db.SAdd("GP7_KEY5_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP7_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER5"}, &int32_ret);
  s = db.SAdd("GP7_KEY6_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP7_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER6"}, &int32_ret);
  delete_keys.push_back("GP7_KEY1_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP7_KEY2_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP7_KEY3_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP7_KEY4_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP7_KEY5_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP7_KEY6_PKEXPIRESCAN_CASE_SINGLE_SET");

  //List
  s = db.LPush("GP7_KEY1_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP7_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP7_KEY2_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP7_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP7_KEY3_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP7_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE3"}, &uint64_ret);
  s = db.LPush("GP7_KEY4_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP7_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE4"}, &uint64_ret);
  s = db.LPush("GP7_KEY5_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP7_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE5"}, &uint64_ret);
  s = db.LPush("GP7_KEY6_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP7_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE6"}, &uint64_ret);
  delete_keys.push_back("GP7_KEY1_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP7_KEY2_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP7_KEY3_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP7_KEY4_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP7_KEY5_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP7_KEY6_PKEXPIRESCAN_CASE_SINGLE_LIST");

  //ZSet
  s = db.ZAdd("GP7_KEY1_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP7_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP7_KEY2_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP7_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP7_KEY3_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP7_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER3"}}, &int32_ret);
  s = db.ZAdd("GP7_KEY4_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP7_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER4"}}, &int32_ret);
  s = db.ZAdd("GP7_KEY5_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP7_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER5"}}, &int32_ret);
  s = db.ZAdd("GP7_KEY6_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP7_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER6"}}, &int32_ret);
  delete_keys.push_back("GP7_KEY1_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP7_KEY2_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP7_KEY3_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP7_KEY4_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP7_KEY5_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP7_KEY6_PKEXPIRESCAN_CASE_SINGLE_ZSET");

  ASSERT_TRUE(set_timeout(&db, "GP7_KEY1_PKEXPIRESCAN_CASE_SINGLE_SET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP7_KEY2_PKEXPIRESCAN_CASE_SINGLE_SET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP7_KEY3_PKEXPIRESCAN_CASE_SINGLE_SET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP7_KEY4_PKEXPIRESCAN_CASE_SINGLE_SET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP7_KEY5_PKEXPIRESCAN_CASE_SINGLE_SET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP7_KEY6_PKEXPIRESCAN_CASE_SINGLE_SET", 15));

  keys.clear();
  cursor = 0;
  cursor = db.PKExpireScan(DataType::kSets, cursor, 10, 20, 6, &keys);
  ASSERT_EQ(cursor, 0);
  ASSERT_EQ(keys.size(), 6);
  ASSERT_EQ(keys[0], "GP7_KEY1_PKEXPIRESCAN_CASE_SINGLE_SET");
  ASSERT_EQ(keys[1], "GP7_KEY2_PKEXPIRESCAN_CASE_SINGLE_SET");
  ASSERT_EQ(keys[2], "GP7_KEY3_PKEXPIRESCAN_CASE_SINGLE_SET");
  ASSERT_EQ(keys[3], "GP7_KEY4_PKEXPIRESCAN_CASE_SINGLE_SET");
  ASSERT_EQ(keys[4], "GP7_KEY5_PKEXPIRESCAN_CASE_SINGLE_SET");
  ASSERT_EQ(keys[5], "GP7_KEY6_PKEXPIRESCAN_CASE_SINGLE_SET");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 30);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 8 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP8_KEY1_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP8_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE1");
  s = db.Set("GP8_KEY2_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP8_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE2");
  s = db.Set("GP8_KEY3_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP8_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE3");
  s = db.Set("GP8_KEY4_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP8_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE4");
  s = db.Set("GP8_KEY5_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP8_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE5");
  s = db.Set("GP8_KEY6_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP8_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE6");
  delete_keys.push_back("GP8_KEY1_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP8_KEY2_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP8_KEY3_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP8_KEY4_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP8_KEY5_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP8_KEY6_PKEXPIRESCAN_CASE_SINGLE_STRING");

  //Hash
  s = db.HSet("GP8_KEY1_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP8_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD1", "GP8_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP8_KEY2_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP8_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD2", "GP8_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP8_KEY3_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP8_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD3", "GP8_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE3", &int32_ret);
  s = db.HSet("GP8_KEY4_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP8_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD4", "GP8_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE4", &int32_ret);
  s = db.HSet("GP8_KEY5_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP8_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD5", "GP8_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE5", &int32_ret);
  s = db.HSet("GP8_KEY6_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP8_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD6", "GP8_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE6", &int32_ret);
  delete_keys.push_back("GP8_KEY1_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP8_KEY2_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP8_KEY3_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP8_KEY4_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP8_KEY5_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP8_KEY6_PKEXPIRESCAN_CASE_SINGLE_HASH");

  //Set
  s = db.SAdd("GP8_KEY1_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP8_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP8_KEY2_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP8_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP8_KEY3_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP8_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER3"}, &int32_ret);
  s = db.SAdd("GP8_KEY4_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP8_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER4"}, &int32_ret);
  s = db.SAdd("GP8_KEY5_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP8_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER5"}, &int32_ret);
  s = db.SAdd("GP8_KEY6_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP8_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER6"}, &int32_ret);
  delete_keys.push_back("GP8_KEY1_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP8_KEY2_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP8_KEY3_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP8_KEY4_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP8_KEY5_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP8_KEY6_PKEXPIRESCAN_CASE_SINGLE_SET");

  //List
  s = db.LPush("GP8_KEY1_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP8_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP8_KEY2_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP8_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP8_KEY3_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP8_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE3"}, &uint64_ret);
  s = db.LPush("GP8_KEY4_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP8_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE4"}, &uint64_ret);
  s = db.LPush("GP8_KEY5_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP8_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE5"}, &uint64_ret);
  s = db.LPush("GP8_KEY6_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP8_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE6"}, &uint64_ret);
  delete_keys.push_back("GP8_KEY1_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP8_KEY2_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP8_KEY3_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP8_KEY4_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP8_KEY5_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP8_KEY6_PKEXPIRESCAN_CASE_SINGLE_LIST");

  //ZSet
  s = db.ZAdd("GP8_KEY1_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP8_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP8_KEY2_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP8_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP8_KEY3_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP8_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER3"}}, &int32_ret);
  s = db.ZAdd("GP8_KEY4_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP8_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER4"}}, &int32_ret);
  s = db.ZAdd("GP8_KEY5_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP8_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER5"}}, &int32_ret);
  s = db.ZAdd("GP8_KEY6_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP8_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER6"}}, &int32_ret);
  delete_keys.push_back("GP8_KEY1_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP8_KEY2_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP8_KEY3_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP8_KEY4_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP8_KEY5_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP8_KEY6_PKEXPIRESCAN_CASE_SINGLE_ZSET");

  ASSERT_TRUE(set_timeout(&db, "GP8_KEY1_PKEXPIRESCAN_CASE_SINGLE_SET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP8_KEY2_PKEXPIRESCAN_CASE_SINGLE_SET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP8_KEY3_PKEXPIRESCAN_CASE_SINGLE_SET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP8_KEY4_PKEXPIRESCAN_CASE_SINGLE_SET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP8_KEY5_PKEXPIRESCAN_CASE_SINGLE_SET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP8_KEY6_PKEXPIRESCAN_CASE_SINGLE_SET", 15));

  keys.clear();
  cursor = 0;
  cursor = db.PKExpireScan(DataType::kSets, cursor, 10, 20, 10, &keys);
  ASSERT_EQ(cursor, 0);
  ASSERT_EQ(keys.size(), 6);
  ASSERT_EQ(keys[0], "GP8_KEY1_PKEXPIRESCAN_CASE_SINGLE_SET");
  ASSERT_EQ(keys[1], "GP8_KEY2_PKEXPIRESCAN_CASE_SINGLE_SET");
  ASSERT_EQ(keys[2], "GP8_KEY3_PKEXPIRESCAN_CASE_SINGLE_SET");
  ASSERT_EQ(keys[3], "GP8_KEY4_PKEXPIRESCAN_CASE_SINGLE_SET");
  ASSERT_EQ(keys[4], "GP8_KEY5_PKEXPIRESCAN_CASE_SINGLE_SET");
  ASSERT_EQ(keys[5], "GP8_KEY6_PKEXPIRESCAN_CASE_SINGLE_SET");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 30);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 9 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP9_KEY1_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP9_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE1");
  s = db.Set("GP9_KEY2_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP9_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE2");
  s = db.Set("GP9_KEY3_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP9_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE3");
  s = db.Set("GP9_KEY4_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP9_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE4");
  s = db.Set("GP9_KEY5_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP9_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE5");
  s = db.Set("GP9_KEY6_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP9_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE6");
  delete_keys.push_back("GP9_KEY1_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP9_KEY2_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP9_KEY3_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP9_KEY4_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP9_KEY5_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP9_KEY6_PKEXPIRESCAN_CASE_SINGLE_STRING");

  //Hash
  s = db.HSet("GP9_KEY1_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP9_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD1", "GP9_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP9_KEY2_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP9_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD2", "GP9_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP9_KEY3_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP9_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD3", "GP9_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE3", &int32_ret);
  s = db.HSet("GP9_KEY4_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP9_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD4", "GP9_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE4", &int32_ret);
  s = db.HSet("GP9_KEY5_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP9_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD5", "GP9_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE5", &int32_ret);
  s = db.HSet("GP9_KEY6_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP9_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD6", "GP9_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE6", &int32_ret);
  delete_keys.push_back("GP9_KEY1_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP9_KEY2_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP9_KEY3_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP9_KEY4_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP9_KEY5_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP9_KEY6_PKEXPIRESCAN_CASE_SINGLE_HASH");

  //Set
  s = db.SAdd("GP9_KEY1_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP9_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP9_KEY2_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP9_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP9_KEY3_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP9_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER3"}, &int32_ret);
  s = db.SAdd("GP9_KEY4_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP9_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER4"}, &int32_ret);
  s = db.SAdd("GP9_KEY5_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP9_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER5"}, &int32_ret);
  s = db.SAdd("GP9_KEY6_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP9_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER6"}, &int32_ret);
  delete_keys.push_back("GP9_KEY1_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP9_KEY2_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP9_KEY3_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP9_KEY4_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP9_KEY5_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP9_KEY6_PKEXPIRESCAN_CASE_SINGLE_SET");

  //List
  s = db.LPush("GP9_KEY1_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP9_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP9_KEY2_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP9_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP9_KEY3_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP9_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE3"}, &uint64_ret);
  s = db.LPush("GP9_KEY4_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP9_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE4"}, &uint64_ret);
  s = db.LPush("GP9_KEY5_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP9_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE5"}, &uint64_ret);
  s = db.LPush("GP9_KEY6_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP9_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE6"}, &uint64_ret);
  delete_keys.push_back("GP9_KEY1_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP9_KEY2_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP9_KEY3_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP9_KEY4_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP9_KEY5_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP9_KEY6_PKEXPIRESCAN_CASE_SINGLE_LIST");

  //ZSet
  s = db.ZAdd("GP9_KEY1_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP9_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP9_KEY2_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP9_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP9_KEY3_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP9_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER3"}}, &int32_ret);
  s = db.ZAdd("GP9_KEY4_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP9_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER4"}}, &int32_ret);
  s = db.ZAdd("GP9_KEY5_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP9_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER5"}}, &int32_ret);
  s = db.ZAdd("GP9_KEY6_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP9_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER6"}}, &int32_ret);
  delete_keys.push_back("GP9_KEY1_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP9_KEY2_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP9_KEY3_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP9_KEY4_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP9_KEY5_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP9_KEY6_PKEXPIRESCAN_CASE_SINGLE_ZSET");

  ASSERT_TRUE(set_timeout(&db, "GP9_KEY1_PKEXPIRESCAN_CASE_SINGLE_ZSET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP9_KEY2_PKEXPIRESCAN_CASE_SINGLE_ZSET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP9_KEY3_PKEXPIRESCAN_CASE_SINGLE_ZSET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP9_KEY4_PKEXPIRESCAN_CASE_SINGLE_ZSET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP9_KEY5_PKEXPIRESCAN_CASE_SINGLE_ZSET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP9_KEY6_PKEXPIRESCAN_CASE_SINGLE_ZSET", 15));

  keys.clear();
  cursor = 0;
  cursor = db.PKExpireScan(DataType::kZSets, cursor, 10, 20, 2, &keys);
  ASSERT_EQ(cursor, 2);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP9_KEY1_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  ASSERT_EQ(keys[1], "GP9_KEY2_PKEXPIRESCAN_CASE_SINGLE_ZSET");

  keys.clear();
  cursor = db.PKExpireScan(DataType::kZSets, cursor, 10, 20, 2, &keys);
  ASSERT_EQ(cursor, 4);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP9_KEY3_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  ASSERT_EQ(keys[1], "GP9_KEY4_PKEXPIRESCAN_CASE_SINGLE_ZSET");

  keys.clear();
  cursor = db.PKExpireScan(DataType::kZSets, cursor, 10, 20, 2, &keys);
  ASSERT_EQ(cursor, 0);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP9_KEY5_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  ASSERT_EQ(keys[1], "GP9_KEY6_PKEXPIRESCAN_CASE_SINGLE_ZSET");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 30);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 10 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP10_KEY1_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP10_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE1");
  s = db.Set("GP10_KEY2_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP10_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE2");
  s = db.Set("GP10_KEY3_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP10_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE3");
  s = db.Set("GP10_KEY4_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP10_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE4");
  s = db.Set("GP10_KEY5_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP10_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE5");
  s = db.Set("GP10_KEY6_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP10_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE6");
  delete_keys.push_back("GP10_KEY1_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP10_KEY2_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP10_KEY3_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP10_KEY4_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP10_KEY5_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP10_KEY6_PKEXPIRESCAN_CASE_SINGLE_STRING");

  //Hash
  s = db.HSet("GP10_KEY1_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP10_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD1", "GP10_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP10_KEY2_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP10_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD2", "GP10_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP10_KEY3_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP10_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD3", "GP10_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE3", &int32_ret);
  s = db.HSet("GP10_KEY4_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP10_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD4", "GP10_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE4", &int32_ret);
  s = db.HSet("GP10_KEY5_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP10_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD5", "GP10_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE5", &int32_ret);
  s = db.HSet("GP10_KEY6_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP10_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD6", "GP10_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE6", &int32_ret);
  delete_keys.push_back("GP10_KEY1_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP10_KEY2_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP10_KEY3_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP10_KEY4_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP10_KEY5_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP10_KEY6_PKEXPIRESCAN_CASE_SINGLE_HASH");

  //Set
  s = db.SAdd("GP10_KEY1_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP10_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP10_KEY2_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP10_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP10_KEY3_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP10_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER3"}, &int32_ret);
  s = db.SAdd("GP10_KEY4_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP10_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER4"}, &int32_ret);
  s = db.SAdd("GP10_KEY5_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP10_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER5"}, &int32_ret);
  s = db.SAdd("GP10_KEY6_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP10_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER6"}, &int32_ret);
  delete_keys.push_back("GP10_KEY1_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP10_KEY2_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP10_KEY3_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP10_KEY4_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP10_KEY5_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP10_KEY6_PKEXPIRESCAN_CASE_SINGLE_SET");

  //List
  s = db.LPush("GP10_KEY1_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP10_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP10_KEY2_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP10_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP10_KEY3_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP10_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE3"}, &uint64_ret);
  s = db.LPush("GP10_KEY4_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP10_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE4"}, &uint64_ret);
  s = db.LPush("GP10_KEY5_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP10_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE5"}, &uint64_ret);
  s = db.LPush("GP10_KEY6_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP10_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE6"}, &uint64_ret);
  delete_keys.push_back("GP10_KEY1_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP10_KEY2_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP10_KEY3_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP10_KEY4_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP10_KEY5_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP10_KEY6_PKEXPIRESCAN_CASE_SINGLE_LIST");

  //ZSet
  s = db.ZAdd("GP10_KEY1_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP10_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP10_KEY2_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP10_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP10_KEY3_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP10_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER3"}}, &int32_ret);
  s = db.ZAdd("GP10_KEY4_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP10_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER4"}}, &int32_ret);
  s = db.ZAdd("GP10_KEY5_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP10_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER5"}}, &int32_ret);
  s = db.ZAdd("GP10_KEY6_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP10_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER6"}}, &int32_ret);
  delete_keys.push_back("GP10_KEY1_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP10_KEY2_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP10_KEY3_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP10_KEY4_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP10_KEY5_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP10_KEY6_PKEXPIRESCAN_CASE_SINGLE_ZSET");

  ASSERT_TRUE(set_timeout(&db, "GP10_KEY1_PKEXPIRESCAN_CASE_SINGLE_ZSET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP10_KEY2_PKEXPIRESCAN_CASE_SINGLE_ZSET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP10_KEY3_PKEXPIRESCAN_CASE_SINGLE_ZSET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP10_KEY4_PKEXPIRESCAN_CASE_SINGLE_ZSET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP10_KEY5_PKEXPIRESCAN_CASE_SINGLE_ZSET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP10_KEY6_PKEXPIRESCAN_CASE_SINGLE_ZSET", 15));

  keys.clear();
  cursor = 0;
  cursor = db.PKExpireScan(DataType::kZSets, cursor, 10, 20, 4, &keys);
  ASSERT_EQ(cursor, 4);
  ASSERT_EQ(keys.size(), 4);
  ASSERT_EQ(keys[0], "GP10_KEY1_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  ASSERT_EQ(keys[1], "GP10_KEY2_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  ASSERT_EQ(keys[2], "GP10_KEY3_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  ASSERT_EQ(keys[3], "GP10_KEY4_PKEXPIRESCAN_CASE_SINGLE_ZSET");

  keys.clear();
  cursor = db.PKExpireScan(DataType::kZSets, cursor, 10, 20, 4, &keys);
  ASSERT_EQ(cursor, 0);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP10_KEY5_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  ASSERT_EQ(keys[1], "GP10_KEY6_PKEXPIRESCAN_CASE_SINGLE_ZSET");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 30);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 11 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP11_KEY1_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP11_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE1");
  s = db.Set("GP11_KEY2_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP11_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE2");
  s = db.Set("GP11_KEY3_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP11_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE3");
  s = db.Set("GP11_KEY4_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP11_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE4");
  s = db.Set("GP11_KEY5_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP11_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE5");
  s = db.Set("GP11_KEY6_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP11_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE6");
  delete_keys.push_back("GP11_KEY1_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP11_KEY2_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP11_KEY3_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP11_KEY4_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP11_KEY5_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP11_KEY6_PKEXPIRESCAN_CASE_SINGLE_STRING");

  //Hash
  s = db.HSet("GP11_KEY1_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP11_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD1", "GP11_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP11_KEY2_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP11_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD2", "GP11_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP11_KEY3_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP11_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD3", "GP11_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE3", &int32_ret);
  s = db.HSet("GP11_KEY4_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP11_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD4", "GP11_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE4", &int32_ret);
  s = db.HSet("GP11_KEY5_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP11_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD5", "GP11_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE5", &int32_ret);
  s = db.HSet("GP11_KEY6_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP11_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD6", "GP11_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE6", &int32_ret);
  delete_keys.push_back("GP11_KEY1_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP11_KEY2_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP11_KEY3_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP11_KEY4_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP11_KEY5_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP11_KEY6_PKEXPIRESCAN_CASE_SINGLE_HASH");

  //Set
  s = db.SAdd("GP11_KEY1_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP11_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP11_KEY2_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP11_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP11_KEY3_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP11_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER3"}, &int32_ret);
  s = db.SAdd("GP11_KEY4_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP11_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER4"}, &int32_ret);
  s = db.SAdd("GP11_KEY5_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP11_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER5"}, &int32_ret);
  s = db.SAdd("GP11_KEY6_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP11_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER6"}, &int32_ret);
  delete_keys.push_back("GP11_KEY1_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP11_KEY2_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP11_KEY3_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP11_KEY4_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP11_KEY5_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP11_KEY6_PKEXPIRESCAN_CASE_SINGLE_SET");

  //List
  s = db.LPush("GP11_KEY1_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP11_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP11_KEY2_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP11_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP11_KEY3_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP11_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE3"}, &uint64_ret);
  s = db.LPush("GP11_KEY4_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP11_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE4"}, &uint64_ret);
  s = db.LPush("GP11_KEY5_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP11_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE5"}, &uint64_ret);
  s = db.LPush("GP11_KEY6_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP11_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE6"}, &uint64_ret);
  delete_keys.push_back("GP11_KEY1_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP11_KEY2_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP11_KEY3_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP11_KEY4_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP11_KEY5_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP11_KEY6_PKEXPIRESCAN_CASE_SINGLE_LIST");

  //ZSet
  s = db.ZAdd("GP11_KEY1_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP11_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP11_KEY2_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP11_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP11_KEY3_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP11_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER3"}}, &int32_ret);
  s = db.ZAdd("GP11_KEY4_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP11_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER4"}}, &int32_ret);
  s = db.ZAdd("GP11_KEY5_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP11_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER5"}}, &int32_ret);
  s = db.ZAdd("GP11_KEY6_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP11_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER6"}}, &int32_ret);
  delete_keys.push_back("GP11_KEY1_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP11_KEY2_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP11_KEY3_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP11_KEY4_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP11_KEY5_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP11_KEY6_PKEXPIRESCAN_CASE_SINGLE_ZSET");

  ASSERT_TRUE(set_timeout(&db, "GP11_KEY1_PKEXPIRESCAN_CASE_SINGLE_ZSET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP11_KEY2_PKEXPIRESCAN_CASE_SINGLE_ZSET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP11_KEY3_PKEXPIRESCAN_CASE_SINGLE_ZSET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP11_KEY4_PKEXPIRESCAN_CASE_SINGLE_ZSET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP11_KEY5_PKEXPIRESCAN_CASE_SINGLE_ZSET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP11_KEY6_PKEXPIRESCAN_CASE_SINGLE_ZSET", 15));

  keys.clear();
  cursor = 0;
  cursor = db.PKExpireScan(DataType::kZSets, cursor, 10, 20, 6, &keys);
  ASSERT_EQ(cursor, 0);
  ASSERT_EQ(keys.size(), 6);
  ASSERT_EQ(keys[0], "GP11_KEY1_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  ASSERT_EQ(keys[1], "GP11_KEY2_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  ASSERT_EQ(keys[2], "GP11_KEY3_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  ASSERT_EQ(keys[3], "GP11_KEY4_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  ASSERT_EQ(keys[4], "GP11_KEY5_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  ASSERT_EQ(keys[5], "GP11_KEY6_PKEXPIRESCAN_CASE_SINGLE_ZSET");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 30);
  sleep(2);
  db.Compact(DataType::kAll, true);


  // ***************** Group 12 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP12_KEY1_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP12_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE1");
  s = db.Set("GP12_KEY2_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP12_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE2");
  s = db.Set("GP12_KEY3_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP12_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE3");
  s = db.Set("GP12_KEY4_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP12_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE4");
  s = db.Set("GP12_KEY5_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP12_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE5");
  s = db.Set("GP12_KEY6_PKEXPIRESCAN_CASE_SINGLE_STRING", "GP12_PKEXPIRESCAN_CASE_SINGLE_STRING_VALUE6");
  delete_keys.push_back("GP12_KEY1_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP12_KEY2_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP12_KEY3_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP12_KEY4_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP12_KEY5_PKEXPIRESCAN_CASE_SINGLE_STRING");
  delete_keys.push_back("GP12_KEY6_PKEXPIRESCAN_CASE_SINGLE_STRING");

  //Hash
  s = db.HSet("GP12_KEY1_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP12_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD1", "GP12_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP12_KEY2_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP12_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD2", "GP12_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP12_KEY3_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP12_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD3", "GP12_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE3", &int32_ret);
  s = db.HSet("GP12_KEY4_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP12_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD4", "GP12_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE4", &int32_ret);
  s = db.HSet("GP12_KEY5_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP12_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD5", "GP12_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE5", &int32_ret);
  s = db.HSet("GP12_KEY6_PKEXPIRESCAN_CASE_SINGLE_HASH", "GP12_PKEXPIRESCAN_CASE_SINGLE_HASH_FIELD6", "GP12_PKEXPIRESCAN_CASE_SINGLE_HASH_VALUE6", &int32_ret);
  delete_keys.push_back("GP12_KEY1_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP12_KEY2_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP12_KEY3_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP12_KEY4_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP12_KEY5_PKEXPIRESCAN_CASE_SINGLE_HASH");
  delete_keys.push_back("GP12_KEY6_PKEXPIRESCAN_CASE_SINGLE_HASH");

  //Set
  s = db.SAdd("GP12_KEY1_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP12_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP12_KEY2_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP12_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP12_KEY3_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP12_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER3"}, &int32_ret);
  s = db.SAdd("GP12_KEY4_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP12_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER4"}, &int32_ret);
  s = db.SAdd("GP12_KEY5_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP12_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER5"}, &int32_ret);
  s = db.SAdd("GP12_KEY6_PKEXPIRESCAN_CASE_SINGLE_SET", {"GP12_PKEXPIRESCAN_CASE_SINGLE_SET_MEMBER6"}, &int32_ret);
  delete_keys.push_back("GP12_KEY1_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP12_KEY2_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP12_KEY3_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP12_KEY4_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP12_KEY5_PKEXPIRESCAN_CASE_SINGLE_SET");
  delete_keys.push_back("GP12_KEY6_PKEXPIRESCAN_CASE_SINGLE_SET");

  //List
  s = db.LPush("GP12_KEY1_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP12_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP12_KEY2_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP12_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP12_KEY3_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP12_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE3"}, &uint64_ret);
  s = db.LPush("GP12_KEY4_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP12_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE4"}, &uint64_ret);
  s = db.LPush("GP12_KEY5_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP12_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE5"}, &uint64_ret);
  s = db.LPush("GP12_KEY6_PKEXPIRESCAN_CASE_SINGLE_LIST", {"GP12_PKEXPIRESCAN_CASE_SINGLE_LIST_NODE6"}, &uint64_ret);
  delete_keys.push_back("GP12_KEY1_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP12_KEY2_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP12_KEY3_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP12_KEY4_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP12_KEY5_PKEXPIRESCAN_CASE_SINGLE_LIST");
  delete_keys.push_back("GP12_KEY6_PKEXPIRESCAN_CASE_SINGLE_LIST");

  //ZSet
  s = db.ZAdd("GP12_KEY1_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP12_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP12_KEY2_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP12_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP12_KEY3_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP12_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER3"}}, &int32_ret);
  s = db.ZAdd("GP12_KEY4_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP12_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER4"}}, &int32_ret);
  s = db.ZAdd("GP12_KEY5_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP12_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER5"}}, &int32_ret);
  s = db.ZAdd("GP12_KEY6_PKEXPIRESCAN_CASE_SINGLE_ZSET", {{1, "GP12_PKEXPIRESCAN_CASE_SINGLE_LIST_MEMBER6"}}, &int32_ret);
  delete_keys.push_back("GP12_KEY1_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP12_KEY2_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP12_KEY3_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP12_KEY4_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP12_KEY5_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  delete_keys.push_back("GP12_KEY6_PKEXPIRESCAN_CASE_SINGLE_ZSET");

  ASSERT_TRUE(set_timeout(&db, "GP12_KEY1_PKEXPIRESCAN_CASE_SINGLE_ZSET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP12_KEY2_PKEXPIRESCAN_CASE_SINGLE_ZSET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP12_KEY3_PKEXPIRESCAN_CASE_SINGLE_ZSET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP12_KEY4_PKEXPIRESCAN_CASE_SINGLE_ZSET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP12_KEY5_PKEXPIRESCAN_CASE_SINGLE_ZSET", 15));
  ASSERT_TRUE(set_timeout(&db, "GP12_KEY6_PKEXPIRESCAN_CASE_SINGLE_ZSET", 15));

  keys.clear();
  cursor = 0;
  cursor = db.PKExpireScan(DataType::kZSets, cursor, 10, 20, 10, &keys);
  ASSERT_EQ(cursor, 0);
  ASSERT_EQ(keys.size(), 6);
  ASSERT_EQ(keys[0], "GP12_KEY1_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  ASSERT_EQ(keys[1], "GP12_KEY2_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  ASSERT_EQ(keys[2], "GP12_KEY3_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  ASSERT_EQ(keys[3], "GP12_KEY4_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  ASSERT_EQ(keys[4], "GP12_KEY5_PKEXPIRESCAN_CASE_SINGLE_ZSET");
  ASSERT_EQ(keys[5], "GP12_KEY6_PKEXPIRESCAN_CASE_SINGLE_ZSET");

  del_num = db.Del(delete_keys, &type_status);
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
  s = db.HSet("GP1_EXPIRE_KEY", "FIELD", "VALUE", &ret);
  ASSERT_TRUE(s.ok());

  // Sets
  s = db.SAdd("GP1_EXPIRE_KEY", {"MEMBER"}, &ret);
  ASSERT_TRUE(s.ok());

  // Lists
  uint64_t llen;
  s = db.RPush("GP1_EXPIRE_KEY", {"NODE"}, &llen);
  ASSERT_TRUE(s.ok());

  // Zsets
  s = db.ZAdd("GP1_EXPIRE_KEY", {{1, "MEMBER"}}, &ret);
  ASSERT_TRUE(s.ok());

  ret = db.Expire("GP1_EXPIRE_KEY", 1, &type_status);
  ASSERT_EQ(ret, 5);
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  // Strings
  s = db.Get("GP1_EXPIRE_KEY", &value);
  ASSERT_TRUE(s.IsNotFound());

  // Hashes
  s = db.HGet("GP1_EXPIRE_KEY", "EXPIRE_FIELD", &value);
  ASSERT_TRUE(s.IsNotFound());

  // Sets
  s = db.SCard("GP1_EXPIRE_KEY", &ret);
  ASSERT_TRUE(s.IsNotFound());

  // Lists
  s = db.LLen("GP1_EXPIRE_KEY", &llen);
  ASSERT_TRUE(s.IsNotFound());

  // ZSets
  s = db.ZCard("GP1_EXPIRE_KEY", &ret);
  ASSERT_TRUE(s.IsNotFound());


  // ***************** Group 2 Test *****************
  // Strings
  s = db.Set("GP2_EXPIRE_STRING_KEY", "VALUE");
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(make_expired(&db, "GP2_EXPIRE_STRING_KEY"));

  type_status.clear();
  ret = db.Expire("GP2_EXPIRE_STRING_KEY", 1, &type_status);
  ASSERT_EQ(ret, 0);

  // Hashes
  s = db.HSet("GP2_EXPIRE_HASHES_KEY", "FIELD", "VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(make_expired(&db, "GP2_EXPIRE_HASHES_KEY"));

  type_status.clear();
  ret = db.Expire("GP2_EXPIRE_HASHES_KEY", 1, &type_status);
  ASSERT_EQ(ret, 0);

  // Sets
  s = db.SAdd("GP2_EXPIRE_SETS_KEY", {"MEMBER"}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(make_expired(&db, "GP2_EXPIRE_SETS_KEY"));

  type_status.clear();
  ret = db.Expire("GP2_EXPIRE_SETS_KEY", 1, &type_status);
  ASSERT_EQ(ret, 0);

  // Lists
  s = db.RPush("GP2_EXPIRE_LISTS_KEY", {"NODE"}, &llen);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(make_expired(&db, "GP2_EXPIRE_LISTS_KEY"));

  type_status.clear();
  ret = db.Expire("GP2_EXPIRE_LISTS_KEY", 1, &type_status);
  ASSERT_EQ(ret, 0);

  // Zsets
  s = db.ZAdd("GP2_EXPIRE_ZSETS_KEY", {{1, "MEMBER"}}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(make_expired(&db, "GP2_EXPIRE_ZSETS_KEY"));

  type_status.clear();
  ret = db.Expire("GP2_EXPIRE_ZSETS_KEY", 1, &type_status);
  ASSERT_EQ(ret, 0);


  // ***************** Group 3 Test *****************
  // Strings
  s = db.Set("GP3_EXPIRE_STRING_KEY", "VALUE");
  ASSERT_TRUE(s.ok());
  ret = db.Del({"GP3_EXPIRE_STRING_KEY"}, &type_status);
  ASSERT_EQ(ret, 1);

  type_status.clear();
  ret = db.Expire("GP3_EXPIRE_STRING_KEY", 1, &type_status);
  ASSERT_EQ(ret, 0);

  // Hashes
  s = db.HSet("GP3_EXPIRE_HASHES_KEY", "FIELD", "VALUE", &ret);
  ASSERT_TRUE(s.ok());
  s = db.HDel("GP3_EXPIRE_HASHES_KEY", {"FIELD"}, &ret);
  ASSERT_TRUE(s.ok());

  type_status.clear();
  ret = db.Expire("GP3_EXPIRE_HASHES_KEY", 1, &type_status);
  ASSERT_EQ(ret, 0);

  // Sets
  s = db.SAdd("GP3_EXPIRE_SETS_KEY", {"MEMBER"}, &ret);
  ASSERT_TRUE(s.ok());
  s = db.SRem("GP3_EXPIRE_SETS_KEY", {"MEMBER"}, &ret);
  ASSERT_TRUE(s.ok());

  type_status.clear();
  ret = db.Expire("GP3_EXPIRE_SETS_KEY", 1, &type_status);
  ASSERT_EQ(ret, 0);

  // Lists
  s = db.RPush("GP3_EXPIRE_LISTS_KEY", {"NODE"}, &llen);
  ASSERT_TRUE(s.ok());
  std::string element;
  s = db.LPop("GP3_EXPIRE_LISTS_KEY", &element);
  ASSERT_TRUE(s.ok());

  type_status.clear();
  ret = db.Expire("GP3_EXPIRE_LISTS_KEY", 1, &type_status);
  ASSERT_EQ(ret, 0);

  // Zsets
  s = db.ZAdd("GP3_EXPIRE_ZSETS_KEY", {{1, "MEMBER"}}, &ret);
  ASSERT_TRUE(s.ok());
  s = db.ZRem("GP3_EXPIRE_ZSETS_KEY", {"MEMBER"}, &ret);
  ASSERT_TRUE(s.ok());

  type_status.clear();
  ret = db.Expire("GP3_EXPIRE_ZSETS_KEY", 1, &type_status);
  ASSERT_EQ(ret, 0);
}

// Del
TEST_F(KeysTest, DelTest) {
  int32_t ret;
  std::string value;
  std::map<storage::DataType, Status> type_status;
  std::vector<std::string> keys {"DEL_KEY"};

  // Strings
  s = db.Set("DEL_KEY", "VALUE");
  ASSERT_TRUE(s.ok());

  // Hashes
  s = db.HSet("DEL_KEY", "FIELD", "VALUE", &ret);
  ASSERT_TRUE(s.ok());

  // Sets
  s = db.SAdd("DEL_KEY", {"MEMBER"}, &ret);
  ASSERT_TRUE(s.ok());

  // Lists
  uint64_t llen;
  s = db.RPush("DEL_KEY", {"NODE"}, &llen);
  ASSERT_TRUE(s.ok());

  // ZSets
  s = db.ZAdd("DEL_KEY", {{1, "MEMBER"}}, &ret);
  ASSERT_TRUE(s.ok());

  ret = db.Del(keys, &type_status);
  ASSERT_EQ(ret, 5);

  // Strings
  s = db.Get("DEL_KEY", &value);
  ASSERT_TRUE(s.IsNotFound());

  // Hashes
  s = db.HGet("DEL_KEY", "DEL_FIELD", &value);
  ASSERT_TRUE(s.IsNotFound());

  // Sets
  s = db.SCard("DEL_KEY", &ret);
  ASSERT_TRUE(s.IsNotFound());

  // Lists
  s = db.LLen("DEL_KEY", &llen);
  ASSERT_TRUE(s.IsNotFound());

  // ZSets
  s = db.ZCard("DEL_KEY", &ret);
  ASSERT_TRUE(s.IsNotFound());
}

// Exists
TEST_F(KeysTest, ExistsTest) {
  int32_t ret;
  uint64_t llen;
  std::map<storage::DataType, Status> type_status;
  std::vector<std::string> keys {"EXISTS_KEY"};

  // Strings
  s = db.Set("EXISTS_KEY", "VALUE");
  ASSERT_TRUE(s.ok());

  // Hashes
  s = db.HSet("EXISTS_KEY", "FIELD", "VALUE", &ret);
  ASSERT_TRUE(s.ok());

  // Sets
  s = db.SAdd("EXISTS_KEY", {"MEMBER"}, &ret);
  ASSERT_TRUE(s.ok());

  // Lists
  s = db.RPush("EXISTS_KEY", {"NODE"}, &llen);
  ASSERT_TRUE(s.ok());

  // ZSets
  s = db.ZAdd("EXISTS_KEY", {{1, "MEMBER"}}, &ret);
  ASSERT_TRUE(s.ok());

  ret = db.Exists(keys, &type_status);
  ASSERT_EQ(ret, 5);
}

// Expireat
TEST_F(KeysTest, ExpireatTest) {
  // If the key does not exist
  std::map<storage::DataType, Status> type_status;
  int32_t ret = db.Expireat("EXPIREAT_KEY", 0, &type_status);
  ASSERT_EQ(ret, 0);

  // Strings
  std::string value;
  s = db.Set("EXPIREAT_KEY", "VALUE");
  ASSERT_TRUE(s.ok());

  // Hashes
  s = db.HSet("EXPIREAT_KEY", "EXPIREAT_FIELD", "VALUE", &ret);
  ASSERT_TRUE(s.ok());

  // Sets
  s = db.SAdd("EXPIREAT_KEY", {"MEMBER"}, &ret);
  ASSERT_TRUE(s.ok());

  // List
  uint64_t llen;
  s = db.RPush("EXPIREAT_KEY", {"NODE"}, &llen);
  ASSERT_TRUE(s.ok());

  // ZSets
  s = db.ZAdd("EXPIREAT_KEY", {{1, "MEMBER"}}, &ret);
  ASSERT_TRUE(s.ok());

  int64_t unix_time;
  rocksdb::Env::Default()->GetCurrentTime(&unix_time);
  int32_t timestamp = static_cast<int32_t>(unix_time) + 1;
  ret = db.Expireat("EXPIREAT_KEY", timestamp, &type_status);
  ASSERT_EQ(ret, 5);

  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  // Strings
  s = db.Get("EXPIREAT_KEY", &value);
  ASSERT_TRUE(s.IsNotFound());

  // Hashes
  s = db.HGet("EXPIREAT_KEY", "EXPIREAT_FIELD", &value);
  ASSERT_TRUE(s.IsNotFound());

  // Sets
  s = db.SCard("EXPIREAT_KEY", &ret);
  ASSERT_TRUE(s.IsNotFound());

  // List
  s = db.LLen("EXPIREAT_KEY", &llen);
  ASSERT_TRUE(s.IsNotFound());

  // ZSets
  s = db.ZCard("EXPIREAT_KEY", &ret);
  ASSERT_TRUE(s.IsNotFound());

  // Expireat key 0
  s = db.Set("EXPIREAT_KEY", "VALUE");
  ASSERT_TRUE(s.ok());

  s = db.HSet("EXPIREAT_KEY", "EXPIREAT_FIELD", "VALUE", &ret);
  ASSERT_TRUE(s.ok());

  s = db.SAdd("EXPIREAT_KEY", {"MEMBER"}, &ret);
  ASSERT_TRUE(s.ok());

  s = db.RPush("EXPIREAT_KEY", {"NODE"}, &llen);
  ASSERT_TRUE(s.ok());

  s = db.ZAdd("EXPIREAT_KEY", {{1, "MEMBER"}}, &ret);
  ASSERT_TRUE(s.ok());

  
  ret = db.Expireat("EXPIREAT_KEY", 0, &type_status);
  ASSERT_EQ(ret, 5);

  // Strings
  s = db.Get("EXPIREAT_KEY", &value);
  ASSERT_TRUE(s.IsNotFound());

  // Hashes
  s = db.HGet("EXPIREAT_KEY", "EXPIREAT_FIELD", &value);
  ASSERT_TRUE(s.IsNotFound());

  // Sets
  s = db.SCard("EXPIREAT_KEY", &ret);
  ASSERT_TRUE(s.IsNotFound());

  // List
  s = db.LLen("EXPIREAT_KEY", &llen);
  ASSERT_TRUE(s.IsNotFound());

  // ZSets
  s = db.ZCard("EXPIREAT_KEY", &ret);
  ASSERT_TRUE(s.IsNotFound());
}

// Persist
TEST_F(KeysTest, PersistTest) {
  // If the key does not exist
  std::map<storage::DataType, Status> type_status;
  int32_t ret = db.Persist("EXPIREAT_KEY", &type_status);
  ASSERT_EQ(ret, 0);

  // If the key does not have an associated timeout
  // Strings
  std::string value;
  s = db.Set("PERSIST_KEY", "VALUE");
  ASSERT_TRUE(s.ok());

  // Hashes
  s = db.HSet("PERSIST_KEY", "FIELD", "VALUE", &ret);
  ASSERT_TRUE(s.ok());

  // Sets
  s = db.SAdd("PERSIST_KEY", {"MEMBER"}, &ret);
  ASSERT_TRUE(s.ok());

  // Lists
  uint64_t llen;
  s = db.LPush("PERSIST_KEY", {"NODE"}, &llen);
  ASSERT_TRUE(s.ok());

  // ZSets
  s = db.ZAdd("PERSIST_KEY", {{1, "MEMBER"}}, &ret);
  ASSERT_TRUE(s.ok());

  ret = db.Persist("PERSIST_KEY", &type_status);
  ASSERT_EQ(ret, 0);

  // If the timeout was set
  ret = db.Expire("PERSIST_KEY", 1000, &type_status);
  ASSERT_EQ(ret, 5);
  ret = db.Persist("PERSIST_KEY", &type_status);
  ASSERT_EQ(ret, 5);

  std::map<storage::DataType, int64_t> ttl_ret;
  ttl_ret = db.TTL("PERSIST_KEY", &type_status);
  ASSERT_EQ(ttl_ret.size(), 5);
  for (auto it = ttl_ret.begin(); it != ttl_ret.end(); it++) {
    ASSERT_EQ(it->second, -1);
  }
}

// TTL
TEST_F(KeysTest, TTLTest) {
  // If the key does not exist
  std::map<storage::DataType, Status> type_status;
  std::map<storage::DataType, int64_t> ttl_ret;
  ttl_ret = db.TTL("TTL_KEY", &type_status);
  ASSERT_EQ(ttl_ret.size(), 5);
  for (auto it = ttl_ret.begin(); it != ttl_ret.end(); it++) {
    ASSERT_EQ(it->second, -2);
  }

  // If the key does not have an associated timeout
  // Strings
  std::string value;
  int32_t ret = 0;
  s = db.Set("TTL_KEY", "VALUE");
  ASSERT_TRUE(s.ok());

  // Hashes
  s = db.HSet("TTL_KEY", "FIELD", "VALUE", &ret);
  ASSERT_TRUE(s.ok());

  // Sets
  s = db.SAdd("TTL_KEY", {"MEMBER"}, &ret);
  ASSERT_TRUE(s.ok());

  // Lists
  uint64_t llen;
  s = db.RPush("TTL_KEY", {"NODE"}, &llen);
  ASSERT_TRUE(s.ok());

  // ZSets
  s = db.ZAdd("TTL_KEY", {{1, "SCORE"}}, &ret);
  ASSERT_TRUE(s.ok());

  ttl_ret = db.TTL("TTL_KEY", &type_status);
  ASSERT_EQ(ttl_ret.size(), 5);
  for (auto it = ttl_ret.begin(); it != ttl_ret.end(); it++) {
    ASSERT_EQ(it->second, -1);
  }

  // If the timeout was set
  ret = db.Expire("TTL_KEY", 10, &type_status);
  ASSERT_EQ(ret, 5);
  ttl_ret = db.TTL("TTL_KEY", &type_status);
  ASSERT_EQ(ttl_ret.size(), 5);
  for (auto it = ttl_ret.begin(); it != ttl_ret.end(); it++) {
    ASSERT_GT(it->second, 0);
    ASSERT_LE(it->second, 10);
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

