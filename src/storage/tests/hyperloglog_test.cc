//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <gtest/gtest.h>
#include <iostream>
#include <thread>

#include "storage/storage.h"
#include "storage/util.h"

using namespace storage;

class HyperLogLogTest : public ::testing::Test {
 public:
  HyperLogLogTest() = default;
  ~HyperLogLogTest() override = default;

  void SetUp() override {
    std::string path = "./db/hyperloglog";
    if (access(path.c_str(), F_OK) != 0) {
      mkdir(path.c_str(), 0755);
    }
    storage_options.options.create_if_missing = true;
    s = db.Open(storage_options, path);
  }

  void TearDown() override {
    std::string path = "./db/hyperloglog";
    DeleteFiles(path.c_str());
  }

  static void SetUpTestSuite() {}
  static void TearDownTestSuite() {}

  StorageOptions storage_options;
  storage::Storage db;
  storage::Status s;
};

TEST_F(HyperLogLogTest, PfaddTest) {
  std::vector<std::string> values;
  bool update;
  std::map<storage::DataType, Status> type_status;
  // PFADD without arguments creates an HLL value
  s = db.PfAdd("HLL", values, &update);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(update);
  std::vector<std::string> keys{"HLL"};
  int64_t nums = db.Exists(keys);
  ASSERT_EQ(nums, 1);

  // Approximated cardinality after creation is zero
  int64_t result;
  s = db.PfCount(keys, &result);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(result, 0);

  nums = db.Del(keys);
  ASSERT_EQ(nums, 1);

  // PFADD the return value is true when at least 1 reg was modified
  values.clear();
  values.emplace_back("A");
  values.emplace_back("B");
  values.emplace_back("C");
  s = db.PfAdd("HLL", values, &update);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(update);

  // PFADD the return value is false when no reg was modified
  values.clear();
  values.emplace_back("A");
  values.emplace_back("B");
  values.emplace_back("C");
  update = false;
  s = db.PfAdd("HLL", values, &update);
  ASSERT_TRUE(s.ok());
  ASSERT_FALSE(update);
  nums = db.Del(keys);
  ASSERT_EQ(nums, 1);

  // PFADD works with empty string (regression)
  values.clear();
  values.emplace_back("");
  s = db.PfAdd("HLL", values, &update);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(update);

  nums = db.Del(keys);
  ASSERT_EQ(nums, 1);
}

TEST_F(HyperLogLogTest, PfCountTest) {
  // PFCOUNT returns approximated cardinality of set
  std::vector<std::string> values;
  bool update;
  std::map<storage::DataType, Status> type_status;

  for (int32_t i = 1; i <= 5; i++) {
    values.push_back(std::to_string(i));
  }
  s = db.PfAdd("HLL", values, &update);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(update);

  std::vector<std::string> keys{"HLL"};
  int64_t result;
  s = db.PfCount(keys, &result);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(result, 5);

  values.clear();
  for (int32_t i = 6; i <= 10; i++) {
    values.push_back(std::to_string(i));
  }
  s = db.PfAdd("HLL", values, &update);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(update);

  s = db.PfCount(keys, &result);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(result, 10);

  int64_t nums = db.Del(keys);
  ASSERT_EQ(nums, 1);
}

TEST_F(HyperLogLogTest, PfMergeTest) {
  // PFMERGE results on the cardinality of union of sets
  bool update;
  std::vector<std::string> values1{"A", "B", "C"};
  s = db.PfAdd("HLL1", values1, &update);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(update);

  std::vector<std::string> values2{"B", "C", "D"};
  s = db.PfAdd("HLL2", values2, &update);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(update);

  std::vector<std::string> values3{"C", "D", "E"};
  s = db.PfAdd("HLL3", values3, &update);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(update);

  std::vector<std::string> keys{"HLL1", "HLL2", "HLL3"};
  std::string result_value;
  s = db.PfMerge(keys, result_value);
  ASSERT_TRUE(s.ok());
  int64_t result;
  s = db.PfCount(keys, &result);
  ASSERT_EQ(result, 5);

  std::map<storage::DataType, Status> type_status;
  int64_t nums = db.Del(keys);
  ASSERT_EQ(nums, 3);
}

TEST_F(HyperLogLogTest, MultipleKeysTest) {
  // PFCOUNT multiple-keys merge returns cardinality of union
  bool update;
  for (int32_t i = 1; i <= 10000; i++) {
    std::vector<std::string> hll1_value{"FOO" + std::to_string(i)};
    std::vector<std::string> hll2_value{"BAR" + std::to_string(i)};
    std::vector<std::string> hll3_value{"ZAP" + std::to_string(i)};
    s = db.PfAdd("HLL1", hll1_value, &update);
    ASSERT_TRUE(s.ok());

    s = db.PfAdd("HLL2", hll2_value, &update);
    ASSERT_TRUE(s.ok());

    s = db.PfAdd("HLL3", hll3_value, &update);
    ASSERT_TRUE(s.ok());
  }
  std::vector<std::string> keys{"HLL1", "HLL2", "HLL3"};
  int64_t result;
  s = db.PfCount(keys, &result);
  ASSERT_TRUE(s.ok());
  int32_t ratio_nums = abs(10000 * 3 - result);
  ASSERT_LT(ratio_nums, static_cast<double>(result / 100) * 5);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
