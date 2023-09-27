//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <dirent.h>
#include <gtest/gtest.h>
#include <unistd.h>
#include <iostream>
#include <iterator>
#include <thread>

#include "storage/storage.h"
#include "storage/util.h"

using namespace storage;

class HashesTest : public ::testing::Test {
 public:
  HashesTest() = default;
  ~HashesTest() override = default;

  void SetUp() override {
    std::string path = "./db/hashes";
    if (access(path.c_str(), F_OK) != 0) {
      mkdir(path.c_str(), 0755);
    }
    storage_options.options.create_if_missing = true;
#ifdef USE_S3
    auto& cloud_fs_opts = storage_options.cloud_fs_options;
    cloud_fs_opts.endpoint_override = "http://127.0.0.1:9000";
    cloud_fs_opts.credentials.InitializeSimple("minioadmin", "minioadmin");
    ASSERT_TRUE(cloud_fs_opts.credentials.HasValid().ok());
    cloud_fs_opts.src_bucket.SetBucketName("database.unit.test", "pika.");
    cloud_fs_opts.dest_bucket.SetBucketName("database.unit.test", "pika.");
    storage_options.options.max_log_file_size = 0;
#endif
    s = db.Open(storage_options, path);
    ASSERT_TRUE(s.ok());
  }

  void TearDown() override {
    std::string path = "./db/hashes";
    DeleteFiles(path.c_str());
  }

  static void SetUpTestSuite() {}
  static void TearDownTestSuite() {}

  StorageOptions storage_options;
  storage::Storage db;
  storage::Status s;
};

static bool field_value_match(storage::Storage* const db, const Slice& key,
                              const std::vector<FieldValue>& expect_field_value) {
  std::vector<FieldValue> field_value_out;
  Status s = db->HGetall(key, &field_value_out);
  if (!s.ok() && !s.IsNotFound()) {
    return false;
  }
  if (field_value_out.size() != expect_field_value.size()) {
    return false;
  }
  if (s.IsNotFound() && expect_field_value.empty()) {
    return true;
  }
  for (const auto& field_value : expect_field_value) {
    if (find(field_value_out.begin(), field_value_out.end(), field_value) == field_value_out.end()) {
      return false;
    }
  }
  return true;
}

static bool field_value_match(const std::vector<FieldValue>& field_value_out,
                              const std::vector<FieldValue>& expect_field_value) {
  if (field_value_out.size() != expect_field_value.size()) {
    return false;
  }
  for (const auto& field_value : expect_field_value) {
    if (find(field_value_out.begin(), field_value_out.end(), field_value) == field_value_out.end()) {
      return false;
    }
  }
  return true;
}

static bool size_match(storage::Storage* const db, const Slice& key, int32_t expect_size) {
  int32_t size = 0;
  Status s = db->HLen(key, &size);
  if (!s.ok() && !s.IsNotFound()) {
    return false;
  }
  if (s.IsNotFound() && (expect_size == 0)) {
    return true;
  }
  return size == expect_size;
}

static bool make_expired(storage::Storage* const db, const Slice& key) {
  std::map<storage::DataType, rocksdb::Status> type_status;
  int ret = db->Expire(key, 1, &type_status);
  if ((ret == 0) || !type_status[storage::DataType::kHashes].ok()) {
    return false;
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  return true;
}

// HDel
TEST_F(HashesTest, HDel) {
  int32_t ret = 0;
  std::vector<storage::FieldValue> fvs;
  fvs.push_back({"TEST_FIELD1", "TEST_VALUE1"});
  fvs.push_back({"TEST_FIELD2", "TEST_VALUE2"});
  fvs.push_back({"TEST_FIELD3", "TEST_VALUE3"});
  fvs.push_back({"TEST_FIELD4", "TEST_VALUE4"});

  s = db.HMSet("HDEL_KEY", fvs);
  ASSERT_TRUE(s.ok());

  std::vector<std::string> fields{"TEST_FIELD1", "TEST_FIELD2", "TEST_FIELD3", "TEST_FIElD2", "TEST_NOT_EXIST_FIELD"};
  s = db.HDel("HDEL_KEY", fields, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  s = db.HLen("HDEL_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  // Delete not exist hash table
  s = db.HDel("HDEL_NOT_EXIST_KEY", fields, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);

  // Delete timeout hash table
  s = db.HMSet("HDEL_TIMEOUT_KEY", fvs);
  ASSERT_TRUE(s.ok());

  std::map<storage::DataType, rocksdb::Status> type_status;
  db.Expire("HDEL_TIMEOUT_KEY", 1, &type_status);
  ASSERT_TRUE(type_status[storage::DataType::kHashes].ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  s = db.HDel("HDEL_TIMEOUT_KEY", fields, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
}

// HExists
TEST_F(HashesTest, HExistsTest) {
  int32_t ret;
  s = db.HSet("HEXIST_KEY", "HEXIST_FIELD", "HEXIST_VALUE", &ret);
  ASSERT_TRUE(s.ok());

  s = db.HExists("HEXIST_KEY", "HEXIST_FIELD");
  ASSERT_TRUE(s.ok());

  // If key does not exist.
  s = db.HExists("HEXIST_NOT_EXIST_KEY", "HEXIST_FIELD");
  ASSERT_TRUE(s.IsNotFound());

  // If field is not present in the hash
  s = db.HExists("HEXIST_KEY", "HEXIST_NOT_EXIST_FIELD");
  ASSERT_TRUE(s.IsNotFound());
}

// HGet
TEST_F(HashesTest, HGetTest) {
  int32_t ret = 0;
  std::string value;
  s = db.HSet("HGET_KEY", "HGET_TEST_FIELD", "HGET_TEST_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.HGet("HGET_KEY", "HGET_TEST_FIELD", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "HGET_TEST_VALUE");

  // If key does not exist.
  s = db.HGet("HGET_NOT_EXIST_KEY", "HGET_TEST_FIELD", &value);
  ASSERT_TRUE(s.IsNotFound());

  // If field is not present in the hash
  s = db.HGet("HGET_KEY", "HGET_NOT_EXIST_FIELD", &value);
  ASSERT_TRUE(s.IsNotFound());
}

// HGetall
TEST_F(HashesTest, HGetall) {
  int32_t ret = 0;
  std::vector<storage::FieldValue> mid_fvs_in;
  mid_fvs_in.push_back({"MID_TEST_FIELD1", "MID_TEST_VALUE1"});
  mid_fvs_in.push_back({"MID_TEST_FIELD2", "MID_TEST_VALUE2"});
  mid_fvs_in.push_back({"MID_TEST_FIELD3", "MID_TEST_VALUE3"});
  s = db.HMSet("B_HGETALL_KEY", mid_fvs_in);
  ASSERT_TRUE(s.ok());

  std::vector<storage::FieldValue> fvs_out;
  s = db.HGetall("B_HGETALL_KEY", &fvs_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(fvs_out.size(), 3);
  ASSERT_EQ(fvs_out[0].field, "MID_TEST_FIELD1");
  ASSERT_EQ(fvs_out[0].value, "MID_TEST_VALUE1");
  ASSERT_EQ(fvs_out[1].field, "MID_TEST_FIELD2");
  ASSERT_EQ(fvs_out[1].value, "MID_TEST_VALUE2");
  ASSERT_EQ(fvs_out[2].field, "MID_TEST_FIELD3");
  ASSERT_EQ(fvs_out[2].value, "MID_TEST_VALUE3");

  // Insert some kv who's position above "mid kv"
  std::vector<storage::FieldValue> pre_fvs_in;
  pre_fvs_in.push_back({"PRE_TEST_FIELD1", "PRE_TEST_VALUE1"});
  pre_fvs_in.push_back({"PRE_TEST_FIELD2", "PRE_TEST_VALUE2"});
  pre_fvs_in.push_back({"PRE_TEST_FIELD3", "PRE_TEST_VALUE3"});
  s = db.HMSet("A_HGETALL_KEY", pre_fvs_in);
  ASSERT_TRUE(s.ok());
  fvs_out.clear();
  s = db.HGetall("B_HGETALL_KEY", &fvs_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(fvs_out.size(), 3);
  ASSERT_EQ(fvs_out[0].field, "MID_TEST_FIELD1");
  ASSERT_EQ(fvs_out[0].value, "MID_TEST_VALUE1");
  ASSERT_EQ(fvs_out[1].field, "MID_TEST_FIELD2");
  ASSERT_EQ(fvs_out[1].value, "MID_TEST_VALUE2");
  ASSERT_EQ(fvs_out[2].field, "MID_TEST_FIELD3");
  ASSERT_EQ(fvs_out[2].value, "MID_TEST_VALUE3");

  // Insert some kv who's position below "mid kv"
  std::vector<storage::FieldValue> suf_fvs_in;
  suf_fvs_in.push_back({"SUF_TEST_FIELD1", "SUF_TEST_VALUE1"});
  suf_fvs_in.push_back({"SUF_TEST_FIELD2", "SUF_TEST_VALUE2"});
  suf_fvs_in.push_back({"SUF_TEST_FIELD3", "SUF_TEST_VALUE3"});
  s = db.HMSet("C_HGETALL_KEY", suf_fvs_in);
  ASSERT_TRUE(s.ok());
  fvs_out.clear();
  s = db.HGetall("B_HGETALL_KEY", &fvs_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(fvs_out.size(), 3);
  ASSERT_EQ(fvs_out[0].field, "MID_TEST_FIELD1");
  ASSERT_EQ(fvs_out[0].value, "MID_TEST_VALUE1");
  ASSERT_EQ(fvs_out[1].field, "MID_TEST_FIELD2");
  ASSERT_EQ(fvs_out[1].value, "MID_TEST_VALUE2");
  ASSERT_EQ(fvs_out[2].field, "MID_TEST_FIELD3");
  ASSERT_EQ(fvs_out[2].value, "MID_TEST_VALUE3");

  // HGetall timeout hash table
  fvs_out.clear();
  std::map<storage::DataType, rocksdb::Status> type_status;
  db.Expire("B_HGETALL_KEY", 1, &type_status);
  ASSERT_TRUE(type_status[storage::DataType::kHashes].ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  s = db.HGetall("B_HGETALL_KEY", &fvs_out);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(fvs_out.size(), 0);

  // HGetall not exist hash table
  fvs_out.clear();
  s = db.HGetall("HGETALL_NOT_EXIST_KEY", &fvs_out);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(fvs_out.size(), 0);
}

// HIncrby
TEST_F(HashesTest, HIncrby) {
  int32_t ret;
  int64_t value;
  std::string str_value;

  // ***************** Group 1 Test *****************
  s = db.HSet("GP1_HINCRBY_KEY", "GP1_HINCRBY_FIELD", "1", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.HIncrby("GP1_HINCRBY_KEY", "GP1_HINCRBY_FIELD", 1, &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, 2);

  // ***************** Group 2 Test *****************
  s = db.HSet("GP2_HINCRBY_KEY", "GP2_HINCRBY_FIELD", " 1", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.HIncrby("GP2_HINCRBY_KEY", "GP2_HINCRBY_FIELD", 1, &value);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_EQ(value, 0);

  // ***************** Group 3 Test *****************
  s = db.HSet("GP3_HINCRBY_KEY", "GP3_HINCRBY_FIELD", "1 ", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.HIncrby("GP3_HINCRBY_KEY", "GP3_HINCRBY_FIELD", 1, &value);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_EQ(value, 0);

  // If key does not exist the value is set to 0 before the
  // operation is performed
  s = db.HIncrby("HINCRBY_NEW_KEY", "HINCRBY_EXIST_FIELD", 1000, &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, 1000);
  s = db.HGet("HINCRBY_NEW_KEY", "HINCRBY_EXIST_FIELD", &str_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(atoll(str_value.data()), 1000);

  // If the hash field contains a string that can not be
  // represented as integer
  s = db.HSet("HINCRBY_KEY", "HINCRBY_STR_FIELD", "HINCRBY_VALEU", &ret);
  ASSERT_TRUE(s.ok());
  s = db.HIncrby("HINCRBY_KEY", "HINCRBY_STR_FIELD", 100, &value);
  ASSERT_TRUE(s.IsCorruption());

  // If field does not exist the value is set to 0 before the
  // operation is performed
  s = db.HIncrby("HINCRBY_KEY", "HINCRBY_NOT_EXIST_FIELD", 100, &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, 100);
  s = db.HGet("HINCRBY_KEY", "HINCRBY_NOT_EXIST_FIELD", &str_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(atoll(str_value.data()), 100);

  s = db.HSet("HINCRBY_KEY", "HINCRBY_NUM_FIELD", "100", &ret);
  ASSERT_TRUE(s.ok());

  // Positive test
  s = db.HIncrby("HINCRBY_KEY", "HINCRBY_NUM_FIELD", 100, &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, 200);
  s = db.HGet("HINCRBY_KEY", "HINCRBY_NUM_FIELD", &str_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(atoll(str_value.data()), 200);

  // Negative test
  s = db.HIncrby("HINCRBY_KEY", "HINCRBY_NUM_FIELD", -100, &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, 100);
  s = db.HGet("HINCRBY_KEY", "HINCRBY_NUM_FIELD", &str_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(atoll(str_value.data()), 100);

  // Larger than the maximum number 9223372036854775807
  s = db.HSet("HINCRBY_KEY", "HINCRBY_NUM_FIELD", "10", &ret);
  ASSERT_TRUE(s.ok());
  s = db.HIncrby("HINCRBY_KEY", "HINCRBY_NUM_FIELD", 9223372036854775807, &value);
  ASSERT_TRUE(s.IsInvalidArgument());

  // Less than the minimum number -9223372036854775808
  s = db.HSet("HINCRBY_KEY", "HINCRBY_NUM_FIELD", "-10", &ret);
  ASSERT_TRUE(s.ok());
  s = db.HIncrby("HINCRBY_KEY", "HINCRBY_NUM_FIELD", -9223372036854775807, &value);
  ASSERT_TRUE(s.IsInvalidArgument());
}

// HIncrbyfloat
TEST_F(HashesTest, HIncrbyfloat) {
  int32_t ret;
  std::string new_value;

  // ***************** Group 1 Test *****************
  s = db.HSet("GP1_HINCRBYFLOAT_KEY", "GP1_HINCRBYFLOAT_FIELD", "1.234", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.HIncrbyfloat("GP1_HINCRBYFLOAT_KEY", "GP1_HINCRBYFLOAT_FIELD", "1.234", &new_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(new_value, "2.468");

  // ***************** Group 2 Test *****************
  s = db.HSet("GP2_HINCRBYFLOAT_KEY", "GP2_HINCRBYFLOAT_FIELD", " 1.234", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.HIncrbyfloat("GP2_HINCRBYFLOAT_KEY", "GP2_HINCRBYFLOAT_FIELD", "1.234", &new_value);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_EQ(new_value, "");

  // ***************** Group 3 Test *****************
  s = db.HSet("GP3_HINCRBYFLOAT_KEY", "GP3_HINCRBYFLOAT_FIELD", "1.234 ", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.HIncrbyfloat("GP3_HINCRBYFLOAT_KEY", "GP3_HINCRBYFLOAT_FIELD", "1.234", &new_value);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_EQ(new_value, "");

  // If the specified increment are not parsable as a double precision
  // floating point number
  s = db.HIncrbyfloat("HINCRBYFLOAT_KEY", "HINCRBYFLOAT_FIELD", "HINCRBYFLOAT_BY", &new_value);
  ASSERT_TRUE(s.IsCorruption());
  s = db.HGet("HINCRBYFLOAT_KEY", "HINCRBYFLOAT_FIELD", &new_value);
  ASSERT_TRUE(s.IsNotFound());

  // If key does not exist the value is set to 0 before the
  // operation is performed
  s = db.HIncrbyfloat("HINCRBYFLOAT_KEY", "HINCRBYFLOAT_FIELD", "12.3456", &new_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(new_value, "12.3456");
  s = db.HGet("HINCRBYFLOAT_KEY", "HINCRBYFLOAT_FIELD", &new_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(new_value, "12.3456");
  s = db.HLen("HINCRBYFLOAT_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  // If the current field content are not parsable as a double precision
  // floating point number
  s = db.HSet("HINCRBYFLOAT_KEY", "HINCRBYFLOAT_STR_FIELD", "HINCRBYFLOAT_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  s = db.HIncrbyfloat("HINCRBYFLOAT_KEY", "HINCRBYFLOAT_STR_FIELD", "123.456", &new_value);
  ASSERT_TRUE(s.IsCorruption());
  s = db.HLen("HINCRBYFLOAT_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);

  // If field does not exist the value is set to 0 before the
  // operation is performed
  s = db.HIncrbyfloat("HINCRBYFLOAT_KEY", "HINCRBYFLOAT_NOT_EXIST_FIELD", "65.4321000", &new_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(new_value, "65.4321");
  s = db.HGet("HINCRBYFLOAT_KEY", "HINCRBYFLOAT_NOT_EXIST_FIELD", &new_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(new_value, "65.4321");
  s = db.HLen("HINCRBYFLOAT_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  s = db.HSet("HINCRBYFLOAT_KEY", "HINCRBYFLOAT_NUM_FIELD", "1000", &ret);
  ASSERT_TRUE(s.ok());

  // Positive test
  s = db.HIncrbyfloat("HINCRBYFLOAT_KEY", "HINCRBYFLOAT_NUM_FIELD", "+123.456789", &new_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(new_value, "1123.456789");
  s = db.HGet("HINCRBYFLOAT_KEY", "HINCRBYFLOAT_NUM_FIELD", &new_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(new_value, "1123.456789");

  // Negative test
  s = db.HIncrbyfloat("HINCRBYFLOAT_KEY", "HINCRBYFLOAT_NUM_FIELD", "-123.456789", &new_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(new_value, "1000");
  s = db.HGet("HINCRBYFLOAT_KEY", "HINCRBYFLOAT_NUM_FIELD", &new_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(new_value, "1000");

  s = db.HLen("HINCRBYFLOAT_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);

  // ***** Special test *****
  // case 1
  s = db.HIncrbyfloat("HINCRBYFLOAT_KEY", "HINCRBYFLOAT_SP_FIELD1", "2.0e2", &new_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(new_value, "200");
  s = db.HGet("HINCRBYFLOAT_KEY", "HINCRBYFLOAT_SP_FIELD1", &new_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(new_value, "200");

  // case2
  s = db.HIncrbyfloat("HINCRBYFLOAT_KEY", "HINCRBYFLOAT_SP_FIELD2", "5.0e3", &new_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(new_value, "5000");
  s = db.HIncrbyfloat("HINCRBYFLOAT_KEY", "HINCRBYFLOAT_SP_FIELD2", "2.0e2", &new_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(new_value, "5200");
  s = db.HGet("HINCRBYFLOAT_KEY", "HINCRBYFLOAT_SP_FIELD2", &new_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(new_value, "5200");

  // case 3
  s = db.HIncrbyfloat("HINCRBYFLOAT_KEY", "HINCRBYFLOAT_SP_FIELD3", "5.0e3", &new_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(new_value, "5000");
  s = db.HIncrbyfloat("HINCRBYFLOAT_KEY", "HINCRBYFLOAT_SP_FIELD3", "-2.0e2", &new_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(new_value, "4800");
  s = db.HGet("HINCRBYFLOAT_KEY", "HINCRBYFLOAT_SP_FIELD3", &new_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(new_value, "4800");

  // case 4
  s = db.HIncrbyfloat("HINCRBYFLOAT_KEY", "HINCRBYFLOAT_SP_FIELD4", ".456789", &new_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(new_value, "0.456789");
  s = db.HGet("HINCRBYFLOAT_KEY", "HINCRBYFLOAT_SP_FIELD4", &new_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(new_value, "0.456789");

  // case5
  s = db.HIncrbyfloat("HINCRBYFLOAT_KEY", "HINCRBYFLOAT_SP_FIELD5", "-.456789", &new_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(new_value, "-0.456789");
  s = db.HGet("HINCRBYFLOAT_KEY", "HINCRBYFLOAT_SP_FIELD5", &new_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(new_value, "-0.456789");

  // case6
  s = db.HIncrbyfloat("HINCRBYFLOAT_KEY", "HINCRBYFLOAT_SP_FIELD6", "+.456789", &new_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(new_value, "0.456789");
  s = db.HGet("HINCRBYFLOAT_KEY", "HINCRBYFLOAT_SP_FIELD6", &new_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(new_value, "0.456789");

  // case7
  s = db.HIncrbyfloat("HINCRBYFLOAT_KEY", "HINCRBYFLOAT_SP_FIELD7", "+.456789", &new_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(new_value, "0.456789");
  s = db.HIncrbyfloat("HINCRBYFLOAT_KEY", "HINCRBYFLOAT_SP_FIELD7", "-.456789", &new_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(new_value, "0");
  s = db.HGet("HINCRBYFLOAT_KEY", "HINCRBYFLOAT_SP_FIELD7", &new_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(new_value, "0");

  // case8
  s = db.HIncrbyfloat("HINCRBYFLOAT_KEY", "HINCRBYFLOAT_SP_FIELD8", "-00000.456789000", &new_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(new_value, "-0.456789");
  s = db.HGet("HINCRBYFLOAT_KEY", "HINCRBYFLOAT_SP_FIELD8", &new_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(new_value, "-0.456789");

  // case9
  s = db.HIncrbyfloat("HINCRBYFLOAT_KEY", "HINCRBYFLOAT_SP_FIELD9", "+00000.456789000", &new_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(new_value, "0.456789");
  s = db.HGet("HINCRBYFLOAT_KEY", "HINCRBYFLOAT_SP_FIELD9", &new_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(new_value, "0.456789");

  s = db.HLen("HINCRBYFLOAT_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 13);
}

// HKeys
TEST_F(HashesTest, HKeys) {
  int32_t ret = 0;
  std::vector<storage::FieldValue> mid_fvs_in;
  mid_fvs_in.push_back({"MID_TEST_FIELD1", "MID_TEST_VALUE1"});
  mid_fvs_in.push_back({"MID_TEST_FIELD2", "MID_TEST_VALUE2"});
  mid_fvs_in.push_back({"MID_TEST_FIELD3", "MID_TEST_VALUE3"});
  s = db.HMSet("B_HKEYS_KEY", mid_fvs_in);
  ASSERT_TRUE(s.ok());

  std::vector<std::string> fields;
  s = db.HKeys("B_HKEYS_KEY", &fields);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(fields.size(), 3);
  ASSERT_EQ(fields[0], "MID_TEST_FIELD1");
  ASSERT_EQ(fields[1], "MID_TEST_FIELD2");
  ASSERT_EQ(fields[2], "MID_TEST_FIELD3");

  // Insert some kv who's position above "mid kv"
  std::vector<storage::FieldValue> pre_fvs_in;
  pre_fvs_in.push_back({"PRE_TEST_FIELD1", "PRE_TEST_VALUE1"});
  pre_fvs_in.push_back({"PRE_TEST_FIELD2", "PRE_TEST_VALUE2"});
  pre_fvs_in.push_back({"PRE_TEST_FIELD3", "PRE_TEST_VALUE3"});
  s = db.HMSet("A_HKEYS_KEY", pre_fvs_in);
  ASSERT_TRUE(s.ok());
  fields.clear();
  s = db.HKeys("B_HKEYS_KEY", &fields);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(fields.size(), 3);
  ASSERT_EQ(fields[0], "MID_TEST_FIELD1");
  ASSERT_EQ(fields[1], "MID_TEST_FIELD2");
  ASSERT_EQ(fields[2], "MID_TEST_FIELD3");

  // Insert some kv who's position below "mid kv"
  std::vector<storage::FieldValue> suf_fvs_in;
  suf_fvs_in.push_back({"SUF_TEST_FIELD1", "SUF_TEST_VALUE1"});
  suf_fvs_in.push_back({"SUF_TEST_FIELD2", "SUF_TEST_VALUE2"});
  suf_fvs_in.push_back({"SUF_TEST_FIELD3", "SUF_TEST_VALUE3"});
  s = db.HMSet("A_HKEYS_KEY", suf_fvs_in);
  ASSERT_TRUE(s.ok());
  fields.clear();
  s = db.HKeys("B_HKEYS_KEY", &fields);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(fields.size(), 3);
  ASSERT_EQ(fields[0], "MID_TEST_FIELD1");
  ASSERT_EQ(fields[1], "MID_TEST_FIELD2");
  ASSERT_EQ(fields[2], "MID_TEST_FIELD3");

  // HKeys timeout hash table
  fields.clear();
  std::map<storage::DataType, rocksdb::Status> type_status;
  db.Expire("B_HKEYS_KEY", 1, &type_status);
  ASSERT_TRUE(type_status[storage::DataType::kHashes].ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  s = db.HKeys("B_HKEYS_KEY", &fields);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(fields.size(), 0);

  // HKeys not exist hash table
  fields.clear();
  s = db.HKeys("HKEYS_NOT_EXIST_KEY", &fields);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(fields.size(), 0);
}

// HLen
TEST_F(HashesTest, HLenTest) {
  int32_t ret = 0;

  // ***************** Group 1 Test *****************
  std::vector<storage::FieldValue> fvs1;
  fvs1.push_back({"GP1_TEST_FIELD1", "GP1_TEST_VALUE1"});
  fvs1.push_back({"GP1_TEST_FIELD2", "GP1_TEST_VALUE2"});
  fvs1.push_back({"GP1_TEST_FIELD3", "GP1_TEST_VALUE3"});
  s = db.HMSet("GP1_HLEN_KEY", fvs1);
  ASSERT_TRUE(s.ok());

  s = db.HLen("GP1_HLEN_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  // ***************** Group 2 Test *****************
  std::vector<storage::FieldValue> fvs2;
  fvs2.push_back({"GP2_TEST_FIELD1", "GP2_TEST_VALUE1"});
  fvs2.push_back({"GP2_TEST_FIELD2", "GP2_TEST_VALUE2"});
  fvs2.push_back({"GP2_TEST_FIELD3", "GP2_TEST_VALUE3"});
  s = db.HMSet("GP2_HLEN_KEY", fvs2);
  ASSERT_TRUE(s.ok());

  s = db.HDel("GP2_HLEN_KEY", {"GP2_TEST_FIELD1", "GP2_TEST_FIELD2"}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);

  s = db.HLen("GP2_HLEN_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.HDel("GP2_HLEN_KEY", {"GP2_TEST_FIELD3"}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.HLen("GP2_HLEN_KEY", &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(ret, 0);
}

// HMGet
TEST_F(HashesTest, HMGetTest) {
  int32_t ret = 0;
  std::vector<storage::ValueStatus> vss;

  // ***************** Group 1 Test *****************
  std::vector<storage::FieldValue> fvs1;
  fvs1.push_back({"TEST_FIELD1", "TEST_VALUE1"});
  fvs1.push_back({"TEST_FIELD2", "TEST_VALUE2"});
  fvs1.push_back({"TEST_FIELD3", "TEST_VALUE3"});
  s = db.HMSet("GP1_HMGET_KEY", fvs1);
  ASSERT_TRUE(s.ok());

  s = db.HLen("GP1_HMGET_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> fields1{"TEST_FIELD1", "TEST_FIELD2", "TEST_FIELD3", "TEST_NOT_EXIST_FIELD"};
  s = db.HMGet("GP1_HMGET_KEY", fields1, &vss);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(vss.size(), 4);

  ASSERT_TRUE(vss[0].status.ok());
  ASSERT_EQ(vss[0].value, "TEST_VALUE1");
  ASSERT_TRUE(vss[1].status.ok());
  ASSERT_EQ(vss[1].value, "TEST_VALUE2");
  ASSERT_TRUE(vss[2].status.ok());
  ASSERT_EQ(vss[2].value, "TEST_VALUE3");
  ASSERT_TRUE(vss[3].status.IsNotFound());
  ASSERT_EQ(vss[3].value, "");

  // ***************** Group 2 Test *****************
  std::vector<storage::FieldValue> fvs2;
  fvs2.push_back({"TEST_FIELD1", "TEST_VALUE1"});
  fvs2.push_back({"TEST_FIELD2", ""});
  s = db.HMSet("GP2_HMGET_KEY", fvs2);
  ASSERT_TRUE(s.ok());

  s = db.HLen("GP2_HMGET_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);

  vss.clear();
  std::vector<std::string> fields2{"TEST_FIELD1", "TEST_FIELD2", "TEST_NOT_EXIST_FIELD"};
  s = db.HMGet("GP2_HMGET_KEY", fields2, &vss);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(vss.size(), 3);

  ASSERT_TRUE(vss[0].status.ok());
  ASSERT_EQ(vss[0].value, "TEST_VALUE1");
  ASSERT_TRUE(vss[1].status.ok());
  ASSERT_EQ(vss[1].value, "");
  ASSERT_TRUE(vss[2].status.IsNotFound());
  ASSERT_EQ(vss[2].value, "");

  // ***************** Group 3 Test *****************
  vss.clear();
  std::vector<std::string> fields3{"TEST_FIELD1", "TEST_FIELD2", "TEST_FIELD3"};
  s = db.HMGet("GP3_HMGET_KEY", fields3, &vss);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(vss.size(), 3);

  ASSERT_TRUE(vss[0].status.IsNotFound());
  ASSERT_EQ(vss[0].value, "");
  ASSERT_TRUE(vss[1].status.IsNotFound());
  ASSERT_EQ(vss[1].value, "");
  ASSERT_TRUE(vss[2].status.IsNotFound());
  ASSERT_EQ(vss[2].value, "");

  // ***************** Group 4 Test *****************
  std::vector<storage::FieldValue> fvs4;
  fvs4.push_back({"TEST_FIELD1", "TEST_VALUE1"});
  fvs4.push_back({"TEST_FIELD2", "TEST_VALUE2"});
  fvs4.push_back({"TEST_FIELD3", "TEST_VALUE3"});

  s = db.HMSet("GP4_HMGET_KEY", fvs4);
  ASSERT_TRUE(s.ok());

  ASSERT_TRUE(make_expired(&db, "GP4_HMGET_KEY"));

  vss.clear();
  std::vector<std::string> fields4{"TEST_FIELD1", "TEST_FIELD2", "TEST_FIELD3"};
  s = db.HMGet("GP4_HMGET_KEY", fields4, &vss);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(vss.size(), 3);

  ASSERT_TRUE(vss[0].status.IsNotFound());
  ASSERT_EQ(vss[0].value, "");
  ASSERT_TRUE(vss[1].status.IsNotFound());
  ASSERT_EQ(vss[1].value, "");
  ASSERT_TRUE(vss[2].status.IsNotFound());
  ASSERT_EQ(vss[2].value, "");
}

// HMSet
TEST_F(HashesTest, HMSetTest) {
  int32_t ret = 0;
  std::vector<storage::FieldValue> fvs1;
  fvs1.push_back({"TEST_FIELD1", "TEST_VALUE1"});
  fvs1.push_back({"TEST_FIELD2", "TEST_VALUE2"});

  // If field already exists in the hash, it is overwritten
  std::vector<storage::FieldValue> fvs2;
  fvs2.push_back({"TEST_FIELD2", "TEST_VALUE2"});
  fvs2.push_back({"TEST_FIELD3", "TEST_VALUE3"});
  fvs2.push_back({"TEST_FIELD4", "TEST_VALUE4"});
  fvs2.push_back({"TEST_FIELD3", "TEST_VALUE5"});

  s = db.HMSet("HMSET_KEY", fvs1);
  ASSERT_TRUE(s.ok());
  s = db.HLen("HMSET_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);

  s = db.HMSet("HMSET_KEY", fvs2);
  ASSERT_TRUE(s.ok());

  s = db.HLen("HMSET_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);

  std::vector<storage::ValueStatus> vss1;
  std::vector<std::string> fields1{"TEST_FIELD1", "TEST_FIELD2", "TEST_FIELD3", "TEST_FIELD4"};
  s = db.HMGet("HMSET_KEY", fields1, &vss1);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(vss1.size(), 4);

  ASSERT_EQ(vss1[0].value, "TEST_VALUE1");
  ASSERT_EQ(vss1[1].value, "TEST_VALUE2");
  ASSERT_EQ(vss1[2].value, "TEST_VALUE5");
  ASSERT_EQ(vss1[3].value, "TEST_VALUE4");

  std::map<storage::DataType, rocksdb::Status> type_status;
  db.Expire("HMSET_KEY", 1, &type_status);
  ASSERT_TRUE(type_status[storage::DataType::kHashes].ok());

  // The key has timeout
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  std::vector<storage::FieldValue> fvs3;
  fvs3.push_back({"TEST_FIELD3", "TEST_VALUE3"});
  fvs3.push_back({"TEST_FIELD4", "TEST_VALUE4"});
  fvs3.push_back({"TEST_FIELD5", "TEST_VALUE5"});
  s = db.HMSet("HMSET_KEY", fvs3);
  ASSERT_TRUE(s.ok());

  s = db.HLen("HMSET_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<storage::ValueStatus> vss2;
  std::vector<std::string> fields2{"TEST_FIELD3", "TEST_FIELD4", "TEST_FIELD5"};
  s = db.HMGet("HMSET_KEY", fields2, &vss2);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(vss2.size(), 3);

  ASSERT_EQ(vss2[0].value, "TEST_VALUE3");
  ASSERT_EQ(vss2[1].value, "TEST_VALUE4");
  ASSERT_EQ(vss2[2].value, "TEST_VALUE5");
}

// HSet
TEST_F(HashesTest, HSetTest) {
  int32_t ret = 0;
  std::string value;

  // ***************** Group 1 Test *****************
  // If field is a new field in the hash and value was set.
  s = db.HSet("GP1_HSET_KEY", "HSET_TEST_FIELD", "HSET_TEST_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.HLen("GP1_HSET_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.HGet("GP1_HSET_KEY", "HSET_TEST_FIELD", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "HSET_TEST_VALUE");

  // If field already exists in the hash and the value was updated.
  s = db.HSet("GP1_HSET_KEY", "HSET_TEST_FIELD", "HSET_TEST_NEW_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  s = db.HLen("GP1_HSET_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.HGet("GP1_HSET_KEY", "HSET_TEST_FIELD", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "HSET_TEST_NEW_VALUE");

  // If field already exists in the hash and the value was equal.
  s = db.HSet("GP1_HSET_KEY", "HSET_TEST_FIELD", "HSET_TEST_NEW_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  s = db.HLen("GP1_HSET_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.HGet("GP1_HSET_KEY", "HSET_TEST_FIELD", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "HSET_TEST_NEW_VALUE");

  // ***************** Group 2 Test *****************
  s = db.HSet("GP2_HSET_KEY", "HSET_TEST_FIELD", "HSET_TEST_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.HLen("GP2_HSET_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.HGet("GP2_HSET_KEY", "HSET_TEST_FIELD", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "HSET_TEST_VALUE");

  ASSERT_TRUE(make_expired(&db, "GP2_HSET_KEY"));

  s = db.HSet("GP2_HSET_KEY", "HSET_TEST_FIELD", "HSET_TEST_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.HLen("GP2_HSET_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.HGet("GP2_HSET_KEY", "HSET_TEST_FIELD", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "HSET_TEST_VALUE");

  // ***************** Group 3 Test *****************
  s = db.HSet("GP3_HSET_KEY", "HSET_TEST_FIELD", "HSET_TEST_NEW_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.HLen("GP3_HSET_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.HGet("GP3_HSET_KEY", "HSET_TEST_FIELD", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "HSET_TEST_NEW_VALUE");

  ASSERT_TRUE(make_expired(&db, "GP3_HSET_KEY"));

  s = db.HSet("GP3_HSET_KEY", "HSET_TEST_NEW_FIELD", "HSET_TEST_NEW_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.HLen("GP3_HSET_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.HGet("GP3_HSET_KEY", "HSET_TEST_NEW_FIELD", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "HSET_TEST_NEW_VALUE");
}

// HSetnx
TEST_F(HashesTest, HSetnxTest) {
  int32_t ret;
  std::string value;
  // If field is a new field in the hash and value was set.
  s = db.HSetnx("HSETNX_KEY", "HSETNX_TEST_FIELD", "HSETNX_TEST_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.HGet("HSETNX_KEY", "HSETNX_TEST_FIELD", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "HSETNX_TEST_VALUE");

  // If field already exists, this operation has no effect.
  s = db.HSetnx("HSETNX_KEY", "HSETNX_TEST_FIELD", "HSETNX_TEST_NEW_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  s = db.HGet("HSETNX_KEY", "HSETNX_TEST_FIELD", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "HSETNX_TEST_VALUE");
}

// HVals
TEST_F(HashesTest, HVals) {
  int32_t ret = 0;
  std::vector<storage::FieldValue> mid_fvs_in;
  mid_fvs_in.push_back({"MID_TEST_FIELD1", "MID_TEST_VALUE1"});
  mid_fvs_in.push_back({"MID_TEST_FIELD2", "MID_TEST_VALUE2"});
  mid_fvs_in.push_back({"MID_TEST_FIELD3", "MID_TEST_VALUE3"});
  s = db.HMSet("B_HVALS_KEY", mid_fvs_in);
  ASSERT_TRUE(s.ok());

  std::vector<std::string> values;
  s = db.HVals("B_HVALS_KEY", &values);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(values.size(), 3);
  ASSERT_EQ(values[0], "MID_TEST_VALUE1");
  ASSERT_EQ(values[1], "MID_TEST_VALUE2");
  ASSERT_EQ(values[2], "MID_TEST_VALUE3");

  // Insert some kv who's position above "mid kv"
  std::vector<storage::FieldValue> pre_fvs_in;
  pre_fvs_in.push_back({"PRE_TEST_FIELD1", "PRE_TEST_VALUE1"});
  pre_fvs_in.push_back({"PRE_TEST_FIELD2", "PRE_TEST_VALUE2"});
  pre_fvs_in.push_back({"PRE_TEST_FIELD3", "PRE_TEST_VALUE3"});
  s = db.HMSet("A_HVALS_KEY", pre_fvs_in);
  ASSERT_TRUE(s.ok());
  values.clear();
  s = db.HVals("B_HVALS_KEY", &values);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(values.size(), 3);
  ASSERT_EQ(values[0], "MID_TEST_VALUE1");
  ASSERT_EQ(values[1], "MID_TEST_VALUE2");
  ASSERT_EQ(values[2], "MID_TEST_VALUE3");

  // Insert some kv who's position below "mid kv"
  std::vector<storage::FieldValue> suf_fvs_in;
  suf_fvs_in.push_back({"SUF_TEST_FIELD1", "SUF_TEST_VALUE1"});
  suf_fvs_in.push_back({"SUF_TEST_FIELD2", "SUF_TEST_VALUE2"});
  suf_fvs_in.push_back({"SUF_TEST_FIELD3", "SUF_TEST_VALUE3"});
  s = db.HMSet("C_HVALS_KEY", suf_fvs_in);
  ASSERT_TRUE(s.ok());
  values.clear();
  s = db.HVals("B_HVALS_KEY", &values);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(values.size(), 3);
  ASSERT_EQ(values[0], "MID_TEST_VALUE1");
  ASSERT_EQ(values[1], "MID_TEST_VALUE2");
  ASSERT_EQ(values[2], "MID_TEST_VALUE3");

  // HVals timeout hash table
  values.clear();
  std::map<storage::DataType, rocksdb::Status> type_status;
  db.Expire("B_HVALS_KEY", 1, &type_status);
  ASSERT_TRUE(type_status[storage::DataType::kHashes].ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  s = db.HVals("B_HVALS_KEY", &values);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(values.size(), 0);

  // HVals not exist hash table
  values.clear();
  s = db.HVals("HVALS_NOT_EXIST_KEY", &values);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(values.size(), 0);
}

// HStrlen
TEST_F(HashesTest, HStrlenTest) {
  int32_t ret = 0;
  int32_t len = 0;
  s = db.HSet("HSTRLEN_KEY", "HSTRLEN_TEST_FIELD", "HSTRLEN_TEST_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.HStrlen("HSTRLEN_KEY", "HSTRLEN_TEST_FIELD", &len);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(len, 18);

  // If the key or the field do not exist, 0 is returned
  s = db.HStrlen("HSTRLEN_KEY", "HSTRLEN_NOT_EXIST_FIELD", &len);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(len, 0);
}

// HScan
TEST_F(HashesTest, HScanTest) {  // NOLINT
  int64_t cursor = 0;
  int64_t next_cursor = 0;
  std::vector<FieldValue> field_value_out;

  // ***************** Group 1 Test *****************
  // {a,v} {b,v} {c,v} {d,v} {e,v} {f,v} {g,v} {h,v}
  // 0     1     2     3     4     5     6     7
  std::vector<FieldValue> gp1_field_value{{"a", "v"}, {"b", "v"}, {"c", "v"}, {"d", "v"},
                                          {"e", "v"}, {"f", "v"}, {"g", "v"}, {"h", "v"}};
  s = db.HMSet("GP1_HSCAN_KEY", gp1_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP1_HSCAN_KEY", 8));

  s = db.HScan("GP1_HSCAN_KEY", 0, "*", 3, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 3);
  ASSERT_EQ(next_cursor, 3);
  ASSERT_TRUE(field_value_match(field_value_out, {{"a", "v"}, {"b", "v"}, {"c", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.HScan("GP1_HSCAN_KEY", cursor, "*", 3, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 3);
  ASSERT_EQ(next_cursor, 6);
  ASSERT_TRUE(field_value_match(field_value_out, {{"d", "v"}, {"e", "v"}, {"f", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.HScan("GP1_HSCAN_KEY", cursor, "*", 3, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 2);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(field_value_match(field_value_out, {{"g", "v"}, {"h", "v"}}));

  // ***************** Group 2 Test *****************
  // {a,v} {b,v} {c,v} {d,v} {e,v} {f,v} {g,v} {h,v}
  // 0     1     2     3     4     5     6     7
  std::vector<FieldValue> gp2_field_value{{"a", "v"}, {"b", "v"}, {"c", "v"}, {"d", "v"},
                                          {"e", "v"}, {"f", "v"}, {"g", "v"}, {"h", "v"}};
  s = db.HMSet("GP2_HSCAN_KEY", gp2_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP2_HSCAN_KEY", 8));

  field_value_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.HScan("GP2_HSCAN_KEY", cursor, "*", 1, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 1);
  ASSERT_TRUE(field_value_match(field_value_out, {{"a", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.HScan("GP2_HSCAN_KEY", cursor, "*", 1, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 2);
  ASSERT_TRUE(field_value_match(field_value_out, {{"b", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.HScan("GP2_HSCAN_KEY", cursor, "*", 1, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 3);
  ASSERT_TRUE(field_value_match(field_value_out, {{"c", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.HScan("GP2_HSCAN_KEY", cursor, "*", 1, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 4);
  ASSERT_TRUE(field_value_match(field_value_out, {{"d", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.HScan("GP2_HSCAN_KEY", cursor, "*", 1, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 5);
  ASSERT_TRUE(field_value_match(field_value_out, {{"e", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.HScan("GP2_HSCAN_KEY", cursor, "*", 1, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 6);
  ASSERT_TRUE(field_value_match(field_value_out, {{"f", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.HScan("GP2_HSCAN_KEY", cursor, "*", 1, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 7);
  ASSERT_TRUE(field_value_match(field_value_out, {{"g", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.HScan("GP2_HSCAN_KEY", cursor, "*", 1, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(field_value_match(field_value_out, {{"h", "v"}}));

  // ***************** Group 3 Test *****************
  // {a,v} {b,v} {c,v} {d,v} {e,v} {f,v} {g,v} {h,v}
  // 0     1     2     3     4     5     6     7
  std::vector<FieldValue> gp3_field_value{{"a", "v"}, {"b", "v"}, {"c", "v"}, {"d", "v"},
                                          {"e", "v"}, {"f", "v"}, {"g", "v"}, {"h", "v"}};
  s = db.HMSet("GP3_HSCAN_KEY", gp3_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP3_HSCAN_KEY", 8));

  field_value_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.HScan("GP3_HSCAN_KEY", cursor, "*", 5, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 5);
  ASSERT_EQ(next_cursor, 5);
  ASSERT_TRUE(field_value_match(field_value_out, {{"a", "v"}, {"b", "v"}, {"c", "v"}, {"d", "v"}, {"e", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.HScan("GP3_HSCAN_KEY", cursor, "*", 5, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 3);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(field_value_match(field_value_out, {{"f", "v"}, {"g", "v"}, {"h", "v"}}));

  // ***************** Group 4 Test *****************
  // {a,v} {b,v} {c,v} {d,v} {e,v} {f,v} {g,v} {h,v}
  // 0     1     2     3     4     5     6     7
  std::vector<FieldValue> gp4_field_value{{"a", "v"}, {"b", "v"}, {"c", "v"}, {"d", "v"},
                                          {"e", "v"}, {"f", "v"}, {"g", "v"}, {"h", "v"}};
  s = db.HMSet("GP4_HSCAN_KEY", gp4_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP4_HSCAN_KEY", 8));

  field_value_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.HScan("GP4_HSCAN_KEY", cursor, "*", 10, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 8);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(field_value_match(
      field_value_out,
      {{"a", "v"}, {"b", "v"}, {"c", "v"}, {"d", "v"}, {"e", "v"}, {"f", "v"}, {"g", "v"}, {"h", "v"}}));

  // ***************** Group 5 Test *****************
  // {a_1_,v} {a_2_,v} {a_3_,v} {b_1_,v} {b_2_,v} {b_3_,v} {c_1_,v} {c_2_,v} {c_3_,v}
  // 0        1        2        3        4        5        6        7        8
  std::vector<FieldValue> gp5_field_value{{"a_1_", "v"}, {"a_2_", "v"}, {"a_3_", "v"}, {"b_1_", "v"}, {"b_2_", "v"},
                                          {"b_3_", "v"}, {"c_1_", "v"}, {"c_2_", "v"}, {"c_3_", "v"}};
  s = db.HMSet("GP5_HSCAN_KEY", gp5_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP5_HSCAN_KEY", 9));

  field_value_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.HScan("GP5_HSCAN_KEY", cursor, "*1*", 3, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 3);
  ASSERT_TRUE(field_value_match(field_value_out, {{"a_1_", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.HScan("GP5_HSCAN_KEY", cursor, "*1*", 3, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 6);
  ASSERT_TRUE(field_value_match(field_value_out, {{"b_1_", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.HScan("GP5_HSCAN_KEY", cursor, "*1*", 3, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(field_value_match(field_value_out, {{"c_1_", "v"}}));

  // ***************** Group 6 Test *****************
  // {a_1_,v} {a_2_,v} {a_3_,v} {b_1_,v} {b_2_,v} {b_3_,v} {c_1_,v} {c_2_,v} {c_3_,v}
  // 0        1        2        3        4        5        6        7        8
  std::vector<FieldValue> gp6_field_value{{"a_1_", "v"}, {"a_2_", "v"}, {"a_3_", "v"}, {"b_1_", "v"}, {"b_2_", "v"},
                                          {"b_3_", "v"}, {"c_1_", "v"}, {"c_2_", "v"}, {"c_3_", "v"}};
  s = db.HMSet("GP6_HSCAN_KEY", gp6_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP6_HSCAN_KEY", 9));

  field_value_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.HScan("GP6_HSCAN_KEY", cursor, "a*", 3, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 3);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(field_value_match(field_value_out, {{"a_1_", "v"}, {"a_2_", "v"}, {"a_3_", "v"}}));

  field_value_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.HScan("GP6_HSCAN_KEY", cursor, "a*", 2, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 2);
  ASSERT_EQ(next_cursor, 2);
  ASSERT_TRUE(field_value_match(field_value_out, {{"a_1_", "v"}, {"a_2_", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.HScan("GP6_HSCAN_KEY", cursor, "a*", 2, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(field_value_match(field_value_out, {{"a_3_", "v"}}));

  field_value_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.HScan("GP6_HSCAN_KEY", cursor, "a*", 1, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 1);
  ASSERT_TRUE(field_value_match(field_value_out, {{"a_1_", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.HScan("GP6_HSCAN_KEY", cursor, "a*", 1, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 2);
  ASSERT_TRUE(field_value_match(field_value_out, {{"a_2_", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.HScan("GP6_HSCAN_KEY", cursor, "a*", 1, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(field_value_match(field_value_out, {{"a_3_", "v"}}));

  // ***************** Group 7 Test *****************
  // {a_1_,v} {a_2_,v} {a_3_,v} {b_1_,v} {b_2_,v} {b_3_,v} {c_1_,v} {c_2_,v} {c_3_,v}
  // 0        1        2        3        4        5        6        7        8
  std::vector<FieldValue> gp7_field_value{{"a_1_", "v"}, {"a_2_", "v"}, {"a_3_", "v"}, {"b_1_", "v"}, {"b_2_", "v"},
                                          {"b_3_", "v"}, {"c_1_", "v"}, {"c_2_", "v"}, {"c_3_", "v"}};
  s = db.HMSet("GP7_HSCAN_KEY", gp7_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP7_HSCAN_KEY", 9));

  field_value_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.HScan("GP7_HSCAN_KEY", cursor, "b*", 3, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 3);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(field_value_match(field_value_out, {{"b_1_", "v"}, {"b_2_", "v"}, {"b_3_", "v"}}));

  field_value_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.HScan("GP7_HSCAN_KEY", cursor, "b*", 2, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 2);
  ASSERT_EQ(next_cursor, 2);
  ASSERT_TRUE(field_value_match(field_value_out, {{"b_1_", "v"}, {"b_2_", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.HScan("GP7_HSCAN_KEY", cursor, "b*", 2, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(field_value_match(field_value_out, {{"b_3_", "v"}}));

  field_value_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.HScan("GP7_HSCAN_KEY", cursor, "b*", 1, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 1);
  ASSERT_TRUE(field_value_match(field_value_out, {{"b_1_", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.HScan("GP7_HSCAN_KEY", cursor, "b*", 1, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 2);
  ASSERT_TRUE(field_value_match(field_value_out, {{"b_2_", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.HScan("GP7_HSCAN_KEY", cursor, "b*", 1, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(field_value_match(field_value_out, {{"b_3_", "v"}}));

  // ***************** Group 8 Test *****************
  // {a_1_,v} {a_2_,v} {a_3_,v} {b_1_,v} {b_2_,v} {b_3_,v} {c_1_,v} {c_2_,v} {c_3_,v}
  // 0        1        2        3        4        5        6        7        8
  std::vector<FieldValue> gp8_field_value{{"a_1_", "v"}, {"a_2_", "v"}, {"a_3_", "v"}, {"b_1_", "v"}, {"b_2_", "v"},
                                          {"b_3_", "v"}, {"c_1_", "v"}, {"c_2_", "v"}, {"c_3_", "v"}};
  s = db.HMSet("GP8_HSCAN_KEY", gp8_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP8_HSCAN_KEY", 9));

  field_value_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.HScan("GP8_HSCAN_KEY", cursor, "c*", 3, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 3);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(field_value_match(field_value_out, {{"c_1_", "v"}, {"c_2_", "v"}, {"c_3_", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.HScan("GP8_HSCAN_KEY", cursor, "c*", 2, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 2);
  ASSERT_EQ(next_cursor, 2);
  ASSERT_TRUE(field_value_match(field_value_out, {{"c_1_", "v"}, {"c_2_", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.HScan("GP8_HSCAN_KEY", cursor, "c*", 2, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(field_value_match(field_value_out, {{"c_3_", "v"}}));

  field_value_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.HScan("GP8_HSCAN_KEY", cursor, "c*", 1, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 1);
  ASSERT_TRUE(field_value_match(field_value_out, {{"c_1_", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.HScan("GP8_HSCAN_KEY", cursor, "c*", 1, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 2);
  ASSERT_TRUE(field_value_match(field_value_out, {{"c_2_", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.HScan("GP8_HSCAN_KEY", cursor, "c*", 1, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(field_value_match(field_value_out, {{"c_3_", "v"}}));

  // ***************** Group 9 Test *****************
  // {a_1_,v} {a_2_,v} {a_3_,v} {b_1_,v} {b_2_,v} {b_3_,v} {c_1_,v} {c_2_,v} {c_3_,v}
  // 0        1        2        3        4        5        6        7        8
  std::vector<FieldValue> gp9_field_value{{"a_1_", "v"}, {"a_2_", "v"}, {"a_3_", "v"}, {"b_1_", "v"}, {"b_2_", "v"},
                                          {"b_3_", "v"}, {"c_1_", "v"}, {"c_2_", "v"}, {"c_3_", "v"}};
  s = db.HMSet("GP9_HSCAN_KEY", gp9_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP9_HSCAN_KEY", 9));

  field_value_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.HScan("GP9_HSCAN_KEY", cursor, "d*", 3, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 0);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(field_value_match(field_value_out, {}));

  // ***************** Group 10 Test *****************
  // {a_1_,v} {a_2_,v} {a_3_,v} {b_1_,v} {b_2_,v} {b_3_,v} {c_1_,v} {c_2_,v} {c_3_,v}
  // 0        1        2        3        4        5        6        7        8
  std::vector<FieldValue> gp10_field_value{{"a_1_", "v"}, {"a_2_", "v"}, {"a_3_", "v"}, {"b_1_", "v"}, {"b_2_", "v"},
                                           {"b_3_", "v"}, {"c_1_", "v"}, {"c_2_", "v"}, {"c_3_", "v"}};
  s = db.HMSet("GP10_HSCAN_KEY", gp10_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP10_HSCAN_KEY", 9));

  ASSERT_TRUE(make_expired(&db, "GP10_HSCAN_KEY"));
  field_value_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.HScan("GP10_HSCAN_KEY", cursor, "*", 10, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(field_value_out.size(), 0);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(field_value_match(field_value_out, {}));

  // ***************** Group 11 Test *****************
  // HScan Not Exist Key
  field_value_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.HScan("GP11_HSCAN_KEY", cursor, "*", 10, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(field_value_out.size(), 0);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(field_value_match(field_value_out, {}));
}

// HScanx
TEST_F(HashesTest, HScanxTest) {
  std::string start_field;
  std::string next_field;
  std::vector<FieldValue> field_value_out;

  // ***************** Group 1 Test *****************
  // {a,v} {b,v} {c,v} {d,v} {e,v} {f,v} {g,v} {h,v}
  // 0     1     2     3     4     5     6     7
  std::vector<FieldValue> gp1_field_value{{"a", "v"}, {"b", "v"}, {"c", "v"}, {"d", "v"},
                                          {"e", "v"}, {"f", "v"}, {"g", "v"}, {"h", "v"}};
  s = db.HMSet("GP1_HSCANX_KEY", gp1_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP1_HSCANX_KEY", 8));

  s = db.HScanx("GP1_HSCANX_KEY", "", "*", 3, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 3);
  ASSERT_EQ(next_field, "d");
  ASSERT_TRUE(field_value_match(field_value_out, {{"a", "v"}, {"b", "v"}, {"c", "v"}}));

  field_value_out.clear();
  start_field = next_field;
  s = db.HScanx("GP1_HSCANX_KEY", start_field, "*", 3, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 3);
  ASSERT_EQ(next_field, "g");
  ASSERT_TRUE(field_value_match(field_value_out, {{"d", "v"}, {"e", "v"}, {"f", "v"}}));

  field_value_out.clear();
  start_field = next_field;
  s = db.HScanx("GP1_HSCANX_KEY", start_field, "*", 3, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 2);
  ASSERT_EQ(next_field, "");
  ASSERT_TRUE(field_value_match(field_value_out, {{"g", "v"}, {"h", "v"}}));

  // ***************** Group 2 Test *****************
  // {a,v} {b,v} {c,v} {d,v} {e,v} {f,v} {g,v} {h,v}
  // 0     1     2     3     4     5     6     7
  std::vector<FieldValue> gp2_field_value{{"a", "v"}, {"b", "v"}, {"c", "v"}, {"d", "v"},
                                          {"e", "v"}, {"f", "v"}, {"g", "v"}, {"h", "v"}};
  s = db.HMSet("GP2_HSCANX_KEY", gp2_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP2_HSCANX_KEY", 8));

  s = db.HScanx("GP2_HSCANX_KEY", "a", "*", 3, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 3);
  ASSERT_EQ(next_field, "d");
  ASSERT_TRUE(field_value_match(field_value_out, {{"a", "v"}, {"b", "v"}, {"c", "v"}}));

  field_value_out.clear();
  start_field = next_field;
  s = db.HScanx("GP2_HSCANX_KEY", start_field, "*", 3, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 3);
  ASSERT_EQ(next_field, "g");
  ASSERT_TRUE(field_value_match(field_value_out, {{"d", "v"}, {"e", "v"}, {"f", "v"}}));

  field_value_out.clear();
  start_field = next_field;
  s = db.HScanx("GP2_HSCANX_KEY", start_field, "*", 3, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 2);
  ASSERT_EQ(next_field, "");
  ASSERT_TRUE(field_value_match(field_value_out, {{"g", "v"}, {"h", "v"}}));

  // ***************** Group 3 Test *****************
  // {a,v} {b,v} {c,v} {d,v} {e,v} {f,v} {g,v} {h,v}
  // 0     1     2     3     4     5     6     7
  std::vector<FieldValue> gp3_field_value{{"a", "v"}, {"b", "v"}, {"c", "v"}, {"d", "v"},
                                          {"e", "v"}, {"f", "v"}, {"g", "v"}, {"h", "v"}};
  s = db.HMSet("GP3_HSCANX_KEY", gp3_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP3_HSCANX_KEY", 8));

  field_value_out.clear();
  s = db.HScanx("GP3_HSCANX_KEY", "a", "*", 10, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 8);
  ASSERT_EQ(next_field, "");
  ASSERT_TRUE(field_value_match(
      field_value_out,
      {{"a", "v"}, {"b", "v"}, {"c", "v"}, {"d", "v"}, {"e", "v"}, {"f", "v"}, {"g", "v"}, {"h", "v"}}));

  // ***************** Group 4 Test *****************
  // {a_f1,v} {a_f2,v} {a_f3,v} {b_f1,v} {b_f2,v} {b_f3,v} {c_f1,v} {c_f2,v}, {c_f3, v}
  // 0        1        2        3        4        5        6        7         8
  std::vector<FieldValue> gp4_field_value{{"a_f1", "v"}, {"a_f2", "v"}, {"a_f3", "v"}, {"b_f1", "v"}, {"b_f2", "v"},
                                          {"b_f3", "v"}, {"c_f1", "v"}, {"c_f2", "v"}, {"c_f3", "v"}};
  s = db.HMSet("GP4_HSCANX_KEY", gp4_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP4_HSCANX_KEY", 9));

  field_value_out.clear();
  s = db.HScanx("GP4_HSCANX_KEY", "a_", "*", 3, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 3);
  ASSERT_EQ(next_field, "b_f1");
  ASSERT_TRUE(field_value_match(field_value_out, {{"a_f1", "v"}, {"a_f2", "v"}, {"a_f3", "v"}}));

  field_value_out.clear();
  s = db.HScanx("GP4_HSCANX_KEY", "b_", "*", 3, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 3);
  ASSERT_EQ(next_field, "c_f1");
  ASSERT_TRUE(field_value_match(field_value_out, {{"b_f1", "v"}, {"b_f2", "v"}, {"b_f3", "v"}}));

  field_value_out.clear();
  s = db.HScanx("GP4_HSCANX_KEY", "c_", "*", 3, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 3);
  ASSERT_EQ(next_field, "");
  ASSERT_TRUE(field_value_match(field_value_out, {{"c_f1", "v"}, {"c_f2", "v"}, {"c_f3", "v"}}));

  field_value_out.clear();
  s = db.HScanx("GP4_HSCANX_KEY", "d_", "*", 3, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 0);
  ASSERT_EQ(next_field, "");
  ASSERT_TRUE(field_value_match(field_value_out, {}));

  // ***************** Group 5 Test *****************
  // {a_f1,v} {a_f2,v} {a_f3,v} {b_f1,v} {b_f2,v} {b_f3,v} {c_f1,v} {c_f2,v}, {c_f3, v}
  // 0        1        2        3        4        5        6        7         8
  std::vector<FieldValue> gp5_field_value{{"a_f1", "v"}, {"a_f2", "v"}, {"a_f3", "v"}, {"b_f1", "v"}, {"b_f2", "v"},
                                          {"b_f3", "v"}, {"c_f1", "v"}, {"c_f2", "v"}, {"c_f3", "v"}};
  s = db.HMSet("GP5_HSCANX_KEY", gp5_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP5_HSCANX_KEY", 9));

  field_value_out.clear();
  s = db.HScanx("GP5_HSCANX_KEY", "a_", "a_*", 5, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 3);
  ASSERT_EQ(next_field, "b_f3");
  ASSERT_TRUE(field_value_match(field_value_out, {{"a_f1", "v"}, {"a_f2", "v"}, {"a_f3", "v"}}));

  field_value_out.clear();
  s = db.HScanx("GP5_HSCANX_KEY", "b_", "b_*", 5, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 3);
  ASSERT_EQ(next_field, "c_f3");
  ASSERT_TRUE(field_value_match(field_value_out, {{"b_f1", "v"}, {"b_f2", "v"}, {"b_f3", "v"}}));

  field_value_out.clear();
  s = db.HScanx("GP5_HSCANX_KEY", "c_", "c_*", 5, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 3);
  ASSERT_EQ(next_field, "");
  ASSERT_TRUE(field_value_match(field_value_out, {{"c_f1", "v"}, {"c_f2", "v"}, {"c_f3", "v"}}));

  field_value_out.clear();
  s = db.HScanx("GP5_HSCANX_KEY", "d_", "d_", 5, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 0);
  ASSERT_EQ(next_field, "");
  ASSERT_TRUE(field_value_match(field_value_out, {}));

  // ***************** Group 6 Test *****************
  // {a_f1,v} {a_f2,v} {a_f3,v} {b_f1,v} {b_f2,v} {b_f3,v} {c_f1,v} {c_f2,v}, {c_f3, v}
  // 0        1        2        3        4        5        6        7         8
  std::vector<FieldValue> gp6_field_value{{"a_f1", "v"}, {"a_f2", "v"}, {"a_f3", "v"}, {"b_f1", "v"}, {"b_f2", "v"},
                                          {"b_f3", "v"}, {"c_f1", "v"}, {"c_f2", "v"}, {"c_f3", "v"}};
  s = db.HMSet("GP6_HSCANX_KEY", gp6_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP6_HSCANX_KEY", 9));

  field_value_out.clear();
  s = db.HScanx("GP6_HSCANX_KEY", "a_", "a_*", 5, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 3);
  ASSERT_EQ(next_field, "b_f3");
  ASSERT_TRUE(field_value_match(field_value_out, {{"a_f1", "v"}, {"a_f2", "v"}, {"a_f3", "v"}}));

  field_value_out.clear();
  s = db.HScanx("GP6_HSCANX_KEY", "b_", "b_*", 5, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 3);
  ASSERT_EQ(next_field, "c_f3");
  ASSERT_TRUE(field_value_match(field_value_out, {{"b_f1", "v"}, {"b_f2", "v"}, {"b_f3", "v"}}));

  field_value_out.clear();
  s = db.HScanx("GP6_HSCANX_KEY", "c_", "c_*", 5, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 3);
  ASSERT_EQ(next_field, "");
  ASSERT_TRUE(field_value_match(field_value_out, {{"c_f1", "v"}, {"c_f2", "v"}, {"c_f3", "v"}}));

  field_value_out.clear();
  s = db.HScanx("GP6_HSCANX_KEY", "d_", "d_", 5, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 0);
  ASSERT_EQ(next_field, "");
  ASSERT_TRUE(field_value_match(field_value_out, {}));

  // ***************** Group 7 Test *****************
  // {a_f1,v} {a_f2,v} {a_f3,v} {b_f1,v} {b_f2,v} {b_f3,v} {c_f1,v} {c_f2,v}, {c_f3, v}
  // 0        1        2        3        4        5        6        7         8
  std::vector<FieldValue> gp7_field_value{{"a_f1", "v"}, {"a_f2", "v"}, {"a_f3", "v"}, {"b_f1", "v"}, {"b_f2", "v"},
                                          {"b_f3", "v"}, {"c_f1", "v"}, {"c_f2", "v"}, {"c_f3", "v"}};
  s = db.HMSet("GP7_HSCANX_KEY", gp7_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP7_HSCANX_KEY", 9));

  field_value_out.clear();
  s = db.HScanx("GP7_HSCANX_KEY", "a_", "*f2", 3, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_field, "b_f1");
  ASSERT_TRUE(field_value_match(field_value_out, {{"a_f2", "v"}}));

  start_field = next_field;
  field_value_out.clear();
  s = db.HScanx("GP7_HSCANX_KEY", start_field, "*f2", 3, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_field, "c_f1");
  ASSERT_TRUE(field_value_match(field_value_out, {{"b_f2", "v"}}));

  start_field = next_field;
  field_value_out.clear();
  s = db.HScanx("GP7_HSCANX_KEY", start_field, "*f2", 3, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_field, "");
  ASSERT_TRUE(field_value_match(field_value_out, {{"c_f2", "v"}}));

  // ***************** Group 8 Test *****************
  // {a_f1,v} {a_f2,v} {a_f3,v} {b_f1,v} {b_f2,v} {b_f3,v} {c_f1,v} {c_f2,v}, {c_f3, v}
  // 0        1        2        3        4        5        6        7         8
  std::vector<FieldValue> gp8_field_value{{"a_f1", "v"}, {"a_f2", "v"}, {"a_f3", "v"}, {"b_f1", "v"}, {"b_f2", "v"},
                                          {"b_f3", "v"}, {"c_f1", "v"}, {"c_f2", "v"}, {"c_f3", "v"}};
  s = db.HMSet("GP8_HSCANX_KEY", gp8_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP8_HSCANX_KEY", 9));

  field_value_out.clear();
  s = db.HScanx("GP8_HSCANX_KEY", "a_", "*n*", 3, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 0);
  ASSERT_EQ(next_field, "b_f1");
  ASSERT_TRUE(field_value_match(field_value_out, {}));

  start_field = next_field;
  field_value_out.clear();
  s = db.HScanx("GP8_HSCANX_KEY", start_field, "*n*", 3, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 0);
  ASSERT_EQ(next_field, "c_f1");
  ASSERT_TRUE(field_value_match(field_value_out, {}));

  start_field = next_field;
  field_value_out.clear();
  s = db.HScanx("GP8_HSCANX_KEY", start_field, "*n*", 3, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 0);
  ASSERT_EQ(next_field, "");
  ASSERT_TRUE(field_value_match(field_value_out, {}));

  // ***************** Group 9 Test *****************
  // {a_f1,v} {a_f2,v} {a_f3,v} {b_f1,v} {b_f2,v} {b_f3,v} {c_f1,v} {c_f2,v}, {c_f3, v}
  // 0        1        2        3        4        5        6        7         8
  std::vector<FieldValue> gp9_field_value{{"a_f1", "v"}, {"a_f2", "v"}, {"a_f3", "v"}, {"b_f1", "v"}, {"b_f2", "v"},
                                          {"b_f3", "v"}, {"c_f1", "v"}, {"c_f2", "v"}, {"c_f3", "v"}};
  s = db.HMSet("GP9_HSCANX_KEY", gp9_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP9_HSCANX_KEY", 9));

  ASSERT_TRUE(make_expired(&db, "GP9_HSCANX_KEY"));

  field_value_out.clear();
  s = db.HScanx("GP9_HSCANX_KEY", "", "*", 10, &field_value_out, &next_field);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(field_value_out.size(), 0);
  ASSERT_EQ(next_field, "");
  ASSERT_TRUE(field_value_match(field_value_out, {}));

  // ***************** Group 10 Test *****************
  // {a_f1,v} {a_f2,v} {a_f3,v} {b_f1,v} {b_f2,v} {b_f3,v} {c_f1,v} {c_f2,v}, {c_f3, v}
  // 0        1        2        3        4        5        6        7         8
  std::vector<FieldValue> gp10_field_value{{"a_f1", "v"}, {"a_f2", "v"}, {"a_f3", "v"}, {"b_f1", "v"}, {"b_f2", "v"},
                                           {"b_f3", "v"}, {"c_f1", "v"}, {"c_f2", "v"}, {"c_f3", "v"}};

  field_value_out.clear();
  s = db.HScanx("GP10_HSCANX_KEY", "", "*", 10, &field_value_out, &next_field);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(field_value_out.size(), 0);
  ASSERT_EQ(next_field, "");
  ASSERT_TRUE(field_value_match(field_value_out, {}));

  // ***************** Group 11 Test *****************
  // {a_f1,v} {a_f2,v} {a_f3,v} {b_f1,v} {b_f2,v} {b_f3,v} {c_f1,v} {c_f2,v}, {c_f3, v}
  // 0        1        2        3        4        5        6        7         8
  std::vector<FieldValue> gp11_field_value{{"a_f1", "v"}, {"a_f2", "v"}, {"a_f3", "v"}, {"b_f1", "v"}, {"b_f2", "v"},
                                           {"b_f3", "v"}, {"c_f1", "v"}, {"c_f2", "v"}, {"c_f3", "v"}};
  std::vector<std::string> gp11_del_field{"a_f1", "a_f2", "a_f3", "b_f1", "b_f2", "b_f3", "c_f1", "c_f2", "c_f3"};

  s = db.HMSet("GP11_HSCANX_KEY", gp9_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP11_HSCANX_KEY", 9));

  int32_t ret = 0;
  s = db.HDel("GP11_HSCANX_KEY", gp11_del_field, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  field_value_out.clear();
  s = db.HScanx("GP11_HSCANX_KEY", "", "*", 10, &field_value_out, &next_field);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(field_value_out.size(), 0);
  ASSERT_EQ(next_field, "");
  ASSERT_TRUE(field_value_match(field_value_out, {}));

  // ***************** Group 12 Test *****************
  // {aaa,v} {aab,v} {aac,v} {aad,v} {aaf,v} {aba,v} {abb,v} {abc,v}, {abd, v}, {abf, v}
  // 0       1       2       3       4        5      6       7        8         9
  std::vector<FieldValue> gp12_field_value{{"aaa", "v"}, {"aab", "v"}, {"aac", "v"}, {"aad", "v"}, {"aaf", "v"},
                                           {"aba", "v"}, {"abb", "v"}, {"abc", "v"}, {"abd", "v"}, {"abf", "v"}};

  s = db.HMSet("GP12_HSCANX_KEY", gp12_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP12_HSCANX_KEY", 10));

  field_value_out.clear();
  s = db.HScanx("GP12_HSCANX_KEY", "aa", "ab*", 3, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 0);
  ASSERT_EQ(next_field, "aad");
  ASSERT_TRUE(field_value_match(field_value_out, {}));

  field_value_out.clear();
  s = db.HScanx("GP12_HSCANX_KEY", "aad", "ab*", 3, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_field, "abb");
  ASSERT_TRUE(field_value_match(field_value_out, {{"aba", "v"}}));

  field_value_out.clear();
  s = db.HScanx("GP12_HSCANX_KEY", "abb", "ab*", 3, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 3);
  ASSERT_EQ(next_field, "abf");
  ASSERT_TRUE(field_value_match(field_value_out, {{"abb", "v"}, {"abc", "v"}, {"abd", "v"}}));

  field_value_out.clear();
  s = db.HScanx("GP12_HSCANX_KEY", "abf", "ab*", 3, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_field, "");
  ASSERT_TRUE(field_value_match(field_value_out, {{"abf", "v"}}));

  field_value_out.clear();
  s = db.HScanx("GP12_HSCANX_KEY", "aa", "ab*", 5, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 0);
  ASSERT_EQ(next_field, "aba");
  ASSERT_TRUE(field_value_match(field_value_out, {}));

  field_value_out.clear();
  s = db.HScanx("GP12_HSCANX_KEY", "aba", "ab*", 5, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 5);
  ASSERT_EQ(next_field, "");
  ASSERT_TRUE(
      field_value_match(field_value_out, {{"aba", "v"}, {"abb", "v"}, {"abc", "v"}, {"abd", "v"}, {"abf", "v"}}));
}

// PKHScanRange
TEST_F(HashesTest, PKHScanRangeTest) {
  int32_t ret;
  std::string start_field;
  std::string next_field;
  std::vector<FieldValue> field_value_out;
  std::vector<FieldValue> expect_field_value;

  // ************************** Group 1 Test **************************
  //        0     1     2     3     4     5     6     7     8     9
  //        a     c     e     g     i     k     m     o     q     s
  //      ^                                                         ^
  // field_start                                           field_end/next_field
  std::vector<FieldValue> gp1_field_value{{"a", "v"}, {"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"},
                                          {"k", "v"}, {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  s = db.HMSet("GP1_PKHSCANRANGE_KEY", gp1_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP1_PKHSCANRANGE_KEY", 10));

  field_value_out.clear();
  expect_field_value.clear();
  s = db.PKHScanRange("GP1_PKHSCANRANGE_KEY", "", "", "*", 10, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 0; idx <= 9; ++idx) {
    expect_field_value.push_back(gp1_field_value[idx]);
  }
  ASSERT_TRUE(field_value_match(field_value_out, expect_field_value));
  ASSERT_EQ(next_field, "");

  // ************************** Group 2 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      a     c     e     g     i     k     m     o     q     s
  //         ^                                                    ^
  //    field_start                                      field_end/next_field
  std::vector<FieldValue> gp2_field_value{{"a", "v"}, {"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"},
                                          {"k", "v"}, {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  s = db.HMSet("GP2_PKHSCANRANGE_KEY", gp2_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP2_PKHSCANRANGE_KEY", 10));

  field_value_out.clear();
  expect_field_value.clear();
  s = db.PKHScanRange("GP2_PKHSCANRANGE_KEY", "b", "", "*", 10, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 9; ++idx) {
    expect_field_value.push_back(gp2_field_value[idx]);
  }
  ASSERT_TRUE(field_value_match(field_value_out, expect_field_value));
  ASSERT_EQ(next_field, "");

  // ************************** Group 3 Test **************************
  //        0     1     2     3     4     5     6     7     8     9
  //        a     c     e     g     i     k     m     o     q     s
  //      ^                                                    ^
  // field_start                                          field_end
  std::vector<FieldValue> gp3_field_value{{"a", "v"}, {"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"},
                                          {"k", "v"}, {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  s = db.HMSet("GP3_PKHSCANRANGE_KEY", gp3_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP3_PKHSCANRANGE_KEY", 10));

  field_value_out.clear();
  expect_field_value.clear();
  s = db.PKHScanRange("GP3_PKHSCANRANGE_KEY", "", "r", "*", 10, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 0; idx <= 8; ++idx) {
    expect_field_value.push_back(gp3_field_value[idx]);
  }
  ASSERT_TRUE(field_value_match(field_value_out, expect_field_value));
  ASSERT_EQ(next_field, "");

  // ************************** Group 4 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      a     c     e     g     i     k     m     o     q     s
  //               ^                                   ^
  //          field_start                         field_end
  std::vector<FieldValue> gp4_field_value{{"a", "v"}, {"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"},
                                          {"k", "v"}, {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  s = db.HMSet("GP4_PKHSCANRANGE_KEY", gp4_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP4_PKHSCANRANGE_KEY", 10));

  field_value_out.clear();
  expect_field_value.clear();
  s = db.PKHScanRange("GP4_PKHSCANRANGE_KEY", "d", "p", "*", 10, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 2; idx <= 7; ++idx) {
    expect_field_value.push_back(gp4_field_value[idx]);
  }
  ASSERT_TRUE(field_value_match(field_value_out, expect_field_value));
  ASSERT_EQ(next_field, "");

  // ************************** Group 5 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      a     c     e     g     i     k     m     o     q     s
  //            ^                                         ^
  //       field_start                               field_end
  std::vector<FieldValue> gp5_field_value{{"a", "v"}, {"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"},
                                          {"k", "v"}, {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  s = db.HMSet("GP5_PKHSCANRANGE_KEY", gp5_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP5_PKHSCANRANGE_KEY", 10));

  field_value_out.clear();
  expect_field_value.clear();
  s = db.PKHScanRange("GP5_PKHSCANRANGE_KEY", "c", "q", "*", 10, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 8; ++idx) {
    expect_field_value.push_back(gp5_field_value[idx]);
  }
  ASSERT_TRUE(field_value_match(field_value_out, expect_field_value));
  ASSERT_EQ(next_field, "");

  // ************************** Group 6 Test **************************
  //      0     1     2     3     4       5     6     7     8     9
  //      a     c     e     g     i       k     m     o     q     s
  //                              ^       ^
  //                    field_start  field_end
  std::vector<FieldValue> gp6_field_value{{"a", "v"}, {"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"},
                                          {"k", "v"}, {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  s = db.HMSet("GP6_PKHSCANRANGE_KEY", gp6_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP6_PKHSCANRANGE_KEY", 10));

  field_value_out.clear();
  expect_field_value.clear();
  s = db.PKHScanRange("GP6_PKHSCANRANGE_KEY", "i", "k", "*", 10, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 4; idx <= 5; ++idx) {
    expect_field_value.push_back(gp6_field_value[idx]);
  }
  ASSERT_TRUE(field_value_match(field_value_out, expect_field_value));
  ASSERT_EQ(next_field, "");

  // ************************** Group 7 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      a     c     e     g     i     k     m     o     q     s
  //                              ^
  //                   field_start/field_end
  std::vector<FieldValue> gp7_field_value{{"a", "v"}, {"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"},
                                          {"k", "v"}, {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  s = db.HMSet("GP7_PKHSCANRANGE_KEY", gp7_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP7_PKHSCANRANGE_KEY", 10));

  field_value_out.clear();
  expect_field_value.clear();
  s = db.PKHScanRange("GP7_PKHSCANRANGE_KEY", "i", "i", "*", 10, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 4; idx <= 4; ++idx) {
    expect_field_value.push_back(gp7_field_value[idx]);
  }
  ASSERT_TRUE(field_value_match(field_value_out, expect_field_value));
  ASSERT_EQ(next_field, "");

  // ************************** Group 8 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      a     c     e     g     i     k     m     o     q     s
  //                              ^     ^
  //                      field_end     field_start
  std::vector<FieldValue> gp8_field_value{{"a", "v"}, {"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"},
                                          {"k", "v"}, {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  s = db.HMSet("GP8_PKHSCANRANGE_KEY", gp8_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP8_PKHSCANRANGE_KEY", 10));

  field_value_out.clear();
  expect_field_value.clear();
  s = db.PKHScanRange("GP8_PKHSCANRANGE_KEY", "k", "i", "*", 10, &field_value_out, &next_field);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_TRUE(field_value_match(field_value_out, expect_field_value));
  ASSERT_EQ(next_field, "");

  // ************************** Group 9 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      a     c     e     g     i     k     m     o     q     s
  //            ^                             ^           ^
  //       field_start                    next_field field_end
  std::vector<FieldValue> gp9_field_value{{"a", "v"}, {"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"},
                                          {"k", "v"}, {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  s = db.HMSet("GP9_PKHSCANRANGE_KEY", gp9_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP9_PKHSCANRANGE_KEY", 10));

  field_value_out.clear();
  expect_field_value.clear();
  s = db.PKHScanRange("GP9_PKHSCANRANGE_KEY", "c", "q", "*", 5, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 5; ++idx) {
    expect_field_value.push_back(gp7_field_value[idx]);
  }
  ASSERT_TRUE(field_value_match(field_value_out, expect_field_value));
  ASSERT_EQ(next_field, "m");

  // ************************** Group 10 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      a     c     e     g     i     k     m     o     q     s
  //            ^           ^                       ^     ^
  //       field_start   deleted           next_field     field_end
  std::vector<FieldValue> gp10_field_value{{"a", "v"}, {"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"},
                                           {"k", "v"}, {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  s = db.HMSet("GP10_PKHSCANRANGE_KEY", gp10_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP10_PKHSCANRANGE_KEY", 10));
  s = db.HDel("GP10_PKHSCANRANGE_KEY", {"g"}, &ret);
  ASSERT_TRUE(s.ok());

  field_value_out.clear();
  expect_field_value.clear();
  s = db.PKHScanRange("GP10_PKHSCANRANGE_KEY", "c", "q", "*", 5, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 6; ++idx) {
    if (idx != 3) {
      expect_field_value.push_back(gp10_field_value[idx]);
    }
  }
  ASSERT_TRUE(field_value_match(field_value_out, expect_field_value));
  ASSERT_EQ(next_field, "o");

  // ************************** Group 11 Test **************************
  //      0     1     2     3     4     5     6     7     8
  //     a_f1  a_f2  a_f3  b_f1  b_f2  b_f3  c_f1  c_f2  c_f3
  //      ^                 ^                             ^
  // field_start        next_field                   field_end
  std::vector<FieldValue> gp11_field_value{{"a_f1", "v"}, {"a_f2", "v"}, {"a_f3", "v"}, {"b_f1", "v"}, {"b_f2", "v"},
                                           {"b_f3", "v"}, {"c_f1", "v"}, {"c_f2", "v"}, {"c_f3", "v"}};
  s = db.HMSet("GP11_PKHSCANRANGE_KEY", gp11_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP11_PKHSCANRANGE_KEY", 9));
  ASSERT_TRUE(s.ok());

  field_value_out.clear();
  expect_field_value.clear();
  s = db.PKHScanRange("GP11_PKHSCANRANGE_KEY", "a_f1", "c_f3", "*f1", 3, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 0; idx <= 2; ++idx) {
    if (idx != 1 && idx != 2) {
      expect_field_value.push_back(gp11_field_value[idx]);
    }
  }
  ASSERT_TRUE(field_value_match(field_value_out, expect_field_value));
  ASSERT_EQ(next_field, "b_f1");

  // ************************** Group 12 Test **************************
  //           0     1     2     3     4     5     6     7     8
  //      a    c     e     g     i     k     m     o     q     s
  //      ^                                  ^     ^
  // field_start                    next_field     field_end
  std::vector<FieldValue> gp12_field_value_a{{"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"}, {"k", "v"},
                                             {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  std::vector<FieldValue> gp12_field_value_b{{"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"}, {"k", "v"},
                                             {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  std::vector<FieldValue> gp12_field_value_c{{"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"}, {"k", "v"},
                                             {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  s = db.HMSet("GP12_PKHSCANRANGE_KEY_A", gp12_field_value_a);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP12_PKHSCANRANGE_KEY_A", 9));
  s = db.HMSet("GP12_PKHSCANRANGE_KEY_B", gp12_field_value_b);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP12_PKHSCANRANGE_KEY_B", 9));
  s = db.HMSet("GP12_PKHSCANRANGE_KEY_C", gp12_field_value_c);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP12_PKHSCANRANGE_KEY_C", 9));

  field_value_out.clear();
  expect_field_value.clear();
  s = db.PKHScanRange("GP12_PKHSCANRANGE_KEY_B", "a", "o", "*", 5, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 0; idx <= 4; ++idx) {
    expect_field_value.push_back(gp12_field_value_b[idx]);
  }
  ASSERT_TRUE(field_value_match(field_value_out, expect_field_value));
  ASSERT_EQ(next_field, "m");

  // ************************** Group 13 Test **************************
  //          0     1     2     3     4     5     6     7     8
  //          c     e     g     i     k     m     o     q     s
  //                ^                             ^     ^
  //            field_start              next_field     field_end
  std::vector<FieldValue> gp13_field_value_a{{"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"}, {"k", "v"},
                                             {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  std::vector<FieldValue> gp13_field_value_b{{"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"}, {"k", "v"},
                                             {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  std::vector<FieldValue> gp13_field_value_c{{"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"}, {"k", "v"},
                                             {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  s = db.HMSet("GP13_PKHSCANRANGE_KEY_A", gp13_field_value_a);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP13_PKHSCANRANGE_KEY_A", 9));
  s = db.HMSet("GP13_PKHSCANRANGE_KEY_B", gp13_field_value_b);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP13_PKHSCANRANGE_KEY_B", 9));
  s = db.HMSet("GP13_PKHSCANRANGE_KEY_C", gp13_field_value_c);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP13_PKHSCANRANGE_KEY_C", 9));

  field_value_out.clear();
  expect_field_value.clear();
  s = db.PKHScanRange("GP13_PKHSCANRANGE_KEY_B", "e", "q", "*", 5, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 1; idx <= 5; ++idx) {
    expect_field_value.push_back(gp13_field_value_b[idx]);
  }
  ASSERT_TRUE(field_value_match(field_value_out, expect_field_value));
  ASSERT_EQ(next_field, "o");

  // ************************** Group 14 Test **************************
  //  0     1     2     3     4     5     6     7     8
  //  c     e     g     i     k     m     o     q     s    u
  //                                                       ^            ^
  //                                             field_start  next_field/field_end
  std::vector<FieldValue> gp14_field_value_a{{"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"}, {"k", "v"},
                                             {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  std::vector<FieldValue> gp14_field_value_b{{"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"}, {"k", "v"},
                                             {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  std::vector<FieldValue> gp14_field_value_c{{"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"}, {"k", "v"},
                                             {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  s = db.HMSet("GP14_PKHSCANRANGE_KEY_A", gp14_field_value_a);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP14_PKHSCANRANGE_KEY_A", 9));
  s = db.HMSet("GP14_PKHSCANRANGE_KEY_B", gp14_field_value_b);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP14_PKHSCANRANGE_KEY_B", 9));
  s = db.HMSet("GP14_PKHSCANRANGE_KEY_C", gp14_field_value_c);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP14_PKHSCANRANGE_KEY_C", 9));

  field_value_out.clear();
  expect_field_value.clear();
  s = db.PKHScanRange("GP14_PKHSCANRANGE_KEY_B", "u", "", "*", 5, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(field_value_match(field_value_out, expect_field_value));
  ASSERT_EQ(next_field, "");
}

// PKHRScanRange
TEST_F(HashesTest, PKHRScanRangeTest) {
  int32_t ret;
  std::string start_field;
  std::string next_field;
  std::vector<FieldValue> field_value_out;
  std::vector<FieldValue> expect_field_value;

  // ************************** Group 1 Test **************************
  //            0     1     2     3     4     5     6     7     8     9
  //            a     c     e     g     i     k     m     o     q     s
  //          ^                                                         ^
  // field_end/next_field                                          field_start
  std::vector<FieldValue> gp1_field_value{{"a", "v"}, {"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"},
                                          {"k", "v"}, {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  s = db.HMSet("GP1_PKHRSCANRANGE_KEY", gp1_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP1_PKHRSCANRANGE_KEY", 10));

  field_value_out.clear();
  expect_field_value.clear();
  s = db.PKHScanRange("GP1_PKHRSCANRANGE_KEY", "", "", "*", 10, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 9; idx >= 0; --idx) {
    expect_field_value.push_back(gp1_field_value[idx]);
  }
  ASSERT_TRUE(field_value_match(field_value_out, expect_field_value));
  ASSERT_EQ(next_field, "");

  // ************************** Group 2 Test **************************
  //         0     1     2     3     4     5     6     7     8     9
  //         a     c     e     g     i     k     m     o     q     s
  //            ^                                                    ^
  //       field_end                                            field_start
  std::vector<FieldValue> gp2_field_value{{"a", "v"}, {"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"},
                                          {"k", "v"}, {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  s = db.HMSet("GP2_PKHRSCANRANGE_KEY", gp2_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP2_PKHRSCANRANGE_KEY", 10));

  field_value_out.clear();
  expect_field_value.clear();
  s = db.PKHRScanRange("GP2_PKHRSCANRANGE_KEY", "", "b", "*", 10, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 9; idx >= 1; --idx) {
    expect_field_value.push_back(gp2_field_value[idx]);
  }
  ASSERT_TRUE(field_value_match(field_value_out, expect_field_value));
  ASSERT_EQ(next_field, "");

  // ************************** Group 3 Test **************************
  //            0     1     2     3     4     5     6     7     8     9
  //            a     c     e     g     i     k     m     o     q     s
  //          ^                                                    ^
  // field_end/next_field                                     field_start
  std::vector<FieldValue> gp3_field_value{{"a", "v"}, {"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"},
                                          {"k", "v"}, {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  s = db.HMSet("GP3_PKHRSCANRANGE_KEY", gp3_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP3_PKHRSCANRANGE_KEY", 10));

  field_value_out.clear();
  expect_field_value.clear();
  s = db.PKHRScanRange("GP3_PKHRSCANRANGE_KEY", "r", "", "*", 10, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 0; --idx) {
    expect_field_value.push_back(gp3_field_value[idx]);
  }
  ASSERT_TRUE(field_value_match(field_value_out, expect_field_value));
  ASSERT_EQ(next_field, "");

  // ************************** Group 4 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      a     c     e     g     i     k     m     o     q     s
  //               ^                                   ^
  //          field_end                           field_start
  std::vector<FieldValue> gp4_field_value{{"a", "v"}, {"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"},
                                          {"k", "v"}, {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  s = db.HMSet("GP4_PKHRSCANRANGE_KEY", gp4_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP4_PKHRSCANRANGE_KEY", 10));

  field_value_out.clear();
  expect_field_value.clear();
  s = db.PKHRScanRange("GP4_PKHRSCANRANGE_KEY", "p", "d", "*", 10, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 7; idx >= 2; --idx) {
    expect_field_value.push_back(gp4_field_value[idx]);
  }
  ASSERT_TRUE(field_value_match(field_value_out, expect_field_value));
  ASSERT_EQ(next_field, "");

  // ************************** Group 5 Test **************************
  //         0     1     2     3     4     5     6     7     8     9
  //         a     c     e     g     i     k     m     o     q     s
  //               ^                                         ^
  //          field_end                                 field_start
  std::vector<FieldValue> gp5_field_value{{"a", "v"}, {"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"},
                                          {"k", "v"}, {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  s = db.HMSet("GP5_PKHRSCANRANGE_KEY", gp5_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP5_PKHRSCANRANGE_KEY", 10));

  field_value_out.clear();
  expect_field_value.clear();
  s = db.PKHRScanRange("GP5_PKHRSCANRANGE_KEY", "q", "c", "*", 10, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 1; --idx) {
    expect_field_value.push_back(gp5_field_value[idx]);
  }
  ASSERT_TRUE(field_value_match(field_value_out, expect_field_value));
  ASSERT_EQ(next_field, "");

  // ************************** Group 6 Test **************************
  //      0     1     2     3      4     5     6     7     8     9
  //      a     c     e     g      i     k     m     o     q     s
  //                               ^     ^
  //                       field_end     field_start
  std::vector<FieldValue> gp6_field_value{{"a", "v"}, {"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"},
                                          {"k", "v"}, {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  s = db.HMSet("GP6_PKHRSCANRANGE_KEY", gp6_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP6_PKHRSCANRANGE_KEY", 10));

  field_value_out.clear();
  expect_field_value.clear();
  s = db.PKHRScanRange("GP6_PKHRSCANRANGE_KEY", "k", "i", "*", 10, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 5; idx >= 4; --idx) {
    expect_field_value.push_back(gp6_field_value[idx]);
  }
  ASSERT_TRUE(field_value_match(field_value_out, expect_field_value));
  ASSERT_EQ(next_field, "");

  // ************************** Group 7 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      a     c     e     g     i     k     m     o     q     s
  //                              ^
  //                  field_start/field_end
  std::vector<FieldValue> gp7_field_value{{"a", "v"}, {"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"},
                                          {"k", "v"}, {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  s = db.HMSet("GP7_PKHRSCANRANGE_KEY", gp7_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP7_PKHRSCANRANGE_KEY", 10));

  field_value_out.clear();
  expect_field_value.clear();
  s = db.PKHRScanRange("GP7_PKHRSCANRANGE_KEY", "i", "i", "*", 10, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 4; idx >= 4; --idx) {
    expect_field_value.push_back(gp7_field_value[idx]);
  }
  ASSERT_TRUE(field_value_match(field_value_out, expect_field_value));
  ASSERT_EQ(next_field, "");

  // ************************** Group 8 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      a     c     e     g     i     k     m     o     q     s
  //                              ^     ^
  //                    field_start     field_end
  std::vector<FieldValue> gp8_field_value{{"a", "v"}, {"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"},
                                          {"k", "v"}, {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  s = db.HMSet("GP8_PKHRSCANRANGE_KEY", gp8_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP8_PKHRSCANRANGE_KEY", 10));

  field_value_out.clear();
  expect_field_value.clear();
  s = db.PKHRScanRange("GP8_PKHRSCANRANGE_KEY", "i", "k", "*", 10, &field_value_out, &next_field);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_TRUE(field_value_match(field_value_out, expect_field_value));
  ASSERT_EQ(next_field, "");

  // ************************** Group 9 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      a     c     e     g     i     k     m     o     q     s
  //            ^           ^                             ^
  //       field_end    next_field                   field_start
  std::vector<FieldValue> gp9_field_value{{"a", "v"}, {"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"},
                                          {"k", "v"}, {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  s = db.HMSet("GP9_PKHRSCANRANGE_KEY", gp9_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP9_PKHRSCANRANGE_KEY", 10));

  field_value_out.clear();
  expect_field_value.clear();
  s = db.PKHRScanRange("GP9_PKHRSCANRANGE_KEY", "q", "c", "*", 5, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 4; --idx) {
    expect_field_value.push_back(gp7_field_value[idx]);
  }
  ASSERT_TRUE(field_value_match(field_value_out, expect_field_value));
  ASSERT_EQ(next_field, "g");

  // ************************** Group 10 Test **************************
  //      0     1     2     3     4     5     6     7     8     9
  //      a     c     e     g     i     k     m     o     q     s
  //            ^     ^                       ^           ^
  //    field_end     next_field           deleted   field_start
  std::vector<FieldValue> gp10_field_value{{"a", "v"}, {"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"},
                                           {"k", "v"}, {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  s = db.HMSet("GP10_PKHRSCANRANGE_KEY", gp10_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP10_PKHRSCANRANGE_KEY", 10));
  s = db.HDel("GP10_PKHRSCANRANGE_KEY", {"m"}, &ret);
  ASSERT_TRUE(s.ok());

  field_value_out.clear();
  expect_field_value.clear();
  s = db.PKHRScanRange("GP10_PKHRSCANRANGE_KEY", "q", "c", "*", 5, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 3; --idx) {
    if (idx != 6) {
      expect_field_value.push_back(gp10_field_value[idx]);
    }
  }
  ASSERT_TRUE(field_value_match(field_value_out, expect_field_value));
  ASSERT_EQ(next_field, "e");

  // ************************** Group 11 Test **************************
  //      0     1     2     3     4     5     6     7     8
  //     a_f1  a_f2  a_f3  b_f1  b_f2  b_f3  c_f1  c_f2  c_f3
  //      ^                 ^                             ^
  // field_end          next_field                   field_start
  std::vector<FieldValue> gp11_field_value{{"a_f1", "v"}, {"a_f2", "v"}, {"a_f3", "v"}, {"b_f1", "v"}, {"b_f2", "v"},
                                           {"b_f3", "v"}, {"c_f1", "v"}, {"c_f2", "v"}, {"c_f3", "v"}};
  s = db.HMSet("GP11_PKHRSCANRANGE_KEY", gp11_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP11_PKHRSCANRANGE_KEY", 9));
  ASSERT_TRUE(s.ok());

  field_value_out.clear();
  expect_field_value.clear();
  s = db.PKHRScanRange("GP11_PKHRSCANRANGE_KEY", "c_f3", "a_f1", "*f3", 3, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 6; --idx) {
    if (idx != 6 && idx != 7) {
      expect_field_value.push_back(gp11_field_value[idx]);
    }
  }
  ASSERT_TRUE(field_value_match(field_value_out, expect_field_value));
  ASSERT_EQ(next_field, "b_f3");

  // ************************** Group 12 Test **************************
  //                                0     1     2     3     4     5     6     7     8
  //                           a    c     e     g     i     k     m     o     q     s
  //          ^                ^
  // field_end/next_field field_start
  std::vector<FieldValue> gp12_field_value_a{{"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"}, {"k", "v"},
                                             {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  std::vector<FieldValue> gp12_field_value_b{{"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"}, {"k", "v"},
                                             {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  std::vector<FieldValue> gp12_field_value_c{{"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"}, {"k", "v"},
                                             {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  s = db.HMSet("GP12_PKHRSCANRANGE_KEY_A", gp12_field_value_a);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP12_PKHRSCANRANGE_KEY_A", 9));
  s = db.HMSet("GP12_PKHRSCANRANGE_KEY_B", gp12_field_value_b);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP12_PKHRSCANRANGE_KEY_B", 9));
  s = db.HMSet("GP12_PKHRSCANRANGE_KEY_C", gp12_field_value_c);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP12_PKHRSCANRANGE_KEY_C", 9));

  field_value_out.clear();
  expect_field_value.clear();
  s = db.PKHRScanRange("GP12_PKHRSCANRANGE_KEY_B", "a", "", "*", 5, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(field_value_match(field_value_out, expect_field_value));
  ASSERT_EQ(next_field, "");

  // ************************** Group 13 Test **************************
  //          0     1     2     3     4     5     6     7     8
  //          c     e     g     i     k     m     o     q     s
  //          ^     ^                             ^
  //     field_end  next_field               field_start
  std::vector<FieldValue> gp13_field_value_a{{"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"}, {"k", "v"},
                                             {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  std::vector<FieldValue> gp13_field_value_b{{"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"}, {"k", "v"},
                                             {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  std::vector<FieldValue> gp13_field_value_c{{"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"}, {"k", "v"},
                                             {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  s = db.HMSet("GP13_PKHRSCANRANGE_KEY_A", gp13_field_value_a);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP13_PKHRSCANRANGE_KEY_A", 9));
  s = db.HMSet("GP13_PKHRSCANRANGE_KEY_B", gp13_field_value_b);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP13_PKHRSCANRANGE_KEY_B", 9));
  s = db.HMSet("GP13_PKHRSCANRANGE_KEY_C", gp13_field_value_c);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP13_PKHRSCANRANGE_KEY_C", 9));

  field_value_out.clear();
  expect_field_value.clear();
  s = db.PKHRScanRange("GP13_PKHRSCANRANGE_KEY_B", "o", "c", "*", 5, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 6; idx >= 2; --idx) {
    expect_field_value.push_back(gp13_field_value_b[idx]);
  }
  ASSERT_TRUE(field_value_match(field_value_out, expect_field_value));
  ASSERT_EQ(next_field, "e");

  // ************************** Group 14 Test **************************
  //  0     1     2        3     4     5     6     7     8
  //  c     e     g        i     k     m     o     q     s    u
  //              ^        ^                                  ^
  //         field_end next_field                        field_start
  std::vector<FieldValue> gp14_field_value_a{{"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"}, {"k", "v"},
                                             {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  std::vector<FieldValue> gp14_field_value_b{{"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"}, {"k", "v"},
                                             {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  std::vector<FieldValue> gp14_field_value_c{{"c", "v"}, {"e", "v"}, {"g", "v"}, {"i", "v"}, {"k", "v"},
                                             {"m", "v"}, {"o", "v"}, {"q", "v"}, {"s", "v"}};
  s = db.HMSet("GP14_PKHRSCANRANGE_KEY_A", gp14_field_value_a);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP14_PKHRSCANRANGE_KEY_A", 9));
  s = db.HMSet("GP14_PKHRSCANRANGE_KEY_B", gp14_field_value_b);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP14_PKHRSCANRANGE_KEY_B", 9));
  s = db.HMSet("GP14_PKHRSCANRANGE_KEY_C", gp14_field_value_c);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP14_PKHRSCANRANGE_KEY_C", 9));

  field_value_out.clear();
  expect_field_value.clear();
  s = db.PKHRScanRange("GP14_PKHRSCANRANGE_KEY_B", "u", "g", "*", 5, &field_value_out, &next_field);
  ASSERT_TRUE(s.ok());
  for (int32_t idx = 8; idx >= 4; --idx) {
    expect_field_value.push_back(gp14_field_value_b[idx]);
  }
  ASSERT_TRUE(field_value_match(field_value_out, expect_field_value));
  ASSERT_EQ(next_field, "i");
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
