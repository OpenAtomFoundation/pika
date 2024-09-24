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

#include "glog/logging.h"

#include "pstd/include/env.h"
#include "pstd/include/pika_codis_slot.h"
#include "storage/storage.h"
#include "storage/util.h"

using namespace storage;

class PKHashesTest : public ::testing::Test {
 public:
  PKHashesTest() = default;
  ~PKHashesTest() override = default;

  void SetUp() override {
    std::string path = "./db/pkhashes";
    pstd::DeleteDirIfExist(path);
    mkdir(path.c_str(), 0755);
    storage_options.options.create_if_missing = true;
    s = db.Open(storage_options, path);
  }

  void TearDown() override {
    std::string path = "./db/pkhashes";
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

static bool field_value_match(const std::vector<FieldValueTTL>& field_value_out,
                              const std::vector<FieldValue>& expect_field_value) {
  if (field_value_out.size() != expect_field_value.size()) {
    return false;
  }
  for (const auto& field_value : expect_field_value) {
    if (find(field_value_out.begin(), field_value_out.end(), FieldValueTTL{field_value.field, field_value.value, -1}) ==
        field_value_out.end()) {
      return false;
    }
  }
  return true;
}

static bool size_match(storage::Storage* const db, const Slice& key, int32_t expect_size) {
  int32_t size = 0;
  Status s = db->PKHLen(key, &size);
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
  int ret = db->Expire(key, 1);
  if ((ret == 0) || !type_status[storage::DataType::kHashes].ok()) {
    return false;
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  return true;
}

// PKHExpireTest
TEST_F(PKHashesTest, PKHExpireTest) {  // NOLINT

  int32_t ret = 0;
  std::vector<int32_t> rets;

  std::string value;

  // ***************** Group 1 Test *****************
  // If a field  with expired time（sec） in the hash and the field should expire after ttl sec.
  s = db.PKHSet("GP1_HSET_KEY", "HSET_TEST_FIELD", "HSET_TEST_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.PKHLen("GP1_HSET_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.PKHExpire("GP1_HSET_KEY", 2, 1, {"HSET_TEST_FIELD"}, &rets);
  s = db.PKHGet("GP1_HSET_KEY", "HSET_TEST_FIELD", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "HSET_TEST_VALUE");

  std::this_thread::sleep_for(std::chrono::milliseconds(3100));

  s = db.PKHGet("GP1_HSET_KEY", "HSET_TEST_FIELD", &value);
  ASSERT_FALSE(s.ok());

  // if [field:value] already expire and then the [field:value] should be updated
  s = db.PKHSet("GP1_HSET_KEY", "HSET_TEST_FIELD", "HSET_TEST_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  // ***************** Group 2 Test *****************
  // If multi fields  with expired timestamp(sec) in the hash and the fields should expire after timestamp.
  // TODO(DDD: cmd basic test cases fisrt)
}

// PKHExpireatTest
TEST_F(PKHashesTest, PKHExpireatTest) {  // NOLINT

  int32_t ret = 0;
  std::vector<int32_t> rets;

  std::string value;

  // ***************** Group 1 Test *****************
  // If a field  with expired time（sec） in the hash and the field should expire after ttl sec.
  s = db.PKHSet("GP1_HSET_KEY", "HSET_TEST_FIELD", "HSET_TEST_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.PKHLen("GP1_HSET_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  int64_t unix_time;
  rocksdb::Env::Default()->GetCurrentTime(&unix_time);
  int64_t timestamp = unix_time + 2;

  // It will expire in 2 seconds
  s = db.PKHExpireat("GP1_HSET_KEY", timestamp, 1, {"HSET_TEST_FIELD"}, &rets);
  s = db.PKHGet("GP1_HSET_KEY", "HSET_TEST_FIELD", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "HSET_TEST_VALUE");

  std::this_thread::sleep_for(std::chrono::milliseconds(3100));

  s = db.PKHGet("GP1_HSET_KEY", "HSET_TEST_FIELD", &value);
  ASSERT_FALSE(s.ok());  // the field has expired

  // if [field:value] already expire and then the [field:value] should be updated
  s = db.PKHSet("GP1_HSET_KEY", "HSET_TEST_FIELD", "HSET_TEST_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
}

// PKHExpiretimeTest
TEST_F(PKHashesTest, PKHExpiretimeTest) {  // NOLINT

  int32_t ret = 0;
  std::vector<int32_t> rets;

  std::string value;

  // ***************** Group 1 Test *****************
  // If a field  with expired time（sec） in the hash and the field should expire after ttl sec.
  s = db.PKHSet("GP1_HSET_KEY", "HSET_TEST_FIELD", "HSET_TEST_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.PKHLen("GP1_HSET_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  int64_t unix_time;
  rocksdb::Env::Default()->GetCurrentTime(&unix_time);
  int64_t timestamp = unix_time + 3;
  std::cout << " unix_time: " << unix_time << " timestamp: " << timestamp << std::endl;

  // It will expire in 3 seconds
  s = db.PKHExpireat("GP1_HSET_KEY", timestamp, 1, {"HSET_TEST_FIELD"}, &rets);
  s = db.PKHGet("GP1_HSET_KEY", "HSET_TEST_FIELD", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "HSET_TEST_VALUE");

  std::vector<int64_t> timestamps;
  s = db.PKHExpiretime("GP1_HSET_KEY", 1, {"HSET_TEST_FIELD"}, &timestamps);

  std::cout << " timestamps[0]: " << timestamps[0] << " timestamp: " << timestamp << std::endl;

  ASSERT_EQ(timestamps[0], timestamp);

  std::this_thread::sleep_for(std::chrono::milliseconds(3100));

  s = db.PKHGet("GP1_HSET_KEY", "HSET_TEST_FIELD", &value);
  // the field has not expired and should wait for 4 sec
  ASSERT_FALSE(s.ok());  // the field has ex/pired

  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "HSET_TEST_VALUE");

  std::this_thread::sleep_for(std::chrono::milliseconds(4100));

  s = db.PKHGet("GP1_HSET_KEY", "HSET_TEST_FIELD", &value);
  ASSERT_FALSE(s.ok());  // the field has expired

  // if [field:value] already expire and then the [field:value] should be updated
  s = db.PKHSet("GP1_HSET_KEY", "HSET_TEST_FIELD", "HSET_TEST_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
}

// PKHTTLTest
TEST_F(PKHashesTest, PKHTTLTest) {  // NOLINT
  int32_t ret = 0;
  std::vector<int32_t> rets;

  std::string value;

  // ***************** Group 1 Test *****************
  // If a field  with expired time（sec） in the hash and the field should expire after ttl sec.
  s = db.PKHSet("GP1_HSET_KEY", "HSET_TEST_FIELD", "HSET_TEST_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.PKHLen("GP1_HSET_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  int64_t ttl = 2;
  s = db.PKHExpire("GP1_HSET_KEY", ttl, 1, {"HSET_TEST_FIELD"}, &rets);
  s = db.PKHGet("GP1_HSET_KEY", "HSET_TEST_FIELD", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "HSET_TEST_VALUE");

  std::vector<int64_t> ttls;

  s = db.PKHTTL("GP1_HSET_KEY", 1, {"HSET_TEST_FIELD"}, &ttls);

  std::cout << " ttls[0]: " << ttls[0] << " ttl: " << ttl << std::endl;

  ASSERT_EQ(ttls[0], ttl);

  std::this_thread::sleep_for(std::chrono::milliseconds(3100));

  s = db.PKHGet("GP1_HSET_KEY", "HSET_TEST_FIELD", &value);
  ASSERT_FALSE(s.ok());

  // if [field:value] already expire and then the [field:value] should be updated
  s = db.PKHSet("GP1_HSET_KEY", "HSET_TEST_FIELD", "HSET_TEST_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
}

// PKHPersistTest
TEST_F(PKHashesTest, PKHPersistTest) {  // NOLINT

  int32_t ret = 0;
  std::vector<int32_t> rets;

  std::string value;

  // ***************** Group 1 Test *****************
  // If a field  with expired time（sec） in the hash and the field should expire after ttl sec.
  s = db.PKHSet("GP1_HSET_KEY", "HSET_TEST_FIELD", "HSET_TEST_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.PKHLen("GP1_HSET_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  int64_t ttl = 2;
  s = db.PKHExpire("GP1_HSET_KEY", ttl, 1, {"HSET_TEST_FIELD"}, &rets);
  s = db.PKHGet("GP1_HSET_KEY", "HSET_TEST_FIELD", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "HSET_TEST_VALUE");

  std::vector<int64_t> ttls;

  s = db.PKHTTL("GP1_HSET_KEY", 1, {"HSET_TEST_FIELD"}, &ttls);

  std::cout << " ttls[0]: " << ttls[0] << " ttl: " << ttl << std::endl;

  ASSERT_EQ(ttls[0], ttl);

  std::this_thread::sleep_for(std::chrono::milliseconds(3100));

  s = db.PKHGet("GP1_HSET_KEY", "HSET_TEST_FIELD", &value);
  ASSERT_FALSE(s.ok());

  // if [field:value] already expire and then the [field:value] should be updated
  s = db.PKHSet("GP1_HSET_KEY", "HSET_TEST_FIELD", "HSET_TEST_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  rets.clear();
  s = db.PKHExpire("GP1_HSET_KEY", ttl, 1, {"HSET_TEST_FIELD"}, &rets);

  rets.clear();
  s = db.PKHPersist("GP1_HSET_KEY", 1, {"HSET_TEST_FIELD"}, &rets);

  std::this_thread::sleep_for(std::chrono::milliseconds(3100));

  s = db.PKHGet("GP1_HSET_KEY", "HSET_TEST_FIELD", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "HSET_TEST_VALUE");
}

// PKHSetexTest
TEST_F(PKHashesTest, PKHSetexTest) {  // NOLINT

  int32_t ret = 0;
  std::vector<int32_t> rets;

  std::string value;

  // ***************** Group 1 Test *****************
  // If a field  with expired time（sec） in the hash and the field should expire after ttl sec.
  int64_t ttl = 2;
  s = db.PKHSetex("GP1_HSET_KEY", "HSET_TEST_FIELD", "HSET_TEST_VALUE", 2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.PKHLen("GP1_HSET_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.PKHGet("GP1_HSET_KEY", "HSET_TEST_FIELD", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "HSET_TEST_VALUE");

  std::vector<int64_t> ttls;

  s = db.PKHTTL("GP1_HSET_KEY", 1, {"HSET_TEST_FIELD"}, &ttls);

  std::cout << " ttls[0]: " << ttls[0] << " ttl: " << ttl << std::endl;

  ASSERT_EQ(ttls[0], ttl);

  std::this_thread::sleep_for(std::chrono::milliseconds(3100));

  s = db.PKHGet("GP1_HSET_KEY", "HSET_TEST_FIELD", &value);
  ASSERT_FALSE(s.ok());

  // if [field:value] already expire and then the [field:value] should be updated
  s = db.PKHSet("GP1_HSET_KEY", "HSET_TEST_FIELD", "HSET_TEST_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  // ***************** Group 2 Test *****************
  // If a field  with expired time（sec） in the hash and persist the field.
  s = db.PKHSetex("GP1_HSET_KEY_1", "HSET_TEST_FIELD_1", "HSET_TEST_VALUE_1", 2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  rets.clear();
  s = db.PKHPersist("GP1_HSET_KEY_1", 1, {"HSET_TEST_FIELD_1"}, &rets);

  std::this_thread::sleep_for(std::chrono::milliseconds(3100));

  s = db.PKHGet("GP1_HSET_KEY_1", "HSET_TEST_FIELD_1", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "HSET_TEST_VALUE_1");
}

// PKHGet
TEST_F(PKHashesTest, PKHGetTest) {
  int32_t ret = 0;
  std::string value;
  s = db.PKHSet("HGET_KEY", "HGET_TEST_FIELD", "HGET_TEST_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.PKHGet("HGET_KEY", "HGET_TEST_FIELD", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "HGET_TEST_VALUE");

  // If key does not exist.
  s = db.PKHGet("HGET_NOT_EXIST_KEY", "HGET_TEST_FIELD", &value);
  ASSERT_TRUE(s.IsNotFound());

  // If field is not present in the hash
  s = db.PKHGet("HGET_KEY", "HGET_NOT_EXIST_FIELD", &value);
  ASSERT_TRUE(s.IsNotFound());
}

// PKHMSet
TEST_F(PKHashesTest, PKHMSetTest) {
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

  s = db.PKHMSet("HMSET_KEY", fvs1);
  ASSERT_TRUE(s.ok());
  s = db.PKHLen("HMSET_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);

  s = db.PKHMSet("HMSET_KEY", fvs2);
  ASSERT_TRUE(s.ok());

  s = db.PKHLen("HMSET_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);

  std::vector<storage::ValueStatus> vss1;
  std::vector<std::string> fields1{"TEST_FIELD1", "TEST_FIELD2", "TEST_FIELD3", "TEST_FIELD4"};
  s = db.PKHMGet("HMSET_KEY", fields1, &vss1);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(vss1.size(), 4);

  ASSERT_EQ(vss1[0].value, "TEST_VALUE1");
  ASSERT_EQ(vss1[1].value, "TEST_VALUE2");
  ASSERT_EQ(vss1[2].value, "TEST_VALUE5");
  ASSERT_EQ(vss1[3].value, "TEST_VALUE4");

  std::map<storage::DataType, rocksdb::Status> type_status;
  db.Expire("HMSET_KEY", 1);
  ASSERT_TRUE(type_status[storage::DataType::kHashes].ok());

  // The key has timeout
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  std::vector<storage::FieldValue> fvs3;
  fvs3.push_back({"TEST_FIELD3", "TEST_VALUE3"});
  fvs3.push_back({"TEST_FIELD4", "TEST_VALUE4"});
  fvs3.push_back({"TEST_FIELD5", "TEST_VALUE5"});
  s = db.PKHMSet("HMSET_KEY", fvs3);
  ASSERT_TRUE(s.ok());

  s = db.PKHLen("HMSET_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<storage::ValueStatus> vss2;
  std::vector<std::string> fields2{"TEST_FIELD3", "TEST_FIELD4", "TEST_FIELD5"};
  s = db.PKHMGet("HMSET_KEY", fields2, &vss2);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(vss2.size(), 3);

  ASSERT_EQ(vss2[0].value, "TEST_VALUE3");
  ASSERT_EQ(vss2[1].value, "TEST_VALUE4");
  ASSERT_EQ(vss2[2].value, "TEST_VALUE5");
}

// PKHExists
TEST_F(PKHashesTest, PKHExistsTest) {
  int32_t ret;
  s = db.PKHSet("HEXIST_KEY", "HEXIST_FIELD", "HEXIST_VALUE", &ret);
  ASSERT_TRUE(s.ok());

  s = db.PKHExists("HEXIST_KEY", "HEXIST_FIELD");
  ASSERT_TRUE(s.ok());

  // If key does not exist.
  s = db.PKHExists("HEXIST_NOT_EXIST_KEY", "HEXIST_FIELD");
  ASSERT_TRUE(s.IsNotFound());

  // If field is not present in the hash
  s = db.PKHExists("HEXIST_KEY", "HEXIST_NOT_EXIST_FIELD");
  ASSERT_TRUE(s.IsNotFound());
}

// PKHDel
TEST_F(PKHashesTest, PKHDel) {
  int32_t ret = 0;
  std::vector<storage::FieldValue> fvs;
  fvs.push_back({"TEST_FIELD1", "TEST_VALUE1"});
  fvs.push_back({"TEST_FIELD2", "TEST_VALUE2"});
  fvs.push_back({"TEST_FIELD3", "TEST_VALUE3"});
  fvs.push_back({"TEST_FIELD4", "TEST_VALUE4"});

  s = db.PKHMSet("HDEL_KEY", fvs);
  ASSERT_TRUE(s.ok());

  std::vector<std::string> fields{"TEST_FIELD1", "TEST_FIELD2", "TEST_FIELD3", "TEST_FIElD2", "TEST_NOT_EXIST_FIELD"};
  s = db.PKHDel("HDEL_KEY", fields, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  s = db.PKHLen("HDEL_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  // Delete not exist hash table
  s = db.PKHDel("HDEL_NOT_EXIST_KEY", fields, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);

  // Delete timeout hash table
  s = db.PKHMSet("HDEL_TIMEOUT_KEY", fvs);
  ASSERT_TRUE(s.ok());

  std::map<storage::DataType, rocksdb::Status> type_status;
  db.Expire("HDEL_TIMEOUT_KEY", 1);
  ASSERT_TRUE(type_status[storage::DataType::kHashes].ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  s = db.PKHDel("HDEL_TIMEOUT_KEY", fields, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
}

// PKHSet
TEST_F(PKHashesTest, PKHSetTest) {
  int32_t ret = 0;
  std::string value;

  // ***************** Group 1 Test *****************
  // If field is a new field in the hash and value was set.
  s = db.PKHSet("GP1_HSET_KEY", "HSET_TEST_FIELD", "HSET_TEST_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.PKHLen("GP1_HSET_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.PKHGet("GP1_HSET_KEY", "HSET_TEST_FIELD", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "HSET_TEST_VALUE");

  // If field already exists in the hash and the value was updated.
  s = db.PKHSet("GP1_HSET_KEY", "HSET_TEST_FIELD", "HSET_TEST_NEW_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  s = db.PKHLen("GP1_HSET_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.PKHGet("GP1_HSET_KEY", "HSET_TEST_FIELD", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "HSET_TEST_NEW_VALUE");

  // If field already exists in the hash and the value was equal.
  s = db.PKHSet("GP1_HSET_KEY", "HSET_TEST_FIELD", "HSET_TEST_NEW_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  s = db.PKHLen("GP1_HSET_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.PKHGet("GP1_HSET_KEY", "HSET_TEST_FIELD", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "HSET_TEST_NEW_VALUE");

  // ***************** Group 2 Test *****************
  s = db.PKHSet("GP2_HSET_KEY", "HSET_TEST_FIELD", "HSET_TEST_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.PKHLen("GP2_HSET_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  // 从这里开始有问题。
  s = db.PKHGet("GP2_HSET_KEY", "HSET_TEST_FIELD", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "HSET_TEST_VALUE");

  ASSERT_TRUE(make_expired(&db, "GP2_HSET_KEY"));

  s = db.PKHSet("GP2_HSET_KEY", "HSET_TEST_FIELD", "HSET_TEST_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.PKHGet("GP2_HSET_KEY", "HSET_TEST_FIELD", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "HSET_TEST_VALUE");

  s = db.PKHLen("GP2_HSET_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.PKHGet("GP2_HSET_KEY", "HSET_TEST_FIELD", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "HSET_TEST_VALUE");

  // ***************** Group 3 Test *****************
  s = db.PKHSet("GP3_HSET_KEY", "HSET_TEST_FIELD", "HSET_TEST_NEW_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.PKHLen("GP3_HSET_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.PKHGet("GP3_HSET_KEY", "HSET_TEST_FIELD", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "HSET_TEST_NEW_VALUE");

  ASSERT_TRUE(make_expired(&db, "GP3_HSET_KEY"));

  s = db.PKHSet("GP3_HSET_KEY", "HSET_TEST_NEW_FIELD", "HSET_TEST_NEW_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.PKHLen("GP3_HSET_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.PKHGet("GP3_HSET_KEY", "HSET_TEST_NEW_FIELD", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "HSET_TEST_NEW_VALUE");

  // ***************** Group 4 Test *****************
  // hset after string type key expires, should success
  s = db.Setex("GP4_HSET_KEY", "STRING_VALUE_WITH_TTL", 1);
  ASSERT_TRUE(s.ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(2100));
  s = db.PKHSet("GP4_HSET_KEY", "HSET_TEST_NEW_FIELD", "HSET_TEST_NEW_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
}

// PKHMGet
TEST_F(PKHashesTest, PKHMGetTest) {
  int32_t ret = 0;
  std::vector<storage::ValueStatus> vss;

  // ***************** Group 1 Test *****************
  std::vector<storage::FieldValue> fvs1;
  fvs1.push_back({"TEST_FIELD1", "TEST_VALUE1"});
  fvs1.push_back({"TEST_FIELD2", "TEST_VALUE2"});
  fvs1.push_back({"TEST_FIELD3", "TEST_VALUE3"});
  s = db.PKHMSet("GP1_HMGET_KEY", fvs1);
  ASSERT_TRUE(s.ok());

  s = db.PKHLen("GP1_HMGET_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> fields1{"TEST_FIELD1", "TEST_FIELD2", "TEST_FIELD3", "TEST_NOT_EXIST_FIELD"};
  s = db.PKHMGet("GP1_HMGET_KEY", fields1, &vss);
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
  s = db.PKHMSet("GP2_HMGET_KEY", fvs2);
  ASSERT_TRUE(s.ok());

  s = db.PKHLen("GP2_HMGET_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);

  vss.clear();
  std::vector<std::string> fields2{"TEST_FIELD1", "TEST_FIELD2", "TEST_NOT_EXIST_FIELD"};
  s = db.PKHMGet("GP2_HMGET_KEY", fields2, &vss);
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
  s = db.PKHMGet("GP3_HMGET_KEY", fields3, &vss);
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

  s = db.PKHMSet("GP4_HMGET_KEY", fvs4);
  ASSERT_TRUE(s.ok());

  ASSERT_TRUE(make_expired(&db, "GP4_HMGET_KEY"));

  vss.clear();
  std::vector<std::string> fields4{"TEST_FIELD1", "TEST_FIELD2", "TEST_FIELD3"};
  s = db.PKHMGet("GP4_HMGET_KEY", fields4, &vss);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(vss.size(), 3);

  ASSERT_TRUE(vss[0].status.IsNotFound());
  ASSERT_EQ(vss[0].value, "");
  ASSERT_TRUE(vss[1].status.IsNotFound());
  ASSERT_EQ(vss[1].value, "");
  ASSERT_TRUE(vss[2].status.IsNotFound());
  ASSERT_EQ(vss[2].value, "");
}

// PKHLen
TEST_F(PKHashesTest, PKHLenTest) {
  int32_t ret = 0;

  // ***************** Group 1 Test *****************
  std::vector<storage::FieldValue> fvs1;
  fvs1.push_back({"GP1_TEST_FIELD1", "GP1_TEST_VALUE1"});
  fvs1.push_back({"GP1_TEST_FIELD2", "GP1_TEST_VALUE2"});
  fvs1.push_back({"GP1_TEST_FIELD3", "GP1_TEST_VALUE3"});
  s = db.PKHMSet("GP1_HLEN_KEY", fvs1);
  ASSERT_TRUE(s.ok());

  s = db.PKHLen("GP1_HLEN_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  // ***************** Group 2 Test *****************
  std::vector<storage::FieldValue> fvs2;
  fvs2.push_back({"GP2_TEST_FIELD1", "GP2_TEST_VALUE1"});
  fvs2.push_back({"GP2_TEST_FIELD2", "GP2_TEST_VALUE2"});
  fvs2.push_back({"GP2_TEST_FIELD3", "GP2_TEST_VALUE3"});
  s = db.PKHMSet("GP2_HLEN_KEY", fvs2);
  ASSERT_TRUE(s.ok());

  s = db.PKHDel("GP2_HLEN_KEY", {"GP2_TEST_FIELD1", "GP2_TEST_FIELD2"}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);

  s = db.PKHLen("GP2_HLEN_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.PKHDel("GP2_HLEN_KEY", {"GP2_TEST_FIELD3"}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.PKHLen("GP2_HLEN_KEY", &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(ret, 0);
}

// PKHGetall
TEST_F(PKHashesTest, PKHGetall) {
  int32_t ret = 0;
  std::vector<storage::FieldValue> mid_fvs_in;
  mid_fvs_in.push_back({"MID_TEST_FIELD1", "MID_TEST_VALUE1"});
  mid_fvs_in.push_back({"MID_TEST_FIELD2", "MID_TEST_VALUE2"});
  mid_fvs_in.push_back({"MID_TEST_FIELD3", "MID_TEST_VALUE3"});
  s = db.PKHMSet("B_HGETALL_KEY", mid_fvs_in);
  ASSERT_TRUE(s.ok());

  std::vector<storage::FieldValueTTL> fvs_out;
  s = db.PKHGetall("B_HGETALL_KEY", &fvs_out);
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
  s = db.PKHMSet("A_HGETALL_KEY", pre_fvs_in);
  ASSERT_TRUE(s.ok());
  fvs_out.clear();
  s = db.PKHGetall("B_HGETALL_KEY", &fvs_out);
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
  s = db.PKHMSet("C_HGETALL_KEY", suf_fvs_in);
  ASSERT_TRUE(s.ok());
  fvs_out.clear();
  s = db.PKHGetall("B_HGETALL_KEY", &fvs_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(fvs_out.size(), 3);
  ASSERT_EQ(fvs_out[0].field, "MID_TEST_FIELD1");
  ASSERT_EQ(fvs_out[0].value, "MID_TEST_VALUE1");
  ASSERT_EQ(fvs_out[1].field, "MID_TEST_FIELD2");
  ASSERT_EQ(fvs_out[1].value, "MID_TEST_VALUE2");
  ASSERT_EQ(fvs_out[2].field, "MID_TEST_FIELD3");
  ASSERT_EQ(fvs_out[2].value, "MID_TEST_VALUE3");

  // PKHGetall timeout hash table
  fvs_out.clear();
  std::map<storage::DataType, rocksdb::Status> type_status;
  db.Expire("B_HGETALL_KEY", 1);
  ASSERT_TRUE(type_status[storage::DataType::kHashes].ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  s = db.PKHGetall("B_HGETALL_KEY", &fvs_out);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(fvs_out.size(), 0);

  // PKHGetall not exist hash table
  fvs_out.clear();
  s = db.PKHGetall("HGETALL_NOT_EXIST_KEY", &fvs_out);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(fvs_out.size(), 0);
}

// PKHIncrby
TEST_F(PKHashesTest, PKHIncrby) {
  int32_t ret;
  int64_t value;
  std::string str_value;

  // ***************** Group 1 Test *****************
  s = db.PKHSet("GP1_HINCRBY_KEY", "GP1_HINCRBY_FIELD", "1", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.PKHIncrby("GP1_HINCRBY_KEY", "GP1_HINCRBY_FIELD", 1, &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, 2);

  // ***************** Group 2 Test *****************
  s = db.PKHSet("GP2_HINCRBY_KEY", "GP2_HINCRBY_FIELD", " 1", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.PKHIncrby("GP2_HINCRBY_KEY", "GP2_HINCRBY_FIELD", 1, &value);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_EQ(value, 0);

  // ***************** Group 3 Test *****************
  s = db.PKHSet("GP3_HINCRBY_KEY", "GP3_HINCRBY_FIELD", "1 ", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.PKHIncrby("GP3_HINCRBY_KEY", "GP3_HINCRBY_FIELD", 1, &value);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_EQ(value, 0);

  // If key does not exist the value is set to 0 before the
  // operation is performed
  s = db.PKHIncrby("HINCRBY_NEW_KEY", "HINCRBY_EXIST_FIELD", 1000, &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, 1000);
  s = db.PKHGet("HINCRBY_NEW_KEY", "HINCRBY_EXIST_FIELD", &str_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(atoll(str_value.data()), 1000);

  // If the hash field contains a string that can not be
  // represented as integer
  s = db.PKHSet("HINCRBY_KEY", "HINCRBY_STR_FIELD", "HINCRBY_VALEU", &ret);
  ASSERT_TRUE(s.ok());
  s = db.PKHIncrby("HINCRBY_KEY", "HINCRBY_STR_FIELD", 100, &value);
  ASSERT_TRUE(s.IsCorruption());

  // If field does not exist the value is set to 0 before the
  // operation is performed
  s = db.PKHIncrby("HINCRBY_KEY", "HINCRBY_NOT_EXIST_FIELD", 100, &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, 100);
  s = db.PKHGet("HINCRBY_KEY", "HINCRBY_NOT_EXIST_FIELD", &str_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(atoll(str_value.data()), 100);

  s = db.PKHSet("HINCRBY_KEY", "HINCRBY_NUM_FIELD", "100", &ret);
  ASSERT_TRUE(s.ok());

  // Positive test
  s = db.PKHIncrby("HINCRBY_KEY", "HINCRBY_NUM_FIELD", 100, &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, 200);
  s = db.PKHGet("HINCRBY_KEY", "HINCRBY_NUM_FIELD", &str_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(atoll(str_value.data()), 200);

  // Negative test
  s = db.PKHIncrby("HINCRBY_KEY", "HINCRBY_NUM_FIELD", -100, &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, 100);
  s = db.PKHGet("HINCRBY_KEY", "HINCRBY_NUM_FIELD", &str_value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(atoll(str_value.data()), 100);

  // Larger than the maximum number 9223372036854775807
  s = db.PKHSet("HINCRBY_KEY", "HINCRBY_NUM_FIELD", "10", &ret);
  ASSERT_TRUE(s.ok());
  s = db.PKHIncrby("HINCRBY_KEY", "HINCRBY_NUM_FIELD", 9223372036854775807, &value);
  ASSERT_TRUE(s.IsInvalidArgument());

  // Less than the minimum number -9223372036854775808
  s = db.PKHSet("HINCRBY_KEY", "HINCRBY_NUM_FIELD", "-10", &ret);
  ASSERT_TRUE(s.ok());
  s = db.PKHIncrby("HINCRBY_KEY", "HINCRBY_NUM_FIELD", -9223372036854775807, &value);
  ASSERT_TRUE(s.IsInvalidArgument());
}

// PKHKeys
TEST_F(PKHashesTest, PKHKeys) {
  int32_t ret = 0;
  std::vector<storage::FieldValue> mid_fvs_in;
  mid_fvs_in.push_back({"MID_TEST_FIELD1", "MID_TEST_VALUE1"});
  mid_fvs_in.push_back({"MID_TEST_FIELD2", "MID_TEST_VALUE2"});
  mid_fvs_in.push_back({"MID_TEST_FIELD3", "MID_TEST_VALUE3"});
  s = db.PKHMSet("B_HKEYS_KEY", mid_fvs_in);
  ASSERT_TRUE(s.ok());

  std::vector<std::string> fields;
  s = db.PKHKeys("B_HKEYS_KEY", &fields);
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
  s = db.PKHMSet("A_HKEYS_KEY", pre_fvs_in);
  ASSERT_TRUE(s.ok());
  fields.clear();
  s = db.PKHKeys("B_HKEYS_KEY", &fields);
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
  s = db.PKHMSet("A_HKEYS_KEY", suf_fvs_in);
  ASSERT_TRUE(s.ok());
  fields.clear();
  s = db.PKHKeys("B_HKEYS_KEY", &fields);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(fields.size(), 3);
  ASSERT_EQ(fields[0], "MID_TEST_FIELD1");
  ASSERT_EQ(fields[1], "MID_TEST_FIELD2");
  ASSERT_EQ(fields[2], "MID_TEST_FIELD3");

  // PKHKeys timeout hash table
  fields.clear();
  std::map<storage::DataType, rocksdb::Status> type_status;
  db.Expire("B_HKEYS_KEY", 1);
  ASSERT_TRUE(type_status[storage::DataType::kHashes].ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  s = db.PKHKeys("B_HKEYS_KEY", &fields);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(fields.size(), 0);

  // PKHKeys not exist hash table
  fields.clear();
  s = db.PKHKeys("HKEYS_NOT_EXIST_KEY", &fields);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(fields.size(), 0);
}

// PKHVals
TEST_F(PKHashesTest, PKHVals) {
  int32_t ret = 0;
  std::vector<storage::FieldValue> mid_fvs_in;
  mid_fvs_in.push_back({"MID_TEST_FIELD1", "MID_TEST_VALUE1"});
  mid_fvs_in.push_back({"MID_TEST_FIELD2", "MID_TEST_VALUE2"});
  mid_fvs_in.push_back({"MID_TEST_FIELD3", "MID_TEST_VALUE3"});
  s = db.PKHMSet("B_HVALS_KEY", mid_fvs_in);
  ASSERT_TRUE(s.ok());

  std::vector<std::string> values;
  s = db.PKHVals("B_HVALS_KEY", &values);
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
  s = db.PKHMSet("A_HVALS_KEY", pre_fvs_in);
  ASSERT_TRUE(s.ok());
  values.clear();
  s = db.PKHVals("B_HVALS_KEY", &values);
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
  s = db.PKHMSet("C_HVALS_KEY", suf_fvs_in);
  ASSERT_TRUE(s.ok());
  values.clear();
  s = db.PKHVals("B_HVALS_KEY", &values);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(values.size(), 3);
  ASSERT_EQ(values[0], "MID_TEST_VALUE1");
  ASSERT_EQ(values[1], "MID_TEST_VALUE2");
  ASSERT_EQ(values[2], "MID_TEST_VALUE3");

  // PKHVals timeout hash table
  values.clear();
  std::map<storage::DataType, rocksdb::Status> type_status;
  db.Expire("B_HVALS_KEY", 1);
  ASSERT_TRUE(type_status[storage::DataType::kHashes].ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  s = db.PKHVals("B_HVALS_KEY", &values);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(values.size(), 0);

  // PKHVals not exist hash table
  values.clear();
  s = db.PKHVals("HVALS_NOT_EXIST_KEY", &values);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(values.size(), 0);
}

// PKHStrlen
TEST_F(PKHashesTest, PKHStrlenTest) {
  int32_t ret = 0;
  int32_t len = 0;
  s = db.PKHSet("HSTRLEN_KEY", "HSTRLEN_TEST_FIELD", "HSTRLEN_TEST_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.PKHStrlen("HSTRLEN_KEY", "HSTRLEN_TEST_FIELD", &len);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(len, 18);

  // If the key or the field do not exist, 0 is returned
  s = db.PKHStrlen("HSTRLEN_KEY", "HSTRLEN_NOT_EXIST_FIELD", &len);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(len, 0);
}

// PKHScan
TEST_F(PKHashesTest, PKHScanTest) {  // NOLINT
  int64_t cursor = 0;
  int64_t next_cursor = 0;
  std::vector<FieldValueTTL> field_value_out;

  // ***************** Group 1 Test *****************
  // {a,v} {b,v} {c,v} {d,v} {e,v} {f,v} {g,v} {h,v}
  // 0     1     2     3     4     5     6     7
  std::vector<FieldValue> gp1_field_value{{"a", "v"}, {"b", "v"}, {"c", "v"}, {"d", "v"},
                                          {"e", "v"}, {"f", "v"}, {"g", "v"}, {"h", "v"}};
  s = db.PKHMSet("GP1_HSCAN_KEY", gp1_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP1_HSCAN_KEY", 8));

  s = db.PKHScan("GP1_HSCAN_KEY", 0, "*", 3, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 3);
  ASSERT_EQ(next_cursor, 3);
  ASSERT_TRUE(field_value_match(field_value_out, {{"a", "v"}, {"b", "v"}, {"c", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.PKHScan("GP1_HSCAN_KEY", cursor, "*", 3, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 3);
  ASSERT_EQ(next_cursor, 6);
  ASSERT_TRUE(field_value_match(field_value_out, {{"d", "v"}, {"e", "v"}, {"f", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.PKHScan("GP1_HSCAN_KEY", cursor, "*", 3, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 2);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(field_value_match(field_value_out, {{"g", "v"}, {"h", "v"}}));

  // ***************** Group 2 Test *****************
  // {a,v} {b,v} {c,v} {d,v} {e,v} {f,v} {g,v} {h,v}
  // 0     1     2     3     4     5     6     7
  std::vector<FieldValue> gp2_field_value{{"a", "v"}, {"b", "v"}, {"c", "v"}, {"d", "v"},
                                          {"e", "v"}, {"f", "v"}, {"g", "v"}, {"h", "v"}};
  s = db.PKHMSet("GP2_HSCAN_KEY", gp2_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP2_HSCAN_KEY", 8));

  field_value_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.PKHScan("GP2_HSCAN_KEY", cursor, "*", 1, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 1);
  ASSERT_TRUE(field_value_match(field_value_out, {{"a", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.PKHScan("GP2_HSCAN_KEY", cursor, "*", 1, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 2);
  ASSERT_TRUE(field_value_match(field_value_out, {{"b", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.PKHScan("GP2_HSCAN_KEY", cursor, "*", 1, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 3);
  ASSERT_TRUE(field_value_match(field_value_out, {{"c", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.PKHScan("GP2_HSCAN_KEY", cursor, "*", 1, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 4);
  ASSERT_TRUE(field_value_match(field_value_out, {{"d", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.PKHScan("GP2_HSCAN_KEY", cursor, "*", 1, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 5);
  ASSERT_TRUE(field_value_match(field_value_out, {{"e", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.PKHScan("GP2_HSCAN_KEY", cursor, "*", 1, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 6);
  ASSERT_TRUE(field_value_match(field_value_out, {{"f", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.PKHScan("GP2_HSCAN_KEY", cursor, "*", 1, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 7);
  ASSERT_TRUE(field_value_match(field_value_out, {{"g", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.PKHScan("GP2_HSCAN_KEY", cursor, "*", 1, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(field_value_match(field_value_out, {{"h", "v"}}));

  // ***************** Group 3 Test *****************
  // {a,v} {b,v} {c,v} {d,v} {e,v} {f,v} {g,v} {h,v}
  // 0     1     2     3     4     5     6     7
  std::vector<FieldValue> gp3_field_value{{"a", "v"}, {"b", "v"}, {"c", "v"}, {"d", "v"},
                                          {"e", "v"}, {"f", "v"}, {"g", "v"}, {"h", "v"}};
  s = db.PKHMSet("GP3_HSCAN_KEY", gp3_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP3_HSCAN_KEY", 8));

  field_value_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.PKHScan("GP3_HSCAN_KEY", cursor, "*", 5, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 5);
  ASSERT_EQ(next_cursor, 5);
  ASSERT_TRUE(field_value_match(field_value_out, {{"a", "v"}, {"b", "v"}, {"c", "v"}, {"d", "v"}, {"e", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.PKHScan("GP3_HSCAN_KEY", cursor, "*", 5, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 3);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(field_value_match(field_value_out, {{"f", "v"}, {"g", "v"}, {"h", "v"}}));

  // ***************** Group 4 Test *****************
  // {a,v} {b,v} {c,v} {d,v} {e,v} {f,v} {g,v} {h,v}
  // 0     1     2     3     4     5     6     7
  std::vector<FieldValue> gp4_field_value{{"a", "v"}, {"b", "v"}, {"c", "v"}, {"d", "v"},
                                          {"e", "v"}, {"f", "v"}, {"g", "v"}, {"h", "v"}};
  s = db.PKHMSet("GP4_HSCAN_KEY", gp4_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP4_HSCAN_KEY", 8));

  field_value_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.PKHScan("GP4_HSCAN_KEY", cursor, "*", 10, &field_value_out, &next_cursor);
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
  s = db.PKHMSet("GP5_HSCAN_KEY", gp5_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP5_HSCAN_KEY", 9));

  field_value_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.PKHScan("GP5_HSCAN_KEY", cursor, "*1*", 3, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 3);
  ASSERT_TRUE(field_value_match(field_value_out, {{"a_1_", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.PKHScan("GP5_HSCAN_KEY", cursor, "*1*", 3, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 6);
  ASSERT_TRUE(field_value_match(field_value_out, {{"b_1_", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.PKHScan("GP5_HSCAN_KEY", cursor, "*1*", 3, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(field_value_match(field_value_out, {{"c_1_", "v"}}));

  // ***************** Group 6 Test *****************
  // {a_1_,v} {a_2_,v} {a_3_,v} {b_1_,v} {b_2_,v} {b_3_,v} {c_1_,v} {c_2_,v} {c_3_,v}
  // 0        1        2        3        4        5        6        7        8
  std::vector<FieldValue> gp6_field_value{{"a_1_", "v"}, {"a_2_", "v"}, {"a_3_", "v"}, {"b_1_", "v"}, {"b_2_", "v"},
                                          {"b_3_", "v"}, {"c_1_", "v"}, {"c_2_", "v"}, {"c_3_", "v"}};
  s = db.PKHMSet("GP6_HSCAN_KEY", gp6_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP6_HSCAN_KEY", 9));

  field_value_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.PKHScan("GP6_HSCAN_KEY", cursor, "a*", 3, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 3);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(field_value_match(field_value_out, {{"a_1_", "v"}, {"a_2_", "v"}, {"a_3_", "v"}}));

  field_value_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.PKHScan("GP6_HSCAN_KEY", cursor, "a*", 2, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 2);
  ASSERT_EQ(next_cursor, 2);
  ASSERT_TRUE(field_value_match(field_value_out, {{"a_1_", "v"}, {"a_2_", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.PKHScan("GP6_HSCAN_KEY", cursor, "a*", 2, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(field_value_match(field_value_out, {{"a_3_", "v"}}));

  field_value_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.PKHScan("GP6_HSCAN_KEY", cursor, "a*", 1, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 1);
  ASSERT_TRUE(field_value_match(field_value_out, {{"a_1_", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.PKHScan("GP6_HSCAN_KEY", cursor, "a*", 1, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 2);
  ASSERT_TRUE(field_value_match(field_value_out, {{"a_2_", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.PKHScan("GP6_HSCAN_KEY", cursor, "a*", 1, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(field_value_match(field_value_out, {{"a_3_", "v"}}));

  // ***************** Group 7 Test *****************
  // {a_1_,v} {a_2_,v} {a_3_,v} {b_1_,v} {b_2_,v} {b_3_,v} {c_1_,v} {c_2_,v} {c_3_,v}
  // 0        1        2        3        4        5        6        7        8
  std::vector<FieldValue> gp7_field_value{{"a_1_", "v"}, {"a_2_", "v"}, {"a_3_", "v"}, {"b_1_", "v"}, {"b_2_", "v"},
                                          {"b_3_", "v"}, {"c_1_", "v"}, {"c_2_", "v"}, {"c_3_", "v"}};
  s = db.PKHMSet("GP7_HSCAN_KEY", gp7_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP7_HSCAN_KEY", 9));

  field_value_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.PKHScan("GP7_HSCAN_KEY", cursor, "b*", 3, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 3);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(field_value_match(field_value_out, {{"b_1_", "v"}, {"b_2_", "v"}, {"b_3_", "v"}}));

  field_value_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.PKHScan("GP7_HSCAN_KEY", cursor, "b*", 2, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 2);
  ASSERT_EQ(next_cursor, 2);
  ASSERT_TRUE(field_value_match(field_value_out, {{"b_1_", "v"}, {"b_2_", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.PKHScan("GP7_HSCAN_KEY", cursor, "b*", 2, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(field_value_match(field_value_out, {{"b_3_", "v"}}));

  field_value_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.PKHScan("GP7_HSCAN_KEY", cursor, "b*", 1, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 1);
  ASSERT_TRUE(field_value_match(field_value_out, {{"b_1_", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.PKHScan("GP7_HSCAN_KEY", cursor, "b*", 1, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 2);
  ASSERT_TRUE(field_value_match(field_value_out, {{"b_2_", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.PKHScan("GP7_HSCAN_KEY", cursor, "b*", 1, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(field_value_match(field_value_out, {{"b_3_", "v"}}));

  // ***************** Group 8 Test *****************
  // {a_1_,v} {a_2_,v} {a_3_,v} {b_1_,v} {b_2_,v} {b_3_,v} {c_1_,v} {c_2_,v} {c_3_,v}
  // 0        1        2        3        4        5        6        7        8
  std::vector<FieldValue> gp8_field_value{{"a_1_", "v"}, {"a_2_", "v"}, {"a_3_", "v"}, {"b_1_", "v"}, {"b_2_", "v"},
                                          {"b_3_", "v"}, {"c_1_", "v"}, {"c_2_", "v"}, {"c_3_", "v"}};
  s = db.PKHMSet("GP8_HSCAN_KEY", gp8_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP8_HSCAN_KEY", 9));

  field_value_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.PKHScan("GP8_HSCAN_KEY", cursor, "c*", 3, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 3);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(field_value_match(field_value_out, {{"c_1_", "v"}, {"c_2_", "v"}, {"c_3_", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.PKHScan("GP8_HSCAN_KEY", cursor, "c*", 2, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 2);
  ASSERT_EQ(next_cursor, 2);
  ASSERT_TRUE(field_value_match(field_value_out, {{"c_1_", "v"}, {"c_2_", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.PKHScan("GP8_HSCAN_KEY", cursor, "c*", 2, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(field_value_match(field_value_out, {{"c_3_", "v"}}));

  field_value_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.PKHScan("GP8_HSCAN_KEY", cursor, "c*", 1, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 1);
  ASSERT_TRUE(field_value_match(field_value_out, {{"c_1_", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.PKHScan("GP8_HSCAN_KEY", cursor, "c*", 1, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 2);
  ASSERT_TRUE(field_value_match(field_value_out, {{"c_2_", "v"}}));

  field_value_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.PKHScan("GP8_HSCAN_KEY", cursor, "c*", 1, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 1);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(field_value_match(field_value_out, {{"c_3_", "v"}}));

  // ***************** Group 9 Test *****************
  // {a_1_,v} {a_2_,v} {a_3_,v} {b_1_,v} {b_2_,v} {b_3_,v} {c_1_,v} {c_2_,v} {c_3_,v}
  // 0        1        2        3        4        5        6        7        8
  std::vector<FieldValue> gp9_field_value{{"a_1_", "v"}, {"a_2_", "v"}, {"a_3_", "v"}, {"b_1_", "v"}, {"b_2_", "v"},
                                          {"b_3_", "v"}, {"c_1_", "v"}, {"c_2_", "v"}, {"c_3_", "v"}};
  s = db.PKHMSet("GP9_HSCAN_KEY", gp9_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP9_HSCAN_KEY", 9));

  field_value_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.PKHScan("GP9_HSCAN_KEY", cursor, "d*", 3, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_value_out.size(), 0);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(field_value_match(field_value_out, {}));

  // ***************** Group 10 Test *****************
  // {a_1_,v} {a_2_,v} {a_3_,v} {b_1_,v} {b_2_,v} {b_3_,v} {c_1_,v} {c_2_,v} {c_3_,v}
  // 0        1        2        3        4        5        6        7        8
  std::vector<FieldValue> gp10_field_value{{"a_1_", "v"}, {"a_2_", "v"}, {"a_3_", "v"}, {"b_1_", "v"}, {"b_2_", "v"},
                                           {"b_3_", "v"}, {"c_1_", "v"}, {"c_2_", "v"}, {"c_3_", "v"}};
  s = db.PKHMSet("GP10_HSCAN_KEY", gp10_field_value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP10_HSCAN_KEY", 9));

  ASSERT_TRUE(make_expired(&db, "GP10_HSCAN_KEY"));
  field_value_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.PKHScan("GP10_HSCAN_KEY", cursor, "*", 10, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(field_value_out.size(), 0);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(field_value_match(field_value_out, {}));

  // ***************** Group 11 Test *****************
  // PKHScan Not Exist Key
  field_value_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.PKHScan("GP11_HSCAN_KEY", cursor, "*", 10, &field_value_out, &next_cursor);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(field_value_out.size(), 0);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(field_value_match(field_value_out, {}));
}

int main(int argc, char** argv) {
  if (!pstd::FileExists("./log")) {
    pstd::CreatePath("./log");
  }
  FLAGS_log_dir = "./log";
  FLAGS_minloglevel = 0;
  FLAGS_max_log_size = 1800;
  FLAGS_logbufsecs = 0;
  ::google::InitGoogleLogging("pkhashes_test");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
