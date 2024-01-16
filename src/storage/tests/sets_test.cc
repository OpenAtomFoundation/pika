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

class SetsTest : public ::testing::Test {
 public:
  SetsTest() = default;
  ~SetsTest() override = default;

  void SetUp() override {
    std::string path = "./db/sets";
    if (access(path.c_str(), F_OK) != 0) {
      mkdir(path.c_str(), 0755);
    }
    storage_options.options.create_if_missing = true;
    s = db.Open(storage_options, path);
  }

  void TearDown() override {
    std::string path = "./db/sets";
    DeleteFiles(path.c_str());
  }

  static void SetUpTestSuite() {}
  static void TearDownTestSuite() {}

  StorageOptions storage_options;
  storage::Storage db;
  storage::Status s;
};

static bool members_match(storage::Storage* const db, const Slice& key,
                          const std::vector<std::string>& expect_members) {
  std::vector<std::string> mm_out;
  Status s = db->SMembers(key, &mm_out);
  if (!s.ok() && !s.IsNotFound()) {
    return false;
  }
  if (mm_out.size() != expect_members.size()) {
    return false;
  }
  if (s.IsNotFound() && expect_members.empty()) {
    return true;
  }
  for (const auto& member : expect_members) {
    if (find(mm_out.begin(), mm_out.end(), member) == mm_out.end()) {
      return false;
    }
  }
  return true;
}

static bool members_match(const std::vector<std::string>& mm_out, const std::vector<std::string>& expect_members) {
  if (mm_out.size() != expect_members.size()) {
    return false;
  }
  for (const auto& member : expect_members) {
    if (find(mm_out.begin(), mm_out.end(), member) == mm_out.end()) {
      return false;
    }
  }
  return true;
}

static bool members_contains(const std::vector<std::string>& mm_out, const std::vector<std::string>& total_members) {
  for (const auto& member : mm_out) {
    if (find(total_members.begin(), total_members.end(), member) == total_members.end()) {
      return false;
    }
  }
  return true;
}

static bool members_uniquen(const std::vector<std::string>& members) {
  for (int32_t idx = 0; idx < members.size(); ++idx) {
    for (int32_t sidx = idx + 1; sidx < members.size(); ++sidx) {
      if (members[idx] == members[sidx]) {
        return false;
      }
    }
  }
  return true;
}

static bool size_match(storage::Storage* const db, const Slice& key, int32_t expect_size) {
  int32_t size = 0;
  Status s = db->SCard(key, &size);
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
  if ((ret == 0) || !type_status[storage::DataType::kSets].ok()) {
    return false;
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  return true;
}

// SAdd
TEST_F(SetsTest, SAddTest) {  // NOLINT
  int32_t ret = 0;
  std::vector<std::string> members1{"a", "b", "c", "b"};
  s = db.SAdd("SADD_KEY", members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  ASSERT_TRUE(size_match(&db, "SADD_KEY", 3));
  ASSERT_TRUE(members_match(&db, "SADD_KEY", {"a", "b", "c"}));

  std::vector<std::string> members2{"d", "e"};
  s = db.SAdd("SADD_KEY", members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  ASSERT_TRUE(size_match(&db, "SADD_KEY", 5));
  ASSERT_TRUE(members_match(&db, "SADD_KEY", {"a", "b", "c", "d", "e"}));

  // The key has timeout
  ASSERT_TRUE(make_expired(&db, "SADD_KEY"));
  ASSERT_TRUE(size_match(&db, "SADD_KEY", 0));

  std::vector<std::string> members3{"a", "b"};
  s = db.SAdd("SADD_KEY", members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  ASSERT_TRUE(size_match(&db, "SADD_KEY", 2));
  ASSERT_TRUE(members_match(&db, "SADD_KEY", {"a", "b"}));

  // Delete the key
  std::vector<std::string> del_keys = {"SADD_KEY"};
  std::map<storage::DataType, storage::Status> type_status;
  db.Del(del_keys, &type_status);
  ASSERT_TRUE(type_status[storage::DataType::kSets].ok());
  ASSERT_TRUE(size_match(&db, "SADD_KEY", 0));
  ASSERT_TRUE(members_match(&db, "SADD_KEY", {}));

  std::vector<std::string> members4{"a", "x", "l"};
  s = db.SAdd("SADD_KEY", members4, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  ASSERT_TRUE(size_match(&db, "SADD_KEY", 3));
  ASSERT_TRUE(members_match(&db, "SADD_KEY", {"a", "x", "l"}));

  std::vector<std::string> members5{"a", "x", "l", "z"};
  s = db.SAdd("SADD_KEY", members5, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  ASSERT_TRUE(size_match(&db, "SADD_KEY", 4));
  ASSERT_TRUE(members_match(&db, "SADD_KEY", {"a", "x", "l", "z"}));
}

// SCard
TEST_F(SetsTest, SCardTest) {  // NOLINT
  int32_t ret = 0;
  std::vector<std::string> members{"MM1", "MM2", "MM3"};
  s = db.SAdd("SCARD_KEY", members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  s = db.SCard("SCARD_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
}

// SDiff
TEST_F(SetsTest, SDiffTest) {  // NOLINT
  int32_t ret = 0;

  // ***************** Group 1 Test *****************
  // key1 = {a, b, c, d}
  // key2 = {c}
  // key3 = {a, c, e}
  // SDIFF key1 key2 key3  = {b, d}
  std::vector<std::string> gp1_members1{"a", "b", "c", "d"};
  std::vector<std::string> gp1_members2{"c"};
  std::vector<std::string> gp1_members3{"a", "c", "e"};
  s = db.SAdd("GP1_SDIFF_KEY1", gp1_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP1_SDIFF_KEY2", gp1_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.SAdd("GP1_SDIFF_KEY3", gp1_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp1_keys{"GP1_SDIFF_KEY1", "GP1_SDIFF_KEY2", "GP1_SDIFF_KEY3"};
  std::vector<std::string> gp1_members_out;
  s = db.SDiff(gp1_keys, &gp1_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(gp1_members_out, {"b", "d"}));

  // key1 = {a, b, c, d}
  // key2 = {c}
  // key3 = {a, c, e}       (expire)
  // SDIFF key1 key2 key3  = {a, b, d}
  std::map<storage::DataType, rocksdb::Status> gp1_type_status;
  db.Expire("GP1_SDIFF_KEY3", 1, &gp1_type_status);
  ASSERT_TRUE(gp1_type_status[storage::DataType::kSets].ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  gp1_members_out.clear();
  s = db.SDiff(gp1_keys, &gp1_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(gp1_members_out, {"a", "b", "d"}));

  // key1 = {a, b, c, d}
  // key2 = {c}
  // key3 = {a, c, e}       (expire key)
  // key4 = {}              (not exist key)
  // SDIFF key1 key2 key3 key4 = {a, b, d}
  gp1_keys.emplace_back("GP1_SDIFF_KEY4");
  gp1_members_out.clear();
  s = db.SDiff(gp1_keys, &gp1_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(gp1_members_out, {"a", "b", "d"}));

  // ***************** Group 2 Test *****************
  // key1 = {}
  // key2 = {c}
  // key3 = {a, c, e}
  // SDIFF key1 key2 key3  = {}
  std::vector<std::string> gp2_members1{};
  std::vector<std::string> gp2_members2{"c"};
  std::vector<std::string> gp2_members3{"a", "c", "e"};
  s = db.SAdd("GP2_SDIFF_KEY1", gp2_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  s = db.SAdd("GP2_SDIFF_KEY2", gp2_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.SAdd("GP2_SDIFF_KEY3", gp2_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp2_keys{"GP2_SDIFF_KEY1", "GP2_SDIFF_KEY2", "GP2_SDIFF_KEY3"};
  std::vector<std::string> gp2_members_out;
  s = db.SDiff(gp2_keys, &gp2_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(gp2_members_out, {}));

  // ***************** Group 3 Test *****************
  // key1 = {a, b, c, d}
  // SDIFF key1 = {a, b, c, d}
  std::vector<std::string> gp3_members1{"a", "b", "c", "d"};
  s = db.SAdd("GP3_SDIFF_KEY1", gp3_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);

  std::vector<std::string> gp3_keys{"GP3_SDIFF_KEY1"};
  std::vector<std::string> gp3_members_out;
  s = db.SDiff(gp3_keys, &gp3_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(gp3_members_out, {"a", "b", "c", "d"}));

  // ***************** Group 4 Test *****************
  // key1 = {a, b, c, d}    (expire key);
  // key2 = {c}
  // key3 = {a, c, e}
  // SDIFF key1 key2 key3  = {}
  std::vector<std::string> gp4_members1{"a", "b", "c", "d"};
  std::vector<std::string> gp4_members2{"c"};
  std::vector<std::string> gp4_members3{"a", "c", "e"};
  s = db.SAdd("GP4_SDIFF_KEY1", gp4_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP4_SDIFF_KEY2", gp4_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.SAdd("GP4_SDIFF_KEY3", gp4_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  ASSERT_TRUE(make_expired(&db, "GP4_SDIFF_KEY1"));

  std::vector<std::string> gp4_keys{"GP4_SDIFF_KEY1", "GP4_SDIFF_KEY2", "GP4_SDIFF_KEY3"};
  std::vector<std::string> gp4_members_out;
  s = db.SDiff(gp4_keys, &gp4_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(gp4_members_out, {}));

  // ***************** Group 5 Test *****************
  // key1 = {a, b, c, d}   (key1 is empty key)
  // key2 = {c}
  // key3 = {a, c, e}
  // SDIFF key1 key2 key3  = {b, d}
  std::vector<std::string> gp5_members1{"a", "b", "c", "d"};
  std::vector<std::string> gp5_members2{"c"};
  std::vector<std::string> gp5_members3{"a", "c", "e"};
  s = db.SAdd("", gp5_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP5_SDIFF_KEY2", gp5_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.SAdd("GP5_SDIFF_KEY3", gp5_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp5_keys{"", "GP5_SDIFF_KEY2", "GP5_SDIFF_KEY3"};
  std::vector<std::string> gp5_members_out;
  s = db.SDiff(gp5_keys, &gp5_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(gp5_members_out, {"b", "d"}));

  // double "GP5_SDIFF_KEY3"
  gp5_keys.emplace_back("GP5_SDIFF_KEY3");
  gp5_members_out.clear();
  s = db.SDiff(gp5_keys, &gp5_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(gp5_members_out, {"b", "d"}));

  // ***************** Group 6 Test *****************
  // empty keys
  std::vector<std::string> gp6_keys;
  std::vector<std::string> gp6_members_out;
  s = db.SDiff(gp6_keys, &gp6_members_out);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(members_match(gp6_members_out, {}));
}

// SDiffstore
TEST_F(SetsTest, SDiffstoreTest) {  // NOLINT
  int32_t ret = 0;

  // ***************** Group 1 Test *****************
  // destination = {};
  // key1 = {a, b, c, d}
  // key2 = {c}
  // key3 = {a, c, e}
  // SDIFFSTORE destination key1 key2 key3
  // destination = {b, d}
  std::vector<std::string> gp1_members1{"a", "b", "c", "d"};
  std::vector<std::string> gp1_members2{"c"};
  std::vector<std::string> gp1_members3{"a", "c", "e"};
  s = db.SAdd("GP1_SDIFFSTORE_KEY1", gp1_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP1_SDIFFSTORE_KEY2", gp1_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.SAdd("GP1_SDIFFSTORE_KEY3", gp1_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp1_members_out;
  std::vector<std::string> value_to_dest;
  std::vector<std::string> gp1_keys{"GP1_SDIFFSTORE_KEY1", "GP1_SDIFFSTORE_KEY2", "GP1_SDIFFSTORE_KEY3"};

  s = db.SDiffstore("GP1_SDIFFSTORE_DESTINATION1", gp1_keys, value_to_dest, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  ASSERT_TRUE(size_match(&db, "GP1_SDIFFSTORE_DESTINATION1", 2));
  ASSERT_TRUE(members_match(&db, "GP1_SDIFFSTORE_DESTINATION1", {"b", "d"}));

  // destination = {};
  // key1 = {a, b, c, d}
  // key2 = {c}
  // key3 = {a, c, e}       (expire)
  // SDIFFSTORE destination key1 key2 key3
  // destination = {a, b, d}
  std::map<storage::DataType, rocksdb::Status> gp1_type_status;
  db.Expire("GP1_SDIFFSTORE_KEY3", 1, &gp1_type_status);
  ASSERT_TRUE(gp1_type_status[storage::DataType::kSets].ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  gp1_members_out.clear();
  s = db.SDiffstore("GP1_SDIFFSTORE_DESTINATION2", gp1_keys, value_to_dest, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  ASSERT_TRUE(size_match(&db, "GP1_SDIFFSTORE_DESTINATION2", 3));
  ASSERT_TRUE(members_match(&db, "GP1_SDIFFSTORE_DESTINATION2", {"a", "b", "d"}));

  // destination = {};
  // key1 = {a, b, c, d}
  // key2 = {c}
  // key3 = {a, c, e}       (expire key)
  // key4 = {}              (not exist key)
  // SDIFFSTORE destination key1 key2 key3
  // destination = {a, b, d}
  gp1_keys.emplace_back("GP1_SDIFFSTORE_KEY4");
  gp1_members_out.clear();
  s = db.SDiffstore("GP1_SDIFFSTORE_DESTINATION3", gp1_keys, value_to_dest, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  ASSERT_TRUE(size_match(&db, "GP1_SDIFFSTORE_DESTINATION3", 3));
  ASSERT_TRUE(members_match(&db, "GP1_SDIFFSTORE_DESTINATION3", {"a", "b", "d"}));

  // ***************** Group 2 Test *****************
  // destination = {};
  // key1 = {}
  // key2 = {c}
  // key3 = {a, c, e}
  // SDIFFSTORE destination key1 key2 key3
  // destination = {}
  std::vector<std::string> gp2_members1{};
  std::vector<std::string> gp2_members2{"c"};
  std::vector<std::string> gp2_members3{"a", "c", "e"};
  s = db.SAdd("GP2_SDIFFSTORE_KEY1", gp2_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  s = db.SAdd("GP2_SDIFFSTORE_KEY2", gp2_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.SAdd("GP2_SDIFFSTORE_KEY3", gp2_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp2_keys{"GP2_SDIFFSTORE_KEY1", "GP2_SDIFFSTORE_KEY2", "GP2_SDIFFSTORE_KEY3"};
  std::vector<std::string> gp2_members_out;
  s = db.SDiffstore("GP2_SDIFFSTORE_DESTINATION1", gp2_keys, value_to_dest, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  ASSERT_TRUE(size_match(&db, "GP2_SDIFFSTORE_DESTINATION1", 0));
  ASSERT_TRUE(members_match(&db, "GP2_SDIFFSTORE_DESTINATION1", {}));

  // ***************** Group 3 Test *****************
  // destination = {};
  // key1 = {a, b, c, d}
  // SDIFFSTORE destination key1
  // destination = {a, b, c, d}
  std::vector<std::string> gp3_members1{"a", "b", "c", "d"};
  s = db.SAdd("GP3_SDIFFSTORE_KEY1", gp3_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);

  std::vector<std::string> gp3_keys{"GP3_SDIFFSTORE_KEY1"};
  std::vector<std::string> gp3_members_out;
  s = db.SDiffstore("GP3_SDIFFSTORE_DESTINATION1", gp3_keys, value_to_dest, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  ASSERT_TRUE(size_match(&db, "GP3_SDIFFSTORE_DESTINATION1", 4));
  ASSERT_TRUE(members_match(&db, "GP3_SDIFFSTORE_DESTINATION1", {"a", "b", "c", "d"}));

  // ***************** Group 4 Test *****************
  // destination = {};
  // key1 = {a, b, c, d}    (expire key);
  // key2 = {c}
  // key3 = {a, c, e}
  // SDIFFSTORE destination key1 key2 key3
  // destination = {}
  std::vector<std::string> gp4_members1{"a", "b", "c", "d"};
  std::vector<std::string> gp4_members2{"c"};
  std::vector<std::string> gp4_members3{"a", "c", "e"};
  s = db.SAdd("GP4_SDIFFSTORE_KEY1", gp4_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP4_SDIFFSTORE_KEY2", gp4_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.SAdd("GP4_SDIFFSTORE_KEY3", gp4_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  ASSERT_TRUE(make_expired(&db, "GP4_SDIFFSTORE_KEY1"));

  std::vector<std::string> gp4_keys{"GP4_SDIFFSTORE_KEY1", "GP4_SDIFFSTORE_KEY2", "GP4_SDIFFSTORE_KEY3"};
  std::vector<std::string> gp4_members_out;
  s = db.SDiffstore("GP4_SDIFFSTORE_DESTINATION1", gp4_keys, value_to_dest, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  ASSERT_TRUE(size_match(&db, "GP4_SDIFFSTORE_DESTINATION1", 0));
  ASSERT_TRUE(members_match(&db, "GP4_SDIFFSTORE_DESTINATION1", {}));

  // ***************** Group 5 Test *****************
  // the destination already exists, it is overwritten
  // destination = {a, x, l}
  // key1 = {a, b, c, d}
  // key2 = {c}
  // key3 = {a, c, e}
  // SDIFFSTORE destination key1 key2 key3
  // destination = {b, d}
  std::vector<std::string> gp5_destination_members{"a", "x", "l"};
  std::vector<std::string> gp5_members1{"a", "b", "c", "d"};
  std::vector<std::string> gp5_members2{"c"};
  std::vector<std::string> gp5_members3{"a", "c", "e"};
  s = db.SAdd("GP5_SDIFFSTORE_DESTINATION1", gp5_destination_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  s = db.SAdd("GP5_SDIFFSTORE_KEY1", gp5_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP5_SDIFFSTORE_KEY2", gp5_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.SAdd("GP5_SDIFFSTORE_KEY3", gp5_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp5_keys{"GP5_SDIFFSTORE_KEY1", "GP5_SDIFFSTORE_KEY2", "GP5_SDIFFSTORE_KEY3"};
  s = db.SDiffstore("GP5_SDIFFSTORE_DESTINATION1", gp5_keys, value_to_dest, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  ASSERT_TRUE(size_match(&db, "GP5_SDIFFSTORE_DESTINATION1", 2));
  ASSERT_TRUE(members_match(&db, "GP5_SDIFFSTORE_DESTINATION1", {"b", "d"}));

  // ***************** Group 6 Test *****************
  // test destination equal key1 (the destination already exists, it is
  // overwritten)
  // destination = {a, b, c, d};
  // key2 = {c}
  // key3 = {a, c, e}
  // SDIFFSTORE destination destination key2 key3
  // destination = {b, d}
  std::vector<std::string> gp6_destination_members{"a", "b", "c", "d"};
  std::vector<std::string> gp6_members2{"c"};
  std::vector<std::string> gp6_members3{"a", "c", "e"};
  s = db.SAdd("GP6_SDIFFSTORE_DESTINATION1", gp6_destination_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP6_SDIFFSTORE_KEY2", gp6_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.SAdd("GP6_SDIFFSTORE_KEY3", gp6_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp6_keys{"GP6_SDIFFSTORE_DESTINATION1", "GP6_SDIFFSTORE_KEY2", "GP6_SDIFFSTORE_KEY3"};
  s = db.SDiffstore("GP6_SDIFFSTORE_DESTINATION1", gp6_keys, value_to_dest, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  ASSERT_TRUE(size_match(&db, "GP6_SDIFFSTORE_DESTINATION1", 2));
  ASSERT_TRUE(members_match(&db, "GP6_SDIFFSTORE_DESTINATION1", {"b", "d"}));

  // ***************** Group 7 Test *****************
  // test destination exist but timeout (the destination already exists, it is
  // overwritten)
  // destination = {a, x, l};
  // key1 = {a, b, c, d}
  // key2 = {c}
  // key3 = {a, c, e}
  // SDIFFSTORE destination key1 key2 key3
  // destination = {b, d}
  std::vector<std::string> gp7_destination_members{"a", "x", "l"};
  std::vector<std::string> gp7_members1{"a", "b", "c", "d"};
  std::vector<std::string> gp7_members2{"c"};
  std::vector<std::string> gp7_members3{"a", "c", "e"};
  s = db.SAdd("GP7_SDIFFSTORE_DESTINATION1", gp7_destination_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  s = db.SAdd("GP7_SDIFFSTORE_KEY1", gp7_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP7_SDIFFSTORE_KEY2", gp7_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.SAdd("GP7_SDIFFSTORE_KEY3", gp7_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  ASSERT_TRUE(make_expired(&db, "GP7_SDIFFSTORE_DESTINATION1"));

  std::vector<std::string> gp7_keys{"GP7_SDIFFSTORE_KEY1", "GP7_SDIFFSTORE_KEY2", "GP7_SDIFFSTORE_KEY3"};
  s = db.SDiffstore("GP7_SDIFFSTORE_DESTINATION1", gp7_keys, value_to_dest, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  ASSERT_TRUE(size_match(&db, "GP7_SDIFFSTORE_DESTINATION1", 2));
  ASSERT_TRUE(members_match(&db, "GP7_SDIFFSTORE_DESTINATION1", {"b", "d"}));
}

// SInter
TEST_F(SetsTest, SInterTest) {  // NOLINT
  int32_t ret = 0;

  // ***************** Group 1 Test *****************
  // key1 = {a, b, c, d}
  // key2 = {a, c}
  // key3 = {a, c, e}
  // SINTER key1 key2 key3  = {a, c}
  std::vector<std::string> gp1_members1{"a", "b", "c", "d"};
  std::vector<std::string> gp1_members2{"a", "c"};
  std::vector<std::string> gp1_members3{"a", "c", "e"};
  s = db.SAdd("GP1_SINTER_KEY1", gp1_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP1_SINTER_KEY2", gp1_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SAdd("GP1_SINTER_KEY3", gp1_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp1_keys{"GP1_SINTER_KEY1", "GP1_SINTER_KEY2", "GP1_SINTER_KEY3"};
  std::vector<std::string> gp1_members_out;
  s = db.SInter(gp1_keys, &gp1_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(gp1_members_out, {"a", "c"}));

  // key1 = {a, b, c, d}
  // key2 = {a, c}
  // key3 = {a, c, e}       (expire)
  // SINTER key1 key2 key3  = {}
  ASSERT_TRUE(make_expired(&db, "GP1_SINTER_KEY3"));

  gp1_members_out.clear();
  s = db.SInter(gp1_keys, &gp1_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(gp1_members_out, {}));

  // ***************** Group 2 Test *****************
  // key1 = {a, b, c, d}
  // key2 = {c}
  // key3 = {a, c, e}
  // SINTER key1 key2 key3 not_exist_key = {}
  std::vector<std::string> gp2_members1{"a", "b", "c", "d"};
  std::vector<std::string> gp2_members2{"c"};
  std::vector<std::string> gp2_members3{"a", "c", "e"};
  s = db.SAdd("GP2_SINTER_KEY1", gp2_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP2_SINTER_KEY2", gp2_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.SAdd("GP2_SINTER_KEY3", gp2_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp2_keys{"GP2_SINTER_KEY1", "GP2_SINTER_KEY2", "GP2_SINTER_KEY3", "NOT_EXIST_KEY"};
  std::vector<std::string> gp2_members_out;
  s = db.SInter(gp2_keys, &gp2_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(gp2_members_out, {}));

  // ***************** Group 3 Test *****************
  // key1 = {a, b, c, d}
  // key2 = {a, c}
  // key3 = {}
  // SINTER key1 key2 key3 = {}
  std::vector<std::string> gp3_members1{"a", "b", "c", "d"};
  std::vector<std::string> gp3_members2{"a", "c"};
  std::vector<std::string> gp3_members3{"a", "b", "c"};
  s = db.SAdd("GP3_SINTER_KEY1", gp3_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP3_SINTER_KEY2", gp3_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SAdd("GP3_SINTER_KEY3", gp3_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  s = db.SRem("GP3_SINTER_KEY3", gp3_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  s = db.SCard("GP3_SINTER_KEY3", &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(ret, 0);
  std::vector<std::string> gp3_members_out;
  s = db.SMembers("GP3_SINTER_KEY3", &gp3_members_out);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_TRUE(members_match(gp3_members_out, {}));

  gp3_members_out.clear();
  std::vector<std::string> gp3_keys{"GP3_SINTER_KEY1", "GP3_SINTER_KEY2", "GP3_SINTER_KEY3"};
  s = db.SInter(gp3_keys, &gp3_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(gp3_members_out, {}));

  // ***************** Group 4 Test *****************
  // key1 = {}
  // key2 = {a, c}
  // key3 = {a, b, c, d}
  // SINTER key1 key2 key3 = {}
  std::vector<std::string> gp4_members1{"a", "b", "c", "d"};
  std::vector<std::string> gp4_members2{"a", "c"};
  std::vector<std::string> gp4_members3{"a", "b", "c", "d"};
  s = db.SAdd("GP4_SINTER_KEY1", gp4_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP4_SINTER_KEY2", gp4_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SAdd("GP4_SINTER_KEY3", gp4_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);

  s = db.SRem("GP4_SINTER_KEY1", gp4_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SCard("GP4_SINTER_KEY1", &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(ret, 0);
  std::vector<std::string> gp4_members_out;
  s = db.SMembers("GP4_SINTER_KEY1", &gp4_members_out);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(gp4_members_out.size(), 0);

  gp4_members_out.clear();
  std::vector<std::string> gp4_keys{"GP4_SINTER_KEY1", "GP4_SINTER_KEY2", "GP4_SINTER_KEY3"};
  s = db.SInter(gp4_keys, &gp4_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(gp4_members_out, {}));

  // ***************** Group 5 Test *****************
  // key1 = {a, b, c, d}
  // key2 = {a, c}
  // key3 = {a, b, c}
  // SINTER key1 key2 key2 key3 = {a, c}
  std::vector<std::string> gp5_members1{"a", "b", "c", "d"};
  std::vector<std::string> gp5_members2{"a", "c"};
  std::vector<std::string> gp5_members3{"a", "b", "c"};
  s = db.SAdd("GP5_SINTER_KEY1", gp5_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP5_SINTER_KEY2", gp5_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SAdd("GP5_SINTER_KEY3", gp5_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp5_members_out;
  std::vector<std::string> gp5_keys{"GP5_SINTER_KEY1", "GP5_SINTER_KEY2", "GP5_SINTER_KEY2", "GP5_SINTER_KEY3"};
  s = db.SInter(gp5_keys, &gp5_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(gp5_members_out, {"a", "c"}));
}

// SInterstore
TEST_F(SetsTest, SInterstoreTest) {  // NOLINT
  int32_t ret = 0;

  // ***************** Group 1 Test *****************
  // destination = {}
  // key1 = {a, b, c, d}
  // key2 = {a, c}
  // key3 = {a, c, e}
  // SINTERSTORE destination key1 key2 key3
  // destination = {a, c}
  std::vector<std::string> gp1_members1{"a", "b", "c", "d"};
  std::vector<std::string> gp1_members2{"a", "c"};
  std::vector<std::string> gp1_members3{"a", "c", "e"};
  s = db.SAdd("GP1_SINTERSTORE_KEY1", gp1_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP1_SINTERSTORE_KEY2", gp1_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SAdd("GP1_SINTERSTORE_KEY3", gp1_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp1_keys{"GP1_SINTERSTORE_KEY1", "GP1_SINTERSTORE_KEY2", "GP1_SINTERSTORE_KEY3"};
  std::vector<std::string> value_to_dest;
  s = db.SInterstore("GP1_SINTERSTORE_DESTINATION1", gp1_keys, value_to_dest, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  ASSERT_TRUE(size_match(&db, "GP1_SINTERSTORE_DESTINATION1", 2));
  ASSERT_TRUE(members_match(&db, "GP1_SINTERSTORE_DESTINATION1", {"a", "c"}));

  // ***************** Group 2 Test *****************
  // the destination already exists, it is overwritten.
  // destination = {a, x, l}
  // key1 = {a, b, c, d}
  // key2 = {a, c}
  // key3 = {a, c, e}
  // SINTERSTORE destination key1 key2 key3
  // destination = {a, c}
  std::vector<std::string> gp2_destination_members{"a", "x", "l"};
  std::vector<std::string> gp2_members1{"a", "b", "c", "d"};
  std::vector<std::string> gp2_members2{"a", "c"};
  std::vector<std::string> gp2_members3{"a", "c", "e"};
  s = db.SAdd("GP2_SINTERSTORE_DESTINATION1", gp2_destination_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  s = db.SAdd("GP2_SINTERSTORE_KEY1", gp2_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP2_SINTERSTORE_KEY2", gp2_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SAdd("GP2_SINTERSTORE_KEY3", gp2_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp2_keys{"GP2_SINTERSTORE_KEY1", "GP2_SINTERSTORE_KEY2", "GP2_SINTERSTORE_KEY3"};
  s = db.SInterstore("GP2_SINTERSTORE_DESTINATION1", gp2_keys, value_to_dest, &ret);

  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  ASSERT_TRUE(size_match(&db, "GP2_SINTERSTORE_DESTINATION1", 2));
  ASSERT_TRUE(members_match(&db, "GP2_SINTERSTORE_DESTINATION1", {"a", "c"}));

  // ***************** Group 3 Test *****************
  // destination = {}
  // key1 = {a, b, c, d}
  // key2 = {a, c}
  // key3 = {a, c, e}
  // SINTERSTORE destination key1 key2 key3 not_exist_key
  // destination = {}
  std::vector<std::string> gp3_members1{"a", "b", "c", "d"};
  std::vector<std::string> gp3_members2{"a", "c"};
  std::vector<std::string> gp3_members3{"a", "c", "e"};
  s = db.SAdd("GP3_SINTERSTORE_KEY1", gp3_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP3_SINTERSTORE_KEY2", gp3_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SAdd("GP3_SINTERSTORE_KEY3", gp3_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp3_keys{"GP3_SINTERSTORE_KEY1", "GP3_SINTERSTORE_KEY2", "GP3_SINTERSTORE_KEY3",
                                    "GP3_SINTERSTORE_NOT_EXIST_KEY"};
  s = db.SInterstore("GP3_SINTERSTORE_DESTINATION1", gp3_keys, value_to_dest, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  ASSERT_TRUE(size_match(&db, "GP3_SINTERSTORE_DESTINATION1", 0));
  ASSERT_TRUE(members_match(&db, "GP3_SINTERSTORE_DESTINATION1", {}));

  // ***************** Group 4 Test *****************
  // destination = {}
  // key1 = {a, b, c, d}
  // key2 = {a, c}          (expire key);
  // key3 = {a, c, e}
  // SINTERSTORE destination key1 key2 key3
  // destination = {}
  std::vector<std::string> gp4_members1{"a", "b", "c", "d"};
  std::vector<std::string> gp4_members2{"a", "c"};
  std::vector<std::string> gp4_members3{"a", "c", "e"};
  s = db.SAdd("GP4_SINTERSTORE_KEY1", gp4_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP4_SINTERSTORE_KEY2", gp4_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SAdd("GP4_SINTERSTORE_KEY3", gp4_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  ASSERT_TRUE(make_expired(&db, "GP4_SINTERSTORE_KEY2"));

  std::vector<std::string> gp4_keys{"GP4_SINTERSTORE_KEY1", "GP4_SINTERSTORE_KEY2", "GP4_SINTERSTORE_KEY3"};
  s = db.SInterstore("GP4_SINTERSTORE_DESTINATION1", gp4_keys, value_to_dest, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  ASSERT_TRUE(size_match(&db, "GP4_SINTERSTORE_DESTINATION1", 0));
  ASSERT_TRUE(members_match(&db, "GP4_SINTERSTORE_DESTINATION1", {}));

  // ***************** Group 5 Test *****************
  // destination = {}
  // key1 = {a, b, c, d}    (expire key);
  // key2 = {a, c}
  // key3 = {a, c, e}
  // SINTERSTORE destination key1 key2 key3
  // destination = {}
  std::vector<std::string> gp5_members1{"a", "b", "c", "d"};
  std::vector<std::string> gp5_members2{"a", "c"};
  std::vector<std::string> gp5_members3{"a", "c", "e"};
  s = db.SAdd("GP5_SINTERSTORE_KEY1", gp5_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP5_SINTERSTORE_KEY2", gp5_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SAdd("GP5_SINTERSTORE_KEY3", gp5_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  ASSERT_TRUE(make_expired(&db, "GP5_SINTERSTORE_KEY1"));

  std::vector<std::string> gp5_keys{"GP5_SINTERSTORE_KEY1", "GP5_SINTERSTORE_KEY2", "GP5_SINTERSTORE_KEY3"};
  s = db.SInterstore("GP5_SINTERSTORE_DESTINATION1", gp5_keys, value_to_dest, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  ASSERT_TRUE(size_match(&db, "GP5_SINTERSTORE_DESTINATION1", 0));
  ASSERT_TRUE(members_match(&db, "GP5_SINTERSTORE_DESTINATION1", {}));

  // ***************** Group 6 Test *****************
  // destination = {}
  // key1 = {}
  // key2 = {a, c}
  // key3 = {a, c, e}
  // SINTERSTORE destination key1 key2 key3
  // destination = {}
  std::vector<std::string> gp6_members1{"a", "b", "c", "d"};
  std::vector<std::string> gp6_members2{"a", "c"};
  std::vector<std::string> gp6_members3{"a", "c", "e"};
  s = db.SAdd("GP6_SINTERSTORE_KEY1", gp6_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP6_SINTERSTORE_KEY2", gp6_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SAdd("GP6_SINTERSTORE_KEY3", gp6_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  s = db.SRem("GP6_SINTERSTORE_KEY1", gp6_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SCard("GP6_SINTERSTORE_KEY1", &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(ret, 0);

  std::vector<std::string> gp6_keys{"GP6_SINTERSTORE_KEY1", "GP6_SINTERSTORE_KEY2", "GP6_SINTERSTORE_KEY3"};
  s = db.SInterstore("GP6_SINTERSTORE_DESTINATION1", gp6_keys, value_to_dest, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  ASSERT_TRUE(size_match(&db, "GP6_SINTERSTORE_DESTINATION1", 0));
  ASSERT_TRUE(members_match(&db, "GP6_SINTERSTORE_DESTINATION1", {}));

  // ***************** Group 7 Test *****************
  // destination = {}
  // key1 = {a, b, c, d}
  // key2 = {a, c}
  // key3 = {a, c, e}
  // SINTERSTORE destination not_exist_key key1 key2 key3
  // destination = {}
  std::vector<std::string> gp7_members1{"a", "b", "c", "d"};
  std::vector<std::string> gp7_members2{"a", "c"};
  std::vector<std::string> gp7_members3{"a", "c", "e"};
  s = db.SAdd("GP7_SINTERSTORE_KEY1", gp7_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP7_SINTERSTORE_KEY2", gp7_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SAdd("GP7_SINTERSTORE_KEY3", gp7_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp7_keys{"GP7_SINTERSTORE_NOT_EXIST_KEY", "GP7_SINTERSTORE_KEY1", "GP7_SINTERSTORE_KEY2",
                                    "GP7_SINTERSTORE_KEY3"};
  s = db.SInterstore("GP7_SINTERSTORE_DESTINATION1", gp7_keys, value_to_dest, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  ASSERT_TRUE(size_match(&db, "GP7_SINTERSTORE_DESTINATION1", 0));
  ASSERT_TRUE(members_match(&db, "GP7_SINTERSTORE_DESTINATION1", {}));

  // ***************** Group 8 Test *****************
  // destination = {}
  // key1 = {a, b, c, d}
  // key2 = {a, b, c, d}
  // key3 = {a, b, c, d}
  // SINTERSTORE destination key1 key2 key3
  // destination = {a, b, c, d}
  std::vector<std::string> gp8_members1{"a", "b", "c", "d"};
  std::vector<std::string> gp8_members2{"a", "b", "c", "d"};
  std::vector<std::string> gp8_members3{"a", "b", "c", "d"};
  s = db.SAdd("GP8_SINTERSTORE_KEY1", gp8_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP8_SINTERSTORE_KEY2", gp8_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP8_SINTERSTORE_KEY3", gp8_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);

  std::vector<std::string> gp8_keys{
      "GP8_SINTERSTORE_KEY1",
      "GP8_SINTERSTORE_KEY2",
      "GP8_SINTERSTORE_KEY3",
  };
  std::vector<std::string> gp8_members_out;
  s = db.SInterstore("GP8_SINTERSTORE_DESTINATION1", gp8_keys, value_to_dest, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  ASSERT_TRUE(size_match(&db, "GP8_SINTERSTORE_DESTINATION1", 4));
  ASSERT_TRUE(members_match(&db, "GP8_SINTERSTORE_DESTINATION1", {"a", "b", "c", "d"}));
}

// SIsmember
TEST_F(SetsTest, SIsmemberTest) {  // NOLINT
  int32_t ret = 0;
  std::vector<std::string> members{"MEMBER"};
  s = db.SAdd("SISMEMBER_KEY", members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  // Not exist set key
  s = db.SIsmember("SISMEMBER_NOT_EXIST_KEY", "MEMBER", &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(ret, 0);

  // Not exist set member
  s = db.SIsmember("SISMEMBER_KEY", "NOT_EXIST_MEMBER", &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(ret, 0);

  s = db.SIsmember("SISMEMBER_KEY", "MEMBER", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  // Expire set key
  std::map<storage::DataType, rocksdb::Status> type_status;
  db.Expire("SISMEMBER_KEY", 1, &type_status);
  ASSERT_TRUE(type_status[storage::DataType::kSets].ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  s = db.SIsmember("SISMEMBER_KEY", "MEMBER", &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(ret, 0);
}

// SMembers
TEST_F(SetsTest, SMembersTest) {  // NOLINT
  int32_t ret = 0;
  std::vector<std::string> mid_members_in;
  mid_members_in.emplace_back("MID_MEMBER1");
  mid_members_in.emplace_back("MID_MEMBER2");
  mid_members_in.emplace_back("MID_MEMBER3");
  s = db.SAdd("B_SMEMBERS_KEY", mid_members_in, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> members_out;
  s = db.SMembers("B_SMEMBERS_KEY", &members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(members_out, mid_members_in));

  // Insert some kv who's position above "mid kv"
  std::vector<std::string> pre_members_in;
  pre_members_in.emplace_back("PRE_MEMBER1");
  pre_members_in.emplace_back("PRE_MEMBER2");
  pre_members_in.emplace_back("PRE_MEMBER3");
  s = db.SAdd("A_SMEMBERS_KEY", pre_members_in, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  members_out.clear();
  s = db.SMembers("B_SMEMBERS_KEY", &members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(members_out, mid_members_in));

  // Insert some kv who's position below "mid kv"
  std::vector<std::string> suf_members_in;
  suf_members_in.emplace_back("SUF_MEMBER1");
  suf_members_in.emplace_back("SUF_MEMBER2");
  suf_members_in.emplace_back("SUF_MEMBER3");
  s = db.SAdd("C_SMEMBERS_KEY", suf_members_in, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  members_out.clear();
  s = db.SMembers("B_SMEMBERS_KEY", &members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(members_out, mid_members_in));

  // SMembers timeout setes
  ASSERT_TRUE(make_expired(&db, "B_SMEMBERS_KEY"));
  ASSERT_TRUE(members_match(&db, "B_SMEMBERS_KEY", {}));

  // SMembers not exist setes
  ASSERT_TRUE(members_match(&db, "SMEMBERS_NOT_EXIST_KEY", {}));
}

// SMove
TEST_F(SetsTest, SMoveTest) {  // NOLINT
  int32_t ret = 0;
  // ***************** Group 1 Test *****************
  // source = {a, b, c, d}
  // destination = {a, c}
  // SMove source destination d
  // source = {a, b, c}
  // destination = {a, c, d}
  std::vector<std::string> gp1_source{"a", "b", "c", "d"};
  std::vector<std::string> gp1_destination{"a", "c"};
  s = db.SAdd("GP1_SMOVE_SOURCE", gp1_source, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP1_SMOVE_DESTINATION", gp1_destination, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);

  s = db.SMove("GP1_SMOVE_SOURCE", "GP1_SMOVE_DESTINATION", "d", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  ASSERT_TRUE(size_match(&db, "GP1_SMOVE_SOURCE", 3));
  ASSERT_TRUE(members_match(&db, "GP1_SMOVE_SOURCE", {"a", "b", "c"}));
  ASSERT_TRUE(size_match(&db, "GP1_SMOVE_DESTINATION", 3));
  ASSERT_TRUE(members_match(&db, "GP1_SMOVE_DESTINATION", {"a", "c", "d"}));

  // ***************** Group 2 Test *****************
  // source = {a, b, c, d}
  // destination = {a, c}   (expire key);
  // SMove source destination d
  // source = {a, b, c}
  // destination = {d}
  std::vector<std::string> gp2_source{"a", "b", "c", "d"};
  std::vector<std::string> gp2_destination{"a", "c"};
  s = db.SAdd("GP2_SMOVE_SOURCE", gp2_source, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP2_SMOVE_DESTINATION", gp2_destination, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);

  ASSERT_TRUE(make_expired(&db, "GP2_SMOVE_DESTINATION"));

  s = db.SMove("GP2_SMOVE_SOURCE", "GP2_SMOVE_DESTINATION", "d", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  ASSERT_TRUE(size_match(&db, "GP2_SMOVE_SOURCE", 3));
  ASSERT_TRUE(members_match(&db, "GP2_SMOVE_SOURCE", {"a", "b", "c"}));
  ASSERT_TRUE(size_match(&db, "GP2_SMOVE_DESTINATION", 1));
  ASSERT_TRUE(members_match(&db, "GP2_SMOVE_DESTINATION", {"d"}));

  // ***************** Group 3 Test *****************
  // source = {a, x, l}
  // destination = {}
  // SMove source destination x
  // source = {a, l}
  // destination = {x}
  std::vector<std::string> gp3_source{"a", "x", "l"};
  std::vector<std::string> gp3_destination{"a", "b"};
  s = db.SAdd("GP3_SMOVE_SOURCE", gp3_source, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  s = db.SAdd("GP3_SMOVE_DESTINATION", gp3_destination, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);

  s = db.SRem("GP3_SMOVE_DESTINATION", gp3_destination, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SCard("GP3_SMOVE_DESTINATION", &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(ret, 0);

  s = db.SMove("GP3_SMOVE_SOURCE", "GP3_SMOVE_DESTINATION", "x", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  ASSERT_TRUE(size_match(&db, "GP3_SMOVE_SOURCE", 2));
  ASSERT_TRUE(members_match(&db, "GP3_SMOVE_SOURCE", {"a", "l"}));
  ASSERT_TRUE(size_match(&db, "GP3_SMOVE_DESTINATION", 1));
  ASSERT_TRUE(members_match(&db, "GP3_SMOVE_DESTINATION", {"x"}));

  // ***************** Group 4 Test *****************
  // source = {a, x, l}
  // SMove source not_exist_key x
  // source = {a, l}
  // not_exist_key = {x}
  std::vector<std::string> gp4_source{"a", "x", "l"};
  s = db.SAdd("GP4_SMOVE_SOURCE", gp4_source, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  s = db.SMove("GP4_SMOVE_SOURCE", "GP4_SMOVE_NOT_EXIST_KEY", "x", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  ASSERT_TRUE(size_match(&db, "GP4_SMOVE_SOURCE", 2));
  ASSERT_TRUE(members_match(&db, "GP4_SMOVE_SOURCE", {"a", "l"}));
  ASSERT_TRUE(size_match(&db, "GP4_SMOVE_NOT_EXIST_KEY", 1));
  ASSERT_TRUE(members_match(&db, "GP4_SMOVE_NOT_EXIST_KEY", {"x"}));

  // ***************** Group 5 Test *****************
  // source = {}
  // destination = {a, x, l}
  // SMove source destination x
  // source = {}
  // destination = {a, x, l}
  std::vector<std::string> gp5_source{"a", "b"};
  std::vector<std::string> gp5_destination{"a", "x", "l"};
  s = db.SAdd("GP5_SMOVE_SOURCE", gp5_source, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SAdd("GP5_SMOVE_DESTINATION", gp5_destination, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  s = db.SRem("GP5_SMOVE_SOURCE", gp5_source, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SCard("GP5_SMOVE_SOURCE", &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(ret, 0);

  s = db.SMove("GP5_SMOVE_SOURCE", "GP5_SMOVE_DESTINATION", "x", &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(ret, 0);

  ASSERT_TRUE(size_match(&db, "GP5_SMOVE_SOURCE", 0));
  ASSERT_TRUE(members_match(&db, "GP5_SMOVE_SOURCE", {}));
  ASSERT_TRUE(size_match(&db, "GP5_SMOVE_DESTINATION", 3));
  ASSERT_TRUE(members_match(&db, "GP5_SMOVE_DESTINATION", {"a", "x", "l"}));

  // ***************** Group 6 Test *****************
  // source = {a, b, c, d}  (expire key);
  // destination = {a, c}
  // SMove source destination d
  // source = {}
  // destination = {d}
  std::vector<std::string> gp6_source{"a", "b", "c", "d"};
  std::vector<std::string> gp6_destination{"a", "c"};
  s = db.SAdd("GP6_SMOVE_SOURCE", gp6_source, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP6_SMOVE_DESTINATION", gp6_destination, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);

  ASSERT_TRUE(make_expired(&db, "GP6_SMOVE_SOURCE"));

  s = db.SMove("GP6_SMOVE_SOURCE", "GP6_SMOVE_DESTINATION", "d", &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(ret, 0);

  ASSERT_TRUE(size_match(&db, "GP6_SMOVE_SOURCE", 0));
  ASSERT_TRUE(members_match(&db, "GP6_SMOVE_SOURCE", {}));
  ASSERT_TRUE(size_match(&db, "GP6_SMOVE_DESTINATION", 2));
  ASSERT_TRUE(members_match(&db, "GP6_SMOVE_DESTINATION", {"a", "c"}));

  // ***************** Group 7 Test *****************
  // source = {a, b, c, d}
  // destination = {a, c}
  // SMove source destination x
  // source = {a, b, c, d}
  // destination = {a, c}
  std::vector<std::string> gp7_source{"a", "b", "c", "d"};
  std::vector<std::string> gp7_destination{"a", "c"};
  s = db.SAdd("GP7_SMOVE_SOURCE", gp7_source, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP7_SMOVE_DESTINATION", gp7_destination, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);

  s = db.SMove("GP7_SMOVE_SOURCE", "GP7_SMOVE_DESTINATION", "x", &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(ret, 0);

  ASSERT_TRUE(size_match(&db, "GP7_SMOVE_SOURCE", 4));
  ASSERT_TRUE(members_match(&db, "GP7_SMOVE_SOURCE", {"a", "b", "c", "d"}));
  ASSERT_TRUE(size_match(&db, "GP7_SMOVE_DESTINATION", 2));
  ASSERT_TRUE(members_match(&db, "GP7_SMOVE_DESTINATION", {"a", "c"}));

  // ***************** Group 8 Test *****************
  // source = {a, b, c, d}
  // destination = {a, c, d}
  // SMove source destination d
  // source = {a, b, c}
  // destination = {a, c, d}
  std::vector<std::string> gp8_source{"a", "b", "c", "d"};
  std::vector<std::string> gp8_destination{"a", "c", "d"};
  s = db.SAdd("GP8_SMOVE_SOURCE", gp8_source, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP8_SMOVE_DESTINATION", gp8_destination, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  s = db.SMove("GP8_SMOVE_SOURCE", "GP8_SMOVE_DESTINATION", "d", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  ASSERT_TRUE(size_match(&db, "GP8_SMOVE_SOURCE", 3));
  ASSERT_TRUE(members_match(&db, "GP8_SMOVE_SOURCE", {"a", "b", "c"}));
  ASSERT_TRUE(size_match(&db, "GP8_SMOVE_DESTINATION", 3));
  ASSERT_TRUE(members_match(&db, "GP8_SMOVE_DESTINATION", {"a", "c", "d"}));

  // ***************** Group 9 Test *****************
  // source = {a, b, c, d}
  // SMove source source d
  // source = {a, b, c, d}
  std::vector<std::string> gp9_source{"a", "b", "c", "d"};
  s = db.SAdd("GP9_SMOVE_SOURCE", gp8_source, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);

  s = db.SMove("GP9_SMOVE_SOURCE", "GP9_SMOVE_SOURCE", "d", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  ASSERT_TRUE(size_match(&db, "GP9_SMOVE_SOURCE", 4));
  ASSERT_TRUE(members_match(&db, "GP9_SMOVE_SOURCE", {"a", "b", "c", "d"}));
}

// SPop
TEST_F(SetsTest, SPopTest) {  // NOLINT
  int32_t ret = 0;
  std::vector<std::string> members;

  // ***************** Group 1 Test *****************
  std::vector<std::string> gp1_members{"gp1_aa", "gp1_bb", "gp1_cc"};
  s = db.SAdd("GP1_SPOP_KEY", gp1_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp1_out_all;
  s = db.SPop("GP1_SPOP_KEY", &members, 1);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP1_SPOP_KEY", 2));

  s = db.SPop("GP1_SPOP_KEY", &members, 1);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP1_SPOP_KEY", 1));

  s = db.SPop("GP1_SPOP_KEY", &members, 1);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP1_SPOP_KEY", 0));

  gp1_out_all.swap(members);
  members.clear();

  ASSERT_TRUE(size_match(&db, "GP1_SPOP_KEY", 0));
  ASSERT_TRUE(members_match(&db, "GP1_SPOP_KEY", {}));
  ASSERT_TRUE(members_match(gp1_out_all, gp1_members));

  // ***************** Group 2 Test *****************
  std::vector<std::string> gp2_members;
  for (int32_t idx = 1; idx <= 1; ++idx) {
    gp2_members.push_back("gb2_" + std::to_string(idx));
  }
  s = db.SAdd("GP2_SPOP_KEY", gp2_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  std::vector<std::string> gp2_out_all;
  for (int32_t idx = 1; idx <= 1; ++idx) {
    s = db.SPop("GP2_SPOP_KEY", &members, 1);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(size_match(&db, "GP2_SPOP_KEY", 1 - idx));
  }

  gp2_out_all.swap(members);
  members.clear();

  ASSERT_TRUE(size_match(&db, "GP2_SPOP_KEY", 0));
  ASSERT_TRUE(members_match(&db, "GP2_SPOP_KEY", {}));
  ASSERT_TRUE(members_match(gp2_out_all, gp2_members));

  // ***************** Group 3 Test *****************
  std::vector<std::string> gp3_members;
  for (int32_t idx = 1; idx <= 100; ++idx) {
    gp3_members.push_back("gb3_" + std::to_string(idx));
  }
  s = db.SAdd("GP3_SPOP_KEY", gp3_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 100);

  std::vector<std::string> gp3_out_all;
  for (int32_t idx = 1; idx <= 100; ++idx) {
    s = db.SPop("GP3_SPOP_KEY", &members, 1);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(size_match(&db, "GP3_SPOP_KEY", 100 - idx));
  }

  gp3_out_all.swap(members);
  members.clear();

  ASSERT_TRUE(size_match(&db, "GP3_SPOP_KEY", 0));
  ASSERT_TRUE(members_match(&db, "GP3_SPOP_KEY", {}));
  ASSERT_TRUE(members_match(gp3_out_all, gp3_members));

  // ***************** Group 4 Test *****************
  std::vector<std::string> gp4_members;
  for (int32_t idx = 1; idx <= 10000; ++idx) {
    gp4_members.push_back("gb4_" + std::to_string(idx));
  }
  s = db.SAdd("GP4_SPOP_KEY", gp4_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 10000);

  std::vector<std::string> gp4_out_all;
  for (int32_t idx = 1; idx <= 10000; ++idx) {
    s = db.SPop("GP4_SPOP_KEY", &members, 1);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(size_match(&db, "GP4_SPOP_KEY", 10000 - idx));
  }

  gp4_out_all.swap(members);
  members.clear();

  ASSERT_TRUE(size_match(&db, "GP4_SPOP_KEY", 0));
  ASSERT_TRUE(members_match(&db, "GP4_SPOP_KEY", {}));
  ASSERT_TRUE(members_match(gp4_out_all, gp4_members));

  // ***************** Group 5 Test *****************
  std::vector<std::string> gp5_members{"gp5_aa", "gp5_bb", "gp5_cc"};
  s = db.SAdd("GP5_SPOP_KEY", gp5_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  ASSERT_TRUE(make_expired(&db, "GP5_SPOP_KEY"));

  s = db.SPop("GP5_SPOP_KEY", &members, 1);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_TRUE(size_match(&db, "GP5_SPOP_KEY", 0));
  ASSERT_TRUE(members_match(&db, "GP5_SPOP_KEY", {}));

  // ***************** Group 6 Test *****************
  std::vector<std::string> gp6_members{"gp6_aa", "gp6_bb", "gp6_cc"};
  s = db.SAdd("GP6_SPOP_KEY", gp6_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  // Delete the key
  std::vector<std::string> del_keys = {"GP6_SPOP_KEY"};
  std::map<storage::DataType, storage::Status> type_status;
  db.Del(del_keys, &type_status);
  ASSERT_TRUE(type_status[storage::DataType::kSets].ok());

  s = db.SPop("GP6_SPOP_KEY", &members, 1);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_TRUE(size_match(&db, "GP6_SPOP_KEY", 0));
  ASSERT_TRUE(members_match(&db, "GP6_SPOP_KEY", {}));

  // ***************** Group 7 Test *****************
  std::vector<std::string> gp7_members{"gp7_aa", "gp7_bb", "gp7_cc"};
  s = db.SAdd("GP7_SPOP_KEY", gp7_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp7_out_all;
  s = db.SPop("GP7_SPOP_KEY", &members, 4);
  ASSERT_TRUE(s.ok());

  gp7_out_all.swap(members);
  members.clear();

  ASSERT_TRUE(size_match(&db, "GP7_SPOP_KEY", 0));
  ASSERT_TRUE(members_match(&db, "GP7_SPOP_KEY", {}));
  ASSERT_TRUE(members_match(gp7_out_all, gp7_members));
}

// SRandmember
TEST_F(SetsTest, SRanmemberTest) {  // NOLINT
  int32_t ret = 0;

  // ***************** Group 1 Test *****************
  std::vector<std::string> gp1_members{"gp1_aa", "gp1_bb", "gp1_cc", "gp1_dd", "gp1_ee", "gp1_ff", "gp1_gg", "gp1_hh"};
  s = db.SAdd("GP1_SRANDMEMBER_KEY", gp1_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 8);

  std::vector<std::string> gp1_out;

  s = db.SRandmember("GP1_SRANDMEMBER_KEY", 1, &gp1_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp1_out.size(), 1);
  ASSERT_TRUE(members_uniquen(gp1_out));
  ASSERT_TRUE(members_contains(gp1_out, gp1_members));

  s = db.SRandmember("GP1_SRANDMEMBER_KEY", 3, &gp1_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp1_out.size(), 3);
  ASSERT_TRUE(members_uniquen(gp1_out));
  ASSERT_TRUE(members_contains(gp1_out, gp1_members));

  s = db.SRandmember("GP1_SRANDMEMBER_KEY", 4, &gp1_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp1_out.size(), 4);
  ASSERT_TRUE(members_uniquen(gp1_out));
  ASSERT_TRUE(members_contains(gp1_out, gp1_members));

  s = db.SRandmember("GP1_SRANDMEMBER_KEY", 8, &gp1_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp1_out.size(), 8);
  ASSERT_TRUE(members_uniquen(gp1_out));
  ASSERT_TRUE(members_contains(gp1_out, gp1_members));

  s = db.SRandmember("GP1_SRANDMEMBER_KEY", 10, &gp1_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp1_out.size(), 8);
  ASSERT_TRUE(members_uniquen(gp1_out));
  ASSERT_TRUE(members_contains(gp1_out, gp1_members));

  s = db.SRandmember("GP1_SRANDMEMBER_KEY", -1, &gp1_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp1_out.size(), 1);
  ASSERT_TRUE(members_contains(gp1_out, gp1_members));

  s = db.SRandmember("GP1_SRANDMEMBER_KEY", -3, &gp1_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp1_out.size(), 3);
  ASSERT_TRUE(members_contains(gp1_out, gp1_members));

  s = db.SRandmember("GP1_SRANDMEMBER_KEY", -4, &gp1_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp1_out.size(), 4);
  ASSERT_TRUE(members_contains(gp1_out, gp1_members));

  s = db.SRandmember("GP1_SRANDMEMBER_KEY", -8, &gp1_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp1_out.size(), 8);
  ASSERT_TRUE(members_contains(gp1_out, gp1_members));

  s = db.SRandmember("GP1_SRANDMEMBER_KEY", -10, &gp1_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp1_out.size(), 10);
  ASSERT_TRUE(members_contains(gp1_out, gp1_members));

  // ***************** Group 2 Test *****************
  s = db.SAdd("GP2_SRANDMEMBER_KEY", {"MM"}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  std::vector<std::string> gp2_out;
  s = db.SRandmember("GP2_SRANDMEMBER_KEY", 1, &gp2_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp2_out.size(), 1);
  ASSERT_TRUE(members_match(gp2_out, {"MM"}));

  s = db.SRandmember("GP2_SRANDMEMBER_KEY", 3, &gp2_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp2_out.size(), 1);
  ASSERT_TRUE(members_match(gp2_out, {"MM"}));

  s = db.SRandmember("GP2_SRANDMEMBER_KEY", -1, &gp2_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp2_out.size(), 1);
  ASSERT_TRUE(members_match(gp2_out, {"MM"}));

  s = db.SRandmember("GP2_SRANDMEMBER_KEY", -3, &gp2_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp2_out.size(), 3);
  ASSERT_TRUE(members_match(gp2_out, {"MM", "MM", "MM"}));

  // ***************** Group 3 Test *****************
  std::vector<std::string> gp3_members{"gp1_aa", "gp1_bb", "gp1_cc", "gp1_dd", "gp1_ee", "gp1_ff", "gp1_gg", "gp1_hh"};
  s = db.SAdd("GP3_SRANDMEMBER_KEY", gp3_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 8);
  ASSERT_TRUE(make_expired(&db, "GP3_SRANDMEMBER_KEY"));

  std::vector<std::string> gp3_out;
  s = db.SRandmember("GP3_SRANDMEMBER_KEY", 1, &gp3_out);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(gp3_out.size(), 0);
  ASSERT_TRUE(members_match(gp3_out, {}));
}

// SRem
TEST_F(SetsTest, SRemTest) {  // NOLINT
  int32_t ret = 0;

  // ***************** Group 1 Test *****************
  std::vector<std::string> gp1_members{"a", "b", "c", "d"};
  s = db.SAdd("GP1_SREM_KEY", gp1_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);

  std::vector<std::string> gp1_del_members{"a", "b"};
  s = db.SRem("GP1_SREM_KEY", gp1_del_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);

  ASSERT_TRUE(size_match(&db, "GP1_SREM_KEY", 2));
  ASSERT_TRUE(members_match(&db, "GP1_SREM_KEY", {"c", "d"}));

  // ***************** Group 2 Test *****************
  // srem not exist members
  std::vector<std::string> gp2_members{"a", "b", "c", "d"};
  s = db.SAdd("GP2_SREM_KEY", gp2_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);

  std::vector<std::string> gp2_del_members{"e", "f"};
  s = db.SRem("GP2_SREM_KEY", gp2_del_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);

  ASSERT_TRUE(size_match(&db, "GP2_SREM_KEY", 4));
  ASSERT_TRUE(members_match(&db, "GP2_SREM_KEY", {"a", "b", "c", "d"}));

  // ***************** Group 3 Test *****************
  // srem not exist key
  std::vector<std::string> gp3_del_members{"a", "b", "c"};
  s = db.SRem("GP3_NOT_EXIST_KEY", gp3_del_members, &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(ret, 0);

  // ***************** Group 4 Test *****************
  // srem timeout key
  std::vector<std::string> gp4_members{"a", "b", "c", "d"};
  s = db.SAdd("GP4_SREM_KEY", gp4_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);

  ASSERT_TRUE(make_expired(&db, "GP4_SREM_KEY"));

  std::vector<std::string> gp4_del_members{"a", "b"};
  s = db.SRem("GP4_SREM_KEY", gp4_del_members, &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(ret, 0);

  ASSERT_TRUE(size_match(&db, "GP4_SREM_KEY", 0));
  ASSERT_TRUE(members_match(&db, "GP4_SREM_KEY", {}));
}

// SUnion
TEST_F(SetsTest, SUnionTest) {  // NOLINT
  int32_t ret = 0;

  // ***************** Group 1 Test *****************
  // key1 = {a, b, c, d}
  // key2 = {a, c}
  // key3 = {a, c, e}
  // SUNION key1 key2 key3  = {a, b, c, d, e}
  std::vector<std::string> gp1_members1{"a", "b", "c", "d"};
  std::vector<std::string> gp1_members2{"a", "c"};
  std::vector<std::string> gp1_members3{"a", "c", "e"};
  s = db.SAdd("GP1_SUNION_KEY1", gp1_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP1_SUNION_KEY2", gp1_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SAdd("GP1_SUNION_KEY3", gp1_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp1_keys{"GP1_SUNION_KEY1", "GP1_SUNION_KEY2", "GP1_SUNION_KEY3"};
  std::vector<std::string> gp1_members_out;
  s = db.SUnion(gp1_keys, &gp1_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(gp1_members_out, {"a", "b", "c", "d", "e"}));

  // key1 = {a, b, c, d}
  // key2 = {a, c}
  // key3 = {a, c, e}          (expire key);
  // SUNION key1 key2 key3  = {a, b, c, d}
  std::map<storage::DataType, rocksdb::Status> gp1_type_status;
  db.Expire("GP1_SUNION_KEY3", 1, &gp1_type_status);
  ASSERT_TRUE(gp1_type_status[storage::DataType::kSets].ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  gp1_members_out.clear();

  s = db.SUnion(gp1_keys, &gp1_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(gp1_members_out, {"a", "b", "c", "d"}));

  // ***************** Group 2 Test *****************
  // key1 = {a, b, c, d}
  // key2 = {a, c}
  // key3 = {a, c, e}
  // SUNION key1 key2 key3 not_exist_key = {a, b, c, d, e}
  std::vector<std::string> gp2_members1{"a", "b", "c", "d"};
  std::vector<std::string> gp2_members2{"a", "c"};
  std::vector<std::string> gp2_members3{"a", "c", "e"};
  s = db.SAdd("GP2_SUNION_KEY1", gp2_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP2_SUNION_KEY2", gp2_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SAdd("GP2_SUNION_KEY3", gp2_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp2_keys{"GP2_SUNION_KEY1", "GP2_SUNION_KEY2", "GP2_SUNION_KEY3", "GP2_NOT_EXIST_KEY"};
  std::vector<std::string> gp2_members_out;
  s = db.SUnion(gp2_keys, &gp2_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(gp2_members_out, {"a", "b", "c", "d", "e"}));

  // ***************** Group 3 Test *****************
  // key1 = {a, b, c, d}
  // key2 = {a, c}
  // key3 = {}
  // SUNION key1 key2 key3 = {a, b, c, d}
  std::vector<std::string> gp3_members1{"a", "b", "c", "d"};
  std::vector<std::string> gp3_members2{"a", "c"};
  std::vector<std::string> gp3_members3{"a", "c", "e", "f", "g"};
  s = db.SAdd("GP3_SUNION_KEY1", gp3_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP3_SUNION_KEY2", gp3_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SAdd("GP3_SUNION_KEY3", gp3_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 5);

  s = db.SRem("GP3_SUNION_KEY3", gp3_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 5);
  s = db.SCard("GP3_SUNION_KEY3", &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(ret, 0);
  std::vector<std::string> gp3_members_out;
  s = db.SMembers("GP3_SUNION_KEY3", &gp3_members_out);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(gp3_members_out.size(), 0);

  std::vector<std::string> gp3_keys{"GP3_SUNION_KEY1", "GP3_SUNION_KEY2", "GP3_SUNION_KEY3"};
  gp3_members_out.clear();
  s = db.SUnion(gp3_keys, &gp3_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(gp3_members_out, {"a", "b", "c", "d"}));

  // ***************** Group 4 Test *****************
  // key1 = {a, b, c, d}
  // SUNION key1 = {a, b, c, d}
  std::vector<std::string> gp4_members1{"a", "b", "c", "d"};
  s = db.SAdd("GP4_SUNION_KEY1", gp4_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);

  std::vector<std::string> gp4_keys{"GP4_SUNION_KEY1"};
  std::vector<std::string> gp4_members_out;
  s = db.SUnion(gp4_keys, &gp4_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(gp4_members_out, {"a", "b", "c", "d"}));
}

// SUnionstore
TEST_F(SetsTest, SUnionstoreTest) {  // NOLINT
  int32_t ret = 0;

  // ***************** Group 1 Test *****************
  // destination = {}
  // key1 = {a, b, c, d}
  // key2 = {a, c}
  // key3 = {a, c, e}
  // SUNIONSTORE destination key1 key2 key3
  // destination = {a, b, c, d, e}
  std::vector<std::string> gp1_members1{"a", "b", "c", "d"};
  std::vector<std::string> gp1_members2{"a", "c"};
  std::vector<std::string> gp1_members3{"a", "c", "e"};
  s = db.SAdd("GP1_SUNIONSTORE_KEY1", gp1_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP1_SUNIONSTORE_KEY2", gp1_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SAdd("GP1_SUNIONSTORE_KEY3", gp1_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp1_keys{"GP1_SUNIONSTORE_KEY1", "GP1_SUNIONSTORE_KEY2", "GP1_SUNIONSTORE_KEY3"};
  std::vector<std::string> value_to_dest;
  s = db.SUnionstore("GP1_SUNIONSTORE_DESTINATION1", gp1_keys, value_to_dest, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 5);
  ASSERT_TRUE(size_match(&db, "GP1_SUNIONSTORE_DESTINATION1", 5));
  ASSERT_TRUE(members_match(&db, "GP1_SUNIONSTORE_DESTINATION1", {"a", "b", "c", "d", "e"}));

  // destination = {}
  // key1 = {a, b, c, d}
  // key2 = {a, c}
  // key3 = {a, c, e}          (expire key);
  // SUNIONSTORE destination key1 key2 key3
  // destination = {a, b, c, d}
  ASSERT_TRUE(make_expired(&db, "GP1_SUNIONSTORE_KEY3"));

  s = db.SUnionstore("GP1_SUNIONSTORE_DESTINATION1", gp1_keys, value_to_dest, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  ASSERT_TRUE(size_match(&db, "GP1_SUNIONSTORE_DESTINATION1", 4));
  ASSERT_TRUE(members_match(&db, "GP1_SUNIONSTORE_DESTINATION1", {"a", "b", "c", "d"}));

  // ***************** Group 2 Test *****************
  // destination already exists, it is overwritten.
  // destination = {a, x, l}
  // key1 = {a, b, c, d}
  // key2 = {a, c}
  // key3 = {a, c, e}
  // SUNIONSTORE destination key1 key2 key3
  // destination = {a, b, c, d, e}
  std::vector<std::string> gp2_members1{"a", "b", "c", "d"};
  std::vector<std::string> gp2_members2{"a", "c"};
  std::vector<std::string> gp2_members3{"a", "c", "e"};
  s = db.SAdd("GP2_SUNIONSTORE_KEY1", gp2_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP2_SUNIONSTORE_KEY2", gp2_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SAdd("GP2_SUNIONSTORE_KEY3", gp2_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp2_keys{"GP2_SUNIONSTORE_KEY1", "GP2_SUNIONSTORE_KEY2", "GP2_SUNIONSTORE_KEY3"};
  s = db.SUnionstore("GP2_SUNIONSTORE_DESTINATION1", gp2_keys, value_to_dest, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 5);
  ASSERT_TRUE(size_match(&db, "GP2_SUNIONSTORE_DESTINATION1", 5));
  ASSERT_TRUE(members_match(&db, "GP2_SUNIONSTORE_DESTINATION1", {"a", "b", "c", "d", "e"}));

  // ***************** Group 3 Test *****************
  // destination = {}
  // key1 = {a, b, c, d}
  // key2 = {a, c}
  // key3 = {}
  // SUNIONSTORE destination key1 key2 key3
  // destination = {a, b, c, d}
  std::vector<std::string> gp3_members1{"a", "b", "c", "d"};
  std::vector<std::string> gp3_members2{"a", "c"};
  std::vector<std::string> gp3_members3{"a", "x", "l"};
  s = db.SAdd("GP3_SUNIONSTORE_KEY1", gp3_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP3_SUNIONSTORE_KEY2", gp3_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SAdd("GP3_SUNIONSTORE_KEY3", gp3_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  s = db.SRem("GP3_SUNIONSTORE_KEY3", gp3_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  ASSERT_TRUE(size_match(&db, "GP3_SUNIONSTORE_KEY3", 0));
  ASSERT_TRUE(members_match(&db, "GP3_SUNIONSTORE_KEY3", {}));

  std::vector<std::string> gp3_keys{"GP3_SUNIONSTORE_KEY1", "GP3_SUNIONSTORE_KEY2", "GP3_SUNIONSTORE_KEY3"};
  s = db.SUnionstore("GP3_SUNIONSTORE_DESTINATION1", gp3_keys, value_to_dest, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  ASSERT_TRUE(size_match(&db, "GP3_SUNIONSTORE_DESTINATION1", 4));
  ASSERT_TRUE(members_match(&db, "GP3_SUNIONSTORE_DESTINATION1", {"a", "b", "c", "d"}));

  // ***************** Group 4 Test *****************
  // destination = {}
  // key1 = {a, x, l}
  // SUNIONSTORE destination key1 not_exist_key
  // destination = {a, x, l}
  std::vector<std::string> gp4_members1{"a", "x", "l"};
  s = db.SAdd("GP4_SUNIONSTORE_KEY1", gp4_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp4_keys{"GP4_SUNIONSTORE_KEY1", "GP4_SUNIONSTORE_NOT_EXIST_KEY"};
  std::vector<std::string> gp4_members_out;
  s = db.SUnionstore("GP4_SUNIONSTORE_DESTINATION1", gp4_keys, value_to_dest, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  ASSERT_TRUE(size_match(&db, "GP4_SUNIONSTORE_DESTINATION1", 3));
  ASSERT_TRUE(members_match(&db, "GP4_SUNIONSTORE_DESTINATION1", {"a", "x", "l"}));
}

// SScan
TEST_F(SetsTest, SScanTest) {  // NOLINT
  int32_t ret = 0;
  int64_t cursor = 0;
  int64_t next_cursor = 0;
  std::vector<std::string> member_out;
  // ***************** Group 1 Test *****************
  // a b c d e f g h
  // 0 1 2 3 4 5 6 7
  std::vector<std::string> gp1_members{"a", "b", "c", "d", "e", "f", "g", "h"};
  s = db.SAdd("GP1_SSCAN_KEY", gp1_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 8);

  s = db.SScan("GP1_SSCAN_KEY", cursor, "*", 3, &member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(member_out.size(), 3);
  ASSERT_EQ(next_cursor, 3);
  ASSERT_TRUE(members_match(member_out, {"a", "b", "c"}));

  member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.SScan("GP1_SSCAN_KEY", cursor, "*", 3, &member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(member_out.size(), 3);
  ASSERT_EQ(next_cursor, 6);
  ASSERT_TRUE(members_match(member_out, {"d", "e", "f"}));

  member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.SScan("GP1_SSCAN_KEY", cursor, "*", 3, &member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(member_out.size(), 2);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(members_match(member_out, {"g", "h"}));

  // ***************** Group 2 Test *****************
  // a b c d e f g h
  // 0 1 2 3 4 5 6 7
  std::vector<std::string> gp2_members{"a", "b", "c", "d", "e", "f", "g", "h"};
  s = db.SAdd("GP2_SSCAN_KEY", gp2_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 8);

  member_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.SScan("GP2_SSCAN_KEY", cursor, "*", 1, &member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(member_out.size(), 1);
  ASSERT_EQ(next_cursor, 1);
  ASSERT_TRUE(members_match(member_out, {"a"}));

  member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.SScan("GP2_SSCAN_KEY", cursor, "*", 1, &member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(member_out.size(), 1);
  ASSERT_EQ(next_cursor, 2);
  ASSERT_TRUE(members_match(member_out, {"b"}));

  member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.SScan("GP2_SSCAN_KEY", cursor, "*", 1, &member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(member_out.size(), 1);
  ASSERT_EQ(next_cursor, 3);
  ASSERT_TRUE(members_match(member_out, {"c"}));

  member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.SScan("GP2_SSCAN_KEY", cursor, "*", 1, &member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(member_out.size(), 1);
  ASSERT_EQ(next_cursor, 4);
  ASSERT_TRUE(members_match(member_out, {"d"}));

  member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.SScan("GP2_SSCAN_KEY", cursor, "*", 1, &member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(member_out.size(), 1);
  ASSERT_EQ(next_cursor, 5);
  ASSERT_TRUE(members_match(member_out, {"e"}));

  member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.SScan("GP2_SSCAN_KEY", cursor, "*", 1, &member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(member_out.size(), 1);
  ASSERT_EQ(next_cursor, 6);
  ASSERT_TRUE(members_match(member_out, {"f"}));

  member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.SScan("GP2_SSCAN_KEY", cursor, "*", 1, &member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(member_out.size(), 1);
  ASSERT_EQ(next_cursor, 7);
  ASSERT_TRUE(members_match(member_out, {"g"}));

  member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.SScan("GP2_SSCAN_KEY", cursor, "*", 1, &member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(member_out.size(), 1);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(members_match(member_out, {"h"}));

  // ***************** Group 3 Test *****************
  // a b c d e f g h
  // 0 1 2 3 4 5 6 7
  std::vector<std::string> gp3_members{"a", "b", "c", "d", "e", "f", "g", "h"};
  s = db.SAdd("GP3_SSCAN_KEY", gp3_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 8);

  member_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.SScan("GP3_SSCAN_KEY", cursor, "*", 5, &member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(member_out.size(), 5);
  ASSERT_EQ(next_cursor, 5);
  ASSERT_TRUE(members_match(member_out, {"a", "b", "c", "d", "e"}));

  member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.SScan("GP3_SSCAN_KEY", cursor, "*", 5, &member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(member_out.size(), 3);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(members_match(member_out, {"f", "g", "h"}));

  // ***************** Group 4 Test *****************
  // a b c d e f g h
  // 0 1 2 3 4 5 6 7
  std::vector<std::string> gp4_members{"a", "b", "c", "d", "e", "f", "g", "h"};
  s = db.SAdd("GP4_SSCAN_KEY", gp4_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 8);

  member_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.SScan("GP4_SSCAN_KEY", cursor, "*", 10, &member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(member_out.size(), 8);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(members_match(member_out, {"a", "b", "c", "d", "e", "f", "g", "h"}));

  // ***************** Group 5 Test *****************
  // a_1_ a_2_ a_3_ b_1_ b_2_ b_3_ c_1_ c_2_ c_3
  //  0    1    2    3    4    5    6    7    8
  std::vector<std::string> gp5_members{"a_1_", "a_2_", "a_3_", "b_1_", "b_2_", "b_3_", "c_1_", "c_2_", "c_3_"};
  s = db.SAdd("GP5_SSCAN_KEY", gp5_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  member_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.SScan("GP5_SSCAN_KEY", cursor, "*1*", 3, &member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(member_out.size(), 1);
  ASSERT_EQ(next_cursor, 3);
  ASSERT_TRUE(members_match(member_out, {"a_1_"}));

  member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.SScan("GP5_SSCAN_KEY", cursor, "*1*", 3, &member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(member_out.size(), 1);
  ASSERT_EQ(next_cursor, 6);
  ASSERT_TRUE(members_match(member_out, {"b_1_"}));

  member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.SScan("GP5_SSCAN_KEY", cursor, "*1*", 3, &member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(member_out.size(), 1);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(members_match(member_out, {"c_1_"}));

  // ***************** Group 6 Test *****************
  // a_1_ a_2_ a_3_ b_1_ b_2_ b_3_ c_1_ c_2_ c_3_
  //  0    1    2    3    4    5    6    7    8
  std::vector<std::string> gp6_members{"a_1_", "a_2_", "a_3_", "b_1_", "b_2_", "b_3_", "c_1_", "c_2_", "c_3_"};
  s = db.SAdd("GP6_SSCAN_KEY", gp6_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  member_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.SScan("GP6_SSCAN_KEY", cursor, "a*", 3, &member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(member_out.size(), 3);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(members_match(member_out, {"a_1_", "a_2_", "a_3_"}));

  member_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.SScan("GP6_SSCAN_KEY", cursor, "a*", 2, &member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(member_out.size(), 2);
  ASSERT_EQ(next_cursor, 2);
  ASSERT_TRUE(members_match(member_out, {"a_1_", "a_2_"}));

  member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.SScan("GP6_SSCAN_KEY", cursor, "a*", 2, &member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(member_out.size(), 1);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(members_match(member_out, {"a_3_"}));

  member_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.SScan("GP6_SSCAN_KEY", cursor, "a*", 1, &member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(member_out.size(), 1);
  ASSERT_EQ(next_cursor, 1);
  ASSERT_TRUE(members_match(member_out, {"a_1_"}));

  member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.SScan("GP6_SSCAN_KEY", cursor, "a*", 1, &member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(member_out.size(), 1);
  ASSERT_EQ(next_cursor, 2);
  ASSERT_TRUE(members_match(member_out, {"a_2_"}));

  member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.SScan("GP6_SSCAN_KEY", cursor, "a*", 1, &member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(member_out.size(), 1);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(members_match(member_out, {"a_3_"}));

  // ***************** Group 7 Test *****************
  // a_1_ a_2_ a_3_ b_1_ b_2_ b_3_ c_1_ c_2_ c_3
  //  0    1    2    3    4    5    6    7    8
  std::vector<std::string> gp7_members{"a_1_", "a_2_", "a_3_", "b_1_", "b_2_", "b_3_", "c_1_", "c_2_", "c_3_"};
  s = db.SAdd("GP7_SSCAN_KEY", gp7_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  member_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.SScan("GP7_SSCAN_KEY", cursor, "b*", 3, &member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(member_out.size(), 3);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(members_match(member_out, {"b_1_", "b_2_", "b_3_"}));

  member_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.SScan("GP7_SSCAN_KEY", cursor, "b*", 2, &member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(member_out.size(), 2);
  ASSERT_EQ(next_cursor, 2);
  ASSERT_TRUE(members_match(member_out, {"b_1_", "b_2_"}));

  member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.SScan("GP7_SSCAN_KEY", cursor, "b*", 2, &member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(member_out.size(), 1);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(members_match(member_out, {"b_3_"}));

  member_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.SScan("GP7_SSCAN_KEY", cursor, "b*", 1, &member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(member_out.size(), 1);
  ASSERT_EQ(next_cursor, 1);
  ASSERT_TRUE(members_match(member_out, {"b_1_"}));

  member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.SScan("GP7_SSCAN_KEY", cursor, "b*", 1, &member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(member_out.size(), 1);
  ASSERT_EQ(next_cursor, 2);
  ASSERT_TRUE(members_match(member_out, {"b_2_"}));

  member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.SScan("GP7_SSCAN_KEY", cursor, "b*", 1, &member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(member_out.size(), 1);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(members_match(member_out, {"b_3_"}));

  // ***************** Group 8 Test *****************
  // a_1_ a_2_ a_3_ b_1_ b_2_ b_3_ c_1_ c_2_ c_3
  //  0    1    2    3    4    5    6    7    8
  std::vector<std::string> gp8_members{"a_1_", "a_2_", "a_3_", "b_1_", "b_2_", "b_3_", "c_1_", "c_2_", "c_3_"};
  s = db.SAdd("GP8_SSCAN_KEY", gp8_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  member_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.SScan("GP8_SSCAN_KEY", cursor, "c*", 3, &member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(member_out.size(), 3);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(members_match(member_out, {"c_1_", "c_2_", "c_3_"}));

  member_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.SScan("GP8_SSCAN_KEY", cursor, "c*", 2, &member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(member_out.size(), 2);
  ASSERT_EQ(next_cursor, 2);
  ASSERT_TRUE(members_match(member_out, {"c_1_", "c_2_"}));

  member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.SScan("GP8_SSCAN_KEY", cursor, "c*", 2, &member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(member_out.size(), 1);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(members_match(member_out, {"c_3_"}));

  member_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.SScan("GP8_SSCAN_KEY", cursor, "c*", 1, &member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(member_out.size(), 1);
  ASSERT_EQ(next_cursor, 1);
  ASSERT_TRUE(members_match(member_out, {"c_1_"}));

  member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.SScan("GP8_SSCAN_KEY", cursor, "c*", 1, &member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(member_out.size(), 1);
  ASSERT_EQ(next_cursor, 2);
  ASSERT_TRUE(members_match(member_out, {"c_2_"}));

  member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.SScan("GP8_SSCAN_KEY", cursor, "c*", 1, &member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(member_out.size(), 1);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(members_match(member_out, {"c_3_"}));

  // ***************** Group 9 Test *****************
  // a_1_ a_2_ a_3_ b_1_ b_2_ b_3_ c_1_ c_2_ c_3
  //  0    1    2    3    4    5    6    7    8
  std::vector<std::string> gp9_members{"a_1_", "a_2_", "a_3_", "b_1_", "b_2_", "b_3_", "c_1_", "c_2_", "c_3_"};
  s = db.SAdd("GP9_SSCAN_KEY", gp9_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  member_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.SScan("GP9_SSCAN_KEY", cursor, "d*", 3, &member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(member_out.size(), 0);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(members_match(member_out, {}));

  // ***************** Group 10 Test *****************
  // a_1_ a_2_ a_3_ b_1_ b_2_ b_3_ c_1_ c_2_ c_3
  //  0    1    2    3    4    5    6    7    8
  // SScan Expired Key
  std::vector<std::string> gp10_members{"a_1_", "a_2_", "a_3_", "b_1_", "b_2_", "b_3_", "c_1_", "c_2_", "c_3_"};
  s = db.SAdd("GP10_SSCAN_KEY", gp10_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  ASSERT_TRUE(make_expired(&db, "GP10_SSCAN_KEY"));
  member_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.SScan("GP10_SSCAN_KEY", cursor, "*", 10, &member_out, &next_cursor);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(member_out.size(), 0);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(members_match(member_out, {}));

  // ***************** Group 11 Test *****************
  // SScan Not Exist Key
  member_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.SScan("GP11_SSCAN_KEY", cursor, "*", 10, &member_out, &next_cursor);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(member_out.size(), 0);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(members_match(member_out, {}));
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
