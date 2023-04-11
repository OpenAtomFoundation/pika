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

class ZSetsTest : public ::testing::Test {
 public:
  ZSetsTest() {}
  virtual ~ZSetsTest() {}

  void SetUp() override {
    std::string path = "./db/zsets";
    if (access(path.c_str(), F_OK)) {
      mkdir(path.c_str(), 0755);
    }
    storage_options.options.create_if_missing = true;
    s = db.Open(storage_options, path);
    if (!s.ok()) {
      printf("Open db failed, exit...\n");
      exit(1);
    }
  }

  void TearDown() override {
    std::string path = "./db/zsets";
    DeleteFiles(path.c_str());
  }

  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

  StorageOptions storage_options;
  storage::Storage db;
  storage::Status s;
};

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

static bool score_members_match(storage::Storage* const db, const Slice& key,
                                const std::vector<storage::ScoreMember>& expect_sm) {
  std::vector<storage::ScoreMember> sm_out;
  Status s = db->ZRange(key, 0, -1, &sm_out);
  if (!s.ok() && !s.IsNotFound()) {
    return false;
  }
  if (sm_out.size() != expect_sm.size()) {
    return false;
  }
  if (s.IsNotFound() && expect_sm.empty()) {
    return true;
  }
  for (int idx = 0; idx < sm_out.size(); ++idx) {
    if (expect_sm[idx].score != sm_out[idx].score || expect_sm[idx].member != sm_out[idx].member) {
      return false;
    }
  }
  return true;
}

static bool score_members_match(const std::vector<storage::ScoreMember>& sm_out,
                                const std::vector<storage::ScoreMember>& expect_sm) {
  if (sm_out.size() != expect_sm.size()) {
    return false;
  }
  for (int idx = 0; idx < sm_out.size(); ++idx) {
    if (expect_sm[idx].score != sm_out[idx].score || expect_sm[idx].member != sm_out[idx].member) {
      return false;
    }
  }
  return true;
}

static bool size_match(storage::Storage* const db, const Slice& key, int32_t expect_size) {
  int32_t size = 0;
  Status s = db->ZCard(key, &size);
  if (!s.ok() && !s.IsNotFound()) {
    return false;
  }
  if (s.IsNotFound() && !expect_size) {
    return true;
  }
  return size == expect_size;
}

static bool make_expired(storage::Storage* const db, const Slice& key) {
  std::map<storage::DataType, rocksdb::Status> type_status;
  int ret = db->Expire(key, 1, &type_status);
  if (!ret || !type_status[storage::DataType::kZSets].ok()) {
    return false;
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  return true;
}

static bool delete_key(storage::Storage* const db, const Slice& key) {
  std::vector<std::string> del_keys = {key.ToString()};
  std::map<storage::DataType, storage::Status> type_status;
  db->Del(del_keys, &type_status);
  return type_status[storage::DataType::kZSets].ok();
}

// ZPopMax
TEST_F(ZSetsTest, ZPopMaxTest) {
  int32_t ret;
  std::map<DataType, int64_t> type_ttl;
  std::map<storage::DataType, rocksdb::Status> type_status;

  // ***************** Group 1 Test *****************
  // [-0.54,        MM4]
  // [0,            MM2]
  // [3.23,         MM1]
  // [8.0004,       MM3]
  std::vector<storage::ScoreMember> gp1_sm{{3.23, "MM1"}, {0, "MM2"}, {8.0004, "MM3"}, {-0.54, "MM4"}};
  Status s = db.ZAdd("GP1_ZPOPMAX_KEY", gp1_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(4, ret);
  ASSERT_TRUE(size_match(&db, "GP1_ZPOPMAX_KEY", 4));
  ASSERT_TRUE(
      score_members_match(&db, "GP1_ZPOPMAX_KEY", {{-0.54, "MM4"}, {0, "MM2"}, {3.23, "MM1"}, {8.0004, "MM3"}}));
  std::vector<storage::ScoreMember> score_members;
  s = db.ZPopMax("GP1_ZPOPMAX_KEY", 1, &score_members);

  // [-0.54,        MM4]             ret: [8.0004,        MM3]
  // [0,            MM2]
  // [3.23,         MM1]
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(1, score_members.size());
  ASSERT_TRUE(score_members_match(score_members, {{8.0004, "MM3"}}));
  ASSERT_TRUE(score_members_match(&db, "GP1_ZPOPMAX_KEY", {{-0.54, "MM4"}, {0, "MM2"}, {3.23, "MM1"}}));
  ASSERT_TRUE(size_match(&db, "GP1_ZPOPMAX_KEY", 3));
  s = db.ZPopMax("GP1_ZPOPMAX_KEY", 3, &score_members);

  //                                 ret: [3.23,          MM1]
  //                                      [0,             MM2]
  //                                      [-0.54,         MM4]
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(3, score_members.size());
  ASSERT_TRUE(score_members_match(score_members, {{3.23, "MM1"}, {0, "MM2"}, {-0.54, "MM4"}}));
  ASSERT_TRUE(size_match(&db, "GP1_ZPOPMAX_KEY", 0));
  ASSERT_TRUE(score_members_match(&db, "GP1_ZPOPMAX_KEY", {}));
  s = db.ZPopMax("GP1_ZPOPMAX_KEY", 1, &score_members);

  //                		     ret:
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(0, score_members.size());
  ASSERT_TRUE(size_match(&db, "GP1_ZPOPMAX_KEY", 0));
  ASSERT_TRUE(score_members_match(&db, "GP1_ZPOPMAX_KEY", {}));

  // ***************** Group 2 Test *****************
  // [0,            MM1]
  // [0,            MM2]
  // [0,            MM3]
  std::vector<storage::ScoreMember> gp2_sm{{0, "MM1"}, {0, "MM2"}, {0, "MM3"}};
  s = db.ZAdd("GP2_ZPOPMAX_KEY", gp2_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(3, ret);
  ASSERT_TRUE(size_match(&db, "GP2_ZPOPMAX_KEY", 3));
  ASSERT_TRUE(score_members_match(&db, "GP2_ZPOPMAX_KEY", {{0, "MM1"}, {0, "MM2"}, {0, "MM3"}}));
  s = db.ZPopMax("GP2_ZPOPMAX_KEY", 1, &score_members);

  // [0,            MM1]             ret: [0,             MM3]
  // [0,            MM2]
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(1, score_members.size());
  ASSERT_TRUE(size_match(&db, "GP2_ZPOPMAX_KEY", 2));
  ASSERT_TRUE(score_members_match(score_members, {{0, "MM3"}}));
  ASSERT_TRUE(score_members_match(&db, "GP2_ZPOPMAX_KEY", {{0, "MM1"}, {0, "MM2"}}));
  s = db.ZPopMax("GP2_ZPOPMAX_KEY", 3, &score_members);

  //                                 ret: [0,             MM2]
  //                                      [0,             MM1]
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(2, score_members.size());
  ASSERT_TRUE(size_match(&db, "GP2_ZPOPMAX_KEY", 0));
  ASSERT_TRUE(score_members_match(score_members, {{0, "MM2"}, {0, "MM1"}}));
  ASSERT_TRUE(score_members_match(&db, "GP2_ZPOPMAX_KEY", {}));

  // ***************** Group 3 Test *****************
  // [-1,           MM3]
  // [-1,           MM4]
  // [1 / 6.0,      MM5]
  // [1 / 6.0,      MM6]
  // [0.532445,     MM7]
  // [0.532445,     MM8]
  // [1,            MM1]
  // [1,            MM2]
  // [2e5 + 3.98,  MM10]
  // [2e5 + 3.98,   MM9]
  std::vector<storage::ScoreMember> gp3_sm{
      {1, "MM1"},       {1, "MM2"},        {-1, "MM3"},       {-1, "MM4"},         {1 / 6.0, "MM5"},
      {1 / 6.0, "MM6"}, {0.532445, "MM7"}, {0.532445, "MM8"}, {2e5 + 3.98, "MM9"}, {2e5 + 3.98, "MM10"}};
  s = db.ZAdd("GP3_ZPOPMAX_KEY", gp3_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(10, ret);
  ASSERT_TRUE(size_match(&db, "GP3_ZPOPMAX_KEY", 10));
  ASSERT_TRUE(score_members_match(&db, "GP3_ZPOPMAX_KEY",
                                  {{-1, "MM3"},
                                   {-1, "MM4"},
                                   {1 / 6.0, "MM5"},
                                   {1 / 6.0, "MM6"},
                                   {0.532445, "MM7"},
                                   {0.532445, "MM8"},
                                   {1, "MM1"},
                                   {1, "MM2"},
                                   {2e5 + 3.98, "MM10"},
                                   {2e5 + 3.98, "MM9"}}));
  s = db.ZPopMax("GP3_ZPOPMAX_KEY", 5, &score_members);

  // [-1,           MM3]             ret: [2e5 + 3.98,    MM9]
  // [-1,           MM4]                  [2e5 + 3.98,   MM10]
  // [1 / 6.0,      MM5]                  [1,             MM2]
  // [1 / 6.0,      MM6]                  [1,             MM1]
  // [0.532445,     MM7]                  [0.532445,      MM8]
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(5, score_members.size());
  ASSERT_TRUE(size_match(&db, "GP3_ZPOPMAX_KEY", 5));
  ASSERT_TRUE(score_members_match(
      score_members, {{2e5 + 3.98, "MM9"}, {2e5 + 3.98, "MM10"}, {1, "MM2"}, {1, "MM1"}, {0.532445, "MM8"}}));
  ASSERT_TRUE(score_members_match(&db, "GP3_ZPOPMAX_KEY",
                                  {{-1, "MM3"}, {-1, "MM4"}, {1 / 6.0, "MM5"}, {1 / 6.0, "MM6"}, {0.532445, "MM7"}}));

  // ***************** Group 4 Test *****************
  //
  s = db.ZPopMax("GP4_ZPOPMAX_KEY", 1, &score_members);

  //                                 ret:
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(0, score_members.size());

  // ***************** Group 5 Test *****************
  // [-1,           MM1]
  // [0,            MM2]
  // [1,            MM3]
  std::vector<storage::ScoreMember> gp5_sm1{{-1, "MM1"}, {0, "MM2"}, {1, "MM3"}};
  s = db.ZAdd("GP5_ZPOPMAX_KEY", gp5_sm1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(3, ret);
  ASSERT_TRUE(size_match(&db, "GP5_ZPOPMAX_KEY", 3));
  ASSERT_TRUE(score_members_match(&db, "GP5_ZPOPMAX_KEY", {{-1, "MM1"}, {0, "MM2"}, {1, "MM3"}}));
  ASSERT_TRUE(make_expired(&db, "GP5_ZPOPMAX_KEY"));
  ASSERT_TRUE(size_match(&db, "GP5_ZPOPMAX_KEY", 0));
  ASSERT_TRUE(score_members_match(&db, "GP5_ZPOPMAX_KEY", {}));
  s = db.ZPopMax("GP5_ZPOPMAX_KEY", 1, &score_members);

  //                                 ret:
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(0, score_members.size());
}

// ZPopMin
TEST_F(ZSetsTest, ZPopMinTest) {
  int32_t ret;
  std::map<DataType, int64_t> type_ttl;
  std::map<storage::DataType, rocksdb::Status> type_status;

  // ***************** Group 1 Test *****************
  // [-0.54,        MM4]
  // [0,            MM2]
  // [3.23,         MM1]
  // [8.0004,       MM3]
  std::vector<storage::ScoreMember> gp1_sm{{3.23, "MM1"}, {0, "MM2"}, {8.0004, "MM3"}, {-0.54, "MM4"}};
  Status s = db.ZAdd("GP1_ZPOPMIN_KEY", gp1_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(4, ret);
  ASSERT_TRUE(size_match(&db, "GP1_ZPOPMIN_KEY", 4));
  ASSERT_TRUE(
      score_members_match(&db, "GP1_ZPOPMIN_KEY", {{-0.54, "MM4"}, {0, "MM2"}, {3.23, "MM1"}, {8.0004, "MM3"}}));
  std::vector<storage::ScoreMember> score_members;
  s = db.ZPopMin("GP1_ZPOPMIN_KEY", 1, &score_members);

  // [0,            MM2]             ret: [-0.54,         MM4]
  // [3.23,         MM1]
  // [8.0004,       MM3]
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(1, score_members.size());
  ASSERT_TRUE(score_members_match(score_members, {{-0.54, "MM4"}}));
  ASSERT_TRUE(size_match(&db, "GP1_ZPOPMIN_KEY", 3));
  ASSERT_TRUE(score_members_match(&db, "GP1_ZPOPMIN_KEY", {{0, "MM2"}, {3.23, "MM1"}, {8.0004, "MM3"}}));
  s = db.ZPopMin("GP1_ZPOPMIN_KEY", 3, &score_members);

  //                                 ret: [0,             MM2]
  //                                      [3.23,          MM1]
  //                                      [8.0004,        MM3]
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(3, score_members.size());
  ASSERT_TRUE(score_members_match(score_members, {{0, "MM2"}, {3.23, "MM1"}, {8.0004, "MM3"}}));
  ASSERT_TRUE(size_match(&db, "GP1_ZPOPMIN_KEY", 0));
  ASSERT_TRUE(score_members_match(&db, "GP1_ZPOPMIN_KEY", {}));
  s = db.ZPopMin("GP1_ZPOPMIN_KEY", 1, &score_members);

  //                                 ret:
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(0, score_members.size());
  ASSERT_TRUE(size_match(&db, "GP1_ZPOPMIN_KEY", 0));
  ASSERT_TRUE(score_members_match(&db, "GP1_ZPOPMIN_KEY", {}));

  // ***************** Group 2 Test *****************
  // [0,            MM1]
  // [0,            MM2]
  // [0,            MM3]
  std::vector<storage::ScoreMember> gp2_sm{{0, "MM1"}, {0, "MM2"}, {0, "MM3"}};
  s = db.ZAdd("GP2_ZPOPMIN_KEY", gp2_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(3, ret);
  ASSERT_TRUE(size_match(&db, "GP2_ZPOPMIN_KEY", 3));
  ASSERT_TRUE(score_members_match(&db, "GP2_ZPOPMIN_KEY", {{0, "MM1"}, {0, "MM2"}, {0, "MM3"}}));
  s = db.ZPopMin("GP2_ZPOPMIN_KEY", 1, &score_members);

  // [0,            MM2]             ret: [0,        MM1]
  // [0,            MM3]
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(1, score_members.size());
  ASSERT_TRUE(size_match(&db, "GP2_ZPOPMIN_KEY", 2));
  ASSERT_TRUE(score_members_match(score_members, {{0, "MM1"}}));
  ASSERT_TRUE(score_members_match(&db, "GP2_ZPOPMIN_KEY", {{0, "MM2"}, {0, "MM3"}}));
  s = db.ZPopMin("GP2_ZPOPMIN_KEY", 3, &score_members);

  //                                 ret: [0,             MM2]
  //                                      [0,             MM3]
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_members.size(), 2);
  ASSERT_TRUE(size_match(&db, "GP2_ZPOPMIN_KEY", 0));
  ASSERT_TRUE(score_members_match(score_members, {{0, "MM2"}, {0, "MM3"}}));
  ASSERT_TRUE(score_members_match(&db, "GP2_ZPOPMIN_KEY", {}));

  // ***************** Group 3 Test *****************
  // [-1,           MM3]
  // [-1,           MM4]
  // [1 / 6.0,      MM5]
  // [1 / 6.0,      MM6]
  // [0.532445,     MM7]
  // [0.532445,     MM8]
  // [1,            MM1]
  // [1,            MM2]
  // [2e5 + 3.98,  MM10]
  // [2e5 + 3.98,   MM9]
  std::vector<storage::ScoreMember> gp3_sm{
      {1, "MM1"},       {1, "MM2"},        {-1, "MM3"},       {-1, "MM4"},         {1 / 6.0, "MM5"},
      {1 / 6.0, "MM6"}, {0.532445, "MM7"}, {0.532445, "MM8"}, {2e5 + 3.98, "MM9"}, {2e5 + 3.98, "MM10"}};
  s = db.ZAdd("GP3_ZPOPMIN_KEY", gp3_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(10, ret);
  ASSERT_TRUE(size_match(&db, "GP3_ZPOPMIN_KEY", 10));
  ASSERT_TRUE(score_members_match(&db, "GP3_ZPOPMIN_KEY",
                                  {{-1, "MM3"},
                                   {-1, "MM4"},
                                   {1 / 6.0, "MM5"},
                                   {1 / 6.0, "MM6"},
                                   {0.532445, "MM7"},
                                   {0.532445, "MM8"},
                                   {1, "MM1"},
                                   {1, "MM2"},
                                   {2e5 + 3.98, "MM10"},
                                   {2e5 + 3.98, "MM9"}}));
  s = db.ZPopMin("GP3_ZPOPMIN_KEY", 5, &score_members);

  // [0.532445,     MM8]             ret: [-1,            MM3]
  // [1,            MM1]                  [-1,            MM4]
  // [1,            MM2]                  [1 / 6.0,       MM5]
  // [2e5 + 3.98,  MM10]                  [1 / 6.0,       MM6]
  // [2e5 + 3.98,   MM9]                  [0.532445,      MM7]
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(5, score_members.size());
  ASSERT_TRUE(size_match(&db, "GP3_ZPOPMIN_KEY", 5));
  ASSERT_TRUE(score_members_match(
      &db, "GP3_ZPOPMIN_KEY", {{0.532445, "MM8"}, {1, "MM1"}, {1, "MM2"}, {2e5 + 3.98, "MM10"}, {2e5 + 3.98, "MM9"}}));
  ASSERT_TRUE(score_members_match(score_members,
                                  {{-1, "MM3"}, {-1, "MM4"}, {1 / 6.0, "MM5"}, {1 / 6.0, "MM6"}, {0.532445, "MM7"}}));

  // ***************** Group 4 Test *****************
  //
  s = db.ZPopMin("GP4_ZPOPMIN_KEY", 1, &score_members);

  //                                 ret:
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(0, score_members.size());

  // ***************** Group 5 Test *****************
  // [-1,           MM1]
  // [0,            MM2]
  // [1,            MM3]
  std::vector<storage::ScoreMember> gp5_sm1{{-1, "MM1"}, {0, "MM2"}, {1, "MM3"}};
  s = db.ZAdd("GP5_ZPOPMIN_KEY", gp5_sm1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(3, ret);
  ASSERT_TRUE(size_match(&db, "GP5_ZPOPMIN_KEY", 3));
  ASSERT_TRUE(score_members_match(&db, "GP5_ZPOPMIN_KEY", {{-1, "MM1"}, {0, "MM2"}, {1, "MM3"}}));
  ASSERT_TRUE(make_expired(&db, "GP5_ZPOPMIN_KEY"));
  ASSERT_TRUE(size_match(&db, "GP5_ZPOPMIN_KEY", 0));
  ASSERT_TRUE(score_members_match(&db, "GP5_ZPOPMIN_KEY", {}));
  s = db.ZPopMin("GP5_ZPOPMIN_KEY", 1, &score_members);

  //                                 ret:
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(0, score_members.size());
}

// ZAdd
TEST_F(ZSetsTest, ZAddTest) {
  int32_t ret;
  std::map<DataType, int64_t> type_ttl;
  std::map<storage::DataType, rocksdb::Status> type_status;

  // ***************** Group 1 Test *****************
  std::vector<storage::ScoreMember> gp1_sm{{3.23, "MM1"}, {0, "MM2"}, {8.0004, "MM3"}, {-0.54, "MM4"}};
  s = db.ZAdd("GP1_ZADD_KEY", gp1_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(4, ret);
  ASSERT_TRUE(size_match(&db, "GP1_ZADD_KEY", 4));
  ASSERT_TRUE(score_members_match(&db, "GP1_ZADD_KEY", {{-0.54, "MM4"}, {0, "MM2"}, {3.23, "MM1"}, {8.0004, "MM3"}}));

  // ***************** Group 2 Test *****************
  std::vector<storage::ScoreMember> gp2_sm{{0, "MM1"}, {0, "MM1"}, {0, "MM2"}, {0, "MM3"}};
  s = db.ZAdd("GP2_ZADD_KEY", gp2_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(3, ret);
  ASSERT_TRUE(size_match(&db, "GP2_ZADD_KEY", 3));
  ASSERT_TRUE(score_members_match(&db, "GP2_ZADD_KEY", {{0, "MM1"}, {0, "MM2"}, {0, "MM3"}}));

  // ***************** Group 3 Test *****************
  std::vector<storage::ScoreMember> gp3_sm{{1 / 1.0, "MM1"}, {1 / 3.0, "MM2"}, {1 / 6.0, "MM3"}, {1 / 7.0, "MM4"}};
  s = db.ZAdd("GP3_ZADD_KEY", gp3_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(4, ret);
  ASSERT_TRUE(size_match(&db, "GP3_ZADD_KEY", 4));
  ASSERT_TRUE(score_members_match(&db, "GP3_ZADD_KEY",
                                  {{1 / 7.0, "MM4"}, {1 / 6.0, "MM3"}, {1 / 3.0, "MM2"}, {1 / 1.0, "MM1"}}));

  // ***************** Group 4 Test *****************
  std::vector<storage::ScoreMember> gp4_sm{{-1 / 1.0, "MM1"}, {-1 / 3.0, "MM2"}, {-1 / 6.0, "MM3"}, {-1 / 7.0, "MM4"}};
  s = db.ZAdd("GP4_ZADD_KEY", gp4_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(4, ret);
  ASSERT_TRUE(size_match(&db, "GP4_ZADD_KEY", 4));
  ASSERT_TRUE(score_members_match(&db, "GP4_ZADD_KEY",
                                  {{-1 / 1.0, "MM1"}, {-1 / 3.0, "MM2"}, {-1 / 6.0, "MM3"}, {-1 / 7.0, "MM4"}}));

  // ***************** Group 5 Test *****************
  // [0, MM1]
  s = db.ZAdd("GP5_ZADD_KEY", {{0, "MM1"}}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(1, ret);
  ASSERT_TRUE(size_match(&db, "GP5_ZADD_KEY", 1));
  ASSERT_TRUE(score_members_match(&db, "GP5_ZADD_KEY", {{0, "MM1"}}));

  // [-0.5333, MM2]
  // [0,       MM1]
  s = db.ZAdd("GP5_ZADD_KEY", {{-0.5333, "MM2"}}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(1, ret);
  ASSERT_TRUE(size_match(&db, "GP5_ZADD_KEY", 2));
  ASSERT_TRUE(score_members_match(&db, "GP5_ZADD_KEY", {{-0.5333, "MM2"}, {0, "MM1"}}));

  // [-0.5333,      MM2]
  // [0,            MM1]
  // [1.79769e+308, MM3]
  s = db.ZAdd("GP5_ZADD_KEY", {{1.79769e+308, "MM3"}}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(1, ret);
  ASSERT_TRUE(size_match(&db, "GP5_ZADD_KEY", 3));
  ASSERT_TRUE(score_members_match(&db, "GP5_ZADD_KEY", {{-0.5333, "MM2"}, {0, "MM1"}, {1.79769e+308, "MM3"}}));

  // [-0.5333,      MM2]
  // [0,            MM1]
  // [50000,        MM4]
  // [1.79769e+308, MM3]
  s = db.ZAdd("GP5_ZADD_KEY", {{50000, "MM4"}}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(1, ret);
  ASSERT_TRUE(size_match(&db, "GP5_ZADD_KEY", 4));
  ASSERT_TRUE(
      score_members_match(&db, "GP5_ZADD_KEY", {{-0.5333, "MM2"}, {0, "MM1"}, {50000, "MM4"}, {1.79769e+308, "MM3"}}));

  // [-1.79769e+308, MM5]
  // [-0.5333,       MM2]
  // [0,             MM1]
  // [50000,         MM4]
  // [1.79769e+308,  MM3]
  s = db.ZAdd("GP5_ZADD_KEY", {{-1.79769e+308, "MM5"}}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(1, ret);
  ASSERT_TRUE(size_match(&db, "GP5_ZADD_KEY", 5));
  ASSERT_TRUE(score_members_match(
      &db, "GP5_ZADD_KEY",
      {{-1.79769e+308, "MM5"}, {-0.5333, "MM2"}, {0, "MM1"}, {50000, "MM4"}, {1.79769e+308, "MM3"}}));

  // [-1.79769e+308, MM5]
  // [-0.5333,       MM2]
  // [0,             MM1]
  // [0,             MM6]
  // [50000,         MM4]
  // [1.79769e+308,  MM3]
  s = db.ZAdd("GP5_ZADD_KEY", {{0, "MM6"}}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(1, ret);
  ASSERT_TRUE(size_match(&db, "GP5_ZADD_KEY", 6));
  ASSERT_TRUE(score_members_match(
      &db, "GP5_ZADD_KEY",
      {{-1.79769e+308, "MM5"}, {-0.5333, "MM2"}, {0, "MM1"}, {0, "MM6"}, {50000, "MM4"}, {1.79769e+308, "MM3"}}));

  // [-1.79769e+308, MM5]
  // [-0.5333,       MM2]
  // [0,             MM1]
  // [50000,         MM4]
  // [100000,        MM6]
  // [1.79769e+308,  MM3]
  s = db.ZAdd("GP5_ZADD_KEY", {{100000, "MM6"}}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(0, ret);
  ASSERT_TRUE(size_match(&db, "GP5_ZADD_KEY", 6));
  ASSERT_TRUE(score_members_match(
      &db, "GP5_ZADD_KEY",
      {{-1.79769e+308, "MM5"}, {-0.5333, "MM2"}, {0, "MM1"}, {50000, "MM4"}, {100000, "MM6"}, {1.79769e+308, "MM3"}}));

  // [-1.79769e+308, MM5]
  // [-0.5333,       MM2]
  // [-0.5333,       MM7]
  // [0,             MM1]
  // [50000,         MM4]
  // [100000,        MM6]
  // [1.79769e+308,  MM3]
  s = db.ZAdd("GP5_ZADD_KEY", {{-0.5333, "MM7"}}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(1, ret);
  ASSERT_TRUE(size_match(&db, "GP5_ZADD_KEY", 7));
  ASSERT_TRUE(score_members_match(&db, "GP5_ZADD_KEY",
                                  {{-1.79769e+308, "MM5"},
                                   {-0.5333, "MM2"},
                                   {-0.5333, "MM7"},
                                   {0, "MM1"},
                                   {50000, "MM4"},
                                   {100000, "MM6"},
                                   {1.79769e+308, "MM3"}}));

  // [-1.79769e+308, MM5]
  // [-0.5333,       MM2]
  // [-0.5333,       MM7]
  // [-1/3.0f,       MM8]
  // [0,             MM1]
  // [50000,         MM4]
  // [100000,        MM6]
  // [1.79769e+308,  MM3]
  s = db.ZAdd("GP5_ZADD_KEY", {{-1 / 3.0, "MM8"}}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(1, ret);
  ASSERT_TRUE(size_match(&db, "GP5_ZADD_KEY", 8));
  ASSERT_TRUE(score_members_match(&db, "GP5_ZADD_KEY",
                                  {{-1.79769e+308, "MM5"},
                                   {-0.5333, "MM2"},
                                   {-0.5333, "MM7"},
                                   {-1 / 3.0, "MM8"},
                                   {0, "MM1"},
                                   {50000, "MM4"},
                                   {100000, "MM6"},
                                   {1.79769e+308, "MM3"}}));

  // [-1.79769e+308, MM5]
  // [-0.5333,       MM2]
  // [-0.5333,       MM7]
  // [-1/3.0f,       MM8]
  // [0,             MM1]
  // [1/3.0f,        MM9]
  // [50000,         MM4]
  // [100000,        MM6]
  // [1.79769e+308,  MM3]
  s = db.ZAdd("GP5_ZADD_KEY", {{1 / 3.0, "MM9"}}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(1, ret);
  ASSERT_TRUE(size_match(&db, "GP5_ZADD_KEY", 9));
  ASSERT_TRUE(score_members_match(&db, "GP5_ZADD_KEY",
                                  {{-1.79769e+308, "MM5"},
                                   {-0.5333, "MM2"},
                                   {-0.5333, "MM7"},
                                   {-1 / 3.0, "MM8"},
                                   {0, "MM1"},
                                   {1 / 3.0, "MM9"},
                                   {50000, "MM4"},
                                   {100000, "MM6"},
                                   {1.79769e+308, "MM3"}}));

  // [0,  MM1]
  // [0,  MM2]
  // [0,  MM3]
  // [0,  MM4]
  // [0,  MM5]
  // [0,  MM6]
  // [0,  MM7]
  // [0,  MM8]
  // [0,  MM9]
  s = db.ZAdd(
      "GP5_ZADD_KEY",
      {{0, "MM1"}, {0, "MM2"}, {0, "MM3"}, {0, "MM4"}, {0, "MM5"}, {0, "MM6"}, {0, "MM7"}, {0, "MM8"}, {0, "MM9"}},
      &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(0, ret);
  ASSERT_TRUE(size_match(&db, "GP5_ZADD_KEY", 9));
  ASSERT_TRUE(score_members_match(
      &db, "GP5_ZADD_KEY",
      {{0, "MM1"}, {0, "MM2"}, {0, "MM3"}, {0, "MM4"}, {0, "MM5"}, {0, "MM6"}, {0, "MM7"}, {0, "MM8"}, {0, "MM9"}}));

  // ***************** Group 6 Test *****************
  std::vector<storage::ScoreMember> gp6_sm1{{-1, "MM1"}, {0, "MM2"}, {1, "MM3"}};
  s = db.ZAdd("GP6_ZADD_KEY", gp6_sm1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(3, ret);
  ASSERT_TRUE(size_match(&db, "GP6_ZADD_KEY", 3));
  ASSERT_TRUE(score_members_match(&db, "GP6_ZADD_KEY", {{-1, "MM1"}, {0, "MM2"}, {1, "MM3"}}));
  ASSERT_TRUE(make_expired(&db, "GP6_ZADD_KEY"));
  ASSERT_TRUE(size_match(&db, "GP6_ZADD_KEY", 0));
  ASSERT_TRUE(score_members_match(&db, "GP6_ZADD_KEY", {}));

  std::vector<storage::ScoreMember> gp6_sm2{{-100, "MM1"}, {0, "MM2"}, {100, "MM3"}};
  s = db.ZAdd("GP6_ZADD_KEY", gp6_sm2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(3, ret);
  ASSERT_TRUE(size_match(&db, "GP6_ZADD_KEY", 3));
  ASSERT_TRUE(score_members_match(&db, "GP6_ZADD_KEY", {{-100, "MM1"}, {0, "MM2"}, {100, "MM3"}}));

  // ***************** Group 7 Test *****************
  std::vector<storage::ScoreMember> gp7_sm1{{-0.123456789, "MM1"}, {0, "MM2"}, {0.123456789, "MM3"}};
  s = db.ZAdd("GP7_ZADD_KEY", gp7_sm1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(3, ret);
  ASSERT_TRUE(size_match(&db, "GP7_ZADD_KEY", 3));
  ASSERT_TRUE(score_members_match(&db, "GP7_ZADD_KEY", {{-0.123456789, "MM1"}, {0, "MM2"}, {0.123456789, "MM3"}}));
  ASSERT_TRUE(delete_key(&db, "GP7_ZADD_KEY"));
  ASSERT_TRUE(size_match(&db, "GP7_ZADD_KEY", 0));
  ASSERT_TRUE(score_members_match(&db, "GP7_ZADD_KEY", {}));

  std::vector<storage::ScoreMember> gp7_sm2{{-1234.56789, "MM1"}, {0, "MM2"}, {1234.56789, "MM3"}};
  s = db.ZAdd("GP7_ZADD_KEY", gp7_sm2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(3, ret);
  ASSERT_TRUE(size_match(&db, "GP7_ZADD_KEY", 3));
  ASSERT_TRUE(score_members_match(&db, "GP7_ZADD_KEY", {{-1234.56789, "MM1"}, {0, "MM2"}, {1234.56789, "MM3"}}));

  s = db.ZAdd("GP7_ZADD_KEY", {{1234.56789, "MM1"}}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(0, ret);
  ASSERT_TRUE(size_match(&db, "GP7_ZADD_KEY", 3));
  ASSERT_TRUE(score_members_match(&db, "GP7_ZADD_KEY", {{0, "MM2"}, {1234.56789, "MM1"}, {1234.56789, "MM3"}}));

  // ***************** Group 8 Test *****************
  std::vector<storage::ScoreMember> gp8_sm1{{1, "MM1"}};
  std::vector<storage::ScoreMember> gp8_sm2{{2, "MM2"}};
  s = db.ZAdd("GP8_ZADD_KEY", gp8_sm1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(1, ret);
  ASSERT_TRUE(size_match(&db, "GP8_ZADD_KEY", 1));
  ASSERT_TRUE(score_members_match(&db, "GP8_ZADD_KEY", {{1, "MM1"}}));

  type_status.clear();
  ret = db.Expire("GP8_ZADD_KEY", 100, &type_status);
  ASSERT_EQ(ret, 1);
  ASSERT_TRUE(type_status[storage::DataType::kZSets].ok());

  type_status.clear();
  type_ttl = db.TTL("GP8_ZADD_KEY", &type_status);
  ASSERT_LE(type_ttl[kZSets], 100);
  ASSERT_GE(type_ttl[kZSets], 0);

  s = db.ZRem("GP8_ZADD_KEY", {"MM1"}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.ZAdd("GP8_ZADD_KEY", gp8_sm2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(1, ret);
  ASSERT_TRUE(size_match(&db, "GP8_ZADD_KEY", 1));
  ASSERT_TRUE(score_members_match(&db, "GP8_ZADD_KEY", {{2, "MM2"}}));

  type_status.clear();
  type_ttl = db.TTL("GP8_ZADD_KEY", &type_status);
  ASSERT_EQ(type_ttl[kZSets], -1);
}

// ZCard
TEST_F(ZSetsTest, ZCardTest) {
  int32_t ret;
  double score;

  // ***************** Group 1 Test *****************
  std::vector<storage::ScoreMember> gp1_sm{{-1, "MM1"}, {-2, "MM2"}, {-3, "MM3"}, {-4, "MM4"}};
  s = db.ZAdd("GP1_ZCARD_KEY", gp1_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(4, ret);
  ASSERT_TRUE(size_match(&db, "GP1_ZCARD_KEY", 4));
  ASSERT_TRUE(score_members_match(&db, "GP1_ZCARD_KEY", {{-4, "MM4"}, {-3, "MM3"}, {-2, "MM2"}, {-1, "MM1"}}));
  s = db.ZCard("GP1_ZCARD_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(4, ret);

  // ***************** Group 2 Test *****************
  std::vector<storage::ScoreMember> gp2_sm{{1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}};
  s = db.ZAdd("GP2_ZCARD_KEY", gp2_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(5, ret);
  ASSERT_TRUE(size_match(&db, "GP2_ZCARD_KEY", 5));
  ASSERT_TRUE(score_members_match(&db, "GP2_ZCARD_KEY", {{1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}}));
  s = db.ZCard("GP2_ZCARD_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(5, ret);

  // ***************** Group 3 Test *****************
  std::vector<storage::ScoreMember> gp3_sm{{1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}};
  s = db.ZAdd("GP3_ZCARD_KEY", gp3_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(5, ret);
  ASSERT_TRUE(size_match(&db, "GP3_ZCARD_KEY", 5));
  ASSERT_TRUE(score_members_match(&db, "GP3_ZCARD_KEY", {{1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}}));
  ASSERT_TRUE(make_expired(&db, "GP3_ZCARD_KEY"));
  s = db.ZCard("GP3_ZCARD_KEY", &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(0, ret);

  // ***************** Group 4 Test *****************
  s = db.ZCard("GP4_ZCARD_KEY", &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(0, ret);
}

// ZCount
TEST_F(ZSetsTest, ZCountTest) {
  int32_t ret;

  // ***************** Group 1 Test *****************
  std::vector<storage::ScoreMember> gp1_sm{{101010.1010101, "MM1"}, {101010.0101010, "MM2"}, {-100.000000001, "MM3"},
                                           {-100.000000002, "MM4"}, {-100.000000001, "MM5"}, {-100.000000002, "MM6"}};
  s = db.ZAdd("GP1_ZCOUNT_KEY", gp1_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(6, ret);
  ASSERT_TRUE(size_match(&db, "GP1_ZCOUNT_KEY", 6));
  ASSERT_TRUE(score_members_match(&db, "GP1_ZCOUNT_KEY",
                                  {{-100.000000002, "MM4"},
                                   {-100.000000002, "MM6"},
                                   {-100.000000001, "MM3"},
                                   {-100.000000001, "MM5"},
                                   {101010.0101010, "MM2"},
                                   {101010.1010101, "MM1"}}));

  s = db.ZCount("GP1_ZCOUNT_KEY", -100.000000002, 101010.1010101, true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 6);

  s = db.ZCount("GP1_ZCOUNT_KEY", -100.000000002, 101010.1010101, false, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);

  s = db.ZCount("GP1_ZCOUNT_KEY", -100.000000002, 101010.1010101, true, false, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 5);

  s = db.ZCount("GP1_ZCOUNT_KEY", -100.000000002, 101010.1010101, false, false, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  s = db.ZCount("GP1_ZCOUNT_KEY", -100000000, 100000000, true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 6);

  s = db.ZCount("GP1_ZCOUNT_KEY", -100000000, 100000000, false, false, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 6);

  s = db.ZCount("GP1_ZCOUNT_KEY", -100.000000002, -100.000000002, true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);

  s = db.ZCount("GP1_ZCOUNT_KEY", -100.000000002, -100.000000002, false, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);

  s = db.ZCount("GP1_ZCOUNT_KEY", -100.000000002, -100.000000002, true, false, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);

  s = db.ZCount("GP1_ZCOUNT_KEY", -100.000000001, -100.000000001, true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);

  s = db.ZCount("GP1_ZCOUNT_KEY", -100000000, 100, true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);

  s = db.ZCount("GP1_ZCOUNT_KEY", -100.000000001, 100000000, true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);

  s = db.ZCount("GP1_ZCOUNT_KEY", -100.000000001, 100000000, false, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);

  // ***************** Group 2 Test *****************
  std::vector<storage::ScoreMember> gp2_sm{{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"},
                                           {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}};
  s = db.ZAdd("GP2_ZCOUNT_KEY", gp2_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP2_ZCOUNT_KEY", 9));
  ASSERT_TRUE(score_members_match(
      &db, "GP2_ZCOUNT_KEY",
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));
  ASSERT_TRUE(make_expired(&db, "GP2_ZCOUNT_KEY"));
  s = db.ZCount("GP2_ZCOUNT_KEY", -100000000, 100000000, true, true, &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(ret, 0);

  // ***************** Group 3 Test *****************
  s = db.ZCount("GP3_ZCOUNT_KEY", -100000000, 100000000, true, true, &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(ret, 0);

  // ***************** Group 4 Test *****************
  std::vector<storage::ScoreMember> gp4_sm{{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"},
                                           {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}};
  s = db.ZAdd("GP4_ZCOUNT_KEY", gp4_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP4_ZCOUNT_KEY", 9));
  ASSERT_TRUE(score_members_match(
      &db, "GP4_ZCOUNT_KEY",
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  s = db.ZCount("GP4_ZCOUNT_KEY", -100, -50, true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);

  s = db.ZCount("GP4_ZCOUNT_KEY", -100, 0, true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.ZCount("GP4_ZCOUNT_KEY", -100, 0, true, false, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);

  s = db.ZCount("GP4_ZCOUNT_KEY", -100, 4, true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 5);

  s = db.ZCount("GP4_ZCOUNT_KEY", -100, 4, true, false, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);

  s = db.ZCount("GP4_ZCOUNT_KEY", 0, 8, true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  s = db.ZCount("GP4_ZCOUNT_KEY", 0, 8, false, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 8);

  s = db.ZCount("GP4_ZCOUNT_KEY", 0, 8, true, false, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 8);

  s = db.ZCount("GP4_ZCOUNT_KEY", 0, 8, false, false, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 7);

  s = db.ZCount("GP4_ZCOUNT_KEY", 3, 5, true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  s = db.ZCount("GP4_ZCOUNT_KEY", 3, 5, false, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);

  s = db.ZCount("GP4_ZCOUNT_KEY", 3, 5, true, false, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);

  s = db.ZCount("GP4_ZCOUNT_KEY", 3, 5, false, false, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.ZCount("GP4_ZCOUNT_KEY", 100, 100, true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);

  s = db.ZCount("GP4_ZCOUNT_KEY", 0, 0, true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.ZCount("GP4_ZCOUNT_KEY", 0, 0, false, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);

  s = db.ZCount("GP4_ZCOUNT_KEY", 8, 8, true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.ZCount("GP4_ZCOUNT_KEY", 7, 8, true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);

  s = db.ZCount("GP4_ZCOUNT_KEY", 7, 8, false, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.ZCount("GP4_ZCOUNT_KEY", 7, 8, true, false, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.ZCount("GP4_ZCOUNT_KEY", 7, 8, false, false, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
}

// ZIncrby
TEST_F(ZSetsTest, ZIncrbyTest) {
  int32_t ret;
  double score;
  std::map<DataType, int64_t> type_ttl;
  std::map<storage::DataType, rocksdb::Status> type_status;

  // ***************** Group 1 Test *****************
  std::vector<storage::ScoreMember> gp1_sm{{101010.1010101, "MM1"}, {101010.0101010, "MM2"}};
  s = db.ZAdd("GP1_ZINCRBY_KEY", gp1_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(2, ret);
  ASSERT_TRUE(size_match(&db, "GP1_ZINCRBY_KEY", 2));
  ASSERT_TRUE(score_members_match(&db, "GP1_ZINCRBY_KEY", {{101010.0101010, "MM2"}, {101010.1010101, "MM1"}}));

  s = db.ZIncrby("GP1_ZINCRBY_KEY", "MM1", -0.1010101, &score);
  ASSERT_TRUE(s.ok());
  ASSERT_DOUBLE_EQ(score, 101010);

  s = db.ZIncrby("GP1_ZINCRBY_KEY", "MM2", -0.0101010, &score);
  ASSERT_TRUE(s.ok());
  ASSERT_DOUBLE_EQ(score, 101010);

  s = db.ZIncrby("GP1_ZINCRBY_KEY", "MM3", 101010, &score);
  ASSERT_TRUE(s.ok());
  ASSERT_DOUBLE_EQ(score, 101010);

  ASSERT_TRUE(size_match(&db, "GP1_ZINCRBY_KEY", 3));
  ASSERT_TRUE(score_members_match(&db, "GP1_ZINCRBY_KEY", {{101010, "MM1"}, {101010, "MM2"}, {101010, "MM3"}}));

  // ***************** Group 2 Test *****************
  std::vector<storage::ScoreMember> gp2_sm{{101010.1010101010, "MM1"}};
  s = db.ZAdd("GP2_ZINCRBY_KEY", gp2_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(1, ret);
  ASSERT_TRUE(size_match(&db, "GP2_ZINCRBY_KEY", 1));
  ASSERT_TRUE(score_members_match(&db, "GP2_ZINCRBY_KEY", {{101010.1010101010, "MM1"}}));

  s = db.ZIncrby("GP2_ZINCRBY_KEY", "MM1", 0.0101010101, &score);
  ASSERT_TRUE(s.ok());
  ASSERT_DOUBLE_EQ(score, 101010.1111111111);

  s = db.ZIncrby("GP2_ZINCRBY_KEY", "MM1", -0.11111, &score);
  ASSERT_TRUE(s.ok());
  ASSERT_DOUBLE_EQ(score, 101010.0000011111);

  s = db.ZIncrby("GP2_ZINCRBY_KEY", "MM1", -0.0000011111, &score);
  ASSERT_TRUE(s.ok());
  ASSERT_DOUBLE_EQ(score, 101010);

  s = db.ZIncrby("GP2_ZINCRBY_KEY", "MM1", 101010, &score);
  ASSERT_TRUE(s.ok());
  ASSERT_DOUBLE_EQ(score, 202020);

  ASSERT_TRUE(size_match(&db, "GP2_ZINCRBY_KEY", 1));
  ASSERT_TRUE(score_members_match(&db, "GP2_ZINCRBY_KEY", {{202020, "MM1"}}));

  // ***************** Group 3 Test *****************
  std::vector<storage::ScoreMember> gp3_sm{{1, "MM1"}, {2, "MM2"}, {3, "MM3"}};
  s = db.ZAdd("GP3_ZINCRBY_KEY", gp3_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(3, ret);
  ASSERT_TRUE(size_match(&db, "GP3_ZINCRBY_KEY", 3));
  ASSERT_TRUE(score_members_match(&db, "GP3_ZINCRBY_KEY", {{1, "MM1"}, {2, "MM2"}, {3, "MM3"}}));

  ASSERT_TRUE(make_expired(&db, "GP3_ZINCRBY_KEY"));
  ASSERT_TRUE(size_match(&db, "GP3_ZINCRBY_KEY", 0));
  ASSERT_TRUE(score_members_match(&db, "GP3_ZINCRBY_KEY", {}));

  s = db.ZIncrby("GP3_ZINCRBY_KEY", "MM1", 101010.010101, &score);
  ASSERT_TRUE(s.ok());
  ASSERT_DOUBLE_EQ(score, 101010.010101);
  ASSERT_TRUE(size_match(&db, "GP3_ZINCRBY_KEY", 1));
  ASSERT_TRUE(score_members_match(&db, "GP3_ZINCRBY_KEY", {{101010.010101, "MM1"}}));

  s = db.ZIncrby("GP3_ZINCRBY_KEY", "MM2", 202020.020202, &score);
  ASSERT_TRUE(s.ok());
  ASSERT_DOUBLE_EQ(score, 202020.020202);
  ASSERT_TRUE(size_match(&db, "GP3_ZINCRBY_KEY", 2));
  ASSERT_TRUE(score_members_match(&db, "GP3_ZINCRBY_KEY", {{101010.010101, "MM1"}, {202020.020202, "MM2"}}));

  s = db.ZIncrby("GP3_ZINCRBY_KEY", "MM3", 303030.030303, &score);
  ASSERT_TRUE(s.ok());
  ASSERT_DOUBLE_EQ(score, 303030.030303);
  ASSERT_TRUE(size_match(&db, "GP3_ZINCRBY_KEY", 3));
  ASSERT_TRUE(score_members_match(&db, "GP3_ZINCRBY_KEY",
                                  {{101010.010101, "MM1"}, {202020.020202, "MM2"}, {303030.030303, "MM3"}}));

  s = db.ZIncrby("GP3_ZINCRBY_KEY", "MM1", 303030.030303, &score);
  ASSERT_TRUE(s.ok());
  ASSERT_DOUBLE_EQ(score, 404040.040404);
  ASSERT_TRUE(size_match(&db, "GP3_ZINCRBY_KEY", 3));
  ASSERT_TRUE(score_members_match(&db, "GP3_ZINCRBY_KEY",
                                  {{202020.020202, "MM2"}, {303030.030303, "MM3"}, {404040.040404, "MM1"}}));

  // ***************** Group 4 Test *****************
  s = db.ZIncrby("GP4_ZINCRBY_KEY", "MM1", -101010.010101, &score);
  ASSERT_TRUE(s.ok());
  ASSERT_DOUBLE_EQ(score, -101010.010101);
  ASSERT_TRUE(size_match(&db, "GP4_ZINCRBY_KEY", 1));
  ASSERT_TRUE(score_members_match(&db, "GP4_ZINCRBY_KEY", {{-101010.010101, "MM1"}}));

  s = db.ZIncrby("GP4_ZINCRBY_KEY", "MM2", 101010.010101, &score);
  ASSERT_TRUE(s.ok());
  ASSERT_DOUBLE_EQ(score, 101010.010101);
  ASSERT_TRUE(size_match(&db, "GP4_ZINCRBY_KEY", 2));
  ASSERT_TRUE(score_members_match(&db, "GP4_ZINCRBY_KEY", {{-101010.010101, "MM1"}, {101010.010101, "MM2"}}));

  // ***************** Group 5 Test *****************
  s = db.ZAdd("GP5_ZINCRBY_KEY", {{1, "MM1"}}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  ASSERT_TRUE(make_expired(&db, "GP5_ZINCRBY_KEY"));

  s = db.ZIncrby("GP5_ZINCRBY_KEY", "MM2", 2, &score);
  ASSERT_TRUE(s.ok());
  ASSERT_DOUBLE_EQ(score, 2);
  ASSERT_TRUE(size_match(&db, "GP5_ZINCRBY_KEY", 1));
  ASSERT_TRUE(score_members_match(&db, "GP5_ZINCRBY_KEY", {{2, "MM2"}}));

  // ***************** Group 6 Test *****************
  s = db.ZAdd("GP6_ZINCRBY_KEY", {{1, "MM1"}}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  type_status.clear();
  ret = db.Expire("GP6_ZINCRBY_KEY", 100, &type_status);
  ASSERT_EQ(ret, 1);
  ASSERT_TRUE(type_status[storage::DataType::kZSets].ok());

  type_status.clear();
  type_ttl = db.TTL("GP6_ZINCRBY_KEY", &type_status);
  ASSERT_LE(type_ttl[kZSets], 100);
  ASSERT_GE(type_ttl[kZSets], 0);

  s = db.ZRem("GP6_ZINCRBY_KEY", {"MM1"}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.ZIncrby("GP6_ZINCRBY_KEY", "MM1", 1, &score);
  ASSERT_TRUE(s.ok());
  ASSERT_DOUBLE_EQ(score, 1);
  ASSERT_TRUE(size_match(&db, "GP6_ZINCRBY_KEY", 1));
  ASSERT_TRUE(score_members_match(&db, "GP6_ZINCRBY_KEY", {{1, "MM1"}}));

  type_status.clear();
  type_ttl = db.TTL("GP6_ZINCRBY_KEY", &type_status);
  ASSERT_EQ(type_ttl[kZSets], -1);
}

// ZRange
TEST_F(ZSetsTest, ZRangeTest) {
  int32_t ret;
  std::vector<storage::ScoreMember> score_members;

  // ***************** Group 1 Test *****************
  std::vector<storage::ScoreMember> gp1_sm{{0, "MM1"}};
  s = db.ZAdd("GP1_ZRANGE_KEY", gp1_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(1, ret);
  ASSERT_TRUE(size_match(&db, "GP1_ZRANGE_KEY", 1));
  ASSERT_TRUE(score_members_match(&db, "GP1_ZRANGE_KEY", {{0, "MM1"}}));

  s = db.ZRange("GP1_ZRANGE_KEY", 0, -1, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{0, "MM1"}}));

  // ***************** Group 2 Test *****************
  //
  // {0, MM0} {1, MM1} {2, MM2} {3, MM3} {4, MM4} {5, MM5} {6, MM6} {7, MM7} {8, MM8}
  //    0        1        2        3        4        5        6        7        8
  //   -9       -8       -7       -6       -5       -4       -3       -2       -1
  std::vector<storage::ScoreMember> gp2_sm{{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"},
                                           {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}};
  s = db.ZAdd("GP2_ZRANGE_KEY", gp2_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP2_ZRANGE_KEY", 9));
  ASSERT_TRUE(score_members_match(
      &db, "GP2_ZRANGE_KEY",
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", 0, 8, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(
      score_members,
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", -9, -1, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(
      score_members,
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", 0, -1, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(
      score_members,
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", -9, 8, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(
      score_members,
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", -100, 8, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(
      score_members,
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", 0, 100, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(
      score_members,
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", -100, 100, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(
      score_members,
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", 0, 0, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{0, "MM0"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", -9, -9, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{0, "MM0"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", 8, 8, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{8, "MM8"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", -1, -1, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{8, "MM8"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", 0, 5, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(
      score_members_match(score_members, {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", 0, -4, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(
      score_members_match(score_members, {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", -9, -4, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(
      score_members_match(score_members, {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", -9, 5, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(
      score_members_match(score_members, {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", -100, 5, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(
      score_members_match(score_members, {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", -100, -4, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(
      score_members_match(score_members, {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", 3, 5, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{3, "MM3"}, {4, "MM4"}, {5, "MM5"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", -6, -4, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{3, "MM3"}, {4, "MM4"}, {5, "MM5"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", 3, -4, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{3, "MM3"}, {4, "MM4"}, {5, "MM5"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", -6, 5, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{3, "MM3"}, {4, "MM4"}, {5, "MM5"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", 3, 8, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(
      score_members_match(score_members, {{3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", -6, -1, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(
      score_members_match(score_members, {{3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", 3, -1, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(
      score_members_match(score_members, {{3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", -6, 8, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(
      score_members_match(score_members, {{3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", -6, 100, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(
      score_members_match(score_members, {{3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", 3, 100, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(
      score_members_match(score_members, {{3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  // ***************** Group 3 Test *****************
  std::vector<storage::ScoreMember> gp3_sm{{0, "MM1"}};
  s = db.ZAdd("GP3_ZRANGE_KEY", gp3_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(1, ret);
  ASSERT_TRUE(size_match(&db, "GP3_ZRANGE_KEY", 1));
  ASSERT_TRUE(score_members_match(&db, "GP3_ZRANGE_KEY", {{0, "MM1"}}));
  ASSERT_TRUE(make_expired(&db, "GP3_ZRANGE_KEY"));

  s = db.ZRange("GP3_ZRANGE_KEY", 0, -1, &score_members);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_TRUE(score_members_match(score_members, {}));

  // ***************** Group 4 Test *****************
  s = db.ZRange("GP4_ZRANGE_KEY", 0, -1, &score_members);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_TRUE(score_members_match(score_members, {}));
}

// ZRangebyscore
TEST_F(ZSetsTest, ZRangebyscoreTest) {
  int32_t ret;
  std::vector<storage::ScoreMember> score_members;

  // ***************** Group 1 Test *****************
  std::vector<storage::ScoreMember> gp1_sm{{-987654321.0000001, "MM1"},
                                           {-87654321.00000001, "MM2"},
                                           {-7654321.000000001, "MM3"},
                                           {-654321.0000000001, "MM4"},
                                           {-54321.00000000001, "MM5"},
                                           {-4321.000000000001, "MM6"},
                                           {-1000.000000000001, "MM7"},
                                           {-1000.000000000001, "MM8"},
                                           {-1000.000000000001, "MM9"},
                                           {-100.0000000000001, "MM10"},
                                           {0, "MM11"},
                                           {100.0000000000001, "MM12"},
                                           {4321.000000000001, "MM13"},
                                           {54321.00000000001, "MM14"},
                                           {654321.0000000001, "MM15"},
                                           {7654321.000000001, "MM16"},
                                           {87654321.00000001, "MM17"},
                                           {987654321.0000001, "MM18"}};

  s = db.ZAdd("GP1_ZRANGEBYSCORE_KEY", gp1_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(18, ret);

  s = db.ZRangebyscore("GP1_ZRANGEBYSCORE_KEY", std::numeric_limits<double>::lowest(),
                       std::numeric_limits<double>::max(), true, true, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{-987654321.0000001, "MM1"},
                                                  {-87654321.00000001, "MM2"},
                                                  {-7654321.000000001, "MM3"},
                                                  {-654321.0000000001, "MM4"},
                                                  {-54321.00000000001, "MM5"},
                                                  {-4321.000000000001, "MM6"},
                                                  {-1000.000000000001, "MM7"},
                                                  {-1000.000000000001, "MM8"},
                                                  {-1000.000000000001, "MM9"},
                                                  {-100.0000000000001, "MM10"},
                                                  {0, "MM11"},
                                                  {100.0000000000001, "MM12"},
                                                  {4321.000000000001, "MM13"},
                                                  {54321.00000000001, "MM14"},
                                                  {654321.0000000001, "MM15"},
                                                  {7654321.000000001, "MM16"},
                                                  {87654321.00000001, "MM17"},
                                                  {987654321.0000001, "MM18"}}));

  // count = max offset = 0
  s = db.ZRangebyscore("GP1_ZRANGEBYSCORE_KEY", std::numeric_limits<double>::lowest(),
                       std::numeric_limits<double>::max(), true, true, std::numeric_limits<int64_t>::max(), 0,
                       &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{-987654321.0000001, "MM1"},
                                                  {-87654321.00000001, "MM2"},
                                                  {-7654321.000000001, "MM3"},
                                                  {-654321.0000000001, "MM4"},
                                                  {-54321.00000000001, "MM5"},
                                                  {-4321.000000000001, "MM6"},
                                                  {-1000.000000000001, "MM7"},
                                                  {-1000.000000000001, "MM8"},
                                                  {-1000.000000000001, "MM9"},
                                                  {-100.0000000000001, "MM10"},
                                                  {0, "MM11"},
                                                  {100.0000000000001, "MM12"},
                                                  {4321.000000000001, "MM13"},
                                                  {54321.00000000001, "MM14"},
                                                  {654321.0000000001, "MM15"},
                                                  {7654321.000000001, "MM16"},
                                                  {87654321.00000001, "MM17"},
                                                  {987654321.0000001, "MM18"}}));

  // count = 18 offset = 0
  s = db.ZRangebyscore("GP1_ZRANGEBYSCORE_KEY", std::numeric_limits<double>::lowest(),
                       std::numeric_limits<double>::max(), true, true, 18, 0, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{-987654321.0000001, "MM1"},
                                                  {-87654321.00000001, "MM2"},
                                                  {-7654321.000000001, "MM3"},
                                                  {-654321.0000000001, "MM4"},
                                                  {-54321.00000000001, "MM5"},
                                                  {-4321.000000000001, "MM6"},
                                                  {-1000.000000000001, "MM7"},
                                                  {-1000.000000000001, "MM8"},
                                                  {-1000.000000000001, "MM9"},
                                                  {-100.0000000000001, "MM10"},
                                                  {0, "MM11"},
                                                  {100.0000000000001, "MM12"},
                                                  {4321.000000000001, "MM13"},
                                                  {54321.00000000001, "MM14"},
                                                  {654321.0000000001, "MM15"},
                                                  {7654321.000000001, "MM16"},
                                                  {87654321.00000001, "MM17"},
                                                  {987654321.0000001, "MM18"}}));

  // count = 10 offset = 0
  s = db.ZRangebyscore("GP1_ZRANGEBYSCORE_KEY", std::numeric_limits<double>::lowest(),
                       std::numeric_limits<double>::max(), true, true, 10, 0, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{-987654321.0000001, "MM1"},
                                                  {-87654321.00000001, "MM2"},
                                                  {-7654321.000000001, "MM3"},
                                                  {-654321.0000000001, "MM4"},
                                                  {-54321.00000000001, "MM5"},
                                                  {-4321.000000000001, "MM6"},
                                                  {-1000.000000000001, "MM7"},
                                                  {-1000.000000000001, "MM8"},
                                                  {-1000.000000000001, "MM9"},
                                                  {-100.0000000000001, "MM10"}}));

  // count = 10 offset = 1
  s = db.ZRangebyscore("GP1_ZRANGEBYSCORE_KEY", std::numeric_limits<double>::lowest(),
                       std::numeric_limits<double>::max(), true, true, 10, 1, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{-87654321.00000001, "MM2"},
                                                  {-7654321.000000001, "MM3"},
                                                  {-654321.0000000001, "MM4"},
                                                  {-54321.00000000001, "MM5"},
                                                  {-4321.000000000001, "MM6"},
                                                  {-1000.000000000001, "MM7"},
                                                  {-1000.000000000001, "MM8"},
                                                  {-1000.000000000001, "MM9"},
                                                  {-100.0000000000001, "MM10"},
                                                  {0, "MM11"}}));

  // count = 10 offset = 17
  s = db.ZRangebyscore("GP1_ZRANGEBYSCORE_KEY", std::numeric_limits<double>::lowest(),
                       std::numeric_limits<double>::max(), true, true, 10, 17, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{987654321.0000001, "MM18"}}));

  // count = 10 offset = 18
  s = db.ZRangebyscore("GP1_ZRANGEBYSCORE_KEY", std::numeric_limits<double>::lowest(),
                       std::numeric_limits<double>::max(), true, true, 10, 18, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {}));

  // count = 10 offset = 19
  s = db.ZRangebyscore("GP1_ZRANGEBYSCORE_KEY", std::numeric_limits<double>::lowest(),
                       std::numeric_limits<double>::max(), true, true, 10, 19, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {}));

  // count = 10000 offset = 1
  s = db.ZRangebyscore("GP1_ZRANGEBYSCORE_KEY", std::numeric_limits<double>::lowest(),
                       std::numeric_limits<double>::max(), true, true, 10000, 1, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{-87654321.00000001, "MM2"},
                                                  {-7654321.000000001, "MM3"},
                                                  {-654321.0000000001, "MM4"},
                                                  {-54321.00000000001, "MM5"},
                                                  {-4321.000000000001, "MM6"},
                                                  {-1000.000000000001, "MM7"},
                                                  {-1000.000000000001, "MM8"},
                                                  {-1000.000000000001, "MM9"},
                                                  {-100.0000000000001, "MM10"},
                                                  {0, "MM11"},
                                                  {100.0000000000001, "MM12"},
                                                  {4321.000000000001, "MM13"},
                                                  {54321.00000000001, "MM14"},
                                                  {654321.0000000001, "MM15"},
                                                  {7654321.000000001, "MM16"},
                                                  {87654321.00000001, "MM17"},
                                                  {987654321.0000001, "MM18"}}));

  // count = 10000 offset = 10000
  s = db.ZRangebyscore("GP1_ZRANGEBYSCORE_KEY", std::numeric_limits<double>::lowest(),
                       std::numeric_limits<double>::max(), true, true, 10000, 10000, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {}));

  s = db.ZRangebyscore("GP1_ZRANGEBYSCORE_KEY", std::numeric_limits<double>::lowest(), -1000.000000000001, true, true,
                       &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{-987654321.0000001, "MM1"},
                                                  {-87654321.00000001, "MM2"},
                                                  {-7654321.000000001, "MM3"},
                                                  {-654321.0000000001, "MM4"},
                                                  {-54321.00000000001, "MM5"},
                                                  {-4321.000000000001, "MM6"},
                                                  {-1000.000000000001, "MM7"},
                                                  {-1000.000000000001, "MM8"},
                                                  {-1000.000000000001, "MM9"}}));

  s = db.ZRangebyscore("GP1_ZRANGEBYSCORE_KEY", std::numeric_limits<double>::lowest(), -1000.000000000001, true, false,
                       &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{-987654321.0000001, "MM1"},
                                                  {-87654321.00000001, "MM2"},
                                                  {-7654321.000000001, "MM3"},
                                                  {-654321.0000000001, "MM4"},
                                                  {-54321.00000000001, "MM5"},
                                                  {-4321.000000000001, "MM6"}}));

  s = db.ZRangebyscore("GP1_ZRANGEBYSCORE_KEY", -1000.000000000001, std::numeric_limits<double>::max(), true, true,
                       &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{-1000.000000000001, "MM7"},
                                                  {-1000.000000000001, "MM8"},
                                                  {-1000.000000000001, "MM9"},
                                                  {-100.0000000000001, "MM10"},
                                                  {0, "MM11"},
                                                  {100.0000000000001, "MM12"},
                                                  {4321.000000000001, "MM13"},
                                                  {54321.00000000001, "MM14"},
                                                  {654321.0000000001, "MM15"},
                                                  {7654321.000000001, "MM16"},
                                                  {87654321.00000001, "MM17"},
                                                  {987654321.0000001, "MM18"}}));

  s = db.ZRangebyscore("GP1_ZRANGEBYSCORE_KEY", -1000.000000000001, std::numeric_limits<double>::max(), false, true,
                       &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{-100.0000000000001, "MM10"},
                                                  {0, "MM11"},
                                                  {100.0000000000001, "MM12"},
                                                  {4321.000000000001, "MM13"},
                                                  {54321.00000000001, "MM14"},
                                                  {654321.0000000001, "MM15"},
                                                  {7654321.000000001, "MM16"},
                                                  {87654321.00000001, "MM17"},
                                                  {987654321.0000001, "MM18"}}));

  s = db.ZRangebyscore("GP1_ZRANGEBYSCORE_KEY", -987654321.0000001, 987654321.0000001, true, true, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{-987654321.0000001, "MM1"},
                                                  {-87654321.00000001, "MM2"},
                                                  {-7654321.000000001, "MM3"},
                                                  {-654321.0000000001, "MM4"},
                                                  {-54321.00000000001, "MM5"},
                                                  {-4321.000000000001, "MM6"},
                                                  {-1000.000000000001, "MM7"},
                                                  {-1000.000000000001, "MM8"},
                                                  {-1000.000000000001, "MM9"},
                                                  {-100.0000000000001, "MM10"},
                                                  {0, "MM11"},
                                                  {100.0000000000001, "MM12"},
                                                  {4321.000000000001, "MM13"},
                                                  {54321.00000000001, "MM14"},
                                                  {654321.0000000001, "MM15"},
                                                  {7654321.000000001, "MM16"},
                                                  {87654321.00000001, "MM17"},
                                                  {987654321.0000001, "MM18"}}));

  s = db.ZRangebyscore("GP1_ZRANGEBYSCORE_KEY", -987654321.0000001, 987654321.0000001, false, false, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{-87654321.00000001, "MM2"},
                                                  {-7654321.000000001, "MM3"},
                                                  {-654321.0000000001, "MM4"},
                                                  {-54321.00000000001, "MM5"},
                                                  {-4321.000000000001, "MM6"},
                                                  {-1000.000000000001, "MM7"},
                                                  {-1000.000000000001, "MM8"},
                                                  {-1000.000000000001, "MM9"},
                                                  {-100.0000000000001, "MM10"},
                                                  {0, "MM11"},
                                                  {100.0000000000001, "MM12"},
                                                  {4321.000000000001, "MM13"},
                                                  {54321.00000000001, "MM14"},
                                                  {654321.0000000001, "MM15"},
                                                  {7654321.000000001, "MM16"},
                                                  {87654321.00000001, "MM17"}}));

  s = db.ZRangebyscore("GP1_ZRANGEBYSCORE_KEY", -999999999, -1000.000000000001, true, true, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{-987654321.0000001, "MM1"},
                                                  {-87654321.00000001, "MM2"},
                                                  {-7654321.000000001, "MM3"},
                                                  {-654321.0000000001, "MM4"},
                                                  {-54321.00000000001, "MM5"},
                                                  {-4321.000000000001, "MM6"},
                                                  {-1000.000000000001, "MM7"},
                                                  {-1000.000000000001, "MM8"},
                                                  {-1000.000000000001, "MM9"}}));

  s = db.ZRangebyscore("GP1_ZRANGEBYSCORE_KEY", -999999999, -1000.000000000001, true, false, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{-987654321.0000001, "MM1"},
                                                  {-87654321.00000001, "MM2"},
                                                  {-7654321.000000001, "MM3"},
                                                  {-654321.0000000001, "MM4"},
                                                  {-54321.00000000001, "MM5"},
                                                  {-4321.000000000001, "MM6"}}));

  s = db.ZRangebyscore("GP1_ZRANGEBYSCORE_KEY", -654321.0000000001, -4321.000000000001, true, true, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(
      score_members, {{-654321.0000000001, "MM4"}, {-54321.00000000001, "MM5"}, {-4321.000000000001, "MM6"}}));

  s = db.ZRangebyscore("GP1_ZRANGEBYSCORE_KEY", -654321.0000000001, -4321.000000000001, false, false, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{-54321.00000000001, "MM5"}}));

  s = db.ZRangebyscore("GP1_ZRANGEBYSCORE_KEY", 0, 0, true, true, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{0, "MM11"}}));

  s = db.ZRangebyscore("GP1_ZRANGEBYSCORE_KEY", 0, 0, false, true, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {}));

  s = db.ZRangebyscore("GP1_ZRANGEBYSCORE_KEY", 4321.000000000001, 987654321.0000001, true, true, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{4321.000000000001, "MM13"},
                                                  {54321.00000000001, "MM14"},
                                                  {654321.0000000001, "MM15"},
                                                  {7654321.000000001, "MM16"},
                                                  {87654321.00000001, "MM17"},
                                                  {987654321.0000001, "MM18"}}));

  s = db.ZRangebyscore("GP1_ZRANGEBYSCORE_KEY", 4321.000000000001, 987654321.0000001, false, true, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{54321.00000000001, "MM14"},
                                                  {654321.0000000001, "MM15"},
                                                  {7654321.000000001, "MM16"},
                                                  {87654321.00000001, "MM17"},
                                                  {987654321.0000001, "MM18"}}));

  s = db.ZRangebyscore("GP1_ZRANGEBYSCORE_KEY", 4321.000000000001, 987654321.0000001, false, false, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {
                                                     {54321.00000000001, "MM14"},
                                                     {654321.0000000001, "MM15"},
                                                     {7654321.000000001, "MM16"},
                                                     {87654321.00000001, "MM17"},
                                                 }));

  s = db.ZRangebyscore("GP1_ZRANGEBYSCORE_KEY", -1000.000000000001, 987654321.0000001, true, true, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{-1000.000000000001, "MM7"},
                                                  {-1000.000000000001, "MM8"},
                                                  {-1000.000000000001, "MM9"},
                                                  {-100.0000000000001, "MM10"},
                                                  {0, "MM11"},
                                                  {100.0000000000001, "MM12"},
                                                  {4321.000000000001, "MM13"},
                                                  {54321.00000000001, "MM14"},
                                                  {654321.0000000001, "MM15"},
                                                  {7654321.000000001, "MM16"},
                                                  {87654321.00000001, "MM17"},
                                                  {987654321.0000001, "MM18"}}));

  s = db.ZRangebyscore("GP1_ZRANGEBYSCORE_KEY", -1000.000000000001, 987654321.0000001, false, true, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{-100.0000000000001, "MM10"},
                                                  {0, "MM11"},
                                                  {100.0000000000001, "MM12"},
                                                  {4321.000000000001, "MM13"},
                                                  {54321.00000000001, "MM14"},
                                                  {654321.0000000001, "MM15"},
                                                  {7654321.000000001, "MM16"},
                                                  {87654321.00000001, "MM17"},
                                                  {987654321.0000001, "MM18"}}));

  s = db.ZRangebyscore("GP1_ZRANGEBYSCORE_KEY", 999999999, std::numeric_limits<double>::max(), true, true,
                       &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {}));

  // ***************** Group 2 Test *****************
  std::vector<storage::ScoreMember> gp2_sm{{-5, "MM0"}, {-3, "MM1"}, {-1, "MM2"}, {0, "MM3"},
                                           {1, "MM4"},  {3, "MM5"},  {5, "MM6"}};
  s = db.ZAdd("GP2_ZRANGEBYSCORE_KEY", gp2_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(7, ret);
  ASSERT_TRUE(make_expired(&db, "GP2_ZRANGEBYSCORE_KEY"));
  s = db.ZRangebyscore("GP2_ZRANGEBYSCORE_KEY", std::numeric_limits<double>::lowest(),
                       std::numeric_limits<double>::max(), true, true, &score_members);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_TRUE(score_members_match(score_members, {}));

  // ***************** Group 3 Test *****************
  s = db.ZRangebyscore("GP3_ZRANGEBYSCORE_KEY", std::numeric_limits<double>::lowest(),
                       std::numeric_limits<double>::max(), true, true, &score_members);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_TRUE(score_members_match(score_members, {}));

  // ***************** Group 4 Test *****************
  std::vector<storage::ScoreMember> gp4_sm{
      {std::numeric_limits<double>::lowest(), "MM0"}, {0, "MM1"}, {std::numeric_limits<double>::max(), "MM2"}};
  s = db.ZAdd("GP4_ZRANGEBYSCORE_KEY", gp4_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(3, ret);

  s = db.ZRangebyscore("GP4_ZRANGEBYSCORE_KEY", std::numeric_limits<double>::lowest(),
                       std::numeric_limits<double>::max(), true, true, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(
      score_members,
      {{std::numeric_limits<double>::lowest(), "MM0"}, {0, "MM1"}, {std::numeric_limits<double>::max(), "MM2"}}));

  s = db.ZRangebyscore("GP4_ZRANGEBYSCORE_KEY", std::numeric_limits<double>::lowest(),
                       std::numeric_limits<double>::max(), false, false, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{0, "MM1"}}));

  s = db.ZRangebyscore("GP4_ZRANGEBYSCORE_KEY", std::numeric_limits<double>::lowest(),
                       std::numeric_limits<double>::max(), true, false, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{std::numeric_limits<double>::lowest(), "MM0"}, {0, "MM1"}}));

  s = db.ZRangebyscore("GP4_ZRANGEBYSCORE_KEY", std::numeric_limits<double>::lowest(),
                       std::numeric_limits<double>::max(), false, true, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{0, "MM1"}, {std::numeric_limits<double>::max(), "MM2"}}));
}

// TODO(@tangruilin): 修复测试代码
// ZRank
// TEST_F(ZSetsTest, ZRankTest) {
//   int32_t ret, rank;

//   // ***************** Group 1 Test *****************
//   // {-5, MM0} {-3, MM1} {-1, MM2} {0, MM3} {1, MM4} {3, MM5} {5, MM6}
//   //     0         1         2        3        4        5        6
//   std::vector<storage::ScoreMember> gp1_sm {{-5, "MM0"}, {-3, "MM1"}, {-1, "MM2"}, {0, "MM3"}, {1, "MM4"}, {3,
//   "MM5"}, {5, "MM6"}}; s = db.ZAdd("GP1_ZRANK_KEY", gp1_sm, &ret); ASSERT_TRUE(s.ok()); ASSERT_EQ(7, ret);

//   s = db.ZRank("GP1_ZRANK_KEY", "MM0", &rank);
//   ASSERT_TRUE(s.ok());
//   ASSERT_EQ(rank, 0);

//   s = db.ZRank("GP1_ZRANK_KEY", "MM2", &rank);
//   ASSERT_TRUE(s.ok());
//   ASSERT_EQ(rank, 2);

//   s = db.ZRank("GP1_ZRANK_KEY", "MM4", &rank);
//   ASSERT_TRUE(s.ok());
//   ASSERT_EQ(rank, 4);

//   s = db.ZRank("GP1_ZRANK_KEY", "MM6", &rank);
//   ASSERT_TRUE(s.ok());
//   ASSERT_EQ(rank, 6);

//   s = db.ZRank("GP1_ZRANK_KEY", "MM", &rank);
//   ASSERT_TRUE(s.IsNotFound());
//   ASSERT_EQ(rank, -1);

//   // ***************** Group 2 Test *****************
//   std::vector<storage::ScoreMember> gp2_sm {{-5, "MM0"}, {-3, "MM1"}, {-1, "MM2"}, {0, "MM3"}, {1, "MM4"}, {3,
//   "MM5"}, {5, "MM6"}}; s = db.ZAdd("GP2_ZRANK_KEY", gp2_sm, &ret); ASSERT_TRUE(s.ok()); ASSERT_EQ(7, ret);
//   ASSERT_TRUE(make_expired(&db, "GP2_ZRANGE_KEY"));

//   s = db.ZRank("GP2_ZRANGE_KEY", "MM0", &rank);
//   ASSERT_TRUE(s.IsNotFound());
//   ASSERT_EQ(-1, rank);

//   // ***************** Group 3 Test *****************
//   s = db.ZRank("GP3_ZRANGE_KEY", "MM0", &rank);
//   ASSERT_TRUE(s.IsNotFound());
//   ASSERT_EQ(-1, rank);
// }

// ZRem
TEST_F(ZSetsTest, ZRemTest) {
  int32_t ret;

  // ***************** Group 1 Test *****************
  // {-5, MM0} {-3, MM1} {-1, MM2} {0, MM3} {1, MM4} {3, MM5} {5, MM6}
  //     0         1         2        3        4        5        6
  std::vector<storage::ScoreMember> gp1_sm{{-5, "MM0"}, {-3, "MM1"}, {-1, "MM2"}, {0, "MM3"},
                                           {1, "MM4"},  {3, "MM5"},  {5, "MM6"}};
  s = db.ZAdd("GP1_ZREM_KEY", gp1_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(7, ret);
  ASSERT_TRUE(size_match(&db, "GP1_ZREM_KEY", 7));
  ASSERT_TRUE(score_members_match(
      &db, "GP1_ZREM_KEY", {{-5, "MM0"}, {-3, "MM1"}, {-1, "MM2"}, {0, "MM3"}, {1, "MM4"}, {3, "MM5"}, {5, "MM6"}}));

  s = db.ZRem("GP1_ZREM_KEY", {"MM1", "MM3", "MM5"}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(3, ret);
  ASSERT_TRUE(size_match(&db, "GP1_ZREM_KEY", 4));
  ASSERT_TRUE(score_members_match(&db, "GP1_ZREM_KEY", {{-5, "MM0"}, {-1, "MM2"}, {1, "MM4"}, {5, "MM6"}}));

  // ***************** Group 2 Test *****************
  // {-5, MM0} {-3, MM1} {-1, MM2} {0, MM3} {1, MM4} {3, MM5} {5, MM6}
  //     0         1         2        3        4        5        6
  std::vector<storage::ScoreMember> gp2_sm{{-5, "MM0"}, {-3, "MM1"}, {-1, "MM2"}, {0, "MM3"},
                                           {1, "MM4"},  {3, "MM5"},  {5, "MM6"}};
  s = db.ZAdd("GP2_ZREM_KEY", gp2_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(7, ret);
  ASSERT_TRUE(size_match(&db, "GP2_ZREM_KEY", 7));
  ASSERT_TRUE(score_members_match(
      &db, "GP2_ZREM_KEY", {{-5, "MM0"}, {-3, "MM1"}, {-1, "MM2"}, {0, "MM3"}, {1, "MM4"}, {3, "MM5"}, {5, "MM6"}}));

  s = db.ZRem("GP2_ZREM_KEY", {"MM0", "MM1", "MM2", "MM3", "MM4", "MM5", "MM6"}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(7, ret);
  ASSERT_TRUE(size_match(&db, "GP2_ZREM_KEY", 0));
  ASSERT_TRUE(score_members_match(&db, "GP2_ZREM_KEY", {}));

  s = db.ZRem("GP2_ZREM_KEY", {"MM0", "MM1", "MM2"}, &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(0, ret);
  ASSERT_TRUE(size_match(&db, "GP2_ZREM_KEY", 0));
  ASSERT_TRUE(score_members_match(&db, "GP2_ZREM_KEY", {}));

  // ***************** Group 3 Test *****************
  // {-5, MM0} {-3, MM1} {-1, MM2} {0, MM3} {1, MM4} {3, MM5} {5, MM6}
  //     0         1         2        3        4        5        6
  std::vector<storage::ScoreMember> gp3_sm{{-5, "MM0"}, {-3, "MM1"}, {-1, "MM2"}, {0, "MM3"},
                                           {1, "MM4"},  {3, "MM5"},  {5, "MM6"}};
  s = db.ZAdd("GP3_ZREM_KEY", gp3_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(7, ret);
  ASSERT_TRUE(size_match(&db, "GP3_ZREM_KEY", 7));
  ASSERT_TRUE(score_members_match(
      &db, "GP3_ZREM_KEY", {{-5, "MM0"}, {-3, "MM1"}, {-1, "MM2"}, {0, "MM3"}, {1, "MM4"}, {3, "MM5"}, {5, "MM6"}}));

  s = db.ZRem("GP3_ZREM_KEY", {"MM0", "MM0", "MM1", "MM1", "MM2", "MM2"}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(3, ret);
  ASSERT_TRUE(size_match(&db, "GP3_ZREM_KEY", 4));
  ASSERT_TRUE(score_members_match(&db, "GP3_ZREM_KEY", {{0, "MM3"}, {1, "MM4"}, {3, "MM5"}, {5, "MM6"}}));

  // ***************** Group 4 Test *****************
  // {-5, MM0} {-3, MM1} {-1, MM2} {0, MM3} {1, MM4} {3, MM5} {5, MM6}
  //     0         1         2        3        4        5        6
  std::vector<storage::ScoreMember> gp4_sm{{-5, "MM0"}, {-3, "MM1"}, {-1, "MM2"}, {0, "MM3"},
                                           {1, "MM4"},  {3, "MM5"},  {5, "MM6"}};
  s = db.ZAdd("GP4_ZREM_KEY", gp4_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(7, ret);
  ASSERT_TRUE(size_match(&db, "GP4_ZREM_KEY", 7));
  ASSERT_TRUE(score_members_match(
      &db, "GP4_ZREM_KEY", {{-5, "MM0"}, {-3, "MM1"}, {-1, "MM2"}, {0, "MM3"}, {1, "MM4"}, {3, "MM5"}, {5, "MM6"}}));

  s = db.ZRem("GP4_ZREM_KEY", {"MM", "YY", "CC"}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(0, ret);
  ASSERT_TRUE(size_match(&db, "GP4_ZREM_KEY", 7));
  ASSERT_TRUE(score_members_match(
      &db, "GP4_ZREM_KEY", {{-5, "MM0"}, {-3, "MM1"}, {-1, "MM2"}, {0, "MM3"}, {1, "MM4"}, {3, "MM5"}, {5, "MM6"}}));

  // ***************** Group 5 Test *****************
  // {-5, MM0} {-3, MM1} {-1, MM2} {0, MM3} {1, MM4} {3, MM5} {5, MM6}
  //     0         1         2        3        4        5        6
  std::vector<storage::ScoreMember> gp5_sm{{-5, "MM0"}, {-3, "MM1"}, {-1, "MM2"}, {0, "MM3"},
                                           {1, "MM4"},  {3, "MM5"},  {5, "MM6"}};
  s = db.ZAdd("GP5_ZREM_KEY", gp4_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(7, ret);
  ASSERT_TRUE(size_match(&db, "GP5_ZREM_KEY", 7));
  ASSERT_TRUE(score_members_match(
      &db, "GP5_ZREM_KEY", {{-5, "MM0"}, {-3, "MM1"}, {-1, "MM2"}, {0, "MM3"}, {1, "MM4"}, {3, "MM5"}, {5, "MM6"}}));
  ASSERT_TRUE(make_expired(&db, "GP5_ZREM_KEY"));

  s = db.ZRem("GP5_ZREM_KEY", {"MM0", "MM1", "MM2"}, &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(0, ret);
  ASSERT_TRUE(size_match(&db, "GP5_ZREM_KEY", 0));
  ASSERT_TRUE(score_members_match(&db, "GP5_ZREM_KEY", {}));

  // ***************** Group 5 Test *****************
  // Not exist ZSet
  s = db.ZRem("GP6_ZREM_KEY", {"MM0", "MM1", "MM2"}, &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(0, ret);
  ASSERT_TRUE(size_match(&db, "GP6_ZREM_KEY", 0));
  ASSERT_TRUE(score_members_match(&db, "GP6_ZREM_KEY", {}));
}

// ZRemrangebyrank
TEST_F(ZSetsTest, ZRemrangebyrankTest) {
  int32_t ret;
  std::vector<storage::ScoreMember> score_members;

  // ***************** Group 1 Test *****************
  std::vector<storage::ScoreMember> gp1_sm{{0, "MM1"}};
  s = db.ZAdd("GP1_ZREMMRANGEBYRANK_KEY", gp1_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(1, ret);
  ASSERT_TRUE(size_match(&db, "GP1_ZREMMRANGEBYRANK_KEY", 1));
  ASSERT_TRUE(score_members_match(&db, "GP1_ZREMMRANGEBYRANK_KEY", {{0, "MM1"}}));

  s = db.ZRemrangebyrank("GP1_ZREMMRANGEBYRANK_KEY", 0, -1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(1, ret);
  ASSERT_TRUE(score_members_match(score_members, {}));

  // ***************** Group 2 Test *****************
  //
  // {0, MM0} {1, MM1} {2, MM2} {3, MM3} {4, MM4} {5, MM5} {6, MM6} {7, MM7} {8, MM8}
  //    0        1        2        3        4        5        6        7        8
  //   -9       -8       -7       -6       -5       -4       -3       -2       -1
  std::vector<storage::ScoreMember> gp2_sm{{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"},
                                           {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}};
  s = db.ZAdd("GP2_ZREMRANGEBYRANK_KEY", gp2_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP2_ZREMRANGEBYRANK_KEY", 9));
  ASSERT_TRUE(score_members_match(
      &db, "GP2_ZREMRANGEBYRANK_KEY",
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  s = db.ZRemrangebyrank("GP2_ZREMRANGEBYRANK_KEY", 0, 8, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP2_ZREMRANGEBYRANK_KEY", 0));
  ASSERT_TRUE(score_members_match(score_members, {}));

  // ***************** Group 3 Test *****************
  //
  // {0, MM0} {1, MM1} {2, MM2} {3, MM3} {4, MM4} {5, MM5} {6, MM6} {7, MM7} {8, MM8}
  //    0        1        2        3        4        5        6        7        8
  //   -9       -8       -7       -6       -5       -4       -3       -2       -1
  std::vector<storage::ScoreMember> gp3_sm{{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"},
                                           {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}};
  s = db.ZAdd("GP3_ZREMRANGEBYRANK_KEY", gp3_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP3_ZREMRANGEBYRANK_KEY", 9));
  ASSERT_TRUE(score_members_match(
      &db, "GP3_ZREMRANGEBYRANK_KEY",
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  s = db.ZRemrangebyrank("GP3_ZREMRANGEBYRANK_KEY", -9, -1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP3_ZREMRANGEBYRANK_KEY", 0));
  ASSERT_TRUE(score_members_match(score_members, {}));

  // ***************** Group 4 Test *****************
  //
  // {0, MM0} {1, MM1} {2, MM2} {3, MM3} {4, MM4} {5, MM5} {6, MM6} {7, MM7} {8, MM8}
  //    0        1        2        3        4        5        6        7        8
  //   -9       -8       -7       -6       -5       -4       -3       -2       -1
  std::vector<storage::ScoreMember> gp4_sm{{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"},
                                           {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}};
  s = db.ZAdd("GP4_ZREMRANGEBYRANK_KEY", gp4_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP4_ZREMRANGEBYRANK_KEY", 9));
  ASSERT_TRUE(score_members_match(
      &db, "GP4_ZREMRANGEBYRANK_KEY",
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));
  s = db.ZRemrangebyrank("GP4_ZREMRANGEBYRANK_KEY", 0, -1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP4_ZREMRANGEBYRANK_KEY", 0));
  ASSERT_TRUE(score_members_match(score_members, {}));

  // ***************** Group 5 Test *****************
  //
  // {0, MM0} {1, MM1} {2, MM2} {3, MM3} {4, MM4} {5, MM5} {6, MM6} {7, MM7} {8, MM8}
  //    0        1        2        3        4        5        6        7        8
  //   -9       -8       -7       -6       -5       -4       -3       -2       -1
  std::vector<storage::ScoreMember> gp5_sm{{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"},
                                           {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}};
  s = db.ZAdd("GP5_ZREMRANGEBYRANK_KEY", gp5_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP5_ZREMRANGEBYRANK_KEY", 9));
  ASSERT_TRUE(score_members_match(
      &db, "GP5_ZREMRANGEBYRANK_KEY",
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));
  s = db.ZRemrangebyrank("GP5_ZREMRANGEBYRANK_KEY", -9, 8, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP5_ZREMRANGEBYRANK_KEY", 0));
  ASSERT_TRUE(score_members_match(score_members, {}));

  // ***************** Group 6 Test *****************
  //
  // {0, MM0} {1, MM1} {2, MM2} {3, MM3} {4, MM4} {5, MM5} {6, MM6} {7, MM7} {8, MM8}
  //    0        1        2        3        4        5        6        7        8
  //   -9       -8       -7       -6       -5       -4       -3       -2       -1
  std::vector<storage::ScoreMember> gp6_sm{{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"},
                                           {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}};
  s = db.ZAdd("GP6_ZREMRANGEBYRANK_KEY", gp6_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP6_ZREMRANGEBYRANK_KEY", 9));
  ASSERT_TRUE(score_members_match(
      &db, "GP6_ZREMRANGEBYRANK_KEY",
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));
  s = db.ZRemrangebyrank("GP6_ZREMRANGEBYRANK_KEY", -100, 8, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP6_ZREMRANGEBYRANK_KEY", 0));
  ASSERT_TRUE(score_members_match(score_members, {}));

  // ***************** Group 7 Test *****************
  //
  // {0, MM0} {1, MM1} {2, MM2} {3, MM3} {4, MM4} {5, MM5} {6, MM6} {7, MM7} {8, MM8}
  //    0        1        2        3        4        5        6        7        8
  //   -9       -8       -7       -6       -5       -4       -3       -2       -1
  std::vector<storage::ScoreMember> gp7_sm{{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"},
                                           {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}};
  s = db.ZAdd("GP7_ZREMRANGEBYRANK_KEY", gp7_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP7_ZREMRANGEBYRANK_KEY", 9));
  ASSERT_TRUE(score_members_match(
      &db, "GP7_ZREMRANGEBYRANK_KEY",
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));
  s = db.ZRemrangebyrank("GP7_ZREMRANGEBYRANK_KEY", 0, 100, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP7_ZREMRANGEBYRANK_KEY", 0));
  ASSERT_TRUE(score_members_match(score_members, {}));

  // ***************** Group 8 Test *****************
  //
  // {0, MM0} {1, MM1} {2, MM2} {3, MM3} {4, MM4} {5, MM5} {6, MM6} {7, MM7} {8, MM8}
  //    0        1        2        3        4        5        6        7        8
  //   -9       -8       -7       -6       -5       -4       -3       -2       -1
  std::vector<storage::ScoreMember> gp8_sm{{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"},
                                           {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}};
  s = db.ZAdd("GP8_ZREMRANGEBYRANK_KEY", gp8_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP8_ZREMRANGEBYRANK_KEY", 9));
  ASSERT_TRUE(score_members_match(
      &db, "GP8_ZREMRANGEBYRANK_KEY",
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));
  s = db.ZRemrangebyrank("GP8_ZREMRANGEBYRANK_KEY", -100, 100, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP8_ZREMRANGEBYRANK_KEY", 0));
  ASSERT_TRUE(score_members_match(score_members, {}));

  // ***************** Group 9 Test *****************
  //
  // {0, MM0} {1, MM1} {2, MM2} {3, MM3} {4, MM4} {5, MM5} {6, MM6} {7, MM7} {8, MM8}
  //    0        1        2        3        4        5        6        7        8
  //   -9       -8       -7       -6       -5       -4       -3       -2       -1
  std::vector<storage::ScoreMember> gp9_sm{{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"},
                                           {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}};
  s = db.ZAdd("GP9_ZREMRANGEBYRANK_KEY", gp9_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP9_ZREMRANGEBYRANK_KEY", 9));
  ASSERT_TRUE(score_members_match(
      &db, "GP9_ZREMRANGEBYRANK_KEY",
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));
  s = db.ZRemrangebyrank("GP9_ZREMRANGEBYRANK_KEY", 0, 0, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(1, ret);
  ASSERT_TRUE(size_match(&db, "GP9_ZREMRANGEBYRANK_KEY", 8));
  ASSERT_TRUE(score_members_match(
      &db, "GP9_ZREMRANGEBYRANK_KEY",
      {{1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  // ***************** Group 10 Test *****************
  //
  // {0, MM0} {1, MM1} {2, MM2} {3, MM3} {4, MM4} {5, MM5} {6, MM6} {7, MM7} {8, MM8}
  //    0        1        2        3        4        5        6        7        8
  //   -9       -8       -7       -6       -5       -4       -3       -2       -1
  std::vector<storage::ScoreMember> gp10_sm{{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"},
                                            {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}};
  s = db.ZAdd("GP10_ZREMRANGEBYRANK_KEY", gp10_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP10_ZREMRANGEBYRANK_KEY", 9));
  ASSERT_TRUE(score_members_match(
      &db, "GP10_ZREMRANGEBYRANK_KEY",
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));
  s = db.ZRemrangebyrank("GP10_ZREMRANGEBYRANK_KEY", -9, -9, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(1, ret);
  ASSERT_TRUE(size_match(&db, "GP10_ZREMRANGEBYRANK_KEY", 8));
  ASSERT_TRUE(score_members_match(
      &db, "GP10_ZREMRANGEBYRANK_KEY",
      {{1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  // ***************** Group 11 Test *****************
  //
  // {0, MM0} {1, MM1} {2, MM2} {3, MM3} {4, MM4} {5, MM5} {6, MM6} {7, MM7} {8, MM8}
  //    0        1        2        3        4        5        6        7        8
  //   -9       -8       -7       -6       -5       -4       -3       -2       -1
  std::vector<storage::ScoreMember> gp11_sm{{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"},
                                            {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}};
  s = db.ZAdd("GP11_ZREMRANGEBYRANK_KEY", gp11_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP11_ZREMRANGEBYRANK_KEY", 9));
  ASSERT_TRUE(score_members_match(
      &db, "GP11_ZREMRANGEBYRANK_KEY",
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));
  s = db.ZRemrangebyrank("GP11_ZREMRANGEBYRANK_KEY", 8, 8, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(1, ret);
  ASSERT_TRUE(size_match(&db, "GP11_ZREMRANGEBYRANK_KEY", 8));
  ASSERT_TRUE(score_members_match(
      &db, "GP11_ZREMRANGEBYRANK_KEY",
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}}));

  // ***************** Group 12 Test *****************
  //
  // {0, MM0} {1, MM1} {2, MM2} {3, MM3} {4, MM4} {5, MM5} {6, MM6} {7, MM7} {8, MM8}
  //    0        1        2        3        4        5        6        7        8
  //   -9       -8       -7       -6       -5       -4       -3       -2       -1
  std::vector<storage::ScoreMember> gp12_sm{{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"},
                                            {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}};
  s = db.ZAdd("GP12_ZREMRANGEBYRANK_KEY", gp12_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP12_ZREMRANGEBYRANK_KEY", 9));
  ASSERT_TRUE(score_members_match(
      &db, "GP12_ZREMRANGEBYRANK_KEY",
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));
  s = db.ZRemrangebyrank("GP12_ZREMRANGEBYRANK_KEY", -1, -1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(1, ret);
  ASSERT_TRUE(size_match(&db, "GP12_ZREMRANGEBYRANK_KEY", 8));
  ASSERT_TRUE(score_members_match(
      &db, "GP12_ZREMRANGEBYRANK_KEY",
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}}));

  // ***************** Group 13 Test *****************
  //
  // {0, MM0} {1, MM1} {2, MM2} {3, MM3} {4, MM4} {5, MM5} {6, MM6} {7, MM7} {8, MM8}
  //    0        1        2        3        4        5        6        7        8
  //   -9       -8       -7       -6       -5       -4       -3       -2       -1
  std::vector<storage::ScoreMember> gp13_sm{{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"},
                                            {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}};
  s = db.ZAdd("GP13_ZREMRANGEBYRANK_KEY", gp13_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP13_ZREMRANGEBYRANK_KEY", 9));
  ASSERT_TRUE(score_members_match(
      &db, "GP13_ZREMRANGEBYRANK_KEY",
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));
  s = db.ZRemrangebyrank("GP13_ZREMRANGEBYRANK_KEY", 0, 5, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(6, ret);
  ASSERT_TRUE(size_match(&db, "GP13_ZREMRANGEBYRANK_KEY", 3));
  ASSERT_TRUE(score_members_match(&db, "GP13_ZREMRANGEBYRANK_KEY", {{6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  // ***************** Group 14 Test *****************
  //
  // {0, MM0} {1, MM1} {2, MM2} {3, MM3} {4, MM4} {5, MM5} {6, MM6} {7, MM7} {8, MM8}
  //    0        1        2        3        4        5        6        7        8
  //   -9       -8       -7       -6       -5       -4       -3       -2       -1
  std::vector<storage::ScoreMember> gp14_sm{{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"},
                                            {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}};
  s = db.ZAdd("GP14_ZREMRANGEBYRANK_KEY", gp14_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP14_ZREMRANGEBYRANK_KEY", 9));
  ASSERT_TRUE(score_members_match(
      &db, "GP14_ZREMRANGEBYRANK_KEY",
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));
  s = db.ZRemrangebyrank("GP14_ZREMRANGEBYRANK_KEY", 0, -4, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(6, ret);
  ASSERT_TRUE(size_match(&db, "GP14_ZREMRANGEBYRANK_KEY", 3));
  ASSERT_TRUE(score_members_match(&db, "GP14_ZREMRANGEBYRANK_KEY", {{6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  // ***************** Group 15 Test *****************
  //
  // {0, MM0} {1, MM1} {2, MM2} {3, MM3} {4, MM4} {5, MM5} {6, MM6} {7, MM7} {8, MM8}
  //    0        1        2        3        4        5        6        7        8
  //   -9       -8       -7       -6       -5       -4       -3       -2       -1
  std::vector<storage::ScoreMember> gp15_sm{{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"},
                                            {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}};
  s = db.ZAdd("GP15_ZREMRANGEBYRANK_KEY", gp15_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP15_ZREMRANGEBYRANK_KEY", 9));
  ASSERT_TRUE(score_members_match(
      &db, "GP15_ZREMRANGEBYRANK_KEY",
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));
  s = db.ZRemrangebyrank("GP15_ZREMRANGEBYRANK_KEY", -9, -4, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(6, ret);
  ASSERT_TRUE(size_match(&db, "GP15_ZREMRANGEBYRANK_KEY", 3));
  ASSERT_TRUE(score_members_match(&db, "GP15_ZREMRANGEBYRANK_KEY", {{6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  // ***************** Group 16 Test *****************
  //
  // {0, MM0} {1, MM1} {2, MM2} {3, MM3} {4, MM4} {5, MM5} {6, MM6} {7, MM7} {8, MM8}
  //    0        1        2        3        4        5        6        7        8
  //   -9       -8       -7       -6       -5       -4       -3       -2       -1
  std::vector<storage::ScoreMember> gp16_sm{{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"},
                                            {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}};
  s = db.ZAdd("GP16_ZREMRANGEBYRANK_KEY", gp16_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP16_ZREMRANGEBYRANK_KEY", 9));
  ASSERT_TRUE(score_members_match(
      &db, "GP16_ZREMRANGEBYRANK_KEY",
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));
  s = db.ZRemrangebyrank("GP16_ZREMRANGEBYRANK_KEY", -9, 5, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(6, ret);
  ASSERT_TRUE(size_match(&db, "GP16_ZREMRANGEBYRANK_KEY", 3));
  ASSERT_TRUE(score_members_match(&db, "GP16_ZREMRANGEBYRANK_KEY", {{6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  // ***************** Group 17 Test *****************
  //
  // {0, MM0} {1, MM1} {2, MM2} {3, MM3} {4, MM4} {5, MM5} {6, MM6} {7, MM7} {8, MM8}
  //    0        1        2        3        4        5        6        7        8
  //   -9       -8       -7       -6       -5       -4       -3       -2       -1
  std::vector<storage::ScoreMember> gp17_sm{{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"},
                                            {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}};
  s = db.ZAdd("GP17_ZREMRANGEBYRANK_KEY", gp17_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP17_ZREMRANGEBYRANK_KEY", 9));
  ASSERT_TRUE(score_members_match(
      &db, "GP17_ZREMRANGEBYRANK_KEY",
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));
  s = db.ZRemrangebyrank("GP17_ZREMRANGEBYRANK_KEY", -100, 5, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(6, ret);
  ASSERT_TRUE(size_match(&db, "GP17_ZREMRANGEBYRANK_KEY", 3));
  ASSERT_TRUE(score_members_match(&db, "GP17_ZREMRANGEBYRANK_KEY", {{6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  // ***************** Group 18 Test *****************
  //
  // {0, MM0} {1, MM1} {2, MM2} {3, MM3} {4, MM4} {5, MM5} {6, MM6} {7, MM7} {8, MM8}
  //    0        1        2        3        4        5        6        7        8
  //   -9       -8       -7       -6       -5       -4       -3       -2       -1
  std::vector<storage::ScoreMember> gp18_sm{{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"},
                                            {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}};
  s = db.ZAdd("GP18_ZREMRANGEBYRANK_KEY", gp18_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP18_ZREMRANGEBYRANK_KEY", 9));
  ASSERT_TRUE(score_members_match(
      &db, "GP18_ZREMRANGEBYRANK_KEY",
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));
  s = db.ZRemrangebyrank("GP18_ZREMRANGEBYRANK_KEY", -100, -4, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(6, ret);
  ASSERT_TRUE(size_match(&db, "GP18_ZREMRANGEBYRANK_KEY", 3));
  ASSERT_TRUE(score_members_match(&db, "GP18_ZREMRANGEBYRANK_KEY", {{6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  // ***************** Group 19 Test *****************
  //
  // {0, MM0} {1, MM1} {2, MM2} {3, MM3} {4, MM4} {5, MM5} {6, MM6} {7, MM7} {8, MM8}
  //    0        1        2        3        4        5        6        7        8
  //   -9       -8       -7       -6       -5       -4       -3       -2       -1
  std::vector<storage::ScoreMember> gp19_sm{{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"},
                                            {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}};
  s = db.ZAdd("GP19_ZREMRANGEBYRANK_KEY", gp19_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP19_ZREMRANGEBYRANK_KEY", 9));
  ASSERT_TRUE(score_members_match(
      &db, "GP19_ZREMRANGEBYRANK_KEY",
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));
  s = db.ZRemrangebyrank("GP19_ZREMRANGEBYRANK_KEY", 3, 5, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(3, ret);
  ASSERT_TRUE(size_match(&db, "GP19_ZREMRANGEBYRANK_KEY", 6));
  ASSERT_TRUE(score_members_match(&db, "GP19_ZREMRANGEBYRANK_KEY",
                                  {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  // ***************** Group 20 Test *****************
  //
  // {0, MM0} {1, MM1} {2, MM2} {3, MM3} {4, MM4} {5, MM5} {6, MM6} {7, MM7} {8, MM8}
  //    0        1        2        3        4        5        6        7        8
  //   -9       -8       -7       -6       -5       -4       -3       -2       -1
  std::vector<storage::ScoreMember> gp20_sm{{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"},
                                            {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}};
  s = db.ZAdd("GP20_ZREMRANGEBYRANK_KEY", gp20_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP20_ZREMRANGEBYRANK_KEY", 9));
  ASSERT_TRUE(score_members_match(
      &db, "GP20_ZREMRANGEBYRANK_KEY",
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));
  s = db.ZRemrangebyrank("GP20_ZREMRANGEBYRANK_KEY", -6, -4, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(3, ret);
  ASSERT_TRUE(size_match(&db, "GP20_ZREMRANGEBYRANK_KEY", 6));
  ASSERT_TRUE(score_members_match(&db, "GP20_ZREMRANGEBYRANK_KEY",
                                  {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  // ***************** Group 21 Test *****************
  //
  // {0, MM0} {1, MM1} {2, MM2} {3, MM3} {4, MM4} {5, MM5} {6, MM6} {7, MM7} {8, MM8}
  //    0        1        2        3        4        5        6        7        8
  //   -9       -8       -7       -6       -5       -4       -3       -2       -1
  std::vector<storage::ScoreMember> gp21_sm{{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"},
                                            {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}};
  s = db.ZAdd("GP21_ZREMRANGEBYRANK_KEY", gp21_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP21_ZREMRANGEBYRANK_KEY", 9));
  ASSERT_TRUE(score_members_match(
      &db, "GP21_ZREMRANGEBYRANK_KEY",
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));
  s = db.ZRemrangebyrank("GP21_ZREMRANGEBYRANK_KEY", 3, -4, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(3, ret);
  ASSERT_TRUE(size_match(&db, "GP21_ZREMRANGEBYRANK_KEY", 6));
  ASSERT_TRUE(score_members_match(&db, "GP21_ZREMRANGEBYRANK_KEY",
                                  {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  // ***************** Group 22 Test *****************
  //
  // {0, MM0} {1, MM1} {2, MM2} {3, MM3} {4, MM4} {5, MM5} {6, MM6} {7, MM7} {8, MM8}
  //    0        1        2        3        4        5        6        7        8
  //   -9       -8       -7       -6       -5       -4       -3       -2       -1
  std::vector<storage::ScoreMember> gp22_sm{{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"},
                                            {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}};
  s = db.ZAdd("GP22_ZREMRANGEBYRANK_KEY", gp22_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP22_ZREMRANGEBYRANK_KEY", 9));
  ASSERT_TRUE(score_members_match(
      &db, "GP22_ZREMRANGEBYRANK_KEY",
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));
  s = db.ZRemrangebyrank("GP22_ZREMRANGEBYRANK_KEY", -6, 5, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(3, ret);
  ASSERT_TRUE(size_match(&db, "GP22_ZREMRANGEBYRANK_KEY", 6));
  ASSERT_TRUE(score_members_match(&db, "GP22_ZREMRANGEBYRANK_KEY",
                                  {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  // ***************** Group 23 Test *****************
  //
  // {0, MM0} {1, MM1} {2, MM2} {3, MM3} {4, MM4} {5, MM5} {6, MM6} {7, MM7} {8, MM8}
  //    0        1        2        3        4        5        6        7        8
  //   -9       -8       -7       -6       -5       -4       -3       -2       -1
  std::vector<storage::ScoreMember> gp23_sm{{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"},
                                            {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}};
  s = db.ZAdd("GP23_ZREMRANGEBYRANK_KEY", gp23_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP23_ZREMRANGEBYRANK_KEY", 9));
  ASSERT_TRUE(score_members_match(
      &db, "GP23_ZREMRANGEBYRANK_KEY",
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));
  s = db.ZRemrangebyrank("GP23_ZREMRANGEBYRANK_KEY", 3, 8, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(6, ret);
  ASSERT_TRUE(size_match(&db, "GP23_ZREMRANGEBYRANK_KEY", 3));
  ASSERT_TRUE(score_members_match(&db, "GP23_ZREMRANGEBYRANK_KEY", {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}}));

  // ***************** Group 24 Test *****************
  //
  // {0, MM0} {1, MM1} {2, MM2} {3, MM3} {4, MM4} {5, MM5} {6, MM6} {7, MM7} {8, MM8}
  //    0        1        2        3        4        5        6        7        8
  //   -9       -8       -7       -6       -5       -4       -3       -2       -1
  std::vector<storage::ScoreMember> gp24_sm{{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"},
                                            {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}};
  s = db.ZAdd("GP24_ZREMRANGEBYRANK_KEY", gp24_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP24_ZREMRANGEBYRANK_KEY", 9));
  ASSERT_TRUE(score_members_match(
      &db, "GP24_ZREMRANGEBYRANK_KEY",
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));
  s = db.ZRemrangebyrank("GP24_ZREMRANGEBYRANK_KEY", -6, -1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(6, ret);
  ASSERT_TRUE(size_match(&db, "GP24_ZREMRANGEBYRANK_KEY", 3));
  ASSERT_TRUE(score_members_match(&db, "GP24_ZREMRANGEBYRANK_KEY", {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}}));

  // ***************** Group 25 Test *****************
  //
  // {0, MM0} {1, MM1} {2, MM2} {3, MM3} {4, MM4} {5, MM5} {6, MM6} {7, MM7} {8, MM8}
  //    0        1        2        3        4        5        6        7        8
  //   -9       -8       -7       -6       -5       -4       -3       -2       -1
  std::vector<storage::ScoreMember> gp25_sm{{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"},
                                            {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}};
  s = db.ZAdd("GP25_ZREMRANGEBYRANK_KEY", gp25_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP25_ZREMRANGEBYRANK_KEY", 9));
  ASSERT_TRUE(score_members_match(
      &db, "GP25_ZREMRANGEBYRANK_KEY",
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));
  s = db.ZRemrangebyrank("GP25_ZREMRANGEBYRANK_KEY", 3, -1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(6, ret);
  ASSERT_TRUE(size_match(&db, "GP25_ZREMRANGEBYRANK_KEY", 3));
  ASSERT_TRUE(score_members_match(&db, "GP25_ZREMRANGEBYRANK_KEY", {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}}));

  // ***************** Group 26 Test *****************
  //
  // {0, MM0} {1, MM1} {2, MM2} {3, MM3} {4, MM4} {5, MM5} {6, MM6} {7, MM7} {8, MM8}
  //    0        1        2        3        4        5        6        7        8
  //   -9       -8       -7       -6       -5       -4       -3       -2       -1
  std::vector<storage::ScoreMember> gp26_sm{{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"},
                                            {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}};
  s = db.ZAdd("GP26_ZREMRANGEBYRANK_KEY", gp26_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP26_ZREMRANGEBYRANK_KEY", 9));
  ASSERT_TRUE(score_members_match(
      &db, "GP26_ZREMRANGEBYRANK_KEY",
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));
  s = db.ZRemrangebyrank("GP26_ZREMRANGEBYRANK_KEY", -6, 8, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(6, ret);
  ASSERT_TRUE(size_match(&db, "GP26_ZREMRANGEBYRANK_KEY", 3));
  ASSERT_TRUE(score_members_match(&db, "GP26_ZREMRANGEBYRANK_KEY", {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}}));

  // ***************** Group 27 Test *****************
  //
  // {0, MM0} {1, MM1} {2, MM2} {3, MM3} {4, MM4} {5, MM5} {6, MM6} {7, MM7} {8, MM8}
  //    0        1        2        3        4        5        6        7        8
  //   -9       -8       -7       -6       -5       -4       -3       -2       -1
  std::vector<storage::ScoreMember> gp27_sm{{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"},
                                            {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}};
  s = db.ZAdd("GP27_ZREMRANGEBYRANK_KEY", gp27_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP27_ZREMRANGEBYRANK_KEY", 9));
  ASSERT_TRUE(score_members_match(
      &db, "GP27_ZREMRANGEBYRANK_KEY",
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));
  s = db.ZRemrangebyrank("GP27_ZREMRANGEBYRANK_KEY", -6, 100, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(6, ret);
  ASSERT_TRUE(size_match(&db, "GP27_ZREMRANGEBYRANK_KEY", 3));
  ASSERT_TRUE(score_members_match(&db, "GP27_ZREMRANGEBYRANK_KEY", {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}}));

  // ***************** Group 28 Test *****************
  //
  // {0, MM0} {1, MM1} {2, MM2} {3, MM3} {4, MM4} {5, MM5} {6, MM6} {7, MM7} {8, MM8}
  //    0        1        2        3        4        5        6        7        8
  //   -9       -8       -7       -6       -5       -4       -3       -2       -1
  std::vector<storage::ScoreMember> gp28_sm{{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"},
                                            {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}};
  s = db.ZAdd("GP28_ZREMRANGEBYRANK_KEY", gp28_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP28_ZREMRANGEBYRANK_KEY", 9));
  ASSERT_TRUE(score_members_match(
      &db, "GP28_ZREMRANGEBYRANK_KEY",
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));
  s = db.ZRemrangebyrank("GP28_ZREMRANGEBYRANK_KEY", 3, 100, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(6, ret);
  ASSERT_TRUE(size_match(&db, "GP28_ZREMRANGEBYRANK_KEY", 3));
  ASSERT_TRUE(score_members_match(&db, "GP28_ZREMRANGEBYRANK_KEY", {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}}));

  // ***************** Group 29 Test *****************
  //
  // {0, MM0} {1, MM1} {2, MM2} {3, MM3} {4, MM4} {5, MM5} {6, MM6} {7, MM7} {8, MM8}
  //    0        1        2        3        4        5        6        7        8
  //   -9       -8       -7       -6       -5       -4       -3       -2       -1
  std::vector<storage::ScoreMember> gp29_sm{{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"},
                                            {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}};
  s = db.ZAdd("GP29_ZREMRANGEBYRANK_KEY", gp29_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP29_ZREMRANGEBYRANK_KEY", 9));
  ASSERT_TRUE(score_members_match(
      &db, "GP29_ZREMRANGEBYRANK_KEY",
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));
  ASSERT_TRUE(make_expired(&db, "GP29_ZREMRANGEBYRANK_KEY"));
  s = db.ZRemrangebyrank("GP29_ZREMRANGEBYRANK_KEY", 0, 0, &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(0, ret);
  ASSERT_TRUE(size_match(&db, "GP29_ZREMRANGEBYRANK_KEY", 0));
  ASSERT_TRUE(score_members_match(&db, "GP29_ZREMRANGEBYRANK_KEY", {}));

  // ***************** Group 30 Test *****************
  s = db.ZRemrangebyrank("GP30_ZREMRANGEBYRANK_KEY", 0, 0, &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(0, ret);
  ASSERT_TRUE(size_match(&db, "GP30_ZREMRANGEBYRANK_KEY", 0));
  ASSERT_TRUE(score_members_match(&db, "GP30_ZREMRANGEBYRANK_KEY", {}));
}

// ZRemrangebyscore
TEST_F(ZSetsTest, ZRemrangebyscoreTest) {
  int32_t ret;

  // ***************** Group 1 Test *****************
  std::vector<storage::ScoreMember> gp1_sm{{-987654321.0000001, "MM1"},
                                           {-87654321.00000001, "MM2"},
                                           {-7654321.000000001, "MM3"},
                                           {-654321.0000000001, "MM4"},
                                           {-54321.00000000001, "MM5"},
                                           {-4321.000000000001, "MM6"},
                                           {-1000.000000000001, "MM7"},
                                           {-1000.000000000001, "MM8"},
                                           {-1000.000000000001, "MM9"},
                                           {-100.0000000000001, "MM10"},
                                           {0, "MM11"},
                                           {100.0000000000001, "MM12"},
                                           {4321.000000000001, "MM13"},
                                           {54321.00000000001, "MM14"},
                                           {654321.0000000001, "MM15"},
                                           {7654321.000000001, "MM16"},
                                           {87654321.00000001, "MM17"},
                                           {987654321.0000001, "MM18"}};

  s = db.ZAdd("GP1_ZREMRANGEBYSCORE_KEY", gp1_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(18, ret);

  s = db.ZRemrangebyscore("GP1_ZREMRANGEBYSCORE_KEY", std::numeric_limits<double>::lowest(),
                          std::numeric_limits<double>::max(), true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(18, ret);
  ASSERT_TRUE(size_match(&db, "GP1_ZREMRANGEBYSCORE_KEY", 0));
  ASSERT_TRUE(score_members_match(&db, "GP1_ZREMRANGEBYSCORE_KEY", {}));

  // ***************** Group 2 Test *****************
  std::vector<storage::ScoreMember> gp2_sm{{-987654321.0000001, "MM1"},
                                           {-87654321.00000001, "MM2"},
                                           {-7654321.000000001, "MM3"},
                                           {-654321.0000000001, "MM4"},
                                           {-54321.00000000001, "MM5"},
                                           {-4321.000000000001, "MM6"},
                                           {-1000.000000000001, "MM7"},
                                           {-1000.000000000001, "MM8"},
                                           {-1000.000000000001, "MM9"},
                                           {-100.0000000000001, "MM10"},
                                           {0, "MM11"},
                                           {100.0000000000001, "MM12"},
                                           {4321.000000000001, "MM13"},
                                           {54321.00000000001, "MM14"},
                                           {654321.0000000001, "MM15"},
                                           {7654321.000000001, "MM16"},
                                           {87654321.00000001, "MM17"},
                                           {987654321.0000001, "MM18"}};

  s = db.ZAdd("GP2_ZREMRANGEBYSCORE_KEY", gp2_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(18, ret);
  s = db.ZRemrangebyscore("GP2_ZREMRANGEBYSCORE_KEY", -10000000000, -999999999, true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(0, ret);
  ASSERT_TRUE(size_match(&db, "GP2_ZREMRANGEBYSCORE_KEY", 18));
  ASSERT_TRUE(score_members_match(&db, "GP2_ZREMRANGEBYSCORE_KEY",
                                  {{-987654321.0000001, "MM1"},
                                   {-87654321.00000001, "MM2"},
                                   {-7654321.000000001, "MM3"},
                                   {-654321.0000000001, "MM4"},
                                   {-54321.00000000001, "MM5"},
                                   {-4321.000000000001, "MM6"},
                                   {-1000.000000000001, "MM7"},
                                   {-1000.000000000001, "MM8"},
                                   {-1000.000000000001, "MM9"},
                                   {-100.0000000000001, "MM10"},
                                   {0, "MM11"},
                                   {100.0000000000001, "MM12"},
                                   {4321.000000000001, "MM13"},
                                   {54321.00000000001, "MM14"},
                                   {654321.0000000001, "MM15"},
                                   {7654321.000000001, "MM16"},
                                   {87654321.00000001, "MM17"},
                                   {987654321.0000001, "MM18"}}));

  // ***************** Group 3 Test *****************
  std::vector<storage::ScoreMember> gp3_sm{{-987654321.0000001, "MM1"},
                                           {-87654321.00000001, "MM2"},
                                           {-7654321.000000001, "MM3"},
                                           {-654321.0000000001, "MM4"},
                                           {-54321.00000000001, "MM5"},
                                           {-4321.000000000001, "MM6"},
                                           {-1000.000000000001, "MM7"},
                                           {-1000.000000000001, "MM8"},
                                           {-1000.000000000001, "MM9"},
                                           {-100.0000000000001, "MM10"},
                                           {0, "MM11"},
                                           {100.0000000000001, "MM12"},
                                           {4321.000000000001, "MM13"},
                                           {54321.00000000001, "MM14"},
                                           {654321.0000000001, "MM15"},
                                           {7654321.000000001, "MM16"},
                                           {87654321.00000001, "MM17"},
                                           {987654321.0000001, "MM18"}};

  s = db.ZAdd("GP3_ZREMRANGEBYSCORE_KEY", gp3_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(18, ret);
  s = db.ZRemrangebyscore("GP3_ZREMRANGEBYSCORE_KEY", -987654321.0000001, -7654321.000000001, true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(3, ret);
  ASSERT_TRUE(size_match(&db, "GP3_ZREMRANGEBYSCORE_KEY", 15));
  ASSERT_TRUE(score_members_match(&db, "GP3_ZREMRANGEBYSCORE_KEY",
                                  {{-654321.0000000001, "MM4"},
                                   {-54321.00000000001, "MM5"},
                                   {-4321.000000000001, "MM6"},
                                   {-1000.000000000001, "MM7"},
                                   {-1000.000000000001, "MM8"},
                                   {-1000.000000000001, "MM9"},
                                   {-100.0000000000001, "MM10"},
                                   {0, "MM11"},
                                   {100.0000000000001, "MM12"},
                                   {4321.000000000001, "MM13"},
                                   {54321.00000000001, "MM14"},
                                   {654321.0000000001, "MM15"},
                                   {7654321.000000001, "MM16"},
                                   {87654321.00000001, "MM17"},
                                   {987654321.0000001, "MM18"}}));

  // ***************** Group 4 Test *****************
  std::vector<storage::ScoreMember> gp4_sm{{-987654321.0000001, "MM1"},
                                           {-87654321.00000001, "MM2"},
                                           {-7654321.000000001, "MM3"},
                                           {-654321.0000000001, "MM4"},
                                           {-54321.00000000001, "MM5"},
                                           {-4321.000000000001, "MM6"},
                                           {-1000.000000000001, "MM7"},
                                           {-1000.000000000001, "MM8"},
                                           {-1000.000000000001, "MM9"},
                                           {-100.0000000000001, "MM10"},
                                           {0, "MM11"},
                                           {100.0000000000001, "MM12"},
                                           {4321.000000000001, "MM13"},
                                           {54321.00000000001, "MM14"},
                                           {654321.0000000001, "MM15"},
                                           {7654321.000000001, "MM16"},
                                           {87654321.00000001, "MM17"},
                                           {987654321.0000001, "MM18"}};

  s = db.ZAdd("GP4_ZREMRANGEBYSCORE_KEY", gp4_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(18, ret);
  s = db.ZRemrangebyscore("GP4_ZREMRANGEBYSCORE_KEY", -999999999, -4321.000000000001, true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(6, ret);
  ASSERT_TRUE(size_match(&db, "GP4_ZREMRANGEBYSCORE_KEY", 12));
  ASSERT_TRUE(score_members_match(&db, "GP4_ZREMRANGEBYSCORE_KEY",
                                  {{-1000.000000000001, "MM7"},
                                   {-1000.000000000001, "MM8"},
                                   {-1000.000000000001, "MM9"},
                                   {-100.0000000000001, "MM10"},
                                   {0, "MM11"},
                                   {100.0000000000001, "MM12"},
                                   {4321.000000000001, "MM13"},
                                   {54321.00000000001, "MM14"},
                                   {654321.0000000001, "MM15"},
                                   {7654321.000000001, "MM16"},
                                   {87654321.00000001, "MM17"},
                                   {987654321.0000001, "MM18"}}));

  // ***************** Group 5 Test *****************
  std::vector<storage::ScoreMember> gp5_sm{{-987654321.0000001, "MM1"},
                                           {-87654321.00000001, "MM2"},
                                           {-7654321.000000001, "MM3"},
                                           {-654321.0000000001, "MM4"},
                                           {-54321.00000000001, "MM5"},
                                           {-4321.000000000001, "MM6"},
                                           {-1000.000000000001, "MM7"},
                                           {-1000.000000000001, "MM8"},
                                           {-1000.000000000001, "MM9"},
                                           {-100.0000000000001, "MM10"},
                                           {0, "MM11"},
                                           {100.0000000000001, "MM12"},
                                           {4321.000000000001, "MM13"},
                                           {54321.00000000001, "MM14"},
                                           {654321.0000000001, "MM15"},
                                           {7654321.000000001, "MM16"},
                                           {87654321.00000001, "MM17"},
                                           {987654321.0000001, "MM18"}};

  s = db.ZAdd("GP5_ZREMRANGEBYSCORE_KEY", gp5_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(18, ret);
  s = db.ZRemrangebyscore("GP5_ZREMRANGEBYSCORE_KEY", -1000.000000000001, -1000.000000000001, true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(3, ret);
  ASSERT_TRUE(size_match(&db, "GP5_ZREMRANGEBYSCORE_KEY", 15));
  ASSERT_TRUE(score_members_match(&db, "GP5_ZREMRANGEBYSCORE_KEY",
                                  {{-987654321.0000001, "MM1"},
                                   {-87654321.00000001, "MM2"},
                                   {-7654321.000000001, "MM3"},
                                   {-654321.0000000001, "MM4"},
                                   {-54321.00000000001, "MM5"},
                                   {-4321.000000000001, "MM6"},
                                   {-100.0000000000001, "MM10"},
                                   {0, "MM11"},
                                   {100.0000000000001, "MM12"},
                                   {4321.000000000001, "MM13"},
                                   {54321.00000000001, "MM14"},
                                   {654321.0000000001, "MM15"},
                                   {7654321.000000001, "MM16"},
                                   {87654321.00000001, "MM17"},
                                   {987654321.0000001, "MM18"}}));

  // ***************** Group 6 Test *****************
  std::vector<storage::ScoreMember> gp6_sm{{-987654321.0000001, "MM1"},
                                           {-87654321.00000001, "MM2"},
                                           {-7654321.000000001, "MM3"},
                                           {-654321.0000000001, "MM4"},
                                           {-54321.00000000001, "MM5"},
                                           {-4321.000000000001, "MM6"},
                                           {-1000.000000000001, "MM7"},
                                           {-1000.000000000001, "MM8"},
                                           {-1000.000000000001, "MM9"},
                                           {-100.0000000000001, "MM10"},
                                           {0, "MM11"},
                                           {100.0000000000001, "MM12"},
                                           {4321.000000000001, "MM13"},
                                           {54321.00000000001, "MM14"},
                                           {654321.0000000001, "MM15"},
                                           {7654321.000000001, "MM16"},
                                           {87654321.00000001, "MM17"},
                                           {987654321.0000001, "MM18"}};

  s = db.ZAdd("GP6_ZREMRANGEBYSCORE_KEY", gp6_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(18, ret);
  s = db.ZRemrangebyscore("GP6_ZREMRANGEBYSCORE_KEY", -100.0000000000001, 100.0000000000001, true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(3, ret);
  ASSERT_TRUE(size_match(&db, "GP6_ZREMRANGEBYSCORE_KEY", 15));
  ASSERT_TRUE(score_members_match(&db, "GP6_ZREMRANGEBYSCORE_KEY",
                                  {{-987654321.0000001, "MM1"},
                                   {-87654321.00000001, "MM2"},
                                   {-7654321.000000001, "MM3"},
                                   {-654321.0000000001, "MM4"},
                                   {-54321.00000000001, "MM5"},
                                   {-4321.000000000001, "MM6"},
                                   {-1000.000000000001, "MM7"},
                                   {-1000.000000000001, "MM8"},
                                   {-1000.000000000001, "MM9"},
                                   {4321.000000000001, "MM13"},
                                   {54321.00000000001, "MM14"},
                                   {654321.0000000001, "MM15"},
                                   {7654321.000000001, "MM16"},
                                   {87654321.00000001, "MM17"},
                                   {987654321.0000001, "MM18"}}));

  // ***************** Group 7 Test *****************
  std::vector<storage::ScoreMember> gp7_sm{{-987654321.0000001, "MM1"},
                                           {-87654321.00000001, "MM2"},
                                           {-7654321.000000001, "MM3"},
                                           {-654321.0000000001, "MM4"},
                                           {-54321.00000000001, "MM5"},
                                           {-4321.000000000001, "MM6"},
                                           {-1000.000000000001, "MM7"},
                                           {-1000.000000000001, "MM8"},
                                           {-1000.000000000001, "MM9"},
                                           {-100.0000000000001, "MM10"},
                                           {0, "MM11"},
                                           {100.0000000000001, "MM12"},
                                           {4321.000000000001, "MM13"},
                                           {54321.00000000001, "MM14"},
                                           {654321.0000000001, "MM15"},
                                           {7654321.000000001, "MM16"},
                                           {87654321.00000001, "MM17"},
                                           {987654321.0000001, "MM18"}};

  s = db.ZAdd("GP7_ZREMRANGEBYSCORE_KEY", gp7_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(18, ret);
  s = db.ZRemrangebyscore("GP7_ZREMRANGEBYSCORE_KEY", 0, 0, true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(1, ret);
  ASSERT_TRUE(size_match(&db, "GP7_ZREMRANGEBYSCORE_KEY", 17));
  ASSERT_TRUE(score_members_match(&db, "GP7_ZREMRANGEBYSCORE_KEY",
                                  {{-987654321.0000001, "MM1"},
                                   {-87654321.00000001, "MM2"},
                                   {-7654321.000000001, "MM3"},
                                   {-654321.0000000001, "MM4"},
                                   {-54321.00000000001, "MM5"},
                                   {-4321.000000000001, "MM6"},
                                   {-1000.000000000001, "MM7"},
                                   {-1000.000000000001, "MM8"},
                                   {-1000.000000000001, "MM9"},
                                   {-100.0000000000001, "MM10"},
                                   {100.0000000000001, "MM12"},
                                   {4321.000000000001, "MM13"},
                                   {54321.00000000001, "MM14"},
                                   {654321.0000000001, "MM15"},
                                   {7654321.000000001, "MM16"},
                                   {87654321.00000001, "MM17"},
                                   {987654321.0000001, "MM18"}}));

  // ***************** Group 8 Test *****************
  std::vector<storage::ScoreMember> gp8_sm{{-987654321.0000001, "MM1"},
                                           {-87654321.00000001, "MM2"},
                                           {-7654321.000000001, "MM3"},
                                           {-654321.0000000001, "MM4"},
                                           {-54321.00000000001, "MM5"},
                                           {-4321.000000000001, "MM6"},
                                           {-1000.000000000001, "MM7"},
                                           {-1000.000000000001, "MM8"},
                                           {-1000.000000000001, "MM9"},
                                           {-100.0000000000001, "MM10"},
                                           {0, "MM11"},
                                           {100.0000000000001, "MM12"},
                                           {4321.000000000001, "MM13"},
                                           {54321.00000000001, "MM14"},
                                           {654321.0000000001, "MM15"},
                                           {7654321.000000001, "MM16"},
                                           {87654321.00000001, "MM17"},
                                           {987654321.0000001, "MM18"}};

  s = db.ZAdd("GP8_ZREMRANGEBYSCORE_KEY", gp8_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(18, ret);
  s = db.ZRemrangebyscore("GP8_ZREMRANGEBYSCORE_KEY", 4321.000000000001, 654321.0000000001, true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(3, ret);
  ASSERT_TRUE(size_match(&db, "GP8_ZREMRANGEBYSCORE_KEY", 15));
  ASSERT_TRUE(score_members_match(&db, "GP8_ZREMRANGEBYSCORE_KEY",
                                  {{-987654321.0000001, "MM1"},
                                   {-87654321.00000001, "MM2"},
                                   {-7654321.000000001, "MM3"},
                                   {-654321.0000000001, "MM4"},
                                   {-54321.00000000001, "MM5"},
                                   {-4321.000000000001, "MM6"},
                                   {-1000.000000000001, "MM7"},
                                   {-1000.000000000001, "MM8"},
                                   {-1000.000000000001, "MM9"},
                                   {-100.0000000000001, "MM10"},
                                   {0, "MM11"},
                                   {100.0000000000001, "MM12"},
                                   {7654321.000000001, "MM16"},
                                   {87654321.00000001, "MM17"},
                                   {987654321.0000001, "MM18"}}));

  // ***************** Group 9 Test *****************
  std::vector<storage::ScoreMember> gp9_sm{{-987654321.0000001, "MM1"},
                                           {-87654321.00000001, "MM2"},
                                           {-7654321.000000001, "MM3"},
                                           {-654321.0000000001, "MM4"},
                                           {-54321.00000000001, "MM5"},
                                           {-4321.000000000001, "MM6"},
                                           {-1000.000000000001, "MM7"},
                                           {-1000.000000000001, "MM8"},
                                           {-1000.000000000001, "MM9"},
                                           {-100.0000000000001, "MM10"},
                                           {0, "MM11"},
                                           {100.0000000000001, "MM12"},
                                           {4321.000000000001, "MM13"},
                                           {54321.00000000001, "MM14"},
                                           {654321.0000000001, "MM15"},
                                           {7654321.000000001, "MM16"},
                                           {87654321.00000001, "MM17"},
                                           {987654321.0000001, "MM18"}};

  s = db.ZAdd("GP9_ZREMRANGEBYSCORE_KEY", gp9_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(18, ret);
  s = db.ZRemrangebyscore("GP9_ZREMRANGEBYSCORE_KEY", 4321.000000000001, 987654321.0000001, true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(6, ret);
  ASSERT_TRUE(size_match(&db, "GP9_ZREMRANGEBYSCORE_KEY", 12));
  ASSERT_TRUE(score_members_match(&db, "GP9_ZREMRANGEBYSCORE_KEY",
                                  {{-987654321.0000001, "MM1"},
                                   {-87654321.00000001, "MM2"},
                                   {-7654321.000000001, "MM3"},
                                   {-654321.0000000001, "MM4"},
                                   {-54321.00000000001, "MM5"},
                                   {-4321.000000000001, "MM6"},
                                   {-1000.000000000001, "MM7"},
                                   {-1000.000000000001, "MM8"},
                                   {-1000.000000000001, "MM9"},
                                   {-100.0000000000001, "MM10"},
                                   {0, "MM11"},
                                   {100.0000000000001, "MM12"}}));

  // ***************** Group 10 Test *****************
  std::vector<storage::ScoreMember> gp10_sm{{-987654321.0000001, "MM1"},
                                            {-87654321.00000001, "MM2"},
                                            {-7654321.000000001, "MM3"},
                                            {-654321.0000000001, "MM4"},
                                            {-54321.00000000001, "MM5"},
                                            {-4321.000000000001, "MM6"},
                                            {-1000.000000000001, "MM7"},
                                            {-1000.000000000001, "MM8"},
                                            {-1000.000000000001, "MM9"},
                                            {-100.0000000000001, "MM10"},
                                            {0, "MM11"},
                                            {100.0000000000001, "MM12"},
                                            {4321.000000000001, "MM13"},
                                            {54321.00000000001, "MM14"},
                                            {654321.0000000001, "MM15"},
                                            {7654321.000000001, "MM16"},
                                            {87654321.00000001, "MM17"},
                                            {987654321.0000001, "MM18"}};

  s = db.ZAdd("GP10_ZREMRANGEBYSCORE_KEY", gp10_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(18, ret);
  s = db.ZRemrangebyscore("GP10_ZREMRANGEBYSCORE_KEY", 987654321.0000001, 987654321.0000001, true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(1, ret);
  ASSERT_TRUE(size_match(&db, "GP10_ZREMRANGEBYSCORE_KEY", 17));
  ASSERT_TRUE(score_members_match(&db, "GP10_ZREMRANGEBYSCORE_KEY",
                                  {{-987654321.0000001, "MM1"},
                                   {-87654321.00000001, "MM2"},
                                   {-7654321.000000001, "MM3"},
                                   {-654321.0000000001, "MM4"},
                                   {-54321.00000000001, "MM5"},
                                   {-4321.000000000001, "MM6"},
                                   {-1000.000000000001, "MM7"},
                                   {-1000.000000000001, "MM8"},
                                   {-1000.000000000001, "MM9"},
                                   {-100.0000000000001, "MM10"},
                                   {0, "MM11"},
                                   {100.0000000000001, "MM12"},
                                   {4321.000000000001, "MM13"},
                                   {54321.00000000001, "MM14"},
                                   {654321.0000000001, "MM15"},
                                   {7654321.000000001, "MM16"},
                                   {87654321.00000001, "MM17"}}));

  // ***************** Group 11 Test *****************
  std::vector<storage::ScoreMember> gp11_sm{{-987654321.0000001, "MM1"},
                                            {-87654321.00000001, "MM2"},
                                            {-7654321.000000001, "MM3"},
                                            {-654321.0000000001, "MM4"},
                                            {-54321.00000000001, "MM5"},
                                            {-4321.000000000001, "MM6"},
                                            {-1000.000000000001, "MM7"},
                                            {-1000.000000000001, "MM8"},
                                            {-1000.000000000001, "MM9"},
                                            {-100.0000000000001, "MM10"},
                                            {0, "MM11"},
                                            {100.0000000000001, "MM12"},
                                            {4321.000000000001, "MM13"},
                                            {54321.00000000001, "MM14"},
                                            {654321.0000000001, "MM15"},
                                            {7654321.000000001, "MM16"},
                                            {87654321.00000001, "MM17"},
                                            {987654321.0000001, "MM18"}};

  s = db.ZAdd("GP11_ZREMRANGEBYSCORE_KEY", gp11_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(18, ret);
  ASSERT_TRUE(make_expired(&db, "GP11_ZREMRANGEBYSCORE_KEY"));

  s = db.ZRemrangebyscore("GP11_ZREMRANGEBYSCORE_KEY", std::numeric_limits<double>::lowest(),
                          std::numeric_limits<double>::max(), true, true, &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(0, ret);
  ASSERT_TRUE(size_match(&db, "GP11_ZREMRANGEBYSCORE_KEY", 0));
  ASSERT_TRUE(score_members_match(&db, "GP11_ZREMRANGEBYSCORE_KEY", {}));

  // ***************** Group 12 Test *****************
  s = db.ZRemrangebyscore("GP12_ZREMRANGEBYSCORE_KEY", std::numeric_limits<double>::lowest(),
                          std::numeric_limits<double>::max(), true, true, &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(0, ret);
  ASSERT_TRUE(size_match(&db, "GP12_ZREMRANGEBYSCORE_KEY", 0));

  // ***************** Group 13 Test *****************
  std::vector<storage::ScoreMember> gp13_sm{{0, "MM0"}};

  s = db.ZAdd("GP13_ZREMRANGEBYSCORE_KEY", gp13_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(1, ret);

  s = db.ZRemrangebyscore("GP13_ZREMRANGEBYSCORE_KEY", -1, 1, true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(1, ret);
  ASSERT_TRUE(size_match(&db, "GP13_ZREMRANGEBYSCORE_KEY", 0));
  ASSERT_TRUE(score_members_match(&db, "GP13_ZREMRANGEBYSCORE_KEY", {}));

  // ***************** Group 14 Test *****************
  std::vector<storage::ScoreMember> gp14_sm{{-987654321.0000001, "MM1"},
                                            {-87654321.00000001, "MM2"},
                                            {-7654321.000000001, "MM3"},
                                            {-654321.0000000001, "MM4"},
                                            {-54321.00000000001, "MM5"},
                                            {-4321.000000000001, "MM6"},
                                            {-1000.000000000001, "MM7"},
                                            {-1000.000000000001, "MM8"},
                                            {-1000.000000000001, "MM9"},
                                            {-100.0000000000001, "MM10"},
                                            {0, "MM11"},
                                            {100.0000000000001, "MM12"},
                                            {4321.000000000001, "MM13"},
                                            {54321.00000000001, "MM14"},
                                            {654321.0000000001, "MM15"},
                                            {7654321.000000001, "MM16"},
                                            {87654321.00000001, "MM17"},
                                            {987654321.0000001, "MM18"}};

  s = db.ZAdd("GP14_ZREMRANGEBYSCORE_KEY", gp14_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(18, ret);
  s = db.ZRemrangebyscore("GP14_ZREMRANGEBYSCORE_KEY", -987654321.0000001, -7654321.000000001, false, false, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(1, ret);
  ASSERT_TRUE(size_match(&db, "GP14_ZREMRANGEBYSCORE_KEY", 17));
  ASSERT_TRUE(score_members_match(&db, "GP14_ZREMRANGEBYSCORE_KEY",
                                  {{-987654321.0000001, "MM1"},
                                   {-7654321.000000001, "MM3"},
                                   {-654321.0000000001, "MM4"},
                                   {-54321.00000000001, "MM5"},
                                   {-4321.000000000001, "MM6"},
                                   {-1000.000000000001, "MM7"},
                                   {-1000.000000000001, "MM8"},
                                   {-1000.000000000001, "MM9"},
                                   {-100.0000000000001, "MM10"},
                                   {0, "MM11"},
                                   {100.0000000000001, "MM12"},
                                   {4321.000000000001, "MM13"},
                                   {54321.00000000001, "MM14"},
                                   {654321.0000000001, "MM15"},
                                   {7654321.000000001, "MM16"},
                                   {87654321.00000001, "MM17"},
                                   {987654321.0000001, "MM18"}}));

  // ***************** Group 15 Test *****************
  std::vector<storage::ScoreMember> gp15_sm{{-987654321.0000001, "MM1"},
                                            {-87654321.00000001, "MM2"},
                                            {-7654321.000000001, "MM3"},
                                            {-654321.0000000001, "MM4"},
                                            {-54321.00000000001, "MM5"},
                                            {-4321.000000000001, "MM6"},
                                            {-1000.000000000001, "MM7"},
                                            {-1000.000000000001, "MM8"},
                                            {-1000.000000000001, "MM9"},
                                            {-100.0000000000001, "MM10"},
                                            {0, "MM11"},
                                            {100.0000000000001, "MM12"},
                                            {4321.000000000001, "MM13"},
                                            {54321.00000000001, "MM14"},
                                            {654321.0000000001, "MM15"},
                                            {7654321.000000001, "MM16"},
                                            {87654321.00000001, "MM17"},
                                            {987654321.0000001, "MM18"}};

  s = db.ZAdd("GP15_ZREMRANGEBYSCORE_KEY", gp15_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(18, ret);
  s = db.ZRemrangebyscore("GP15_ZREMRANGEBYSCORE_KEY", -987654321.0000001, -7654321.000000001, true, false, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(2, ret);
  ASSERT_TRUE(size_match(&db, "GP15_ZREMRANGEBYSCORE_KEY", 16));
  ASSERT_TRUE(score_members_match(&db, "GP15_ZREMRANGEBYSCORE_KEY",
                                  {{-7654321.000000001, "MM3"},
                                   {-654321.0000000001, "MM4"},
                                   {-54321.00000000001, "MM5"},
                                   {-4321.000000000001, "MM6"},
                                   {-1000.000000000001, "MM7"},
                                   {-1000.000000000001, "MM8"},
                                   {-1000.000000000001, "MM9"},
                                   {-100.0000000000001, "MM10"},
                                   {0, "MM11"},
                                   {100.0000000000001, "MM12"},
                                   {4321.000000000001, "MM13"},
                                   {54321.00000000001, "MM14"},
                                   {654321.0000000001, "MM15"},
                                   {7654321.000000001, "MM16"},
                                   {87654321.00000001, "MM17"},
                                   {987654321.0000001, "MM18"}}));

  // ***************** Group 16 Test *****************
  std::vector<storage::ScoreMember> gp16_sm{{-987654321.0000001, "MM1"},
                                            {-87654321.00000001, "MM2"},
                                            {-7654321.000000001, "MM3"},
                                            {-654321.0000000001, "MM4"},
                                            {-54321.00000000001, "MM5"},
                                            {-4321.000000000001, "MM6"},
                                            {-1000.000000000001, "MM7"},
                                            {-1000.000000000001, "MM8"},
                                            {-1000.000000000001, "MM9"},
                                            {-100.0000000000001, "MM10"},
                                            {0, "MM11"},
                                            {100.0000000000001, "MM12"},
                                            {4321.000000000001, "MM13"},
                                            {54321.00000000001, "MM14"},
                                            {654321.0000000001, "MM15"},
                                            {7654321.000000001, "MM16"},
                                            {87654321.00000001, "MM17"},
                                            {987654321.0000001, "MM18"}};

  s = db.ZAdd("GP16_ZREMRANGEBYSCORE_KEY", gp16_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(18, ret);
  s = db.ZRemrangebyscore("GP16_ZREMRANGEBYSCORE_KEY", -987654321.0000001, -7654321.000000001, false, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(2, ret);
  ASSERT_TRUE(size_match(&db, "GP16_ZREMRANGEBYSCORE_KEY", 16));
  ASSERT_TRUE(score_members_match(&db, "GP16_ZREMRANGEBYSCORE_KEY",
                                  {{-987654321.0000001, "MM1"},
                                   {-654321.0000000001, "MM4"},
                                   {-54321.00000000001, "MM5"},
                                   {-4321.000000000001, "MM6"},
                                   {-1000.000000000001, "MM7"},
                                   {-1000.000000000001, "MM8"},
                                   {-1000.000000000001, "MM9"},
                                   {-100.0000000000001, "MM10"},
                                   {0, "MM11"},
                                   {100.0000000000001, "MM12"},
                                   {4321.000000000001, "MM13"},
                                   {54321.00000000001, "MM14"},
                                   {654321.0000000001, "MM15"},
                                   {7654321.000000001, "MM16"},
                                   {87654321.00000001, "MM17"},
                                   {987654321.0000001, "MM18"}}));
}

// ZRevrange
TEST_F(ZSetsTest, ZRevrangeTest) {
  int32_t ret;
  std::vector<storage::ScoreMember> score_members;

  // ***************** Group 1 Test *****************
  std::vector<storage::ScoreMember> gp1_sm{{0, "MM1"}};
  s = db.ZAdd("GP1_ZREVRANGE_KEY", gp1_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(1, ret);
  ASSERT_TRUE(size_match(&db, "GP1_ZREVRANGE_KEY", 1));
  ASSERT_TRUE(score_members_match(&db, "GP1_ZREVRANGE_KEY", {{0, "MM1"}}));

  s = db.ZRevrange("GP1_ZREVRANGE_KEY", 0, -1, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{0, "MM1"}}));

  s = db.ZRevrange("GP1_ZREVRANGE_KEY", 0, 0, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{0, "MM1"}}));

  s = db.ZRevrange("GP1_ZREVRANGE_KEY", -1, -1, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{0, "MM1"}}));

  // ***************** Group 2 Test *****************
  //
  // {0, MM0} {1, MM1} {2, MM2} {3, MM3} {4, MM4} {5, MM5} {6, MM6} {7, MM7} {8, MM8}
  //    8        7        6        5        4        3        2        1        0
  //   -1       -2       -3       -4       -5       -6       -7       -8       -9
  std::vector<storage::ScoreMember> gp2_sm{{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"},
                                           {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}};
  s = db.ZAdd("GP2_ZREVRANGE_KEY", gp2_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP2_ZREVRANGE_KEY", 9));
  ASSERT_TRUE(score_members_match(
      &db, "GP2_ZREVRANGE_KEY",
      {{0, "MM0"}, {1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}, {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  s = db.ZRevrange("GP2_ZREVRANGE_KEY", 0, 8, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(
      score_members,
      {{8, "MM8"}, {7, "MM7"}, {6, "MM6"}, {5, "MM5"}, {4, "MM4"}, {3, "MM3"}, {2, "MM2"}, {1, "MM1"}, {0, "MM0"}}));

  s = db.ZRevrange("GP2_ZREVRANGE_KEY", -9, -1, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(
      score_members,
      {{8, "MM8"}, {7, "MM7"}, {6, "MM6"}, {5, "MM5"}, {4, "MM4"}, {3, "MM3"}, {2, "MM2"}, {1, "MM1"}, {0, "MM0"}}));

  s = db.ZRevrange("GP2_ZREVRANGE_KEY", 0, -1, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(
      score_members,
      {{8, "MM8"}, {7, "MM7"}, {6, "MM6"}, {5, "MM5"}, {4, "MM4"}, {3, "MM3"}, {2, "MM2"}, {1, "MM1"}, {0, "MM0"}}));

  s = db.ZRevrange("GP2_ZREVRANGE_KEY", -9, 8, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(
      score_members,
      {{8, "MM8"}, {7, "MM7"}, {6, "MM6"}, {5, "MM5"}, {4, "MM4"}, {3, "MM3"}, {2, "MM2"}, {1, "MM1"}, {0, "MM0"}}));

  s = db.ZRevrange("GP2_ZREVRANGE_KEY", -100, 8, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(
      score_members,
      {{8, "MM8"}, {7, "MM7"}, {6, "MM6"}, {5, "MM5"}, {4, "MM4"}, {3, "MM3"}, {2, "MM2"}, {1, "MM1"}, {0, "MM0"}}));

  s = db.ZRevrange("GP2_ZREVRANGE_KEY", 0, 100, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(
      score_members,
      {{8, "MM8"}, {7, "MM7"}, {6, "MM6"}, {5, "MM5"}, {4, "MM4"}, {3, "MM3"}, {2, "MM2"}, {1, "MM1"}, {0, "MM0"}}));

  s = db.ZRevrange("GP2_ZREVRANGE_KEY", -100, 100, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(
      score_members,
      {{8, "MM8"}, {7, "MM7"}, {6, "MM6"}, {5, "MM5"}, {4, "MM4"}, {3, "MM3"}, {2, "MM2"}, {1, "MM1"}, {0, "MM0"}}));

  s = db.ZRevrange("GP2_ZREVRANGE_KEY", 0, 0, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{8, "MM8"}}));

  s = db.ZRevrange("GP2_ZREVRANGE_KEY", -9, -9, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{8, "MM8"}}));

  s = db.ZRevrange("GP2_ZREVRANGE_KEY", 8, 8, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{0, "MM0"}}));

  s = db.ZRevrange("GP2_ZREVRANGE_KEY", -1, -1, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{0, "MM0"}}));

  s = db.ZRevrange("GP2_ZREVRANGE_KEY", 0, 5, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(
      score_members_match(score_members, {{8, "MM8"}, {7, "MM7"}, {6, "MM6"}, {5, "MM5"}, {4, "MM4"}, {3, "MM3"}}));

  s = db.ZRevrange("GP2_ZREVRANGE_KEY", 0, -4, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(
      score_members_match(score_members, {{8, "MM8"}, {7, "MM7"}, {6, "MM6"}, {5, "MM5"}, {4, "MM4"}, {3, "MM3"}}));

  s = db.ZRevrange("GP2_ZREVRANGE_KEY", -9, -4, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(
      score_members_match(score_members, {{8, "MM8"}, {7, "MM7"}, {6, "MM6"}, {5, "MM5"}, {4, "MM4"}, {3, "MM3"}}));

  s = db.ZRevrange("GP2_ZREVRANGE_KEY", -9, 5, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(
      score_members_match(score_members, {{8, "MM8"}, {7, "MM7"}, {6, "MM6"}, {5, "MM5"}, {4, "MM4"}, {3, "MM3"}}));

  s = db.ZRevrange("GP2_ZREVRANGE_KEY", -100, 5, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(
      score_members_match(score_members, {{8, "MM8"}, {7, "MM7"}, {6, "MM6"}, {5, "MM5"}, {4, "MM4"}, {3, "MM3"}}));

  s = db.ZRevrange("GP2_ZREVRANGE_KEY", -100, -4, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(
      score_members_match(score_members, {{8, "MM8"}, {7, "MM7"}, {6, "MM6"}, {5, "MM5"}, {4, "MM4"}, {3, "MM3"}}));

  s = db.ZRevrange("GP2_ZREVRANGE_KEY", 3, 5, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{5, "MM5"}, {4, "MM4"}, {3, "MM3"}}));

  s = db.ZRevrange("GP2_ZREVRANGE_KEY", -6, -4, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{5, "MM5"}, {4, "MM4"}, {3, "MM3"}}));

  s = db.ZRevrange("GP2_ZREVRANGE_KEY", 3, -4, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{5, "MM5"}, {4, "MM4"}, {3, "MM3"}}));

  s = db.ZRevrange("GP2_ZREVRANGE_KEY", -6, 5, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{5, "MM5"}, {4, "MM4"}, {3, "MM3"}}));

  s = db.ZRevrange("GP2_ZREVRANGE_KEY", 3, 8, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(
      score_members_match(score_members, {{5, "MM5"}, {4, "MM4"}, {3, "MM3"}, {2, "MM2"}, {1, "MM1"}, {0, "MM0"}}));

  s = db.ZRevrange("GP2_ZREVRANGE_KEY", -6, -1, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(
      score_members_match(score_members, {{5, "MM5"}, {4, "MM4"}, {3, "MM3"}, {2, "MM2"}, {1, "MM1"}, {0, "MM0"}}));

  s = db.ZRevrange("GP2_ZREVRANGE_KEY", 3, -1, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(
      score_members_match(score_members, {{5, "MM5"}, {4, "MM4"}, {3, "MM3"}, {2, "MM2"}, {1, "MM1"}, {0, "MM0"}}));

  s = db.ZRevrange("GP2_ZREVRANGE_KEY", -6, 8, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(
      score_members_match(score_members, {{5, "MM5"}, {4, "MM4"}, {3, "MM3"}, {2, "MM2"}, {1, "MM1"}, {0, "MM0"}}));

  s = db.ZRevrange("GP2_ZREVRANGE_KEY", -6, 100, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(
      score_members_match(score_members, {{5, "MM5"}, {4, "MM4"}, {3, "MM3"}, {2, "MM2"}, {1, "MM1"}, {0, "MM0"}}));

  s = db.ZRevrange("GP2_ZREVRANGE_KEY", 3, 100, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(
      score_members_match(score_members, {{5, "MM5"}, {4, "MM4"}, {3, "MM3"}, {2, "MM2"}, {1, "MM1"}, {0, "MM0"}}));

  // ***************** Group 3 Test *****************
  std::vector<storage::ScoreMember> gp3_sm1{{0, "MM0"}, {1, "MM1"}, {2, "MM2"}};
  std::vector<storage::ScoreMember> gp3_sm2{{0, "MM0"}, {1, "MM1"}, {2, "MM2"}};
  std::vector<storage::ScoreMember> gp3_sm3{{0, "MM0"}, {1, "MM1"}, {2, "MM2"}};
  s = db.ZAdd("GP3_ZREVRANGE_KEY1", gp3_sm1, &ret);
  ASSERT_TRUE(s.ok());
  s = db.ZAdd("GP3_ZREVRANGE_KEY2", gp3_sm2, &ret);
  ASSERT_TRUE(s.ok());
  s = db.ZAdd("GP3_ZREVRANGE_KEY3", gp3_sm3, &ret);
  ASSERT_TRUE(s.ok());

  s = db.ZRevrange("GP3_ZREVRANGE_KEY2", 0, -1, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{2, "MM2"}, {1, "MM1"}, {0, "MM0"}}));

  s = db.ZRevrange("GP3_ZREVRANGE_KEY2", 0, 0, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{2, "MM2"}}));

  s = db.ZRevrange("GP3_ZREVRANGE_KEY2", -1, -1, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{0, "MM0"}}));

  s = db.ZRevrange("GP3_ZREVRANGE_KEY2", 1, 1, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{1, "MM1"}}));

  // ***************** Group 4 Test *****************
  std::vector<storage::ScoreMember> gp4_sm{{0, "MM1"}};
  s = db.ZAdd("GP4_ZREVRANGE_KEY", gp4_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(1, ret);
  ASSERT_TRUE(size_match(&db, "GP4_ZREVRANGE_KEY", 1));
  ASSERT_TRUE(score_members_match(&db, "GP4_ZREVRANGE_KEY", {{0, "MM1"}}));
  ASSERT_TRUE(make_expired(&db, "GP4_ZREVRANGE_KEY"));

  s = db.ZRevrange("GP4_ZREVRANGE_KEY", 0, -1, &score_members);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_TRUE(score_members_match(score_members, {}));

  // ***************** Group 5 Test *****************
  s = db.ZRevrange("GP5_ZREVRANGE_KEY", 0, -1, &score_members);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_TRUE(score_members_match(score_members, {}));
}

// TODO(@tangruilin): 修复测试代码
// ZRevrangebyscore
// TEST_F(ZSetsTest, ZRevrangebyscoreTest) {
//   int32_t ret;
//   std::vector<storage::ScoreMember> score_members;

//   // ***************** Group 1 Test *****************
//   std::vector<storage::ScoreMember> gp1_sm {{-987654321.0000001, "MM1" }, {-87654321.00000001, "MM2" },
//   {-7654321.000000001, "MM3" },
//                                                {-654321.0000000001, "MM4" }, {-54321.00000000001, "MM5" },
//                                                {-4321.000000000001, "MM6" },
//                                                {-1000.000000000001, "MM7" }, {-1000.000000000001, "MM8" },
//                                                {-1000.000000000001, "MM9" },
//                                                {-100.0000000000001, "MM10"}, { 0,                 "MM11"}, {
//                                                100.0000000000001, "MM12"}, { 4321.000000000001, "MM13"}, {
//                                                54321.00000000001, "MM14"}, { 654321.0000000001, "MM15"}, {
//                                                7654321.000000001, "MM16"}, { 87654321.00000001, "MM17"}, {
//                                                987654321.0000001, "MM18"}};

//   s = db.ZAdd("GP1_ZREVRANGEBYSCORE_KEY", gp1_sm, &ret);
//   ASSERT_TRUE(s.ok());
//   ASSERT_EQ(18, ret);

//   s = db.ZRevrangebyscore("GP1_ZREVRANGEBYSCORE_KEY", std::numeric_limits<double>::lowest(),
//   std::numeric_limits<double>::max(), true, true, &score_members); ASSERT_TRUE(s.ok());
//   ASSERT_TRUE(score_members_match(score_members, {{ 987654321.0000001, "MM18"}, { 87654321.00000001, "MM17"}, {
//   7654321.000000001, "MM16"},
//                                                   { 654321.0000000001, "MM15"}, { 54321.00000000001, "MM14"}, {
//                                                   4321.000000000001, "MM13"}, { 100.0000000000001, "MM12"}, { 0,
//                                                   "MM11"}, {-100.0000000000001, "MM10"},
//                                                   {-1000.000000000001, "MM9" }, {-1000.000000000001, "MM8" },
//                                                   {-1000.000000000001, "MM7" },
//                                                   {-4321.000000000001, "MM6" }, {-54321.00000000001, "MM5" },
//                                                   {-654321.0000000001, "MM4" },
//                                                   {-7654321.000000001, "MM3" }, {-87654321.00000001, "MM2" },
//                                                   {-987654321.0000001, "MM1" }}));

//   // count = max offset = 0
//   s = db.ZRevrangebyscore("GP1_ZRANGEBYSCORE_KEY", std::numeric_limits<double>::lowest(),
//   std::numeric_limits<double>::max(), true, true, std::numeric_limits<int64_t>::max(), 0, &score_members);
//   ASSERT_TRUE(s.ok());
//   ASSERT_TRUE(score_members_match(score_members, {{ 987654321.0000001, "MM18"}, { 87654321.00000001, "MM17"}, {
//   7654321.000000001, "MM16"},
//                                                   { 654321.0000000001, "MM15"}, { 54321.00000000001, "MM14"}, {
//                                                   4321.000000000001, "MM13"}, { 100.0000000000001, "MM12"}, { 0,
//                                                   "MM11"}, {-100.0000000000001, "MM10"},
//                                                   {-1000.000000000001, "MM9" }, {-1000.000000000001, "MM8" },
//                                                   {-1000.000000000001, "MM7" },
//                                                   {-4321.000000000001, "MM6" }, {-54321.00000000001, "MM5" },
//                                                   {-654321.0000000001, "MM4" },
//                                                   {-7654321.000000001, "MM3" }, {-87654321.00000001, "MM2" },
//                                                   {-987654321.0000001, "MM1" }}));

//   // count = 18 offset = 0
//   s = db.ZRevrangebyscore("GP1_ZRANGEBYSCORE_KEY", std::numeric_limits<double>::lowest(),
//   std::numeric_limits<double>::max(), true, true, 18, 0, &score_members); ASSERT_TRUE(s.ok());
//   ASSERT_TRUE(score_members_match(score_members, {{ 987654321.0000001, "MM18"}, { 87654321.00000001, "MM17"}, {
//   7654321.000000001, "MM16"},
//                                                   { 654321.0000000001, "MM15"}, { 54321.00000000001, "MM14"}, {
//                                                   4321.000000000001, "MM13"}, { 100.0000000000001, "MM12"}, { 0,
//                                                   "MM11"}, {-100.0000000000001, "MM10"},
//                                                   {-1000.000000000001, "MM9" }, {-1000.000000000001, "MM8" },
//                                                   {-1000.000000000001, "MM7" },
//                                                   {-4321.000000000001, "MM6" }, {-54321.00000000001, "MM5" },
//                                                   {-654321.0000000001, "MM4" },
//                                                   {-7654321.000000001, "MM3" }, {-87654321.00000001, "MM2" },
//                                                   {-987654321.0000001, "MM1" }}));

//   // count = 10 offset = 0
//   s = db.ZRevrangebyscore("GP1_ZRANGEBYSCORE_KEY", std::numeric_limits<double>::lowest(),
//   std::numeric_limits<double>::max(), true, true, 10, 0, &score_members); ASSERT_TRUE(s.ok());
//   ASSERT_TRUE(score_members_match(score_members, {{ 987654321.0000001, "MM18"}, { 87654321.00000001, "MM17"}, {
//   7654321.000000001, "MM16"},
//                                                   { 654321.0000000001, "MM15"}, { 54321.00000000001, "MM14"}, {
//                                                   4321.000000000001, "MM13"}, { 100.0000000000001, "MM12"}, { 0,
//                                                   "MM11"}, {-100.0000000000001, "MM10"},
//                                                   {-1000.000000000001, "MM9" }}));
//   // count = 10 offset = 1
//   s = db.ZRevrangebyscore("GP1_ZRANGEBYSCORE_KEY", std::numeric_limits<double>::lowest(),
//   std::numeric_limits<double>::max(), true, true, 10, 1, &score_members); ASSERT_TRUE(s.ok());
//   ASSERT_TRUE(score_members_match(score_members, {                              { 87654321.00000001, "MM17"}, {
//   7654321.000000001, "MM16"},
//                                                   { 654321.0000000001, "MM15"}, { 54321.00000000001, "MM14"}, {
//                                                   4321.000000000001, "MM13"}, { 100.0000000000001, "MM12"}, { 0,
//                                                   "MM11"}, {-100.0000000000001, "MM10"},
//                                                   {-1000.000000000001, "MM9" }, {-1000.000000000001, "MM8" }}));

//   // count = 10 offset = 2
//   s = db.ZRevrangebyscore("GP1_ZRANGEBYSCORE_KEY", std::numeric_limits<double>::lowest(),
//   std::numeric_limits<double>::max(), true, true, 10, 2, &score_members); ASSERT_TRUE(s.ok());
//   ASSERT_TRUE(score_members_match(score_members, {                                                            {
//   7654321.000000001, "MM16"},
//                                                   { 654321.0000000001, "MM15"}, { 54321.00000000001, "MM14"}, {
//                                                   4321.000000000001, "MM13"}, { 100.0000000000001, "MM12"}, { 0,
//                                                   "MM11"}, {-100.0000000000001, "MM10"},
//                                                   {-1000.000000000001, "MM9" }, {-1000.000000000001, "MM8" },
//                                                   {-1000.000000000001, "MM7" }}));

//   // count = 10 offset = 17
//   s = db.ZRevrangebyscore("GP1_ZRANGEBYSCORE_KEY", std::numeric_limits<double>::lowest(),
//   std::numeric_limits<double>::max(), true, true, 10, 17, &score_members); ASSERT_TRUE(s.ok());
//   ASSERT_TRUE(score_members_match(score_members, {{-987654321.0000001, "MM1" }}));

//   // count = 10 offset = 18
//   s = db.ZRevrangebyscore("GP1_ZRANGEBYSCORE_KEY", std::numeric_limits<double>::lowest(),
//   std::numeric_limits<double>::max(), true, true, 10, 18, &score_members); ASSERT_TRUE(s.ok());
//   ASSERT_TRUE(score_members_match(score_members, {}));

//   // count = 10 offset = 19
//   s = db.ZRevrangebyscore("GP1_ZRANGEBYSCORE_KEY", std::numeric_limits<double>::lowest(),
//   std::numeric_limits<double>::max(), true, true, 10, 19, &score_members); ASSERT_TRUE(s.ok());
//   ASSERT_TRUE(score_members_match(score_members, {}));

//   // count = 10000 offset = 1
//   s = db.ZRevrangebyscore("GP1_ZRANGEBYSCORE_KEY", std::numeric_limits<double>::lowest(),
//   std::numeric_limits<double>::max(), true, true, 10000, 1, &score_members); ASSERT_TRUE(s.ok());
//   ASSERT_TRUE(score_members_match(score_members, {                              { 87654321.00000001, "MM17"}, {
//   7654321.000000001, "MM16"},
//                                                   { 654321.0000000001, "MM15"}, { 54321.00000000001, "MM14"}, {
//                                                   4321.000000000001, "MM13"}, { 100.0000000000001, "MM12"}, { 0,
//                                                   "MM11"}, {-100.0000000000001, "MM10"},
//                                                   {-1000.000000000001, "MM9" }, {-1000.000000000001, "MM8" },
//                                                   {-1000.000000000001, "MM7" },
//                                                   {-4321.000000000001, "MM6" }, {-54321.00000000001, "MM5" },
//                                                   {-654321.0000000001, "MM4" },
//                                                   {-7654321.000000001, "MM3" }, {-87654321.00000001, "MM2" },
//                                                   {-987654321.0000001, "MM1" }}));

//   // count = 10000 offset = 10000
//   s = db.ZRevrangebyscore("GP1_ZRANGEBYSCORE_KEY", std::numeric_limits<double>::lowest(),
//   std::numeric_limits<double>::max(), true, true, 10000, 10000, &score_members); ASSERT_TRUE(s.ok());
//   ASSERT_TRUE(score_members_match(score_members, {}));

//   s = db.ZRevrangebyscore("GP1_ZREVRANGEBYSCORE_KEY", std::numeric_limits<double>::lowest(), -1000.000000000001,
//   true, true, &score_members); ASSERT_TRUE(s.ok()); ASSERT_TRUE(score_members_match(score_members,
//   {{-1000.000000000001, "MM9" }, {-1000.000000000001, "MM8" }, {-1000.000000000001, "MM7" },
//                                                   {-4321.000000000001, "MM6" }, {-54321.00000000001, "MM5" },
//                                                   {-654321.0000000001, "MM4" },
//                                                   {-7654321.000000001, "MM3" }, {-87654321.00000001, "MM2" },
//                                                   {-987654321.0000001, "MM1" }}));

//   s = db.ZRevrangebyscore("GP1_ZREVRANGEBYSCORE_KEY", std::numeric_limits<double>::lowest(), -1000.000000000001,
//   true, false, &score_members); ASSERT_TRUE(s.ok()); ASSERT_TRUE(score_members_match(score_members,
//   {{-4321.000000000001, "MM6" }, {-54321.00000000001, "MM5" }, {-654321.0000000001, "MM4" },
//                                                   {-7654321.000000001, "MM3" }, {-87654321.00000001, "MM2" },
//                                                   {-987654321.0000001, "MM1" }}));

//   s = db.ZRevrangebyscore("GP1_ZREVRANGEBYSCORE_KEY", -1000.000000000001, std::numeric_limits<double>::max(), true,
//   true, &score_members); ASSERT_TRUE(s.ok()); ASSERT_TRUE(score_members_match(score_members, {{ 987654321.0000001,
//   "MM18"}, { 87654321.00000001, "MM17"}, { 7654321.000000001, "MM16"},
//                                                   { 654321.0000000001, "MM15"}, { 54321.00000000001, "MM14"}, {
//                                                   4321.000000000001, "MM13"}, { 100.0000000000001, "MM12"}, { 0,
//                                                   "MM11"}, {-100.0000000000001, "MM10"},
//                                                   {-1000.000000000001, "MM9" }, {-1000.000000000001, "MM8" },
//                                                   {-1000.000000000001, "MM7" }}));

//   s = db.ZRevrangebyscore("GP1_ZREVRANGEBYSCORE_KEY", -1000.000000000001, std::numeric_limits<double>::max(), false,
//   true, &score_members); ASSERT_TRUE(s.ok()); ASSERT_TRUE(score_members_match(score_members, {{ 987654321.0000001,
//   "MM18"}, { 87654321.00000001, "MM17"}, { 7654321.000000001, "MM16"},
//                                                   { 654321.0000000001, "MM15"}, { 54321.00000000001, "MM14"}, {
//                                                   4321.000000000001, "MM13"}, { 100.0000000000001, "MM12"}, { 0,
//                                                   "MM11"}, {-100.0000000000001, "MM10"}}));

//   s = db.ZRevrangebyscore("GP1_ZREVRANGEBYSCORE_KEY", -987654321.0000001, 987654321.0000001, true, true,
//   &score_members); ASSERT_TRUE(s.ok()); ASSERT_TRUE(score_members_match(score_members, {{ 987654321.0000001, "MM18"},
//   { 87654321.00000001, "MM17"}, { 7654321.000000001, "MM16"},
//                                                   { 654321.0000000001, "MM15"}, { 54321.00000000001, "MM14"}, {
//                                                   4321.000000000001, "MM13"}, { 100.0000000000001, "MM12"}, { 0,
//                                                   "MM11"}, {-100.0000000000001, "MM10"},
//                                                   {-1000.000000000001, "MM9" }, {-1000.000000000001, "MM8" },
//                                                   {-1000.000000000001, "MM7" },
//                                                   {-4321.000000000001, "MM6" }, {-54321.00000000001, "MM5" },
//                                                   {-654321.0000000001, "MM4" },
//                                                   {-7654321.000000001, "MM3" }, {-87654321.00000001, "MM2" },
//                                                   {-987654321.0000001, "MM1" }}));

//   s = db.ZRevrangebyscore("GP1_ZREVRANGEBYSCORE_KEY", -987654321.0000001, 987654321.0000001, false, false,
//   &score_members); ASSERT_TRUE(s.ok()); ASSERT_TRUE(score_members_match(score_members, { { 87654321.00000001,
//   "MM17"}, { 7654321.000000001, "MM16"},
//                                                   { 654321.0000000001, "MM15"}, { 54321.00000000001, "MM14"}, {
//                                                   4321.000000000001, "MM13"}, { 100.0000000000001, "MM12"}, { 0,
//                                                   "MM11"}, {-100.0000000000001, "MM10"},
//                                                   {-1000.000000000001, "MM9" }, {-1000.000000000001, "MM8" },
//                                                   {-1000.000000000001, "MM7" },
//                                                   {-4321.000000000001, "MM6" }, {-54321.00000000001, "MM5" },
//                                                   {-654321.0000000001, "MM4" },
//                                                   {-7654321.000000001, "MM3" }, {-87654321.00000001, "MM2" }, }));

//   s = db.ZRevrangebyscore("GP1_ZREVRANGEBYSCORE_KEY", -999999999, -1000.000000000001 , true, true, &score_members);
//   ASSERT_TRUE(s.ok());
//   ASSERT_TRUE(score_members_match(score_members, {{-1000.000000000001, "MM9" }, {-1000.000000000001, "MM8" },
//   {-1000.000000000001, "MM7" },
//                                                   {-4321.000000000001, "MM6" }, {-54321.00000000001, "MM5" },
//                                                   {-654321.0000000001, "MM4" },
//                                                   {-7654321.000000001, "MM3" }, {-87654321.00000001, "MM2" },
//                                                   {-987654321.0000001, "MM1" }}));

//   s = db.ZRevrangebyscore("GP1_ZREVRANGEBYSCORE_KEY", -999999999, -1000.000000000001 , true, false, &score_members);
//   ASSERT_TRUE(s.ok());
//   ASSERT_TRUE(score_members_match(score_members, {{-4321.000000000001, "MM6" }, {-54321.00000000001, "MM5" },
//   {-654321.0000000001, "MM4" },
//                                                   {-7654321.000000001, "MM3" }, {-87654321.00000001, "MM2" },
//                                                   {-987654321.0000001, "MM1" }}));

//   s = db.ZRevrangebyscore("GP1_ZREVRANGEBYSCORE_KEY", -654321.0000000001, -4321.000000000001, true, true,
//   &score_members); ASSERT_TRUE(s.ok()); ASSERT_TRUE(score_members_match(score_members, {{-4321.000000000001, "MM6" },
//   {-54321.00000000001, "MM5" }, {-654321.0000000001, "MM4" }}));

//   s = db.ZRevrangebyscore("GP1_ZREVRANGEBYSCORE_KEY", -654321.0000000001, -4321.000000000001, false, false,
//   &score_members); ASSERT_TRUE(s.ok()); ASSERT_TRUE(score_members_match(score_members, {{-54321.00000000001, "MM5"
//   }}));

//   s = db.ZRevrangebyscore("GP1_ZREVRANGEBYSCORE_KEY", 0, 0, true, true, &score_members);
//   ASSERT_TRUE(s.ok());
//   ASSERT_TRUE(score_members_match(score_members, {{0, "MM11"}}));

//   s = db.ZRevrangebyscore("GP1_ZREVRANGEBYSCORE_KEY", 0, 0, false, true, &score_members);
//   ASSERT_TRUE(s.ok());
//   ASSERT_TRUE(score_members_match(score_members, {}));

//   s = db.ZRevrangebyscore("GP1_ZREVRANGEBYSCORE_KEY", 4321.000000000001, 987654321.0000001, true, true,
//   &score_members); ASSERT_TRUE(s.ok()); ASSERT_TRUE(score_members_match(score_members, {{ 987654321.0000001, "MM18"},
//   { 87654321.00000001, "MM17"}, { 7654321.000000001, "MM16"},
//                                                   { 654321.0000000001, "MM15"}, { 54321.00000000001, "MM14"}, {
//                                                   4321.000000000001, "MM13"}}));

//   s = db.ZRevrangebyscore("GP1_ZREVRANGEBYSCORE_KEY", 4321.000000000001, 987654321.0000001, false, true,
//   &score_members); ASSERT_TRUE(s.ok()); ASSERT_TRUE(score_members_match(score_members, {{ 987654321.0000001, "MM18"},
//   { 87654321.00000001, "MM17"}, { 7654321.000000001, "MM16"},
//                                                   { 654321.0000000001, "MM15"}, { 54321.00000000001, "MM14"}, }));

//   s = db.ZRevrangebyscore("GP1_ZREVRANGEBYSCORE_KEY", 4321.000000000001, 987654321.0000001, false, false,
//   &score_members); ASSERT_TRUE(s.ok()); ASSERT_TRUE(score_members_match(score_members, { { 87654321.00000001,
//   "MM17"}, { 7654321.000000001, "MM16"},
//                                                   { 654321.0000000001, "MM15"}, { 54321.00000000001, "MM14"}, }));

//   s = db.ZRevrangebyscore("GP1_ZREVRANGEBYSCORE_KEY", -1000.000000000001, 987654321.0000001, true, true,
//   &score_members); ASSERT_TRUE(s.ok()); ASSERT_TRUE(score_members_match(score_members, {{ 987654321.0000001, "MM18"},
//   { 87654321.00000001, "MM17"}, { 7654321.000000001, "MM16"},
//                                                   { 654321.0000000001, "MM15"}, { 54321.00000000001, "MM14"}, {
//                                                   4321.000000000001, "MM13"}, { 100.0000000000001, "MM12"}, { 0,
//                                                   "MM11"}, {-100.0000000000001, "MM10"},
//                                                   {-1000.000000000001, "MM9" }, {-1000.000000000001, "MM8" },
//                                                   {-1000.000000000001, "MM7" }}));

//   s = db.ZRevrangebyscore("GP1_ZREVRANGEBYSCORE_KEY", -1000.000000000001, 987654321.0000001, false, true,
//   &score_members); ASSERT_TRUE(s.ok()); ASSERT_TRUE(score_members_match(score_members, {{ 987654321.0000001, "MM18"},
//   { 87654321.00000001, "MM17"}, { 7654321.000000001, "MM16"},
//                                                   { 654321.0000000001, "MM15"}, { 54321.00000000001, "MM14"}, {
//                                                   4321.000000000001, "MM13"}, { 100.0000000000001, "MM12"}, { 0,
//                                                   "MM11"}, {-100.0000000000001, "MM10"}}));

//   s = db.ZRevrangebyscore("GP1_ZREVRANGEBYSCORE_KEY", 999999999, std::numeric_limits<double>::max(), true, true,
//   &score_members); ASSERT_TRUE(s.ok()); ASSERT_TRUE(score_members_match(score_members, {}));

//   // ***************** Group 2 Test *****************
//   std::vector<storage::ScoreMember> gp2_sm {{-5, "MM0"}, {-3, "MM1"}, {-1, "MM2"},
//                                                {0,  "MM3"}, {1,  "MM4"}, {3,  "MM5"},
//                                                {5, "MM6"}};
//   s = db.ZAdd("GP2_ZREVRANGEBYSCORE_KEY", gp2_sm, &ret);
//   ASSERT_TRUE(s.ok());
//   ASSERT_EQ(7, ret);
//   ASSERT_TRUE(make_expired(&db, "GP2_ZREVRANGEBYSCORE_KEY"));
//   s = db.ZRevrangebyscore("GP2_ZREVRANGEBYSCORE_KEY", std::numeric_limits<double>::lowest(),
//   std::numeric_limits<double>::max(), true, true, &score_members); ASSERT_TRUE(s.IsNotFound());
//   ASSERT_TRUE(score_members_match(score_members, {}));

//   // ***************** Group 3 Test *****************
//   s = db.ZRevrangebyscore("GP3_ZREVRANGEBYSCORE_KEY", std::numeric_limits<double>::lowest(),
//   std::numeric_limits<double>::max(), true, true, &score_members); ASSERT_TRUE(s.IsNotFound());
//   ASSERT_TRUE(score_members_match(score_members, {}));

//   // ***************** Group 4 Test *****************
//   std::vector<storage::ScoreMember> gp4_sm {{-1000000000.0000000001, "MM0"},
//                                                {0, "MM1"},
//                                                { 1000000000.0000000001, "MM2"}};
//   s = db.ZAdd("GP4_ZREVRANGEBYSCORE_KEY", gp4_sm, &ret);
//   ASSERT_TRUE(s.ok());
//   ASSERT_EQ(3, ret);

//   s = db.ZRevrangebyscore("GP4_ZREVRANGEBYSCORE_KEY", -1000000000.0000000001, 1000000000.0000000001, true, true,
//   &score_members); ASSERT_TRUE(s.ok()); ASSERT_TRUE(score_members_match(score_members, {{1000000000.0000000001,
//   "MM2"}, {0, "MM1"}, {-1000000000.0000000001, "MM0"}}));

//   s = db.ZRevrangebyscore("GP4_ZREVRANGEBYSCORE_KEY", -1000000000.0000000001, 1000000000.0000000001, false, false,
//   &score_members); ASSERT_TRUE(s.ok()); ASSERT_TRUE(score_members_match(score_members, {{0, "MM1"}}));

//   s = db.ZRevrangebyscore("GP4_ZREVRANGEBYSCORE_KEY", -1000000000.0000000001, 1000000000.0000000001, true, false,
//   &score_members); ASSERT_TRUE(s.ok()); ASSERT_TRUE(score_members_match(score_members, {{0, "MM1"},
//   {-1000000000.0000000001, "MM0"}}));

//   s = db.ZRevrangebyscore("GP4_ZREVRANGEBYSCORE_KEY", -1000000000.0000000001, 1000000000.0000000001, false, true,
//   &score_members); ASSERT_TRUE(s.ok()); ASSERT_TRUE(score_members_match(score_members, {{1000000000.0000000001,
//   "MM2"}, {0, "MM1"}}));
// }

// ZRevrank
TEST_F(ZSetsTest, ZRevrankTest) {
  int32_t ret, rank;

  // ***************** Group 1 Test *****************
  // {-5, MM0} {-3, MM1} {-1, MM2} {0, MM3} {1, MM4} {3, MM5} {5, MM6}
  //     6         5         4        3        2        1        0
  std::vector<storage::ScoreMember> gp1_sm{{-5, "MM0"}, {-3, "MM1"}, {-1, "MM2"}, {0, "MM3"},
                                           {1, "MM4"},  {3, "MM5"},  {5, "MM6"}};
  s = db.ZAdd("GP1_ZREVRANK_KEY", gp1_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(7, ret);

  s = db.ZRevrank("GP1_ZREVRANK_KEY", "MM0", &rank);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(rank, 6);

  s = db.ZRevrank("GP1_ZREVRANK_KEY", "MM2", &rank);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(rank, 4);

  s = db.ZRevrank("GP1_ZREVRANK_KEY", "MM4", &rank);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(rank, 2);

  s = db.ZRevrank("GP1_ZREVRANK_KEY", "MM6", &rank);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(rank, 0);

  s = db.ZRevrank("GP1_ZREVRANK_KEY", "MM", &rank);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(rank, -1);

  // ***************** Group 2 Test *****************
  std::vector<storage::ScoreMember> gp2_sm{{-5, "MM0"}, {-3, "MM1"}, {-1, "MM2"}, {0, "MM3"},
                                           {1, "MM4"},  {3, "MM5"},  {5, "MM6"}};
  s = db.ZAdd("GP2_ZREVRANK_KEY", gp2_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(7, ret);
  ASSERT_TRUE(make_expired(&db, "GP2_ZREVRANK_KEY"));

  s = db.ZRevrank("GP2_ZREVRANK_KEY", "MM0", &rank);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(-1, rank);

  // ***************** Group 3 Test *****************
  s = db.ZRevrank("GP3_ZREVRANK_KEY", "MM0", &rank);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(-1, rank);
}

// ZSCORE
TEST_F(ZSetsTest, ZScoreTest) {
  int32_t ret;
  double score;

  // ***************** Group 1 Test *****************
  std::vector<storage::ScoreMember> gp1_sm{{54354.497895352, "MM1"}, {100.987654321, "MM2"},  {-100.000000001, "MM3"},
                                           {-100.000000002, "MM4"},  {-100.000000001, "MM5"}, {-100.000000002, "MM6"}};
  s = db.ZAdd("GP1_ZSCORE_KEY", gp1_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(6, ret);
  ASSERT_TRUE(size_match(&db, "GP1_ZSCORE_KEY", 6));
  ASSERT_TRUE(score_members_match(&db, "GP1_ZSCORE_KEY",
                                  {{-100.000000002, "MM4"},
                                   {-100.000000002, "MM6"},
                                   {-100.000000001, "MM3"},
                                   {-100.000000001, "MM5"},
                                   {100.987654321, "MM2"},
                                   {54354.497895352, "MM1"}}));
  s = db.ZScore("GP1_ZSCORE_KEY", "MM1", &score);
  ASSERT_TRUE(s.ok());
  ASSERT_DOUBLE_EQ(54354.497895352, score);

  s = db.ZScore("GP1_ZSCORE_KEY", "MM2", &score);
  ASSERT_TRUE(s.ok());
  ASSERT_DOUBLE_EQ(100.987654321, score);

  s = db.ZScore("GP1_ZSCORE_KEY", "MM3", &score);
  ASSERT_TRUE(s.ok());
  ASSERT_DOUBLE_EQ(-100.000000001, score);

  s = db.ZScore("GP1_ZSCORE_KEY", "MM4", &score);
  ASSERT_TRUE(s.ok());
  ASSERT_DOUBLE_EQ(-100.000000002, score);

  s = db.ZScore("GP1_ZSCORE_KEY", "MM5", &score);
  ASSERT_TRUE(s.ok());
  ASSERT_DOUBLE_EQ(-100.000000001, score);

  s = db.ZScore("GP1_ZSCORE_KEY", "MM6", &score);
  ASSERT_TRUE(s.ok());
  ASSERT_DOUBLE_EQ(-100.000000002, score);

  s = db.ZScore("GP1_ZSCORE_KEY", "MM7", &score);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_DOUBLE_EQ(0, score);

  // ***************** Group 2 Test *****************
  std::vector<storage::ScoreMember> gp2_sm{{4, "MM1"}, {3, "MM2"}, {2, "MM3"}, {1, "MM4"}};
  s = db.ZAdd("GP2_ZSCORE_KEY", gp2_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(4, ret);
  ASSERT_TRUE(size_match(&db, "GP2_ZSCORE_KEY", 4));
  ASSERT_TRUE(score_members_match(&db, "GP2_ZSCORE_KEY", {{1, "MM4"}, {2, "MM3"}, {3, "MM2"}, {4, "MM1"}}));
  ASSERT_TRUE(make_expired(&db, "GP2_ZSCORE_KEY"));
  s = db.ZScore("GP2_ZSCORE_KEY", "MM1", &score);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_DOUBLE_EQ(0, score);

  // ***************** Group 3 Test *****************
  s = db.ZScore("GP3_ZSCORE_KEY", "MM1", &score);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_DOUBLE_EQ(0, score);
}

// ZUNIONSTORE
TEST_F(ZSetsTest, ZUnionstoreTest) {
  int32_t ret;

  // ***************** Group 1 Test *****************
  // {      1, MM1} {      10, MM2} {      100, MM3}  weight 1
  // {   1000, MM1} {   10000, MM2} {   100000, MM3}  weight 1
  // {1000000, MM1} {10000000, MM2} {100000000, MM3}  weight 1
  //
  // {1001001, MM1} {10010010, MM2} {100100100, MM3}
  //
  std::vector<storage::ScoreMember> gp1_sm1{{1, "MM1"}, {10, "MM2"}, {100, "MM3"}};
  std::vector<storage::ScoreMember> gp1_sm2{{1000, "MM1"}, {10000, "MM2"}, {100000, "MM3"}};
  std::vector<storage::ScoreMember> gp1_sm3{{1000000, "MM1"}, {10000000, "MM2"}, {100000000, "MM3"}};
  s = db.ZAdd("GP1_ZUNIONSTORE_SM1", gp1_sm1, &ret);
  s = db.ZAdd("GP1_ZUNIONSTORE_SM2", gp1_sm2, &ret);
  s = db.ZAdd("GP1_ZUNIONSTORE_SM3", gp1_sm3, &ret);
  s = db.ZUnionstore("GP1_ZUNIONSTORE_DESTINATION",
                     {"GP1_ZUNIONSTORE_SM1", "GP1_ZUNIONSTORE_SM2", "GP1_ZUNIONSTORE_SM3"}, {1, 1, 1}, storage::SUM,
                     &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  ASSERT_TRUE(size_match(&db, "GP1_ZUNIONSTORE_DESTINATION", 3));
  ASSERT_TRUE(score_members_match(&db, "GP1_ZUNIONSTORE_DESTINATION",
                                  {{1001001, "MM1"}, {10010010, "MM2"}, {100100100, "MM3"}}));

  // ***************** Group 2 Test *****************
  // {      1, MM1} {      10, MM2} {      100, MM3}  weight 1
  // {   1000, MM1} {   10000, MM2} {   100000, MM3}  weight 1
  // {1000000, MM1} {10000000, MM2} {100000000, MM3}  weight 1
  //
  // {      1, MM1} {      10, MM2} {      100, MM3}
  //
  std::vector<storage::ScoreMember> gp2_sm1{{1, "MM1"}, {10, "MM2"}, {100, "MM3"}};
  std::vector<storage::ScoreMember> gp2_sm2{{1000, "MM1"}, {10000, "MM2"}, {100000, "MM3"}};
  std::vector<storage::ScoreMember> gp2_sm3{{1000000, "MM1"}, {10000000, "MM2"}, {100000000, "MM3"}};
  s = db.ZAdd("GP2_ZUNIONSTORE_SM1", gp2_sm1, &ret);
  s = db.ZAdd("GP2_ZUNIONSTORE_SM2", gp2_sm2, &ret);
  s = db.ZAdd("GP2_ZUNIONSTORE_SM3", gp2_sm3, &ret);
  s = db.ZUnionstore("GP2_ZUNIONSTORE_DESTINATION",
                     {"GP2_ZUNIONSTORE_SM1", "GP2_ZUNIONSTORE_SM2", "GP2_ZUNIONSTORE_SM3"}, {1, 1, 1}, storage::MIN,
                     &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  ASSERT_TRUE(size_match(&db, "GP2_ZUNIONSTORE_DESTINATION", 3));
  ASSERT_TRUE(score_members_match(&db, "GP2_ZUNIONSTORE_DESTINATION", {{1, "MM1"}, {10, "MM2"}, {100, "MM3"}}));

  // ***************** Group 3 Test *****************
  // {      1, MM1} {      10, MM2} {      100, MM3}  weight 1
  // {   1000, MM1} {   10000, MM2} {   100000, MM3}  weight 1
  // {1000000, MM1} {10000000, MM2} {100000000, MM3}  weight 1
  //
  // {1000000, MM1} {10000000, MM2} {100000000, MM3}
  //
  std::vector<storage::ScoreMember> gp3_sm1{{1, "MM1"}, {10, "MM2"}, {100, "MM3"}};
  std::vector<storage::ScoreMember> gp3_sm2{{1000, "MM1"}, {10000, "MM2"}, {100000, "MM3"}};
  std::vector<storage::ScoreMember> gp3_sm3{{1000000, "MM1"}, {10000000, "MM2"}, {100000000, "MM3"}};
  s = db.ZAdd("GP3_ZUNIONSTORE_SM1", gp3_sm1, &ret);
  s = db.ZAdd("GP3_ZUNIONSTORE_SM2", gp3_sm2, &ret);
  s = db.ZAdd("GP3_ZUNIONSTORE_SM3", gp3_sm3, &ret);
  s = db.ZUnionstore("GP3_ZUNIONSTORE_DESTINATION",
                     {"GP3_ZUNIONSTORE_SM1", "GP3_ZUNIONSTORE_SM2", "GP3_ZUNIONSTORE_SM3"}, {1, 1, 1}, storage::MAX,
                     &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  ASSERT_TRUE(size_match(&db, "GP3_ZUNIONSTORE_DESTINATION", 3));
  ASSERT_TRUE(score_members_match(&db, "GP3_ZUNIONSTORE_DESTINATION",
                                  {{1000000, "MM1"}, {10000000, "MM2"}, {100000000, "MM3"}}));

  // ***************** Group 4 Test *****************
  // {      1, MM1} {      10, MM2} {      100, MM3}  weight 1
  // {   1000, MM1} {   10000, MM2} {   100000, MM3}  weight 2
  // {1000000, MM1} {10000000, MM2} {100000000, MM3}  weight 3
  //
  // {3002001, MM1} {30020010, MM2} {300200100, MM3}
  //
  std::vector<storage::ScoreMember> gp4_sm1{{1, "MM1"}, {10, "MM2"}, {100, "MM3"}};
  std::vector<storage::ScoreMember> gp4_sm2{{1000, "MM1"}, {10000, "MM2"}, {100000, "MM3"}};
  std::vector<storage::ScoreMember> gp4_sm3{{1000000, "MM1"}, {10000000, "MM2"}, {100000000, "MM3"}};
  s = db.ZAdd("GP4_ZUNIONSTORE_SM1", gp4_sm1, &ret);
  s = db.ZAdd("GP4_ZUNIONSTORE_SM2", gp4_sm2, &ret);
  s = db.ZAdd("GP4_ZUNIONSTORE_SM3", gp4_sm3, &ret);
  s = db.ZUnionstore("GP4_ZUNIONSTORE_DESTINATION",
                     {"GP4_ZUNIONSTORE_SM1", "GP4_ZUNIONSTORE_SM2", "GP4_ZUNIONSTORE_SM3"}, {1, 2, 3}, storage::SUM,
                     &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  ASSERT_TRUE(size_match(&db, "GP4_ZUNIONSTORE_DESTINATION", 3));
  ASSERT_TRUE(score_members_match(&db, "GP4_ZUNIONSTORE_DESTINATION",
                                  {{3002001, "MM1"}, {30020010, "MM2"}, {300200100, "MM3"}}));

  // ***************** Group 5 Test *****************
  // {      1, MM1} {      10, MM2} {      100, MM3}  weight 1
  // {   1000, MM1} {   10000, MM2} {   100000, MM3}  weight 2
  // {1000000, MM1}                 {100000000, MM3}  weight 3
  //
  // {3002001, MM1} {   20010, MM2} {300200100, MM3}
  //
  std::vector<storage::ScoreMember> gp5_sm1{{1, "MM1"}, {10, "MM2"}, {100, "MM3"}};
  std::vector<storage::ScoreMember> gp5_sm2{{1000, "MM1"}, {10000, "MM2"}, {100000, "MM3"}};
  std::vector<storage::ScoreMember> gp5_sm3{{1000000, "MM1"}, {100000000, "MM3"}};
  s = db.ZAdd("GP5_ZUNIONSTORE_SM1", gp5_sm1, &ret);
  s = db.ZAdd("GP5_ZUNIONSTORE_SM2", gp5_sm2, &ret);
  s = db.ZAdd("GP5_ZUNIONSTORE_SM3", gp5_sm3, &ret);
  s = db.ZUnionstore("GP5_ZUNIONSTORE_DESTINATION",
                     {"GP5_ZUNIONSTORE_SM1", "GP5_ZUNIONSTORE_SM2", "GP5_ZUNIONSTORE_SM3"}, {1, 2, 3}, storage::SUM,
                     &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  ASSERT_TRUE(size_match(&db, "GP5_ZUNIONSTORE_DESTINATION", 3));
  ASSERT_TRUE(
      score_members_match(&db, "GP5_ZUNIONSTORE_DESTINATION", {{20010, "MM2"}, {3002001, "MM1"}, {300200100, "MM3"}}));

  // ***************** Group 6 Test *****************
  // {      1, MM1} {      10, MM2} {      100, MM3}  weight 1
  // {   1000, MM1} {   10000, MM2} {   100000, MM3}  weight 2  (expire)
  // {1000000, MM1}                 {100000000, MM3}  weight 3
  //
  // {3000001, MM1} {      10, MM2} {300000100, MM3}
  //
  std::vector<storage::ScoreMember> gp6_sm1{{1, "MM1"}, {10, "MM2"}, {100, "MM3"}};
  std::vector<storage::ScoreMember> gp6_sm2{{1000, "MM1"}, {10000, "MM2"}, {100000, "MM3"}};
  std::vector<storage::ScoreMember> gp6_sm3{{1000000, "MM1"}, {100000000, "MM3"}};
  s = db.ZAdd("GP6_ZUNIONSTORE_SM1", gp6_sm1, &ret);
  s = db.ZAdd("GP6_ZUNIONSTORE_SM2", gp6_sm2, &ret);
  s = db.ZAdd("GP6_ZUNIONSTORE_SM3", gp6_sm3, &ret);
  ASSERT_TRUE(make_expired(&db, "GP6_ZUNIONSTORE_SM2"));
  s = db.ZUnionstore("GP6_ZUNIONSTORE_DESTINATION",
                     {"GP6_ZUNIONSTORE_SM1", "GP6_ZUNIONSTORE_SM2", "GP6_ZUNIONSTORE_SM3"}, {1, 2, 3}, storage::SUM,
                     &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  ASSERT_TRUE(size_match(&db, "GP6_ZUNIONSTORE_DESTINATION", 3));
  ASSERT_TRUE(
      score_members_match(&db, "GP6_ZUNIONSTORE_DESTINATION", {{10, "MM2"}, {3000001, "MM1"}, {300000100, "MM3"}}));

  // ***************** Group 7 Test *****************
  // {   1, MM1} {   10, MM2} {   100, MM3}             weight 1
  // {1000, MM1} {10000, MM2} {100000, MM3}             weight 2  (expire)
  //                                        {1000, MM4} weight 3
  //
  // {   1, MM1} {   10, MM2} {   100, MM3} {3000, MM4}
  //
  std::vector<storage::ScoreMember> gp7_sm1{{1, "MM1"}, {10, "MM2"}, {100, "MM3"}};
  std::vector<storage::ScoreMember> gp7_sm2{{1000, "MM1"}, {10000, "MM2"}, {100000, "MM3"}};
  std::vector<storage::ScoreMember> gp7_sm3{{1000, "MM4"}};
  s = db.ZAdd("GP7_ZUNIONSTORE_SM1", gp7_sm1, &ret);
  s = db.ZAdd("GP7_ZUNIONSTORE_SM2", gp7_sm2, &ret);
  s = db.ZAdd("GP7_ZUNIONSTORE_SM3", gp7_sm3, &ret);
  ASSERT_TRUE(make_expired(&db, "GP7_ZUNIONSTORE_SM2"));
  s = db.ZUnionstore("GP7_ZUNIONSTORE_DESTINATION",
                     {"GP7_ZUNIONSTORE_SM1", "GP7_ZUNIONSTORE_SM2", "GP7_ZUNIONSTORE_SM3"}, {1, 2, 3}, storage::SUM,
                     &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  ASSERT_TRUE(size_match(&db, "GP7_ZUNIONSTORE_DESTINATION", 4));
  ASSERT_TRUE(
      score_members_match(&db, "GP7_ZUNIONSTORE_DESTINATION", {{1, "MM1"}, {10, "MM2"}, {100, "MM3"}, {3000, "MM4"}}));

  // ***************** Group 8 Test *****************
  // {1, MM1}                        weight 1
  //            {1, MM2}             weight 1
  //                       {1, MM3}  weight 1
  //
  // {1, MM1}   {1, MM2}   {1, MM3}
  //
  std::vector<storage::ScoreMember> gp8_sm1{{1, "MM1"}};
  std::vector<storage::ScoreMember> gp8_sm2{{1, "MM2"}};
  std::vector<storage::ScoreMember> gp8_sm3{{1, "MM3"}};
  s = db.ZAdd("GP8_ZUNIONSTORE_SM1", gp8_sm1, &ret);
  s = db.ZAdd("GP8_ZUNIONSTORE_SM2", gp8_sm2, &ret);
  s = db.ZAdd("GP8_ZUNIONSTORE_SM3", gp8_sm3, &ret);
  s = db.ZUnionstore("GP8_ZUNIONSTORE_DESTINATION",
                     {"GP8_ZUNIONSTORE_SM1", "GP8_ZUNIONSTORE_SM2", "GP8_ZUNIONSTORE_SM3"}, {1, 1, 1}, storage::MIN,
                     &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  ASSERT_TRUE(size_match(&db, "GP8_ZUNIONSTORE_DESTINATION", 3));
  ASSERT_TRUE(score_members_match(&db, "GP8_ZUNIONSTORE_DESTINATION", {{1, "MM1"}, {1, "MM2"}, {1, "MM3"}}));

  // ***************** Group 9 Test *****************
  // {      1, MM1} {      10, MM2} {      100, MM3}  weight 1
  // {   1000, MM1} {   10000, MM2} {   100000, MM3}  weight 1
  // {1000000, MM1} {10000000, MM2} {100000000, MM3}  weight 1
  //
  // {1001001, MM1} {10010010, MM2} {100100100, MM3}
  //
  std::vector<storage::ScoreMember> gp9_sm1{{1, "MM1"}, {10, "MM2"}, {100, "MM3"}};
  std::vector<storage::ScoreMember> gp9_sm2{{1000, "MM1"}, {10000, "MM2"}, {100000, "MM3"}};
  std::vector<storage::ScoreMember> gp9_sm3{{1000000, "MM1"}, {10000000, "MM2"}, {100000000, "MM3"}};
  std::vector<storage::ScoreMember> gp9_destination{{1, "MM1"}};
  s = db.ZAdd("GP9_ZUNIONSTORE_SM1", gp9_sm1, &ret);
  s = db.ZAdd("GP9_ZUNIONSTORE_SM2", gp9_sm2, &ret);
  s = db.ZAdd("GP9_ZUNIONSTORE_SM3", gp9_sm3, &ret);
  s = db.ZAdd("GP9_ZUNIONSTORE_DESTINATION", gp9_destination, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  ASSERT_TRUE(size_match(&db, "GP9_ZUNIONSTORE_DESTINATION", 1));
  ASSERT_TRUE(score_members_match(&db, "GP9_ZUNIONSTORE_DESTINATION", {{1, "MM1"}}));

  s = db.ZUnionstore("GP9_ZUNIONSTORE_DESTINATION",
                     {"GP9_ZUNIONSTORE_SM1", "GP9_ZUNIONSTORE_SM2", "GP9_ZUNIONSTORE_SM3"}, {1, 1, 1}, storage::SUM,
                     &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  ASSERT_TRUE(size_match(&db, "GP9_ZUNIONSTORE_DESTINATION", 3));
  ASSERT_TRUE(score_members_match(&db, "GP9_ZUNIONSTORE_DESTINATION",
                                  {{1001001, "MM1"}, {10010010, "MM2"}, {100100100, "MM3"}}));

  // ***************** Group 10 Test *****************
  // {      1, MM1} {      10, MM2} {      100, MM3}  weight 1
  // {   1000, MM1} {   10000, MM2} {   100000, MM3}  weight 1
  // {1000000, MM1} {10000000, MM2} {100000000, MM3}  weight 1
  //
  // {1001001, MM1} {10010010, MM2} {100100100, MM3}
  //
  std::vector<storage::ScoreMember> gp10_sm1{{1, "MM1"}, {10, "MM2"}, {100, "MM3"}};
  std::vector<storage::ScoreMember> gp10_sm2{{1000, "MM1"}, {10000, "MM2"}, {100000, "MM3"}};
  std::vector<storage::ScoreMember> gp10_sm3{{1000000, "MM1"}, {10000000, "MM2"}, {100000000, "MM3"}};
  s = db.ZAdd("GP10_ZUNIONSTORE_SM1", gp10_sm1, &ret);
  s = db.ZAdd("GP10_ZUNIONSTORE_SM2", gp10_sm2, &ret);
  s = db.ZAdd("GP10_ZUNIONSTORE_SM3", gp10_sm3, &ret);
  s = db.ZUnionstore("GP10_ZUNIONSTORE_DESTINATION",
                     {"GP10_ZUNIONSTORE_SM1", "GP10_ZUNIONSTORE_SM2", "GP10_ZUNIONSTORE_SM3", "GP10_ZUNIONSTORE_SM4"},
                     {1, 1, 1, 1}, storage::SUM, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  ASSERT_TRUE(size_match(&db, "GP10_ZUNIONSTORE_DESTINATION", 3));
  ASSERT_TRUE(score_members_match(&db, "GP10_ZUNIONSTORE_DESTINATION",
                                  {{1001001, "MM1"}, {10010010, "MM2"}, {100100100, "MM3"}}));

  // ***************** Group 11 Test *****************
  // {-999999999, MM1}  weight 0
  //
  // {         0, MM1}
  //
  std::vector<storage::ScoreMember> gp11_sm1{{-999999999, "MM1"}};
  s = db.ZAdd("GP11_ZUNIONSTORE_SM1", gp11_sm1, &ret);
  s = db.ZUnionstore("GP11_ZUNIONSTORE_DESTINATION", {"GP11_ZUNIONSTORE_SM1"}, {0}, storage::SUM, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  ASSERT_TRUE(size_match(&db, "GP11_ZUNIONSTORE_DESTINATION", 1));
  ASSERT_TRUE(score_members_match(&db, "GP11_ZUNIONSTORE_DESTINATION", {{0, "MM1"}}));
}

// ZINTERSTORE
TEST_F(ZSetsTest, ZInterstoreTest) {
  int32_t ret;

  // ***************** Group 1 Test *****************
  // {      1, MM1} {      10, MM2} {      100, MM3}  weight 1
  // {   1000, MM1} {   10000, MM2} {   100000, MM3}  weight 1
  // {1000000, MM1} {10000000, MM2} {100000000, MM3}  weight 1
  //
  // {1001001, MM1} {10010010, MM2} {100100100, MM3}
  //
  std::vector<storage::ScoreMember> gp1_sm1{{1, "MM1"}, {10, "MM2"}, {100, "MM3"}};
  std::vector<storage::ScoreMember> gp1_sm2{{1000, "MM1"}, {10000, "MM2"}, {100000, "MM3"}};
  std::vector<storage::ScoreMember> gp1_sm3{{1000000, "MM1"}, {10000000, "MM2"}, {100000000, "MM3"}};
  s = db.ZAdd("GP1_ZINTERSTORE_SM1", gp1_sm1, &ret);
  s = db.ZAdd("GP1_ZINTERSTORE_SM2", gp1_sm2, &ret);
  s = db.ZAdd("GP1_ZINTERSTORE_SM3", gp1_sm3, &ret);
  s = db.ZInterstore("GP1_ZINTERSTORE_DESTINATION",
                     {"GP1_ZINTERSTORE_SM1", "GP1_ZINTERSTORE_SM2", "GP1_ZINTERSTORE_SM3"}, {1, 1, 1}, storage::SUM,
                     &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  ASSERT_TRUE(size_match(&db, "GP1_ZINTERSTORE_DESTINATION", 3));
  ASSERT_TRUE(score_members_match(&db, "GP1_ZINTERSTORE_DESTINATION",
                                  {{1001001, "MM1"}, {10010010, "MM2"}, {100100100, "MM3"}}));

  // ***************** Group 2 Test *****************
  // {      1, MM1} {      10, MM2} {      100, MM3}  weight 1
  // {   1000, MM1} {   10000, MM2} {   100000, MM3}  weight 1
  // {1000000, MM1} {10000000, MM2} {100000000, MM3}  weight 1
  //
  // {      1, MM1} {      10, MM2} {      100, MM3}
  //
  std::vector<storage::ScoreMember> gp2_sm1{{1, "MM1"}, {10, "MM2"}, {100, "MM3"}};
  std::vector<storage::ScoreMember> gp2_sm2{{1000, "MM1"}, {10000, "MM2"}, {100000, "MM3"}};
  std::vector<storage::ScoreMember> gp2_sm3{{1000000, "MM1"}, {10000000, "MM2"}, {100000000, "MM3"}};
  s = db.ZAdd("GP2_ZINTERSTORE_SM1", gp2_sm1, &ret);
  s = db.ZAdd("GP2_ZINTERSTORE_SM2", gp2_sm2, &ret);
  s = db.ZAdd("GP2_ZINTERSTORE_SM3", gp2_sm3, &ret);
  s = db.ZInterstore("GP2_ZINTERSTORE_DESTINATION",
                     {"GP2_ZINTERSTORE_SM1", "GP2_ZINTERSTORE_SM2", "GP2_ZINTERSTORE_SM3"}, {1, 1, 1}, storage::MIN,
                     &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  ASSERT_TRUE(size_match(&db, "GP2_ZINTERSTORE_DESTINATION", 3));
  ASSERT_TRUE(score_members_match(&db, "GP2_ZINTERSTORE_DESTINATION", {{1, "MM1"}, {10, "MM2"}, {100, "MM3"}}));

  // ***************** Group 3 Test *****************
  // {      1, MM1} {      10, MM2} {      100, MM3}  weight 1
  // {   1000, MM1} {   10000, MM2} {   100000, MM3}  weight 1
  // {1000000, MM1} {10000000, MM2} {100000000, MM3}  weight 1
  //
  // {1000000, MM1} {10000000, MM2} {100000000, MM3}
  //
  std::vector<storage::ScoreMember> gp3_sm1{{1, "MM1"}, {10, "MM2"}, {100, "MM3"}};
  std::vector<storage::ScoreMember> gp3_sm2{{1000, "MM1"}, {10000, "MM2"}, {100000, "MM3"}};
  std::vector<storage::ScoreMember> gp3_sm3{{1000000, "MM1"}, {10000000, "MM2"}, {100000000, "MM3"}};
  s = db.ZAdd("GP3_ZINTERSTORE_SM1", gp3_sm1, &ret);
  s = db.ZAdd("GP3_ZINTERSTORE_SM2", gp3_sm2, &ret);
  s = db.ZAdd("GP3_ZINTERSTORE_SM3", gp3_sm3, &ret);
  s = db.ZInterstore("GP3_ZINTERSTORE_DESTINATION",
                     {"GP3_ZINTERSTORE_SM1", "GP3_ZINTERSTORE_SM2", "GP3_ZINTERSTORE_SM3"}, {1, 1, 1}, storage::MAX,
                     &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  ASSERT_TRUE(size_match(&db, "GP3_ZINTERSTORE_DESTINATION", 3));
  ASSERT_TRUE(score_members_match(&db, "GP3_ZINTERSTORE_DESTINATION",
                                  {{1000000, "MM1"}, {10000000, "MM2"}, {100000000, "MM3"}}));

  // ***************** Group 4 Test *****************
  // {      1, MM1} {      10, MM2} {      100, MM3}  weight 1
  // {   1000, MM1} {   10000, MM2} {   100000, MM3}  weight 2
  // {1000000, MM1} {10000000, MM2} {100000000, MM3}  weight 3
  //
  // {3002001, MM1} {30020010, MM2} {300200100, MM3}
  //
  std::vector<storage::ScoreMember> gp4_sm1{{1, "MM1"}, {10, "MM2"}, {100, "MM3"}};
  std::vector<storage::ScoreMember> gp4_sm2{{1000, "MM1"}, {10000, "MM2"}, {100000, "MM3"}};
  std::vector<storage::ScoreMember> gp4_sm3{{1000000, "MM1"}, {10000000, "MM2"}, {100000000, "MM3"}};
  s = db.ZAdd("GP4_ZINTERSTORE_SM1", gp4_sm1, &ret);
  s = db.ZAdd("GP4_ZINTERSTORE_SM2", gp4_sm2, &ret);
  s = db.ZAdd("GP4_ZINTERSTORE_SM3", gp4_sm3, &ret);
  s = db.ZInterstore("GP4_ZINTERSTORE_DESTINATION",
                     {"GP4_ZINTERSTORE_SM1", "GP4_ZINTERSTORE_SM2", "GP4_ZINTERSTORE_SM3"}, {1, 2, 3}, storage::SUM,
                     &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  ASSERT_TRUE(size_match(&db, "GP4_ZINTERSTORE_DESTINATION", 3));
  ASSERT_TRUE(score_members_match(&db, "GP4_ZINTERSTORE_DESTINATION",
                                  {{3002001, "MM1"}, {30020010, "MM2"}, {300200100, "MM3"}}));

  // ***************** Group 5 Test *****************
  // {      1, MM1} {      10, MM2} {      100, MM3}  weight 1
  // {   1000, MM1} {   10000, MM2} {   100000, MM3}  weight 2
  // {1000000, MM1}                 {100000000, MM3}  weight 3
  //
  // {3002001, MM1}                 {300200100, MM3}
  //
  std::vector<storage::ScoreMember> gp5_sm1{{1, "MM1"}, {10, "MM2"}, {100, "MM3"}};
  std::vector<storage::ScoreMember> gp5_sm2{{1000, "MM1"}, {10000, "MM2"}, {100000, "MM3"}};
  std::vector<storage::ScoreMember> gp5_sm3{{1000000, "MM1"}, {100000000, "MM3"}};
  s = db.ZAdd("GP5_ZINTERSTORE_SM1", gp5_sm1, &ret);
  s = db.ZAdd("GP5_ZINTERSTORE_SM2", gp5_sm2, &ret);
  s = db.ZAdd("GP5_ZINTERSTORE_SM3", gp5_sm3, &ret);
  s = db.ZInterstore("GP5_ZINTERSTORE_DESTINATION",
                     {"GP5_ZINTERSTORE_SM1", "GP5_ZINTERSTORE_SM2", "GP5_ZINTERSTORE_SM3"}, {1, 2, 3}, storage::SUM,
                     &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  ASSERT_TRUE(size_match(&db, "GP5_ZINTERSTORE_DESTINATION", 2));
  ASSERT_TRUE(score_members_match(&db, "GP5_ZINTERSTORE_DESTINATION", {{3002001, "MM1"}, {300200100, "MM3"}}));

  // ***************** Group 6 Test *****************
  // {      1, MM1} {      10, MM2} {      100, MM3}  weight 1
  // {   1000, MM1} {   10000, MM2} {   100000, MM3}  weight 2  (expire)
  // {1000000, MM1}                 {100000000, MM3}  weight 3
  //
  // {3000001, MM1} {      10, MM2} {300000100, MM3}
  //
  std::vector<storage::ScoreMember> gp6_sm1{{1, "MM1"}, {10, "MM2"}, {100, "MM3"}};
  std::vector<storage::ScoreMember> gp6_sm2{{1000, "MM1"}, {10000, "MM2"}, {100000, "MM3"}};
  std::vector<storage::ScoreMember> gp6_sm3{{1000000, "MM1"}, {100000000, "MM3"}};
  s = db.ZAdd("GP6_ZINTERSTORE_SM1", gp6_sm1, &ret);
  s = db.ZAdd("GP6_ZINTERSTORE_SM2", gp6_sm2, &ret);
  s = db.ZAdd("GP6_ZINTERSTORE_SM3", gp6_sm3, &ret);
  ASSERT_TRUE(make_expired(&db, "GP6_ZINTERSTORE_SM2"));
  s = db.ZInterstore("GP6_ZINTERSTORE_DESTINATION",
                     {"GP6_ZINTERSTORE_SM1", "GP6_ZINTERSTORE_SM2", "GP6_ZINTERSTORE_SM3"}, {1, 2, 3}, storage::SUM,
                     &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  ASSERT_TRUE(size_match(&db, "GP6_ZINTERSTORE_DESTINATION", 0));
  ASSERT_TRUE(score_members_match(&db, "GP6_ZINTERSTORE_DESTINATION", {}));

  // ***************** Group 7 Test *****************
  // {   1, MM1} {   10, MM2} {   100, MM3}             weight 1
  // {1000, MM1} {10000, MM2} {100000, MM3}             weight 2  (expire)
  //                                        {1000, MM4} weight 3
  //
  // {   1, MM1} {   10, MM2} {   100, MM3} {3000, MM4}
  //
  std::vector<storage::ScoreMember> gp7_sm1{{1, "MM1"}, {10, "MM2"}, {100, "MM3"}};
  std::vector<storage::ScoreMember> gp7_sm2{{1000, "MM1"}, {10000, "MM2"}, {100000, "MM3"}};
  std::vector<storage::ScoreMember> gp7_sm3{{1000, "MM4"}};
  s = db.ZAdd("GP7_ZINTERSTORE_SM1", gp7_sm1, &ret);
  s = db.ZAdd("GP7_ZINTERSTORE_SM2", gp7_sm2, &ret);
  s = db.ZAdd("GP7_ZINTERSTORE_SM3", gp7_sm3, &ret);
  ASSERT_TRUE(make_expired(&db, "GP7_ZINTERSTORE_SM2"));
  s = db.ZInterstore("GP7_ZINTERSTORE_DESTINATION",
                     {"GP7_ZINTERSTORE_SM1", "GP7_ZINTERSTORE_SM2", "GP7_ZINTERSTORE_SM3"}, {1, 2, 3}, storage::SUM,
                     &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  ASSERT_TRUE(size_match(&db, "GP7_ZINTERSTORE_DESTINATION", 0));
  ASSERT_TRUE(score_members_match(&db, "GP7_ZINTERSTORE_DESTINATION", {}));

  // ***************** Group 8 Test *****************
  // {1, MM1}                        weight 1
  //            {1, MM2}             weight 1
  //                       {1, MM3}  weight 1
  //
  // {1, MM1}   {1, MM2}   {1, MM3}
  //
  std::vector<storage::ScoreMember> gp8_sm1{{1, "MM1"}};
  std::vector<storage::ScoreMember> gp8_sm2{{1, "MM2"}};
  std::vector<storage::ScoreMember> gp8_sm3{{1, "MM3"}};
  s = db.ZAdd("GP8_ZINTERSTORE_SM1", gp8_sm1, &ret);
  s = db.ZAdd("GP8_ZINTERSTORE_SM2", gp8_sm2, &ret);
  s = db.ZAdd("GP8_ZINTERSTORE_SM3", gp8_sm3, &ret);
  s = db.ZInterstore("GP8_ZINTERSTORE_DESTINATION",
                     {"GP8_ZINTERSTORE_SM1", "GP8_ZINTERSTORE_SM2", "GP8_ZINTERSTORE_SM3"}, {1, 1, 1}, storage::MIN,
                     &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  ASSERT_TRUE(size_match(&db, "GP8_ZINTERSTORE_DESTINATION", 0));
  ASSERT_TRUE(score_members_match(&db, "GP8_ZINTERSTORE_DESTINATION", {}));

  // ***************** Group 9 Test *****************
  // {      1, MM1} {      10, MM2} {      100, MM3}  weight 1
  // {   1000, MM1} {   10000, MM2} {   100000, MM3}  weight 1
  // {1000000, MM1} {10000000, MM2} {100000000, MM3}  weight 1
  //
  // {1001001, MM1} {10010010, MM2} {100100100, MM3}
  //
  std::vector<storage::ScoreMember> gp9_sm1{{1, "MM1"}, {10, "MM2"}, {100, "MM3"}};
  std::vector<storage::ScoreMember> gp9_sm2{{1000, "MM1"}, {10000, "MM2"}, {100000, "MM3"}};
  std::vector<storage::ScoreMember> gp9_sm3{{1000000, "MM1"}, {10000000, "MM2"}, {100000000, "MM3"}};
  std::vector<storage::ScoreMember> gp9_destination{{1, "MM1"}};
  s = db.ZAdd("GP9_ZINTERSTORE_SM1", gp9_sm1, &ret);
  s = db.ZAdd("GP9_ZINTERSTORE_SM2", gp9_sm2, &ret);
  s = db.ZAdd("GP9_ZINTERSTORE_SM3", gp9_sm3, &ret);
  s = db.ZAdd("GP9_ZINTERSTORE_DESTINATION", gp9_destination, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  ASSERT_TRUE(size_match(&db, "GP9_ZINTERSTORE_DESTINATION", 1));
  ASSERT_TRUE(score_members_match(&db, "GP9_ZINTERSTORE_DESTINATION", {{1, "MM1"}}));

  s = db.ZInterstore("GP9_ZINTERSTORE_DESTINATION",
                     {"GP9_ZINTERSTORE_SM1", "GP9_ZINTERSTORE_SM2", "GP9_ZINTERSTORE_SM3"}, {1, 1, 1}, storage::SUM,
                     &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  ASSERT_TRUE(size_match(&db, "GP9_ZINTERSTORE_DESTINATION", 3));
  ASSERT_TRUE(score_members_match(&db, "GP9_ZINTERSTORE_DESTINATION",
                                  {{1001001, "MM1"}, {10010010, "MM2"}, {100100100, "MM3"}}));

  // ***************** Group 10 Test *****************
  // {      1, MM1} {      10, MM2} {      100, MM3}  weight 1
  // {   1000, MM1} {   10000, MM2} {   100000, MM3}  weight 1
  // {1000000, MM1} {10000000, MM2} {100000000, MM3}  weight 1
  //
  // {1001001, MM1} {10010010, MM2} {100100100, MM3}
  //
  std::vector<storage::ScoreMember> gp10_sm1{{1, "MM1"}, {10, "MM2"}, {100, "MM3"}};
  std::vector<storage::ScoreMember> gp10_sm2{{1000, "MM1"}, {10000, "MM2"}, {100000, "MM3"}};
  std::vector<storage::ScoreMember> gp10_sm3{{1000000, "MM1"}, {10000000, "MM2"}, {100000000, "MM3"}};
  s = db.ZAdd("GP10_ZINTERSTORE_SM1", gp10_sm1, &ret);
  s = db.ZAdd("GP10_ZINTERSTORE_SM2", gp10_sm2, &ret);
  s = db.ZAdd("GP10_ZINTERSTORE_SM3", gp10_sm3, &ret);
  s = db.ZInterstore("GP10_ZINTERSTORE_DESTINATION",
                     {"GP10_ZINTERSTORE_SM1", "GP10_ZINTERSTORE_SM2", "GP10_ZINTERSTORE_SM3", "GP10_ZINTERSTORE_SM4"},
                     {1, 1, 1, 1}, storage::SUM, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  ASSERT_TRUE(size_match(&db, "GP10_ZINTERSTORE_DESTINATION", 0));
  ASSERT_TRUE(score_members_match(&db, "GP10_ZINTERSTORE_DESTINATION", {}));
}

// ZRANGEBYLEX
TEST_F(ZSetsTest, ZRangebylexTest) {
  int32_t ret;

  std::vector<std::string> members;
  // ***************** Group 1 Test *****************
  // {1, e} {1, f} {1, g} {1, h} {1, i} {1, j} {1, k} {1, l} {1, m}
  //
  std::vector<storage::ScoreMember> gp1_sm1{{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "i"},
                                            {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}};
  s = db.ZAdd("GP1_ZRANGEBYLEX", gp1_sm1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  s = db.ZRangebylex("GP1_ZRANGEBYLEX", "a", "n", true, true, &members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(members, {"e", "f", "g", "h", "i", "j", "k", "l", "m"}));

  s = db.ZRangebylex("GP1_ZRANGEBYLEX", "e", "m", true, true, &members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(members, {"e", "f", "g", "h", "i", "j", "k", "l", "m"}));

  s = db.ZRangebylex("GP1_ZRANGEBYLEX", "e", "m", true, false, &members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(members, {"e", "f", "g", "h", "i", "j", "k", "l"}));

  s = db.ZRangebylex("GP1_ZRANGEBYLEX", "e", "m", false, true, &members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(members, {"f", "g", "h", "i", "j", "k", "l", "m"}));

  s = db.ZRangebylex("GP1_ZRANGEBYLEX", "e", "m", false, false, &members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(members, {"f", "g", "h", "i", "j", "k", "l"}));

  s = db.ZRangebylex("GP1_ZRANGEBYLEX", "h", "j", true, true, &members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(members, {"h", "i", "j"}));

  s = db.ZRangebylex("GP1_ZRANGEBYLEX", "h", "j", true, false, &members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(members, {"h", "i"}));

  s = db.ZRangebylex("GP1_ZRANGEBYLEX", "h", "j", false, false, &members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(members, {"i"}));

  s = db.ZRangebylex("GP1_ZRANGEBYLEX", "i", "i", true, true, &members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(members, {"i"}));

  s = db.ZRangebylex("GP1_ZRANGEBYLEX", "i", "i", true, false, &members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(members, {}));

  s = db.ZRangebylex("GP1_ZRANGEBYLEX", "i", "i", false, true, &members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(members, {}));

  s = db.ZRangebylex("GP1_ZRANGEBYLEX", "i", "i", false, false, &members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(members, {}));

  s = db.ZRangebylex("GP1_ZRANGEBYLEX", "-", "+", true, true, &members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(members, {"e", "f", "g", "h", "i", "j", "k", "l", "m"}));

  s = db.ZRangebylex("GP1_ZRANGEBYLEX", "-", "+", true, false, &members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(members, {"e", "f", "g", "h", "i", "j", "k", "l", "m"}));

  s = db.ZRangebylex("GP1_ZRANGEBYLEX", "-", "+", false, true, &members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(members, {"e", "f", "g", "h", "i", "j", "k", "l", "m"}));

  s = db.ZRangebylex("GP1_ZRANGEBYLEX", "-", "+", false, false, &members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(members, {"e", "f", "g", "h", "i", "j", "k", "l", "m"}));

  s = db.ZRangebylex("GP1_ZRANGEBYLEX", "i", "+", true, true, &members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(members, {"i", "j", "k", "l", "m"}));

  s = db.ZRangebylex("GP1_ZRANGEBYLEX", "i", "+", false, true, &members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(members, {"j", "k", "l", "m"}));

  s = db.ZRangebylex("GP1_ZRANGEBYLEX", "-", "i", true, true, &members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(members, {"e", "f", "g", "h", "i"}));

  s = db.ZRangebylex("GP1_ZRANGEBYLEX", "-", "i", true, false, &members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(members, {"e", "f", "g", "h"}));

  s = db.ZRangebylex("GP1_ZRANGEBYLEX", "-", "e", true, true, &members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(members, {"e"}));

  s = db.ZRangebylex("GP1_ZRANGEBYLEX", "-", "e", true, false, &members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(members, {}));

  s = db.ZRangebylex("GP1_ZRANGEBYLEX", "m", "+", true, true, &members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(members, {"m"}));

  s = db.ZRangebylex("GP1_ZRANGEBYLEX", "m", "+", false, true, &members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(members, {}));

  // ***************** Group 2 Test *****************
  // {1, e} {1, f} {1, g} {1, h} {1, i} {1, j} {1, k} {1, l} {1, m}   (expire)
  //
  std::vector<storage::ScoreMember> gp2_sm1{{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "i"},
                                            {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}};
  s = db.ZAdd("GP2_ZRANGEBYLEX", gp1_sm1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);
  ASSERT_TRUE(make_expired(&db, "GP2_ZRANGEBYLEX"));

  s = db.ZRangebylex("GP2_ZRANGEBYLEX", "-", "+", true, true, &members);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_TRUE(members_match(members, {}));

  // ***************** Group 3 Test *****************
  s = db.ZRangebylex("GP3_ZRANGEBYLEX", "-", "+", true, true, &members);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_TRUE(members_match(members, {}));
}

// ZLEXCOUNT
TEST_F(ZSetsTest, ZLexcountTest) {
  int32_t ret;

  std::vector<std::string> members;
  // ***************** Group 1 Test *****************
  // {1, e} {1, f} {1, g} {1, h} {1, i} {1, j} {1, k} {1, l} {1, m}
  //
  std::vector<storage::ScoreMember> gp1_sm1{{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "i"},
                                            {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}};
  s = db.ZAdd("GP1_ZLEXCOUNT", gp1_sm1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  s = db.ZLexcount("GP1_ZLEXCOUNT", "a", "n", true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  s = db.ZLexcount("GP1_ZLEXCOUNT", "e", "m", true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  s = db.ZLexcount("GP1_ZLEXCOUNT", "e", "m", true, false, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 8);

  s = db.ZLexcount("GP1_ZLEXCOUNT", "e", "m", false, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 8);

  s = db.ZLexcount("GP1_ZLEXCOUNT", "e", "m", false, false, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 7);

  s = db.ZLexcount("GP1_ZLEXCOUNT", "h", "j", true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  s = db.ZLexcount("GP1_ZLEXCOUNT", "h", "j", true, false, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);

  s = db.ZLexcount("GP1_ZLEXCOUNT", "h", "j", false, false, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.ZLexcount("GP1_ZLEXCOUNT", "i", "i", true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.ZLexcount("GP1_ZLEXCOUNT", "i", "i", true, false, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);

  s = db.ZLexcount("GP1_ZLEXCOUNT", "i", "i", false, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);

  s = db.ZLexcount("GP1_ZLEXCOUNT", "i", "i", false, false, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);

  s = db.ZLexcount("GP1_ZLEXCOUNT", "-", "+", true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  s = db.ZLexcount("GP1_ZLEXCOUNT", "-", "+", true, false, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  s = db.ZLexcount("GP1_ZLEXCOUNT", "-", "+", false, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  s = db.ZLexcount("GP1_ZLEXCOUNT", "-", "+", false, false, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  s = db.ZLexcount("GP1_ZLEXCOUNT", "i", "+", true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 5);

  s = db.ZLexcount("GP1_ZLEXCOUNT", "i", "+", false, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);

  s = db.ZLexcount("GP1_ZLEXCOUNT", "-", "i", true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 5);

  s = db.ZLexcount("GP1_ZLEXCOUNT", "-", "i", true, false, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);

  s = db.ZLexcount("GP1_ZLEXCOUNT", "-", "e", true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.ZLexcount("GP1_ZLEXCOUNT", "-", "e", true, false, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);

  s = db.ZLexcount("GP1_ZLEXCOUNT", "m", "+", true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.ZLexcount("GP1_ZLEXCOUNT", "m", "+", false, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);

  // ***************** Group 2 Test *****************
  // {1, e} {1, f} {1, g} {1, h} {1, i} {1, j} {1, k} {1, l} {1, m}   (expire)
  //
  std::vector<storage::ScoreMember> gp2_sm1{{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "i"},
                                            {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}};
  s = db.ZAdd("GP2_ZLEXCOUNT", gp1_sm1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);
  ASSERT_TRUE(make_expired(&db, "GP2_ZLEXCOUNT"));

  s = db.ZLexcount("GP2_ZLEXCOUNT", "-", "+", true, true, &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(ret, 0);

  // ***************** Group 3 Test *****************
  s = db.ZLexcount("GP3_ZLEXCOUNT", "-", "+", true, true, &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(ret, 0);
}

// ZREMRANGEBYLEX
TEST_F(ZSetsTest, ZRemrangebylexTest) {
  int32_t ret;
  std::vector<std::string> members;

  // ***************** Group 1 Test *****************
  // {1, e} {1, f} {1, g} {1, h} {1, i} {1, j} {1, k} {1, l} {1, m}
  //
  std::vector<storage::ScoreMember> gp1_sm{{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "i"},
                                           {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}};
  s = db.ZAdd("GP1_ZREMRANGEBYLEX", gp1_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  s = db.ZRemrangebylex("GP1_ZREMRANGEBYLEX", "a", "n", true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);
  ASSERT_TRUE(size_match(&db, "GP1_ZREMRANGEBYLEX", 0));
  ASSERT_TRUE(score_members_match(&db, "GP1_ZREMRANGEBYLEX", {}));

  // ***************** Group 2 Test *****************
  // {1, e} {1, f} {1, g} {1, h} {1, i} {1, j} {1, k} {1, l} {1, m}
  //
  std::vector<storage::ScoreMember> gp2_sm{{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "i"},
                                           {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}};
  s = db.ZAdd("GP2_ZREMRANGEBYLEX", gp2_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  s = db.ZRemrangebylex("GP2_ZREMRANGEBYLEX", "e", "m", true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);
  ASSERT_TRUE(size_match(&db, "GP2_ZREMRANGEBYLEX", 0));
  ASSERT_TRUE(score_members_match(&db, "GP2_ZREMRANGEBYLEX", {}));

  // ***************** Group 3 Test *****************
  // {1, e} {1, f} {1, g} {1, h} {1, i} {1, j} {1, k} {1, l} {1, m}
  //
  std::vector<storage::ScoreMember> gp3_sm{{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "i"},
                                           {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}};
  s = db.ZAdd("GP3_ZREMRANGEBYLEX", gp3_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  s = db.ZRemrangebylex("GP3_ZREMRANGEBYLEX", "e", "m", true, false, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 8);
  ASSERT_TRUE(size_match(&db, "GP3_ZREMRANGEBYLEX", 1));
  ASSERT_TRUE(score_members_match(&db, "GP3_ZREMRANGEBYLEX", {{1, "m"}}));

  // ***************** Group 4 Test *****************
  // {1, e} {1, f} {1, g} {1, h} {1, i} {1, j} {1, k} {1, l} {1, m}
  //
  std::vector<storage::ScoreMember> gp4_sm{{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "i"},
                                           {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}};
  s = db.ZAdd("GP4_ZREMRANGEBYLEX", gp4_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  s = db.ZRemrangebylex("GP4_ZREMRANGEBYLEX", "e", "m", false, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 8);
  ASSERT_TRUE(size_match(&db, "GP4_ZREMRANGEBYLEX", 1));
  ASSERT_TRUE(score_members_match(&db, "GP4_ZREMRANGEBYLEX", {{1, "e"}}));

  // ***************** Group 5 Test *****************
  // {1, e} {1, f} {1, g} {1, h} {1, i} {1, j} {1, k} {1, l} {1, m}
  //
  std::vector<storage::ScoreMember> gp5_sm{{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "i"},
                                           {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}};
  s = db.ZAdd("GP5_ZREMRANGEBYLEX", gp5_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  s = db.ZRemrangebylex("GP5_ZREMRANGEBYLEX", "e", "m", false, false, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 7);
  ASSERT_TRUE(size_match(&db, "GP5_ZREMRANGEBYLEX", 2));
  ASSERT_TRUE(score_members_match(&db, "GP5_ZREMRANGEBYLEX", {{1, "e"}, {1, "m"}}));

  // ***************** Group 6 Test *****************
  // {1, e} {1, f} {1, g} {1, h} {1, i} {1, j} {1, k} {1, l} {1, m}
  //
  std::vector<storage::ScoreMember> gp6_sm{{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "i"},
                                           {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}};
  s = db.ZAdd("GP6_ZREMRANGEBYLEX", gp6_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  s = db.ZRemrangebylex("GP6_ZREMRANGEBYLEX", "h", "j", true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  ASSERT_TRUE(size_match(&db, "GP6_ZREMRANGEBYLEX", 6));
  ASSERT_TRUE(
      score_members_match(&db, "GP6_ZREMRANGEBYLEX", {{1, "e"}, {1, "f"}, {1, "g"}, {1, "k"}, {1, "l"}, {1, "m"}}));

  // ***************** Group 7 Test *****************
  // {1, e} {1, f} {1, g} {1, h} {1, i} {1, j} {1, k} {1, l} {1, m}
  //
  std::vector<storage::ScoreMember> gp7_sm{{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "i"},
                                           {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}};
  s = db.ZAdd("GP7_ZREMRANGEBYLEX", gp7_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  s = db.ZRemrangebylex("GP7_ZREMRANGEBYLEX", "h", "j", true, false, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  ASSERT_TRUE(size_match(&db, "GP7_ZREMRANGEBYLEX", 7));
  ASSERT_TRUE(score_members_match(&db, "GP7_ZREMRANGEBYLEX",
                                  {{1, "e"}, {1, "f"}, {1, "g"}, {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}}));

  // ***************** Group 8 Test *****************
  // {1, e} {1, f} {1, g} {1, h} {1, i} {1, j} {1, k} {1, l} {1, m}
  //
  std::vector<storage::ScoreMember> gp8_sm{{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "i"},
                                           {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}};
  s = db.ZAdd("GP8_ZREMRANGEBYLEX", gp8_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  s = db.ZRemrangebylex("GP8_ZREMRANGEBYLEX", "h", "j", false, false, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  ASSERT_TRUE(size_match(&db, "GP8_ZREMRANGEBYLEX", 8));
  ASSERT_TRUE(score_members_match(&db, "GP8_ZREMRANGEBYLEX",
                                  {{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}}));

  // ***************** Group 9 Test *****************
  // {1, e} {1, f} {1, g} {1, h} {1, i} {1, j} {1, k} {1, l} {1, m}
  //
  std::vector<storage::ScoreMember> gp9_sm{{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "i"},
                                           {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}};
  s = db.ZAdd("GP9_ZREMRANGEBYLEX", gp9_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  s = db.ZRemrangebylex("GP9_ZREMRANGEBYLEX", "i", "i", true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  ASSERT_TRUE(size_match(&db, "GP9_ZREMRANGEBYLEX", 8));
  ASSERT_TRUE(score_members_match(&db, "GP9_ZREMRANGEBYLEX",
                                  {{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}}));

  // ***************** Group 10 Test *****************
  // {1, e} {1, f} {1, g} {1, h} {1, i} {1, j} {1, k} {1, l} {1, m}
  //
  std::vector<storage::ScoreMember> gp10_sm{{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "i"},
                                            {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}};
  s = db.ZAdd("GP10_ZREMRANGEBYLEX", gp10_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  s = db.ZRemrangebylex("GP10_ZREMRANGEBYLEX", "i", "i", true, false, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  ASSERT_TRUE(size_match(&db, "GP10_ZREMRANGEBYLEX", 9));
  ASSERT_TRUE(
      score_members_match(&db, "GP10_ZREMRANGEBYLEX",
                          {{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "i"}, {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}}));

  // ***************** Group 11 Test *****************
  // {1, e} {1, f} {1, g} {1, h} {1, i} {1, j} {1, k} {1, l} {1, m}
  //
  std::vector<storage::ScoreMember> gp11_sm{{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "i"},
                                            {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}};
  s = db.ZAdd("GP11_ZREMRANGEBYLEX", gp11_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  s = db.ZRemrangebylex("GP11_ZREMRANGEBYLEX", "i", "i", false, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  ASSERT_TRUE(size_match(&db, "GP11_ZREMRANGEBYLEX", 9));
  ASSERT_TRUE(
      score_members_match(&db, "GP11_ZREMRANGEBYLEX",
                          {{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "i"}, {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}}));

  // ***************** Group 12 Test *****************
  // {1, e} {1, f} {1, g} {1, h} {1, i} {1, j} {1, k} {1, l} {1, m}
  //
  std::vector<storage::ScoreMember> gp12_sm{{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "i"},
                                            {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}};
  s = db.ZAdd("GP12_ZREMRANGEBYLEX", gp12_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  s = db.ZRemrangebylex("GP12_ZREMRANGEBYLEX", "i", "i", false, false, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  ASSERT_TRUE(size_match(&db, "GP12_ZREMRANGEBYLEX", 9));
  ASSERT_TRUE(
      score_members_match(&db, "GP12_ZREMRANGEBYLEX",
                          {{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "i"}, {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}}));

  // ***************** Group 13 Test *****************
  // {1, e} {1, f} {1, g} {1, h} {1, i} {1, j} {1, k} {1, l} {1, m}
  //
  std::vector<storage::ScoreMember> gp13_sm{{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "i"},
                                            {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}};
  s = db.ZAdd("GP13_ZREMRANGEBYLEX", gp13_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  s = db.ZRemrangebylex("GP13_ZREMRANGEBYLEX", "-", "+", true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);
  ASSERT_TRUE(size_match(&db, "GP13_ZREMRANGEBYLEX", 0));
  ASSERT_TRUE(score_members_match(&db, "GP13_ZREMRANGEBYLEX", {}));

  // ***************** Group 14 Test *****************
  // {1, e} {1, f} {1, g} {1, h} {1, i} {1, j} {1, k} {1, l} {1, m}
  //
  std::vector<storage::ScoreMember> gp14_sm{{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "i"},
                                            {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}};
  s = db.ZAdd("GP14_ZREMRANGEBYLEX", gp14_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  s = db.ZRemrangebylex("GP14_ZREMRANGEBYLEX", "-", "+", true, false, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);
  ASSERT_TRUE(size_match(&db, "GP14_ZREMRANGEBYLEX", 0));
  ASSERT_TRUE(score_members_match(&db, "GP14_ZREMRANGEBYLEX", {}));

  // ***************** Group 15 Test *****************
  // {1, e} {1, f} {1, g} {1, h} {1, i} {1, j} {1, k} {1, l} {1, m}
  //
  std::vector<storage::ScoreMember> gp15_sm{{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "i"},
                                            {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}};
  s = db.ZAdd("GP15_ZREMRANGEBYLEX", gp15_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  s = db.ZRemrangebylex("GP15_ZREMRANGEBYLEX", "-", "+", false, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);
  ASSERT_TRUE(size_match(&db, "GP15_ZREMRANGEBYLEX", 0));
  ASSERT_TRUE(score_members_match(&db, "GP15_ZREMRANGEBYLEX", {}));

  // ***************** Group 16 Test *****************
  // {1, e} {1, f} {1, g} {1, h} {1, i} {1, j} {1, k} {1, l} {1, m}
  //
  std::vector<storage::ScoreMember> gp16_sm{{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "i"},
                                            {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}};
  s = db.ZAdd("GP16_ZREMRANGEBYLEX", gp16_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  s = db.ZRemrangebylex("GP16_ZREMRANGEBYLEX", "-", "+", false, false, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);
  ASSERT_TRUE(size_match(&db, "GP16_ZREMRANGEBYLEX", 0));
  ASSERT_TRUE(score_members_match(&db, "GP16_ZREMRANGEBYLEX", {}));

  // ***************** Group 17 Test *****************
  // {1, e} {1, f} {1, g} {1, h} {1, i} {1, j} {1, k} {1, l} {1, m}
  //
  std::vector<storage::ScoreMember> gp17_sm{{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "i"},
                                            {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}};
  s = db.ZAdd("GP17_ZREMRANGEBYLEX", gp17_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  s = db.ZRemrangebylex("GP17_ZREMRANGEBYLEX", "i", "+", true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 5);
  ASSERT_TRUE(size_match(&db, "GP17_ZREMRANGEBYLEX", 4));
  ASSERT_TRUE(score_members_match(&db, "GP17_ZREMRANGEBYLEX", {{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}}));

  // ***************** Group 18 Test *****************
  // {1, e} {1, f} {1, g} {1, h} {1, i} {1, j} {1, k} {1, l} {1, m}
  //
  std::vector<storage::ScoreMember> gp18_sm{{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "i"},
                                            {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}};
  s = db.ZAdd("GP18_ZREMRANGEBYLEX", gp18_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  s = db.ZRemrangebylex("GP18_ZREMRANGEBYLEX", "i", "+", false, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  ASSERT_TRUE(size_match(&db, "GP18_ZREMRANGEBYLEX", 5));
  ASSERT_TRUE(score_members_match(&db, "GP18_ZREMRANGEBYLEX", {{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "i"}}));

  // ***************** Group 19 Test *****************
  // {1, e} {1, f} {1, g} {1, h} {1, i} {1, j} {1, k} {1, l} {1, m}
  //
  std::vector<storage::ScoreMember> gp19_sm{{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "i"},
                                            {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}};
  s = db.ZAdd("GP19_ZREMRANGEBYLEX", gp19_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  s = db.ZRemrangebylex("GP19_ZREMRANGEBYLEX", "-", "i", true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 5);
  ASSERT_TRUE(size_match(&db, "GP19_ZREMRANGEBYLEX", 4));
  ASSERT_TRUE(score_members_match(&db, "GP19_ZREMRANGEBYLEX", {{1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}}));

  // ***************** Group 20 Test *****************
  // {1, e} {1, f} {1, g} {1, h} {1, i} {1, j} {1, k} {1, l} {1, m}
  //
  std::vector<storage::ScoreMember> gp20_sm{{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "i"},
                                            {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}};
  s = db.ZAdd("GP20_ZREMRANGEBYLEX", gp20_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  s = db.ZRemrangebylex("GP20_ZREMRANGEBYLEX", "-", "i", true, false, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  ASSERT_TRUE(size_match(&db, "GP20_ZREMRANGEBYLEX", 5));
  ASSERT_TRUE(score_members_match(&db, "GP20_ZREMRANGEBYLEX", {{1, "i"}, {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}}));

  // ***************** Group 21 Test *****************
  // {1, e} {1, f} {1, g} {1, h} {1, i} {1, j} {1, k} {1, l} {1, m}
  //
  std::vector<storage::ScoreMember> gp21_sm{{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "i"},
                                            {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}};
  s = db.ZAdd("GP21_ZREMRANGEBYLEX", gp21_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  s = db.ZRemrangebylex("GP21_ZREMRANGEBYLEX", "-", "e", true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  ASSERT_TRUE(size_match(&db, "GP21_ZREMRANGEBYLEX", 8));
  ASSERT_TRUE(score_members_match(&db, "GP21_ZREMRANGEBYLEX",
                                  {{1, "f"}, {1, "g"}, {1, "h"}, {1, "i"}, {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}}));

  // ***************** Group 22 Test *****************
  // {1, e} {1, f} {1, g} {1, h} {1, i} {1, j} {1, k} {1, l} {1, m}
  //
  std::vector<storage::ScoreMember> gp22_sm{{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "i"},
                                            {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}};
  s = db.ZAdd("GP22_ZREMRANGEBYLEX", gp22_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  s = db.ZRemrangebylex("GP22_ZREMRANGEBYLEX", "-", "e", true, false, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  ASSERT_TRUE(size_match(&db, "GP22_ZREMRANGEBYLEX", 9));
  ASSERT_TRUE(
      score_members_match(&db, "GP22_ZREMRANGEBYLEX",
                          {{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "i"}, {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}}));

  // ***************** Group 23 Test *****************
  // {1, e} {1, f} {1, g} {1, h} {1, i} {1, j} {1, k} {1, l} {1, m}
  //
  std::vector<storage::ScoreMember> gp23_sm{{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "i"},
                                            {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}};
  s = db.ZAdd("GP23_ZREMRANGEBYLEX", gp23_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  s = db.ZRemrangebylex("GP23_ZREMRANGEBYLEX", "m", "+", true, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  ASSERT_TRUE(size_match(&db, "GP23_ZREMRANGEBYLEX", 8));
  ASSERT_TRUE(score_members_match(&db, "GP23_ZREMRANGEBYLEX",
                                  {{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "i"}, {1, "j"}, {1, "k"}, {1, "l"}}));

  // ***************** Group 24 Test *****************
  // {1, e} {1, f} {1, g} {1, h} {1, i} {1, j} {1, k} {1, l} {1, m}
  //
  std::vector<storage::ScoreMember> gp24_sm{{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "i"},
                                            {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}};
  s = db.ZAdd("GP24_ZREMRANGEBYLEX", gp24_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  s = db.ZRemrangebylex("GP24_ZREMRANGEBYLEX", "m", "+", false, true, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  ASSERT_TRUE(size_match(&db, "GP24_ZREMRANGEBYLEX", 9));
  ASSERT_TRUE(
      score_members_match(&db, "GP24_ZREMRANGEBYLEX",
                          {{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "i"}, {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}}));

  // ***************** Group 25 Test *****************
  // {1, e} {1, f} {1, g} {1, h} {1, i} {1, j} {1, k} {1, l} {1, m}   (expire)
  //
  std::vector<storage::ScoreMember> gp25_sm{{1, "e"}, {1, "f"}, {1, "g"}, {1, "h"}, {1, "i"},
                                            {1, "j"}, {1, "k"}, {1, "l"}, {1, "m"}};
  s = db.ZAdd("GP25_ZREMRANGEBYLEX", gp25_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);
  ASSERT_TRUE(make_expired(&db, "GP25_ZREMRANGEBYLEX"));

  s = db.ZRemrangebylex("GP25_ZREMRANGEBYLEX", "-", "+", true, true, &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(ret, 0);

  // ***************** Group 26 Test *****************
  s = db.ZRemrangebylex("GP26_ZREMRANGEBYLEX", "-", "+", true, true, &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(ret, 0);
}

// ZScan
TEST_F(ZSetsTest, ZScanTest) {
  int32_t ret = 0;
  int64_t cursor = 0, next_cursor = 0;
  std::vector<ScoreMember> score_member_out;

  // ***************** Group 1 Test *****************
  // {0,a} {0,b} {0,c} {0,d} {0,e} {0,f} {0,g} {0,h}
  // 0     1     2     3     4     5     6     7
  std::vector<ScoreMember> gp1_score_member{{0, "a"}, {0, "b"}, {0, "c"}, {0, "d"},
                                            {0, "e"}, {0, "f"}, {0, "g"}, {0, "h"}};
  s = db.ZAdd("GP1_ZSCAN_KEY", gp1_score_member, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP1_ZSCAN_KEY", 8));

  s = db.ZScan("GP1_ZSCAN_KEY", 0, "*", 3, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_member_out.size(), 3);
  ASSERT_EQ(next_cursor, 3);
  ASSERT_TRUE(score_members_match(score_member_out, {{0, "a"}, {0, "b"}, {0, "c"}}));

  score_member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.ZScan("GP1_ZSCAN_KEY", cursor, "*", 3, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_member_out.size(), 3);
  ASSERT_EQ(next_cursor, 6);
  ASSERT_TRUE(score_members_match(score_member_out, {{0, "d"}, {0, "e"}, {0, "f"}}));

  score_member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.ZScan("GP1_ZSCAN_KEY", cursor, "*", 3, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_member_out.size(), 2);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(score_members_match(score_member_out, {{0, "g"}, {0, "h"}}));

  // ***************** Group 2 Test *****************
  // {0,a} {0,b} {0,c} {0,d} {0,e} {0,f} {0,g} {0,h}
  // 0     1     2     3     4     5     6     7
  std::vector<ScoreMember> gp2_score_member{{0, "a"}, {0, "b"}, {0, "c"}, {0, "d"},
                                            {0, "e"}, {0, "f"}, {0, "g"}, {0, "h"}};
  s = db.ZAdd("GP2_ZSCAN_KEY", gp2_score_member, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP2_ZSCAN_KEY", 8));

  s = db.ZScan("GP2_ZSCAN_KEY", 0, "*", 1, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_member_out.size(), 1);
  ASSERT_EQ(next_cursor, 1);
  ASSERT_TRUE(score_members_match(score_member_out, {{0, "a"}}));

  score_member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.ZScan("GP2_ZSCAN_KEY", cursor, "*", 1, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_member_out.size(), 1);
  ASSERT_EQ(next_cursor, 2);
  ASSERT_TRUE(score_members_match(score_member_out, {{0, "b"}}));

  score_member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.ZScan("GP2_ZSCAN_KEY", cursor, "*", 1, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_member_out.size(), 1);
  ASSERT_EQ(next_cursor, 3);
  ASSERT_TRUE(score_members_match(score_member_out, {{0, "c"}}));

  score_member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.ZScan("GP2_ZSCAN_KEY", cursor, "*", 1, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_member_out.size(), 1);
  ASSERT_EQ(next_cursor, 4);
  ASSERT_TRUE(score_members_match(score_member_out, {{0, "d"}}));

  score_member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.ZScan("GP2_ZSCAN_KEY", cursor, "*", 1, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_member_out.size(), 1);
  ASSERT_EQ(next_cursor, 5);
  ASSERT_TRUE(score_members_match(score_member_out, {{0, "e"}}));

  score_member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.ZScan("GP2_ZSCAN_KEY", cursor, "*", 1, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_member_out.size(), 1);
  ASSERT_EQ(next_cursor, 6);
  ASSERT_TRUE(score_members_match(score_member_out, {{0, "f"}}));

  score_member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.ZScan("GP2_ZSCAN_KEY", cursor, "*", 1, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_member_out.size(), 1);
  ASSERT_EQ(next_cursor, 7);
  ASSERT_TRUE(score_members_match(score_member_out, {{0, "g"}}));

  score_member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.ZScan("GP2_ZSCAN_KEY", cursor, "*", 1, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_member_out.size(), 1);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(score_members_match(score_member_out, {{0, "h"}}));

  // ***************** Group 3 Test *****************
  // {0,a} {0,b} {0,c} {0,d} {0,e} {0,f} {0,g} {0,h}
  // 0     1     2     3     4     5     6     7
  std::vector<ScoreMember> gp3_score_member{{0, "a"}, {0, "b"}, {0, "c"}, {0, "d"},
                                            {0, "e"}, {0, "f"}, {0, "g"}, {0, "h"}};
  s = db.ZAdd("GP3_ZSCAN_KEY", gp3_score_member, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP3_ZSCAN_KEY", 8));

  score_member_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.ZScan("GP3_ZSCAN_KEY", cursor, "*", 5, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_member_out.size(), 5);
  ASSERT_EQ(next_cursor, 5);
  ASSERT_TRUE(score_members_match(score_member_out, {{0, "a"}, {0, "b"}, {0, "c"}, {0, "d"}, {0, "e"}}));

  score_member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.ZScan("GP3_ZSCAN_KEY", cursor, "*", 5, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_member_out.size(), 3);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(score_members_match(score_member_out, {{0, "f"}, {0, "g"}, {0, "h"}}));

  // ***************** Group 4 Test *****************
  // {0,a} {0,b} {0,c} {0,d} {0,e} {0,f} {0,g} {0,h}
  // 0     1     2     3     4     5     6     7
  std::vector<ScoreMember> gp4_score_member{{0, "a"}, {0, "b"}, {0, "c"}, {0, "d"},
                                            {0, "e"}, {0, "f"}, {0, "g"}, {0, "h"}};
  s = db.ZAdd("GP4_ZSCAN_KEY", gp4_score_member, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(size_match(&db, "GP4_ZSCAN_KEY", 8));

  score_member_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.ZScan("GP4_ZSCAN_KEY", cursor, "*", 10, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_member_out.size(), 8);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(score_members_match(score_member_out,
                                  {{0, "a"}, {0, "b"}, {0, "c"}, {0, "d"}, {0, "e"}, {0, "f"}, {0, "g"}, {0, "h"}}));

  // ***************** Group 5 Test *****************
  // {0,a_1_} {0,a_2_} {0,a_3_} {0,b_1_} {0,b_2_} {0,b_3_} {0,c_1_} {0,c_2_} {0,c_3_}
  // 0        1        2        3        4        5        6        7        8
  std::vector<ScoreMember> gp5_score_member{{0, "a_1_"}, {0, "a_2_"}, {0, "a_3_"}, {0, "b_1_"}, {0, "b_2_"},
                                            {0, "b_3_"}, {0, "c_1_"}, {0, "c_2_"}, {0, "c_3_"}};
  s = db.ZAdd("GP5_ZSCAN_KEY", gp5_score_member, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);
  ASSERT_TRUE(size_match(&db, "GP5_ZSCAN_KEY", 9));

  score_member_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.ZScan("GP5_ZSCAN_KEY", cursor, "*1*", 3, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_member_out.size(), 1);
  ASSERT_EQ(next_cursor, 3);
  ASSERT_TRUE(score_members_match(score_member_out, {{0, "a_1_"}}));

  score_member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.ZScan("GP5_ZSCAN_KEY", cursor, "*1*", 3, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_member_out.size(), 1);
  ASSERT_EQ(next_cursor, 6);
  ASSERT_TRUE(score_members_match(score_member_out, {{0, "b_1_"}}));

  score_member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.ZScan("GP5_ZSCAN_KEY", cursor, "*1*", 3, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_member_out.size(), 1);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(score_members_match(score_member_out, {{0, "c_1_"}}));

  // ***************** Group 6 Test *****************
  // {0,a_1_} {0,a_2_} {0,a_3_} {0,b_1_} {0,b_2_} {0,b_3_} {0,c_1_} {0,c_2_} {0,c_3_}
  // 0        1        2        3        4        5        6        7        8
  std::vector<ScoreMember> gp6_score_member{{0, "a_1_"}, {0, "a_2_"}, {0, "a_3_"}, {0, "b_1_"}, {0, "b_2_"},
                                            {0, "b_3_"}, {0, "c_1_"}, {0, "c_2_"}, {0, "c_3_"}};
  s = db.ZAdd("GP6_ZSCAN_KEY", gp6_score_member, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);
  ASSERT_TRUE(size_match(&db, "GP6_ZSCAN_KEY", 9));

  score_member_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.ZScan("GP6_ZSCAN_KEY", cursor, "a*", 3, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_member_out.size(), 3);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(score_members_match(score_member_out, {{0, "a_1_"}, {0, "a_2_"}, {0, "a_3_"}}));

  score_member_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.ZScan("GP6_ZSCAN_KEY", cursor, "a*", 2, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_member_out.size(), 2);
  ASSERT_EQ(next_cursor, 2);
  ASSERT_TRUE(score_members_match(score_member_out, {{0, "a_1_"}, {0, "a_2_"}}));

  score_member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.ZScan("GP6_ZSCAN_KEY", cursor, "a*", 2, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_member_out.size(), 1);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(score_members_match(score_member_out, {{0, "a_3_"}}));

  score_member_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.ZScan("GP6_ZSCAN_KEY", cursor, "a*", 1, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_member_out.size(), 1);
  ASSERT_EQ(next_cursor, 1);
  ASSERT_TRUE(score_members_match(score_member_out, {{0, "a_1_"}}));

  score_member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.ZScan("GP6_ZSCAN_KEY", cursor, "a*", 1, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_member_out.size(), 1);
  ASSERT_EQ(next_cursor, 2);
  ASSERT_TRUE(score_members_match(score_member_out, {{0, "a_2_"}}));

  score_member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.ZScan("GP6_ZSCAN_KEY", cursor, "a*", 1, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_member_out.size(), 1);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(score_members_match(score_member_out, {{0, "a_3_"}}));

  // ***************** Group 7 Test *****************
  // {0,a_1_} {0,a_2_} {0,a_3_} {0,b_1_} {0,b_2_} {0,b_3_} {0,c_1_} {0,c_2_} {0,c_3_}
  // 0        1        2        3        4        5        6        7        8
  std::vector<ScoreMember> gp7_score_member{{0, "a_1_"}, {0, "a_2_"}, {0, "a_3_"}, {0, "b_1_"}, {0, "b_2_"},
                                            {0, "b_3_"}, {0, "c_1_"}, {0, "c_2_"}, {0, "c_3_"}};
  s = db.ZAdd("GP7_ZSCAN_KEY", gp7_score_member, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);
  ASSERT_TRUE(size_match(&db, "GP7_ZSCAN_KEY", 9));

  score_member_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.ZScan("GP7_ZSCAN_KEY", cursor, "b*", 3, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_member_out.size(), 3);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(score_members_match(score_member_out, {{0, "b_1_"}, {0, "b_2_"}, {0, "b_3_"}}));

  score_member_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.ZScan("GP7_ZSCAN_KEY", cursor, "b*", 2, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_member_out.size(), 2);
  ASSERT_EQ(next_cursor, 2);
  ASSERT_TRUE(score_members_match(score_member_out, {{0, "b_1_"}, {0, "b_2_"}}));

  score_member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.ZScan("GP7_ZSCAN_KEY", cursor, "b*", 2, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_member_out.size(), 1);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(score_members_match(score_member_out, {{0, "b_3_"}}));

  score_member_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.ZScan("GP7_ZSCAN_KEY", cursor, "b*", 1, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_member_out.size(), 1);
  ASSERT_EQ(next_cursor, 1);
  ASSERT_TRUE(score_members_match(score_member_out, {{0, "b_1_"}}));

  score_member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.ZScan("GP7_ZSCAN_KEY", cursor, "b*", 1, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_member_out.size(), 1);
  ASSERT_EQ(next_cursor, 2);
  ASSERT_TRUE(score_members_match(score_member_out, {{0, "b_2_"}}));

  score_member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.ZScan("GP7_ZSCAN_KEY", cursor, "b*", 1, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_member_out.size(), 1);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(score_members_match(score_member_out, {{0, "b_3_"}}));

  // ***************** Group 8 Test *****************
  // {0,a_1_} {0,a_2_} {0,a_3_} {0,b_1_} {0,b_2_} {0,b_3_} {0,c_1_} {0,c_2_} {0,c_3_}
  // 0        1        2        3        4        5        6        7        8
  std::vector<ScoreMember> gp8_score_member{{0, "a_1_"}, {0, "a_2_"}, {0, "a_3_"}, {0, "b_1_"}, {0, "b_2_"},
                                            {0, "b_3_"}, {0, "c_1_"}, {0, "c_2_"}, {0, "c_3_"}};
  s = db.ZAdd("GP8_ZSCAN_KEY", gp8_score_member, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);
  ASSERT_TRUE(size_match(&db, "GP8_ZSCAN_KEY", 9));

  score_member_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.ZScan("GP8_ZSCAN_KEY", cursor, "c*", 3, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_member_out.size(), 3);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(score_members_match(score_member_out, {{0, "c_1_"}, {0, "c_2_"}, {0, "c_3_"}}));

  score_member_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.ZScan("GP8_ZSCAN_KEY", cursor, "c*", 2, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_member_out.size(), 2);
  ASSERT_EQ(next_cursor, 2);
  ASSERT_TRUE(score_members_match(score_member_out, {{0, "c_1_"}, {0, "c_2_"}}));

  score_member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.ZScan("GP8_ZSCAN_KEY", cursor, "c*", 2, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_member_out.size(), 1);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(score_members_match(score_member_out, {{0, "c_3_"}}));

  score_member_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.ZScan("GP8_ZSCAN_KEY", cursor, "c*", 1, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_member_out.size(), 1);
  ASSERT_EQ(next_cursor, 1);
  ASSERT_TRUE(score_members_match(score_member_out, {{0, "c_1_"}}));

  score_member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.ZScan("GP8_ZSCAN_KEY", cursor, "c*", 1, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_member_out.size(), 1);
  ASSERT_EQ(next_cursor, 2);
  ASSERT_TRUE(score_members_match(score_member_out, {{0, "c_2_"}}));

  score_member_out.clear();
  cursor = next_cursor, next_cursor = 0;
  s = db.ZScan("GP8_ZSCAN_KEY", cursor, "c*", 1, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_member_out.size(), 1);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(score_members_match(score_member_out, {{0, "c_3_"}}));

  // ***************** Group 9 Test *****************
  // {0,a_1_} {0,a_2_} {0,a_3_} {0,b_1_} {0,b_2_} {0,b_3_} {0,c_1_} {0,c_2_} {0,c_3_}
  // 0        1        2        3        4        5        6        7        8
  std::vector<ScoreMember> gp9_score_member{{0, "a_1_"}, {0, "a_2_"}, {0, "a_3_"}, {0, "b_1_"}, {0, "b_2_"},
                                            {0, "b_3_"}, {0, "c_1_"}, {0, "c_2_"}, {0, "c_3_"}};
  s = db.ZAdd("GP9_ZSCAN_KEY", gp9_score_member, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);
  ASSERT_TRUE(size_match(&db, "GP9_ZSCAN_KEY", 9));

  score_member_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.ZScan("GP9_ZSCAN_KEY", cursor, "d*", 3, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(score_member_out.size(), 0);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(score_members_match(score_member_out, {}));

  // ***************** Group 10 Test *****************
  // {0,a_1_} {0,a_2_} {0,a_3_} {0,b_1_} {0,b_2_} {0,b_3_} {0,c_1_} {0,c_2_} {0,c_3_}
  // 0        1        2        3        4        5        6        7        8
  std::vector<ScoreMember> gp10_score_member{{0, "a_1_"}, {0, "a_2_"}, {0, "a_3_"}, {0, "b_1_"}, {0, "b_2_"},
                                             {0, "b_3_"}, {0, "c_1_"}, {0, "c_2_"}, {0, "c_3_"}};
  s = db.ZAdd("GP10_ZSCAN_KEY", gp10_score_member, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);
  ASSERT_TRUE(size_match(&db, "GP10_ZSCAN_KEY", 9));

  ASSERT_TRUE(make_expired(&db, "GP10_ZSCAN_KEY"));
  score_member_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.ZScan("GP10_ZSCAN_KEY", cursor, "*", 10, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(score_member_out.size(), 0);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(score_members_match(score_member_out, {}));

  // ***************** Group 11 Test *****************
  // ZScan Not Exist Key
  score_member_out.clear();
  cursor = 0, next_cursor = 0;
  s = db.ZScan("GP11_ZSCAN_KEY", cursor, "*", 10, &score_member_out, &next_cursor);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(score_member_out.size(), 0);
  ASSERT_EQ(next_cursor, 0);
  ASSERT_TRUE(score_members_match(score_member_out, {}));
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
