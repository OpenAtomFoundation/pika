//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "src/lru_cache.h"

#include <gtest/gtest.h>

#include "storage/storage.h"

using namespace storage;

TEST(LRUCacheTest, TestSetCapacityCase1) {
  Status s;
  std::string value;
  storage::LRUCache<std::string, std::string> lru_cache;
  lru_cache.SetCapacity(15);

  // ***************** Step 1 *****************
  // (k5, v5) -> (k4, v4) -> (k3, v3) -> (k2, v2) -> (k1, v1);
  lru_cache.Insert("k1", "v1", 1);
  lru_cache.Insert("k2", "v2", 2);
  lru_cache.Insert("k3", "v3", 3);
  lru_cache.Insert("k4", "v4", 4);
  lru_cache.Insert("k5", "v5", 5);
  ASSERT_EQ(lru_cache.Size(), 5);
  ASSERT_EQ(lru_cache.TotalCharge(), 15);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected(
      {{"k5", "v5"}, {"k4", "v4"}, {"k3", "v3"}, {"k2", "v2"}, {"k1", "v1"}}));

  // ***************** Step 2 *****************
  // (k5, v5) -> (k4, v4) -> (k3, v3)
  lru_cache.SetCapacity(12);
  ASSERT_EQ(lru_cache.Size(), 3);
  ASSERT_EQ(lru_cache.TotalCharge(), 12);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(
      lru_cache.LRUAsExpected({{"k5", "v5"}, {"k4", "v4"}, {"k3", "v3"}}));

  // ***************** Step 3 *****************
  // (k5, v5)
  lru_cache.SetCapacity(5);
  ASSERT_EQ(lru_cache.Size(), 1);
  ASSERT_EQ(lru_cache.TotalCharge(), 5);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected({{"k5", "v5"}}));

  // ***************** Step 4 *****************
  // (k5, v5)
  lru_cache.SetCapacity(15);
  ASSERT_EQ(lru_cache.Size(), 1);
  ASSERT_EQ(lru_cache.TotalCharge(), 5);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected({{"k5", "v5"}}));

  // ***************** Step 5 *****************
  // empty
  lru_cache.SetCapacity(1);
  ASSERT_EQ(lru_cache.Size(), 0);
  ASSERT_EQ(lru_cache.TotalCharge(), 0);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected({}));
}

TEST(LRUCacheTest, TestLookupCase1) {
  Status s;
  std::string value;
  storage::LRUCache<std::string, std::string> lru_cache;
  lru_cache.SetCapacity(5);

  // ***************** Step 1 *****************
  // (k5, v5) -> (k4, v4) -> (k3, v3) -> (k2, v2) -> (k1, v1);
  lru_cache.Insert("k1", "v1");
  lru_cache.Insert("k2", "v2");
  lru_cache.Insert("k3", "v3");
  lru_cache.Insert("k4", "v4");
  lru_cache.Insert("k5", "v5");
  ASSERT_EQ(lru_cache.Size(), 5);
  ASSERT_EQ(lru_cache.TotalCharge(), 5);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected(
      {{"k5", "v5"}, {"k4", "v4"}, {"k3", "v3"}, {"k2", "v2"}, {"k1", "v1"}}));

  // ***************** Step 2 *****************
  // (k3, v3) -> (k5, v5) -> (k4, v4) -> (k2, v2) -> (k1, v1);
  s = lru_cache.Lookup("k3", &value);
  ASSERT_EQ(lru_cache.Size(), 5);
  ASSERT_EQ(lru_cache.TotalCharge(), 5);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected(
      {{"k3", "v3"}, {"k5", "v5"}, {"k4", "v4"}, {"k2", "v2"}, {"k1", "v1"}}));

  // ***************** Step 3 *****************
  // (k1, v1) -> (k3, v3) -> (k5, v5) -> (k4, v4) -> (k2, v2);
  s = lru_cache.Lookup("k1", &value);
  ASSERT_EQ(lru_cache.Size(), 5);
  ASSERT_EQ(lru_cache.TotalCharge(), 5);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected(
      {{"k1", "v1"}, {"k3", "v3"}, {"k5", "v5"}, {"k4", "v4"}, {"k2", "v2"}}));

  // ***************** Step 4 *****************
  // (k4, v4) -> (k1, v1) -> (k3, v3) -> (k5, v5) -> (k2, v2);
  s = lru_cache.Lookup("k4", &value);
  ASSERT_EQ(lru_cache.Size(), 5);
  ASSERT_EQ(lru_cache.TotalCharge(), 5);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected(
      {{"k4", "v4"}, {"k1", "v1"}, {"k3", "v3"}, {"k5", "v5"}, {"k2", "v2"}}));

  // ***************** Step 5 *****************
  // (k5, v5) -> (k4, v4) -> (k1, v1) -> (k3, v3) -> (k2, v2);
  s = lru_cache.Lookup("k5", &value);
  ASSERT_EQ(lru_cache.Size(), 5);
  ASSERT_EQ(lru_cache.TotalCharge(), 5);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected(
      {{"k5", "v5"}, {"k4", "v4"}, {"k1", "v1"}, {"k3", "v3"}, {"k2", "v2"}}));

  // ***************** Step 6 *****************
  // (k5, v5) -> (k4, v4) -> (k1, v1) -> (k3, v3) -> (k2, v2);
  s = lru_cache.Lookup("k5", &value);
  ASSERT_EQ(lru_cache.Size(), 5);
  ASSERT_EQ(lru_cache.TotalCharge(), 5);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected(
      {{"k5", "v5"}, {"k4", "v4"}, {"k1", "v1"}, {"k3", "v3"}, {"k2", "v2"}}));
}

TEST(LRUCacheTest, TestInsertCase1) {
  Status s;
  storage::LRUCache<std::string, std::string> lru_cache;
  lru_cache.SetCapacity(3);

  // ***************** Step 1 *****************
  // (k1, v1)
  s = lru_cache.Insert("k1", "v1");
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(lru_cache.Size(), 1);
  ASSERT_EQ(lru_cache.TotalCharge(), 1);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected({{"k1", "v1"}}));

  // ***************** Step 2 *****************
  // (k2, v2) -> (k1, v1)
  s = lru_cache.Insert("k2", "v2");
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(lru_cache.Size(), 2);
  ASSERT_EQ(lru_cache.TotalCharge(), 2);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected({{"k2", "v2"}, {"k1", "v1"}}));

  // ***************** Step 3 *****************
  // (k3, v3) -> (k2, v2) -> (k1, v1)
  s = lru_cache.Insert("k3", "v3");
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(lru_cache.Size(), 3);
  ASSERT_EQ(lru_cache.TotalCharge(), 3);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(
      lru_cache.LRUAsExpected({{"k3", "v3"}, {"k2", "v2"}, {"k1", "v1"}}));

  // ***************** Step 4 *****************
  // (k4, v4) -> (k3, v3) -> (k2, v2)
  s = lru_cache.Insert("k4", "v4");
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(lru_cache.Size(), 3);
  ASSERT_EQ(lru_cache.TotalCharge(), 3);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(
      lru_cache.LRUAsExpected({{"k4", "v4"}, {"k3", "v3"}, {"k2", "v2"}}));

  // ***************** Step 5 *****************
  // (k5, v5) -> (k4, v4) -> (k3, v3)
  s = lru_cache.Insert("k5", "v5");
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(lru_cache.Size(), 3);
  ASSERT_EQ(lru_cache.TotalCharge(), 3);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(
      lru_cache.LRUAsExpected({{"k5", "v5"}, {"k4", "v4"}, {"k3", "v3"}}));
}

TEST(LRUCacheTest, TestInsertCase2) {
  Status s;
  storage::LRUCache<std::string, std::string> lru_cache;
  lru_cache.SetCapacity(5);

  // ***************** Step 1 *****************
  // (k5, v5) -> (k4, v4) -> (k3, v3) -> (k2, v2) -> (k1, v1)
  lru_cache.Insert("k1", "v1");
  lru_cache.Insert("k2", "v2");
  lru_cache.Insert("k3", "v3");
  lru_cache.Insert("k4", "v4");
  lru_cache.Insert("k5", "v5");
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(lru_cache.Size(), 5);
  ASSERT_EQ(lru_cache.TotalCharge(), 5);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected(
      {{"k5", "v5"}, {"k4", "v4"}, {"k3", "v3"}, {"k2", "v2"}, {"k1", "v1"}}));

  // ***************** Step 2 *****************
  // (k3, v3) -> (k5, v5) -> (k4, v4) -> (k2, v2) -> (k1, v1)
  s = lru_cache.Insert("k3", "v3");
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(lru_cache.Size(), 5);
  ASSERT_EQ(lru_cache.TotalCharge(), 5);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected(
      {{"k3", "v3"}, {"k5", "v5"}, {"k4", "v4"}, {"k2", "v2"}, {"k1", "v1"}}));

  // ***************** Step 3 *****************
  // (k2, v2) -> (k3, v3) -> (k5, v5) -> (k4, v4) -> (k1, v1)
  s = lru_cache.Insert("k2", "v2");
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(lru_cache.Size(), 5);
  ASSERT_EQ(lru_cache.TotalCharge(), 5);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected(
      {{"k2", "v2"}, {"k3", "v3"}, {"k5", "v5"}, {"k4", "v4"}, {"k1", "v1"}}));

  // ***************** Step 4 *****************
  // (k1, v1) -> (k2, v2) -> (k3, v3) -> (k5, v5) -> (k4, v4)
  s = lru_cache.Insert("k1", "v1");
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(lru_cache.Size(), 5);
  ASSERT_EQ(lru_cache.TotalCharge(), 5);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected(
      {{"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}, {"k5", "v5"}, {"k4", "v4"}}));

  // ***************** Step 5 *****************
  // (k4, v4) -> (k1, v1) -> (k2, v2) -> (k3, v3) -> (k5, v5)
  s = lru_cache.Insert("k4", "v4");
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(lru_cache.Size(), 5);
  ASSERT_EQ(lru_cache.TotalCharge(), 5);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected(
      {{"k4", "v4"}, {"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}, {"k5", "v5"}}));

  // ***************** Step 6 *****************
  // (k4, v4) -> (k1, v1) -> (k2, v2) -> (k3, v3) -> (k5, v5)
  s = lru_cache.Insert("k4", "v4");
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(lru_cache.Size(), 5);
  ASSERT_EQ(lru_cache.TotalCharge(), 5);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected(
      {{"k4", "v4"}, {"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}, {"k5", "v5"}}));

  // ***************** Step 6 *****************
  // (k4, v4) -> (k1, v1) -> (k2, v2) -> (k3, v3) -> (k5, v5)
  s = lru_cache.Insert("k0", "v0");
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(lru_cache.Size(), 5);
  ASSERT_EQ(lru_cache.TotalCharge(), 5);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected(
      {{"k0", "v0"}, {"k4", "v4"}, {"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}}));
}

TEST(LRUCacheTest, TestInsertCase3) {
  Status s;
  storage::LRUCache<std::string, std::string> lru_cache;
  lru_cache.SetCapacity(10);

  // ***************** Step 1 *****************
  // (k1, v1)
  s = lru_cache.Insert("k1", "v1");
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(lru_cache.Size(), 1);
  ASSERT_EQ(lru_cache.TotalCharge(), 1);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected({{"k1", "v1"}}));

  // ***************** Step 2 *****************
  // (k2, v2) -> (k1, v1)
  s = lru_cache.Insert("k2", "v2", 2);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(lru_cache.Size(), 2);
  ASSERT_EQ(lru_cache.TotalCharge(), 3);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected({{"k2", "v2"}, {"k1", "v1"}}));

  // ***************** Step 3 *****************
  // (k3, v3) -> (k2, v1) -> (k1, v1)
  s = lru_cache.Insert("k3", "v3", 3);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(lru_cache.Size(), 3);
  ASSERT_EQ(lru_cache.TotalCharge(), 6);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(
      lru_cache.LRUAsExpected({{"k3", "v3"}, {"k2", "v2"}, {"k1", "v1"}}));

  // ***************** Step 4 *****************
  // (k4, v4) -> (k3, v3) -> (k2, v2) -> (k1, v1)
  s = lru_cache.Insert("k4", "v4", 4);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(lru_cache.Size(), 4);
  ASSERT_EQ(lru_cache.TotalCharge(), 10);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected(
      {{"k4", "v4"}, {"k3", "v3"}, {"k2", "v2"}, {"k1", "v1"}}));

  // ***************** Step 5 *****************
  // (k5, v5) -> (k4, v4)
  s = lru_cache.Insert("k5", "v5", 5);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(lru_cache.Size(), 2);
  ASSERT_EQ(lru_cache.TotalCharge(), 9);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected({{"k5", "v5"}, {"k4", "v4"}}));

  // ***************** Step 6 *****************
  // (k6, v6)
  s = lru_cache.Insert("k6", "v6", 6);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(lru_cache.Size(), 1);
  ASSERT_EQ(lru_cache.TotalCharge(), 6);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected({{"k6", "v6"}}));
}

TEST(LRUCacheTest, TestInsertCase4) {
  Status s;
  storage::LRUCache<std::string, std::string> lru_cache;
  lru_cache.SetCapacity(10);

  // ***************** Step 1 *****************
  // (k4, v4) -> (k3, v3) -> (k2, v2) -> (k1, v1)
  lru_cache.Insert("k1", "v1", 1);
  lru_cache.Insert("k2", "v2", 2);
  lru_cache.Insert("k3", "v3", 3);
  lru_cache.Insert("k4", "v4", 4);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(lru_cache.Size(), 4);
  ASSERT_EQ(lru_cache.TotalCharge(), 10);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected(
      {{"k4", "v4"}, {"k3", "v3"}, {"k2", "v2"}, {"k1", "v1"}}));

  // ***************** Step 2 *****************
  // empty
  lru_cache.Insert("k11", "v11", 11);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(lru_cache.Size(), 0);
  ASSERT_EQ(lru_cache.TotalCharge(), 0);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected({}));

  // ***************** Step 3 *****************
  // empty
  lru_cache.Insert("k11", "v11", 11);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(lru_cache.Size(), 0);
  ASSERT_EQ(lru_cache.TotalCharge(), 0);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected({}));

  // ***************** Step 4 *****************
  // (k4, v4) -> (k3, v3) -> (k2, v2) -> (k1, v1)
  lru_cache.Insert("k1", "v1", 1);
  lru_cache.Insert("k2", "v2", 2);
  lru_cache.Insert("k3", "v3", 3);
  lru_cache.Insert("k4", "v4", 4);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(lru_cache.Size(), 4);
  ASSERT_EQ(lru_cache.TotalCharge(), 10);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected(
      {{"k4", "v4"}, {"k3", "v3"}, {"k2", "v2"}, {"k1", "v1"}}));

  // ***************** Step 5 *****************
  // (k5, k5) -> (k4, v4)
  lru_cache.Insert("k5", "v5", 5);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(lru_cache.Size(), 2);
  ASSERT_EQ(lru_cache.TotalCharge(), 9);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected({{"k5", "v5"}, {"k4", "v4"}}));

  // ***************** Step 6 *****************
  // (k1, v1) -> (k5, k5) -> (k4, v4)
  lru_cache.Insert("k1", "v1", 1);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(lru_cache.Size(), 3);
  ASSERT_EQ(lru_cache.TotalCharge(), 10);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(
      lru_cache.LRUAsExpected({{"k1", "v1"}, {"k5", "v5"}, {"k4", "v4"}}));

  // ***************** Step 7 *****************
  // (k5, v5) -> (k1, k1) -> (k4, v4)
  lru_cache.Insert("k5", "v5", 5);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(lru_cache.Size(), 3);
  ASSERT_EQ(lru_cache.TotalCharge(), 10);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(
      lru_cache.LRUAsExpected({{"k5", "v5"}, {"k1", "v1"}, {"k4", "v4"}}));

  // ***************** Step 8 *****************
  // (k6, v6)
  lru_cache.Insert("k6", "v6", 6);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(lru_cache.Size(), 1);
  ASSERT_EQ(lru_cache.TotalCharge(), 6);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected({{"k6", "v6"}}));

  // ***************** Step 8 *****************
  // (k2, v2) -> (k6, v6)
  lru_cache.Insert("k2", "v2", 2);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(lru_cache.Size(), 2);
  ASSERT_EQ(lru_cache.TotalCharge(), 8);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected({{"k2", "v2"}, {"k6", "v6"}}));

  // ***************** Step 9 *****************
  // (k1, v1) -> (k2, v2) -> (k6, v6)
  lru_cache.Insert("k1", "v1", 1);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(lru_cache.Size(), 3);
  ASSERT_EQ(lru_cache.TotalCharge(), 9);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(
      lru_cache.LRUAsExpected({{"k1", "v1"}, {"k2", "v2"}, {"k6", "v6"}}));

  // ***************** Step 10 *****************
  // (k3, v3) -> (k1, v1) -> (k2, v2)
  lru_cache.Insert("k3", "v3", 3);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(lru_cache.Size(), 3);
  ASSERT_EQ(lru_cache.TotalCharge(), 6);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(
      lru_cache.LRUAsExpected({{"k3", "v3"}, {"k1", "v1"}, {"k2", "v2"}}));
}

TEST(LRUCacheTest, TestRemoveCase1) {
  Status s;
  storage::LRUCache<std::string, std::string> lru_cache;
  lru_cache.SetCapacity(5);

  // ***************** Step 1 *****************
  // (k5, v5) -> (k4, v4) -> (k3, v3) -> (k2, v2) -> (k1, v1);
  lru_cache.Insert("k1", "v1");
  lru_cache.Insert("k2", "v2");
  lru_cache.Insert("k3", "v3");
  lru_cache.Insert("k4", "v4");
  lru_cache.Insert("k5", "v5");
  ASSERT_EQ(lru_cache.Size(), 5);
  ASSERT_EQ(lru_cache.TotalCharge(), 5);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected(
      {{"k5", "v5"}, {"k4", "v4"}, {"k3", "v3"}, {"k2", "v2"}, {"k1", "v1"}}));

  // ***************** Step 2 *****************
  // (k4, v4) -> (k3, v3) -> (k2, v2) -> (k1, v1);
  s = lru_cache.Remove("k5");
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(lru_cache.Size(), 4);
  ASSERT_EQ(lru_cache.TotalCharge(), 4);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected(
      {{"k4", "v4"}, {"k3", "v3"}, {"k2", "v2"}, {"k1", "v1"}}));

  // ***************** Step 3 *****************
  // (k4, v4) -> (k3, v3) -> (k2, v2)
  s = lru_cache.Remove("k1");
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(lru_cache.Size(), 3);
  ASSERT_EQ(lru_cache.TotalCharge(), 3);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(
      lru_cache.LRUAsExpected({{"k4", "v4"}, {"k3", "v3"}, {"k2", "v2"}}));

  // ***************** Step 4 *****************
  // (k4, v4) -> (k2, v2)
  s = lru_cache.Remove("k3");
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(lru_cache.Size(), 2);
  ASSERT_EQ(lru_cache.TotalCharge(), 2);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected({{"k4", "v4"}, {"k2", "v2"}}));

  // ***************** Step 5 *****************
  // (k4, v4)
  s = lru_cache.Remove("k2");
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(lru_cache.Size(), 1);
  ASSERT_EQ(lru_cache.TotalCharge(), 1);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected({{"k4", "v4"}}));

  // ***************** Step 6 *****************
  // empty
  s = lru_cache.Remove("k4");
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(lru_cache.Size(), 0);
  ASSERT_EQ(lru_cache.TotalCharge(), 0);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected({}));

  // ***************** Step 7 *****************
  // empty
  s = lru_cache.Remove("k4");
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(lru_cache.Size(), 0);
  ASSERT_EQ(lru_cache.TotalCharge(), 0);
  ASSERT_TRUE(lru_cache.LRUAndHandleTableConsistent());
  ASSERT_TRUE(lru_cache.LRUAsExpected({}));
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
