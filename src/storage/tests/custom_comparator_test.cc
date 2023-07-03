//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "src/custom_comparator.h"

#include <gtest/gtest.h>

#include <iostream>
#include <thread>

#include "src/redis.h"
#include "src/zsets_data_key_format.h"
#include "storage/storage.h"

using namespace storage;

// FindShortestSeparator
TEST(ZSetScoreKeyComparator, FindShortestSeparatorTest) {
  ZSetsScoreKeyComparatorImpl impl;

  // ***************** Group 1 Test *****************
  ZSetsScoreKey zsets_score_key_start_1("Axlgrep", 1557212501, 3.1415, "abc");
  ZSetsScoreKey zsets_score_key_limit_1("Axlgreq", 1557212501, 3.1415, "abc");
  std::string start_1 = zsets_score_key_start_1.Encode().ToString();
  std::string limit_1 = zsets_score_key_limit_1.Encode().ToString();
  std::string change_start_1 = start_1;
  impl.FindShortestSeparator(&change_start_1, Slice(limit_1));
  // impl.ParseAndPrintZSetsScoreKey("origin  start : ", start_1);
  // impl.ParseAndPrintZSetsScoreKey("changed start : ", change_start_1);
  // impl.ParseAndPrintZSetsScoreKey("limit         : ", limit_1);
  // printf("**********************************************************************\n");
  ASSERT_TRUE(impl.Compare(change_start_1, start_1) >= 0);
  ASSERT_TRUE(impl.Compare(change_start_1, limit_1) < 0);

  // ***************** Group 2 Test *****************
  ZSetsScoreKey zsets_score_key_start_2("Axlgrep", 1557212501, 3.1314, "abc");
  ZSetsScoreKey zsets_score_key_limit_2("Axlgrep", 1557212502, 3.1314, "abc");
  std::string start_2 = zsets_score_key_start_2.Encode().ToString();
  std::string limit_2 = zsets_score_key_limit_2.Encode().ToString();
  std::string change_start_2 = start_2;
  impl.FindShortestSeparator(&change_start_2, Slice(limit_2));
  // impl.ParseAndPrintZSetsScoreKey("origin  start : ", start_2);
  // impl.ParseAndPrintZSetsScoreKey("changed start : ", change_start_2);
  // impl.ParseAndPrintZSetsScoreKey("limit         : ", limit_2);
  // printf("**********************************************************************\n");
  ASSERT_TRUE(impl.Compare(change_start_2, start_2) >= 0);
  ASSERT_TRUE(impl.Compare(change_start_2, limit_2) < 0);

  // ***************** Group 3 Test *****************
  ZSetsScoreKey zsets_score_key_start_3("Axlgrep", 1557212501, 3.1415, "abc");
  ZSetsScoreKey zsets_score_key_limit_3("Axlgrep", 1557212501, 4.1415, "abc");
  std::string start_3 = zsets_score_key_start_3.Encode().ToString();
  std::string limit_3 = zsets_score_key_limit_3.Encode().ToString();
  std::string change_start_3 = start_3;
  impl.FindShortestSeparator(&change_start_3, Slice(limit_3));
  // impl.ParseAndPrintZSetsScoreKey("origin  start : ", start_3);
  // impl.ParseAndPrintZSetsScoreKey("changed start : ", change_start_3);
  // impl.ParseAndPrintZSetsScoreKey("limit         : ", limit_3);
  // printf("**********************************************************************\n");
  ASSERT_TRUE(impl.Compare(change_start_3, start_3) >= 0);
  ASSERT_TRUE(impl.Compare(change_start_3, limit_3) < 0);

  // ***************** Group 4 Test *****************
  ZSetsScoreKey zsets_score_key_start_4("Axlgrep", 1557212501, 3.1415, "abc");
  ZSetsScoreKey zsets_score_key_limit_4("Axlgrep", 1557212501, 5.1415, "abc");
  std::string start_4 = zsets_score_key_start_4.Encode().ToString();
  std::string limit_4 = zsets_score_key_limit_4.Encode().ToString();
  std::string change_start_4 = start_4;
  impl.FindShortestSeparator(&change_start_4, Slice(limit_4));
  // impl.ParseAndPrintZSetsScoreKey("origin  start : ", start_4);
  // impl.ParseAndPrintZSetsScoreKey("changed start : ", change_start_4);
  // impl.ParseAndPrintZSetsScoreKey("limit         : ", limit_4);
  // printf("**********************************************************************\n");
  ASSERT_TRUE(impl.Compare(change_start_4, start_4) >= 0);
  ASSERT_TRUE(impl.Compare(change_start_4, limit_4) < 0);

  // ***************** Group 5 Test *****************
  ZSetsScoreKey zsets_score_key_start_5("Axlgrep", 1557212501, 3.1415, "abc");
  ZSetsScoreKey zsets_score_key_limit_5("Axlgrep", 1557212501, 3.1415, "abd");
  std::string start_5 = zsets_score_key_start_5.Encode().ToString();
  std::string limit_5 = zsets_score_key_limit_5.Encode().ToString();
  std::string change_start_5 = start_5;
  impl.FindShortestSeparator(&change_start_5, Slice(limit_5));
  // impl.ParseAndPrintZSetsScoreKey("origin  start : ", start_5);
  // impl.ParseAndPrintZSetsScoreKey("changed start : ", change_start_5);
  // impl.ParseAndPrintZSetsScoreKey("limit         : ", limit_5);
  // printf("**********************************************************************\n");
  ASSERT_TRUE(impl.Compare(change_start_5, start_5) >= 0);
  ASSERT_TRUE(impl.Compare(change_start_5, limit_5) < 0);

  // ***************** Group 6 Test *****************
  ZSetsScoreKey zsets_score_key_start_6("Axlgrep", 1557212501, 3.1415,
                                        "abccccccc");
  ZSetsScoreKey zsets_score_key_limit_6("Axlgrep", 1557212501, 3.1415, "abd");
  std::string start_6 = zsets_score_key_start_6.Encode().ToString();
  std::string limit_6 = zsets_score_key_limit_6.Encode().ToString();
  std::string change_start_6 = start_6;
  impl.FindShortestSeparator(&change_start_6, Slice(limit_6));
  // impl.ParseAndPrintZSetsScoreKey("origin  start : ", start_6);
  // impl.ParseAndPrintZSetsScoreKey("changed start : ", change_start_6);
  // impl.ParseAndPrintZSetsScoreKey("limit         : ", limit_6);
  // printf("**********************************************************************\n");
  ASSERT_TRUE(impl.Compare(change_start_6, start_6) >= 0);
  ASSERT_TRUE(impl.Compare(change_start_6, limit_6) < 0);

  // ***************** Group 7 Test *****************
  ZSetsScoreKey zsets_score_key_start_7("Axlgrep", 1557212501, 3.1415,
                                        "abcccaccc");
  ZSetsScoreKey zsets_score_key_limit_7("Axlgrep", 1557212501, 3.1415,
                                        "abccccccc");
  std::string start_7 = zsets_score_key_start_7.Encode().ToString();
  std::string limit_7 = zsets_score_key_limit_7.Encode().ToString();
  std::string change_start_7 = start_7;
  impl.FindShortestSeparator(&change_start_7, Slice(limit_7));
  // impl.ParseAndPrintZSetsScoreKey("origin  start : ", start_7);
  // impl.ParseAndPrintZSetsScoreKey("changed start : ", change_start_7);
  // impl.ParseAndPrintZSetsScoreKey("limit         : ", limit_7);
  // printf("**********************************************************************\n");
  ASSERT_TRUE(impl.Compare(change_start_7, start_7) >= 0);
  ASSERT_TRUE(impl.Compare(change_start_7, limit_7) < 0);

  // ***************** Group 8 Test *****************
  ZSetsScoreKey zsets_score_key_start_8("Axlgrep", 1557212501, 3.1415, "");
  ZSetsScoreKey zsets_score_key_limit_8("Axlgrep", 1557212501, 3.1415,
                                        "abccccccc");
  std::string start_8 = zsets_score_key_start_8.Encode().ToString();
  std::string limit_8 = zsets_score_key_limit_8.Encode().ToString();
  std::string change_start_8 = start_8;
  impl.FindShortestSeparator(&change_start_8, Slice(limit_8));
  // impl.ParseAndPrintZSetsScoreKey("origin  start : ", start_8);
  // impl.ParseAndPrintZSetsScoreKey("changed start : ", change_start_8);
  // impl.ParseAndPrintZSetsScoreKey("limit         : ", limit_8);
  // printf("**********************************************************************\n");
  ASSERT_TRUE(impl.Compare(change_start_8, start_8) >= 0);
  ASSERT_TRUE(impl.Compare(change_start_8, limit_8) < 0);

  // ***************** Group 9 Test *****************
  ZSetsScoreKey zsets_score_key_start_9("Axlgrep", 1557212501, 3.1415, "aaaa");
  ZSetsScoreKey zsets_score_key_limit_9("Axlgrep", 1557212501, 4.1415, "");
  std::string start_9 = zsets_score_key_start_9.Encode().ToString();
  std::string limit_9 = zsets_score_key_limit_9.Encode().ToString();
  std::string change_start_9 = start_9;
  impl.FindShortestSeparator(&change_start_9, Slice(limit_9));
  // impl.ParseAndPrintZSetsScoreKey("origin  start : ", start_9);
  // impl.ParseAndPrintZSetsScoreKey("changed start : ", change_start_9);
  // impl.ParseAndPrintZSetsScoreKey("limit         : ", limit_9);
  // printf("**********************************************************************\n");
  ASSERT_TRUE(impl.Compare(change_start_9, start_9) >= 0);
  ASSERT_TRUE(impl.Compare(change_start_9, limit_9) < 0);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
