//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <gtest/gtest.h>
#include <thread>
#include <iostream>

#include "storage/storage.h"
#include "src/strings_filter.h"

using namespace storage;

// Filter
TEST(StringsFilterTest, FilterTest) {
  std::string new_value;
  bool is_stale, value_changed;
  StringsFilter* filter = new StringsFilter;

  int32_t ttl = 1;
  StringsValue strings_value("FILTER_VALUE");
  strings_value.SetRelativeTimestamp(ttl);
  is_stale = filter->Filter(0, "FILTER_KEY",
          strings_value.Encode(), &new_value, &value_changed);
  ASSERT_FALSE(is_stale);
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  is_stale = filter->Filter(0, "FILTER_KEY",
          strings_value.Encode(), &new_value, &value_changed);
  ASSERT_TRUE(is_stale);

  delete filter;
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

