//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <gtest/gtest.h>
#include <iostream>
#include <thread>

#include "pstd/include/pika_conf.h"
#include "src/strings_filter.h"
#include "storage/storage.h"

using namespace storage;

std::unique_ptr<PikaConf> g_pika_conf;

// Filter
TEST(StringsFilterTest, FilterTest) {
  std::string new_value;
  bool is_stale;
  bool value_changed;
  auto filter = std::make_unique<StringsFilter>();

  int32_t ttl = 1;
  StringsValue strings_value("FILTER_VALUE");
  strings_value.SetRelativeTimestamp(ttl);
  is_stale = filter->Filter(0, "FILTER_KEY", strings_value.Encode(), &new_value, &value_changed);
  ASSERT_FALSE(is_stale);
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  is_stale = filter->Filter(0, "FILTER_KEY", strings_value.Encode(), &new_value, &value_changed);
  ASSERT_TRUE(is_stale);
}

int main(int argc, char** argv) {
  std::string pika_conf_path = "./pika.conf";
#ifdef PIKA_ROOT_DIR
  pika_conf_path = PIKA_ROOT_DIR;
  pika_conf_path += "/tests/conf/pika.conf";
#endif
  LOG(WARNING) << "pika_conf_path: " << pika_conf_path;
  g_pika_conf = std::make_unique<PikaConf>(pika_conf_path);
  if (g_pika_conf->Load()) {
    printf("pika load conf error\n");
    return 0;
  }
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
