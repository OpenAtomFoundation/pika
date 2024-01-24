//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <gtest/gtest.h>
#include <iostream>
#include <unordered_map>

#include "pstd/include/pika_conf.h"
#include "storage/storage.h"

using namespace storage;

std::unique_ptr<PikaConf> g_pika_conf;

class StorageOptionsTest : public ::testing::Test {
 public:
  StorageOptionsTest() = default;
  ~StorageOptionsTest() override = default;

  static void SetUpTestSuite() {}
  static void TearDownTestSuite() {}

  StorageOptions storage_options;
  storage::Status s;
};

// ResetOptions
TEST_F(StorageOptionsTest, ResetOptionsTest) {
  std::unordered_map<std::string, std::string> cf_options_map{{"write_buffer_size", "4096"},
                                                              {"max_write_buffer_number", "10"}};
  s = storage_options.ResetOptions(OptionType::kColumnFamily, cf_options_map);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(storage_options.options.write_buffer_size, 4096);
  ASSERT_EQ(storage_options.options.max_write_buffer_number, 10);

  std::unordered_map<std::string, std::string> invalid_cf_options_map{{"write_buffer_size", "abc"},
                                                                      {"max_write_buffer_number", "0x33"}};
  s = storage_options.ResetOptions(OptionType::kColumnFamily, invalid_cf_options_map);
  ASSERT_FALSE(s.ok());
  ASSERT_EQ(storage_options.options.write_buffer_size, 4096);
  ASSERT_EQ(storage_options.options.max_write_buffer_number, 10);

  std::unordered_map<std::string, std::string> db_options_map{{"max_open_files", "16"},
                                                              {"max_background_compactions", "32"}};
  s = storage_options.ResetOptions(OptionType::kDB, db_options_map);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(storage_options.options.max_open_files, 16);
  ASSERT_EQ(storage_options.options.max_background_compactions, 32);

  std::unordered_map<std::string, std::string> invalid_db_options_map{{"max_open_files", "a"},
                                                                      {"max_background_compactions", "bac"}};
  s = storage_options.ResetOptions(OptionType::kDB, invalid_db_options_map);
  ASSERT_FALSE(s.ok());
  ASSERT_EQ(storage_options.options.max_open_files, 16);
  ASSERT_EQ(storage_options.options.max_background_compactions, 32);
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
