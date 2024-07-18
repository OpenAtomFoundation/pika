//  Copyright (c) 2024-present, Qihoo, Inc.  All rights reserved.
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
#include "src/redis_streams.h"

using namespace storage;

class StreamsTest : public ::testing::Test {
 public:
  StreamsTest() = default;
  ~StreamsTest() override = default;

  void SetUp() override {
    std::string path = "./db/streams";
    if (access("./db", F_OK) != 0) {
      mkdir("./db", 0755);
    }
    if (access(path.c_str(), F_OK) != 0) {
      mkdir(path.c_str(), 0755);
    }
    db = new RedisStreams(nullptr, storage::kStreams);
    storage_options.options.create_if_missing = true;
    db->Open(storage_options, path);
  }

  void TearDown() override {
    std::string path = "./db/streams";
    DeleteFiles(path.c_str());
  }

  static void SetUpTestSuite() {}
  static void TearDownTestSuite() {}

  StorageOptions storage_options;
  RedisStreams* db;
};

TEST_F(StreamsTest, DataFilter) {
  int32_t ret = 0;
  rocksdb::ReadOptions r_opts;
  rocksdb::FlushOptions f_opts;
  rocksdb::CompactRangeOptions c_opts;
  storage::StreamAddTrimArgs args;

  rocksdb::DB* rocks_db = db->GetDB();
  auto handles = db->GetHandles();

  auto s = db->XAdd("STREAM_KEY_0", "STREAM_MESSAGE_1", args);
  ASSERT_TRUE(s.ok());

  rocks_db->Flush(f_opts, handles);
  auto iter = rocks_db->NewIterator(r_opts, handles[1]);
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  delete iter;

  s = db->Del("STREAM_KEY_0");
  ASSERT_TRUE(s.ok());

  rocks_db->Flush(f_opts, handles);
  rocks_db->CompactRange(c_opts, handles[1], nullptr, nullptr);
  iter = rocks_db->NewIterator(r_opts, handles[1]);
  iter->SeekToFirst();
  ASSERT_FALSE(iter->Valid());
  delete iter;
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

