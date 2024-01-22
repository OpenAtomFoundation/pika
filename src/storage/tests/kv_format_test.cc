//  Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <iostream>
#include <thread>

#include <gtest/gtest.h>
#include "glog/logging.h"

#include "src/debug.h"
#include "src/coding.h"
#include "src/base_key_format.h"
#include "src/base_data_key_format.h"
#include "src/zsets_data_key_format.h"
#include "src/lists_data_key_format.h"
#include "storage/storage_define.h"

using namespace storage;

TEST(KVFormatTest, BaseKeyFormat) {
  rocksdb::Slice slice_key("\u0000\u0001abc\u0000", 6);
  BaseKey bk(slice_key);

  rocksdb::Slice slice_enc = bk.Encode();
  std::string expect_enc(8, '\0');
  expect_enc.append("\u0000\u0001\u0001abc\u0000\u0001\u0000\u0000", 10);
  expect_enc.append(16, '\0');
  ASSERT_EQ(slice_enc, Slice(expect_enc));

  ParsedBaseKey pbk(slice_enc);
  ASSERT_EQ(pbk.Key(), slice_key);
}

TEST(KVFormatTest, BaseDataKeyFormat) {
  rocksdb::Slice slice_key("\u0000\u0001base_data_key\u0000", 16);
  rocksdb::Slice slice_data("\u0000\u0001data\u0000", 7);
  uint64_t version = 1701848429;

  BaseDataKey bdk(slice_key, version, slice_data);
  rocksdb::Slice seek_key_enc = bdk.EncodeSeekKey();
  std::string expect_enc(8, '\0');
  expect_enc.append("\u0000\u0001\u0001base_data_key\u0000\u0001\u0000\u0000", 20);
  char dst[9];
  EncodeFixed64(dst, version);
  expect_enc.append(dst, 8);
  expect_enc.append("\u0000\u0001data\u0000", 7);
  ASSERT_EQ(seek_key_enc, Slice(expect_enc));

  rocksdb::Slice key_enc = bdk.Encode();
  expect_enc.append(16, '\0');
  ASSERT_EQ(key_enc, Slice(expect_enc));

  ParsedBaseDataKey pbmk(key_enc);
  ASSERT_EQ(pbmk.Key(), slice_key);
  ASSERT_EQ(pbmk.Data(), slice_data);
  ASSERT_EQ(pbmk.Version(), version);
}

TEST(KVFormatTest, ZsetsScoreKeyFormat) {
  rocksdb::Slice slice_key("\u0000\u0001base_data_key\u0000", 16);
  rocksdb::Slice slice_data("\u0000\u0001data\u0000", 7);
  uint64_t version = 1701848429;
  double score = -3.5;

  ZSetsScoreKey zsk(slice_key, version, score, slice_data);
  // reserve
  std::string expect_enc(8, '\0');
  // user_key
  expect_enc.append("\u0000\u0001\u0001base_data_key\u0000\u0001\u0000\u0000", 20);
  // version
  char dst[9];
  EncodeFixed64(dst, version);
  expect_enc.append(dst, 8);
  // score
  const void* addr_score = reinterpret_cast<const void*>(&score);
  EncodeFixed64(dst, *reinterpret_cast<const uint64_t*>(addr_score));
  expect_enc.append(dst, 8);
  // data
  expect_enc.append("\u0000\u0001data\u0000", 7);
  // reserve
  expect_enc.append(16, '\0');
  rocksdb::Slice key_enc = zsk.Encode();
  ASSERT_EQ(key_enc, Slice(expect_enc));

  ParsedZSetsScoreKey pzsk(key_enc);
  ASSERT_EQ(pzsk.key(), slice_key);
  ASSERT_EQ(pzsk.member(), slice_data);
  ASSERT_EQ(pzsk.Version(), version);
  ASSERT_EQ(pzsk.score(), score);
}

TEST(KVFormatTest, ListDataKeyFormat) {
  rocksdb::Slice slice_key("\u0000\u0001list_data_key\u0000", 16);
  uint64_t version = 1701848429;
  uint64_t index = 10;

  ListsDataKey ldk(slice_key, version, index);
  rocksdb::Slice key_enc = ldk.Encode();
  std::string expect_enc(8, '\0');
  expect_enc.append("\u0000\u0001\u0001list_data_key\u0000\u0001\u0000\u0000", 20);
  char dst[9];
  EncodeFixed64(dst, version);
  expect_enc.append(dst, 8);
  EncodeFixed64(dst, index);
  expect_enc.append(dst, 8);
  expect_enc.append(16, '\0');
  ASSERT_EQ(key_enc, Slice(expect_enc));

  ParsedListsDataKey pldk(key_enc);
  ASSERT_EQ(pldk.key(), slice_key);
  ASSERT_EQ(pldk.index(), index);
  ASSERT_EQ(pldk.Version(), version);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
