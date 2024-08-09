//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <gtest/gtest.h>
#include <iostream>
#include <thread>

#include "src/base_key_format.h"
#include "src/lists_filter.h"
#include "src/redis.h"
#include "src/zsets_filter.h"
#include "storage/storage.h"

using namespace storage;
using storage::EncodeFixed64;
using storage::ListsDataFilter;
using storage::ListsDataKey;
using storage::ListsMetaValue;
using storage::Slice;
using storage::Status;

class ListsFilterTest : public ::testing::Test {
 public:
  ListsFilterTest() {
    std::string db_path = "./db/list_meta";
    if (access(db_path.c_str(), F_OK) != 0) {
      mkdir(db_path.c_str(), 0755);
    }
    options.create_if_missing = true;
    s = rocksdb::DB::Open(options, db_path, &meta_db);
    if (s.ok()) {
      // create column family
      rocksdb::ColumnFamilyHandle* cf;
      s = meta_db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "data_cf", &cf);
      delete cf;
      delete meta_db;
    }

    rocksdb::ColumnFamilyOptions meta_cf_ops(options);
    rocksdb::ColumnFamilyOptions data_cf_ops(options);

    // Meta CF
    column_families.emplace_back(rocksdb::kDefaultColumnFamilyName, meta_cf_ops);
    // Data CF
    column_families.emplace_back("data_cf", data_cf_ops);

    s = rocksdb::DB::Open(options, db_path, column_families, &handles, &meta_db);
  }
  ~ListsFilterTest() override = default;

  void SetUp() override {}
  void TearDown() override {
    for (auto handle : handles) {
      delete handle;
    }
    delete meta_db;
  }

  storage::Options options;
  rocksdb::DB* meta_db;
  storage::Status s;

  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
};

// Data Filter
TEST_F(ListsFilterTest, DataFilterTest) {
  char str[8];
  char buf[4];
  bool filter_result;
  bool value_changed;
  uint64_t version = 0;
  std::string new_value;

  // Timeout timestamp is not set, the version is valid.
  auto lists_data_filter1 = std::make_unique<ListsDataFilter>(meta_db, &handles, DataType::kLists);
  ASSERT_TRUE(lists_data_filter1 != nullptr);

  EncodeFixed64(str, 1);
  ListsMetaValue lists_meta_value1(Slice(str, sizeof(uint64_t)));
  version = lists_meta_value1.UpdateVersion();

  std::string user_key = "FILTER_TEST_KEY";
  BaseMetaKey bmk(user_key);
  s = meta_db->Put(rocksdb::WriteOptions(), handles[0], bmk.Encode(), lists_meta_value1.Encode());
  ASSERT_TRUE(s.ok());

  ListsDataKey lists_data_key1(user_key, version, 1);
  filter_result =
      lists_data_filter1->Filter(0, lists_data_key1.Encode(), "FILTER_TEST_VALUE", &new_value, &value_changed);
  ASSERT_EQ(filter_result, false);
  s = meta_db->Delete(rocksdb::WriteOptions(), handles[0], "FILTER_TEST_KEY");
  ASSERT_TRUE(s.ok());

  // Timeout timestamp is set, but not expired.
  auto lists_data_filter2 = std::make_unique<ListsDataFilter>(meta_db, &handles, DataType::kLists);
  ASSERT_TRUE(lists_data_filter2 != nullptr);

  EncodeFixed64(str, 1);
  ListsMetaValue lists_meta_value2(Slice(str, sizeof(uint64_t)));
  version = lists_meta_value2.UpdateVersion();
  lists_meta_value2.SetRelativeTimeInMillsec(1);
  s = meta_db->Put(rocksdb::WriteOptions(), handles[0], bmk.Encode(), lists_meta_value2.Encode());
  ASSERT_TRUE(s.ok());
  ListsDataKey lists_data_key2("FILTER_TEST_KEY", version, 1);
  filter_result =
      lists_data_filter2->Filter(0, lists_data_key2.Encode(), "FILTER_TEST_VALUE", &new_value, &value_changed);
  ASSERT_EQ(filter_result, false);
  s = meta_db->Delete(rocksdb::WriteOptions(), handles[0], bmk.Encode());
  ASSERT_TRUE(s.ok());

  // Timeout timestamp is set, already expired.
  auto lists_data_filter3 = std::make_unique<ListsDataFilter>(meta_db, &handles, DataType::kLists);
  ASSERT_TRUE(lists_data_filter3 != nullptr);

  EncodeFixed64(str, 1);
  ListsMetaValue lists_meta_value3(Slice(str, sizeof(uint64_t)));
  version = lists_meta_value3.UpdateVersion();
  lists_meta_value3.SetRelativeTimeInMillsec(1);
  s = meta_db->Put(rocksdb::WriteOptions(), handles[0], bmk.Encode(), lists_meta_value3.Encode());
  ASSERT_TRUE(s.ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  ListsDataKey lists_data_key3("FILTER_TEST_KEY", version, 1);
  filter_result =
      lists_data_filter3->Filter(0, lists_data_key3.Encode(), "FILTER_TEST_VALUE", &new_value, &value_changed);
  ASSERT_EQ(filter_result, true);
  s = meta_db->Delete(rocksdb::WriteOptions(), handles[0], bmk.Encode());
  ASSERT_TRUE(s.ok());

  // Timeout timestamp is not set, the version is invalid
  auto lists_data_filter4 = std::make_unique<ListsDataFilter>(meta_db, &handles, DataType::kLists);
  ASSERT_TRUE(lists_data_filter4 != nullptr);

  EncodeFixed64(str, 1);
  ListsMetaValue lists_meta_value4(Slice(str, sizeof(uint64_t)));
  version = lists_meta_value4.UpdateVersion();
  s = meta_db->Put(rocksdb::WriteOptions(), handles[0], bmk.Encode(), lists_meta_value4.Encode());
  ASSERT_TRUE(s.ok());
  ListsDataKey lists_data_key4("FILTER_TEST_KEY", version, 1);
  version = lists_meta_value4.UpdateVersion();
  s = meta_db->Put(rocksdb::WriteOptions(), handles[0], bmk.Encode(), lists_meta_value4.Encode());
  ASSERT_TRUE(s.ok());
  filter_result =
      lists_data_filter4->Filter(0, lists_data_key4.Encode(), "FILTER_TEST_VALUE", &new_value, &value_changed);
  ASSERT_EQ(filter_result, true);
  s = meta_db->Delete(rocksdb::WriteOptions(), handles[0], bmk.Encode());
  ASSERT_TRUE(s.ok());

  // Meta data has been clear
  auto lists_data_filter5 = std::make_unique<ListsDataFilter>(meta_db, &handles, DataType::kLists);
  ASSERT_TRUE(lists_data_filter5 != nullptr);

  EncodeFixed64(str, 1);
  ListsMetaValue lists_meta_value5(Slice(str, sizeof(uint64_t)));
  version = lists_meta_value5.UpdateVersion();
  s = meta_db->Put(rocksdb::WriteOptions(), handles[0], bmk.Encode(), lists_meta_value5.Encode());
  ASSERT_TRUE(s.ok());
  ListsDataKey lists_data_value5("FILTER_TEST_KEY", version, 1);
  s = meta_db->Delete(rocksdb::WriteOptions(), handles[0], bmk.Encode());
  ASSERT_TRUE(s.ok());
  filter_result =
      lists_data_filter5->Filter(0, lists_data_value5.Encode(), "FILTER_TEST_VALUE", &new_value, &value_changed);
  ASSERT_EQ(filter_result, true);

  /*
   * The types of keys conflict with each other and trigger compaction, zset filter
   */
  BaseMetaKey meta_key(user_key);
  auto zset_filter = std::make_unique<ZSetsScoreFilter>(meta_db, &handles, DataType::kZSets);
  ASSERT_TRUE(zset_filter != nullptr);

  // Insert a zset key
  EncodeFixed32(buf, 1);
  ZSetsMetaValue zsets_meta_value(DataType::kZSets, Slice(buf, 4));
  version = zsets_meta_value.UpdateVersion();
  s = meta_db->Put(rocksdb::WriteOptions(), handles[0], meta_key.Encode(), zsets_meta_value.Encode());
  ASSERT_TRUE(s.ok());

  // Insert a key of type string with the same name as the list
  StringsValue strings_value("FILTER_TEST_VALUE");
  s = meta_db->Put(rocksdb::WriteOptions(), meta_key.Encode(), strings_value.Encode());

  // zset-filter was used for elimination detection
  ZSetsScoreKey base_key(user_key, version, 1, "FILTER_TEST_KEY");
  filter_result = zset_filter->Filter(0, base_key.Encode(), "FILTER_TEST_VALUE", &new_value, &value_changed);
  ASSERT_EQ(filter_result, true);
  s = meta_db->Delete(rocksdb::WriteOptions(), handles[0], "FILTER_TEST_KEY");
  ASSERT_TRUE(s.ok());

  /*
   * The types of keys conflict with each other and trigger compaction, list filter
   */
  auto lists_data_filter = std::make_unique<ListsDataFilter>(meta_db, &handles, DataType::kLists);
  ASSERT_TRUE(lists_data_filter != nullptr);

  // Insert a list key
  EncodeFixed64(str, 1);
  ListsMetaValue lists_meta_value(Slice(str, sizeof(uint64_t)));
  lists_meta_value.UpdateVersion();
  s = meta_db->Put(rocksdb::WriteOptions(), handles[0], meta_key.Encode(), lists_meta_value.Encode());
  ASSERT_TRUE(s.ok());

  // Insert a key of type set with the same name as the list
  EncodeFixed32(buf, 1);
  SetsMetaValue sets_meta_value(DataType::kSets, Slice(str, 4));
  sets_meta_value.UpdateVersion();
  s = meta_db->Put(rocksdb::WriteOptions(), handles[0], meta_key.Encode(), sets_meta_value.Encode());
  ASSERT_TRUE(s.ok());

  // list-filter was used for elimination detection
  ListsDataKey lists_data_key(user_key, version, 1);
  filter_result = lists_data_filter->Filter(0, lists_data_key.Encode(), "FILTER_TEST_VALUE", &new_value, &value_changed);
  ASSERT_EQ(filter_result, true);
  s = meta_db->Delete(rocksdb::WriteOptions(), handles[0], "FILTER_TEST_KEY");
  ASSERT_TRUE(s.ok());

  /*
   *  The types of keys conflict with each other and trigger compaction, base filter
   */
  auto base_filter = std::make_unique<BaseDataFilter>(meta_db, &handles, DataType::kHashes);
  ASSERT_TRUE(lists_data_filter != nullptr);

  // Insert a hash key
  EncodeFixed32(buf, 1);
  HashesMetaValue hash_meta_value(DataType::kHashes, Slice(buf, 4));
  hash_meta_value.UpdateVersion();
  s = meta_db->Put(rocksdb::WriteOptions(), handles[0], meta_key.Encode(), hash_meta_value.Encode());
  ASSERT_TRUE(s.ok());

  // Insert a key of type list with the same name as the hash
  EncodeFixed64(str, 1);
  ListsMetaValue lists_meta_value6(Slice(str, sizeof(uint64_t)));
  lists_meta_value6.UpdateVersion();
  s = meta_db->Put(rocksdb::WriteOptions(), handles[0], meta_key.Encode(), lists_meta_value6.Encode());
  ASSERT_TRUE(s.ok());

  // base-filter was used for elimination detection
  ListsDataKey lists_data_key6(user_key, version, 1);
  filter_result = base_filter->Filter(0, lists_data_key6.Encode(), "FILTER_TEST_VALUE", &new_value, &value_changed);
  ASSERT_EQ(filter_result, true);
  s = meta_db->Delete(rocksdb::WriteOptions(), handles[0], "FILTER_TEST_KEY");
  ASSERT_TRUE(s.ok());
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
