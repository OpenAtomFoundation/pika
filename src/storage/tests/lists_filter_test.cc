//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <gtest/gtest.h>
#include <iostream>
#include <thread>

#include "src/lists_filter.h"
#include "src/redis.h"
#include "storage/storage.h"

using namespace storage;

class ListsFilterTest : public ::testing::Test {
 public:
  ListsFilterTest() {
    std::string db_path = "./db/list_meta";
    if (access(db_path.c_str(), F_OK)) {
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
    column_families.push_back(rocksdb::ColumnFamilyDescriptor(rocksdb::kDefaultColumnFamilyName, meta_cf_ops));
    // Data CF
    column_families.push_back(rocksdb::ColumnFamilyDescriptor("data_cf", data_cf_ops));

    s = rocksdb::DB::Open(options, db_path, column_families, &handles, &meta_db);
  }
  virtual ~ListsFilterTest() {}

  virtual void SetUp() {}
  virtual void TearDown() {
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

// Meta Filter
TEST_F(ListsFilterTest, MetaFilterTest) {
  char str[8];
  bool filter_result;
  bool value_changed;
  int32_t version = 0;
  std::string new_value;

  // Test Meta Filter
  ListsMetaFilter* lists_meta_filter = new ListsMetaFilter();
  ASSERT_TRUE(lists_meta_filter != nullptr);

  // Timeout timestamp is not set, but it's an empty list.
  EncodeFixed64(str, 0);
  ListsMetaValue lists_meta_value1(std::string(str, sizeof(uint64_t)));
  lists_meta_value1.UpdateVersion();
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  filter_result =
      lists_meta_filter->Filter(0, "FILTER_TEST_KEY", lists_meta_value1.Encode(), &new_value, &value_changed);
  ASSERT_EQ(filter_result, true);

  // Timeout timestamp is not set, it's not an empty list.
  EncodeFixed64(str, 1);
  ListsMetaValue lists_meta_value2(std::string(str, sizeof(uint64_t)));
  lists_meta_value2.UpdateVersion();
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  filter_result =
      lists_meta_filter->Filter(0, "FILTER_TEST_KEY", lists_meta_value2.Encode(), &new_value, &value_changed);
  ASSERT_EQ(filter_result, false);

  // Timeout timestamp is set, but not expired.
  EncodeFixed64(str, 1);
  ListsMetaValue lists_meta_value3(std::string(str, sizeof(uint64_t)));
  lists_meta_value3.UpdateVersion();
  lists_meta_value3.SetRelativeTimestamp(3);
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  filter_result =
      lists_meta_filter->Filter(0, "FILTER_TEST_KEY", lists_meta_value3.Encode(), &new_value, &value_changed);
  ASSERT_EQ(filter_result, false);

  // Timeout timestamp is set, already expired.
  EncodeFixed64(str, 1);
  ListsMetaValue lists_meta_value4(std::string(str, sizeof(uint64_t)));
  lists_meta_value4.UpdateVersion();
  lists_meta_value4.SetRelativeTimestamp(1);
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  ParsedListsMetaValue parsed_meta_value(lists_meta_value4.Encode());
  filter_result =
      lists_meta_filter->Filter(0, "FILTER_TEST_KEY", lists_meta_value4.Encode(), &new_value, &value_changed);
  ASSERT_EQ(filter_result, true);
  delete lists_meta_filter;
}

// Data Filter
TEST_F(ListsFilterTest, DataFilterTest) {
  char str[8];
  bool filter_result;
  bool value_changed;
  int32_t version = 0;
  std::string new_value;

  // Timeout timestamp is not set, the version is valid.
  ListsDataFilter* lists_data_filter1 = new ListsDataFilter(meta_db, &handles);
  ASSERT_TRUE(lists_data_filter1 != nullptr);

  EncodeFixed64(str, 1);
  ListsMetaValue lists_meta_value1(std::string(str, sizeof(uint64_t)));
  version = lists_meta_value1.UpdateVersion();
  s = meta_db->Put(rocksdb::WriteOptions(), handles[0], "FILTER_TEST_KEY", lists_meta_value1.Encode());
  ASSERT_TRUE(s.ok());

  ListsDataKey lists_data_key1("FILTER_TEST_KEY", version, 1);
  filter_result =
      lists_data_filter1->Filter(0, lists_data_key1.Encode(), "FILTER_TEST_VALUE", &new_value, &value_changed);
  ASSERT_EQ(filter_result, false);
  s = meta_db->Delete(rocksdb::WriteOptions(), handles[0], "FILTER_TEST_KEY");
  ASSERT_TRUE(s.ok());
  delete lists_data_filter1;

  // Timeout timestamp is set, but not expired.
  ListsDataFilter* lists_data_filter2 = new ListsDataFilter(meta_db, &handles);
  ASSERT_TRUE(lists_data_filter2 != nullptr);

  EncodeFixed64(str, 1);
  ListsMetaValue lists_meta_value2(std::string(str, sizeof(uint64_t)));
  version = lists_meta_value2.UpdateVersion();
  lists_meta_value2.SetRelativeTimestamp(1);
  s = meta_db->Put(rocksdb::WriteOptions(), handles[0], "FILTER_TEST_KEY", lists_meta_value2.Encode());
  ASSERT_TRUE(s.ok());
  ListsDataKey lists_data_key2("FILTER_TEST_KEY", version, 1);
  filter_result =
      lists_data_filter2->Filter(0, lists_data_key2.Encode(), "FILTER_TEST_VALUE", &new_value, &value_changed);
  ASSERT_EQ(filter_result, false);
  s = meta_db->Delete(rocksdb::WriteOptions(), handles[0], "FILTER_TEST_KEY");
  ASSERT_TRUE(s.ok());
  delete lists_data_filter2;

  // Timeout timestamp is set, already expired.
  ListsDataFilter* lists_data_filter3 = new ListsDataFilter(meta_db, &handles);
  ASSERT_TRUE(lists_data_filter3 != nullptr);

  EncodeFixed64(str, 1);
  ListsMetaValue lists_meta_value3(std::string(str, sizeof(uint64_t)));
  version = lists_meta_value3.UpdateVersion();
  lists_meta_value3.SetRelativeTimestamp(1);
  s = meta_db->Put(rocksdb::WriteOptions(), handles[0], "FILTER_TEST_KEY", lists_meta_value3.Encode());
  ASSERT_TRUE(s.ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  ListsDataKey lists_data_key3("FILTER_TEST_KEY", version, 1);
  filter_result =
      lists_data_filter3->Filter(0, lists_data_key3.Encode(), "FILTER_TEST_VALUE", &new_value, &value_changed);
  ASSERT_EQ(filter_result, true);
  s = meta_db->Delete(rocksdb::WriteOptions(), handles[0], "FILTER_TEST_KEY");
  ASSERT_TRUE(s.ok());
  delete lists_data_filter3;

  // Timeout timestamp is not set, the version is invalid
  ListsDataFilter* lists_data_filter4 = new ListsDataFilter(meta_db, &handles);
  ASSERT_TRUE(lists_data_filter4 != nullptr);

  EncodeFixed64(str, 1);
  ListsMetaValue lists_meta_value4(std::string(str, sizeof(uint64_t)));
  version = lists_meta_value4.UpdateVersion();
  s = meta_db->Put(rocksdb::WriteOptions(), handles[0], "FILTER_TEST_KEY", lists_meta_value4.Encode());
  ASSERT_TRUE(s.ok());
  ListsDataKey lists_data_key4("FILTER_TEST_KEY", version, 1);
  version = lists_meta_value4.UpdateVersion();
  s = meta_db->Put(rocksdb::WriteOptions(), handles[0], "FILTER_TEST_KEY", lists_meta_value4.Encode());
  ASSERT_TRUE(s.ok());
  filter_result =
      lists_data_filter4->Filter(0, lists_data_key4.Encode(), "FILTER_TEST_VALUE", &new_value, &value_changed);
  ASSERT_EQ(filter_result, true);
  s = meta_db->Delete(rocksdb::WriteOptions(), handles[0], "FILTER_TEST_KEY");
  ASSERT_TRUE(s.ok());
  delete lists_data_filter4;

  // Meta data has been clear
  ListsDataFilter* lists_data_filter5 = new ListsDataFilter(meta_db, &handles);
  ASSERT_TRUE(lists_data_filter5 != nullptr);

  EncodeFixed64(str, 1);
  ListsMetaValue lists_meta_value5(std::string(str, sizeof(uint64_t)));
  version = lists_meta_value5.UpdateVersion();
  s = meta_db->Put(rocksdb::WriteOptions(), handles[0], "FILTER_TEST_KEY", lists_meta_value5.Encode());
  ASSERT_TRUE(s.ok());
  ListsDataKey lists_data_value5("FILTER_TEST_KEY", version, 1);
  s = meta_db->Delete(rocksdb::WriteOptions(), handles[0], "FILTER_TEST_KEY");
  ASSERT_TRUE(s.ok());
  filter_result =
      lists_data_filter5->Filter(0, lists_data_value5.Encode(), "FILTER_TEST_VALUE", &new_value, &value_changed);
  ASSERT_EQ(filter_result, true);
  delete lists_data_filter5;
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
