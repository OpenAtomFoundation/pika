//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <gtest/gtest.h>
// #include <thread>
// #include <iostream>

// #include "src/redis.h"
// #include "src/base_filter.h"
// #include "storage/storage.h"

// using namespace storage;

// // Filter
// TEST(HashesFilterTest, FilterTest) {
//   rocksdb::DB* meta_db;
//   std::string db_path = "./db/hash_meta";
//   std::vector<rocksdb::ColumnFamilyHandle*> handles;

//   storage::Options options;
//   options.create_if_missing = true;
//   rocksdb::Status s = rocksdb::DB::Open(options, db_path, &meta_db);

//   if (s.ok()) {
//     // create column family
//     rocksdb::ColumnFamilyHandle* cf;
//     s = meta_db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(),
//         "data_cf", &cf);
//     ASSERT_TRUE(s.ok());
//     // close DB
//     delete cf;
//     delete meta_db;
//   }

//   // Open
//   rocksdb::ColumnFamilyOptions meta_cf_ops(options);
//   rocksdb::ColumnFamilyOptions data_cf_ops(options);
//   std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
//   // Meta CF
//   column_families.push_back(rocksdb::ColumnFamilyDescriptor(
//       rocksdb::kDefaultColumnFamilyName, meta_cf_ops));
//   // Data CF
//   column_families.push_back(rocksdb::ColumnFamilyDescriptor(
//       "data_cf", data_cf_ops));

//   s = rocksdb::DB::Open(options, db_path, column_families, &handles, &meta_db);
//   ASSERT_TRUE(s.ok());

//   char str[4];
//   bool filter_result;
//   bool value_changed;
//   int32_t version = 0;
//   std::string new_value;

//   /*************** TEST META FILTER ***************/
//   HashesMetaFilter* hashes_meta_filter = new HashesMetaFilter();
//   ASSERT_TRUE(hashes_meta_filter != nullptr);

//   // Timeout timestamp is not set, but it's an empty hash table.
//   EncodeFixed32(str, 0);
//   HashesMetaValue tmf_meta_value1(std::string(str, sizeof(int32_t)));
//   tmf_meta_value1.UpdateVersion();
//   std::this_thread::sleep_for(std::chrono::milliseconds(1000));
//   filter_result = hashes_meta_filter->Filter(0, "FILTER_TEST_KEY",
//       tmf_meta_value1.Encode(), &new_value, &value_changed);
//   ASSERT_EQ(filter_result, true);

//   // Timeout timestamp is not set, it's not an empty hash table.
//   EncodeFixed32(str, 1);
//   HashesMetaValue tmf_meta_value2(std::string(str, sizeof(int32_t)));
//   tmf_meta_value2.UpdateVersion();
//   std::this_thread::sleep_for(std::chrono::milliseconds(1000));
//   filter_result = hashes_meta_filter->Filter(0, "FILTER_TEST_KEY",
//       tmf_meta_value2.Encode(), &new_value, &value_changed);
//   ASSERT_EQ(filter_result, false);

//   // Timeout timestamp is set, but not expired.
//   EncodeFixed32(str, 1);
//   HashesMetaValue tmf_meta_value3(std::string(str, sizeof(int32_t)));
//   tmf_meta_value3.UpdateVersion();
//   tmf_meta_value3.SetRelativeTimestamp(3);
//   std::this_thread::sleep_for(std::chrono::milliseconds(1000));
//   filter_result = hashes_meta_filter->Filter(0, "FILTER_TEST_KEY",
//       tmf_meta_value3.Encode(), &new_value, &value_changed);
//   ASSERT_EQ(filter_result, false);

//   // Timeout timestamp is set, already expired.
//   EncodeFixed32(str, 1);
//   HashesMetaValue tmf_meta_value4(std::string(str, sizeof(int32_t)));
//   tmf_meta_value4.UpdateVersion();
//   tmf_meta_value4.SetRelativeTimestamp(1);
//   std::this_thread::sleep_for(std::chrono::milliseconds(2000));
//   filter_result = hashes_meta_filter->Filter(0, "FILTER_TEST_KEY",
//       tmf_meta_value4.Encode(), &new_value, &value_changed);
//   ASSERT_EQ(filter_result, true);
//   delete hashes_meta_filter;

//   /*************** TEST DATA FILTER ***************/

//   // No timeout is set, version not outmoded.
//   HashesDataFilter* hashes_data_filter1
//     = new HashesDataFilter(meta_db, &handles);
//   ASSERT_TRUE(hashes_data_filter1 != nullptr);
//   EncodeFixed32(str, 1);
//   HashesMetaValue tdf_meta_value1(std::string(str, sizeof(int32_t)));
//   version = tdf_meta_value1.UpdateVersion();
//   s = meta_db->Put(rocksdb::WriteOptions(), handles[0],
//       "FILTER_TEST_KEY", tdf_meta_value1.Encode());
//   ASSERT_TRUE(s.ok());
//   HashesDataKey tdf_data_key1("FILTER_TEST_KEY", version, "FILTER_TEST_FIELD");
//   filter_result = hashes_data_filter1->Filter(0, tdf_data_key1.Encode(),
//       "FILTER_TEST_VALUE", &new_value, &value_changed);
//   ASSERT_EQ(filter_result, false);
//   s = meta_db->Delete(rocksdb::WriteOptions(),
//       handles[0], "FILTER_TEST_KEY");
//   ASSERT_TRUE(s.ok());
//   delete hashes_data_filter1;

//   // timeout timestamp is set, but not timeout.
//   HashesDataFilter* hashes_data_filter2
//     = new HashesDataFilter(meta_db, &handles);
//   ASSERT_TRUE(hashes_data_filter2 != nullptr);
//   EncodeFixed32(str, 1);
//   HashesMetaValue tdf_meta_value2(std::string(str, sizeof(int32_t)));
//   version = tdf_meta_value2.UpdateVersion();
//   tdf_meta_value2.SetRelativeTimestamp(1);
//   s = meta_db->Put(rocksdb::WriteOptions(), handles[0],
//       "FILTER_TEST_KEY", tdf_meta_value2.Encode());
//   ASSERT_TRUE(s.ok());
//   HashesDataKey tdf_data_key2("FILTER_TEST_KEY", version, "FILTER_TEST_FIELD");
//   filter_result = hashes_data_filter2->Filter(0, tdf_data_key2.Encode(),
//       "FILTER_TEST_VALUE", &new_value, &value_changed);
//   ASSERT_EQ(filter_result, false);
//   s = meta_db->Delete(rocksdb::WriteOptions(),
//       handles[0], "FILTER_TEST_KEY");
//   ASSERT_TRUE(s.ok());
//   delete hashes_data_filter2;

//   // timeout timestamp is set, already timeout.
//   HashesDataFilter* hashes_data_filter3
//     = new HashesDataFilter(meta_db, &handles);
//   ASSERT_TRUE(hashes_data_filter3 != nullptr);
//   EncodeFixed32(str, 1);
//   HashesMetaValue tdf_meta_value3(std::string(str, sizeof(int32_t)));
//   version = tdf_meta_value3.UpdateVersion();
//   tdf_meta_value3.SetRelativeTimestamp(1);
//   s = meta_db->Put(rocksdb::WriteOptions(), handles[0],
//       "FILTER_TEST_KEY", tdf_meta_value3.Encode());
//   ASSERT_TRUE(s.ok());
//   std::this_thread::sleep_for(std::chrono::milliseconds(2000));
//   HashesDataKey tdf_data_key3("FILTER_TEST_KEY", version, "FILTER_TEST_FIELD");
//   filter_result = hashes_data_filter3->Filter(0, tdf_data_key3.Encode(),
//       "FILTER_TEST_VALUE", &new_value, &value_changed);
//   ASSERT_EQ(filter_result, true);
//   s = meta_db->Delete(rocksdb::WriteOptions(),
//       handles[0], "FILTER_TEST_KEY");
//   ASSERT_TRUE(s.ok());
//   delete hashes_data_filter3;

//   // No timeout is set, version outmoded.
//   HashesDataFilter* hashes_data_filter4
//     = new HashesDataFilter(meta_db, &handles);
//   ASSERT_TRUE(hashes_data_filter4 != nullptr);
//   EncodeFixed32(str, 1);
//   HashesMetaValue tdf_meta_value4(std::string(str, sizeof(int32_t)));
//   version = tdf_meta_value4.UpdateVersion();
//   s = meta_db->Put(rocksdb::WriteOptions(), handles[0],
//       "FILTER_TEST_KEY", tdf_meta_value4.Encode());
//   ASSERT_TRUE(s.ok());
//   HashesDataKey tdf_data_key4("FILTER_TEST_KEY", version, "FILTER_TEST_FIELD");
//   version = tdf_meta_value4.UpdateVersion();
//   s = meta_db->Put(rocksdb::WriteOptions(), handles[0],
//       "FILTER_TEST_KEY", tdf_meta_value4.Encode());
//   ASSERT_TRUE(s.ok());
//   filter_result = hashes_data_filter4->Filter(0, tdf_data_key4.Encode(),
//       "FILTER_TEST_VALUE", &new_value, &value_changed);
//   ASSERT_EQ(filter_result, true);
//   s = meta_db->Delete(rocksdb::WriteOptions(),
//       handles[0], "FILTER_TEST_KEY");
//   ASSERT_TRUE(s.ok());
//   delete hashes_data_filter4;

//   // Hash table meta data has been clear.
//   HashesDataFilter* hashes_data_filter5
//     = new HashesDataFilter(meta_db, &handles);
//   ASSERT_TRUE(hashes_data_filter5 != nullptr);
//   EncodeFixed32(str, 1);
//   HashesMetaValue tdf_meta_value5(std::string(str, sizeof(int32_t)));
//   version = tdf_meta_value5.UpdateVersion();
//   s = meta_db->Put(rocksdb::WriteOptions(), handles[0],
//       "FILTER_TEST_KEY", tdf_meta_value5.Encode());
//   ASSERT_TRUE(s.ok());
//   HashesDataKey tdf_data_key5("FILTER_TEST_KEY", version, "FILTER_TEST_FIELD");
//   s = meta_db->Delete(rocksdb::WriteOptions(),
//       handles[0], "FILTER_TEST_KEY");
//   ASSERT_TRUE(s.ok());
//   filter_result = hashes_data_filter5->Filter(0, tdf_data_key5.Encode(),
//       "FILTER_TEST_VALUE", &new_value, &value_changed);
//   ASSERT_EQ(filter_result, true);
//   delete hashes_data_filter5;

//   // Delete Meta db
//   delete meta_db;
// }

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
