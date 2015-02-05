#include "leveldb/db.h"
#include <iostream>
#include <assert.h>
#include <string>

#include "leveldb/write_batch.h"

using namespace std;
int main()
{
    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;
    //options.max_open_files = 2;
    //options.error_if_exists = true;

    std::string key1 = "czz", key2 = "baotiao"; 
    std::string value = "tiancai";
    leveldb::Status s = leveldb::DB::Open(options, "/tmp/testdb", &db);

    leveldb::DB* db1;
    leveldb::Options options1;
    leveldb::Status s1 = leveldb::DB::Open(options1, "/tmp/testdb1", &db1);
    string p_value;
    string property="stat";


    //db->GetProperty(property, p_value);
    for (int i = 0; i < 100000000; i++) {
        value = value + char(i + 'a');
        key1 = key1 + char(i + 'a');
        s = db->Put(leveldb::WriteOptions(), key1, value);
    }
    s = db->Put(leveldb::WriteOptions(), key1, value);

    s = db->Get(leveldb::ReadOptions(), key1, &value);
    s = db->Delete(leveldb::WriteOptions(), "czza");
    //if (s.ok()) {
    //    leveldb::WriteBatch batch;
    //    batch.Delete(key1);
    //    batch.Put(key2, value);
    //    s = db->Write(leveldb::WriteOptions(), &batch);
    //}
    //std::string value1;
    //s = db->Get(leveldb::ReadOptions(), key2, &value1);

    //printf("values %s\n", value1.c_str());
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next())
    {
        cout << it->key().ToString() << ": " << it->value().ToString() << endl;
    }
    delete it;
    delete db;
    return 0;
}
