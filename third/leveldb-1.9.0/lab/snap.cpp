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
    string p_value;
    string property="stat";


    leveldb::ReadOptions rop;
    s = db->Put(leveldb::WriteOptions(), key1, value);
    rop.snapshot = db->GetSnapshot();
    value = "tiancaiafter";
    s = db->Put(leveldb::WriteOptions(), key1, value);
    std::string value1;

    s = db->Get(rop, key1, &value1);
    cout << "value1 " << value1 << endl;
    //if (s.ok()) {
    //    leveldb::WriteBatch batch;
    //    batch.Delete(key1);
    //    batch.Put(key2, value);
    //    s = db->Write(leveldb::WriteOptions(), &batch);
    //}
    //std::string value1;
    //s = db->Get(leveldb::ReadOptions(), key2, &value1);

    //printf("values %s\n", value1.c_str());
    printf("Printf all database\n");
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next())
    {
        cout << it->key().ToString() << ": " << it->value().ToString() << endl;
    }
    delete it;
    delete db;
    return 0;
}
