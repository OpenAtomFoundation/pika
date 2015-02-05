#include "leveldb/db.h"
#include <iostream>
#include <assert.h>
#include <string>
#include <stdio.h>
#include <unistd.h>

#include <ctime>
#include "leveldb/write_batch.h"

using namespace std;

void *callback(void *args)
{
    sleep(1);
    leveldb::DB* db = (leveldb::DB *)args;
    string key1 = "czz";
    string value = "baotiao_thread";
    leveldb::Status s = db->Put(leveldb::WriteOptions(), key1, value);

    printf("in subthread: running OK\n");
    pthread_exit(0);
}
int main()
{
    system("rm -rf ~/tmp/testdb");

    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;
    // options.error_if_exists = false;

    leveldb::Status s = leveldb::DB::Open(options, "/tmp/testdb", &db);
    std::string value1;


    pthread_t pid;

    int32_t ret = pthread_create(&pid, NULL, callback, db);
    // int32_t ret = pthread_create(&tid, NULL, thread, p);

    printf("after: ret = %d\n", ret);

    string key1 = "czz";
    string value = "baotiao";
    s = db->Put(leveldb::WriteOptions(), key1, value);

    void *tret;
    int32_t err;
    err = pthread_join(pid, &tret);

    s = db->Get(leveldb::ReadOptions(), key1, &value1);

    printf("values %s\n", value1.c_str());
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next())
    {
        cout << it->key().ToString() << ": " << it->value().ToString() << endl;
    }
    delete it;
    delete db;
    return 0;
}
