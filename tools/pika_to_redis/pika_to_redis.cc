#include <iostream>
#include <string>
#include <sstream>
#include "pink/include/redis_cli.h"
#include "nemo.h"
#include "parse_thread.h"
#include "sender_thread.h"
#include "migrator_thread.h"

const int64_t kTestPoint = 500000;
const int64_t kTestNum = LLONG_MAX;
const int64_t kDataSetNum = 5;

std::string db_path = "/home/yinshucheng/test/db";
std::string ip = "127.0.0.1";
int port = 6379;
size_t num_thread = 12;

std::vector<ParseThread*> parsers;
std::vector<SenderThread*> senders;
std::vector<MigratorThread*> migrators;
nemo::Nemo *db;

void HumanTime(int64_t time) {
  time = time / 1000000;
  int64_t hours = time / 3600;
  time = time % 3600;
  int64_t minutes = time / 60;
  int64_t secs = time % 60;

  std::cout << hours << " hour " << minutes << " min " << secs << " s\n";
}

int64_t GetNum() {
  int64_t num = 0;
  for (size_t i = 0; i < migrators.size(); i++) {
    num += migrators[i]->num();
  }
  return num;
}

void PrintConf() {
  std::cout << "db_path : " << db_path << std::endl;
  std::cout << "ip : " << ip << std::endl;
  std::cout << "port : " << port << std::endl;
  std::cout << "num_sender : " << num_thread << std::endl;
  std::cout << "====================================" << std::endl;

}

void Usage() {
  std::cout << "Usage: " << std::endl;
  std::cout << "      ./pika_to_redis db_path ip port sender_num\n";
  std::cout << "      example: ./pika_to_redis ~/db 127.0.0.1 6379 16\n";
}

int64_t NowMicros() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

int main(int argc, char **argv)
{
  if (argc != 5) {
    Usage();
    return -1;
  }
  db_path = std::string(argv[1]);
  ip = std::string(argv[2]);
  port = atoi(argv[3]);
  num_thread = atoi(argv[4]);

  if (db_path[db_path.length() - 1] != '/') {
    db_path.append("/");
  }
  std::cout<<db_path << "";

  bool should_exit = true;
  PrintConf();

  // init db
  nemo::Options option;
  option.write_buffer_size = 256 * 1024 * 1024; // 256M
  option.target_file_size_base = 20 * 1024 * 1024; // 20M
  db = new nemo::Nemo(db_path ,option);

  // Open Options
  rocksdb::Options open_options_;
  open_options_.create_if_missing = true;
  open_options_.write_buffer_size = 256 * 1024 * 1024; // 256M
  open_options_.max_manifest_file_size = 64*1024*1024;
  open_options_.max_log_file_size = 512*1024*1024;
  open_options_.keep_log_file_num = 10;

/*
  std::shared_ptr<rocksdb::DBNemo> kv_db_;
  std::shared_ptr<rocksdb::DBNemo> hash_db_;
  //std::unique_ptr<rocksdb::DB> hash_db_;
  std::shared_ptr<rocksdb::DBNemo> list_db_;
  std::shared_ptr<rocksdb::DBNemo> zset_db_;
  std::shared_ptr<rocksdb::DBNemo> set_db_;

  rocksdb::DBNemo *db_ttl;

  rocksdb::Status s = rocksdb::DBNemo::Open(open_options_, db_path + "kv", &db_ttl, rocksdb::kMetaPrefixKv);
  if (!s.ok()) {
    fprintf (stderr, "[FATAL] open kv db failed, %s\n", s.ToString().c_str());
    exit(-1);
  }
  kv_db_ = std::shared_ptr<rocksdb::DBNemo>(db_ttl);

  s = rocksdb::DBNemo::Open(open_options_, db_path + "hash", &db_ttl, rocksdb::kMetaPrefixHash);
  if (!s.ok()) {
    fprintf (stderr, "[FATAL] open hash db failed, %s\n", s.ToString().c_str());
    exit(-1);
  }
  hash_db_ = std::shared_ptr<rocksdb::DBNemo>(db_ttl);

  s = rocksdb::DBNemo::Open(open_options_, db_path + "list", &db_ttl, rocksdb::kMetaPrefixList);
  if (!s.ok()) {
    fprintf (stderr, "[FATAL] open list db failed, %s\n", s.ToString().c_str());
    exit(-1);
  }
  list_db_ = std::shared_ptr<rocksdb::DBNemo>(db_ttl);

  s = rocksdb::DBNemo::Open(open_options_, db_path + "zset", &db_ttl, rocksdb::kMetaPrefixZset);
  if (!s.ok()) {
    fprintf (stderr, "[FATAL] open zset db failed, %s\n", s.ToString().c_str());
    exit(-1);
  }
  zset_db_ = std::shared_ptr<rocksdb::DBNemo>(db_ttl);

  s = rocksdb::DBNemo::Open(open_options_, db_path + "set", &db_ttl, rocksdb::kMetaPrefixSet);
  if (!s.ok()) {
    fprintf (stderr, "[FATAL] open set db failed, %s\n", s.ToString().c_str());
    exit(-1);
  }
  set_db_ = std::shared_ptr<rocksdb::DBNemo>(db_ttl);
*/

  // init ParseThread and SenderThread
  slash::Status pink_s;
  for (size_t i = 0; i < kDataSetNum; i++) {
     // init a redis-cli
    pink::PinkCli *cli = pink::NewRedisCli();
    cli->set_connect_timeout(3000);
    pink_s = cli->Connect(ip, port);
    if(!pink_s.ok()) {
      Usage();
      log_err("cann't connect %s:%d:%s\n", ip.data(), port, pink_s.ToString().data());
      return -1;
    }
    SenderThread *sender = new SenderThread(cli);
    senders.push_back(sender);
    //parsers.push_back(new ParseThread(db, sender));
  }

  migrators.push_back(new MigratorThread(db, senders[0], nemo::DataType::kKv));
  migrators.push_back(new MigratorThread(db, senders[1], nemo::DataType::kHSize));
  migrators.push_back(new MigratorThread(db, senders[2], nemo::DataType::kSSize));
  migrators.push_back(new MigratorThread(db, senders[3], nemo::DataType::kLMeta));
  migrators.push_back(new MigratorThread(db, senders[4], nemo::DataType::kZSize));

  // start threads
  for (size_t i = 0; i < migrators.size(); i++) {
    migrators[i]->StartThread();
  }
  for (size_t i = 0; i < kDataSetNum; i++) {
    //parsers[i]->StartThread();
    senders[i]->StartThread();
  }


  int64_t start_time = NowMicros();
  int times = 1;
  /*
  while(!should_exit) {
    sleep(1);
    int64_t num = GetNum();
    if (num >= kTestPoint * times) {
      times++;
      int64_t dur = NowMicros() - start_time;
      std::cout << "Running time:" << dur / 1000000 << "s "
         << num << " records" << std::endl;
    }

    // check if all migrators have exited

    for (size_t i = 0; i < migrators.size(); i++) {
      if (!migrators[i]->should_exit_) {
        should_exit = false;
        break;
      }
    }


    if (num >= kTestNum) {
      should_exit = true;
    }

    // inform parser to exit
    if (should_exit) {
      for (size_t i = 0; i < num_thread; i++) {
        parsers[i]->Stop();
      }
      break;
    }
  }
  */

  for (size_t i = 0; i < kDataSetNum; i++) {
    //parsers[i]->JoinThread();
    //migrators[i]->JoinThread();
    senders[i]->JoinThread();
  }


  int64_t replies = 0;
  int64_t errors = 0;
  int64_t records = GetNum();
  for (size_t i = 0; i < kDataSetNum; i++) {
    replies += senders[i]->elements();
    errors += senders[i]->err();
  }

  for (size_t i = 0; i < migrators.size(); i++) {
    delete migrators[i];
  }
  for (size_t i = 0; i < 5; i++) {
    //delete parsers[i];
    delete senders[i];
  }

  std::cout << "====================================" << std::endl;
  int64_t curr = NowMicros() - start_time;
  std::cout << "Running time  :";
  HumanTime(curr);
  std::cout << "Total records : " << records << " have been migreated\n";
  std::cout << "Total replies : " << replies << " received from redis server\n";
  std::cout << "Total errors  : " << errors << " received from redis server\n";
  delete db;
  return 0;
}
