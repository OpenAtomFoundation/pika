#include <iostream>
#include <string>
#include <sstream>
#include "pink/include/redis_cli.h"
#include "nemo.h"
//#include "sender_thread.h"
#include "sender.h"
#include "migrator_thread.h"

using std::chrono::high_resolution_clock;
using std::chrono::milliseconds;

const int64_t kTestPoint = 500000;
const int64_t kTestNum = LLONG_MAX;
const int64_t kDataSetNum = 5;

std::string db_path;
std::string ip;
int port;
size_t thread_num;
std::string password;

//std::vector<ParseThread*> parsers;
std::vector<Sender*> senders;
std::vector<MigratorThread*> migrators;
nemo::Nemo *db;

void PrintConf() {
  std::cout << "db_path : " << db_path << std::endl;
  std::cout << "redis ip : " << ip << std::endl;
  std::cout << "redis port : " << port << std::endl;
  std::cout << "thread_num : " << thread_num << std::endl;
  std::cout << "====================================" << std::endl;

}

void Usage() {
  std::cout << "Usage: " << std::endl;
  std::cout << "      ./pika_to_redis db_path ip port [password]\n";
  std::cout << "      example: ./pika_to_redis ~/db 127.0.0.1 6379 123456\n";
}

int64_t NowMicros() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

int main(int argc, char **argv)
{
  if (argc < 5) {
    Usage();
    return -1;
  }
  high_resolution_clock::time_point start = high_resolution_clock::now();

  db_path = std::string(argv[1]);
  ip = std::string(argv[2]);
  port = atoi(argv[3]);
  thread_num = atoi(argv[4]);
  if (argc == 6) {
    password = std::string(argv[5]);
  }

  if (db_path[db_path.length() - 1] != '/') {
    db_path.append("/");
  }

  PrintConf();

  // Init db
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


  // Init SenderThread
  for (size_t i = 0; i < thread_num; i++) {
    Sender *sender = new Sender(db, ip, port, password);
    senders.push_back(sender);
  }

  migrators.push_back(new MigratorThread(db, senders, nemo::DataType::kKv, thread_num));
  migrators.push_back(new MigratorThread(db, senders, nemo::DataType::kHSize, thread_num));
  migrators.push_back(new MigratorThread(db, senders, nemo::DataType::kSSize, thread_num));
  migrators.push_back(new MigratorThread(db, senders, nemo::DataType::kLMeta, thread_num));
  migrators.push_back(new MigratorThread(db, senders, nemo::DataType::kZSize, thread_num));

  // start threads
  for (size_t i = 0; i < kDataSetNum; i++) {
    migrators[i]->StartThread();
  }

  for (size_t i = 0; i < thread_num; i++) {
    senders[i]->StartThread();
  }


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
  for(size_t i = 0; i < kDataSetNum; i++) {
    migrators[i]->JoinThread();
  }
  for(size_t i = 0; i < thread_num; i++) {
    senders[i]->Stop();
  }

  for (size_t i = 0; i < thread_num; i++) {
    senders[i]->JoinThread();
  }


  int64_t replies = 0, records = 0;
  for (size_t i = 0; i < kDataSetNum; i++) {
    records += migrators[i]->num();
    //delete migrators[i];
  }
  for (size_t i = 0; i < thread_num; i++) {
    replies += senders[i]->elements();
    delete senders[i];
  }

  high_resolution_clock::time_point end = high_resolution_clock::now();
  std::chrono::hours  h = std::chrono::duration_cast<std::chrono::hours>(end - start);
  std::chrono::minutes  m = std::chrono::duration_cast<std::chrono::minutes>(end - start);
  std::chrono::seconds  s = std::chrono::duration_cast<std::chrono::seconds>(end - start);

  std::cout << "====================================" << std::endl;
  std::cout << "Running time  :";
  std::cout << h.count() << " hour " << m.count() << " min " << s.count() << " s\n";
  std::cout << "Total records : " << records << " have been Scaned\n";
  std::cout << "Total replies : " << replies << " received from redis server\n";
  delete db;
  return 0;
}
