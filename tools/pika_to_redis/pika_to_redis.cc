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

  std::cout << hours << " hour " << minutes << " min " << secs << "s\n";
}

int64_t GetNum() {
  int64_t num = 0;
  for (size_t i = 0; i < num_thread; i++) {
    num += parsers[i]->num();
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

  PrintConf();

  // init db
  nemo::Options option;
  option.write_buffer_size = 256 * 1024 * 1024; // 256M
  option.target_file_size_base = 20 * 1024 * 1024; // 20M
  db = new nemo::Nemo(db_path ,option);

  // init ParseThread and SenderThread
  slash::Status pink_s;
  for (size_t i = 0; i < num_thread; i++) {
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
    parsers.push_back(new ParseThread(db, sender));
  }

  migrators.push_back(new MigratorThread(db, parsers, nemo::DataType::kKv));
  migrators.push_back(new MigratorThread(db, parsers, nemo::DataType::kHSize));
  migrators.push_back(new MigratorThread(db, parsers, nemo::DataType::kSSize));
  migrators.push_back(new MigratorThread(db, parsers, nemo::DataType::kLMeta));
  migrators.push_back(new MigratorThread(db, parsers, nemo::DataType::kZSize));

  // start threads
  for (size_t i = 0; i < migrators.size(); i++) {
    migrators[i]->StartThread();
  }
  for (size_t i = 0; i < num_thread; i++) {
    parsers[i]->StartThread();
    senders[i]->StartThread();
  }

  int64_t start_time = NowMicros();
  int times = 1;
  while(1) {
    sleep(1);
    int64_t num = GetNum();
    if (num >= kTestPoint * times) {
      times++;
      int64_t dur = NowMicros() - start_time;
      std::cout << "Running time:" << dur / 1000000 << "s "
         << num << " records" << std::endl;
    }

    // check if all migrators have exited
    bool should_exit = true;
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

  for (size_t i = 0; i < num_thread; i++) {
    parsers[i]->JoinThread();
    senders[i]->JoinThread();
  }

  int64_t replies = 0;
  int64_t errors = 0;
  int64_t records = GetNum();
  for (size_t i = 0; i < num_thread; i++) {
    replies += senders[i]->elements();
    errors += senders[i]->err();
  }

  for (size_t i = 0; i < migrators.size(); i++) {
    delete migrators[i];
  }
  for (size_t i = 0; i < num_thread; i++) {
    delete parsers[i];
    delete senders[i];
  }

  std::cout << "====================================" << std::endl;
  int64_t curr = NowMicros() - start_time;
  std::cout << "Running time  :";
  HumanTime(curr);
  std::cout << "Total records :" << records << " have been migreated\n";
  std::cout << "Total replies :" << replies << " received from redis server\n";
  std::cout << "Total errors  :" << errors << " received from redis server\n";
  delete db;
  return 0;
}
