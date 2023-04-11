#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <chrono>
#include <ctime>
#include <functional>
#include <iostream>
#include <random>
#include <vector>

#include "hiredis.h"
#include "slash/include/slash_status.h"
#include "slash/include/slash_string.h"

#define TIME_OF_LOOP 1000000

using slash::Status;
using std::default_random_engine;

Status RunSetCommand(redisContext* c);
Status RunSetCommandPipeline(redisContext* c);
Status RunZAddCommand(redisContext* c);

struct ThreadArg {
  pthread_t tid;
  std::string table_name;
  size_t idx;
};

enum TransmitMode { kNormal = 0, kPipeline = 1 };

static int32_t last_seed = 0;

std::string tables_str = "0";
std::vector<std::string> tables;

std::string hostname = "127.0.0.1";
int port = 9221;
std::string password = "";
uint32_t payload_size = 50;
uint32_t number_of_request = 100000;
uint32_t thread_num_each_table = 1;
TransmitMode transmit_mode = kNormal;
int pipeline_num = 0;

void GenerateRandomString(int32_t len, std::string* target) {
  target->clear();
  char c_map[67] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'g', 'k', 'l', 'm', 'n', 'o', 'p', 'q',
                    'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
                    'I', 'G', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y',
                    'Z', '~', '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '-', '=', '_', '+'};

  default_random_engine e;
  for (int i = 0; i < len; i++) {
    e.seed(last_seed);
    last_seed = e();
    int32_t rand_num = last_seed % 67;
    target->push_back(c_map[rand_num]);
  }
}

void PrintInfo(const std::time_t& now) {
  std::cout << "=================== Benchmark Client ===================" << std::endl;
  std::cout << "Server host name: " << hostname << std::endl;
  std::cout << "Server port: " << port << std::endl;
  std::cout << "Thread num : " << thread_num_each_table << std::endl;
  std::cout << "Payload size : " << payload_size << std::endl;
  std::cout << "Number of request : " << number_of_request << std::endl;
  std::cout << "Transmit mode: " << (transmit_mode == kNormal ? "No Pipeline" : "Pipeline") << std::endl;
  std::cout << "Collection of tables: " << tables_str << std::endl;
  std::cout << "Startup Time : " << asctime(localtime(&now));
  std::cout << "========================================================" << std::endl;
}

void Usage() {
  std::cout << "Usage: " << std::endl;
  std::cout << "\tBenchmark_client writes data to the specified table" << std::endl;
  std::cout << "\t-h    -- server hostname (default 127.0.0.1)" << std::endl;
  std::cout << "\t-p    -- server port (default 9221)" << std::endl;
  std::cout << "\t-t    -- thread num of each table (default 1)" << std::endl;
  std::cout << "\t-c    -- collection of table names (default db1)" << std::endl;
  std::cout << "\t-d    -- data size of SET value in bytes (default 50)" << std::endl;
  std::cout << "\t-n    -- number of requests single thread (default 100000)" << std::endl;
  std::cout << "\t-P    -- pipeline <numreq> requests. (default no pipeline)" << std::endl;
  std::cout << "\texample: ./benchmark_client -t 3 -c db1,db2 -d 1024" << std::endl;
}

std::vector<ThreadArg> thread_args;

void* ThreadMain(void* arg) {
  ThreadArg* ta = reinterpret_cast<ThreadArg*>(arg);
  redisContext* c;
  redisReply* res = NULL;
  struct timeval timeout = {1, 500000};  // 1.5 seconds
  c = redisConnectWithTimeout(hostname.data(), port, timeout);

  if (c == NULL || c->err) {
    if (c) {
      printf("Thread %lu, Connection error: %s\n", ta->tid, c->errstr);
      redisFree(c);
    } else {
      printf("Thread %lu, Connection error: can't allocate redis context\n", ta->tid);
    }
    return NULL;
  }

  if (!password.empty()) {
    const char* auth_argv[2] = {"AUTH", password.data()};
    size_t auth_argv_len[2] = {4, password.size()};
    res = reinterpret_cast<redisReply*>(redisCommandArgv(c, 2, reinterpret_cast<const char**>(auth_argv),
                                                         reinterpret_cast<const size_t*>(auth_argv_len)));
    if (res == NULL) {
      printf("Thread %lu  Auth Failed, Get reply Error\n", ta->tid);
      freeReplyObject(res);
      redisFree(c);
      return NULL;
    } else {
      if (!strcasecmp(res->str, "OK")) {
      } else {
        printf("Thread %lu Auth Failed: %s, thread exit...\n", ta->idx, res->str);
        freeReplyObject(res);
        redisFree(c);
        return NULL;
      }
    }
    freeReplyObject(res);
  }

  const char* select_argv[2] = {"SELECT", ta->table_name.data()};
  size_t select_argv_len[2] = {6, ta->table_name.size()};
  res = reinterpret_cast<redisReply*>(redisCommandArgv(c, 2, reinterpret_cast<const char**>(select_argv),
                                                       reinterpret_cast<const size_t*>(select_argv_len)));
  if (res == NULL) {
    printf("Thread %lu Select Table %s Failed, Get reply Error\n", ta->tid, ta->table_name.data());
    freeReplyObject(res);
    redisFree(c);
    return NULL;
  } else {
    if (!strcasecmp(res->str, "OK")) {
      printf("Table %s Thread %lu Select DB Success, start to write data...\n", ta->table_name.data(), ta->idx);
    } else {
      printf("Table %s Thread %lu Select DB Failed: %s, thread exit...\n", ta->table_name.data(), ta->idx, res->str);
      freeReplyObject(res);
      redisFree(c);
      return NULL;
    }
  }
  freeReplyObject(res);

  if (transmit_mode == kNormal) {
    Status s = RunSetCommand(c);
    if (!s.ok()) {
      std::string thread_info = "Table " + ta->table_name + ", Thread " + std::to_string(ta->idx);
      printf("%s, %s, thread exit...\n", thread_info.c_str(), s.ToString().c_str());
      redisFree(c);
      return NULL;
    }
  } else if (transmit_mode == kPipeline) {
    Status s = RunSetCommandPipeline(c);
    if (!s.ok()) {
      std::string thread_info = "Table " + ta->table_name + ", Thread " + std::to_string(ta->idx);
      printf("%s, %s, thread exit...\n", thread_info.c_str(), s.ToString().c_str());
      redisFree(c);
      return NULL;
    }
  }

  redisFree(c);
  return NULL;
}

Status RunSetCommandPipeline(redisContext* c) {
  redisReply* res = NULL;
  for (size_t idx = 0; idx < number_of_request; (idx += pipeline_num)) {
    const char* argv[3] = {"SET", NULL, NULL};
    size_t argv_len[3] = {3, 0, 0};
    for (int32_t batch_idx = 0; batch_idx < pipeline_num; ++batch_idx) {
      std::string key;
      std::string value;

      default_random_engine e;
      e.seed(last_seed);
      last_seed = e();
      int32_t rand_num = last_seed % 10 + 1;
      GenerateRandomString(rand_num, &key);
      GenerateRandomString(payload_size, &value);

      argv[1] = key.c_str();
      argv_len[1] = key.size();
      argv[2] = value.c_str();
      argv_len[2] = value.size();

      if (redisAppendCommandArgv(c, 3, reinterpret_cast<const char**>(argv),
                                 reinterpret_cast<const size_t*>(argv_len)) == REDIS_ERR) {
        return Status::Corruption("Redis Append Command Argv Error");
      }
    }

    for (int32_t batch_idx = 0; batch_idx < pipeline_num; ++batch_idx) {
      if (redisGetReply(c, reinterpret_cast<void**>(&res)) == REDIS_ERR) {
        return Status::Corruption("Redis Pipeline Get Reply Error");
      } else {
        if (res == NULL || strcasecmp(res->str, "OK")) {
          std::string res_str = "Exec command error: " + (res != NULL ? std::string(res->str) : "");
          freeReplyObject(res);
          return Status::Corruption(res_str);
        }
        freeReplyObject(res);
      }
    }
  }
  return Status::OK();
}

Status RunSetCommand(redisContext* c) {
  redisReply* res = NULL;
  for (size_t idx = 0; idx < number_of_request; ++idx) {
    const char* set_argv[3];
    size_t set_argvlen[3];
    std::string key;
    std::string value;
    default_random_engine e;
    e.seed(last_seed);
    last_seed = e();
    int32_t rand_num = last_seed % 10 + 1;

    GenerateRandomString(rand_num, &key);
    GenerateRandomString(payload_size, &value);

    set_argv[0] = "set";
    set_argvlen[0] = 3;
    set_argv[1] = key.c_str();
    set_argvlen[1] = key.size();
    set_argv[2] = value.c_str();
    set_argvlen[2] = value.size();

    res = reinterpret_cast<redisReply*>(
        redisCommandArgv(c, 3, reinterpret_cast<const char**>(set_argv), reinterpret_cast<const size_t*>(set_argvlen)));
    if (res == NULL || strcasecmp(res->str, "OK")) {
      std::string res_str = "Exec command error: " + (res != NULL ? std::string(res->str) : "");
      freeReplyObject(res);
      return Status::Corruption(res_str);
    }
    freeReplyObject(res);
  }
  return Status::OK();
}

Status RunZAddCommand(redisContext* c) {
  redisReply* res = NULL;
  for (size_t idx = 0; idx < 1; ++idx) {
    const char* zadd_argv[4];
    size_t zadd_argvlen[4];
    std::string key;
    std::string score;
    std::string member;
    GenerateRandomString(10, &key);

    zadd_argv[0] = "zadd";
    zadd_argvlen[0] = 4;
    zadd_argv[1] = key.c_str();
    zadd_argvlen[1] = key.size();
    for (size_t sidx = 0; sidx < 10000; ++sidx) {
      score = std::to_string(sidx * 2);
      GenerateRandomString(payload_size, &member);
      zadd_argv[2] = score.c_str();
      zadd_argvlen[2] = score.size();
      zadd_argv[3] = member.c_str();
      zadd_argvlen[3] = member.size();

      res = reinterpret_cast<redisReply*>(redisCommandArgv(c, 4, reinterpret_cast<const char**>(zadd_argv),
                                                           reinterpret_cast<const size_t*>(zadd_argvlen)));
      if (res == NULL || res->integer == 0) {
        std::string res_str = "Exec command error: " + (res != NULL ? std::string(res->str) : "");
        freeReplyObject(res);
        return Status::Corruption(res_str);
      }
      freeReplyObject(res);
    }
  }
  return Status::OK();
}

// ./benchmark_client
// ./benchmark_client -h
// ./benchmark_client -b db1:5:10000,db2:3:10000
int main(int argc, char* argv[]) {
  int opt;
  while ((opt = getopt(argc, argv, "P:h:p:a:t:c:d:n:")) != -1) {
    switch (opt) {
      case 'h':
        hostname = std::string(optarg);
        break;
      case 'p':
        port = atoi(optarg);
        break;
      case 'P':
        transmit_mode = kPipeline;
        pipeline_num = atoi(optarg);
        break;
      case 'a':
        password = std::string(optarg);
        break;
      case 't':
        thread_num_each_table = atoi(optarg);
        break;
      case 'c':
        tables_str = std::string(optarg);
        break;
      case 'd':
        payload_size = atoi(optarg);
        break;
      case 'n':
        number_of_request = atoi(optarg);
        break;
      default:
        Usage();
        exit(-1);
    }
  }

  slash::StringSplit(tables_str, ',', tables);

  if (tables.empty()) {
    Usage();
    exit(-1);
  }

  std::chrono::system_clock::time_point start_time = std::chrono::system_clock::now();
  std::time_t now = std::chrono::system_clock::to_time_t(start_time);
  PrintInfo(now);

  for (const auto& table : tables) {
    for (size_t idx = 0; idx < thread_num_each_table; ++idx) {
      thread_args.push_back({0, table, idx});
    }
  }

  for (size_t idx = 0; idx < thread_args.size(); ++idx) {
    pthread_create(&thread_args[idx].tid, NULL, ThreadMain, &thread_args[idx]);
  }

  for (size_t idx = 0; idx < thread_args.size(); ++idx) {
    pthread_join(thread_args[idx].tid, NULL);
  }

  std::chrono::system_clock::time_point end_time = std::chrono::system_clock::now();
  now = std::chrono::system_clock::to_time_t(end_time);
  std::cout << "Finish Time : " << asctime(localtime(&now));

  auto hours = std::chrono::duration_cast<std::chrono::hours>(end_time - start_time).count();
  auto minutes = std::chrono::duration_cast<std::chrono::minutes>(end_time - start_time).count();
  auto seconds = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time).count();

  std::cout << "Total Time Cost : " << hours << " hours " << minutes % 60 << " minutes " << seconds % 60 << " seconds "
            << std::endl;
  return 0;
}
