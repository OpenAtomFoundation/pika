#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <chrono>
#include <ctime>
#include <functional>
#include <iostream>
#include <set>
#include <random>
#include <vector>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "monitoring/histogram.h"
#include "hiredis/hiredis.h"

#include "pstd/include/pstd_status.h"
#include "pstd/include/pstd_string.h"
#include "pstd/include/env.h"

DEFINE_string(command, "generate", "command to execute, eg: generate/get/set/zadd");
DEFINE_bool(pipeline, false, "whether to enable pipeline");
DEFINE_string(host, "127.0.0.1", "target server's host");
DEFINE_int32(port, 9221, "target server's listen port");
DEFINE_int32(timeout, 1000, "request timeout");
DEFINE_string(password, "", "password");
DEFINE_int32(key_size, 50, "key size int bytes");
DEFINE_int32(value_size, 100, "value size in bytes");
DEFINE_int32(count, 100000, "request counts");
DEFINE_int32(thread_num, 10, "concurrent thread num");
DEFINE_string(dbs, "0", "dbs name, eg: 0,1,2");
DEFINE_int32(element_count, 1, "elements number in hash/list/set/zset");
DEFINE_bool(compare_value, false, "whether compare result or not");

using std::default_random_engine;
using pstd::Status;

struct RequestStat {
  int success_cnt = 0;
  int timeout_cnt = 0;
  int error_cnt = 0;
  RequestStat operator+(const RequestStat& stat) {
    RequestStat res;
    res.success_cnt = this->success_cnt + stat.success_cnt;
    res.timeout_cnt = this->timeout_cnt + stat.timeout_cnt;
    res.error_cnt = this->error_cnt + stat.error_cnt;
    return res;
  }
};

struct ThreadArg {
  ThreadArg(pthread_t t, const std::string& tn, int i)
      : idx(i), tid(t), table_name(tn), stat() {}
  int idx;
  pthread_t tid;
  std::string table_name;
  RequestStat stat;
};

thread_local int last_seed = 0;
std::vector<ThreadArg> thread_args;
std::vector<std::string> tables;
std::unique_ptr<rocksdb::HistogramImpl> hist;
int pipeline_num = 0;

bool CompareValue(std::set<std::string> expect, const redisReply* reply) {
  if (!FLAGS_compare_value) {
    return true;
  }
  if (reply == nullptr ||
      reply->type != REDIS_REPLY_ARRAY ||
      reply->elements != expect.size()) {
    return false;
  }
  for (size_t i = 0; i < reply->elements; i++) {
    std::string key = reply->element[i]->str;
    auto it = expect.find(key);
    if (it == expect.end()) {
      return false;
    }
    expect.erase(key);
  }
  return expect.size() == 0;
}

// for hash type
bool CompareValue(std::map<std::string, std::string> expect, const redisReply* reply) {
  if (!FLAGS_compare_value) {
    return true;
  }
  if (reply == nullptr ||
      reply->type != REDIS_REPLY_ARRAY ||
      reply->elements != expect.size() * 2) {
    return false;
  }
  std::unordered_map<std::string, std::string> actual;
  for (size_t i = 0; i < reply->elements; i++) {
    std::string key = reply->element[i]->str;
    std::string value = reply->element[++i]->str;
    auto it = expect.find(key);
    if (it == expect.end() ||
        it->second != value) {
      return false;
    }
    expect.erase(key);
  }
  return expect.size() == 0;
}

// for string type
bool CompareValue(const std::string& expect, const std::string& actual) {
  if (!FLAGS_compare_value) {
    return true;
  }
  return expect == actual;
}

void PrepareKeys(int suffix, std::vector<std::string>* keys) {
  keys->resize(FLAGS_count);
  std::string filename = "benchmark_keyfile_" + std::to_string(suffix);
  FILE* fp = fopen(filename.c_str(), "r");
  for (int idx = 0; idx < FLAGS_count; ++idx) {
    char* key = new char[FLAGS_key_size + 2];
    fgets(key, FLAGS_key_size + 2, fp);
    key[FLAGS_key_size] = '\0';
    (*keys)[idx] = std::string(key);
    delete []key;
  }
  fclose(fp);
}

void PreparePkeyMembers(int suffix, std::vector<std::pair<std::string, std::set<std::string>>>* keys) {
  keys->resize(FLAGS_count);
  std::string filename = "benchmark_keyfile_" + std::to_string(suffix);
  FILE* fp = fopen(filename.c_str(), "r");
  for (int idx = 0; idx < FLAGS_count; ++idx) {
    char* key = new char[FLAGS_key_size + 2];
    fgets(key, FLAGS_key_size + 2, fp);
    key[FLAGS_key_size] = '\0';
    std::set<std::string> elements;
    elements.insert(std::string(key));
    for (int idy = 1; idy < FLAGS_element_count; ++idy) {
      char* element = new char[FLAGS_key_size + 2];
      fgets(element, FLAGS_key_size + 2, fp);
      element[FLAGS_key_size] = '\0';
      elements.insert(std::string(element));
      delete []element;
    }
    (*keys)[idx] = std::make_pair(std::string(key), elements);
    delete []key;
  }
  fclose(fp);
}

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

void GenerateValue(const std::string& key, int32_t len, std::string* value) {
  if (!FLAGS_compare_value) {
    return GenerateRandomString(len, value);
  }
  value->resize(0);
  while (len - value->size() != 0) {
    size_t min_len = len - value->size();
    if (min_len > key.size()) {
      min_len = key.size();
    }
    value->append(key.substr(0, min_len));
  }
  return;
}

void PrintInfo(const std::time_t& now) {
  std::cout << "=================== Benchmark Client ===================" << std::endl;
  std::cout << "Server host name: " << FLAGS_host << std::endl;
  std::cout << "Server port: " << FLAGS_port << std::endl;
  std::cout << "command: " << FLAGS_command << std::endl;
  std::cout << "Thread num : " << FLAGS_thread_num << std::endl;
  std::cout << "Payload size : " << FLAGS_value_size << std::endl;
  std::cout << "Number of request : " << FLAGS_count << std::endl;
  std::cout << "Transmit mode: " << (FLAGS_pipeline ? "Pipeline" : "No Pipeline") << std::endl;
  std::cout << "Collection of dbs: " << FLAGS_dbs << std::endl;
  std::cout << "Elements num: " << FLAGS_element_count << std::endl;
  std::cout << "CompareValue : " << FLAGS_compare_value << std::endl;
  std::cout << "Startup Time : " << asctime(localtime(&now)) << std::endl;
  std::cout << "========================================================" << std::endl;
}

void RunGenerateCommand(int index) {
  std::string filename = "benchmark_keyfile_" + std::to_string(index);
  FILE* fp = fopen(filename.c_str(), "w+");
  if (fp == nullptr) {
    LOG(INFO) << "open file error";
    return;
  }
  for (int i = 0; i < FLAGS_count * FLAGS_element_count; i++) {
    std::string key;
    GenerateRandomString(FLAGS_key_size, &key);
    key.append("\n");
    fwrite(key.data(), sizeof(char), key.size(), fp);
  }
  fclose(fp);
}

redisContext* Prepare(ThreadArg* arg) {
  int index = arg->idx;
  std::string table = arg->table_name;
  struct timeval timeout = {FLAGS_timeout / 1000, (FLAGS_timeout % 1000) * 1000};
  redisContext* c = redisConnectWithTimeout(FLAGS_host.data(), FLAGS_port, timeout);
  if (!c || c->err) {
    if (c) {
      printf("Table: %s Thread %d, Connection error: %s\n",
             table.c_str(), index, c->errstr);
      redisFree(c);
    } else {
      printf("Table %s Thread %d, Connection error: "
             "can't allocate redis context\n", table.c_str(), index);
    }
    return nullptr;
  }

  if (!FLAGS_password.empty()) {
    const char* auth_argv[2] = {"AUTH", FLAGS_password.data()};
    size_t auth_argv_len[2] = {4, FLAGS_password.size()};
    auto res = reinterpret_cast<redisReply*>(
        redisCommandArgv(c, 2, reinterpret_cast<const char**>(auth_argv),
                         reinterpret_cast<const size_t*>(auth_argv_len)));
    if (!res) {
      printf("Table %s Thread %d Auth Failed: Get reply Error\n", table.c_str(), index);
      freeReplyObject(res);
      redisFree(c);
      return nullptr;
    } else {
      if (!strcasecmp(res->str, "OK")) {
      } else {
        printf("Table %s Thread %d Auth Failed: %s\n", table.c_str(), index, res->str);
        freeReplyObject(res);
        redisFree(c);
        return nullptr;
      }
    }
    freeReplyObject(res);
  }

  const char* select_argv[2] = {"SELECT", arg->table_name.data()};
  size_t select_argv_len[2] = {6, arg->table_name.size()};
  auto res = reinterpret_cast<redisReply*>(
      redisCommandArgv(c, 2, reinterpret_cast<const char**>(select_argv),
                       reinterpret_cast<const size_t*>(select_argv_len)));
  if (!res) {
    printf("Thread %d Select Table %s Failed, Get reply Error\n", index, table.c_str());
    freeReplyObject(res);
    redisFree(c);
    return nullptr;
  } else {
    if (!strcasecmp(res->str, "OK")) {
      printf("Table %s Thread %d Select DB Success, start to write data...\n",
             table.c_str(), index);
    } else {
      printf("Table %s Thread %d Select DB Failed: %s, thread exit...\n",
             table.c_str(), index, res->str);
      freeReplyObject(res);
      redisFree(c);
      return nullptr;
    }
  }
  return c;
}

Status RunGetCommand(redisContext*& c, ThreadArg* arg) {
  redisReply* res = nullptr;
  std::vector<std::string> keys;
  PrepareKeys(arg->idx, &keys);

  for (int idx = 0; idx < FLAGS_count; ++idx) {
    if (idx % 10000 == 0) {
      LOG(INFO) << "finish " << idx << " request";
    }
    const char* argv[2];
    size_t argvlen[2];
    std::string value;
    std::string key = keys[idx];
    argv[0] = "get";
    argvlen[0] = 3;
    argv[1] = key.c_str();
    argvlen[1] = key.size();
    GenerateValue(key, FLAGS_value_size, &value);

    uint64_t begin = pstd::NowMicros();
    res = reinterpret_cast<redisReply*>(
        redisCommandArgv(c, 2, reinterpret_cast<const char**>(argv),
                         reinterpret_cast<const size_t*>(argvlen)));
    hist->Add(pstd::NowMicros() - begin);

    if (!res) {
      LOG(INFO) << FLAGS_command << " timeout, key: " << key;
      arg->stat.timeout_cnt++;
      redisFree(c);
      c = Prepare(arg);
      if (!c) {
        return Status::InvalidArgument("reconnect failed");
      }
    } else if (res->type != REDIS_REPLY_STRING) {
      LOG(INFO) << FLAGS_command << " invalid type: " << res->type
                << " key: " << key;
      arg->stat.error_cnt++;
    } else {
      if (CompareValue(value, std::string(res->str))) {
        arg->stat.success_cnt++;
      } else {
        LOG(INFO) << FLAGS_command << " key: " << key
                  << " compare value failed";
        arg->stat.error_cnt++;
      }
    }

    freeReplyObject(res);
  }
  return Status::OK();
}

Status RunSAddCommand(redisContext*& c, ThreadArg* arg) {
  redisReply* res = nullptr;
  std::vector<std::pair<std::string, std::set<std::string>>> keys;
  PreparePkeyMembers(arg->idx, &keys);

  for (int idx = 0; idx < FLAGS_count; ++idx) {
    const char* argv[3];
    size_t argvlen[3];
    std::string pkey = keys[idx].first;
    argv[0] = "sadd";
    argvlen[0] = 4;
    argv[1] = pkey.c_str();
    argvlen[1] = pkey.size();
    for (const auto& member : keys[idx].second) {
      argv[2] = member.c_str();
      argvlen[2] = member.size();

      uint64_t begin = pstd::NowMicros();
      res = reinterpret_cast<redisReply*>(
          redisCommandArgv(c, 3, reinterpret_cast<const char**>(argv),
                           reinterpret_cast<const size_t*>(argvlen)));
      hist->Add(pstd::NowMicros() - begin);

      if (!res) {
        LOG(INFO) << FLAGS_command << " timeout, key: " << pkey;
        arg->stat.timeout_cnt++;
        redisFree(c);
        c = Prepare(arg);
        if (!c) {
          return Status::InvalidArgument("reconnect failed");
        }
      } else if (res->type != REDIS_REPLY_INTEGER) {
        LOG(INFO) << FLAGS_command << " invalid type: " << res->type
                  << " key: " << pkey;
        arg->stat.error_cnt++;
      } else {
        arg->stat.success_cnt++;
      }
      freeReplyObject(res);
    }
  }
  return Status::OK();
}

Status RunSMembersCommand(redisContext*& c, ThreadArg* arg) {
  redisReply* res = nullptr;
  std::vector<std::pair<std::string, std::set<std::string>>> keys;
  PreparePkeyMembers(arg->idx, &keys);

  for (int idx = 0; idx < FLAGS_count; ++idx) {
    if (idx % 10000 == 0) {
      LOG(INFO) << "finish " << idx << " request";
    }
    const char* argv[2];
    size_t argvlen[2];
    std::string pkey = keys[idx].first;
    auto elements = keys[idx].second;

    argv[0] = "smembers";
    argvlen[0] = 8;
    argv[1] = pkey.c_str();
    argvlen[1] = pkey.size();

    uint64_t begin = pstd::NowMicros();
    res = reinterpret_cast<redisReply*>(
        redisCommandArgv(c, 2, reinterpret_cast<const char**>(argv),
                         reinterpret_cast<const size_t*>(argvlen)));
    hist->Add(pstd::NowMicros() - begin);

    if (!res) {
      LOG(INFO) << FLAGS_command << " timeout, key: " << pkey;
      arg->stat.timeout_cnt++;
      redisFree(c);
      c = Prepare(arg);
      if (!c) {
        return Status::InvalidArgument("reconnect failed");
      }
    } else if (res->type != REDIS_REPLY_ARRAY) {
      LOG(INFO) << FLAGS_command << " invalid type: " << res->type
                << " key: " << pkey;
      arg->stat.error_cnt++;
    } else {
      if (CompareValue(elements, res)) {
        arg->stat.success_cnt++;
      } else {
        LOG(INFO) << FLAGS_command << " key: " << pkey
                  << " compare value failed";
        arg->stat.error_cnt++;
      }
    }
    freeReplyObject(res);
  }
  return Status::OK();
}

Status RunHGetAllCommand(redisContext*& c, ThreadArg* arg) {
  redisReply* res = nullptr;
  std::vector<std::pair<std::string, std::set<std::string>>> keys;
  PreparePkeyMembers(arg->idx, &keys);

  for (int idx = 0; idx < FLAGS_count; ++idx) {
    if (idx % 10000 == 0) {
      LOG(INFO) << "finish " << idx << " request";
    }
    const char* argv[2];
    size_t argvlen[2];
    std::string pkey = keys[idx].first;
    auto elements = keys[idx].second;
    std::map<std::string, std::string> m;
    for (const auto& ele : elements) {
      std::string value;
      GenerateValue(ele, FLAGS_value_size, &value);
      m[ele] = value;
    }

    argv[0] = "hgetall";
    argvlen[0] = 7;
    argv[1] = pkey.c_str();
    argvlen[1] = pkey.size();

    uint64_t begin = pstd::NowMicros();
    res = reinterpret_cast<redisReply*>(
        redisCommandArgv(c, 2, reinterpret_cast<const char**>(argv),
                         reinterpret_cast<const size_t*>(argvlen)));
    hist->Add(pstd::NowMicros() - begin);

    if (!res) {
      LOG(INFO) << FLAGS_command << " timeout, key: " << pkey;
      arg->stat.timeout_cnt++;
      redisFree(c);
      c = Prepare(arg);
      if (!c) {
        return Status::InvalidArgument("reconnect failed");
      }
    } else if (res->type != REDIS_REPLY_ARRAY) {
      LOG(INFO) << FLAGS_command << " invalid type: " << res->type
                << " key: " << pkey;
      arg->stat.error_cnt++;
    } else {
      if (CompareValue(m, res)) {
        arg->stat.success_cnt++;
      } else {
        LOG(INFO) << FLAGS_command << " key: " << pkey
                  << " compare value failed";
        arg->stat.error_cnt++;
      }
    }

    freeReplyObject(res);
  }
  return Status::OK();
}

Status RunHSetCommand(redisContext*& c, ThreadArg* arg) {
  redisReply* res = nullptr;
  std::vector<std::pair<std::string, std::set<std::string>>> keys;
  PreparePkeyMembers(arg->idx, &keys);

  for (int idx = 0; idx < FLAGS_count; ++idx) {
    const char* set_argv[4];
    size_t set_argvlen[4];
    std::string pkey;
    std::string value;

    pkey = keys[idx].first;
    set_argv[0] = "hset";
    set_argvlen[0] = 4;
    set_argv[1] = pkey.c_str();
    set_argvlen[1] = pkey.size();

    for (const auto& member : keys[idx].second) {
      GenerateValue(member, FLAGS_value_size, &value);
      set_argv[2] = member.c_str();
      set_argvlen[2] = member.size();
      set_argv[3] = value.c_str();
      set_argvlen[3] = value.size();

      uint64_t begin = pstd::NowMicros();
      res = reinterpret_cast<redisReply*>(
          redisCommandArgv(c, 4, reinterpret_cast<const char**>(set_argv),
                           reinterpret_cast<const size_t*>(set_argvlen)));
      hist->Add(pstd::NowMicros() - begin);

      if (!res) {
        LOG(INFO) << FLAGS_command << " timeout, key: " << pkey;
        arg->stat.timeout_cnt++;
        redisFree(c);
        c = Prepare(arg);
        if (!c) {
          return Status::InvalidArgument("reconnect failed");
        }
      } else if (res->type != REDIS_REPLY_INTEGER) {
        LOG(INFO) << FLAGS_command << " invalid type: " << res->type
                  << " key: " << pkey;
        arg->stat.error_cnt++;
      } else {
        arg->stat.success_cnt++;
      }
      freeReplyObject(res);
    }
  }
  return Status::OK();
}

Status RunSetCommand(redisContext*& c, ThreadArg* arg) {
  redisReply* res = nullptr;
  std::vector<std::string> keys;
  PrepareKeys(arg->idx, &keys);

  for (int idx = 0; idx < FLAGS_count; ++idx) {
    if (idx % 10000 == 0) {
      LOG(INFO) << "finish " << idx << " request";
    }
    const char* set_argv[3];
    size_t set_argvlen[3];

    std::string value;
    std::string key = keys[idx];
    GenerateValue(key, FLAGS_value_size, &value);

    set_argv[0] = "set";
    set_argvlen[0] = 3;
    set_argv[1] = key.c_str();
    set_argvlen[1] = key.size();
    set_argv[2] = value.c_str();
    set_argvlen[2] = value.size();

    uint64_t begin = pstd::NowMicros();

    res = reinterpret_cast<redisReply*>(
        redisCommandArgv(c, 3, reinterpret_cast<const char**>(set_argv),
                         reinterpret_cast<const size_t*>(set_argvlen)));
    hist->Add(pstd::NowMicros() - begin);

    if (!res) {
      LOG(INFO) << FLAGS_command << " timeout, key: " << key;
      arg->stat.timeout_cnt++;
      redisFree(c);
      c = Prepare(arg);
      if (!c) {
        return Status::InvalidArgument("reconnect failed");
      }
    } else if (res->type != REDIS_REPLY_STATUS) {
      LOG(INFO) << FLAGS_command << " invalid type: " << res->type
                << " key: " << key;
      arg->stat.error_cnt++;
    } else {
      arg->stat.success_cnt++;
    }
    freeReplyObject(res);
  }
  return Status::OK();
}

Status RunZAddCommand(redisContext*& c, ThreadArg* arg) {
  redisReply* res = nullptr;
  std::vector<std::pair<std::string, std::set<std::string>>> keys;
  PreparePkeyMembers(arg->idx, &keys);

  for (int idx = 0; idx < FLAGS_count; ++idx) {
    const char* argv[4];
    size_t argvlen[4];
    std::string pkey = keys[idx].first;
    argv[0] = "zadd";
    argvlen[0] = 4;
    argv[1] = pkey.c_str();
    argvlen[1] = pkey.size();
    int score = 0;
    for (const auto& member : keys[idx].second) {
      score++;
      argv[2] = std::to_string(score).c_str();
      argvlen[2] = std::to_string(score).size();
      argv[3] = member.c_str();
      argvlen[3] = member.size();

      uint64_t begin = pstd::NowMicros();
      res = reinterpret_cast<redisReply*>(
          redisCommandArgv(c, 4, reinterpret_cast<const char**>(argv),
                           reinterpret_cast<const size_t*>(argvlen)));
      hist->Add(pstd::NowMicros() - begin);

      if (!res) {
        LOG(INFO) << FLAGS_command << " timeout, key: " << pkey;
        arg->stat.timeout_cnt++;
        redisFree(c);
        c = Prepare(arg);
        if (!c) {
          return Status::InvalidArgument("reconnect failed");
        }
      } else if (res->type != REDIS_REPLY_INTEGER) {
        LOG(INFO) << FLAGS_command << " invalid type: " << res->type
                  << " key: " << pkey;
        arg->stat.error_cnt++;
      } else {
        arg->stat.success_cnt++;
      }

      freeReplyObject(res);
    }
  }
  return Status::OK();
}

Status RunZRangeCommand(redisContext*& c, ThreadArg* arg) {
  redisReply* res = nullptr;
  std::vector<std::pair<std::string, std::set<std::string>>> keys;
  PreparePkeyMembers(arg->idx, &keys);

  for (int idx = 0; idx < FLAGS_count; ++idx) {
    if (idx % 10000 == 0) {
      LOG(INFO) << "finish " << idx << " request";
    }
    const char* argv[4];
    size_t argvlen[4];
    std::string pkey = keys[idx].first;
    auto elements = keys[idx].second;

    argv[0] = "zrange";
    argvlen[0] = 6;
    argv[1] = pkey.c_str();
    argvlen[1] = pkey.size();
    argv[2] = "0";
    argvlen[2] = 1;
    argv[3] = "-1";
    argvlen[3] = 2;

    uint64_t begin = pstd::NowMicros();
    res = reinterpret_cast<redisReply*>(
        redisCommandArgv(c, 4, reinterpret_cast<const char**>(argv),
                         reinterpret_cast<const size_t*>(argvlen)));
    hist->Add(pstd::NowMicros() - begin);

    if (!res) {
      LOG(INFO) << FLAGS_command << " timeout, key: " << pkey;
      arg->stat.timeout_cnt++;
      redisFree(c);
      c = Prepare(arg);
      if (!c) {
        return Status::InvalidArgument("reconnect failed");
      }
    } else if (res->type != REDIS_REPLY_ARRAY) {
      LOG(INFO) << FLAGS_command << " invalid type: " << res->type
                << " key: " << pkey;
      arg->stat.error_cnt++;
    } else {
      if (CompareValue(elements, res)) {
        arg->stat.success_cnt++;
      } else {
        LOG(INFO) << FLAGS_command << " key: " << pkey
                  << " compare value failed";
        arg->stat.error_cnt++;
      }
    }

    freeReplyObject(res);
  }
  return Status::OK();
}

Status RunLPushCommand(redisContext*& c, ThreadArg* arg) {
  redisReply* res = nullptr;
  std::vector<std::pair<std::string, std::set<std::string>>> keys;
  PreparePkeyMembers(arg->idx, &keys);

  for (int idx = 0; idx < FLAGS_count; ++idx) {
    const char* argv[3];
    size_t argvlen[3];
    std::string pkey = keys[idx].first;
    argv[0] = "lpush";
    argvlen[0] = 5;
    argv[1] = pkey.c_str();
    argvlen[1] = pkey.size();
    for (const auto& member : keys[idx].second) {
      argv[2] = member.c_str();
      argvlen[2] = member.size();

      uint64_t begin = pstd::NowMicros();
      res = reinterpret_cast<redisReply*>(
          redisCommandArgv(c, 3, reinterpret_cast<const char**>(argv),
                           reinterpret_cast<const size_t*>(argvlen)));
      hist->Add(pstd::NowMicros() - begin);

      if (!res) {
        LOG(INFO) << FLAGS_command << " timeout, key: " << pkey;
        arg->stat.timeout_cnt++;
        redisFree(c);
        c = Prepare(arg);
        if (!c) {
          return Status::InvalidArgument("reconnect failed");
        }
      } else if (res->type != REDIS_REPLY_INTEGER) {
        LOG(INFO) << FLAGS_command << " invalid type: " << res->type
                  << " key: " << pkey;
        arg->stat.error_cnt++;
      } else {
        arg->stat.success_cnt++;
      }
      freeReplyObject(res);
    }
  }
  return Status::OK();
}

Status RunLRangeCommand(redisContext*& c, ThreadArg* arg) {
  redisReply* res = nullptr;
  std::vector<std::pair<std::string, std::set<std::string>>> keys;
  PreparePkeyMembers(arg->idx, &keys);

  for (int idx = 0; idx < FLAGS_count; ++idx) {
    if (idx % 10000 == 0) {
      LOG(INFO) << "finish " << idx << " request";
    }
    const char* argv[4];
    size_t argvlen[4];
    std::string pkey = keys[idx].first;
    auto elements = keys[idx].second;

    argv[0] = "lrange";
    argvlen[0] = 6;
    argv[1] = pkey.c_str();
    argvlen[1] = pkey.size();
    argv[2] = "0";
    argvlen[2] = 1;
    argv[3] = "-1";
    argvlen[3] = 2;

    uint64_t begin = pstd::NowMicros();
    res = reinterpret_cast<redisReply*>(
        redisCommandArgv(c, 4, reinterpret_cast<const char**>(argv),
                         reinterpret_cast<const size_t*>(argvlen)));
    hist->Add(pstd::NowMicros() - begin);

    if (!res) {
      LOG(INFO) << FLAGS_command << " timeout, key: " << pkey;
      arg->stat.timeout_cnt++;
      redisFree(c);
      c = Prepare(arg);
      if (!c) {
        return Status::InvalidArgument("reconnect failed");
      }
    } else if (res->type != REDIS_REPLY_ARRAY) {
      LOG(INFO) << FLAGS_command << " invalid type: " << res->type
                << " key: " << pkey;
      arg->stat.error_cnt++;
    } else {
      if (CompareValue(elements, res)) {
        arg->stat.success_cnt++;
      } else {
        LOG(INFO) << FLAGS_command << " key: " << pkey
                  << " compare value failed";
        arg->stat.error_cnt++;
      }
    }

    freeReplyObject(res);
  }
  return Status::OK();
}

void* ThreadMain(void* arg) {
  ThreadArg* ta = reinterpret_cast<ThreadArg*>(arg);
  last_seed = ta->idx;

  if (FLAGS_command == "generate") {
    RunGenerateCommand(ta->idx);
    return nullptr;
  }

  redisContext* c = Prepare(ta);
  if (!c) {
    return nullptr;
  }

  Status s;
  if (FLAGS_command == "get") {
    s = RunGetCommand(c, ta);
  } else if (FLAGS_command == "set") {
    s = RunSetCommand(c, ta);
  } else if (FLAGS_command == "hset") {
    s = RunHSetCommand(c, ta);
  } else if (FLAGS_command == "hgetall") {
    s = RunHGetAllCommand(c,ta);
  } else if (FLAGS_command == "sadd") {
    s = RunSAddCommand(c, ta);
  } else if (FLAGS_command == "smembers") {
    s = RunSMembersCommand(c,ta);
  } else if (FLAGS_command == "zadd") {
    s = RunZAddCommand(c, ta);
  } else if (FLAGS_command == "zrange") {
    s = RunZRangeCommand(c,ta);
  } else if (FLAGS_command == "lpush") {
    s = RunLPushCommand(c, ta);
  } else if (FLAGS_command == "lrange") {
    s = RunLRangeCommand(c,ta);
  }

  if (!s.ok()) {
    std::string thread_info = "Table " + ta->table_name + ", Thread " + std::to_string(ta->idx);
    printf("%s, %s, thread exit...\n", thread_info.c_str(), s.ToString().c_str());
  }
  redisFree(c);
  return nullptr;
}

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  pstd::StringSplit(FLAGS_dbs, ',', tables);
  if (tables.empty()) {
    exit(-1);
  }

  FLAGS_logtostdout = true;
  FLAGS_minloglevel = 0;
  FLAGS_logbufsecs = 0;
  ::google::InitGoogleLogging("benchmark_client");

  hist.reset(new rocksdb::HistogramImpl());
  std::chrono::system_clock::time_point start_time = std::chrono::system_clock::now();
  std::time_t now = std::chrono::system_clock::to_time_t(start_time);
  PrintInfo(now);

  for (const auto& table : tables) {
    for (int idx = 0; idx < FLAGS_thread_num; ++idx) {
      thread_args.push_back({0, table, idx});
    }
  }

  for (size_t idx = 0; idx < thread_args.size(); ++idx) {
    pthread_create(&thread_args[idx].tid, nullptr, ThreadMain, &thread_args[idx]);
  }

  RequestStat stat;
  for (size_t idx = 0; idx < thread_args.size(); ++idx) {
    pthread_join(thread_args[idx].tid, nullptr);
    stat = stat + thread_args[idx].stat;
  }

  std::chrono::system_clock::time_point end_time = std::chrono::system_clock::now();
  now = std::chrono::system_clock::to_time_t(end_time);
  std::cout << "Finish Time : " << asctime(localtime(&now));

  auto hours = std::chrono::duration_cast<std::chrono::hours>(end_time - start_time).count();
  auto minutes = std::chrono::duration_cast<std::chrono::minutes>(end_time - start_time).count();
  auto seconds = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time).count();


  std::cout << "Total Time Cost : " << hours << " hours " << minutes % 60 << " minutes " << seconds % 60 << " seconds "
            << std::endl;
  std::cout << "Timeout Count: " << stat.timeout_cnt << " Error Count: " << stat.error_cnt << std::endl;
  std::cout << "stats: " << hist->ToString() << std::endl;
  return 0;
}
