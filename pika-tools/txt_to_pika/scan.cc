#include <iostream>
#include <fstream>
#include "scan.h"

ScanThread::~ScanThread() {
}

void ScanThread::ScanFile() {
  log_info("Start to scan");

  std::ifstream fout(filename_, std::ios::binary);
  fout.seekg(0, std::ios::end);
  uint64_t  filesize = fout.tellg();
  fout.seekg(0, std::ios::beg);

  uint64_t index = 0;
  while (index < filesize) {
    std::string str_key, str_value, cmd;
    uint32_t key_len;
    fout.read(reinterpret_cast<char *>(&key_len), sizeof(uint32_t));
    char * key = new char[key_len];
    fout.read(key, key_len);
    str_key.append(key, key_len);

    uint32_t value_len;
    fout.read(reinterpret_cast<char *>(&value_len), sizeof(uint32_t));
    char * value = new char[value_len];
    fout.read(value, value_len);
    str_value.append(value, value_len);

    net::RedisCmdArgsType argv;
    if (ttl_ > 0) {
      argv.push_back("SETEX");
      argv.push_back(str_key);
      argv.push_back(std::to_string(ttl_));
      argv.push_back(str_value);
    } else {
      argv.push_back("SET");
      argv.push_back(str_key);
      argv.push_back(str_value);
    }
    
    net::SerializeRedisCommand(argv, &cmd);
    
    DispatchCmd(cmd);
    num_++;

    delete []key;
    delete []value;
    
    index += key_len + value_len + sizeof(uint32_t) * 2;
  }
  fout.close();
}

void ScanThread::DispatchCmd(const std::string &cmd) {
  senders_[thread_index_]->LoadCmd(cmd);
  thread_index_ = (thread_index_ + 1) % senders_.size();
}

void *ScanThread::ThreadMain() {
  ScanFile();
  log_info("Scan file complete");
  return NULL;
}
