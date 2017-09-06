#include <iostream>
#include <fstream>
#include "scan.h"

ScanThread::~ScanThread() {
}

void ScanThread::ScanFile() {
  std::cout << "Start to scan" << std::endl;

  std::ifstream fout(filename_, std::ios::binary);
  std::string str_key, str_value;
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

  std::cout << key << ":" << value << std::endl;
  fout.close();
  delete []key;
  delete []value;
}

void *ScanThread::ThreadMain() {
  ScanFile();
  return NULL;
}
