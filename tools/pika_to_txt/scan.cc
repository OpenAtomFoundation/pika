#include "scan.h"

ScanThread::~ScanThread() {
}

void ScanThread::Scandb() {
  nemo::KIterator *it = db_->KScan("", "", -1, false);
  std::string key, value;

  while (it->Valid()) {
    key = it->key();
    value = it->value();
    uint32_t key_len, value_len; 
    std::string data_info;
    key_len = key.length();
    slash::PutFixed32(&data_info, key_len);
    data_info += key;
    value_len = value.length();
    slash::PutFixed32(&data_info, value_len);
    data_info += value;

    write_thread_->Load(data_info);
    num_++;
    it->Next();
  }
  delete it;
}

void *ScanThread::ThreadMain() {
  Scandb();
  write_thread_->should_exit_ = true;
  return NULL;
}
