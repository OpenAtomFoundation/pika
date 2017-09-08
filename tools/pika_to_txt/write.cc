#include <iostream>
#include <fstream>

#include "write.h"

WriteThread::~WriteThread() {
}

void WriteThread::Load(std::string data) {
  data_mutex_.Lock();
  if (data_.size() < 100000) {
    data_.push(data);
    rsignal_.Signal();
    data_mutex_.Unlock();
  } else {
    while (data_.size() > 100000) {
      wsignal_.Wait(); 
    }
    data_.push(data);
    rsignal_.Signal();
    data_mutex_.Unlock();
  }
}

void *WriteThread::ThreadMain() {
  std::cout << "Start write to file" << std::endl;
  std::fstream fout(filename_, std::ios::out|std::ios::binary);
  while(!should_exit_ || QueueSize() != 0) {
    data_mutex_.Lock();
    while (data_.size() == 0 && !should_exit_) {
      rsignal_.Wait();
    }
    data_mutex_.Unlock();

    if (QueueSize() != 0) {
      data_mutex_.Lock();
      std::string data = data_.front();
      std::cout << data << std::endl;
      data_.pop();  
      wsignal_.Signal();
      data_mutex_.Unlock();
      fout.write(data.c_str(), data.size());
      num_++;
    }
  }
  std::cout <<"Write to " << filename_ <<" complete";
  fout.close();
  return NULL;
}

