#ifndef SCAN_H_
#define SCAN_H_

#include "pink/include/bg_thread.h"

#include <iostream>

class ScanThread : public pink::Thread {
  public:
    ScanThread(std::string filename) :
      filename_(filename),
      num_(0)
      {
      }

    virtual ~ScanThread();
    int Num() {
      return num_; 
    }
  private:
    void ScanFile();
    std::string filename_;
    int num_;

    void* ThreadMain();
};

#endif

