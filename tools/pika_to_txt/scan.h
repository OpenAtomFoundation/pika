#ifndef SCAN_H_
#define SCAN_H_

#include <iostream>
#include "nemo.h"
#include "write.h"
#include "slash/include/slash_coding.h"

class ScanThread : public pink::Thread {
  public:
    ScanThread(nemo::Nemo *db, WriteThread * write_thread) :
      db_(db),
      write_thread_(write_thread),
      num_(0)
      {
      }

    virtual ~ScanThread();
    int Num() {
      return num_; 
    }
  private:
    void Scandb();
    nemo::Nemo* db_;
    WriteThread* write_thread_;
    int64_t num_;

    void* ThreadMain();
};

#endif

