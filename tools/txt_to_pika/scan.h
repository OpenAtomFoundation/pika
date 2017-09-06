#ifndef SCAN_H_
#define SCAN_H_

#include <vector>

#include "sender.h"
#include "pink/include/bg_thread.h"

#include <iostream>

class ScanThread : public pink::Thread {
  public:
    ScanThread(std::string filename, std::vector<SenderThread *> senders) :
      filename_(filename),
      num_(0),
      senders_(senders),
      thread_index_(0)
      {
      }

    virtual ~ScanThread();
    int Num() {
      return num_; 
    }
    void DispatchCmd(const std::string &cmd);
  private:
    void ScanFile();
    std::string filename_;
    int num_;
    std::vector<SenderThread *> senders_;
    int thread_index_;

    void* ThreadMain();
};

#endif

