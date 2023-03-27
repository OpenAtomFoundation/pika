#ifndef SCAN_H_
#define SCAN_H_

#include <vector>

#include "sender.h"
#include "net/include/bg_thread.h"

#include <iostream>

class ScanThread : public net::Thread {
  public:
    ScanThread(std::string filename, std::vector<SenderThread *> senders, int ttl) :
      filename_(filename),
      num_(0),
      senders_(senders),
      thread_index_(0),
      ttl_(ttl)
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
    int ttl_;

    void* ThreadMain();
};

#endif

