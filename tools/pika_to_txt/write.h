#ifndef WRITE_H_
#define WRITE_H_

#include <queue>
#include "pink/include/bg_thread.h"

class WriteThread : public pink::Thread {
  public:
    WriteThread(std::string& filename) : 
      should_exit_(false),
      num_(0),
      filename_(filename),
      rsignal_(&data_mutex_),
      wsignal_(&data_mutex_)
  {
  }
    void Stop() {
      should_exit_ = true;
	    data_mutex_.Lock();
	    rsignal_.Signal();
      data_mutex_.Unlock();
	  }
    ~WriteThread();

    void Load(std::string data);

    int QueueSize() {
      slash::MutexLock l(&data_mutex_);
      int len = data_.size();
      return len; 
    }

    int Num() {
      return num_; 
    }

    bool should_exit_;
  private:
    int num_;
    std::string filename_;
    std::queue<std::string> data_;
    slash::Mutex data_mutex_;
    slash::CondVar rsignal_;
    slash::CondVar wsignal_;
    
    virtual void *ThreadMain();
};

#endif

