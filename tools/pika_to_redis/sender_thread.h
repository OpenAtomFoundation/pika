#ifndef SENDER_THREAD_H_
#define SENDER_THREAD_H_

#include <sys/poll.h>
#include <iostream>
#include "pink/include/pink_cli.h"
#include "pink/include/pink_thread.h"
#include "slash/include/slash_mutex.h"
#include "nemo.h"

class SenderThread : public pink::Thread{
public:
  SenderThread(pink::PinkCli *cli);
  ~SenderThread();
  void LoadCmd(const std::string &cmd);
  void Stop();
  int64_t elements() {
    return elements_;
  }

  int64_t err() {
    return err_;
  }
  bool should_exit_;
private:
  static const int kReadable = 1;
  static const int kWritable = 2;
  static const size_t kBufSize = 2097152; //2M
  static const size_t kWirteLoopMaxBYTES = 1024 * 2048; // 10k cmds

  int Wait(int fd, int mask, long long milliseconds);

  pink::PinkCli *cli_;
  char buf_[kBufSize];
  size_t buf_len_ = 0;
  size_t buf_pos_ = 0;

  char rbuf_[kBufSize];
  int32_t rbuf_size_ = kBufSize;
  int32_t rbuf_pos_;
  int32_t rbuf_offset_;
  int64_t elements_;    // the elements number of this current reply
  int64_t err_;

  int TryRead();
  int TryReadType();

  char* ReadBytes(unsigned int bytes);
  char *seekNewline(char *s, size_t len);
  char* ReadLine(int *_len);

  slash::Mutex buf_mutex_;
  slash::CondVar buf_r_cond_;
  slash::CondVar buf_w_cond_;
  int64_t NowMicros_();
  virtual void *ThreadMain();
};
#endif
