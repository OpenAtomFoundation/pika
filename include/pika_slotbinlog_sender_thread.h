#ifndef PIKA_SLOTBINLOG_SENDER_THREAD_H_
#define PIKA_SLOTBINLOG_SENDER_THREAD_H_

#include "pink_thread.h"
//#include "redis_cli.h"
#include "slice.h"
#include "status.h"

#include "env.h"
#include "slash_mutex.h"


namespace pink {
class RedisCli;
}

class PikaSlotBinlogSenderThread : public pink::Thread {
 public:

  PikaSlotBinlogSenderThread(std::string &ip, int port, slash::SequentialFile *queue, uint32_t filenum, uint64_t con_offset);

  virtual ~PikaSlotBinlogSenderThread();

  /*
   * Get and Set
   */
  uint64_t last_record_offset () {
    slash::RWLock l(&rwlock_, false);
    return last_record_offset_;
  }
  uint32_t filenum() {
    slash::RWLock l(&rwlock_, false);
    return filenum_;
  }
  uint64_t con_offset() {
    slash::RWLock l(&rwlock_, false);
    return con_offset_;
  }

  int trim();
  uint64_t get_next(bool &is_error);


 private:

  slash::Status Parse(std::string &scratch);
  slash::Status Consume(std::string &scratch);
  unsigned int ReadPhysicalRecord(slash::Slice *fragment);

  uint64_t con_offset_;
  uint32_t filenum_;

  uint64_t initial_offset_;
  uint64_t last_record_offset_;
  uint64_t end_of_buffer_offset_;

  slash::SequentialFile* queue_;
  char* const backing_store_;
  slash::Slice buffer_;


  std::string ip_;
  int port_;

  pthread_rwlock_t rwlock_;


//  int sockfd_;
  pink::RedisCli *cli_;

 // bool Init();
 // bool Connect();
 // bool Send(const std::string &msg);
 // bool Recv();

  virtual void* ThreadMain();
};

#endif
