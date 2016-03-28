#ifndef PIKA_BINLOG_H_
#define PIKA_BINLOG_H_

#include <cstdio>
#include <list>
#include <string>
#include <deque>
#include <pthread.h>

#ifndef __STDC_FORMAT_MACROS
# define __STDC_FORMAT_MACROS
# include <inttypes.h>
#endif 

#include "env.h"
//#include "port.h"
#include "pika_define.h"

#include "slash_status.h"
#include "slash_mutex.h"

using slash::Status;
using slash::Slice;


std::string NewFileName(const std::string name, const uint32_t current);

class Version;

class Binlog
{
 public:
  Binlog(const std::string& Binlog_path, const int file_size = 100 * 1024 * 1024);
  ~Binlog();

  void Lock()         { mutex_.Lock(); }
  void Unlock()       { mutex_.Unlock(); }

  Status Put(const std::string &item);
  Status Put(const char* item, int len);

  Status GetProducerStatus(uint32_t* filenum, uint64_t* pro_offset);
  /*
   * Set Producer pro_num and pro_offset with lock
   */
  Status SetProducerStatus(uint32_t filenum, uint64_t pro_offset);


  // Set the filenum and con_offset of the consumer which has the given ip and port;
  // return NotFound when can not find the consumer with the given ip and port;
  // return InvalidArgument when the filenum and con_offset are invalid;
  Status SetConsumer(int fd, uint32_t filenum, uint64_t con_offset);
  // no lock
  Status GetConsumerStatus(int fd, uint32_t* filenum, uint64_t* con_offset);

  static Status AppendBlank(slash::WritableFile *file, uint64_t len);

  slash::WritableFile *queue() { return queue_; }
  //slash::WritableFile *writefile() { return writefile_; }
  uint64_t file_size() {
    return file_size_;
  }

  std::string filename;
  Version* version_;

 private:

  void InitLogFile();
  Status EmitPhysicalRecord(RecordType t, const char *ptr, size_t n, int *temp_pro_offset);

  /*
   * Produce
   */
  Status Produce(const Slice &item, int *pro_offset);

  uint32_t consumer_num_;
  uint64_t item_num_;

  slash::WritableFile *queue_;
  slash::RWFile *versionfile_;

  slash::Mutex mutex_;

  uint32_t pro_num_;
  int32_t retry_;

  int block_offset_;

  char* pool_;
  bool exit_all_consume_;
  const std::string binlog_path_;

  uint64_t file_size_;
  // No copying allowed
  Binlog(const Binlog&);
  void operator=(const Binlog&);
};

class Version {
 public:
  Version(slash::RWFile *save);
  ~Version();

  // Status Recovery(WritableFile *save);

  Status StableSave();
  Status Init();

  uint64_t pro_offset() { 
    slash::RWLock(&rwlock_, false);
    return pro_offset_;
  }
  void set_pro_offset(uint64_t pro_offset) {
    slash::RWLock(&rwlock_, true);
    pro_offset_ = pro_offset;
  }
  void rise_pro_offset(uint64_t r) {
    slash::RWLock(&rwlock_, true);
    pro_offset_ += r;
  }

  uint32_t pro_num() {
    slash::RWLock(&rwlock_, false);
    return pro_num_;
  }
  void set_pro_num(uint32_t pro_num) {
    slash::RWLock(&rwlock_, true);
    pro_num_ = pro_num;
  }

  uint32_t item_num() {
    slash::RWLock(&rwlock_, false);
    return item_num_;
  }
  void set_item_num(uint32_t item_num) {
    slash::RWLock(&rwlock_, true);
    item_num_ = item_num;
  }
  void plus_item_num() {
    slash::RWLock(&rwlock_, true);
    item_num_++;
  }
  void minus_item_num() {
    slash::RWLock(&rwlock_, true);
    item_num_--;
  }

  void debug() {
    slash::RWLock(&rwlock_, false);
    printf ("Current pro_num %u pro_offset %lu\n", pro_num_, pro_offset_);
  }

 private:

  uint64_t pro_offset_;
  uint32_t pro_num_;


  slash::RWFile *save_;
  pthread_rwlock_t rwlock_;
  // port::Mutex mutex_;

  // Not used
  uint64_t con_offset_;
  uint32_t con_num_;
  uint32_t item_num_;

  // No copying allowed;
  Version(const Version&);
  void operator=(const Version&);
};

#endif
