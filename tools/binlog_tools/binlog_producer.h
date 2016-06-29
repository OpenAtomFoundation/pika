#ifndef PIKA_TOOLS_BINLOGTOOLS_BINLOG_PRODUCER_H_
#define PIKA_TOOLS_BINLOGTOOLS_BINLOG_PRODUCER_H_

//#include "redis_cli.h"
#include <atomic>
#include <cstdio>
#include <list>
#include <string>
#include <deque>
#include <pthread.h>

#include "slice.h"
#include "slash_mutex.h"
#include "status.h"

#ifndef __STDC_FORMAT_MACROS
# define __STDC_FORMAT_MACROS
# include <inttypes.h>
#endif 

#include "env.h"
#include "pika_define.h"


#include "binlog.h"

// binlog consumer can read a binlog file and parse the data in it
class BinlogProducer {
 public:

  BinlogProducer(const std::string& binlog_path);

  virtual ~BinlogProducer();

  Status Put(const std::string& scratch);
  Status LoadFile(uint32_t file);
  Status LoadNextFile();
  virtual Status Produce(const Slice &item, int *temp_pro_offset);
  virtual Status EmitPhysicalRecord(RecordType t, const char *ptr, size_t n, int *temp_pro_offset) = 0;
  Binlog* logger_;
  

  size_t header_size_;
  uint32_t pro_num_;
  Version * version_;
  slash::WritableFile *queue_;
  slash::RWFile *versionfile_;
  const std::string binlog_path_;
  std::string filename_;
  uint32_t pro_offset_;
  int block_offset_;

};

class NewBinlogProducer : public BinlogProducer {
 public:

  NewBinlogProducer(const std::string& binlog_path);

  virtual ~NewBinlogProducer();


  virtual Status EmitPhysicalRecord(RecordType t, const char *ptr, size_t n, int *temp_pro_offset);
};

class OldBinlogProducer : public BinlogProducer {
 public:

  OldBinlogProducer(const std::string& binlog_path);

  virtual ~OldBinlogProducer();


  virtual Status EmitPhysicalRecord(RecordType t, const char *ptr, size_t n, int *temp_pro_offset);
};

#endif
