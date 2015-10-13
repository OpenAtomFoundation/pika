#ifndef __PIKA_CONF_H__
#define __PIKA_CONF_H__
#include <pthread.h>
#include "stdlib.h"
#include "stdio.h"

#include "mutexlock.h"
#include "pika_define.h"
#include "xdebug.h"
#include "base_conf.h"

class PikaConf : public BaseConf
{
public:
    PikaConf(const char* path);
    int port()              { RWLock l(&rwlock_, false); return port_; }
    int thread_num()        { RWLock l(&rwlock_, false); return thread_num_; }
    char* log_path()        { RWLock l(&rwlock_, false); return log_path_; }
    int log_level()         { RWLock l(&rwlock_, false); return log_level_; }
    char* db_path()         { RWLock l(&rwlock_, false); return db_path_; }
    int write_buffer_size() { RWLock l(&rwlock_, false); return write_buffer_size_; }
    int timeout()           { RWLock l(&rwlock_, false); return timeout_; }
    char* requirepass()     { RWLock l(&rwlock_, false); return requirepass_; }

    void SetPort(const int value)                 { RWLock l(&rwlock_, true); port_ = value; }
    void SetThreadNum(const int value)            { RWLock l(&rwlock_, true); thread_num_ = value; }
    void SetLogLevel(const int value)             { RWLock l(&rwlock_, true); log_level_ = value; }
    void SetWriteBufferSize(const int value)      { RWLock l(&rwlock_, true); write_buffer_size_ = value; }
    void SetTimeout(const int value)              { RWLock l(&rwlock_, true); timeout_ = value; }
    void SetRequirePass(const std::string &value) {
        RWLock l(&rwlock_, true);
        snprintf (requirepass_, sizeof(requirepass_), "%s", value.data());
    }
 //   void setLogPath(const std::string &value) {
 //       RWLock l(&rwlock_, true);
 //       snprintf (log_path_, sizeof(log_path_), "%s", value.data());
 //   }
 //   void setDBPath(const std::string &value) {
 //       RWLock l(&rwlock_, true);
 //       snprintf (db_path_, sizeof(db_path_), "%s", value.data());
 //   }
private:
    int port_;
    int thread_num_;
    char log_path_[PIKA_WORD_SIZE];
    char db_path_[PIKA_WORD_SIZE];
    int write_buffer_size_;
    int log_level_;
    int timeout_;
    char requirepass_[PIKA_WORD_SIZE];

    pthread_rwlock_t rwlock_;
};

#endif
