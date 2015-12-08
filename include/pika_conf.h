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
    ~PikaConf()             { pthread_rwlock_destroy(&rwlock_); }
    int port()              { RWLock l(&rwlock_, false); return port_; }
    int thread_num()        { RWLock l(&rwlock_, false); return thread_num_; }
    int slave_thread_num()  { RWLock l(&rwlock_, false); return slave_thread_num_; }
    char* log_path()        { RWLock l(&rwlock_, false); return log_path_; }
    int log_level()         { RWLock l(&rwlock_, false); return log_level_; }
    char* db_path()         { RWLock l(&rwlock_, false); return db_path_; }
    int write_buffer_size() { RWLock l(&rwlock_, false); return write_buffer_size_; }
    int timeout()           { RWLock l(&rwlock_, false); return timeout_; }
    bool daemonize()        { RWLock l(&rwlock_, false); return daemonize_; }
    char* requirepass()     { RWLock l(&rwlock_, false); return requirepass_; }
    char* conf_path()       { RWLock l(&rwlock_, false); return conf_path_; }
    char* dump_prefix()     { RWLock l(&rwlock_, false); return dump_prefix_; }
    char* dump_path()       { RWLock l(&rwlock_, false); return dump_path_; }
    int maxconnection()     { RWLock l(&rwlock_, false); return maxconnection_; }
    int target_file_size_base()     { RWLock l(&rwlock_, false); return target_file_size_base_; }
    int expire_logs_days()  { RWLock l(&rwlock_, false); return expire_logs_days_; }
    int expire_logs_nums()  { RWLock l(&rwlock_, false); return expire_logs_nums_; }

    void SetPort(const int value)                 { RWLock l(&rwlock_, true); port_ = value; }
    void SetThreadNum(const int value)            { RWLock l(&rwlock_, true); thread_num_ = value; }
    void SetLogLevel(const int value)             { RWLock l(&rwlock_, true); log_level_ = value; }
    void SetWriteBufferSize(const int value)      { RWLock l(&rwlock_, true); write_buffer_size_ = value; }
    void SetTimeout(const int value)              { RWLock l(&rwlock_, true); timeout_ = value; }
    void SetRequirePass(const std::string &value) {
        RWLock l(&rwlock_, true);
        snprintf (requirepass_, sizeof(requirepass_), "%s", value.data());
    }
    void SetDumpPrefix(const std::string &value) {
        RWLock l(&rwlock_, true);
        snprintf (dump_prefix_, sizeof(dump_prefix_), "%s", value.data());
    }
    void SetMaxConnection(const int value)                 { RWLock l(&rwlock_, true); maxconnection_ = value; }
    void SetExpireLogsDays(const int value)                 { RWLock l(&rwlock_, true); expire_logs_days_ = value; }
    void SetExpireLogsNums(const int value)                 { RWLock l(&rwlock_, true); expire_logs_nums_ = value; }

private:
    int port_;
    int thread_num_;
    int slave_thread_num_;
    char log_path_[PIKA_WORD_SIZE];
    char db_path_[PIKA_WORD_SIZE];
    int write_buffer_size_;
    int log_level_;
    bool daemonize_;
    int timeout_;
    char requirepass_[PIKA_WORD_SIZE];
    char dump_prefix_[PIKA_WORD_SIZE];
    char dump_path_[PIKA_WORD_SIZE];
    int maxconnection_;
    int target_file_size_base_;
    int expire_logs_days_;
    int expire_logs_nums_;

    char conf_path_[PIKA_WORD_SIZE];
    pthread_rwlock_t rwlock_;
};

#endif
