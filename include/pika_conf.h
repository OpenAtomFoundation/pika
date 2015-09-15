#ifndef __PIKA_CONF_H__
#define __PIKA_CONF_H__
#include "pika_define.h"
#include "stdlib.h"
#include "stdio.h"
#include "xdebug.h"
#include "base_conf.h"

class PikaConf : public BaseConf
{
public:
    PikaConf(const char* path);
    int port() { return port_; }
    int thread_num() { return thread_num_; }
    char* log_path() { return log_path_; }
    int log_level() { return log_level_; }
    char* db_path() { return db_path_; }
    int write_buffer_size() { return write_buffer_size_; }

private:
    int port_;
    int thread_num_;
    char log_path_[PIKA_WORD_SIZE];
    char db_path_[PIKA_WORD_SIZE];
    int write_buffer_size_;
    int log_level_;
};

#endif
