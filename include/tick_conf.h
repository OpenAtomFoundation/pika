#ifndef __TICK_CONF_H__
#define __TICK_CONF_H__
#include "tick_define.h"
#include "stdlib.h"
#include "stdio.h"
#include "xdebug.h"
#include "status.h"
#include "base_conf.h"

class TickConf : public BaseConf
{
public:
    TickConf(const char* path);
    int port() { return port_; }
    int thread_num() { return thread_num_; }
    char* log_path() { return log_path_; }
    int log_level() { return log_level_; }
    char* qbus_cluster() { return qbus_cluster_; }
    char* qbus_topic() { return qbus_topic_; }
    char* qbus_conf_path() { return qbus_conf_path_; }

private:
    int port_;
    int thread_num_;
    char log_path_[TICK_WORD_SIZE];
    int log_level_;
    char qbus_cluster_[TICK_WORD_SIZE];
    char qbus_topic_[TICK_WORD_SIZE];
    char qbus_conf_path_[TICK_WORD_SIZE];
};

#endif
