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

    /*
     * The repetion return variable warpper
     * remember to add the initial getConf* in the constructer
     */
    int port() { return port_; }
    int thread_num() { return thread_num_; }
    char* log_path() { return log_path_; }
    int log_level() { return log_level_; }
    int hb_port() { return hb_port_; }
    char* seed() { return seed_; }
    int seed_port() { return seed_port_; }
    char* data_path() { return data_path_; }

private:
    int port_;
    int hb_port_;
    int thread_num_;
    char log_path_[TICK_WORD_SIZE];
    int log_level_;
    char seed_[TICK_WORD_SIZE];
    int seed_port_;
    char data_path_[TICK_WORD_SIZE];
};

#endif
