#include "sys/stat.h"
#include "base_conf.h"
#include "tick_conf.h"


TickConf::TickConf(const char* path) :
    BaseConf(path)
{
    getConfInt("port", &port_);
    getConfInt("thread_num", &thread_num_);
    getConfStr("log_path", log_path_);
    getConfInt("log_level", &log_level_);
    getConfInt("hb_port", &hb_port_);
    getConfStr("seed", seed_);
    getConfInt("seed_port", &seed_port_);
    getConfStr("data_path", data_path_);
}
