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
    getConfStr("qbus_cluster", qbus_cluster_);
    getConfStr("qbus_topic", qbus_topic_);
    getConfStr("qbus_conf_path", qbus_conf_path_);
}
