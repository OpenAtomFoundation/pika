#ifndef BINLOG_CONF_H_
#define BINLOG_CONF_H_

#include <string>

class PortConf {
public:
  PortConf() {
    local_ip = "127.0.0.1";
    local_port = 0;
    master_ip = "127.0.0.1";
    master_port = 0;
    forward_ip = "127.0.0.1";
    forward_port = 0;
	forward_thread_num = 1;
    filenum = size_t(UINT32_MAX); // src/pika_trysync_thread.cc:48
    offset = 0;
    log_path = "./log/";
    dump_path = "./rsync_dump/";
  }

public:
    size_t filenum;
    size_t offset;
    std::string local_ip;
    int local_port;
    std::string master_ip;
    int master_port;
    std::string forward_ip;
    int forward_port;
    std::string forward_passwd;
	int forward_thread_num;
    std::string passwd;
    std::string log_path;
    std::string dump_path;
};

extern PortConf g_port_conf;

#endif
