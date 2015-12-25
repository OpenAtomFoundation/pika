#include "sys/stat.h"
#include "base_conf.h"
#include "pika_conf.h"


PikaConf::PikaConf(const char* path) :
    BaseConf(path)
{
    strcpy(conf_path_, path);

    getConfInt("port", &port_);
    getConfInt("thread_num", &thread_num_);
    getConfInt("slave_thread_num", &slave_thread_num_);
    getConfStr("log_path", log_path_);
    getConfInt("log_level", &log_level_);
    getConfStr("db_path", db_path_);
    getConfInt("write_buffer_size", &write_buffer_size_);
    getConfInt("timeout", &timeout_);
    getConfStr("requirepass", requirepass_);
    getConfStr("dump_prefix", dump_prefix_);
    getConfStr("dump_path", dump_path_);
    getConfInt("maxconnection", &maxconnection_);
    getConfInt("target_file_size_base", &target_file_size_base_);
    getConfInt("expire_logs_days", &expire_logs_days_);
    getConfInt("expire_logs_nums", &expire_logs_nums_);
    getConfInt("root_connection_num", &root_connection_num_);
    getConfInt("slowlog_log_slower_than", &slowlog_slower_than_);
    getConfInt("binlog_file_size", &binlog_file_size_);

    if (thread_num_ <= 0) {
        thread_num_ = 16;
    }
    if (slave_thread_num_ <= 0) {
        slave_thread_num_ = 7;
    }
    if (write_buffer_size_ <= 0 ) {
        write_buffer_size_ = 4194304; // 40M
    }
    if (timeout_ <= 0) {
        timeout_ = 60; // 60s
    }
    if (maxconnection_ <= 0) {
        maxconnection_ = 20000;
    }
    if (target_file_size_base_ <= 0) {
        target_file_size_base_ = 1048576; // 10M
    }
    if (expire_logs_days_ <= 0 ) {
        expire_logs_days_ = 1;
    }
    if (expire_logs_nums_ <= 10 ) {
        expire_logs_nums_ = 10;
    }
    if (root_connection_num_ < 0) {
        root_connection_num_ = 0;
    }

    char str[PIKA_WORD_SIZE];
    getConfStr("daemonize", str);

    if (strcmp(str, "yes") == 0) {
        daemonize_ = true;
    } else {
        daemonize_ = false;
    }

    if (binlog_file_size_ < 1024 || static_cast<int64_t>(binlog_file_size_) > (1024LL * 1024 * 1024 * 2)) {
      binlog_file_size_ = 100 * 1024 * 1024;    // 100M
    }

    pthread_rwlock_init(&rwlock_, NULL);
}
