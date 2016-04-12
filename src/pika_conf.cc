#include "sys/stat.h"
#include "pika_conf.h"
#include "glog/logging.h"

#include <fstream>

#include <iostream>

PikaConf::PikaConf(const std::string& path):
  slash::BaseConf(path), conf_path_(path)
{
  pthread_rwlock_init(&rwlock_, NULL);
}

int PikaConf::Load()
{
  int ret = LoadConf();
  if (ret != 0) {
    return ret;
  }

  // Mutable Section
  GetConfInt("log_level", &log_level_);

  GetConfInt("timeout", &timeout_);
  GetConfStr("requirepass", &requirepass_);
  GetConfStr("userpass", &userpass_);

  GetConfInt("maxconnection", &maxconnection_);
  if (maxconnection_ <= 0) {
    maxconnection_ = 20000;
  }

  GetConfInt("root_connection_num", &root_connection_num_);
  if (root_connection_num_ < 0) {
      root_connection_num_ = 2;
  }

  GetConfInt("slowlog_log_slower_than", &slowlog_log_slower_than_);
  
  std::string user_blacklist;
  GetConfStr("userblacklist", &user_blacklist);
  SetUserBlackList(std::string(user_blacklist));
  GetConfStr("dump_path", &bgsave_path_);
  GetConfInt("expire_logs_nums", &expire_logs_nums_);
  GetConfInt("expire_logs_days", &expire_logs_days_);
  GetConfStr("compression", &compression_);
  GetConfBool("slave-read-only", &readonly_);

  if (log_path_[log_path_.length() - 1] != '/') {
    log_path_ += "/";
  }

  if (bgsave_path_[bgsave_path_.length() - 1] != '/') {
    bgsave_path_ += "/";
  }
  
  //
  // Immutable Sections
  //

  GetConfInt("port", &port_);
  GetConfStr("log_path", &log_path_);
  GetConfStr("db_path", &db_path_);
  
  GetConfInt("thread_num", &thread_num_);
  if (thread_num_ <= 0) {
    thread_num_ = 12;
  }
  if (thread_num_ > 24) {
    thread_num_ = 24;
  }

  // write_buffer_size
  GetConfInt("write_buffer_size", &write_buffer_size_);
  if (write_buffer_size_ <= 0 ) {
      write_buffer_size_ = 4194304; // 40M
  }

  // target_file_size_base
  GetConfInt("target_file_size_base", &target_file_size_base_);
  if (target_file_size_base_ <= 0) {
      target_file_size_base_ = 1048576; // 10M
  }

  // daemonize
  std::string dmz;
  GetConfStr("daemonize", &dmz);
  daemonize_ =  (dmz == "yes") ? true : false;

  GetConfInt("binlog_file_size", &binlog_file_size_);
  if (binlog_file_size_ < 1024 || static_cast<int64_t>(binlog_file_size_) > (1024LL * 1024 * 1024)) {
    binlog_file_size_ = 100 * 1024 * 1024;    // 100M
  }

  GetConfStr("pidfile", &pidfile_);

  if (timeout_ <= 0) {
      timeout_ = 60; // 60s
  }
  if (expire_logs_days_ <= 0 ) {
      expire_logs_days_ = 1;
  }
  if (expire_logs_nums_ <= 10 ) {
      expire_logs_nums_ = 10;
  }

  return ret;
}

int PikaConf::ConfigRewrite() {
  SetConfInt("port", port_);
  SetConfInt("thread_num", thread_num_);
  SetConfStr("log_path", log_path_);
  SetConfInt("log_level", log_level_);
  SetConfStr("db_path", db_path_);
  SetConfInt("write_buffer_size", write_buffer_size_);
  SetConfInt("timeout", timeout_);
  SetConfStr("requirepass", requirepass_);
  SetConfStr("userpass", userpass_);
  SetConfStr("userblacklist", suser_blacklist());
  SetConfStr("dump_path", bgsave_path_);
  SetConfInt("maxconnection", maxconnection_);
  SetConfInt("root_connection_num", root_connection_num_);
  SetConfInt("slowlog_log_slower_than", slowlog_log_slower_than_);
  SetConfInt("target_file_size_base", target_file_size_base_);
  SetConfInt("expire_logs_nums", expire_logs_nums_);
  SetConfInt("expire_logs_days", expire_logs_days_);
  SetConfBool("slave_read_only", readonly_);

  SetConfInt("binlog_file_size_", binlog_file_size_);
  SetConfStr("compression", compression_);

  return WriteBack();
}
