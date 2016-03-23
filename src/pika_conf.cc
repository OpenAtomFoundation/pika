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
  GetConfInt("port", &port_);
  GetConfInt("thread_num", &thread_num_);
  GetConfInt("slave_thread_num", &slave_thread_num_);
  GetConfStr("log_path", &log_path_);
  GetConfInt("log_level", &log_level_);
  GetConfStr("db_path", &db_path_);
  GetConfInt("write_buffer_size", &write_buffer_size_);
  GetConfInt("timeout", &timeout_);
  GetConfStr("requirepass", &requirepass_);
  GetConfStr("userpass", &userpass_);

  std::string user_blacklist;
  GetConfStr("userblacklist", &user_blacklist);
  SetUserBlackList(std::string(user_blacklist));
  GetConfStr("dump_path", &bgsave_path_);
  GetConfInt("target_file_size_base", &target_file_size_base_);
  GetConfBool("slave-read-only", &readonly_);

  if (bgsave_path_[bgsave_path_.length() - 1] != '/') {
    bgsave_path_ += "/";
  }
  
  std::string dmz;
  GetConfStr("daemonize", &dmz);
  daemonize_ =  (dmz == "yes") ? true : false;

  GetConfStr("pidfile", &pidfile_);

  return ret;
  //if (thread_num_ <= 0) {
  //    thread_num_ = 16;
  //}
  //if (slave_thread_num_ <= 0) {
  //    slave_thread_num_ = 7;
  //}
  //if (write_buffer_size_ <= 0 ) {
  //    write_buffer_size_ = 4194304; // 40M
  //}
  //if (timeout_ <= 0) {
  //    timeout_ = 60; // 60s
  //}
  //if (maxconnection_ <= 0) {
  //    maxconnection_ = 20000;
  //}
  //if (target_file_size_base_ <= 0) {
  //    target_file_size_base_ = 1048576; // 10M
  //}
  //if (expire_logs_days_ <= 0 ) {
  //    expire_logs_days_ = 1;
  //}
  //if (expire_logs_nums_ <= 10 ) {
  //    expire_logs_nums_ = 10;
  //}
  //if (root_connection_num_ < 0) {
  //    root_connection_num_ = 0;
  //}
  //if (db_sync_speed_ < 0 || db_sync_speed_ > 125) {
  //    db_sync_speed_ = 125;
  //}


  //if (binlog_file_size_ < 1024 || static_cast<int64_t>(binlog_file_size_) > (1024LL * 1024 * 1024 * 2)) {
  //  binlog_file_size_ = 100 * 1024 * 1024;    // 100M
  //}
}

int PikaConf::ConfigRewrite() {
  SetConfInt("port", port_);
  SetConfInt("thread_num", thread_num_);
  SetConfInt("slave_thread_num", slave_thread_num_);
  SetConfStr("log_path", log_path_);
  SetConfInt("log_level", log_level_);
  SetConfStr("db_path", db_path_);
  SetConfInt("write_buffer_size", write_buffer_size_);
  SetConfInt("timeout", timeout_);
  SetConfStr("requirepass", requirepass_);
  SetConfStr("userpass", userpass_);
  SetConfStr("userblacklist", suser_blacklist());
  SetConfStr("dump_path", bgsave_path_);
  SetConfInt("target_file_size_base", target_file_size_base_);
  SetConfBool("slave_read_only", readonly_);

  return WriteBack();
}
