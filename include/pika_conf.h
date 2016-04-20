#ifndef __PIKA_CONF_H__
#define __PIKA_CONF_H__
#include <pthread.h>
#include "stdlib.h"
#include <string>
#include <vector>

#include "base_conf.h"
#include "slash_mutex.h"
#include "slash_string.h"
#include "pika_define.h"
#include "xdebug.h"

typedef slash::RWLock RWLock;

class PikaConf : public slash::BaseConf {
public:
  PikaConf(const std::string& path);
  ~PikaConf()             { pthread_rwlock_destroy(&rwlock_); }

  // Getter
  int port()              { RWLock l(&rwlock_, false); return port_; }
  int thread_num()        { RWLock l(&rwlock_, false); return thread_num_; }
  std::string log_path()  { RWLock l(&rwlock_, false); return log_path_; }
  int log_level()         { RWLock l(&rwlock_, false); return log_level_; }
  std::string db_path()   { RWLock l(&rwlock_, false); return db_path_; }
  std::string db_sync_path()   { RWLock l(&rwlock_, false); return db_sync_path_; }
  int db_sync_speed()   { RWLock l(&rwlock_, false); return db_sync_speed_; }
  int write_buffer_size() { RWLock l(&rwlock_, false); return write_buffer_size_; }
  int timeout()           { RWLock l(&rwlock_, false); return timeout_; }

  std::string requirepass()     { RWLock l(&rwlock_, false); return requirepass_; }
  std::string bgsave_path()     { RWLock l(&rwlock_, false); return bgsave_path_; }
  std::string bgsave_prefix()     { RWLock l(&rwlock_, false); return bgsave_prefix_; }
  std::string userpass()        { RWLock l(&rwlock_, false); return userpass_; }
  const std::string suser_blacklist() {
    RWLock l(&rwlock_, false);
    return slash::StringConcat(user_blacklist_, COMMA);
  }
  const std::vector<std::string>& vuser_blacklist() {
    RWLock l(&rwlock_, false); return user_blacklist_;
  }
  std::string compression()     { RWLock l(&rwlock_, false); return compression_; }
  int target_file_size_base()   { RWLock l(&rwlock_, false); return target_file_size_base_; }
  int expire_logs_nums()        { RWLock l(&rwlock_, false); return expire_logs_nums_; }
  int expire_logs_days()        { RWLock l(&rwlock_, false); return expire_logs_days_; }
  std::string conf_path()       { RWLock l(&rwlock_, false); return conf_path_; }
  bool readonly()               { RWLock l(&rwlock_, false); return readonly_; }
  int maxconnection()           { RWLock l(&rwlock_, false); return maxconnection_; }
  int root_connection_num()     { RWLock l(&rwlock_, false); return root_connection_num_; }
  int slowlog_slower_than()     { RWLock l(&rwlock_, false); return slowlog_log_slower_than_; }

  // Immutable config items, we don't use lock.
  bool daemonize()              { return daemonize_; }
  std::string pidfile()         { return pidfile_; }
  int binlog_file_size()        { return binlog_file_size_; }

  // Setter
  void SetPort(const int value)                 { RWLock l(&rwlock_, true); port_ = value; }
  void SetThreadNum(const int value)            { RWLock l(&rwlock_, true); thread_num_ = value; }
  void SetLogLevel(const int value)             { RWLock l(&rwlock_, true); log_level_ = value; }
  void SetTimeout(const int value)              { RWLock l(&rwlock_, true); timeout_ = value; }
  void SetBgsavePath(const std::string &value) {
    RWLock l(&rwlock_, true);
    bgsave_path_ = value;
    if (value[value.length() - 1] != '/') {
      bgsave_path_ += "/";
    }
  }
  void SetBgsavePrefix(const std::string &value) {
    RWLock l(&rwlock_, true);
    bgsave_prefix_ = value;
  }
  void SetRequirePass(const std::string &value) {
    RWLock l(&rwlock_, true);
    requirepass_ = value;
  }
  void SetUserPass(const std::string &value) {
    RWLock l(&rwlock_, true);
    userpass_ = value;
  }
  void SetUserBlackList(const std::string &value) {
    RWLock l(&rwlock_, true);
    slash::StringSplit(value, COMMA, user_blacklist_);
  }
  void SetReadonly(const bool value) {
    RWLock l(&rwlock_, true); readonly_ = value;
  }
  void SetExpireLogsNums(const int value){
    RWLock l(&rwlock_, true);
    expire_logs_nums_ = value;
  }
  void SetExpireLogsDays(const int value) {
    RWLock l(&rwlock_, true);
    expire_logs_days_ = value;
  }
  void SetMaxConnection(const int value) {
    RWLock l(&rwlock_, true);
    maxconnection_ = value;
  }
  void SetRootConnectionNum(const int value) {
    RWLock l(&rwlock_, true);
    root_connection_num_ = value;
  }
  void SetSlowlogSlowerThan(const int value) {
    RWLock l(&rwlock_, true);
    slowlog_log_slower_than_ = value;
  }
  void SetDbSyncSpeed(const int value) {
    RWLock l(&rwlock_, true);
    db_sync_speed_ = value;
  }

  int Load();
  int ConfigRewrite();

private:
  int port_;
  int thread_num_;
  std::string log_path_;
  std::string db_path_;
  std::string db_sync_path_;
  int db_sync_speed_;
  int write_buffer_size_;
  int log_level_;
  bool daemonize_;
  int timeout_;
  std::string requirepass_;
  std::string userpass_;
  std::vector<std::string> user_blacklist_;
  std::string bgsave_path_;
  std::string bgsave_prefix_;
  std::string pidfile_;

  //char pidfile_[PIKA_WORD_SIZE];
  std::string compression_;
  int maxconnection_;
  int root_connection_num_;
  int slowlog_log_slower_than_;
  int expire_logs_days_;
  int expire_logs_nums_;
  bool readonly_;
  std::string conf_path_;
  //char username_[30];
  //char password_[30];

  //
  // Critical configure items
  //
  int target_file_size_base_;
  int binlog_file_size_;

  pthread_rwlock_t rwlock_;
};

#endif
