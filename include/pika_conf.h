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
  int slave_thread_num()  { RWLock l(&rwlock_, false); return slave_thread_num_; }
  std::string log_path()        { RWLock l(&rwlock_, false); return log_path_; }
  int log_level()         { RWLock l(&rwlock_, false); return log_level_; }
  std::string db_path()         { RWLock l(&rwlock_, false); return db_path_; }
  int write_buffer_size()         { RWLock l(&rwlock_, false); return write_buffer_size_; }
  int timeout()           { RWLock l(&rwlock_, false); return timeout_; }

  std::string requirepass()     { RWLock l(&rwlock_, false); return requirepass_; }
  std::string bgsave_path()     { RWLock l(&rwlock_, false); return bgsave_path_; }
  std::string userpass()     { RWLock l(&rwlock_, false); return userpass_; }
  const std::string suser_blacklist() {
    RWLock l(&rwlock_, false); return slash::StringConcat(user_blacklist_, COMMA);
  }
  const std::vector<std::string>& vuser_blacklist() {
    RWLock l(&rwlock_, false); return user_blacklist_;
  }
  int target_file_size_base()         { RWLock l(&rwlock_, false); return target_file_size_base_; }
  std::string conf_path()       { RWLock l(&rwlock_, false); return conf_path_; }

  // Setter
  void SetPort(const int value)                 { RWLock l(&rwlock_, true); port_ = value; }
  void SetThreadNum(const int value)            { RWLock l(&rwlock_, true); thread_num_ = value; }
  void SetLogLevel(const int value)             { RWLock l(&rwlock_, true); log_level_ = value; }
  void SetTimeout(const int value)              { RWLock l(&rwlock_, true); timeout_ = value; }
  void SetDumpPath(const std::string &value) {
    RWLock l(&rwlock_, true);
    bgsave_path_ = value;
    if (value[value.length() - 1] != '/') {
      bgsave_path_ += "/";
    }
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
  void AddToUserBlackList(const std::string &value) {
    RWLock l(&rwlock_, true);
    user_blacklist_.push_back(value);
  }
  int Load();
  int ConfigRewrite();
private:
  int port_;
  int thread_num_;
  int slave_thread_num_;
  std::string log_path_;
  std::string db_path_;
  //char master_db_sync_path_[PIKA_WORD_SIZE];
  //char slave_db_sync_path_[PIKA_WORD_SIZE];
  int write_buffer_size_;
  int log_level_;
  //bool daemonize_;
  int timeout_;
  std::string requirepass_;
  std::string userpass_;
  std::vector<std::string> user_blacklist_;
  std::string bgsave_path_;
  //char pidfile_[PIKA_WORD_SIZE];
  //char compression_[PIKA_WORD_SIZE];
  //int maxconnection_;
  int target_file_size_base_;
  //int expire_logs_days_;
  //int expire_logs_nums_;
  //int root_connection_num_;
  //int slowlog_slower_than_;
  //int binlog_file_size_;
  //bool readonly_;
  std::string conf_path_;
  //char username_[30];
  //char password_[30];
  //int db_sync_speed_;
  pthread_rwlock_t rwlock_;
};

#endif
