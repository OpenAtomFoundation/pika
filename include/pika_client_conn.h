#ifndef PIKA_CLIENT_CONN_H_
#define PIKA_CLIENT_CONN_H_

#include <glog/logging.h>
#include <atomic>

#include "redis_conn.h"
#include "pink_thread.h"
#include "pika_command.h"


class PikaWorkerThread;

class PikaClientConn: public pink::RedisConn {
public:
  PikaClientConn(int fd, std::string ip_port, pink::Thread *thread);
  virtual ~PikaClientConn();
  virtual int DealMessage();

private:
  PikaWorkerThread* self_thread_;
  std::string DoCmd(const std::string& opt);
  std::string RestoreArgs();

  // Auth related
  class AuthStat {
  public:
    void Init();
    bool IsAuthed(const std::string& opt);
    bool ChecknUpdate(const std::string& arg);
  private:
    enum StatType {
      kNoAuthed = 0,
      kAdminAuthed,
      kLimitAuthed,
    };
    StatType stat_;
  };
  AuthStat auth_stat_;
};

#endif
