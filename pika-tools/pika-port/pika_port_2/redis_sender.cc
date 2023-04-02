#include <time.h>
#include <unistd.h>
#include <glog/logging.h>
#include "redis_sender.h"
// #include "binlog_log.h"

static time_t kCheckDiff = 1;

RedisSender::RedisSender(int id, std::string ip, int64_t port, std::string password):
  id_(id),
  cli_(NULL),
  rsignal_(&commands_mutex_),
  wsignal_(&commands_mutex_),
  ip_(ip),
  port_(port),
  password_(password),
  should_exit_(false),
  elements_(0) {

  last_write_time_ = ::time(NULL);
}

RedisSender::~RedisSender() {
  DLOG(INFO) << "RedisSender thread " << id_ << " exit!!!";
}

void RedisSender::ConnectRedis() {
  while (cli_ == NULL) {
    // Connect to redis
    cli_ = net::NewRedisCli();
    cli_->set_connect_timeout(1000);
    slash::Status s = cli_->Connect(ip_, port_);
    if (!s.ok()) {
      delete cli_;
      cli_ = NULL;
      log_info("Can not connect to %s:%d: %s", ip_.data(), port_, s.ToString().data());
      continue;
    } else {
      // Connect success
      log_info("Connect to %s:%d:%s", ip_.data(), port_, s.ToString().data());

      // Authentication
      if (!password_.empty()) {
        net::RedisCmdArgsType argv, resp;
        std::string cmd;

        argv.push_back("AUTH");
        argv.push_back(password_);
        net::SerializeRedisCommand(argv, &cmd);
        slash::Status s = cli_->Send(&cmd);

        if (s.ok()) {
          s = cli_->Recv(&resp);
          if (resp[0] == "OK") {
            log_info("Authentic success");
          } else {
            cli_->Close();
            log_warn("Invalid password");
            cli_ = NULL;
            should_exit_ = true;
            return;
          }
        } else {
          cli_->Close();
          log_info("%s", s.ToString().data());
          cli_ = NULL;
          continue;
        }
      } else {
        // If forget to input password
        net::RedisCmdArgsType argv, resp;
        std::string cmd;

        argv.push_back("PING");
        net::SerializeRedisCommand(argv, &cmd);
        slash::Status s = cli_->Send(&cmd);

        if (s.ok()) {
          s = cli_->Recv(&resp);
          if (s.ok()) {
            if (resp[0] == "NOAUTH Authentication required.") {
              cli_->Close();
              log_warn("Authentication required");
              cli_ = NULL;
              should_exit_ = true;
              return;
            }
          } else {
            cli_->Close();
            log_info("%s", s.ToString().data());
            cli_ = NULL;
          }
        }
      }
    }
  }
}

void RedisSender::Stop() {
  should_exit_ = true;
  commands_mutex_.Lock();
  rsignal_.Signal();
  commands_mutex_.Unlock();
}

void RedisSender::SendRedisCommand(const std::string &command) {
  commands_mutex_.Lock();
  if (commands_queue_.size() < 100000) {
    commands_queue_.push(command);
    rsignal_.Signal();
    commands_mutex_.Unlock();
    return;
  }

  DLOG(WARNING) << id_ << " commands queue size is beyond 100000";
  while (commands_queue_.size() > 100000) {
    wsignal_.Wait();
  }
  commands_queue_.push(command);
  rsignal_.Signal();
  commands_mutex_.Unlock();
}

int RedisSender::SendCommand(std::string &command) {
  time_t now = ::time(NULL);
  if (kCheckDiff < now - last_write_time_) {
    int ret = cli_->CheckAliveness();
    if (ret < 0) {
      ConnectRedis();
    }
    last_write_time_ = now;
  }

  // Send command
  int idx = 0;
  do {
    slash::Status s = cli_->Send(&command);
    if (s.ok()) {
      return 0;
    }

    DLOG(WARNING) << "RedisSender " << id_ << " fails to send redis command " << command << ", times:" << (idx+1);
    cli_->Close();
    log_info("%s", s.ToString().data());
    cli_ = NULL;
    ConnectRedis();
  } while(++idx < 3);

  return -1;
}

void *RedisSender::ThreadMain() {
  DLOG(INFO) << "Start sender " << id_ << " thread...";
  // sleep(15);
  int cnt = 0;
  int ret = 0;

  ConnectRedis();

  while (!should_exit_) {
    commands_mutex_.Lock();
    while (commands_queue_.size() == 0 && !should_exit_) {
      // rsignal_.TimedWait(100);
      rsignal_.Wait();
    }
    if (commands_queue_.size() == 0 && should_exit_) {
      commands_mutex_.Unlock();
      break;
    }

    if (commands_queue_.size() == 0) {
      commands_mutex_.Unlock();
      continue;
    }
    commands_mutex_.Unlock();

    // get redis command
    std::string command;
    commands_mutex_.Lock();
    command = commands_queue_.front();
    // printf("%d, command %s\n", id_, command.c_str());
    elements_++;
    commands_queue_.pop();
    wsignal_.Signal();
    commands_mutex_.Unlock();
    ret = SendCommand(command);
    if (ret == 0) {
      cnt++;
    }

    if (cnt >= 200) {
      for(; cnt > 0; cnt--) {
        cli_->Recv(NULL);
      }
    }
  }
  for(; cnt > 0; cnt--) {
    cli_->Recv(NULL);
  }

  delete cli_;
  cli_ = NULL;
  DLOG(INFO) << "RedisSender thread " << id_ << " complete";
  return NULL;
}

