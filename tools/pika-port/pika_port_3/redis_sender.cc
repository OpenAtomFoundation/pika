
#include "redis_sender.h"

#include <unistd.h>
#include <ctime>

#include <glog/logging.h>

#include <utility>

#include "pstd/include/xdebug.h"

static time_t kCheckDiff = 1;

RedisSender::RedisSender(int id, std::string ip, int64_t port, std::string password)
    : id_(id), cli_(nullptr), ip_(std::move(ip)), port_(port), password_(std::move(password)), should_exit_(false), cnt_(0), elements_(0) {
  last_write_time_ = ::time(nullptr);
}

RedisSender::~RedisSender() { LOG(INFO) << "RedisSender thread " << id_ << " exit!!!"; }

void RedisSender::ConnectRedis() {
  while (cli_ == nullptr) {
    // Connect to redis
    cli_ = net::NewRedisCli();
    cli_->set_connect_timeout(1000);
    cli_->set_recv_timeout(10000);
    cli_->set_send_timeout(10000);
    pstd::Status s = cli_->Connect(ip_, port_);
    if (!s.ok()) {
      delete cli_;
      cli_ = nullptr;
      LOG(WARNING) << "Can not connect to " << ip_ << ":" << port_ << ", status: " << s.ToString();
      sleep(3);
      continue;
    } else {
      // Connect success
      LOG(INFO) << "Connect to " << ip_ << ":" << port_ << " success";

      // Authentication
      if (!password_.empty()) {
        net::RedisCmdArgsType argv;
        net::RedisCmdArgsType resp;
        std::string cmd;

        argv.push_back("AUTH");
        argv.push_back(password_);
        net::SerializeRedisCommand(argv, &cmd);
        pstd::Status s = cli_->Send(&cmd);

        if (s.ok()) {
          s = cli_->Recv(&resp);
          if (resp[0] == "OK") {
            LOG(INFO) << "Authentic success";
          } else {
            cli_->Close();
            LOG(WARNING) << "Invalid password";
            cli_ = nullptr;
            should_exit_ = true;
            return;
          }
        } else {
          cli_->Close();
          LOG(INFO) << s.ToString();
          cli_ = nullptr;
          continue;
        }
      } else {
        // If forget to input password
        net::RedisCmdArgsType argv;
        net::RedisCmdArgsType resp;
        std::string cmd;

        argv.push_back("PING");
        net::SerializeRedisCommand(argv, &cmd);
        pstd::Status s = cli_->Send(&cmd);

        if (s.ok()) {
          s = cli_->Recv(&resp);
          if (s.ok()) {
            if (resp[0] == "NOAUTH Authentication required.") {
              cli_->Close();
              LOG(WARNING) << "Authentication required";
              cli_ = nullptr;
              should_exit_ = true;
              return;
            }
          } else {
            cli_->Close();
            LOG(INFO) << s.ToString();
            cli_ = nullptr;
          }
        }
      }
    }
  }
}

void RedisSender::Stop() {
  set_should_stop();
  should_exit_ = true;
  rsignal_.notify_one();
}

void RedisSender::SendRedisCommand(const std::string& command) {
  std::unique_lock lock(commands_mutex_);
  if (commands_queue_.size() < 100000) {
    commands_queue_.push(command);
    rsignal_.notify_one();
    return;
  }

  // LOG(WARNING) << id_ << "commands queue size is beyond 100000";
  wsignal_.wait(lock, [this] { return commands_queue_.size() <= 100000; });
  commands_queue_.push(command);
  rsignal_.notify_one();
}

int RedisSender::SendCommand(std::string& command) {
  time_t now = ::time(nullptr);
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
    pstd::Status s = cli_->Send(&command);
    if (s.ok()) {
      return 0;
    }

    LOG(WARNING) << "RedisSender " << id_ << "fails to send redis command " << command << ", times: " << idx + 1;
    cnt_ = 0;
    cli_->Close();
    delete cli_;
    LOG(INFO) << s.ToString();
    cli_ = nullptr;
    ConnectRedis();
  } while (++idx < 3);

  return -1;
}

void* RedisSender::ThreadMain() {
  LOG(INFO) << "Start sender " << id_ << " thread...";
  // sleep(15);
  int ret = 0;

  ConnectRedis();

  while (!should_exit_) {
    std::unique_lock lock(commands_mutex_);
    while (commands_queue_.empty() && !should_exit_) {
      rsignal_.wait_for(lock, std::chrono::milliseconds(100));
      // rsignal_.Wait();
    }
    // if (commands_queue_.size() == 0 && should_exit_) {
    if (should_exit_) {
      break;
    }

    if (commands_queue_.empty()) {
      continue;
    }

    auto command = commands_queue_.front();
    // printf("%d, command %s\n", id_, command.c_str());
    elements_++;
    commands_queue_.pop();
    lock.unlock();
    wsignal_.notify_one();
    ret = SendCommand(command);
    if (ret == 0) {
      cnt_++;
    }

    if (cnt_ >= 200) {
      for (; cnt_ > 0; cnt_--) {
        cli_->Recv(nullptr);
      }
    }
  }
  for (; cnt_ > 0; cnt_--) {
    cli_->Recv(nullptr);
  }

  delete cli_;
  cli_ = nullptr;
  LOG(INFO) << "RedisSender thread " << id_ << " complete";
  return nullptr;
}
