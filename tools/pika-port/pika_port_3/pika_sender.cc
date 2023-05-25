#include <glog/logging.h>
#include <chrono>
#include <utility>

#include "const.h"
#include "pika_sender.h"
#include "pstd/include/xdebug.h"

PikaSender::PikaSender(std::string ip, int64_t port, std::string password)
    : cli_(nullptr), ip_(std::move(ip)), port_(port), password_(std::move(password)), should_exit_(false), cnt_(0), elements_(0) {}

PikaSender::~PikaSender() = default;

int PikaSender::QueueSize() {
  std::lock_guard l(keys_mutex_);
  return keys_queue_.size();
}

void PikaSender::Stop() {
  should_exit_ = true;
  signal_.notify_one();
}

void PikaSender::ConnectRedis() {
  while (!cli_) {
    // Connect to redis
    cli_ = net::NewRedisCli();
    cli_->set_connect_timeout(1000);
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
          LOG(INFO) << "auth faild: " << s.ToString();
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

void PikaSender::LoadKey(const std::string& key) {
  std::unique_lock lock(keys_mutex_);

  if (keys_queue_.size() < 100000) {
    keys_queue_.push(key);
    signal_.notify_one();
    lock.unlock();
  } else {
    while (keys_queue_.size() > 100000 && !should_exit_) {
      signal_.wait_for(lock, std::chrono::milliseconds(100));
    }
    keys_queue_.push(key);
    lock.unlock();
    signal_.notify_one();
  }
}

void PikaSender::SendCommand(std::string& command, const std::string& key) {
  // Send command
  pstd::Status s = cli_->Send(&command);
  if (!s.ok()) {
    elements_--;
    LoadKey(key);
    cnt_ = 0;
    cli_->Close();
    LOG(INFO) << s.ToString();
    delete cli_;
    cli_ = nullptr;
    ConnectRedis();
  }
}

void* PikaSender::ThreadMain() {
  LOG(INFO) << "Start sender thread...";

  if (!cli_) {
    ConnectRedis();
  }

  while (!should_exit_ || QueueSize() != 0) {
    {
      std::unique_lock lock(keys_mutex_);
      signal_.wait_for(lock, std::chrono::milliseconds(200),
                       [this]() { return !keys_queue_.empty() || should_exit_; });
    }

    if (QueueSize() == 0 && should_exit_) {
      // if (should_exit_) {
      return nullptr;
    }

    keys_mutex_.lock();
    std::string key = keys_queue_.front();
    elements_++;
    keys_queue_.pop();
    keys_mutex_.unlock();

    SendCommand(key, key);
    cnt_++;
    if (cnt_ >= 200) {
      for (; cnt_ > 0; cnt_--) {
        cli_->Recv(nullptr);
      }
    }
  }
  for (; cnt_ > 0; cnt_--) {
    cli_->Recv(nullptr);
  }

  cli_->Close();
  delete cli_;
  cli_ = nullptr;
  LOG(INFO) << "PikaSender thread complete";
  return nullptr;
}
