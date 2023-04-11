#include <glog/logging.h>

#include "const.h"
#include "pika_sender.h"
#include "slash/include/xdebug.h"

PikaSender::PikaSender(std::string ip, int64_t port, std::string password)
    : cli_(NULL),
      signal_(&keys_mutex_),
      ip_(ip),
      port_(port),
      password_(password),
      should_exit_(false),
      cnt_(0),
      elements_(0) {}

PikaSender::~PikaSender() {}

int PikaSender::QueueSize() {
  slash::MutexLock l(&keys_mutex_);
  return keys_queue_.size();
}

void PikaSender::Stop() {
  should_exit_ = true;
  keys_mutex_.Lock();
  signal_.Signal();
  keys_mutex_.Unlock();
}

void PikaSender::ConnectRedis() {
  while (cli_ == NULL) {
    // Connect to redis
    cli_ = net::NewRedisCli();
    cli_->set_connect_timeout(1000);
    slash::Status s = cli_->Connect(ip_, port_);
    if (!s.ok()) {
      delete cli_;
      cli_ = NULL;
      LOG(WARNING) << "Can not connect to " << ip_ << ":" << port_ << ", status: " << s.ToString();
      sleep(3);
      continue;
    } else {
      // Connect success
      LOG(INFO) << "Connect to " << ip_ << ":" << port_ << " success";

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
            LOG(INFO) << "Authentic success";
          } else {
            cli_->Close();
            LOG(WARNING) << "Invalid password";
            cli_ = NULL;
            should_exit_ = true;
            return;
          }
        } else {
          cli_->Close();
          LOG(INFO) << "auth faild: " << s.ToString();
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
              LOG(WARNING) << "Authentication required";
              cli_ = NULL;
              should_exit_ = true;
              return;
            }
          } else {
            cli_->Close();
            LOG(INFO) << s.ToString();
            cli_ = NULL;
          }
        }
      }
    }
  }
}

void PikaSender::LoadKey(const std::string& key) {
  keys_mutex_.Lock();
  if (keys_queue_.size() < 100000) {
    keys_queue_.push(key);
    signal_.Signal();
    keys_mutex_.Unlock();
  } else {
    while (keys_queue_.size() > 100000 && !should_exit_) {
      signal_.TimedWait(100);
    }
    keys_queue_.push(key);
    signal_.Signal();
    keys_mutex_.Unlock();
  }
}

void PikaSender::SendCommand(std::string& command, const std::string& key) {
  // Send command
  slash::Status s = cli_->Send(&command);
  if (!s.ok()) {
    elements_--;
    LoadKey(key);
    cnt_ = 0;
    cli_->Close();
    LOG(INFO) << s.ToString();
    delete cli_;
    cli_ = NULL;
    ConnectRedis();
  }
}

void* PikaSender::ThreadMain() {
  LOG(INFO) << "Start sender thread...";

  if (cli_ == NULL) {
    ConnectRedis();
  }

  while (!should_exit_ || QueueSize() != 0) {
    std::string command;

    keys_mutex_.Lock();
    while (keys_queue_.size() == 0 && !should_exit_) {
      signal_.TimedWait(200);
    }
    keys_mutex_.Unlock();
    if (QueueSize() == 0 && should_exit_) {
      // if (should_exit_) {
      return NULL;
    }

    keys_mutex_.Lock();
    std::string key = keys_queue_.front();
    elements_++;
    keys_queue_.pop();
    keys_mutex_.Unlock();

    SendCommand(key, key);
    cnt_++;
    if (cnt_ >= 200) {
      for (; cnt_ > 0; cnt_--) {
        cli_->Recv(NULL);
      }
    }
  }
  for (; cnt_ > 0; cnt_--) {
    cli_->Recv(NULL);
  }

  cli_->Close();
  delete cli_;
  cli_ = NULL;
  LOG(INFO) << "PikaSender thread complete";
  return NULL;
}
