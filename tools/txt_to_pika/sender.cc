#include "sender.h"

SenderThread::SenderThread(std::string ip, int64_t port, std::string password)
    : cli_(nullptr), ip_(ip), port_(port), password_(password), should_exit_(false), elements_(0) {}

SenderThread::~SenderThread() {}

void SenderThread::ConnectPika() {
  while (cli_ == nullptr) {
    // Connect to redis
    cli_ = net::NewRedisCli();
    cli_->set_connect_timeout(1000);
    pstd::Status s = cli_->Connect(ip_, port_);
    if (!s.ok()) {
      cli_ = nullptr;
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
        pstd::Status s = cli_->Send(&cmd);

        if (s.ok()) {
          s = cli_->Recv(&resp);
          if (resp[0] == "OK") {
            log_info("Authentic success");
          } else {
            cli_->Close();
            log_warn("Invalid password");
            cli_ = nullptr;
            should_exit_ = true;
            return;
          }
        } else {
          cli_->Close();
          log_info("%s", s.ToString().data());
          cli_ = nullptr;
          continue;
        }
      } else {
        // If forget to input password
        net::RedisCmdArgsType argv, resp;
        std::string cmd;

        argv.push_back("PING");
        net::SerializeRedisCommand(argv, &cmd);
        pstd::Status s = cli_->Send(&cmd);

        if (s.ok()) {
          s = cli_->Recv(&resp);
          if (s.ok()) {
            if (resp[0] == "NOAUTH Authentication required.") {
              cli_->Close();
              log_warn("Authentication required");
              cli_ = nullptr;
              should_exit_ = true;
              return;
            }
          } else {
            cli_->Close();
            log_info("%s", s.ToString().data());
            cli_ = nullptr;
          }
        }
      }
    }
  }
}

void SenderThread::LoadCmd(const std::string& cmd) {
  std::unique_lock lock(cmd_mutex_);
  if (cmd_queue_.size() < 100000) {
    cmd_queue_.push(cmd);
    rsignal_.notify_one();
  } else {
    wsignal_.wait(lock, [this] { return cmd_queue_.size() <= 100000; });
    cmd_queue_.push(cmd);
    rsignal_.notify_one();
  }
}

void SenderThread::SendCommand(std::string& command) {
  // Send command
  pstd::Status s = cli_->Send(&command);
  if (!s.ok()) {
    elements_--;
    LoadCmd(command);
    cli_->Close();
    log_info("%s", s.ToString().data());
    cli_ = nullptr;
    ConnectPika();
  } else {
    net::RedisCmdArgsType resp;
    s = cli_->Recv(&resp);
    // std::cout << resp[0] << std::endl;
    elements_++;
  }
}

void* SenderThread::ThreadMain() {
  log_info("Start sender thread...");

  while (!should_exit_ || QueueSize() != 0) {
    {
      std::unique_lock lock(cmd_mutex_);
      rsignal_.wait(lock, [this] { return !cmd_queue_.size() || should_exit_; });
    }

    if (cli_ == nullptr) {
      ConnectPika();
      continue;
    }
    if (QueueSize() != 0) {
      cmd_mutex_.lock();
      std::string cmd = cmd_queue_.front();
      cmd_queue_.pop();
      cmd_mutex_.unlock();
      wsignal_.notify_one();
      SendCommand(cmd);
    }
  }

  delete cli_;
  log_info("Sender thread complete");
  return nullptr;
}
