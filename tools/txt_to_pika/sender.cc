#include "sender.h"

SenderThread::SenderThread(std::string ip, int64_t port, std::string password)
    : cli_(NULL),
      rsignal_(&cmd_mutex_),
      wsignal_(&cmd_mutex_),
      ip_(ip),
      port_(port),
      password_(password),
      should_exit_(false),
      elements_(0) {}

SenderThread::~SenderThread() {}

void SenderThread::ConnectPika() {
  while (cli_ == NULL) {
    // Connect to redis
    cli_ = net::NewRedisCli();
    cli_->set_connect_timeout(1000);
    pstd::Status s = cli_->Connect(ip_, port_);
    if (!s.ok()) {
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
        pstd::Status s = cli_->Send(&cmd);

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
        pstd::Status s = cli_->Send(&cmd);

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

void SenderThread::LoadCmd(const std::string& cmd) {
  cmd_mutex_.Lock();
  if (cmd_queue_.size() < 100000) {
    cmd_queue_.push(cmd);
    rsignal_.Signal();
    cmd_mutex_.Unlock();
  } else {
    while (cmd_queue_.size() > 100000) {
      wsignal_.Wait();
    }
    cmd_queue_.push(cmd);
    rsignal_.Signal();
    cmd_mutex_.Unlock();
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
    cli_ = NULL;
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
    cmd_mutex_.Lock();
    while (cmd_queue_.size() == 0 && !should_exit_) {
      rsignal_.Wait();
    }
    cmd_mutex_.Unlock();

    if (cli_ == NULL) {
      ConnectPika();
      continue;
    }
    if (QueueSize() != 0) {
      cmd_mutex_.Lock();
      std::string cmd = cmd_queue_.front();
      cmd_queue_.pop();
      wsignal_.Signal();
      cmd_mutex_.Unlock();
      SendCommand(cmd);
    }
  }

  delete cli_;
  log_info("Sender thread complete");
  return NULL;
}
