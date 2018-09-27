#include "const.h"
#include "pika_sender.h"

#include <glog/logging.h>

#include "slash/include/xdebug.h"

PikaSender::PikaSender(std::string ip, int64_t port, std::string password):
  cli_(NULL),
  rsignal_(&keys_mutex_),
  wsignal_(&keys_mutex_),
  ip_(ip),
  port_(port),
  password_(password),
  should_exit_(false),
  elements_(0)
  {
  }

PikaSender::~PikaSender() {
}

void PikaSender::ConnectRedis() {
  while (cli_ == NULL) {
    // Connect to redis
    cli_ = pink::NewRedisCli();
    cli_->set_connect_timeout(1000);
    slash::Status s = cli_->Connect(ip_, port_);
    if (!s.ok()) {
      cli_ = NULL;
      log_info("Can not connect to %s:%d: %s", ip_.data(), port_, s.ToString().data());
      continue;
    } else {
      // Connect success
      log_info("Connect to %s:%d:%s", ip_.data(), port_, s.ToString().data());

      // Authentication
      if (!password_.empty()) {
        pink::RedisCmdArgsType argv, resp;
        std::string cmd;

        argv.push_back("AUTH");
        argv.push_back(password_);
        pink::SerializeRedisCommand(argv, &cmd);
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
        pink::RedisCmdArgsType argv, resp;
        std::string cmd;

        argv.push_back("PING");
        pink::SerializeRedisCommand(argv, &cmd);
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

void PikaSender::LoadKey(const std::string &key) {
  keys_mutex_.Lock();
  if (keys_queue_.size() < 100000) {
    keys_queue_.push(key);
    rsignal_.Signal();
    keys_mutex_.Unlock();
  } else {
    while (keys_queue_.size() > 100000) {
      wsignal_.Wait();
    }
    keys_queue_.push(key);
    rsignal_.Signal();
    keys_mutex_.Unlock();
  }
}

void PikaSender::SendCommand(std::string &command, const std::string &key) {
  // Send command
  slash::Status s = cli_->Send(&command);
  if (!s.ok()) {
    elements_--;
    LoadKey(key);
    cli_->Close();
    log_info("%s", s.ToString().data());
    cli_ = NULL;
    ConnectRedis();
  }
}

void *PikaSender::ThreadMain() {
  log_info("Start sender thread...");
  expire_command_.clear();
  int cnt = 0;

  while (!should_exit_ || QueueSize() != 0 || !expire_command_.empty()) {
    std::string command;
    // Resend expire command
    if (!expire_command_.empty() && cli_ != NULL) {
      slash::Status s = cli_->Send(&expire_command_);
      // std::cout << expire_command_ << std::endl;
      if (!s.ok()) {
        cli_->Close();
        cli_ = NULL;
        log_info("%s", s.ToString().data());
        ConnectRedis();
      } else {
        expire_command_.clear();
      }
    }

    if (expire_command_.empty()) {
      keys_mutex_.Lock();
      while (keys_queue_.size() == 0 && !should_exit_) {
        rsignal_.TimedWait(100);
        // rsignal_.Wait();
      }
      keys_mutex_.Unlock();
      // if (QueueSize() == 0 && should_exit_) {
      if (should_exit_) {
        return NULL;
      }
    }

    if (cli_ == NULL) {
      ConnectRedis();
      // if (QueueSize() == 0 && should_exit_) {
      if (should_exit_) {
        return NULL;
      }
    } else {
      if (QueueSize() == 0) {
        continue;
      }
      // Parse keys
      std::string key;

      keys_mutex_.Lock();
      key = keys_queue_.front();
      elements_++;
      keys_queue_.pop();
      wsignal_.Signal();
      keys_mutex_.Unlock();

      size_t keySize = key.size();
	  if (keySize == 0) {
		  continue;
	  }

      char type = key[keySize - 1];
	  switch (type) {
		case kSuffixKv:
		case kSuffixZset:
		case kSuffixSet:
		case kSuffixList:
	    case kSuffixHash: {
          command.assign(key.data(), keySize - 1);
          SendCommand(command, key);
          cnt++;
	      break;
		}
	    default: {
	      LOG(WARNING) << "illegal type:" << type;
	      break;
		}
	  }

      if (cnt >= 200) {
        for(; cnt > 0; cnt--) {
          cli_->Recv(NULL);
        }
      }

      continue;
    }
  }
  for(; cnt > 0; cnt--) {
    cli_->Recv(NULL);
  }

  delete cli_;
  cli_ = NULL;
  log_info("PikaSender thread complete");
  return NULL;
}

