// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_sender.h"

#include "slash/include/xdebug.h"

PikaSender::PikaSender(std::string ip, int64_t port, std::string password):
  cli_(NULL),
  signal_(&keys_mutex_),
  ip_(ip),
  port_(port),
  password_(password),
  should_exit_(false),
  elements_(0)
  {
  }

PikaSender::~PikaSender() {
}

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
    cli_ = pink::NewRedisCli();
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

void PikaSender::SendCommand(std::string &command, const std::string &key) {
  // Send command
  slash::Status s = cli_->Send(&command);
  if (!s.ok()) {
    elements_--;
    LoadKey(key);
    cli_->Close();
    log_info("%s", s.ToString().data());
    delete cli_;
    cli_ = NULL;
    ConnectRedis();
  }
}

void *PikaSender::ThreadMain() {
  log_info("Start sender thread...");
  int cnt = 0;

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
    cnt++;
    if (cnt >= 200) {
      for(; cnt > 0; cnt--) {
        cli_->Recv(NULL);
      }
    }
  }
  for(; cnt > 0; cnt--) {
    cli_->Recv(NULL);
  }

  cli_->Close();
  delete cli_;
  cli_ = NULL;
  log_info("PikaSender thread complete");
  return NULL;
}

