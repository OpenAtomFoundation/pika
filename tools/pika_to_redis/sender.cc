#include "sender.h"

Sender::Sender(nemo::Nemo *db, std::string ip, int64_t port, std::string password):
  cli_(NULL),
  rsignal_(&keys_mutex_),
  wsignal_(&keys_mutex_),
  db_(db),
  ip_(ip),
  port_(port),
  password_(password),
  should_exit_(false),
  elements_(0)
  {
  }

Sender::~Sender() {
}

void Sender::ConnectRedis() {
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

void Sender::LoadKey(const std::string &key) {
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

void Sender::SendCommand(std::string &command, const std::string &key) {
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

void *Sender::ThreadMain() {
  log_info("Start sender thread...");
  expire_command_.clear();

  while (!should_exit_ || QueueSize() != 0 || !expire_command_.empty()) {
    std::string command;
    // Resend expire command
    if (!expire_command_.empty() && cli_ != NULL) {
      slash::Status s = cli_->Send(&expire_command_);
      std::cout << expire_command_ << std::endl;
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
        rsignal_.Wait();
      }
      keys_mutex_.Unlock();
      if (QueueSize() == 0 && should_exit_) {
        return NULL;
      }
    }

    if (cli_ == NULL) {
      ConnectRedis();
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

      char type = key[0];
      if (type == nemo::DataType::kHSize) {   // Hash
        std::string h_key = key.substr(1);
        nemo::HIterator *iter = db_->HScan(h_key, "", "", -1, false);
        for (; iter->Valid(); iter->Next()) {
          pink::RedisCmdArgsType argv;

          argv.push_back("HSET");
          argv.push_back(iter->key());
          argv.push_back(iter->field());
          argv.push_back(iter->value());

          pink::SerializeRedisCommand(argv, &command);
          SendCommand(command, key);
        }
        delete iter;
      } else if (type == nemo::DataType::kSSize) {  // Set
        std::string s_key = key.substr(1);
        nemo::SIterator *iter = db_->SScan(s_key, -1, false);
        for (; iter->Valid(); iter->Next()) {
          pink::RedisCmdArgsType argv;

          argv.push_back("SADD");
          argv.push_back(iter->key());
          argv.push_back(iter->member());

          pink::SerializeRedisCommand(argv, &command);
          SendCommand(command, key);
        }
        delete iter;
      } else if (type == nemo::DataType::kLMeta) {  // List
        std::string l_key = key.substr(1);
        std::vector<nemo::IV> ivs;
        std::vector<nemo::IV>::const_iterator it;
        int64_t pos = 0;
        int64_t len = 512;

        db_->LRange(l_key, pos, pos+len-1, ivs);

        while (!ivs.empty()) {
          pink::RedisCmdArgsType argv;
          std::string cmd;

          argv.push_back("RPUSH");
          argv.push_back(l_key);

          for (it = ivs.begin(); it != ivs.end(); ++it) {
            argv.push_back(it->val);
          }
          pink::SerializeRedisCommand(argv, &command);
          SendCommand(command, key);

          pos += len;
          ivs.clear();
          db_->LRange(l_key, pos, pos+len-1, ivs);
        }
      } else if (type == nemo::DataType::kZSize) {  // Zset
        std::string z_key = key.substr(1);
        nemo::ZIterator *iter = db_->ZScan(z_key, nemo::ZSET_SCORE_MIN,
                                     nemo::ZSET_SCORE_MAX, -1, false);
        for (; iter->Valid(); iter->Next()) {
          pink::RedisCmdArgsType argv;

          std::string score = std::to_string(iter->score());

          argv.push_back("ZADD");
          argv.push_back(iter->key());
          argv.push_back(score);
          argv.push_back(iter->member());

          pink::SerializeRedisCommand(argv, &command);
          SendCommand(command, key);
        }
        delete iter;
      } else if (type == nemo::DataType::kKv) {   // Kv
        std::string k_key = key.substr(1);
        command = k_key;
        SendCommand(command, key);
      }

      // expire command
      if (type != nemo::DataType::kKv) {
        int64_t ttl = -1;
        std::string e_key = key.substr(1);
        db_->TTL(key, &ttl);

        if (ttl >= 0) {
          pink::RedisCmdArgsType argv;
          std::string cmd;

          argv.push_back("EXPIRE");
          argv.push_back(e_key);
          argv.push_back(std::to_string(ttl));

          pink::SerializeRedisCommand(argv, &expire_command_);
          slash::Status s = cli_->Send(&expire_command_);
          if (!s.ok()) {
            cli_->Close();
            log_info("%s", s.ToString().data());
            cli_ = NULL;
            ConnectRedis();
          } else {
            expire_command_.clear();
          }
        }
      }
    }
  }

  delete cli_;
  log_info("Sender thread complete");
  return NULL;
}

