#include "sender.h"

Sender::Sender(nemo::Nemo *db, std::string ip, int64_t port, std::string password):
  rsignal_(&mu_),
  wsignal_(&mu_),
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

void Sender::LoadKey(const std::string &key) {
  slash::MutexLock l(&keys_mutex_);
  if (keys_queue_.size() < 100000) {
    keys_queue_.push(key);
    rsignal_.Signal();
  } else {
    wsignal_.Wait();
  }
}

void *Sender::ThreadMain() {
  log_info("Start sender thread...");

  pink::PinkCli *cli = NULL;
  while (!should_exit_ ) {
    std::string command, expire_command;
  	while (!should_exit_ || QueueSize() == 0)
  		rsignal_.Wait();
    if (should_exit_) {
      return NULL;
    }
  	if (cli == NULL) {
  	  // Connect to redis
      cli = pink::NewRedisCli();
  	  cli->set_connect_timeout(1000);
  	  slash::Status s = cli->Connect(ip_, port_);
  	  if (!s.ok()) {
  	  	cli = NULL;
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
    	  slash::Status s = cli->Send(&cmd);
    	  
    	  if (s.ok()) {
      	  	s = cli->Recv(&resp);
       	  	if (resp[0] == "OK") {
  	  		  log_info("Authentic success");
  	  	    } else {
  	  		  log_info("Invalid password");
  	  		  return NULL;
  	  		}
  		  } else {
  		  	  cli->Close();
  	  	    log_info("%s", s.ToString().data());
  	  	    cli = NULL;
  	 		    continue;
    	    }
  		  } else {
  		    // If forget to input password
  		    pink::RedisCmdArgsType argv, resp;
    	    std::string cmd;

    	    argv.push_back("PING");
    	    pink::SerializeRedisCommand(argv, &cmd);
    	    slash::Status s = cli->Send(&cmd);
    	  
    	    if (s.ok()) {
      	    s = cli->Recv(&resp);
      	    if (s.ok()) {
       	  	  if (resp[0] == "NOAUTH Authentication required.") {
  	  		      log_info("Authentication required");
  	  		      return NULL;
  	  	      }
  	  	    } else {
  	  	      cli->Close();
  	  	      log_info("%s", s.ToString().data());
  	  	      cli = NULL;
  	 		      continue;
  	 		    }
  	 	    }
  		  }
	    }
    } else {
      // Parse keys
      std::string key;
	    {
        slash::MutexLock l(&keys_mutex_);
        key = keys_queue_.front();
        keys_queue_.pop();
      }
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
          argv.push_back(key);

          for (it = ivs.begin(); it != ivs.end(); ++it) {
            argv.push_back(it->val);
          }
          pink::SerializeRedisCommand(argv, &command);

          pos += len;
          ivs.clear();
          db_->LRange(key, pos, pos+len-1, ivs);
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
        }
        delete iter;
      } else if (type == nemo::DataType::kKv) {   // Kv
        std::string k_key = key.substr(1);
  	    command = k_key;
      } 

      // expire
      if (type != nemo::DataType::kKv) {
  	    int64_t ttl = -1;
        std::string e_key = key.substr(1);
	      db_->TTL(key, &ttl);

	      if (ttl >= 0) {
  	      pink::RedisCmdArgsType argv;
          std::string cmd;

	        argv.push_back("EXPIRE");
  	      argv.push_back(key);
  	      argv.push_back(std::to_string(ttl));

  	      pink::SerializeRedisCommand(argv, &expire_command);
 	      }
      }
  	
      // Send command
      slash::Status s = cli->Send(&command);
      if (s.ok()) {
  	    elements_++;
        wsignal_.Signal();
  	  } else {
        keys_queue_.push(key);
  	    cli->Close();
  	    log_info("%s", s.ToString().data());
  	    cli = NULL;
        continue;
  	  }
    
      // Send expire command
      if (!expire_command.empty()) {
        slash::Status s = cli->Send(&expire_command);
        if (!s.ok()) {
  	      cli->Close();
  	      log_info("%s", s.ToString().data());
  	      cli = NULL;
          continue;
  	    }
      }
    }
  }
  delete cli;
  log_info("Sender thread complete");
  return NULL;
}