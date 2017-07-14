#include "migrator_thread.h"

MigratorThread::~MigratorThread() {
}

void MigratorThread::MigrateDB(char type) {
    if (type == nemo::DataType::kKv) {
      nemo::KIterator *it = db_->KScan("", "", -1, false);
      std::string key, value;

      while (it->Valid()) {
        key = it->key();
        value = it->value();
        pink::RedisCmdArgsType argv;
        std::string cmd;

        int64_t ttl;
        db_->TTL(key, &ttl);

        argv.push_back("SET");
        argv.push_back(key);
        argv.push_back(value);
        if (ttl > 0) {
          argv.push_back("EX");
          argv.push_back(std::to_string(ttl));
        }
        
        it->Next();
        pink::SerializeRedisCommand(argv, &cmd);
        PlusNum();
        sender_->LoadCmd(cmd);

        //ParseKey(key, type);
        //DispatchKey(cmd, type);
      }
      delete it;
    } else {
      char c_type = 'a';
      switch (type) {
        case nemo::DataType::kHSize:
            c_type = 'h';
            break;
        case nemo::DataType::kSSize:
            c_type = 's';
            break;
        case nemo::DataType::kLMeta:
            c_type = 'l';
            break;
        case nemo::DataType::kZSize:
            c_type = 'z';
            break;
        }
        std::vector<std::string> keys;
        std::string pattern  = "*";
        db_->Scanbytype(c_type, pattern, keys);
        for (size_t i = 0; i < keys.size(); i++) {
          ParseKey(keys[i], type);
          //DispatchKey(keys[i], type);
      }
    }
}

std::string  MigratorThread::GetKey(const rocksdb::Iterator *it) {
  return it->key().ToString().substr(1);
}
/*
void MigratorThread::DispatchKey(const std::string &key, char type) {
  parsers_[thread_index_]->Schedul(key, type);
  thread_index_ = (thread_index_ + 1) % num_thread_;
}
*/
void MigratorThread::ParseKey(const std::string &key,char type) {
  if (type == nemo::DataType::kHSize) {
    ParseHKey(key);
  } else if (type == nemo::DataType::kSSize) {
    ParseSKey(key);
  } else if (type == nemo::DataType::kLMeta) {
    ParseLKey(key);
  } else if (type == nemo::DataType::kZSize) {
    ParseZKey(key);
  } else if (type == nemo::DataType::kSSize) {
    ParseSKey(key);
  } else if (type == nemo::DataType::kKv) {
    ParseKKey(key);
  }

  if (type == nemo::DataType::kKv) {
    return;
  }

  int64_t ttl;
  // int64_t *ttl = -1;

  db_->TTL(key, &ttl);
  /*
  if (type == nemo::DataType::kHSize) {
    db_->HTTL(key, &ttl);
  } else if (type == nemo::DataType::kSSize) {
    db_->STTL(key, &ttl);
  } else if (type == nemo::DataType::kLMeta) {
    db_->LTTL(key, &ttl);
  } else if (type == nemo::DataType::kZSize) {
    db_->ZTTL(key, &ttl);
  }
  */
  // no kv, because kv cmd: SET key value ttl
  SetTTL(key, ttl);

}

void MigratorThread::SetTTL(const std::string &key, int64_t ttl) {
  if (ttl < 0) return;
  pink::RedisCmdArgsType argv;
  std::string cmd;

  argv.push_back("EXPIRE");
  argv.push_back(key);
  argv.push_back(std::to_string(ttl));

  pink::SerializeRedisCommand(argv, &cmd);
  sender_->LoadCmd(cmd);
}

void MigratorThread::ParseKKey(const std::string &cmd) {
    PlusNum();
    sender_->LoadCmd(cmd);
}

void MigratorThread::ParseHKey(const std::string &key) {
  nemo::HIterator *iter = db_->HScan(key, "", "", -1, false);
  for (; iter->Valid(); iter->Next()) {
    pink::RedisCmdArgsType argv;
    std::string cmd;

    argv.push_back("HSET");
    argv.push_back(iter->key());
    argv.push_back(iter->field());
    argv.push_back(iter->value());

    pink::SerializeRedisCommand(argv, &cmd);
    PlusNum();
    sender_->LoadCmd(cmd);
  }
  delete iter;
}

void MigratorThread::ParseSKey(const std::string &key) {
  nemo::SIterator *iter = db_->SScan(key, -1, false);
  for (; iter->Valid(); iter->Next()) {
    pink::RedisCmdArgsType argv;
    std::string cmd;

    argv.push_back("SADD");
    argv.push_back(iter->key());
    argv.push_back(iter->member());

    pink::SerializeRedisCommand(argv, &cmd);
    PlusNum();
    sender_->LoadCmd(cmd);
  }
  delete iter;
}

void MigratorThread::ParseZKey(const std::string &key) {
  nemo::ZIterator *iter = db_->ZScan(key, nemo::ZSET_SCORE_MIN,
                                     nemo::ZSET_SCORE_MAX, -1, false);
  for (; iter->Valid(); iter->Next()) {
    pink::RedisCmdArgsType argv;
    std::string cmd;

    std::string score = std::to_string(iter->score());

    argv.push_back("ZADD");
    argv.push_back(iter->key());
    argv.push_back(score);
    argv.push_back(iter->member());

    pink::SerializeRedisCommand(argv, &cmd);
    PlusNum();
    sender_->LoadCmd(cmd);
  }
  delete iter;
}

void MigratorThread::ParseLKey(const std::string &key) {
  std::vector<nemo::IV> ivs;
  std::vector<nemo::IV>::const_iterator it;
  int64_t pos = 0;
  int64_t len = 512;

  db_->LRange(key, pos, pos+len-1, ivs);

  while (!ivs.empty()) {
    pink::RedisCmdArgsType argv;
    std::string cmd;

    argv.push_back("RPUSH");
    argv.push_back(key);

    for (it = ivs.begin(); it != ivs.end(); ++it) {
      PlusNum();
      argv.push_back(it->val);
    }
    pink::SerializeRedisCommand(argv, &cmd);
    sender_->LoadCmd(cmd);

    pos += len;
    ivs.clear();
    db_->LRange(key, pos, pos+len-1, ivs);
  }
}

void *MigratorThread::ThreadMain() {
  MigrateDB(type_);
  should_exit_ = true;
  std::cout << type_ << " keys have been dispatched completly" << std::endl;
  sender_->Stop();

  return NULL;
}
