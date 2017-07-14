#include "parse_thread.h"

ParseThread::~ParseThread() {
}
void ParseThread::ParseKey(const std::string &key,char type) {
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

void ParseThread::SetTTL(const std::string &key, int64_t ttl) {
  if (ttl < 0) return;
  pink::RedisCmdArgsType argv;
  std::string cmd;

  argv.push_back("EXPIRE");
  argv.push_back(key);
  argv.push_back(std::to_string(ttl));

  pink::SerializeRedisCommand(argv, &cmd);
  sender_->LoadCmd(cmd);
}

void ParseThread::ParseKKey(const std::string &cmd) {
    PlusNum();
    std::cout << cmd << std::endl;
    sender_->LoadCmd(cmd);
}

void ParseThread::ParseHKey(const std::string &key) {
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

void ParseThread::ParseSKey(const std::string &key) {
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

void ParseThread::ParseZKey(const std::string &key) {
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

void ParseThread::ParseLKey(const std::string &key) {
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
      std::cout << "list key " << key << " list value " << it->val << std::endl;
      argv.push_back(it->val);
    }
    pink::SerializeRedisCommand(argv, &cmd);
    sender_->LoadCmd(cmd);

    pos += len;
    ivs.clear();
    db_->LRange(key, pos, pos+len-1, ivs);
  }
}

void ParseThread::Schedul(const std::string &key, char type) {
  slash::MutexLock l(&task_mutex_);
  while (task_queue_.size() >= full_) {
    task_w_cond_.Wait();
  }

  if (task_queue_.size() < full_) {
    task_queue_.push_back(Task(key, type));
    task_r_cond_.Signal();
  }
}

void ParseThread::Stop() {
  should_exit_ = true;
  slash::MutexLock l(&task_mutex_);
  task_r_cond_.Signal();
}

void *ParseThread::ThreadMain() {
  char type;
  std::string key;
  while (!should_exit_) {
    {
      slash::MutexLock l(&task_mutex_);
      while (task_queue_.empty() && !should_exit_) {
        task_r_cond_.Wait();
      }

      if (task_queue_.empty()) {
        break;
      }
      key = task_queue_.front().key;
      type = task_queue_.front().type;
      task_queue_.pop_front();
      task_w_cond_.Signal();
    }
    ParseKey(key, type);
  }

  if (!task_queue_.empty()) {
    key = task_queue_.front().key;
    type = task_queue_.front().type;
    task_queue_.pop_front();
    ParseKey(key, type);
  }
  sender_->Stop();
  std::cout << "Parser " <<  pthread_self() << " exit \n";
  return NULL;
}
