#include "migrator_thread.h"

MigratorThread::~MigratorThread() {
}

void MigratorThread::ComposeCmd(nemo::Iterator * iter,char type) {

}

void MigratorThread::MigrateDB(char type) {
  if (type == nemo::DataType::kKv) {
    nemo::KIterator *iter = db_->KScan("", "", -1, false);
    // SET <key> <vaule> [EX SEC]
    for (; iter->Valid(); iter->Next()) {
      pink::RedisCmdArgsType argv;
      std::string cmd;

      std::string key = iter->key();
      int64_t ttl;
      db_->TTL(key, &ttl);

      argv.push_back("SET");
      argv.push_back(key);
      argv.push_back(iter->value());

      if (ttl > 0) {
        argv.push_back("EX");
        argv.push_back(std::to_string(ttl));
      }

      pink::SerializeRedisCommand(argv, &cmd);
      DispatchKey(cmd, type);
    }
  }
  /*
  else if (type == nemo::DataType::kHash) {
    nemo::KIterator *iter = db_->HScan("", "", -1, false);
    ComposeCmd(iter, type);
  } else if (type == nemo::DataType::kZSet) {
    nemo::KIterator *iter = db_->ZScan("", "", -1, false);
    ComposeCmd(iter, type);
  } else if (type == nemo::DataType::kSet) {
    nemo::KIterator *iter = db_->SScan("", "", -1, false);
    ComposeCmd(iter, type);
  } else {  // List
  }*/
}

std::string  MigratorThread::GetKey(const rocksdb::Iterator *it) {
  return it->key().ToString().substr(1);
}

void MigratorThread::DispatchKey(const std::string &key,char type) {
  parsers_[thread_index_]->Schedul(key, type);
  thread_index_ = (thread_index_ + 1) % num_thread_;
}

void *MigratorThread::ThreadMain() {
  MigrateDB(type_);
  should_exit_ = true;
  std::cout << type_ << " keys have been dispatched completly" << std::endl;
  return NULL;
}
