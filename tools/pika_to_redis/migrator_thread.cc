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
        std::cout << key << std::endl;
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
        DispatchKey(cmd, type);
      }
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
          std::cout << keys[i] << std::endl;
          DispatchKey(keys[i], type);
      }
    }
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
