#include "migrator_thread.h"

MigratorThread::~MigratorThread() {
}

void MigratorThread::MigrateDB(const char type) {
    if (type == nemo::DataType::kKv) {
      nemo::KIterator *it = db_->KScan("", "", -1, false);
      std::string key, value;

      while (it->Valid()) {
        key = it->key();
        value = it->value();
        net::RedisCmdArgsType argv;
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
        net::SerializeRedisCommand(argv, &cmd);
        PlusNum();
        cmd = 'k' + cmd;
        DispatchKey(cmd);
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
        rocksdb::Iterator *it = db_->Scanbytype(c_type);
        std::string key_start = "a";
        key_start[0] = type;
        it->Seek(key_start);
        for (; it->Valid(); it->Next()) {
          PlusNum();
          DispatchKey(it->key().ToString());
        }
    }
}

void MigratorThread::DispatchKey(const std::string &key) {
  (*senders_)[thread_index_]->LoadKey(key);
  thread_index_ = (thread_index_ + 1) % thread_num_;
}

void *MigratorThread::ThreadMain() {
  MigrateDB(type_);
  should_exit_ = true;
  log_info("%c keys have been dispatched completly", static_cast<char>(type_));
  return NULL;
}
