#include "migrator_thread.h"

MigratorThread::~MigratorThread() {
}

void MigratorThread::MigrateDB(char type) {
    rocksdb::ReadOptions iterate_options;
    rocksdb::Iterator *it = db_->NewIterator(iterate_options);

    if (type == nemo::DataType::kKv) {
      std::string start_key = "k";
      std::string key, value;
      it->Seek(start_key);
      while (it->Valid()) {
        key = it->key().ToString();
        value = it->value().ToString();

        pink::RedisCmdArgsType argv;
        std::string cmd;

        int32_t ttl;
        db_->GetKeyTTL(rocksdb::ReadOptions(), key, &ttl);

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
      std::string key_start;
      key_start[0] = type;
      it->Seek(key_start);
      for (; it->Valid(); it->Next()) {
        if (type != it->key().ToString().at(0)) {
          break;
        }
        std::string key = it->key().ToString().substr(1);
        std::cout << key << std::endl;
        DispatchKey(key, type);
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
