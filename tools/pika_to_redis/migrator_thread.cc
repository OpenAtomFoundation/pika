#include "migrator_thread.h"

MigratorThread::~MigratorThread() {
}

void MigratorThread::MigrateDB(char type) {
    rocksdb::ReadOptions iterate_options;
    rocksdb::Iterator *it = db_->NewIterator(iterate_options);
    std::string key_start = "a";
    key_start[0] = type;
    it->Seek(key_start);

    for (; it->Valid(); it->Next()) {
      if (type != it->key().ToString().at(0)) {
        break;
      }
      std::string key = it->key().ToString().substr(1);
      DispatchKey(key, type);
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
