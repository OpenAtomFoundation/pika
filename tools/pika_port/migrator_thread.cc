#include "migrator_thread.h"
#include "const.h"

#include <glog/logging.h>

#include "blackwidow/blackwidow.h"
#include "src/redis_strings.h"
#include "src/scope_snapshot.h"
#include "src/strings_value_format.h"


MigratorThread::~MigratorThread() {
}

void MigratorThread::MigrateStringsDB() {
  blackwidow::RedisStrings* db = (blackwidow::RedisStrings*)(db_);

  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  rocksdb::DB* rocksDB = db->get_db();
  blackwidow::ScopeSnapshot ss(rocksDB, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;
  int64_t curtime;
  if (!rocksDB->GetEnv()->GetCurrentTime(&curtime).ok()) {
    LOG(WARNING) << "failed to get current time by db->GetEnv()->GetCurrentTime()";
	return;
  }

  auto iter = rocksDB->NewIterator(iterator_options);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    blackwidow::ParsedStringsValue parsed_strings_value(iter->value());
	// printf("[key : %-30s] [value : %-30s] [timestamp : %-10d] [version : %d] [survival_time : %d]\n",
    //   iter->key().ToString().c_str(),
    //   parsed_strings_value.value().ToString().c_str(),
    //   parsed_strings_value.timestamp(),
    //   parsed_strings_value.version(),
    //   survival_time);

	int32_t ttl = 0;
    int64_t ts = (int64_t)(parsed_strings_value.timestamp());
	if (ts != 0) {
	  int64_t diff = ts - curtime;
	  ttl = diff > 0 ? diff : -1;
	  if (ttl < 0) {
	    continue;
	  }
	}

    pink::RedisCmdArgsType argv;
    std::string cmd;

    argv.push_back("SET");
    argv.push_back(iter->key().ToString().c_str());
    argv.push_back(parsed_strings_value.value().ToString().c_str());
    if (ts != 0 && ttl > 0) {
      argv.push_back("EX");
      argv.push_back(std::to_string(ttl));
    }
    
    pink::SerializeRedisCommand(argv, &cmd);
    PlusNum();
    cmd += kSuffixKv;
    DispatchKey(cmd);
  }
  delete iter;
}

void MigratorThread::MigrateDB(const char type) {
  if (type == blackwidow::kStrings) {
    MigrateStringsDB();
  } /* else {
    char c_type = 'a';
    switch (type) {
      case blackwidow::kHSize:
          c_type = 'h';
          break;
      case blackwidow::kSSize:
          c_type = 's';
          break;
      case blackwidow:::kLMeta:
          c_type = 'l';
          break;
      case blackwidow::kZSize:
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
  */
}

void MigratorThread::DispatchKey(const std::string &key) {
  (*senders_)[thread_index_]->LoadKey(key);
  thread_index_ = (thread_index_ + 1) % thread_num_;
}

void *MigratorThread::ThreadMain() {
  MigrateDB(type_);
  should_exit_ = true;
  LOG(INFO) << static_cast<char>(type_) << " keys have been dispatched completly";
  return NULL;
}

