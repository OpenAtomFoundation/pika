#include "migrator_thread.h"
#include "const.h"

#include <unistd.h>

#include <vector>

#include <glog/logging.h>

#include "blackwidow/blackwidow.h"

#include "src/redis_strings.h"
#include "src/redis_lists.h"
#include "src/redis_hashes.h"
#include "src/redis_sets.h"
#include "src/redis_zsets.h"
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
  for (iter->SeekToFirst(); !should_exit_ && iter->Valid(); iter->Next()) {
    blackwidow::ParsedStringsValue parsed_strings_value(iter->value());
    int32_t ttl = 0;
    int64_t ts = (int64_t)(parsed_strings_value.timestamp());
    if (ts != 0) {
      int64_t diff = ts - curtime;
      ttl = diff > 0 ? diff : -1;
      if (ttl < 0) {
        continue;
      }
    }

    // printf("[key : %-30s] [value : %-30s] [timestamp : %-10d] [version : %d] [survival_time : %d]\n",
    //   iter->key().ToString().c_str(),
    //   parsed_strings_value.value().ToString().c_str(),
    //   parsed_strings_value.timestamp(),
    //   parsed_strings_value.version(),
    //   ttl);
    // sleep(3);

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
    DispatchKey(cmd);
  }
  delete iter;
}

void MigratorThread::MigrateListsDB() {

}

void MigratorThread::MigrateHashesDB() {
  blackwidow::RedisHashes* db = (blackwidow::RedisHashes*)(db_);
  std::vector<std::string> keys;

  std::string pattern("*");
  blackwidow::Status s = db->ScanKeys(pattern, &keys);
  if (!s.ok()) {
    LOG(FATAL) << "db->ScanKeys(pattern:*) = " << s.ToString();
    return;
  }

  for (auto k : keys) {
    std::vector<blackwidow::FieldValue> fvs;
    s = db->HGetall(k, &fvs);
    if (!s.ok()) {
      LOG(WARNING) << "db->HGetall(key:" << k << ") = " << s.ToString();
      continue;
    }
    for (auto fv : fvs) {
      pink::RedisCmdArgsType argv;
      std::string cmd;

      argv.push_back("HSET");
      argv.push_back(k);
      argv.push_back(fv.field);
      argv.push_back(fv.value);
      
      pink::SerializeRedisCommand(argv, &cmd);
      PlusNum();
      DispatchKey(cmd);
    }
  }
}

void MigratorThread::MigrateSetsDB() {
}

void MigratorThread::MigrateZsetsDB() {
  blackwidow::RedisZSets* db = (blackwidow::RedisZSets*)(db_);
  std::vector<std::string> keys;

  std::string pattern("*");
  blackwidow::Status s = db->ScanKeys(pattern, &keys);
  if (!s.ok()) {
    LOG(FATAL) << "db->ScanKeys(pattern:*) = " << s.ToString();
    return;
  }

  for (auto k : keys) {
    std::vector<blackwidow::ScoreMember> score_members;
    s = db->ZRange(k, 0, -1, &score_members);
    if (!s.ok()) {
      LOG(WARNING) << "db->ZRange(key:" << k << ") = " << s.ToString();
      continue;
    }
    for (auto sm : score_members) {
      pink::RedisCmdArgsType argv;
      std::string cmd;

	  std::string score = std::to_string(sm.score);

      argv.push_back("ZADD");
      argv.push_back(k);
      argv.push_back(score);
      argv.push_back(sm.member);
      
      pink::SerializeRedisCommand(argv, &cmd);
      PlusNum();
      DispatchKey(cmd);
    }
  }
}

void MigratorThread::MigrateDB() {
  switch (int(type_)) {
    case int(blackwidow::kStrings) : {
      MigrateStringsDB();
      break;
    }

    case int(blackwidow::kLists) : {
      MigrateListsDB();
      break;
    }

    case int(blackwidow::kHashes) : {
      MigrateHashesDB();
      break;
    }

    case int(blackwidow::kSets) : {
      MigrateSetsDB();
      break;
    }

    case int(blackwidow::kZSets) : {
      MigrateZsetsDB();
      break;
    }

    default: {
      LOG(ERROR) << "illegal db type " << type_;
      break;
    }
  }
}

void MigratorThread::DispatchKey(const std::string &key) {
  (*senders_)[thread_index_]->LoadKey(key);
  thread_index_ = (thread_index_ + 1) % thread_num_;
}

void *MigratorThread::ThreadMain() {
  MigrateDB();
  should_exit_ = true;

  LOG(INFO) << GetDBTypeString(type_) << " keys have been dispatched completly";
  return NULL;
}

