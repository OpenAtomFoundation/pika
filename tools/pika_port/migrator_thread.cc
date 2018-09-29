#include "migrator_thread.h"
#include "const.h"

#include <unistd.h>

#include <vector>
#include <functional>

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
    // DispatchKey(cmd, iter->key().ToString());
    DispatchKey(cmd);
  }
  delete iter;
}

void MigratorThread::MigrateListsDB() {
  blackwidow::RedisLists* db = (blackwidow::RedisLists*)(db_);
  std::vector<std::string> keys;

  std::string pattern("*");
  blackwidow::Status s = db->ScanKeys(pattern, &keys);
  if (!s.ok()) {
    LOG(FATAL) << "db->ScanKeys(pattern:*) = " << s.ToString();
    return;
  }

  for (auto k : keys) {
    if (should_exit_) {
      break;
    }

    int64_t pos = 0;
    std::vector<std::string> list;
    s = db->LRange(k, pos, pos + g_conf.sync_batch_num - 1, &list);
    if (!s.ok()) {
      LOG(WARNING) << "db->LRange(key:" << k << ", pos:" << pos
                   << ", batch size: " << g_conf.sync_batch_num << ") = " << s.ToString();
      continue;
    }

    while (s.ok() && !should_exit_ && !list.empty()) {
      pink::RedisCmdArgsType argv;
      std::string cmd;

      argv.push_back("RPUSH");
      argv.push_back(k);
      for (auto e : list) {
        // PlusNum();
        argv.push_back(e);
      }

      pink::SerializeRedisCommand(argv, &cmd);

      PlusNum();
      DispatchKey(cmd, k);

      pos += g_conf.sync_batch_num;
      list.clear();
      s = db->LRange(k, pos, pos + g_conf.sync_batch_num - 1, &list);
      if (!s.ok()) {
        LOG(WARNING) << "db->LRange(key:" << k << ", pos:" << pos
                     << ", batch size: " << g_conf.sync_batch_num << ") = " << s.ToString();
      }
    }
  }
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
    if (should_exit_) {
      break;
    }
    std::vector<blackwidow::FieldValue> fvs;
    s = db->HGetall(k, &fvs);
    if (!s.ok()) {
      LOG(WARNING) << "db->HGetall(key:" << k << ") = " << s.ToString();
      continue;
    }

    auto it = fvs.begin();
    while (!should_exit_ && it != fvs.end()) {
      pink::RedisCmdArgsType argv;
      std::string cmd;

      argv.push_back("HMSET");
      argv.push_back(k);
      for (size_t idx = 0;
           idx < g_conf.sync_batch_num && !should_exit_ && it != fvs.end();
           idx ++, it ++) {
        argv.push_back(it->field);
        argv.push_back(it->value);
        // PlusNum();
      }

      pink::SerializeRedisCommand(argv, &cmd);
      PlusNum();
      // DispatchKey(cmd, k);
      DispatchKey(cmd);
    }
  }
}

void MigratorThread::MigrateSetsDB() {
  blackwidow::RedisSets* db = (blackwidow::RedisSets*)(db_);
  std::vector<std::string> keys;

  std::string pattern("*");
  blackwidow::Status s = db->ScanKeys(pattern, &keys);
  if (!s.ok()) {
    LOG(FATAL) << "db->ScanKeys(pattern:*) = " << s.ToString();
    return;
  }

  for (auto k : keys) {
    if (should_exit_) {
      break;
    }
    std::vector<std::string> members;
    s = db->SMembers(k, &members);
    if (!s.ok()) {
      LOG(WARNING) << "db->SMembers(key:" << k << ") = " << s.ToString();
      continue;
    }
    auto it = members.begin();
    while (!should_exit_ && it != members.end()) {
      std::string cmd;
      pink::RedisCmdArgsType argv;

      argv.push_back("SADD");
      argv.push_back(k);
      for (size_t idx = 0;
           idx < g_conf.sync_batch_num && !should_exit_ && it != members.end();
           idx ++, it ++) {
        argv.push_back(*it);
      }

      pink::SerializeRedisCommand(argv, &cmd);
      PlusNum();
      // DispatchKey(cmd, k);
      DispatchKey(cmd);
    }
  }
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
    if (should_exit_) {
      break;
    }
    std::vector<blackwidow::ScoreMember> score_members;
    s = db->ZRange(k, 0, -1, &score_members);
    if (!s.ok()) {
      LOG(WARNING) << "db->ZRange(key:" << k << ") = " << s.ToString();
      continue;
    }
    auto it = score_members.begin();
    while (!should_exit_ && it != score_members.end()) {
      pink::RedisCmdArgsType argv;
      std::string cmd;

      argv.push_back("ZADD");
      argv.push_back(k);

      for (size_t idx = 0;
           idx < g_conf.sync_batch_num && !should_exit_ && it != score_members.end();
           idx ++, it ++) {
        argv.push_back(std::to_string(it->score));
        argv.push_back(it->member);
      }

      pink::SerializeRedisCommand(argv, &cmd);
      PlusNum();
      // DispatchKey(cmd, k);
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

void MigratorThread::DispatchKey(const std::string &command, const std::string& key) {
  thread_index_ = (thread_index_ + 1) % thread_num_;
  size_t idx = thread_index_;
  if (key.size()) { // no empty
    idx = std::hash<std::string>()(key) % thread_num_;
  }
  (*senders_)[idx]->LoadKey(command);
}

void *MigratorThread::ThreadMain() {
  MigrateDB();
  should_exit_ = true;

  LOG(INFO) << GetDBTypeString(type_) << " keys have been dispatched completly";
  return NULL;
}

