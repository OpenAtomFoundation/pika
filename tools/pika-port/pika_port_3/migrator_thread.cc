#include "migrator_thread.h"
#include "const.h"

#include <unistd.h>

#include <glog/logging.h>
#include <functional>
#include <vector>

#include "storage/storage.h"
#include "src/redis_hashes.h"
#include "src/redis_lists.h"
#include "src/redis_sets.h"
#include "src/redis_strings.h"
#include "src/redis_zsets.h"
#include "src/scope_snapshot.h"
#include "src/strings_value_format.h"

const int64_t MAX_BATCH_NUM = 30000;

MigratorThread::~MigratorThread() = default;

void MigratorThread::MigrateStringsDB() {
  auto db = static_cast<storage::RedisStrings*>(db_);

  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  rocksdb::DB* rocksDB = db->GetDB();
  storage::ScopeSnapshot ss(rocksDB, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;
  int64_t curtime;
  if (!rocksDB->GetEnv()->GetCurrentTime(&curtime).ok()) {
    LOG(WARNING) << "failed to get current time by db->GetEnv()->GetCurrentTime()";
    return;
  }

  auto iter = rocksDB->NewIterator(iterator_options);
  for (iter->SeekToFirst(); !should_exit_ && iter->Valid(); iter->Next()) {
    storage::ParsedStringsValue parsed_strings_value(iter->value());
    int32_t ttl = 0;
    auto ts = static_cast<int64_t>(parsed_strings_value.timestamp());
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

    net::RedisCmdArgsType argv;
    std::string cmd;

    argv.push_back("SET");
    argv.push_back(iter->key().ToString());
    argv.push_back(parsed_strings_value.value().ToString());
    if (ts != 0 && ttl > 0) {
      argv.push_back("EX");
      argv.push_back(std::to_string(ttl));
    }

    net::SerializeRedisCommand(argv, &cmd);
    PlusNum();
    // DispatchKey(cmd, iter->key().ToString());
    DispatchKey(cmd);
  }
  delete iter;
}

void MigratorThread::MigrateListsDB() {
  auto db = static_cast<storage::RedisLists*>(db_);

  std::string start_key;
  std::string next_key;
  std::string pattern("*");
  int64_t batch_count = g_conf.sync_batch_num * 10;
  if (MAX_BATCH_NUM < batch_count) {
    if (g_conf.sync_batch_num < MAX_BATCH_NUM) {
      batch_count = MAX_BATCH_NUM;
    } else {
      batch_count = g_conf.sync_batch_num * 2;
    }
  }

  bool fin = false;
  while (!fin) {
    int64_t count = batch_count;
    std::vector<std::string> keys;
    fin = db->Scan(start_key, pattern, &keys, &count, &next_key);
    // LOG(INFO) << "batch count: " << count << ", fin: " << fin << ", keys.size(): " << keys.size() << ", next_key: "
    // << next_key;
    if (fin && keys.empty()) {
      break;
    }
    start_key = next_key;

    for (const auto& k : keys) {
      if (should_exit_) {
        break;
      }

      int64_t pos = 0;
      std::vector<std::string> list;
      storage::Status s = db->LRange(k, pos, pos + g_conf.sync_batch_num - 1, &list);
      if (!s.ok()) {
        LOG(WARNING) << "db->LRange(key:" << k << ", pos:" << pos << ", batch size: " << g_conf.sync_batch_num
                     << ") = " << s.ToString();
        continue;
      }

      while (s.ok() && !should_exit_ && !list.empty()) {
        net::RedisCmdArgsType argv;
        std::string cmd;

        argv.push_back("RPUSH");
        argv.push_back(k);
        for (const auto& e : list) {
          // PlusNum();
          argv.push_back(e);
        }

        net::SerializeRedisCommand(argv, &cmd);

        PlusNum();
        DispatchKey(cmd, k);

        pos += g_conf.sync_batch_num;
        list.clear();
        s = db->LRange(k, pos, pos + g_conf.sync_batch_num - 1, &list);
        if (!s.ok()) {
          LOG(WARNING) << "db->LRange(key:" << k << ", pos:" << pos << ", batch size:" << g_conf.sync_batch_num
                       << ") = " << s.ToString();
        }
      }

      int64_t ttl = -1;
      s = db->TTL(k, &ttl);
      if (s.ok() && ttl > 0) {
        net::RedisCmdArgsType argv;
        std::string cmd;

        argv.push_back("EXPIRE");
        argv.push_back(k);
        argv.push_back(std::to_string(ttl));
        net::SerializeRedisCommand(argv, &cmd);
        PlusNum();
        DispatchKey(cmd);
      }
    }  // for
  }    // while
}

void MigratorThread::MigrateHashesDB() {
  auto db = static_cast<storage::RedisHashes*>(db_);

  std::string start_key;
  std::string next_key;
  std::string pattern("*");
  int64_t batch_count = g_conf.sync_batch_num * 10;
  if (MAX_BATCH_NUM < batch_count) {
    if (g_conf.sync_batch_num < MAX_BATCH_NUM) {
      batch_count = MAX_BATCH_NUM;
    } else {
      batch_count = g_conf.sync_batch_num * 2;
    }
  }
  bool fin = false;
  while (!fin) {
    int64_t count = batch_count;
    std::vector<std::string> keys;
    fin = db->Scan(start_key, pattern, &keys, &count, &next_key);
    if (fin && keys.empty()) {
      break;
    }
    start_key = next_key;

    for (const auto& k : keys) {
      if (should_exit_) {
        break;
      }
      std::vector<storage::FieldValue> fvs;
      storage::Status s = db->HGetall(k, &fvs);
      if (!s.ok()) {
        LOG(WARNING) << "db->HGetall(key:" << k << ") = " << s.ToString();
        continue;
      }

      auto it = fvs.begin();
      while (!should_exit_ && it != fvs.end()) {
        net::RedisCmdArgsType argv;
        std::string cmd;

        argv.push_back("HMSET");
        argv.push_back(k);
        for (size_t idx = 0; idx < g_conf.sync_batch_num && !should_exit_ && it != fvs.end(); idx++, it++) {
          argv.push_back(it->field);
          argv.push_back(it->value);
          // PlusNum();
        }

        net::SerializeRedisCommand(argv, &cmd);
        PlusNum();
        // DispatchKey(cmd, k);
        DispatchKey(cmd);
      }

      int64_t ttl = -1;
      s = db->TTL(k, &ttl);
      if (s.ok() && ttl > 0) {
        net::RedisCmdArgsType argv;
        std::string cmd;

        argv.push_back("EXPIRE");
        argv.push_back(k);
        argv.push_back(std::to_string(ttl));
        net::SerializeRedisCommand(argv, &cmd);
        PlusNum();
        DispatchKey(cmd);
      }
    }  // for
  }    // while
}

void MigratorThread::MigrateSetsDB() {
  auto db = static_cast<storage::RedisSets*>(db_);

  std::string start_key;
  std::string next_key;
  std::string pattern("*");
  int64_t batch_count = g_conf.sync_batch_num * 10;
  if (MAX_BATCH_NUM < batch_count) {
    if (g_conf.sync_batch_num < MAX_BATCH_NUM) {
      batch_count = MAX_BATCH_NUM;
    } else {
      batch_count = g_conf.sync_batch_num * 2;
    }
  }
  bool fin = false;
  while (!fin) {
    int64_t count = batch_count;
    std::vector<std::string> keys;
    fin = db->Scan(start_key, pattern, &keys, &count, &next_key);
    if (fin && keys.empty()) {
      break;
    }
    start_key = next_key;

    for (const auto& k : keys) {
      if (should_exit_) {
        break;
      }
      std::vector<std::string> members;
      storage::Status s = db->SMembers(k, &members);
      if (!s.ok()) {
        LOG(WARNING) << "db->SMembers(key:" << k << ") = " << s.ToString();
        continue;
      }
      auto it = members.begin();
      while (!should_exit_ && it != members.end()) {
        std::string cmd;
        net::RedisCmdArgsType argv;

        argv.push_back("SADD");
        argv.push_back(k);
        for (size_t idx = 0; idx < g_conf.sync_batch_num && !should_exit_ && it != members.end(); idx++, it++) {
          argv.push_back(*it);
        }

        net::SerializeRedisCommand(argv, &cmd);
        PlusNum();
        // DispatchKey(cmd, k);
        DispatchKey(cmd);
      }

      int64_t ttl = -1;
      s = db->TTL(k, &ttl);
      if (s.ok() && ttl > 0) {
        net::RedisCmdArgsType argv;
        std::string cmd;

        argv.push_back("EXPIRE");
        argv.push_back(k);
        argv.push_back(std::to_string(ttl));
        net::SerializeRedisCommand(argv, &cmd);
        PlusNum();
        DispatchKey(cmd);
      }
    }  // for
  }    // while
}

void MigratorThread::MigrateZsetsDB() {
  auto db = static_cast<storage::RedisZSets*>(db_);

  std::string start_key;
  std::string next_key;
  std::string pattern("*");
  int64_t batch_count = g_conf.sync_batch_num * 10;
  if (MAX_BATCH_NUM < batch_count) {
    if (g_conf.sync_batch_num < MAX_BATCH_NUM) {
      batch_count = MAX_BATCH_NUM;
    } else {
      batch_count = g_conf.sync_batch_num * 2;
    }
  }
  bool fin = false;
  while (!fin) {
    int64_t count = batch_count;
    std::vector<std::string> keys;
    fin = db->Scan(start_key, pattern, &keys, &count, &next_key);
    if (fin && keys.empty()) {
      break;
    }
    start_key = next_key;

    for (const auto& k : keys) {
      if (should_exit_) {
        break;
      }
      std::vector<storage::ScoreMember> score_members;
      storage::Status s = db->ZRange(k, 0, -1, &score_members);
      if (!s.ok()) {
        LOG(WARNING) << "db->ZRange(key:" << k << ") = " << s.ToString();
        continue;
      }
      auto it = score_members.begin();
      while (!should_exit_ && it != score_members.end()) {
        net::RedisCmdArgsType argv;
        std::string cmd;

        argv.push_back("ZADD");
        argv.push_back(k);

        for (size_t idx = 0; idx < g_conf.sync_batch_num && !should_exit_ && it != score_members.end(); idx++, it++) {
          argv.push_back(std::to_string(it->score));
          argv.push_back(it->member);
        }

        net::SerializeRedisCommand(argv, &cmd);
        PlusNum();
        // DispatchKey(cmd, k);
        DispatchKey(cmd);
      }

      int64_t ttl = -1;
      s = db->TTL(k, &ttl);
      if (s.ok() && ttl > 0) {
        net::RedisCmdArgsType argv;
        std::string cmd;

        argv.push_back("EXPIRE");
        argv.push_back(k);
        argv.push_back(std::to_string(ttl));
        net::SerializeRedisCommand(argv, &cmd);
        PlusNum();
        DispatchKey(cmd);
      }
    }  // for
  }    // while
}

void MigratorThread::MigrateDB() {
  switch (static_cast<int>(type_)) {
    case static_cast<int>(storage::DataType::kStrings): {
      MigrateStringsDB();
      break;
    }

    case static_cast<int>(storage::DataType::kLists): {
      MigrateListsDB();
      break;
    }

    case static_cast<int>(storage::DataType::kHashes): {
      MigrateHashesDB();
      break;
    }

    case static_cast<int>(storage::DataType::kSets): {
      MigrateSetsDB();
      break;
    }

    case static_cast<int>(storage::DataType::kZSets): {
      MigrateZsetsDB();
      break;
    }

    default: {
      LOG(WARNING) << "illegal db type " << type_;
      break;
    }
  }
}

void MigratorThread::DispatchKey(const std::string& command, const std::string& key) {
  thread_index_ = (thread_index_ + 1) % thread_num_;
  size_t idx = thread_index_;
  if (!key.empty()) {  // no empty
    idx = std::hash<std::string>()(key) % thread_num_;
  }
  (*senders_)[idx]->LoadKey(command);
}

void* MigratorThread::ThreadMain() {
  MigrateDB();
  should_exit_ = true;
  LOG(INFO) << GetDBTypeString(type_) << " keys have been dispatched completly";
  return nullptr;
}
