// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/migrator_thread.h"

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

#include "include/pika_conf.h"

const int64_t MAX_BATCH_NUM = 30000;

extern PikaConf* g_pika_conf;

MigratorThread::~MigratorThread() {
}

void MigratorThread::MigrateStringsDB() {
  blackwidow::BlackWidow *bw = (blackwidow::BlackWidow*)(db_);

  int64_t scan_batch_num = g_pika_conf->sync_batch_num() * 10;
  if (MAX_BATCH_NUM < scan_batch_num) {
    if (g_pika_conf->sync_batch_num() < MAX_BATCH_NUM) {
      scan_batch_num = MAX_BATCH_NUM;
    } else {
      scan_batch_num = g_pika_conf->sync_batch_num() * 2;
    }
  }

  int64_t ttl = -1;
  int64_t cursor = 0;
  blackwidow::Status s;
  std::string value;
  std::vector<std::string> keys;
  std::map<blackwidow::DataType, int64_t> type_timestamp;
  std::map<blackwidow::DataType, rocksdb::Status> type_status;
  while (true) {
    cursor = bw->Scan(blackwidow::DataType::kStrings, cursor, "*", scan_batch_num, &keys);

    for (const auto& key : keys) {
      s = bw->Get(key, &value);
      if (!s.ok()) {
        LOG(WARNING) << "get " << key << " error: " << s.ToString();
        continue;
      }

      pink::RedisCmdArgsType argv;
      std::string cmd;

      argv.push_back("SET");
      argv.push_back(key);
      argv.push_back(value);

      ttl = -1;
      type_status.clear();
      type_timestamp = bw->TTL(key, &type_status);
      if (type_timestamp[blackwidow::kStrings] != -2) {
        ttl = type_timestamp[blackwidow::kStrings];
      }

      if (ttl > 0) {
        argv.push_back("EX");
        argv.push_back(std::to_string(ttl));
      }

      pink::SerializeRedisCommand(argv, &cmd);
      PlusNum();
      DispatchKey(cmd, key);
    }

    if (!cursor) {
      break;
    }
  }
}

void MigratorThread::MigrateListsDB() {
  blackwidow::BlackWidow *bw = (blackwidow::BlackWidow*)(db_);

  int64_t scan_batch_num = g_pika_conf->sync_batch_num() * 10;
  if (MAX_BATCH_NUM < scan_batch_num) {
    if (g_pika_conf->sync_batch_num() < MAX_BATCH_NUM) {
      scan_batch_num = MAX_BATCH_NUM;
    } else {
      scan_batch_num = g_pika_conf->sync_batch_num() * 2;
    }
  }

  int64_t ttl = -1;
  int64_t cursor = 0;
  blackwidow::Status s;
  std::vector<std::string> keys;
  std::map<blackwidow::DataType, int64_t> type_timestamp;
  std::map<blackwidow::DataType, rocksdb::Status> type_status;

  while (true) {
    cursor = bw->Scan(blackwidow::DataType::kLists, cursor, "*", scan_batch_num, &keys);

    for (const auto& key : keys) {
      int64_t pos = 0;
      std::vector<std::string> nodes;
      blackwidow::Status s = bw->LRange(key, pos, pos + g_pika_conf->sync_batch_num() - 1, &nodes);
      if (!s.ok()) {
        LOG(WARNING) << "db->LRange(key:" << key << ", pos:" << pos
          << ", batch size: " << g_pika_conf->sync_batch_num() << ") = " << s.ToString();
        continue;
      }

      while (s.ok() && !should_exit_ && !nodes.empty()) {
        pink::RedisCmdArgsType argv;
        std::string cmd;

        argv.push_back("RPUSH");
        argv.push_back(key);
        for (const auto& node : nodes) {
          argv.push_back(node);
        }

        pink::SerializeRedisCommand(argv, &cmd);
        PlusNum();
        DispatchKey(cmd, key);

        pos += g_pika_conf->sync_batch_num();
        nodes.clear();
        s = bw->LRange(key, pos, pos + g_pika_conf->sync_batch_num() - 1, &nodes);
        if (!s.ok()) {
          LOG(WARNING) << "db->LRange(key:" << key << ", pos:" << pos
            << ", batch size:" << g_pika_conf->sync_batch_num() << ") = " << s.ToString();
        }
      }

      ttl = -1;
      type_status.clear();
      type_timestamp = bw->TTL(key, &type_status);
      if (type_timestamp[blackwidow::kLists] != -2) {
        ttl = type_timestamp[blackwidow::kLists];
      }

      if (s.ok() && ttl > 0) {
        pink::RedisCmdArgsType argv;
        std::string cmd;

        argv.push_back("EXPIRE");
        argv.push_back(key);
        argv.push_back(std::to_string(ttl));

        pink::SerializeRedisCommand(argv, &cmd);
        PlusNum();
        DispatchKey(cmd, key);
      }
    }

    if (!cursor) {
      break;
    }
  }
}

void MigratorThread::MigrateHashesDB() {
  blackwidow::BlackWidow *bw = (blackwidow::BlackWidow*)(db_);

  int64_t scan_batch_num = g_pika_conf->sync_batch_num() * 10;
  if (MAX_BATCH_NUM < scan_batch_num) {
    if (g_pika_conf->sync_batch_num() < MAX_BATCH_NUM) {
      scan_batch_num = MAX_BATCH_NUM;
    } else {
      scan_batch_num = g_pika_conf->sync_batch_num() * 2;
    }
  }

  int64_t ttl = -1;
  int64_t cursor = 0;
  blackwidow::Status s;
  std::vector<std::string> keys;
  std::map<blackwidow::DataType, int64_t> type_timestamp;
  std::map<blackwidow::DataType, rocksdb::Status> type_status;

  while (true) {
    cursor = bw->Scan(blackwidow::DataType::kHashes, cursor, "*", scan_batch_num, &keys);

    for (const auto& key : keys) {
      std::vector<blackwidow::FieldValue> fvs;
      blackwidow::Status s = bw->HGetall(key, &fvs);
      if (!s.ok()) {
        LOG(WARNING) << "db->HGetall(key:" << key << ") = " << s.ToString();
        continue;
      }

      auto it = fvs.begin();
      while (!should_exit_ && it != fvs.end()) {
        pink::RedisCmdArgsType argv;
        std::string cmd;

        argv.push_back("HMSET");
        argv.push_back(key);
        for (int idx = 0;
             idx < g_pika_conf->sync_batch_num() && !should_exit_ && it != fvs.end();
             idx++, it++) {
          argv.push_back(it->field);
          argv.push_back(it->value);
        }

        pink::SerializeRedisCommand(argv, &cmd);
        PlusNum();
        DispatchKey(cmd, key);
      }

      ttl = -1;
      type_status.clear();
      type_timestamp = bw->TTL(key, &type_status);
      if (type_timestamp[blackwidow::kHashes] != -2) {
        ttl = type_timestamp[blackwidow::kHashes];
      }

      if (s.ok() && ttl > 0) {
        pink::RedisCmdArgsType argv;
        std::string cmd;

        argv.push_back("EXPIRE");
        argv.push_back(key);
        argv.push_back(std::to_string(ttl));

        pink::SerializeRedisCommand(argv, &cmd);
        PlusNum();
        DispatchKey(cmd, key);
      }
    }

    if (!cursor) {
      break;
    }
  }
}

void MigratorThread::MigrateSetsDB() {
  blackwidow::BlackWidow *bw = (blackwidow::BlackWidow*)(db_);

  int64_t scan_batch_num = g_pika_conf->sync_batch_num() * 10;
  if (MAX_BATCH_NUM < scan_batch_num) {
    if (g_pika_conf->sync_batch_num() < MAX_BATCH_NUM) {
      scan_batch_num = MAX_BATCH_NUM;
    } else {
      scan_batch_num = g_pika_conf->sync_batch_num() * 2;
    }
  }

  int64_t ttl = -1;
  int64_t cursor = 0;
  blackwidow::Status s;
  std::vector<std::string> keys;
  std::map<blackwidow::DataType, int64_t> type_timestamp;
  std::map<blackwidow::DataType, rocksdb::Status> type_status;

  while (true) {
    cursor = bw->Scan(blackwidow::DataType::kSets, cursor, "*", scan_batch_num, &keys);

    for (const auto& key : keys) {
      std::vector<std::string> members;
      blackwidow::Status s = bw->SMembers(key, &members);
      if (!s.ok()) {
        LOG(WARNING) << "db->SMembers(key:" << key << ") = " << s.ToString();
        continue;
      }
      auto it = members.begin();
      while (!should_exit_ && it != members.end()) {
        std::string cmd;
        pink::RedisCmdArgsType argv;

        argv.push_back("SADD");
        argv.push_back(key);
        for (int idx = 0;
             idx < g_pika_conf->sync_batch_num() && !should_exit_ && it != members.end();
             idx++, it++) {
          argv.push_back(*it);
        }

        pink::SerializeRedisCommand(argv, &cmd);
        PlusNum();
        DispatchKey(cmd, key);
      }

      ttl = -1;
      type_status.clear();
      type_timestamp = bw->TTL(key, &type_status);
      if (type_timestamp[blackwidow::kSets] != -2) {
        ttl = type_timestamp[blackwidow::kSets];
      }

      if (s.ok() && ttl > 0) {
        pink::RedisCmdArgsType argv;
        std::string cmd;

        argv.push_back("EXPIRE");
        argv.push_back(key);
        argv.push_back(std::to_string(ttl));

        pink::SerializeRedisCommand(argv, &cmd);
        PlusNum();
        DispatchKey(cmd, key);
      }
    }

    if (!cursor) {
      break;
    }
  }
}

void MigratorThread::MigrateZsetsDB() {
  blackwidow::BlackWidow *bw = (blackwidow::BlackWidow*)(db_);

  int64_t scan_batch_num = g_pika_conf->sync_batch_num() * 10;
  if (MAX_BATCH_NUM < scan_batch_num) {
    if (g_pika_conf->sync_batch_num() < MAX_BATCH_NUM) {
      scan_batch_num = MAX_BATCH_NUM;
    } else {
      scan_batch_num = g_pika_conf->sync_batch_num() * 2;
    }
  }

  int64_t ttl = -1;
  int64_t cursor = 0;
  blackwidow::Status s;
  std::vector<std::string> keys;
  std::map<blackwidow::DataType, int64_t> type_timestamp;
  std::map<blackwidow::DataType, rocksdb::Status> type_status;

  while (true) {
    cursor = bw->Scan(blackwidow::DataType::kZSets, cursor, "*", scan_batch_num, &keys);

    for (const auto& key : keys) {
      std::vector<blackwidow::ScoreMember> score_members;
      blackwidow::Status s = bw->ZRange(key, 0, -1, &score_members);
      if (!s.ok()) {
        LOG(WARNING) << "db->ZRange(key:" << key << ") = " << s.ToString();
        continue;
      }
      auto it = score_members.begin();
      while (!should_exit_ && it != score_members.end()) {
        pink::RedisCmdArgsType argv;
        std::string cmd;

        argv.push_back("ZADD");
        argv.push_back(key);
        for (int idx = 0;
             idx < g_pika_conf->sync_batch_num() && !should_exit_ && it != score_members.end();
             idx++, it++) {
          argv.push_back(std::to_string(it->score));
          argv.push_back(it->member);
        }

        pink::SerializeRedisCommand(argv, &cmd);
        PlusNum();
        DispatchKey(cmd, key);
      }

      ttl = -1;
      type_status.clear();
      type_timestamp = bw->TTL(key, &type_status);
      if (type_timestamp[blackwidow::kZSets] != -2) {
        ttl = type_timestamp[blackwidow::kZSets];
      }

      if (s.ok() && ttl > 0) {
        pink::RedisCmdArgsType argv;
        std::string cmd;

        argv.push_back("EXPIRE");
        argv.push_back(key);
        argv.push_back(std::to_string(ttl));

        pink::SerializeRedisCommand(argv, &cmd);
        PlusNum();
        DispatchKey(cmd, key);
      }
    }

    if (!cursor) {
      break;
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
      LOG(WARNING) << "illegal db type " << type_;
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

const char* GetDBTypeString(int type) {
  switch (type) {
    case int(blackwidow::kStrings) : {
	  return "blackwidow::kStrings";
    }

    case int(blackwidow::kLists) : {
	  return "blackwidow::kLists";
    }

    case int(blackwidow::kHashes) : {
	  return "blackwidow::kHashes";
    }

    case int(blackwidow::kSets) : {
	  return "blackwidow::kSets";
    }

    case int(blackwidow::kZSets) : {
	  return "blackwidow::kZSets";
    }

    default: {
	  return "blackwidow::Unknown";
    }
  }
}

void *MigratorThread::ThreadMain() {
  MigrateDB();
  should_exit_ = true;
  LOG(INFO) << GetDBTypeString(type_) << " keys have been dispatched completly";
  return NULL;
}

