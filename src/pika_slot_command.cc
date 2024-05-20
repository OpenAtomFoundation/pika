// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "include/pika_admin.h"
#include "include/pika_cmd_table_manager.h"
#include "include/pika_command.h"
#include "include/pika_conf.h"
#include "include/pika_data_distribution.h"
#include "include/pika_define.h"
#include "include/pika_migrate_thread.h"
#include "include/pika_rm.h"
#include "include/pika_server.h"
#include "include/pika_slot_command.h"
#include "pstd/include/pika_codis_slot.h"
#include "pstd/include/pstd_status.h"
#include "pstd/include/pstd_string.h"
#include "src/redis_streams.h"
#include "storage/include/storage/storage.h"

#define min(a, b) (((a) > (b)) ? (b) : (a))
#define MAX_MEMBERS_NUM 512

extern std::unique_ptr<PikaServer> g_pika_server;
extern std::unique_ptr<PikaConf> g_pika_conf;
extern std::unique_ptr<PikaReplicaManager> g_pika_rm;
extern std::unique_ptr<PikaCmdTableManager> g_pika_cmd_table_manager;

PikaMigrate::PikaMigrate() { migrate_clients_.clear(); }

PikaMigrate::~PikaMigrate() {
  // close and release all clients
  // get the mutex lock
  std::lock_guard lm(mutex_);
  KillAllMigrateClient();
}

net::NetCli *PikaMigrate::GetMigrateClient(const std::string &host, const int port, int timeout) {
  std::string ip_port = host + ":" + std::to_string(port);
  net::NetCli *migrate_cli;
  pstd::Status s;

  auto migrate_clients_iter = migrate_clients_.find(ip_port);
  if (migrate_clients_iter == migrate_clients_.end()) {
    migrate_cli = net::NewRedisCli();
    s = migrate_cli->Connect(host, port, g_pika_server->host());
    if (!s.ok()) {
      LOG(ERROR) << "GetMigrateClient: new  migrate_cli[" << ip_port.c_str() << "] failed";

      delete migrate_cli;
      return nullptr;
    }

    LOG(INFO) << "GetMigrateClient: new  migrate_cli[" << ip_port.c_str() << "]";

    // add a new migrate client to the map
    migrate_clients_[ip_port] = migrate_cli;
  } else {
    migrate_cli = static_cast<net::NetCli *>(migrate_clients_iter->second);
  }

  // set the client connect timeout
  migrate_cli->set_send_timeout(timeout);
  migrate_cli->set_recv_timeout(timeout);

  // modify the client last time
  gettimeofday(&migrate_cli->last_interaction_, nullptr);

  return migrate_cli;
}

void PikaMigrate::KillMigrateClient(net::NetCli *migrate_cli) {
  auto migrate_clients_iter = migrate_clients_.begin();
  while (migrate_clients_iter != migrate_clients_.end()) {
    if (migrate_cli == static_cast<net::NetCli *>(migrate_clients_iter->second)) {
      LOG(INFO) << "KillMigrateClient: kill  migrate_cli[" << migrate_clients_iter->first.c_str() << "]";

      migrate_cli->Close();
      delete migrate_cli;
      migrate_cli = nullptr;

      migrate_clients_.erase(migrate_clients_iter);
      break;
    }

    ++migrate_clients_iter;
  }
}

// clean and realse timeout client
void PikaMigrate::CleanMigrateClient() {
  struct timeval now;

  // if the size of migrate_clients_ <= 0, don't need clean
  if (migrate_clients_.size() <= 0) {
    return;
  }

  gettimeofday(&now, nullptr);
  auto migrate_clients_iter = migrate_clients_.begin();
  while (migrate_clients_iter != migrate_clients_.end()) {
    auto migrate_cli = static_cast<net::NetCli *>(migrate_clients_iter->second);
    // pika_server do DoTimingTask every 10s, so we Try colse the migrate_cli before pika timeout, do it at least 20s in
    // advance
    int timeout = (g_pika_conf->timeout() > 0) ? g_pika_conf->timeout() : 60;
    if (now.tv_sec - migrate_cli->last_interaction_.tv_sec > timeout - 20) {
      LOG(INFO) << "CleanMigrateClient: clean  migrate_cli[" << migrate_clients_iter->first.c_str() << "]";
      migrate_cli->Close();
      delete migrate_cli;

      migrate_clients_iter = migrate_clients_.erase(migrate_clients_iter);
    } else {
      ++migrate_clients_iter;
    }
  }
}

// clean and realse all client
void PikaMigrate::KillAllMigrateClient() {
  auto migrate_clients_iter = migrate_clients_.begin();
  while (migrate_clients_iter != migrate_clients_.end()) {
    auto migrate_cli = static_cast<net::NetCli *>(migrate_clients_iter->second);

    LOG(INFO) << "KillAllMigrateClient: kill  migrate_cli[" << migrate_clients_iter->first.c_str() << "]";

    migrate_cli->Close();
    delete migrate_cli;

    migrate_clients_iter = migrate_clients_.erase(migrate_clients_iter);
  }
}

/* *
 * do migrate a key-value for slotsmgrt/slotsmgrtone commands
 * return value:
 *    -1 - error happens
 *   >=0 - # of success migration (0 or 1)
 * */
int PikaMigrate::MigrateKey(const std::string &host, const int port, int timeout, const std::string& key,
                            const char type, std::string &detail, const std::shared_ptr<DB>& db) {
  int send_command_num = -1;

  net::NetCli *migrate_cli = GetMigrateClient(host, port, timeout);
  if (!migrate_cli) {
    detail = "IOERR error or timeout connecting to the client";
    return -1;
  }

  send_command_num = MigrateSend(migrate_cli, key, type, detail, db);
  if (send_command_num <= 0) {
    return send_command_num;
  }

  if (MigrateRecv(migrate_cli, send_command_num, detail)) {
    return send_command_num;
  }

  return -1;
}

int PikaMigrate::MigrateSend(net::NetCli* migrate_cli, const std::string& key, const char type, std::string& detail,
                             const std::shared_ptr<DB>& db) {
  std::string wbuf_str;
  pstd::Status s;
  int command_num = -1;

  // chech the client is alive
  if (!migrate_cli) {
    return -1;
  }

  command_num = ParseKey(key, type, wbuf_str, db);
  if (command_num < 0) {
    detail = "ParseKey failed";
    return command_num;
  }

  // don't need seed data, key is not exists
  if (command_num == 0 || wbuf_str.empty()) {
    return 0;
  }

  s = migrate_cli->Send(&wbuf_str);
  if (!s.ok()) {
    LOG(ERROR) << "Connect slots target, Send error: " << s.ToString();
    detail = "Connect slots target, Send error: " + s.ToString();
    KillMigrateClient(migrate_cli);
    return -1;
  }

  return command_num;
}

bool PikaMigrate::MigrateRecv(net::NetCli* migrate_cli, int need_receive, std::string& detail) {
  pstd::Status s;
  std::string reply;
  int64_t ret;

  if (nullptr == migrate_cli || need_receive < 0) {
    return false;
  }

  net::RedisCmdArgsType argv;
  while (need_receive) {
    s = migrate_cli->Recv(&argv);
    if (!s.ok()) {
      LOG(ERROR) << "Connect slots target, Recv error: " << s.ToString();
      detail = "Connect slots target, Recv error: " + s.ToString();
      KillMigrateClient(migrate_cli);
      return false;
    }

    reply = argv[0];
    need_receive--;

    // set   return ok
    // zadd  return number
    // hset  return 0 or 1
    // hmset return ok
    // sadd  return number
    // rpush return length
    // xadd  return stream-id
    if (argv.size() == 1 &&
        (kInnerReplOk == pstd::StringToLower(reply) || pstd::string2int(reply.data(), reply.size(), &ret))) {
      // continue reiceve response
      if (need_receive > 0) {
        continue;
      }

      // has got all responses
      break;
    }

    // failed
    detail = "something wrong with slots migrate, reply: " + reply;
    LOG(ERROR) << "something wrong with slots migrate, reply:" << reply;
    return false;
  }

  return true;
}

// return -1 is error; 0 don't migrate; >0 the number of commond
int PikaMigrate::ParseKey(const std::string& key, const char type, std::string& wbuf_str, const std::shared_ptr<DB>& db) {
  int command_num = -1;
  int64_t ttl = 0;
  rocksdb::Status s;
  switch (type) {
    case 'k':
      command_num = ParseKKey(key, wbuf_str, db);
      break;
    case 'h':
      command_num = ParseHKey(key, wbuf_str, db);
      break;
    case 'l':
      command_num = ParseLKey(key, wbuf_str, db);
      break;
    case 'z':
      command_num = ParseZKey(key, wbuf_str, db);
      break;
    case 's':
      command_num = ParseSKey(key, wbuf_str, db);
      break;
    case 'm':
      command_num = ParseMKey(key, wbuf_str, db);
      break;
    default:
      LOG(INFO) << "ParseKey key[" << key << "], the type[" << type << "] is not support.";
      return -1;
      break;
  }

  // error or key is not existed
  if (command_num <= 0) {
    LOG(INFO) << "ParseKey key[" << key << "], parse return " << command_num
              << ", the key maybe is not exist or expired.";
    return command_num;
  }

  // skip kv, stream because kv and stream cmd: SET key value ttl
  if (type == 'k' || type == 'm') {
    return command_num;
  }

  ttl = TTLByType(type, key, db);

  //-1 indicates the key is valid forever
  if (ttl == -1) {
    return command_num;
  }

  // key is expired or not exist, don't migrate
  if (ttl == 0 or ttl == -2) {
    wbuf_str.clear();
    return 0;
  }

  // no kv, because kv cmd: SET key value ttl
  if (SetTTL(key, wbuf_str, ttl)) {
    command_num += 1;
  }

  return command_num;
}

bool PikaMigrate::SetTTL(const std::string& key, std::string& wbuf_str, int64_t ttl) {
  //-1 indicates the key is valid forever
  if (ttl == -1) {
    return false;
  }

  // if ttl = -2 indicates, the key is not existed
  if (ttl < 0) {
    LOG(INFO) << "SetTTL key[" << key << "], ttl is " << ttl;
    ttl = 0;
  }

  net::RedisCmdArgsType argv;
  std::string cmd;

  argv.emplace_back("EXPIRE");
  argv.emplace_back(key);
  argv.emplace_back(std::to_string(ttl));

  net::SerializeRedisCommand(argv, &cmd);
  wbuf_str.append(cmd);

  return true;
}

// return -1 is error; 0 don't migrate; >0 the number of commond
int PikaMigrate::ParseKKey(const std::string& key, std::string& wbuf_str, const std::shared_ptr<DB>& db) {
  net::RedisCmdArgsType argv;
  std::string cmd;
  std::string value;
  int64_t ttl = 0;
  rocksdb::Status s;

  s = db->storage()->Get(key, &value);

  // if key is not existed, don't migrate
  if (s.IsNotFound()) {
    return 0;
  }

  if (!s.ok()) {
    return -1;
  }

  argv.emplace_back("SET");
  argv.emplace_back(key);
  argv.emplace_back(value);

  ttl = TTLByType('k', key, db);

  // ttl = -1 indicates the key is valid forever, dont process
  // key is expired or not exist, dont migrate
  // todo check ttl
  if (ttl == 0 || ttl == -2) {
    wbuf_str.clear();
    return 0;
  }

  if (ttl > 0) {
    argv.emplace_back("EX");
    argv.emplace_back(std::to_string(ttl));
  }
  net::SerializeRedisCommand(argv, &cmd);
  wbuf_str.append(cmd);
  return 1;
}

int64_t PikaMigrate::TTLByType(const char key_type, const std::string& key, const std::shared_ptr<DB>& db) {
  return db->storage()->TTL(key);
}

int PikaMigrate::ParseZKey(const std::string& key, std::string& wbuf_str, const std::shared_ptr<DB>& db) {
  int command_num = 0;

  int64_t next_cursor = 0;
  std::vector<storage::ScoreMember> score_members;
  do {
    score_members.clear();
    rocksdb::Status s = db->storage()->ZScan(key, next_cursor, "*", MAX_MEMBERS_NUM, &score_members, &next_cursor);
    if (s.ok()) {
      if (score_members.empty()) {
        break;
      }

      net::RedisCmdArgsType argv;
      std::string cmd;
      argv.emplace_back("ZADD");
      argv.emplace_back(key);

      for (const auto &score_member : score_members) {
        argv.emplace_back(std::to_string(score_member.score));
        argv.emplace_back(score_member.member);
      }

      net::SerializeRedisCommand(argv, &cmd);
      wbuf_str.append(cmd);
      command_num++;
    } else if (s.IsNotFound()) {
      wbuf_str.clear();
      return 0;
    } else {
      wbuf_str.clear();
      return -1;
    }
  } while (next_cursor > 0);

  return command_num;
}

// return -1 is error; 0 don't migrate; >0 the number of commond
int PikaMigrate::ParseHKey(const std::string& key, std::string& wbuf_str, const std::shared_ptr<DB>& db) {
  int64_t next_cursor = 0;
  int command_num = 0;
  std::vector<storage::FieldValue> field_values;
  do {
    field_values.clear();
    rocksdb::Status s = db->storage()->HScan(key, next_cursor, "*", MAX_MEMBERS_NUM, &field_values, &next_cursor);
    if (s.ok()) {
      if (field_values.empty()) {
        break;
      }

      net::RedisCmdArgsType argv;
      std::string cmd;
      argv.emplace_back("HMSET");
      argv.emplace_back(key);

      for (const auto &field_value : field_values) {
        argv.emplace_back(field_value.field);
        argv.emplace_back(field_value.value);
      }

      net::SerializeRedisCommand(argv, &cmd);
      wbuf_str.append(cmd);
      command_num++;
    } else if (s.IsNotFound()) {
      wbuf_str.clear();
      return 0;
    } else {
      wbuf_str.clear();
      return -1;
    }
  } while (next_cursor > 0);

  return command_num;
}

// return -1 is error; 0 don't migrate; >0 the number of commond
int PikaMigrate::ParseSKey(const std::string& key, std::string& wbuf_str, const std::shared_ptr<DB>& db) {
  int command_num = 0;
  int64_t next_cursor = 0;
  std::vector<std::string> members;

  do {
    members.clear();
    rocksdb::Status s = db->storage()->SScan(key, next_cursor, "*", MAX_MEMBERS_NUM, &members, &next_cursor);

    if (s.ok()) {
      if (members.empty()) {
        break;
      }

      net::RedisCmdArgsType argv;
      std::string cmd;
      argv.emplace_back("SADD");
      argv.emplace_back(key);

      for (const auto &member : members) {
        argv.emplace_back(member);
      }

      net::SerializeRedisCommand(argv, &cmd);
      wbuf_str.append(cmd);
      command_num++;
    } else if (s.IsNotFound()) {
      wbuf_str.clear();
      return 0;
    } else {
      wbuf_str.clear();
      return -1;
    }
  } while (next_cursor > 0);

  return command_num;
}

int PikaMigrate::ParseMKey(const std::string& key, std::string& wbuf_str, const std::shared_ptr<DB>& db) {
  int command_num = 0;
  std::vector<storage::IdMessage> id_messages;
  storage::StreamScanArgs arg;
  storage::StreamUtils::StreamParseIntervalId("-", arg.start_sid, &arg.start_ex, 0);
  storage::StreamUtils::StreamParseIntervalId("+", arg.end_sid, &arg.end_ex, UINT64_MAX);
  auto s = db->storage()->XRange(key, arg, id_messages);

  if (s.ok()) {
    net::RedisCmdArgsType argv;
    std::string cmd;
    argv.emplace_back("XADD");
    argv.emplace_back(key);
    for (auto &fv : id_messages) {
      std::vector<std::string> message;
      storage::StreamUtils::DeserializeMessage(fv.value, message);
      storage::streamID sid;
      sid.DeserializeFrom(fv.field);
      argv.emplace_back(sid.ToString());
      for (auto &m : message) {
        argv.emplace_back(m);
      }
    }
    net::SerializeRedisCommand(argv, &cmd);
    wbuf_str.append(cmd);
    command_num++;
  } else if (s.IsNotFound()) {
    wbuf_str.clear();
    return 0;
  } else {
    wbuf_str.clear();
    return -1;
  }
  return command_num;
}

// return -1 is error; 0 don't migrate; >0 the number of commond
int PikaMigrate::ParseLKey(const std::string& key, std::string& wbuf_str, const std::shared_ptr<DB>& db) {
  int64_t left = 0;
  int command_num = 0;
  std::vector<std::string> values;

  net::RedisCmdArgsType argv;
  std::string cmd;

  // del old key, before migrate list; prevent redo when failed
  argv.emplace_back("DEL");
  argv.emplace_back(key);
  net::SerializeRedisCommand(argv, &cmd);
  wbuf_str.append(cmd);
  command_num++;

  do {
    values.clear();
    rocksdb::Status s = db->storage()->LRange(key, left, left + (MAX_MEMBERS_NUM - 1), &values);
    if (s.ok()) {
      if (values.empty()) {
        break;
      }

      net::RedisCmdArgsType argv;
      std::string cmd;

      argv.emplace_back("RPUSH");
      argv.emplace_back(key);

      for (const auto &value : values) {
        argv.emplace_back(value);
      }

      net::SerializeRedisCommand(argv, &cmd);
      wbuf_str.append(cmd);
      command_num++;

      left += MAX_MEMBERS_NUM;
    } else if (s.IsNotFound()) {
      wbuf_str.clear();
      return 0;
    } else {
      wbuf_str.clear();
      return -1;
    }
  } while (!values.empty());

  if (command_num == 1) {
    wbuf_str.clear();
    command_num = 0;
  }

  return command_num;
}

/* *
 * do migrate a key-value for slotsmgrt/slotsmgrtone commands
 * return value:
 *    -1 - error happens
 *   >=0 - # of success migration (0 or 1)
 * */
static int SlotsMgrtOne(const std::string &host, const int port, int timeout, const std::string& key, const char type,
                        std::string& detail, const std::shared_ptr<DB>& db) {
  int send_command_num = 0;
  rocksdb::Status s;
  std::map<storage::DataType, rocksdb::Status> type_status;

  send_command_num = g_pika_server->pika_migrate_->MigrateKey(host, port, timeout, key, type, detail, db);

  // the key is migrated to target, delete key and slotsinfo
  if (send_command_num >= 1) {
    std::vector<std::string> keys;
    keys.emplace_back(key);
    int64_t count = db->storage()->Del(keys);
    if (count > 0) {
      WriteDelKeyToBinlog(key, db);
    }

    // del slots info
    RemSlotKeyByType(std::string(1, type), key, db);
    return 1;
  }

  // key is not existed, only del slotsinfo
  if (send_command_num == 0) {
    // del slots info
    RemSlotKeyByType(std::string(1, type), key, db);
    return 0;
  }
  return -1;
}

void RemSlotKeyByType(const std::string& type, const std::string& key, const std::shared_ptr<DB>& db) {
  uint32_t crc;
  int hastag;
  uint32_t slotNum = GetSlotsID(g_pika_conf->default_slot_num(), key, &crc, &hastag);

  std::string slot_key = GetSlotKey(slotNum);
  int32_t res = 0;

  std::vector<std::string> members;
  members.emplace_back(type + key);
  rocksdb::Status s = db->storage()->SRem(slot_key, members, &res);
  if (!s.ok()) {
    LOG(ERROR) << "srem key[" << key << "] from slotKey[" << slot_key << "] failed, error: " << s.ToString();
    return;
  }

  if (hastag) {
    std::string tag_key = GetSlotsTagKey(crc);
    s = db->storage()->SRem(tag_key, members, &res);
    if (!s.ok()) {
      LOG(ERROR) << "srem key[" << key << "] from tagKey[" << tag_key << "] failed, error: " << s.ToString();
      return;
    }
  }
}

/* *
 * do migrate mutli key-value(s) for {slotsmgrt/slotsmgrtone}with tag commands
 * return value:
 *    -1 - error happens
 *   >=0 - # of success migration
 * */
static int SlotsMgrtTag(const std::string& host, const int port, int timeout, const std::string& key, const char type,
                        std::string& detail, const std::shared_ptr<DB>& db) {
  int count = 0;
  uint32_t crc;
  int hastag;
  GetSlotsID(g_pika_conf->default_slot_num(), key, &crc, &hastag);
  if (!hastag) {
    if (type == 0) {
      return 0;
    }
    return SlotsMgrtOne(host, port, timeout, key, type, detail, db);
  }

  std::string tag_key = GetSlotsTagKey(crc);
  std::vector<std::string> members;

  // get all keys that have the same crc
  rocksdb::Status s = db->storage()->SMembers(tag_key, &members);
  if (!s.ok()) {
    return -1;
  }

  auto iter = members.begin();
  for (; iter != members.end(); iter++) {
    std::string key = *iter;
    char type = key.at(0);
    key.erase(key.begin());
    int ret = SlotsMgrtOne(host, port, timeout, key, type, detail, db);

    // the key is migrated to target
    if (ret == 1) {
      count++;
      continue;
    }

    if (ret == 0) {
      LOG(WARNING) << "slots migrate tag failed, key: " << key << ", detail: " << detail;
      continue;
    }

    return -1;
  }

  return count;
}

std::string GetSlotKey(uint32_t slot) {
  return SlotKeyPrefix + std::to_string(slot);
}

// add key to slotkey
void AddSlotKey(const std::string& type, const std::string& key, const std::shared_ptr<DB>& db) {
  if (g_pika_conf->slotmigrate() != true) {
    return;
  }

  rocksdb::Status s;
  int32_t res = -1;
  uint32_t crc;
  int hastag;
  uint32_t slotID = GetSlotsID(g_pika_conf->default_slot_num(), key, &crc, &hastag);
  std::string slot_key = GetSlotKey(slotID);
  std::vector<std::string> members;
  members.emplace_back(type + key);
  s = db->storage()->SAdd(slot_key, members, &res);
  if (!s.ok()) {
    LOG(ERROR) << "sadd key[" << key << "] to slotKey[" << slot_key << "] failed, error: " << s.ToString();
    return;
  }

  // if res == 0, indicate the key is existed; may return,
  // prevent write slot_key success, but write tag_key failed, so always write tag_key
  if (hastag) {
    std::string tag_key = GetSlotsTagKey(crc);
    s = db->storage()->SAdd(tag_key, members, &res);
    if (!s.ok()) {
      LOG(ERROR) << "sadd key[" << key << "] to tagKey[" << tag_key << "] failed, error: " << s.ToString();
      return;
    }
  }
}

// del key from slotkey
void RemSlotKey(const std::string& key, const std::shared_ptr<DB>& db) {
  if (g_pika_conf->slotmigrate() != true) {
    return;
  }
  std::string type;
  if (GetKeyType(key, type, db) < 0) {
    LOG(WARNING) << "SRem key: " << key << " from slotKey error";
    return;
  }
  std::string slotKey = GetSlotKey(GetSlotID(g_pika_conf->default_slot_num(), key));
  int32_t count = 0;
  std::vector<std::string> members(1, type + key);
  rocksdb::Status s = db->storage()->SRem(slotKey, members, &count);
  if (!s.ok()) {
    LOG(WARNING) << "SRem key: " << key << " from slotKey, error: " << s.ToString();
    return;
  }
}

int GetKeyType(const std::string& key, std::string& key_type, const std::shared_ptr<DB>& db) {
  enum storage::DataType type;
  rocksdb::Status s = db->storage()->GetType(key, type);
  if (!s.ok()) {
    LOG(WARNING) << "Get key type error: " << key << " " << s.ToString();
    key_type = "";
    return -1;
  }
  auto key_type_char = storage::DataTypeToTag(type);
  if (key_type_char == DataTypeToTag(storage::DataType::kNones)) {
    LOG(WARNING) << "Get key type error: " << key;
    key_type = "";
    return -1;
  }
  key_type = key_type_char;
  return 1;
}

// get slotstagkey by key
std::string GetSlotsTagKey(uint32_t crc) {
  return SlotTagPrefix + std::to_string(crc);
}

// delete key from db && cache
int DeleteKey(const std::string& key, const char key_type, const std::shared_ptr<DB>& db) {
  int32_t res = 0;
  std::string slotKey = GetSlotKey(GetSlotID(g_pika_conf->default_slot_num(), key));

  // delete slotkey
  std::vector<std::string> members;
  members.emplace_back(key_type + key);
  rocksdb::Status s = db->storage()->SRem(slotKey, members, &res);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      LOG(INFO) << "Del key Srem key " << key << " not found";
      return 0;
    } else {
      LOG(WARNING) << "Del key Srem key: " << key << " from slotKey, error: " << strerror(errno);
      return -1;
    }
  }

  // delete from cache
  if (PIKA_CACHE_NONE != g_pika_conf->cache_mode()
      && PIKA_CACHE_STATUS_OK == db->cache()->CacheStatus()) {
    db->cache()->Del(members);
  }

  // delete key from db
  members.clear();
  members.emplace_back(key);
  std::map<storage::DataType, storage::Status> type_status;
  int64_t del_nums = db->storage()->Del(members);
  if (0 > del_nums) {
    LOG(WARNING) << "Del key: " << key << " at slot " << GetSlotID(g_pika_conf->default_slot_num(), key) << " error";
    return -1;
  }

  return 1;
}

void SlotsMgrtTagSlotCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsMgrtTagSlot);
    return;
  }
  // Remember the first args is the opt name
  auto it = argv_.begin() + 1;
  dest_ip_ = *it++;
  pstd::StringToLower(dest_ip_);

  std::string str_dest_port = *it++;
  if (!pstd::string2int(str_dest_port.data(), str_dest_port.size(), &dest_port_)) {
    std::string detail = "invalid port number " + std::to_string(dest_port_);
    res_.SetRes(CmdRes::kErrOther, detail);
    return;
  }
  if (dest_port_ < 0 || dest_port_ > 65535) {
    std::string detail = "invalid port number " + std::to_string(dest_port_);
    res_.SetRes(CmdRes::kErrOther, detail);
    return;
  }

  if ((dest_ip_ == "127.0.0.1" || dest_ip_ == g_pika_server->host()) && dest_port_ == g_pika_server->port()) {
    res_.SetRes(CmdRes::kErrOther, "destination address error");
    return;
  }

  std::string str_timeout_ms = *it++;
  if (!pstd::string2int(str_timeout_ms.data(), str_timeout_ms.size(), &timeout_ms_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  if (timeout_ms_ < 0) {
    std::string detail = "invalid timeout number " + std::to_string(timeout_ms_);
    res_.SetRes(CmdRes::kErrOther, detail);
    return;
  }
  if (timeout_ms_ == 0) {
    timeout_ms_ = 100;
  }

  std::string str_slot_num = *it++;
  if (!pstd::string2int(str_slot_num.data(), str_slot_num.size(), &slot_id_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  if (slot_id_ < 0 || slot_id_ >= g_pika_conf->default_slot_num()) {
    std::string detail = "invalid slot number " + std::to_string(slot_id_);
    res_.SetRes(CmdRes::kErrOther, detail);
    return;
  }
}

void SlotsMgrtTagSlotCmd::Do() {
  if (g_pika_conf->slotmigrate() != true) {
    LOG(WARNING) << "Not in slotmigrate mode";
    res_.SetRes(CmdRes::kErrOther, "not set slotmigrate");
    return;
  }

  int32_t len = 0;
  int ret = 0;
  std::string detail;
  std::string slot_key = GetSlotKey(static_cast<int32_t>(slot_id_));

  // first, get the count of slot_key, prevent to sscan key very slowly when the key is not found
  rocksdb::Status s = db_->storage()->SCard(slot_key, &len);
  if (len < 0) {
    detail = "Get the len of slot Error";
  }
  // mutex between SlotsMgrtTagSlotCmd、SlotsMgrtTagOneCmd and migrator_thread
  if (len > 0 && g_pika_server->pika_migrate_->Trylock()) {
    g_pika_server->pika_migrate_->CleanMigrateClient();
    int64_t next_cursor = 0;
    std::vector<std::string> members;
    rocksdb::Status s = db_->storage()->SScan(slot_key, 0, "*", 1, &members, &next_cursor);
    if (s.ok()) {
      for (const auto &member : members) {
        std::string key = member;
        char type = key.at(0);
        key.erase(key.begin());
        ret = SlotsMgrtTag(dest_ip_, static_cast<int32_t>(dest_port_), static_cast<int32_t>(timeout_ms_), key, type, detail, db_);
      }
    }
    // unlock
    g_pika_server->pika_migrate_->Unlock();
  } else {
    LOG(WARNING) << "pika migrate is running, try again later, slot_id_: " << slot_id_;
  }
  if (ret == 0) {
    LOG(WARNING) << "slots migrate without tag failed, slot_id_: " << slot_id_ << ", detail: " << detail;
  }
  if (len >= 0 && ret >= 0) {
    res_.AppendArrayLen(2);
    // the number of keys migrated
    res_.AppendInteger(ret);
    // the number of keys remained
    res_.AppendInteger(len - ret);
  } else {
    res_.SetRes(CmdRes::kErrOther, detail);
  }

  return;
}

// check key type
int SlotsMgrtTagOneCmd::KeyTypeCheck(const std::shared_ptr<DB>& db) {
  enum storage::DataType type;
  std::string key_type;
  rocksdb::Status s = db->storage()->GetType(key_, type);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      LOG(WARNING) << "Migrate slot key " << key_ << " not found";
      res_.AppendInteger(0);
    } else {
      LOG(WARNING) << "Migrate slot key: " << key_ << " error: " << s.ToString();
      res_.SetRes(CmdRes::kErrOther, "migrate slot error");
    }
    return -1;
  }
  key_type_ = storage::DataTypeToTag(type);
  if (type == storage::DataType::kNones) {
    LOG(WARNING) << "Migrate slot key: " << key_ << " not found";
    res_.AppendInteger(0);
    return -1;
  }
  return 0;
}

void SlotsMgrtTagOneCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsMgrtTagSlot);
    return;
  }
  // Remember the first args is the opt name
  auto it = argv_.begin() + 1;
  dest_ip_ = *it++;
  pstd::StringToLower(dest_ip_);

  std::string str_dest_port = *it++;
  if (!pstd::string2int(str_dest_port.data(), str_dest_port.size(), &dest_port_)) {
    std::string detail = "invalid port number " + std::to_string(dest_port_);
    res_.SetRes(CmdRes::kErrOther, detail);
    return;
  }
  if (dest_port_ < 0 || dest_port_ > 65535) {
    std::string detail = "invalid port number " + std::to_string(dest_port_);
    res_.SetRes(CmdRes::kErrOther, detail);
    return;
  }

  if ((dest_ip_ == "127.0.0.1" || dest_ip_ == g_pika_server->host()) && dest_port_ == g_pika_server->port()) {
    res_.SetRes(CmdRes::kErrOther, "destination address error");
    return;
  }

  std::string str_timeout_ms = *it++;
  if (!pstd::string2int(str_timeout_ms.data(), str_timeout_ms.size(), &timeout_ms_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  if (timeout_ms_ < 0) {
    std::string detail = "invalid timeout number " + std::to_string(timeout_ms_);
    res_.SetRes(CmdRes::kErrOther, detail);
    return;
  }
  if (timeout_ms_ == 0) {
    timeout_ms_ = 100;
  }

  key_ = *it++;
}

void SlotsMgrtTagOneCmd::Do() {
  if (!g_pika_conf->slotmigrate()) {
    LOG(WARNING) << "Not in slotmigrate mode";
    res_.SetRes(CmdRes::kErrOther, "not set slotmigrate");
    return;
  }

  int64_t ret = 0;
  int32_t len = 0;
  int hastag = 0;
  uint32_t crc = 0;
  std::string detail;
  rocksdb::Status s;
  std::map<storage::DataType, rocksdb::Status> type_status;

  // if you need migrates key, if the key is not existed, return
  GetSlotsID(g_pika_conf->default_slot_num(), key_, &crc, &hastag);
  if (!hastag) {
    std::vector<std::string> keys;
    keys.emplace_back(key_);

    // check the key is not existed
    ret = db_->storage()->Exists(keys);

    // when the key is not existed, ret = 0
    if (ret == -1) {
      res_.SetRes(CmdRes::kErrOther, "exists internal error");
      return;
    }

    if (ret == 0) {
      res_.AppendInteger(0);
      return;
    }

    // else need to migrate
  } else {
    // key is tag_key, check the number of the tag_key
    std::string tag_key = GetSlotsTagKey(crc);
    s = db_->storage()->SCard(tag_key, &len);
    if (s.IsNotFound()) {
      res_.AppendInteger(0);
      return;
    }
    if (!s.ok() || len == -1) {
      res_.SetRes(CmdRes::kErrOther, "can't get the number of tag_key");
      return;
    }

    if (len == 0) {
      res_.AppendInteger(0);
      return;
    }

    // else need to migrate
  }

  // lock batch migrate, dont do slotsmgrttagslot when do slotsmgrttagone
  // pika_server thread exit(~PikaMigrate) and dispatch thread do CronHandle nead lock()
  g_pika_server->pika_migrate_->Lock();

  // if the key is not existed, return
  if (!hastag) {
    std::vector<std::string> keys;
    keys.emplace_back(key_);
    // the key may be deleted by another thread
    std::map<storage::DataType, rocksdb::Status> type_status;
    ret = db_->storage()->Exists(keys);

    // when the key is not existed, ret = 0
    if (ret == -1) {
      detail = s.ToString();
    } else if (KeyTypeCheck(db_) != 0) {
      detail = "cont get the key type.";
      ret = -1;
    } else {
      ret = SlotsMgrtTag(dest_ip_, static_cast<int32_t>(dest_port_), static_cast<int32_t>(timeout_ms_), key_, key_type_, detail, db_);
    }
  } else {
    // key maybe doesn't exist, the key is tag key, migrate the same tag key
    ret = SlotsMgrtTag(dest_ip_, static_cast<int32_t>(dest_port_), static_cast<int32_t>(timeout_ms_), key_, 0, detail, db_);
  }

  // unlock the record lock
  g_pika_server->pika_migrate_->Unlock();

  if (ret >= 0) {
    res_.AppendInteger(ret);
  } else {
    if (detail.size() == 0) {
      detail = "Unknown Error";
    }
    res_.SetRes(CmdRes::kErrOther, detail);
  }

  return;
}

/* *
 * slotsinfo [start] [count]
 * */
void SlotsInfoCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsInfo);
    return;
  }

  if (argv_.size() >= 2) {
    if (!pstd::string2int(argv_[1].data(), argv_[1].size(), &begin_)) {
      res_.SetRes(CmdRes::kInvalidInt);
      return;
    }

    if (begin_ < 0 || begin_ >= end_) {
      std::string detail = "invalid slot begin = " + argv_[1];
      res_.SetRes(CmdRes::kErrOther, detail);
      return;
    }
  }

  if (argv_.size() >= 3) {
    int64_t count = 0;
    if (!pstd::string2int(argv_[2].data(), argv_[2].size(), &count)) {
      res_.SetRes(CmdRes::kInvalidInt);
      return;
    }

    if (count < 0) {
      std::string detail = "invalid slot count = " + argv_[2];
      res_.SetRes(CmdRes::kErrOther, detail);
      return;
    }

    if (begin_ + count < end_) {
      end_ = begin_ + count;
    }
  }

  if (argv_.size() >= 4) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsInfo);
    return;
  }
}

void SlotsInfoCmd::Do() {
  int slotNum = g_pika_conf->default_slot_num();
  int slots_slot[slotNum];
  int slots_size[slotNum];
  memset(slots_slot, 0, slotNum);
  memset(slots_size, 0, slotNum);
  int n = 0;
  int32_t len = 0;
  std::string slot_key;

  for (auto i = static_cast<int32_t>(begin_); i < end_; i++) {
    slot_key = GetSlotKey(i);
    len = 0;
    rocksdb::Status s = db_->storage()->SCard(slot_key, &len);
    if (!s.ok() || len == 0) {
      continue;
    }

    slots_slot[n] = i;
    slots_size[n] = len;
    n++;
  }

  res_.AppendArrayLen(n);
  for (int i = 0; i < n; i++) {
    res_.AppendArrayLen(2);
    res_.AppendInteger(slots_slot[i]);
    res_.AppendInteger(slots_size[i]);
  }

  return;
}

void SlotsMgrtTagSlotAsyncCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsMgrtTagSlotAsync);
  }
  // Remember the first args is the opt name
  auto it = argv_.begin() + 1;
  dest_ip_ = *it++;
  pstd::StringToLower(dest_ip_);

  std::string str_dest_port = *it++;
  if (!pstd::string2int(str_dest_port.data(), str_dest_port.size(), &dest_port_) || dest_port_ <= 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }

  if ((dest_ip_ == "127.0.0.1" || dest_ip_ == g_pika_server->host()) && dest_port_ == g_pika_server->port()) {
    res_.SetRes(CmdRes::kErrOther, "destination address error");
    return;
  }

  std::string str_timeout_ms = *it++;
  if (!pstd::string2int(str_timeout_ms.data(), str_timeout_ms.size(), &timeout_ms_) || timeout_ms_ <= 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }

  std::string str_max_bulks = *it++;
  if (!pstd::string2int(str_max_bulks.data(), str_max_bulks.size(), &max_bulks_) || max_bulks_ <= 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }

  std::string str_max_bytes_ = *it++;
  if (!pstd::string2int(str_max_bytes_.data(), str_max_bytes_.size(), &max_bytes_) || max_bytes_ <= 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }

  std::string str_slot_num = *it++;
  if (!pstd::string2int(str_slot_num.data(), str_slot_num.size(), &slot_id_) || slot_id_ < 0 ||
      slot_id_ >= g_pika_conf->default_slot_num()) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }

  std::string str_keys_num = *it++;
  if (!pstd::string2int(str_keys_num.data(), str_keys_num.size(), &keys_num_) || keys_num_ < 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  return;
}

void SlotsMgrtTagSlotAsyncCmd::Do() {
  // check whether open slotmigrate
  if (!g_pika_conf->slotmigrate()) {
    res_.SetRes(CmdRes::kErrOther, "please open slotmigrate and reload slot");
    return;
  }

  int32_t remained = 0;
  std::string slotKey = GetSlotKey(static_cast<int32_t>(slot_id_));
  storage::Status status = db_->storage()->SCard(slotKey, &remained);
  if (status.IsNotFound()) {
    LOG(INFO) << "find no record in slot " << slot_id_;
    res_.AppendArrayLen(2);
    res_.AppendInteger(0);
    res_.AppendInteger(remained);
    return;
  }
  if (!status.ok()) {
    LOG(WARNING) << "Slot batch migrate keys get result error";
    res_.SetRes(CmdRes::kErrOther, "Slot batch migrating keys get result error");
    return;
  }

  bool ret = g_pika_server->SlotsMigrateBatch(dest_ip_, dest_port_, timeout_ms_, slot_id_, keys_num_, db_);
  if (!ret) {
    LOG(WARNING) << "Slot batch migrate keys error";
    res_.SetRes(CmdRes::kErrOther, "Slot batch migrating keys error, may be currently migrating");
    return;
  }

  res_.AppendArrayLen(2);
  res_.AppendInteger(0);
  res_.AppendInteger(remained);
  return;
}

void SlotsMgrtAsyncStatusCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsMgrtAsyncStatus);
  }
  return;
}

void SlotsMgrtAsyncStatusCmd::Do() {
  std::string status;
  std::string ip;
  int64_t port = -1, slots = -1, moved = -1, remained = -1;
  bool migrating = false;
  g_pika_server->GetSlotsMgrtSenderStatus(&ip, &port, &slots, &migrating, &moved, &remained);
  std::string mstatus = migrating ? "yes" : "no";
  res_.AppendArrayLen(5);
  status = "dest server: " + ip + ":" + std::to_string(port);
  res_.AppendStringLenUint64(status.size());
  res_.AppendContent(status);
  status = "slot number: " + std::to_string(slots);
  res_.AppendStringLenUint64(status.size());
  res_.AppendContent(status);
  status = "migrating  : " + mstatus;
  res_.AppendStringLenUint64(status.size());
  res_.AppendContent(status);
  status = "moved keys : " + std::to_string(moved);
  res_.AppendStringLenUint64(status.size());
  res_.AppendContent(status);
  status = "remain keys: " + std::to_string(remained);
  res_.AppendStringLenUint64(status.size());
  res_.AppendContent(status);

  return;
}

void SlotsMgrtAsyncCancelCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsMgrtAsyncCancel);
  }
  return;
}

void SlotsMgrtAsyncCancelCmd::Do() {
  bool ret = g_pika_server->SlotsMigrateAsyncCancel();
  if (!ret) {
    res_.SetRes(CmdRes::kErrOther, "slotsmgrt-async-cancel error");
  }
  res_.SetRes(CmdRes::kOk);
  return;
}

void SlotsDelCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsDel);
  }
  slots_.assign(argv_.begin(), argv_.end());
  return;
}

void SlotsDelCmd::Do() {
  std::vector<std::string> keys;
  std::vector<std::string>::const_iterator iter;
  for (iter = slots_.begin(); iter != slots_.end(); iter++) {
    keys.emplace_back(SlotKeyPrefix + *iter);
  }
  std::map<storage::DataType, rocksdb::Status> type_status;
  int64_t count = db_->storage()->Del(keys);
  if (count >= 0) {
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, "SlotsDel error");
  }
  return;
}

/* *
 * slotshashkey [key1 key2...]
 * */
void SlotsHashKeyCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsHashKey);
    return;
  }

  auto iter = argv_.begin();
  keys_.assign(++iter, argv_.end());
  return;
}

void SlotsHashKeyCmd::Do() {
  std::vector<std::string>::const_iterator keys_it;

  res_.AppendArrayLenUint64(keys_.size());
  for (keys_it = keys_.begin(); keys_it != keys_.end(); ++keys_it) {
    res_.AppendInteger(GetSlotsID(g_pika_conf->default_slot_num(), *keys_it, nullptr, nullptr));
  }

  return;
}

void SlotsScanCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsScan);
    return;
  }
  key_ = SlotKeyPrefix + argv_[1];
  if (std::stoll(argv_[1].data()) < 0 || std::stoll(argv_[1].data()) >= g_pika_conf->default_slot_num()) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsScan);
    return;
  }
  if (!pstd::string2int(argv_[2].data(), argv_[2].size(), &cursor_)) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsScan);
    return;
  }
  size_t argc = argv_.size(), index = 3;
  while (index < argc) {
    std::string opt = argv_[index];
    if (!strcasecmp(opt.data(), "match") || !strcasecmp(opt.data(), "count")) {
      index++;
      if (index >= argc) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      if (!strcasecmp(opt.data(), "match")) {
        pattern_ = argv_[index];
      } else if (!pstd::string2int(argv_[index].data(), argv_[index].size(), &count_)) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
      }
    } else {
      res_.SetRes(CmdRes::kSyntaxErr);
      return;
    }
    index++;
  }
  if (count_ < 0) {
    res_.SetRes(CmdRes::kSyntaxErr);
    return;
  }
  return;
}

void SlotsScanCmd::Do() {
  std::vector<std::string> members;
  rocksdb::Status s = db_->storage()->SScan(key_, cursor_, pattern_, count_, &members, &cursor_);

  if (members.size() <= 0) {
    cursor_ = 0;
  }
  res_.AppendContent("*2");

  char buf[32];
  int64_t len = pstd::ll2string(buf, sizeof(buf), cursor_);
  res_.AppendStringLen(len);
  res_.AppendContent(buf);

  res_.AppendArrayLenUint64(members.size());
  auto iter_member = members.begin();
  for (; iter_member != members.end(); iter_member++) {
    res_.AppendStringLenUint64(iter_member->size());
    res_.AppendContent(*iter_member);
  }
  return;
}

void SlotsMgrtExecWrapperCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsMgrtExecWrapper);
  }
  auto it = argv_.begin() + 1;
  key_ = *it++;
  pstd::StringToLower(key_);
  return;
}

// return 0 means key doesn't exist, or key is not migrating
// return 1 means key is migrating
// return -1 means something wrong
void SlotsMgrtExecWrapperCmd::Do() {
  res_.AppendArrayLen(2);
  int ret = g_pika_server->SlotsMigrateOne(key_, db_);
  switch (ret) {
    case 0:
    case -2:
      res_.AppendInteger(0);
      res_.AppendInteger(0);
      return;
    case 1:
      res_.AppendInteger(1);
      res_.AppendInteger(1);
      return;
    default:
      res_.AppendInteger(-1);
      res_.AppendInteger(-1);
      return;
  }
  return;
}

void SlotsReloadCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsReload);
  }
  return;
}

void SlotsReloadCmd::Do() {
  g_pika_server->Bgslotsreload(db_);
  const PikaServer::BGSlotsReload &info = g_pika_server->bgslots_reload();
  char buf[256];
  snprintf(buf, sizeof(buf), "+%s : %lld", info.s_start_time.c_str(), g_pika_server->GetSlotsreloadingCursor());
  res_.AppendContent(buf);
  return;
}

void SlotsReloadOffCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsReloadOff);
  }
  return;
}

void SlotsReloadOffCmd::Do() {
  g_pika_server->SetSlotsreloading(false);
  res_.SetRes(CmdRes::kOk);
  return;
}

void SlotsCleanupCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsCleanup);
  }

  auto iter = argv_.begin() + 1;
  std::string slot;
  long slotLong = 0;
  std::vector<int> slots;
  for (; iter != argv_.end(); iter++) {
    slot = *iter;
    if (!pstd::string2int(slot.data(), slot.size(), &slotLong) || slotLong < 0) {
      res_.SetRes(CmdRes::kInvalidInt);
      return;
    }
    slots.emplace_back(static_cast<int32_t>(slotLong));
  }
  cleanup_slots_.swap(slots);
  return;
}

void SlotsCleanupCmd::Do() {
  g_pika_server->Bgslotscleanup(cleanup_slots_, db_);
  std::vector<int> cleanup_slots(g_pika_server->GetCleanupSlots());
  res_.AppendArrayLenUint64(cleanup_slots.size());
  auto iter = cleanup_slots.begin();
  for (; iter != cleanup_slots.end(); iter++) {
    res_.AppendInteger(*iter);
  }
  return;
}

void SlotsCleanupOffCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsCleanupOff);
  }
  return;
}

void SlotsCleanupOffCmd::Do() {
  g_pika_server->StopBgslotscleanup();
  res_.SetRes(CmdRes::kOk);
  return;
}
