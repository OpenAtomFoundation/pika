// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_slot_command.h"
#include <algorithm>
#include <vector>
#include <string>
#include "include/pika_conf.h"
#include "include/pika_data_distribution.h"
#include "include/pika_server.h"
#include "pstd/include/pstd_status.h"
#include "pstd/include/pstd_string.h"
#include "storage/include/storage/storage.h"
#include "include/pika_migrate_thread.h"

#define min(a, b) (((a) > (b)) ? (b) : (a))
#define MAX_MEMBERS_NUM 512

extern std::unique_ptr<PikaServer> g_pika_server;
extern std::unique_ptr<PikaConf> g_pika_conf;

uint32_t crc32tab[256];
void CRC32TableInit(uint32_t poly) {
  int i, j;
  for (i = 0; i < 256; i++) {
    uint32_t crc = i;
    for (j = 0; j < 8; j++) {
      if (crc & 1) {
        crc = (crc >> 1) ^ poly;
      } else {
        crc = (crc >> 1);
      }
    }
    crc32tab[i] = crc;
  }
}

void InitCRC32Table() {
  CRC32TableInit(IEEE_POLY);
}

uint32_t CRC32Update(uint32_t crc, const char *buf, int len) {
  int i;
  crc = ~crc;
  for (i = 0; i < len; i++) {
    crc = crc32tab[(uint8_t)((char)crc ^ buf[i])] ^ (crc >> 8);
  }
  return ~crc;
}

PikaMigrate::PikaMigrate() { migrate_clients_.clear(); }

PikaMigrate::~PikaMigrate() {
  // close and release all client
  // get the mutex lock
  std::lock_guard lm(mutex_);
  KillAllMigrateClient();
}

net::NetCli *PikaMigrate::GetMigrateClient(const std::string &host, const int port, int timeout) {
  std::string ip_port = host + ":" + std::to_string(port);
  net::NetCli *migrate_cli;
  pstd::Status s;

  std::map<std::string, void *>::iterator migrate_clients_iter = migrate_clients_.find(ip_port);
  if (migrate_clients_iter == migrate_clients_.end()) {
    migrate_cli = net::NewRedisCli();
    s = migrate_cli->Connect(host, port, g_pika_server->host());
    if (!s.ok()) {
      LOG(ERROR) << "GetMigrateClient: new  migrate_cli[" << ip_port.c_str() << "] failed";

      delete migrate_cli;
      return NULL;
    }

    LOG(INFO) << "GetMigrateClient: new  migrate_cli[" << ip_port.c_str() << "]";

    std::string userpass = g_pika_conf->userpass();
    if (userpass != "") {
      net::RedisCmdArgsType argv;
      std::string wbuf_str;
      argv.push_back("auth");
      argv.push_back(userpass);
      net::SerializeRedisCommand(argv, &wbuf_str);

      s = migrate_cli->Send(&wbuf_str);
      if (!s.ok()) {
        LOG(ERROR) << "GetMigrateClient: new  migrate_cli Send, error: " << s.ToString();
        delete migrate_cli;
        return NULL;
      }

      s = migrate_cli->Recv(&argv);
      if (!s.ok()) {
        LOG(ERROR) << "GetMigrateClient: new  migrate_cli Recv, error: " << s.ToString();
        delete migrate_cli;
        return NULL;
      }

      if (strcasecmp(argv[0].data(), kInnerReplOk.data())) {
        LOG(ERROR) << "GetMigrateClient: new  migrate_cli auth error";
        delete migrate_cli;
        return NULL;
      }
    }

    // add new migrate client to the map
    migrate_clients_[ip_port] = migrate_cli;
  } else {
    // LOG(INFO) << "GetMigrateClient: find  migrate_cli[" << ip_port.c_str() << "]";
    migrate_cli = static_cast<net::NetCli *>(migrate_clients_iter->second);
  }

  // set the client connect timeout
  migrate_cli->set_send_timeout(timeout);
  migrate_cli->set_recv_timeout(timeout);

  // modify the client last time
  //  migrate_cli->
  gettimeofday(&migrate_cli->last_interaction_, NULL);

  return migrate_cli;
}

void PikaMigrate::KillMigrateClient(net::NetCli *migrate_cli) {
  std::map<std::string, void *>::iterator migrate_clients_iter = migrate_clients_.begin();
  while (migrate_clients_iter != migrate_clients_.end()) {
    if (migrate_cli == static_cast<net::NetCli *>(migrate_clients_iter->second)) {
      LOG(INFO) << "KillMigrateClient: kill  migrate_cli[" << migrate_clients_iter->first.c_str() << "]";

      migrate_cli->Close();
      delete migrate_cli;
      migrate_cli = NULL;

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

  gettimeofday(&now, NULL);
  std::map<std::string, void *>::iterator migrate_clients_iter = migrate_clients_.begin();
  while (migrate_clients_iter != migrate_clients_.end()) {
    net::NetCli *migrate_cli = static_cast<net::NetCli *>(migrate_clients_iter->second);
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
  std::map<std::string, void *>::iterator migrate_clients_iter = migrate_clients_.begin();
  while (migrate_clients_iter != migrate_clients_.end()) {
    net::NetCli *migrate_cli = static_cast<net::NetCli *>(migrate_clients_iter->second);

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
int PikaMigrate::MigrateKey(const std::string &host, const int port, int db, int timeout, const std::string &key,
                            const char type, std::string &detail, std::shared_ptr<Slot> slot) {
  int send_command_num = -1;

  net::NetCli *migrate_cli = GetMigrateClient(host, port, timeout);
  if (NULL == migrate_cli) {
    detail = "GetMigrateClient failed";
    LOG(INFO) << "GetMigrateClient failed, key: " << key;
    return -1;
  }

  send_command_num = MigrateSend(migrate_cli, key, type, detail, slot);
  if (send_command_num <= 0) {
    return send_command_num;
  }

  if (MigrateRecv(migrate_cli, send_command_num, detail)) {
    return send_command_num;
  }

  return -1;
}

int PikaMigrate::MigrateSend(net::NetCli *migrate_cli, const std::string &key, const char type, std::string &detail,
                             std::shared_ptr<Slot> slot) {
  std::string wbuf_str;
  pstd::Status s;
  int command_num = -1;

  // chech the client is alive
  if (NULL == migrate_cli) {
    return -1;
  }

  command_num = ParseKey(key, type, wbuf_str, slot);
  if (command_num < 0) {
    detail = "ParseKey failed";
    return command_num;
  }

  // dont need seed data, key is not exist
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

bool PikaMigrate::MigrateRecv(net::NetCli *migrate_cli, int need_receive, std::string &detail) {
  pstd::Status s;
  std::string reply;
  int64_t ret;

  if (NULL == migrate_cli || need_receive < 0) {
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
    if (argv.size() == 1 &&
        (kInnerReplOk == pstd::StringToLower(reply) || pstd::string2int(reply.data(), reply.size(), &ret))) {
      // success

      // continue reiceve response
      if (need_receive > 0) {
        continue;
      }

      // has got all response
      break;
    }

    // failed
    detail = "something wrong with slots migrate, reply: " + reply;
    LOG(ERROR) << "something wrong with slots migrate, reply:" << reply;
    return false;
  }

  return true;
}

// return -1 is error; 0 dont migrate; >0 the number of commond
int PikaMigrate::ParseKey(const std::string &key, const char type, std::string &wbuf_str, std::shared_ptr<Slot> slot) {
  int command_num = -1;
  int64_t ttl = 0;
  rocksdb::Status s;

  switch (type) {
    case 'k':
      command_num = ParseKKey(key, wbuf_str, slot);
      break;
    case 'h':
      command_num = ParseHKey(key, wbuf_str, slot);
      break;
    case 'l':
      command_num = ParseLKey(key, wbuf_str, slot);
      break;
    case 'z':
      command_num = ParseZKey(key, wbuf_str, slot);
      break;
    case 's':
      command_num = ParseSKey(key, wbuf_str, slot);
      break;
    default:
      LOG(INFO) << "ParseKey key[" << key << "], the type[" << type << "] is not support.";
      return -1;
      break;
  }

  // error or key is not exist
  if (command_num <= 0) {
    LOG(INFO) << "ParseKey key[" << key << "], parse return " << command_num
              << ", the key maybe is not exist or expired.";
    return command_num;
  }

  // skip kv, because kv cmd: SET key value ttl
  if (type == 'k') {
    return command_num;
  }

  ttl = TTLByType(type, key, slot);

  //-1 indicates the key is valid forever
  if (ttl == -1) {
    return command_num;
  }

  // key is expired or not exist, dont migrate
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

bool PikaMigrate::SetTTL(const std::string &key, std::string &wbuf_str, int64_t ttl) {
  //-1 indicates the key is valid forever
  if (ttl == -1) {
    return false;
  }

  // if ttl = -2 indicates the key is not exist
  if (ttl < 0) {
    LOG(INFO) << "SetTTL key[" << key << "], ttl is " << ttl;
    ttl = 0;
  }

  net::RedisCmdArgsType argv;
  std::string cmd;

  argv.push_back("EXPIRE");
  argv.push_back(key);
  argv.push_back(std::to_string(ttl));

  net::SerializeRedisCommand(argv, &cmd);
  wbuf_str.append(cmd);

  return true;
}

// return -1 is error; 0 dont migrate; >0 the number of commond
int PikaMigrate::ParseKKey(const std::string &key, std::string &wbuf_str, std::shared_ptr<Slot> slot) {
  net::RedisCmdArgsType argv;
  std::string cmd;
  std::string value;
  int64_t ttl = 0;
  rocksdb::Status s;

  s = slot->db()->Get(key, &value);

  // key is not exist, dont migrate
  if (s.IsNotFound()) {
    return 0;
  }

  if (!s.ok()) {
    return -1;
  }

  argv.push_back("SET");
  argv.push_back(key);
  argv.push_back(value);

  ttl = TTLByType('k', key, slot);

  // ttl = -1 indicates the key is valid forever, dont process
  // key is expired or not exist, dont migrate
  // todo check ttl
  if (ttl == 0 || ttl == -2) {
    wbuf_str.clear();
    return 0;
  }

  if (ttl > 0) {
    argv.push_back("EX");
    argv.push_back(std::to_string(ttl));
  }
  net::SerializeRedisCommand(argv, &cmd);
  wbuf_str.append(cmd);
  return 1;
}

int64_t PikaMigrate::TTLByType(const char key_type, const std::string &key, std::shared_ptr<Slot> slot) {
  std::map<storage::DataType, int64_t> type_timestamp;
  std::map<storage::DataType, rocksdb::Status> type_status;
  type_timestamp = slot->db()->TTL(key, &type_status);

  switch (key_type) {
    case 'k': {
      return type_timestamp[storage::kStrings];
    } break;
    case 'h': {
      return type_timestamp[storage::kHashes];
    } break;
    case 'z': {
      return type_timestamp[storage::kZSets];
    } break;
    case 's': {
      return type_timestamp[storage::kSets];
    } break;
    case 'l': {
      return type_timestamp[storage::kLists];
    } break;
    default:
      return -3;
  }
}

int PikaMigrate::ParseZKey(const std::string &key, std::string &wbuf_str, std::shared_ptr<Slot> slot) {
  int command_num = 0;

  int64_t next_cursor = 0;
  std::vector<storage::ScoreMember> score_members;
  do {
    score_members.clear();
    rocksdb::Status s = slot->db()->ZScan(key, next_cursor, "*", MAX_MEMBERS_NUM, &score_members, &next_cursor);
    if (s.ok()) {
      if (score_members.empty()) {
        break;
      }

      net::RedisCmdArgsType argv;
      std::string cmd;
      argv.push_back("ZADD");
      argv.push_back(key);

      for (const auto &score_member : score_members) {
        argv.push_back(std::to_string(score_member.score));
        argv.push_back(score_member.member);
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

// return -1 is error; 0 dont migrate; >0 the number of commond
int PikaMigrate::ParseHKey(const std::string &key, std::string &wbuf_str, std::shared_ptr<Slot> slot) {
  int64_t next_cursor = 0;
  int command_num = 0;
  std::vector<storage::FieldValue> field_values;
  do {
    field_values.clear();
    rocksdb::Status s = slot->db()->HScan(key, next_cursor, "*", MAX_MEMBERS_NUM, &field_values, &next_cursor);
    if (s.ok()) {
      if (field_values.empty()) {
        break;
      }

      net::RedisCmdArgsType argv;
      std::string cmd;
      argv.push_back("HMSET");
      argv.push_back(key);

      for (const auto &field_value : field_values) {
        argv.push_back(field_value.field);
        argv.push_back(field_value.value);
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

// return -1 is error; 0 dont migrate; >0 the number of commond
int PikaMigrate::ParseSKey(const std::string &key, std::string &wbuf_str, std::shared_ptr<Slot> slot) {
  int command_num = 0;
  int64_t next_cursor = 0;
  std::vector<std::string> members;

  do {
    members.clear();
    rocksdb::Status s = slot->db()->SScan(key, next_cursor, "*", MAX_MEMBERS_NUM, &members, &next_cursor);

    if (s.ok()) {
      if (members.empty()) {
        break;
      }

      net::RedisCmdArgsType argv;
      std::string cmd;
      argv.push_back("SADD");
      argv.push_back(key);

      for (const auto &member : members) {
        argv.push_back(member);
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

// return -1 is error; 0 dont migrate; >0 the number of commond
int PikaMigrate::ParseLKey(const std::string &key, std::string &wbuf_str, std::shared_ptr<Slot> slot) {
  int64_t left = 0;
  int command_num = 0;
  std::vector<std::string> values;

  net::RedisCmdArgsType argv;
  std::string cmd;

  // del old key, before migrate list; prevent redo when failed
  argv.push_back("DEL");
  argv.push_back(key);
  net::SerializeRedisCommand(argv, &cmd);
  wbuf_str.append(cmd);
  command_num++;

  do {
    values.clear();
    rocksdb::Status s = slot->db()->LRange(key, left, left + (MAX_MEMBERS_NUM - 1), &values);
    if (s.ok()) {
      if (values.empty()) {
        break;
      }

      net::RedisCmdArgsType argv;
      std::string cmd;

      argv.push_back("RPUSH");
      argv.push_back(key);

      for (const auto &value : values) {
        argv.push_back(value);
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
static int SlotsMgrtOne(const std::string &host, const int port, int timeout, const std::string &key, const char type,
                        std::string &detail, std::shared_ptr<Slot> slot) {
  int send_command_num = 0;
  rocksdb::Status s;
  std::map<storage::DataType, rocksdb::Status> type_status;

  send_command_num = g_pika_server->pika_migrate_->MigrateKey(host, port, 0, timeout, key, type, detail, slot);

  // the key is migrated to target, delete key and slotsinfo
  if (send_command_num >= 1) {
    LOG(INFO) << "【send command success】Migrate key: " << key << " success, host: " << host << ", port: " << port;
    std::vector<std::string> keys;
    keys.push_back(key);
    int64_t count = slot->db()->Del(keys, &type_status);
    if (count > 0) {
      // todo add bin log
      //      WriteDelKeyToBinlog(key);
    }

    // del slots info
    SlotKeyRemByType(std::string(1, type), key, slot);
    return 1;
  }

  // key is not exist, only del slotsinfo
  if (send_command_num == 0) {
    // del slots info
    SlotKeyRemByType(std::string(1, type), key, slot);
    return 0;
  }
  return -1;
}

void SlotKeyRemByType(const std::string &type, const std::string &key, std::shared_ptr<Slot> slot) {
  uint32_t crc;
  int hastag;
  int slotNum = GetSlotsID(key, &crc, &hastag);

  std::string slot_key = GetSlotKey(slotNum);
  int32_t res = 0;

  std::vector<std::string> members;
  members.push_back(type + key);
  rocksdb::Status s = slot->db()->SRem(slot_key, members, &res);
  if (!s.ok()) {
    LOG(ERROR) << "srem key[" << key << "] from slotKey[" << slot_key << "] failed, error: " << s.ToString();
    return;
  }

  if (hastag) {
    std::string tag_key = GetSlotsTagKey(crc);
    s = slot->db()->SRem(tag_key, members, &res);
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
static int SlotsMgrtTag(const std::string &host, const int port, int timeout, const std::string &key, const char type, std::string &detail, std::shared_ptr<Slot> slot) {
  int count = 0;
  uint32_t crc;
  int hastag;
  GetSlotsID(key, &crc, &hastag);
  if (!hastag) {
    if (type == 0) {
      return 0;
    }
    int ret = SlotsMgrtOne(host, port, timeout, key, type, detail, slot);
    if (ret == 0) {
      LOG(INFO) << "slots migrate without tag failed, key: " << key << ", detail: " << detail;
    }
    return ret;
  }

  std::string tag_key = GetSlotsTagKey(crc);
  std::vector<std::string> members;

  //get all key that has the same crc
  rocksdb::Status s = slot->db()->SMembers(tag_key, &members);
  if (!s.ok()) {
    return -1;
  }

  std::vector<std::string>::const_iterator iter = members.begin();
  for (; iter != members.end(); iter++) {
    std::string key = *iter;
    char type = key.at(0);
    key.erase(key.begin());
    int ret = SlotsMgrtOne(host, port, timeout, key, type, detail, slot);

    //the key is migrated to target
    if (ret == 1){
      count++;
      continue;
    }

    if (ret == 0){
      LOG(WARNING) << "slots migrate tag failed, key: " << key << ", detail: " << detail;
      continue;
    }

    return -1;
  }

  return count;
}


// get slot tag
static const char *GetSlotsTag(const std::string &str, int *plen) {
  const char *s = str.data();
  int i, j, n = str.length();
  for (i = 0; i < n && s[i] != '{'; i++) {
  }
  if (i == n) {
    return NULL;
  }
  i++;
  for (j = i; j < n && s[j] != '}'; j++) {
  }
  if (j == n) {
    return NULL;
  }
  if (plen != NULL) {
    *plen = j - i;
  }
  return s + i;
}

std::string GetSlotKey(int slot){
  return SlotKeyPrefix + std::to_string(slot);
}

// get key slot number
int GetSlotID(const std::string &str) { return GetSlotsID(str, NULL, NULL); }

// get the slot number by key
int GetSlotsID(const std::string &str, uint32_t *pcrc, int *phastag) {
  const char *s = str.data();
  int taglen;
  int hastag = 0;
  const char *tag = GetSlotsTag(str, &taglen);
  if (tag == NULL) {
    tag = s, taglen = str.length();
  } else {
    hastag = 1;
  }
  uint32_t crc = CRC32CheckSum(tag, taglen);
  if (pcrc != NULL) {
    *pcrc = crc;
  }
  if (phastag != NULL) {
    *phastag = hastag;
  }
  return (int)(crc & HASH_SLOTS_MASK);
}

uint32_t CRC32CheckSum(const char *buf, int len) { return CRC32Update(0, buf, len); }

// add key to slotkey
void AddSlotKey(const std::string type, const std::string key, std::shared_ptr<Slot> slot) {
  if (g_pika_conf->slotmigrate() != true) {
    return;
  }

  rocksdb::Status s;
  int32_t res = -1;
  uint32_t crc;
  int hastag;
  int slotID = GetSlotsID(key, &crc, &hastag);
  std::string slot_key = GetSlotKey(slotID);
  std::vector<std::string> members;
  members.push_back(type + key);
  s = slot->db()->SAdd(slot_key, members, &res);
  if (!s.ok()) {
    LOG(ERROR) << "sadd key[" << key << "] to slotKey[" << slot_key << "] failed, error: " << s.ToString();
    return;
  }

  // if res == 0, indicate the key is exist; may return,
  // prevent write slot_key success, but write tag_key failed, so always write tag_key
  if (hastag) {
    std::string tag_key = GetSlotsTagKey(crc);
    s = slot->db()->SAdd(tag_key, members, &res);
    if (!s.ok()) {
      LOG(ERROR) << "sadd key[" << key << "] to tagKey[" << tag_key << "] failed, error: " << s.ToString();
      return;
    }
  }
}

// check key exists
void RemKeyNotExists(const std::string type, const std::string key, std::shared_ptr<Slot> slot) {
  if (g_pika_conf->slotmigrate() != true) {
    return;
  }
  std::vector<std::string> vkeys;
  vkeys.push_back(key);
  std::map<storage::DataType, rocksdb::Status> type_status;
  int64_t res = slot->db()->Exists(vkeys, &type_status);
  if (res == 0) {
    std::string slotKey = GetSlotKey(GetSlotID(key));
    std::vector<std::string> members(1, type + key);
    int32_t count = 0;
    rocksdb::Status s = slot->db()->SRem(slotKey, members, &count);
    if (!s.ok()) {
      LOG(WARNING) << "Zrem key: " << key << " from slotKey, error: " << s.ToString();
      return;
    }
  }
  return;
}

// del key from slotkey
void RemSlotKey(const std::string key, std::shared_ptr<Slot> slot) {
  if (g_pika_conf->slotmigrate() != true) {
    return;
  }
  std::string type;
  if (GetKeyType(key, type, slot) < 0) {
    LOG(WARNING) << "SRem key: " << key << " from slotKey error";
    return;
  }
  std::string slotKey = GetSlotKey(GetSlotID(key));
  int32_t count = 0;
  std::vector<std::string> members(1, type + key);
  rocksdb::Status s = slot->db()->SRem(slotKey, members, &count);
  if (!s.ok()) {
    LOG(WARNING) << "SRem key: " << key << " from slotKey, error: " << s.ToString();
    return;
  }
}

int GetKeyType(const std::string key, std::string &key_type, std::shared_ptr<Slot> slot) {
  std::string type_str;
  rocksdb::Status s = slot->db()->Type(key, &type_str);
  if (!s.ok()) {
    LOG(WARNING) << "Get key type error: " << key << " " << s.ToString();
    key_type = "";
    return -1;
  }
  if (type_str == "string") {
    key_type = "k";
  } else if (type_str == "hash") {
    key_type = "h";
  } else if (type_str == "list") {
    key_type = "l";
  } else if (type_str == "set") {
    key_type = "s";
  } else if (type_str == "zset") {
    key_type = "z";
  } else {
    LOG(WARNING) << "Get key type error: " << key;
    key_type = "";
    return -1;
  }
  return 1;
}

// do migrate cli auth
static int doAuth(net::NetCli *cli) {
  net::RedisCmdArgsType argv;
  std::string wbuf_str;
  std::string requirepass = g_pika_conf->requirepass();
  if (requirepass != "") {
    argv.push_back("auth");
    argv.push_back(requirepass);
  } else {
    argv.push_back("ping");
  }
  net::SerializeRedisCommand(argv, &wbuf_str);

  pstd::Status s;
  s = cli->Send(&wbuf_str);
  if (!s.ok()) {
    LOG(WARNING) << "Slot Migrate auth Send error: " << s.ToString();
    return -1;
  }
  // Recv
  s = cli->Recv(&argv);
  if (!s.ok()) {
    LOG(WARNING) << "Slot Migrate auth Recv error: " << s.ToString();
    return -1;
  }
  pstd::StringToLower(argv[0]);
  if (argv[0] != "ok" && argv[0] != "pong" && argv[0].find("no password") == std::string::npos) {
    LOG(WARNING) << "Slot Migrate auth error: " << argv[0];
    return -1;
  }
  return 0;
}


// rocksdb namespace function
std::string GetSlotsTagKey(uint32_t crc) { return SlotTagPrefix + std::to_string(crc); }

// delete key from db
int DeleteKey(const std::string key, const char key_type, std::shared_ptr<Slot> slot) {
  LOG(INFO) << "Del key Srem key " << key;
  int32_t res = 0;
  std::string slotKey = GetSlotKey(GetSlotID(key));
  LOG(INFO) << "Del key Srem key " << key;

  // delete key from slot
  std::vector<std::string> members;
  members.push_back(key_type + key);
  rocksdb::Status s = slot->db()->SRem(slotKey, members, &res);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      LOG(INFO) << "Del key Srem key " << key << " not found";
      return 0;
    } else {
      LOG(WARNING) << "Del key Srem key: " << key << " from slotKey, error: " << strerror(errno);
      return -1;
    }
  }

  // delete key from db
  members.clear();
  members.push_back(key);
  std::map<storage::DataType, storage::Status> type_status;
  int64_t del_nums = slot->db()->Del(members, &type_status);
  if (0 > del_nums) {
    LOG(WARNING) << "Del key: " << key << " at slot " << GetSlotID(key) << " error";
    return -1;
  }

  return 1;
}

// get all hash field and values
static int hashGetall(const std::string key, std::vector<storage::FieldValue> *fvs, std::shared_ptr<Slot> slot) {
  rocksdb::Status s = slot->db()->HGetall(key, fvs);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      LOG(WARNING) << "HGetall key: " << key << " not found ";
      return 0;
    } else {
      LOG(WARNING) << "HGetall key: " << key << " error: " << s.ToString();
      return -1;
    }
  }
  return 1;
}

// get list key all values
static int listGetall(const std::string key, std::vector<std::string> *values, std::shared_ptr<Slot> slot) {
  rocksdb::Status s = slot->db()->LRange(key, 0, -1, values);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      LOG(WARNING) << "List get key: " << key << " value not found ";
      return 0;
    } else {
      LOG(WARNING) << "List get key: " << key << " value error: " << s.ToString();
      return -1;
    }
  }
  return 1;
}

// check slotkey remaind keys number
static void SlotKeyLenCheck(const std::string slotKey, CmdRes &res, std::shared_ptr<Slot> slot) {
  int32_t card = 0;
  rocksdb::Status s = slot->db()->SCard(slotKey, &card);
  if (!(s.ok() || s.IsNotFound())) {
    res.SetRes(CmdRes::kErrOther, "migrate slot kv error");
    res.AppendArrayLen(2);
    res.AppendInteger(1);
    res.AppendInteger(1);
    return;
  }
  res.AppendArrayLen(2);
  res.AppendInteger(1);
  res.AppendInteger(card);
  return;
}

// get set key all values
static int setGetall(const std::string key, std::vector<std::string> *members, std::shared_ptr<Slot> slot) {
  rocksdb::Status s = slot->db()->SMembers(key, members);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      LOG(WARNING) << "Set get key: " << key << " value not found ";
      return 0;
    } else {
      LOG(WARNING) << "Set get key: " << key << " value error: " << s.ToString();
      return -1;
    }
  }
  return 1;
}

// get one zset key all values
static int zsetGetall(const std::string key, std::vector<storage::ScoreMember> *score_members,
                      std::shared_ptr<Slot> slot) {
  rocksdb::Status s = slot->db()->ZRange(key, 0, -1, score_members);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      LOG(WARNING) << "zset get key: " << key << " not found ";
      return 0;
    } else {
      LOG(WARNING) << "zset get key: " << key << " value error: " << s.ToString();
      return -1;
    }
  }
  return 1;
}

void SlotsMgrtTagSlotCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsMgrtTagSlot);
    return;
  }
  // Remember the first args is the opt name
  PikaCmdArgsType::const_iterator it = argv_.begin() + 1;
  dest_ip_ = *it++;
  pstd::StringToLower(dest_ip_);

  std::string str_dest_port = *it++;
  if (!pstd::string2int(str_dest_port.data(), str_dest_port.size(), &dest_port_)) {
    std::string detail = "invalid port nummber = " + dest_port_;
    res_.SetRes(CmdRes::kErrOther, detail);
    return;
  }
  if (dest_port_ < 0 || dest_port_ > 65535) {
    std::string detail = "invalid port nummber = " + dest_port_;
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
    std::string detail = "invalid timeout nummber = " + timeout_ms_;
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
  if (slot_id_ < 0 || slot_id_ >= HASH_SLOTS_SIZE) {
    std::string detail = "invalid slot nummber = " + slot_id_;
    res_.SetRes(CmdRes::kErrOther, detail);
    return;
  }
}

void SlotsMgrtTagSlotCmd::Do(std::shared_ptr<Slot> slot) {
  if (g_pika_conf->slotmigrate() != true) {
    LOG(WARNING) << "Not in slotmigrate mode";
    res_.SetRes(CmdRes::kErrOther, "not set slotmigrate");
    return;
  }

  int32_t len = 0;
  int ret = 0;
  std::string detail;
  std::string slot_key = GetSlotKey(slot_id_);

  // first get the count of slot_key, prevent to sscan key very slowly when the key is notfund
  rocksdb::Status s = slot->db()->SCard(slot_key, &len);
  LOG(INFO) << "【SlotsMgrtTagSlotCmd::Do】Get count, slot_key: " << slot_key << ", len: " << len;
  if (len < 0) {
    detail = "Get the len of slot Error";
  }
  // mutex between SlotsMgrtTagSlotCmd、SlotsMgrtTagOneCmd and migrator_thread
  if (len > 0 && g_pika_server->pika_migrate_->Trylock()) {
    g_pika_server->pika_migrate_->CleanMigrateClient();
    int64_t next_cursor = 0;
    std::vector<std::string> members;
    rocksdb::Status s = slot->db()->SScan(slot_key, 0, "*", 1, &members, &next_cursor);
    if (s.ok()) {
      for (const auto &member : members) {
        std::string key = member;
        char type = key.at(0);
        key.erase(key.begin());
        ret = SlotsMgrtTag(dest_ip_, dest_port_, timeout_ms_, key, type, detail, slot);
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
    res_.AppendInteger(ret);    // the number of keys migrated
    res_.AppendInteger(len-ret);  // the number of keys remained
  } else {
    res_.SetRes(CmdRes::kErrOther, detail);
  }

  return;
}

// check key type
int SlotsMgrtTagOneCmd::KeyTypeCheck(std::shared_ptr<Slot> slot) {
  std::string type_str;
  rocksdb::Status s = slot->db()->Type(key_, &type_str);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      LOG(INFO) << "Migrate slot key " << key_ << " not found";
      res_.AppendInteger(0);
    } else {
      LOG(WARNING) << "Migrate slot key: " << key_ << " error: " << s.ToString();
      res_.SetRes(CmdRes::kErrOther, "migrate slot error");
    }
    return -1;
  }
  if (type_str == "string") {
    key_type_ = 'k';
  } else if (type_str == "hash") {
    key_type_ = 'h';
  } else if (type_str == "list") {
    key_type_ = 'l';
  } else if (type_str == "set") {
    key_type_ = 's';
  } else if (type_str == "zset") {
    key_type_ = 'z';
  } else {
    LOG(WARNING) << "Migrate slot key: " << key_ << " not found";
    res_.AppendInteger(0);
    return -1;
  }
  return 0;
}

// delete one key from slotkey
int SlotsMgrtTagOneCmd::SlotKeyRemCheck(std::shared_ptr<Slot> slot) {
  std::string slotKey = GetSlotKey(slot_id_);
  std::string tkey = std::string(1, key_type_) + key_;
  std::vector<std::string> members(1, tkey);
  int32_t count = 0;
  rocksdb::Status s = slot->db()->SRem(slotKey, members, &count);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      LOG(INFO) << "Migrate slot: " << slot_id_ << " not found ";
      res_.AppendInteger(0);
    } else {
      LOG(WARNING) << "Migrate slot key: " << key_ << " error: " << s.ToString();
      res_.SetRes(CmdRes::kErrOther, "migrate slot error");
    }
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
  PikaCmdArgsType::const_iterator it = argv_.begin() + 1;
  dest_ip_ = *it++;
  pstd::StringToLower(dest_ip_);

  std::string str_dest_port = *it++;
  if (!pstd::string2int(str_dest_port.data(), str_dest_port.size(), &dest_port_)) {
    std::string detail = "invalid port nummber = " + dest_port_;
    res_.SetRes(CmdRes::kErrOther, detail);
    return;
  }
  if (dest_port_ < 0 || dest_port_ > 65535) {
    std::string detail = "invalid port nummber = " + dest_port_;
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
    std::string detail = "invalid timeout nummber = " + timeout_ms_;
    res_.SetRes(CmdRes::kErrOther, detail);
    return;
  }
  if (timeout_ms_ == 0) {
    timeout_ms_ = 100;
  }

  key_ = *it++;
}

void SlotsMgrtTagOneCmd::Do(std::shared_ptr<Slot> slot) {
  if (g_pika_conf->slotmigrate() != true){
    LOG(WARNING) << "Not in slotmigrate mode";
    res_.SetRes(CmdRes::kErrOther, "not set slotmigrate");
    return;
  }

  int64_t ret = 0;
  int32_t len = 0;
  int     hastag = 0;
  uint32_t crc;
  std::string detail;
  rocksdb::Status s;
  std::map<storage::DataType, rocksdb::Status> type_status;

  //check if need migrate key, if the key is not exist, return
  GetSlotsID(key_, &crc, &hastag);
  if (!hastag) {
    std::vector<std::string> keys;
    keys.push_back(key_);

    //check the key is not exist
    ret = slot->db()->Exists(keys, &type_status);

    //when the key is not exist, ret = 0
    if ( ret == -1 ){
      res_.SetRes(CmdRes::kErrOther, "exists internal error");
      return;
    }

    if( ret == 0 ) {
      res_.AppendInteger(0);
      return;
    }

    //else need to migrate
  }else{
    //key is tag_key, check the number of the tag_key
    std::string tag_key = GetSlotsTagKey(crc);
    s = slot->db()->SCard(tag_key, &len);
    if (!s.ok() || len == -1) {
      res_.SetRes(CmdRes::kErrOther, "cont get the mumber of tag_key");
      return;
    }

    if (len == 0) {
      res_.AppendInteger(0);
      return;
    }

    //else need to migrate
  }

  //lock batch migrate, dont do slotsmgrttagslot when do slotsmgrttagone
  //g_pika_server->mgrttagslot_mutex_.Lock();
  //pika_server thread exit(~PikaMigrate) and dispatch thread do CronHandle nead lock()
  g_pika_server->pika_migrate_->Lock();

  //check if need migrate key, if the key is not exist, return
  //GetSlotsNum(key_, &crc, &hastag);
  if (!hastag) {
    std::vector<std::string> keys;
    keys.push_back(key_);
    //the key may be deleted by other thread
    std::map<storage::DataType, rocksdb::Status> type_status;
    ret = slot->db()->Exists(keys, &type_status);

    //when the key is not exist, ret = 0
    if (ret == -1) {
      detail = s.ToString();
    } else if (KeyTypeCheck(slot) != 0) {
      detail = "cont get the key type.";
      ret = -1;
    } else {
      ret = SlotsMgrtTag(dest_ip_, dest_port_, timeout_ms_, key_, key_type_, detail, slot);
    }
  } else {
    //key maybe not exist, the key is tag key, migrate the same tag key
    ret = SlotsMgrtTag(dest_ip_, dest_port_, timeout_ms_, key_, 0, detail, slot);
  }

  //unlock the record lock
  //g_pika_server->mutex_record_.Unlock(key_);

  //unlock
  g_pika_server->pika_migrate_->Unlock();


  if(ret >= 0){
    res_.AppendInteger(ret);
  }else{
    if(detail.size() == 0){
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

  if(argv_.size() >= 2){
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

  if(argv_.size() >= 3){
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

void SlotsInfoCmd::Do(std::shared_ptr<Slot>slot) {
  std::map<int64_t,int64_t> slotsMap;
  for (int i = 0; i < HASH_SLOTS_SIZE; ++i){
    int32_t card = 0;
    rocksdb::Status s = slot->db()->SCard(SlotKeyPrefix+std::to_string(i), &card);
    if (s.ok()) {
      slotsMap[i] = card;
    } else if (s.IsNotFound()){
      continue;
    } else {
      res_.SetRes(CmdRes::kErrOther, "Slotsinfo scard error");
      return;
    }
  }
  res_.AppendArrayLen(slotsMap.size());
  std::map<int64_t,int64_t>::iterator it;
  for (it = slotsMap.begin(); it != slotsMap.end(); ++it){
    res_.AppendArrayLen(2);
    res_.AppendInteger(it->first);
    res_.AppendInteger(it->second);
  }
  return;
}

void SlotsMgrtTagSlotAsyncCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsMgrtTagSlotAsync);
  }
  // Remember the first args is the opt name
  PikaCmdArgsType::const_iterator it = argv_.begin() + 1;
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
  if (!pstd::string2int(str_dest_port.data(), str_dest_port.size(), &dest_port_) || dest_port_ <= 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }

  std::string str_max_bulks = *it++;
  if (!pstd::string2int(str_dest_port.data(), str_dest_port.size(), &dest_port_) || dest_port_ <= 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }

  std::string str_max_bytes_ = *it++;
  if (!pstd::string2int(str_dest_port.data(), str_dest_port.size(), &dest_port_) || dest_port_ <= 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }

  std::string str_slot_num = *it++;
  if (!pstd::string2int(str_slot_num.data(), str_slot_num.size(), &slot_num_) || slot_num_ < 0 ||
      slot_num_ >= HASH_SLOTS_SIZE) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }

  std::string str_keys_num = *it++;
  if (!pstd::string2int(str_dest_port.data(), str_dest_port.size(), &dest_port_) || dest_port_ <= 0){
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  return;
}

void SlotsMgrtTagSlotAsyncCmd::Do(std::shared_ptr<Slot> slot) {
  // check whether open slotmigrate
  if (!g_pika_conf->slotmigrate()) {
    res_.SetRes(CmdRes::kErrOther, "please open slotmigrate and reload slot");
    return;
  }

  bool ret = g_pika_server->SlotsMigrateBatch(dest_ip_, dest_port_, timeout_ms_, slot_num_, keys_num_, slot);
  if (!ret) {
    LOG(WARNING) << "Slot batch migrate keys error";
    res_.SetRes(CmdRes::kErrOther, "Slot batch migrating keys error, may be currently migrating");
    return;
  }

  int32_t remained = 0;
  std::string slotKey = GetSlotKey(slot_num_);
  storage::Status status = slot->db()->SCard(slotKey, &remained);
  if (status.ok()) {
    res_.AppendArrayLen(2);
    res_.AppendInteger(0);
    res_.AppendInteger(remained);
  } else {
    LOG(WARNING) << "Slot batch migrate keys get result error";
    res_.SetRes(CmdRes::kErrOther, "Slot batch migrating keys get result error");
    return;
  }
  return;
}

void SlotsMgrtAsyncStatusCmd::DoInitial() {
  if (CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsMgrtAsyncStatus);
  }
  return;
}

void SlotsMgrtAsyncStatusCmd::Do(std::shared_ptr<Slot> slot) {
  std::string status;
  std::string ip;
  int64_t port, slots, moved, remained;
  bool migrating;
  g_pika_server->GetSlotsMgrtSenderStatus(&ip, &port, &slots, &migrating, &moved, &remained);
  std::string mstatus = migrating ? "yes" : "no";
  res_.AppendArrayLen(5);
  status = "dest server: " + ip + ":" + std::to_string(port);
  res_.AppendStringLen(status.size());
  res_.AppendContent(status);
  status = "slot number: " + std::to_string(slots);
  res_.AppendStringLen(status.size());
  res_.AppendContent(status);
  status = "migrating  : " + mstatus;
  res_.AppendStringLen(status.size());
  res_.AppendContent(status);
  status = "moved keys : " + std::to_string(moved);
  res_.AppendStringLen(status.size());
  res_.AppendContent(status);
  status = "remain keys: " + std::to_string(remained);
  res_.AppendStringLen(status.size());
  res_.AppendContent(status);

  return;
}

void SlotsMgrtAsyncCancelCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsMgrtAsyncCancel);
  }
  return;
}

void SlotsMgrtAsyncCancelCmd::Do(std::shared_ptr<Slot> slot) {
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

void SlotsDelCmd::Do(std::shared_ptr<Slot> slot) {
  std::vector<std::string> keys;
  std::vector<std::string>::const_iterator iter;
  for (iter = slots_.begin(); iter != slots_.end(); iter++) {
    keys.push_back(SlotKeyPrefix + *iter);
  }
  std::map<storage::DataType, rocksdb::Status> type_status;
  int64_t count = slot->db()->Del(keys, &type_status);
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

void SlotsHashKeyCmd::Do(std::shared_ptr<Slot> slot) {
  std::vector<std::string>::const_iterator keys_it;

  res_.AppendArrayLen(keys_.size());
  for (keys_it = keys_.begin(); keys_it != keys_.end(); ++keys_it) {
    res_.AppendInteger(GetSlotsID(*keys_it, NULL, NULL));
  }

  return;
}

void SlotsScanCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsScan);
    return;
  }
  key_ = SlotKeyPrefix + argv_[1];
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

void SlotsScanCmd::Do(std::shared_ptr<Slot>slot) {
  std::vector<std::string> members;
  rocksdb::Status s = slot->db()->SScan(key_, cursor_, pattern_, count_, &members, &cursor_);

  if (members.size() <= 0) {
    cursor_ = 0;
  }
  res_.AppendContent("*2");

  char buf[32];
  int64_t len = pstd::ll2string(buf, sizeof(buf), cursor_);
  res_.AppendStringLen(len);
  res_.AppendContent(buf);

  res_.AppendArrayLen(members.size());
  std::vector<std::string>::const_iterator iter_member = members.begin();
  for (; iter_member != members.end(); iter_member++) {
    res_.AppendStringLen(iter_member->size());
    res_.AppendContent(*iter_member);
  }
  return;
}

void SlotsMgrtExecWrapperCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsMgrtExecWrapper);
  }
  PikaCmdArgsType::const_iterator it = argv_.begin() + 1;
  key_ = *it++;
  pstd::StringToLower(key_);
  return;
}

void SlotsMgrtExecWrapperCmd::Do(std::shared_ptr<Slot> slot) {
  res_.AppendArrayLen(2);
  int ret = g_pika_server->SlotsMigrateOne(key_, slot);
  switch (ret) {
    case 0:
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

