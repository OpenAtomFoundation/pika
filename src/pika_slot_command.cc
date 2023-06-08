// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <algorithm>
#include <vector>
#include "include/pika_conf.h"
#include "include/pika_server.h"
#include "include/pika_slot_command.h"
#include "pstd/include/pstd_status.h"
#include "pstd/include/pstd_string.h"
#include "include/pika_data_distribution.h"
#include "storage/include/storage/storage.h"

#define min(a, b)  (((a) > (b)) ? (b) : (a))

extern std::unique_ptr<PikaServer> g_pika_server;
extern std::unique_ptr<PikaConf> g_pika_conf;

uint32_t crc32tab[256];
void CRC32TableInit(uint32_t poly) {
  int i, j;
  for (i = 0; i < 256; i ++) {
    uint32_t crc = i;
    for (j = 0; j < 8; j ++) {
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
  for (i = 0; i < len; i ++) {
    crc = crc32tab[(uint8_t)((char)crc ^ buf[i])] ^ (crc >> 8);
  }
  return ~crc;
}

//get slot tag
static const char* GetSlotsTag(const std::string &str, int *plen) {
  const char *s = str.data();
  int i, j, n = str.length();
  for (i = 0; i < n && s[i] != '{'; i ++) {}
  if (i == n) {
    return NULL;
  }
  i ++;
  for (j = i; j < n && s[j] != '}'; j ++) {}
  if (j == n) {
    return NULL;
  }
  if (plen != NULL) {
    *plen = j - i;
  }
  return s + i;
}


// get key slot number
int GetSlotNum(const std::string &str) {
  uint32_t crc = CRC32Update(0, str.data(), (int)str.size());
  return (int)(crc & HASH_SLOTS_MASK);
}


// get the slot number by key
int GetSlotsNum(const std::string &str, uint32_t *pcrc, int *phastag) {
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


uint32_t CRC32CheckSum(const char *buf, int len)
{
  return CRC32Update(0, buf, len);
}

// add key to slotkey
void AddSlotKey(const std::string type, const std::string key, std::shared_ptr<Slot>slot) {
  if (g_pika_conf->slotmigrate() != true){
    return;
  }
  int32_t count = 0;
  std::vector<std::string> members(1, type + key);
  std::string slotKey = SlotKeyPrefix + std::to_string(GetSlotNum(key));
  rocksdb::Status s = slot->db()->SAdd(slotKey, members, &count);
  if (!s.ok()) {
    LOG(WARNING) << "SAdd key: " << key << " to slotKey, error: " << s.ToString();
  }
}

//check key exists
void RemKeyNotExists(const std::string type, const std::string key, std::shared_ptr<Slot>slot) {
  if (g_pika_conf->slotmigrate() != true){
    return;
  }
  std::vector<std::string> vkeys;
  vkeys.push_back(key);
//  std::map<storage::DataType, Status> type_status;
  std::map<storage::DataType, rocksdb::Status> type_status;
  int64_t res = slot->db()->Exists(vkeys, &type_status);
  if (res == 0) {
    std::string slotKey = SlotKeyPrefix + std::to_string(GetSlotNum(key));
    std::vector<std::string> members(1, type + key);
    int32_t count = 0;
    rocksdb::Status s =slot->db()->SRem(slotKey, members, &count);
    if (!s.ok()) {
      LOG(WARNING) << "Zrem key: " << key << " from slotKey, error: " << s.ToString();
      return;
    }
  }
  return;
}

//del key from slotkey
void RemSlotKey(const std::string key, std::shared_ptr<Slot>slot) {
  if (g_pika_conf->slotmigrate() != true){
    return;
  }
  std::string type;
  if (GetKeyType(key, type, slot) < 0){
    LOG(WARNING) << "SRem key: " << key << " from slotKey error";
    return;
  }
  std::string slotKey = SlotKeyPrefix + std::to_string(GetSlotNum(key));
  int32_t count = 0;
  std::vector<std::string> members(1, type + key);
  rocksdb::Status s = slot->db()->SRem(slotKey, members, &count);
  if (!s.ok()) {
    LOG(WARNING) << "SRem key: " << key << " from slotKey, error: " << s.ToString();
    return;
  }
}

int GetKeyType(const std::string key, std::string &key_type, std::shared_ptr<Slot>slot) {
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

//do migrate key to dest pika server
static int doMigrate(net::NetCli *cli, std::string send_str){
  pstd::Status s;
  s = cli->Send(&send_str);
  if (!s.ok()) {
    LOG(WARNING) << "Slot Migrate Send error: " << s.ToString();
    return -1;
  }
  return 1;
}

//do migrate cli auth
static int doAuth(net::NetCli *cli){
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

// get kv key value
static int kvGet(const std::string key, std::string &value, std::shared_ptr<Slot>slot){
  rocksdb::Status s = slot->db()->Get(key, &value);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      value = "";
      LOG(WARNING) << "Get kv key: " << key << " not found ";
      return 0;
    } else {
      value = "";
      LOG(WARNING) << "Get kv key: " << key << " error: " << s.ToString();
      return -1;
    }
  }
  return 0;
}

std::string GetSlotKey(int slot) { return SlotKeyPrefix + std::to_string(slot); }

// delete key from db
int DeleteKey(const std::string key, const char key_type, std::shared_ptr<Slot> slot) {
  int32_t res = 0;
  std::string slotKey = GetSlotKey(GetSlotNum(key));

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
    LOG(WARNING) << "Del key: " << key << " at slot " << GetSlotNum(key) << " error";
    return -1;
  }

  return 1;
}

static int migrateKeyTTl(net::NetCli *cli, const std::string key, std::shared_ptr<Slot>slot){
  net::RedisCmdArgsType argv;
  std::string send_str;
  std::map<storage::DataType, rocksdb::Status> type_status;
  std::map<storage::DataType, int64_t> type_timestamp;
  type_timestamp = slot->db()->TTL(key, &type_status);

  for (const auto& item : type_timestamp) {
    // mean operation exception errors happen in database
    if (item.second == -3) {
      return 0;
    }
  }

  int64_t ttl = 0;
  if (type_timestamp[storage::kStrings] != -2) {
    ttl = type_timestamp[storage::kStrings];
  } else if (type_timestamp[storage::kHashes] != -2) {
    ttl = type_timestamp[storage::kHashes];
  } else if (type_timestamp[storage::kLists] != -2) {
    ttl = type_timestamp[storage::kLists];
  } else if (type_timestamp[storage::kSets] != -2) {
    ttl = type_timestamp[storage::kSets];
  } else if (type_timestamp[storage::kZSets] != -2) {
    ttl = type_timestamp[storage::kZSets];
  } else {
    // mean this key not exist
    return 0;
  }

  if (ttl > 0){
    argv.push_back("expire");
    argv.push_back(key);
    argv.push_back(std::to_string(ttl));
    net::SerializeRedisCommand(argv, &send_str);
    if (doMigrate(cli, send_str) < 0){
      return -1;
    }
    return 1;
  }
  return 0;
}

// migrate one kv key
static int migrateKv(net::NetCli *cli, const std::string key, bool async, std::shared_ptr<Slot>slot){
  int r, ret = 0;
  std::string value;
  if (kvGet(key, value, slot) < 0){
    return -1;
  }
  if (value == ""){
    return 0;
  }

  net::RedisCmdArgsType argv;
  std::string send_str;
  argv.push_back("set");
  argv.push_back(key);
  argv.push_back(value);
  net::SerializeRedisCommand(argv, &send_str);

  if (doMigrate(cli, send_str) < 0){
    return -1;
  } else {
    ret++;
  }

  if ((r = migrateKeyTTl(cli, key, slot)) < 0){
    return -1;
  } else {
    ret += r;
  }

  if (!async) {
    DeleteKey(key, 'k', slot); //key already been migrated successfully, del error doesn't matter
  }
  return ret;
}

//get all hash field and values
static int hashGetall(const std::string key, std::vector<storage::FieldValue> *fvs, std::shared_ptr<Slot>slot){
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

// migrate one hash key
static int migrateHash(net::NetCli *cli, const std::string key, bool async, std::shared_ptr<Slot>slot){
  int r, ret = 0;
  std::vector<storage::FieldValue> fvs;
  if (hashGetall(key, &fvs, slot) < 0){
    return -1;
  }
  size_t keySize = fvs.size();
  if (keySize == 0){
    return 0;
  }

  net::RedisCmdArgsType argv;
  std::string send_str;
  for (size_t i = 0; i <= keySize / MaxKeySendSize; ++i){
    if (i > 0){
      LOG(WARNING) << "Migrate big key: " << key << " size: " << keySize << " migrated value: " << min((i + 1) * MaxKeySendSize, keySize);
    }
    argv.clear();
    send_str = "";
    argv.push_back("hmset");
    argv.push_back(key);
    for (size_t j = i * MaxKeySendSize; j < (i + 1) * MaxKeySendSize && j < keySize; ++j){
      argv.push_back(fvs[j].field);
      argv.push_back(fvs[j].value);
    }
    net::SerializeRedisCommand(argv, &send_str);
    if (doMigrate(cli, send_str) < 0){
      return -1;
    } else {
      ret++;
    }
  }

  if ((r = migrateKeyTTl(cli, key, slot)) < 0){
    return -1;
  } else {
    ret += r;
  }

  if (!async) {
    DeleteKey(key, 'h', slot); //key already been migrated successfully, del error doesn't matter
  }
  return ret;
}

// get list key all values
static int listGetall(const std::string key, std::vector<std::string> *values, std::shared_ptr<Slot>slot){
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
static void SlotKeyLenCheck(const std::string slotKey, CmdRes& res, std::shared_ptr<Slot>slot){
  int32_t card = 0;
  rocksdb::Status s = slot->db()->SCard(slotKey, &card);
  if (!(s.ok() || s.IsNotFound())){
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

// migrate one list key
static int migrateList(net::NetCli *cli, const std::string key, bool async, std::shared_ptr<Slot>slot){
  int r, ret = 0;
  std::vector<std::string> values;
  if (listGetall(key, &values, slot) < 0){
    return -1;
  }
  size_t keySize = values.size();
  if (keySize == 0){
    return 0;
  }

  net::RedisCmdArgsType argv;
  std::string send_str;
  for (size_t i = 0; i <= keySize / MaxKeySendSize; ++i){
    if (i > 0){
      LOG(WARNING) << "Migrate big key: " << key << " size: " << keySize << " migrated value: " << min((i + 1) * MaxKeySendSize, keySize);
    }
    argv.clear();
    send_str = "";
    argv.push_back("lpush");
    argv.push_back(key);
    for (size_t j = i * MaxKeySendSize; j < (i + 1) * MaxKeySendSize && j < keySize; ++j){
      argv.push_back(values[j]);
    }
    net::SerializeRedisCommand(argv, &send_str);
    if (doMigrate(cli, send_str) < 0){
      return -1;
    } else {
      ret++;
    }
  }

  if ((r = migrateKeyTTl(cli, key, slot)) < 0){
    return -1;
  } else {
    ret += r;
  }

  if (!async) {
    DeleteKey(key, 'l', slot); //key already been migrated successfully, del error doesn't matter
  }
  return ret;
}

// get set key all values
static int setGetall(const std::string key, std::vector<std::string> *members, std::shared_ptr<Slot>slot){
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

// migrate one set key
static int migrateSet(net::NetCli *cli, const std::string key, bool async, std::shared_ptr<Slot>slot){
  int r, ret = 0;
  std::vector<std::string> members;
  if (setGetall(key, &members, slot) < 0){
    return -1;
  }
  size_t keySize = members.size();
  if (keySize == 0){
    return 0;
  }

  net::RedisCmdArgsType argv;
  std::string send_str;
  for (size_t i = 0; i <= keySize / MaxKeySendSize; ++i){
    if (i > 0){
      LOG(WARNING) << "Migrate big key: " << key << " size: " << keySize << " migrated value: " << min((i + 1) * MaxKeySendSize, keySize);
    }
    argv.clear();
    send_str = "";
    argv.push_back("sadd");
    argv.push_back(key);
    for (size_t j = i * MaxKeySendSize; j < (i + 1) * MaxKeySendSize && j < keySize; ++j){
      argv.push_back(members[j]);
    }
    net::SerializeRedisCommand(argv, &send_str);
    if (doMigrate(cli, send_str) < 0){
      return -1;
    } else {
      ret++;
    }
  }

  if ((r = migrateKeyTTl(cli, key, slot)) < 0){
    return -1;
  } else {
    ret += r;
  }

  if (!async) {
    DeleteKey(key, 's', slot); //key already been migrated successfully, del error doesn't matter
  }
  return ret;
}

// get one zset key all values
static int zsetGetall(const std::string key, std::vector<storage::ScoreMember> *score_members, std::shared_ptr<Slot>slot){
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

// migrate zset key
static int migrateZset(net::NetCli *cli, const std::string key, bool async, std::shared_ptr<Slot>slot){
  int r, ret = 0;
  std::vector<storage::ScoreMember> score_members;
  if (zsetGetall(key, &score_members, slot) < 0){
    return -1;
  }
  size_t keySize = score_members.size();
  if (keySize == 0){
    return 0;
  }

  net::RedisCmdArgsType argv;
  std::string send_str;
  for (size_t i = 0; i <= keySize / MaxKeySendSize; ++i){
    if (i > 0) {
      LOG(WARNING) << "Migrate big key: " << key << " size: " << keySize << " migrated value: " << min((i + 1) * MaxKeySendSize, keySize);
    }
    argv.clear();
    send_str = "";
    argv.push_back("zadd");
    argv.push_back(key);
    for (size_t j = i * MaxKeySendSize; j < (i + 1) * MaxKeySendSize && j < keySize; ++j){
      argv.push_back(std::to_string(score_members[j].score));
      argv.push_back(score_members[j].member);
    }
    net::SerializeRedisCommand(argv, &send_str);
    if (doMigrate(cli, send_str) < 0){
      return -1;
    } else {
      ret++;
    }
  }

  if ((r = migrateKeyTTl(cli, key, slot)) < 0){
    return -1;
  } else {
    ret += r;
  }

  if (!async) {
    DeleteKey(key, 'z', slot);
  }
  return ret;
}

// migrate key
static int MigrateOneKey(net::NetCli *cli, const std::string key, const char key_type, bool async, std::shared_ptr<Slot>slot){
  int ret;
  switch (key_type) {
    case 'k':
      if ((ret = migrateKv(cli, key, async, slot)) < 0){
        AddSlotKey("k", key, slot);
        return -1;
      }
      break;
    case 'h':
      if ((ret = migrateHash(cli, key, async, slot)) < 0){
        AddSlotKey("h", key, slot);
        return -1;
      }
      break;
    case 'l':
      if ((ret = migrateList(cli, key, async, slot)) < 0){
        AddSlotKey("l", key, slot);
        return -1;
      }
      break;
    case 's':
      if ((ret = migrateSet(cli, key, async, slot)) < 0){
        AddSlotKey("s", key, slot);
        return -1;
      }
      break;
    case 'z':
      if ((ret = migrateZset(cli, key, async, slot)) < 0){
        AddSlotKey("z", key, slot);
        return -1;
      }
      break;
    default:
      return -1;
      break;
  }
  return ret;
}

void SlotsMgrtTagSlotCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsMgrtTagSlot);
    return;
  }
  PikaCmdArgsType::const_iterator it = argv_.begin() + 1; //Remember the first args is the opt name
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

  std::string str_slot_num = *it++;
  if (!pstd::string2int(str_slot_num.data(), str_slot_num.size(), &slot_num_) || slot_num_ < 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
}

void SlotsMgrtTagSlotCmd::Do(std::shared_ptr<Slot>slot) {
  if (SlotKeyPop(slot) < 0){
    return;
  }
  net::NetCli *cli = net::NewRedisCli();
  cli->set_connect_timeout(timeout_ms_);
  if ((cli->Connect(dest_ip_, dest_port_, "")).ok()) {
    cli->set_send_timeout(timeout_ms_);
    cli->set_recv_timeout(timeout_ms_);
    if (doAuth(cli) < 0) {
      res_.SetRes(CmdRes::kErrOther, "Slot Migrate auth destination server error");
      cli->Close();
      delete cli;
      return;
    }

    if (MigrateOneKey(cli, key_, key_type_, false, slot) < 0){
      res_.SetRes(CmdRes::kErrOther, "migrate slot error");
      cli->Close();
      delete cli;
      return;
    }
  } else  {
    LOG(WARNING) << "Slot Migrate Connect destination error: " <<strerror(errno);
    res_.SetRes(CmdRes::kErrOther, "Slot Migrate connect destination server error");
    cli->Close();
    delete cli;
    return;
  }

  cli->Close();
  delete cli;
  std::string slotKey = SlotKeyPrefix+std::to_string(slot_num_);
  SlotKeyLenCheck(slotKey, res_, slot);
  return;
}


// pop one key from slotkey
int SlotsMgrtTagSlotCmd::SlotKeyPop(std::shared_ptr<Slot>slot){
  std::string slotKey = SlotKeyPrefix+std::to_string(slot_num_);
  std::vector<std::string> member;
  rocksdb::Status s = slot->db()->SPop(slotKey, &member, 1);
  if (!s.ok()) {
    LOG(WARNING) << "Migrate slot: " << slot_num_ << " error: " << s.ToString();
    res_.AppendArrayLen(2);
    res_.AppendInteger(0);
    res_.AppendInteger(0);
    return -1;
  }

  key_type_ = member[0].at(0);
  key_ = member[0];
  key_.erase(key_.begin());

  return 0;
}
void SlotsMgrtTagOneCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsMgrtTagOne);
    return;
  }
  PikaCmdArgsType::const_iterator it = argv_.begin() + 1; //Remember the first args is the opt name
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

  key_ = *it++;
  pstd::StringToLower(key_);

  slot_num_ = GetSlotNum(key_);
}

// check key type
int SlotsMgrtTagOneCmd::KeyTypeCheck(std::shared_ptr<Slot>slot) {
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
    LOG(WARNING) << "Migrate slot key: " <<key_ << " not found";
    res_.AppendInteger(0);
    return -1;
  }
  return 0;
}

// delete one key from slotkey
int SlotsMgrtTagOneCmd::SlotKeyRemCheck(std::shared_ptr<Slot>slot){
  std::string slotKey = SlotKeyPrefix+std::to_string(slot_num_);
  std::string tkey = std::string(1,key_type_) + key_;
  std::vector<std::string> members(1, tkey);
  int32_t count = 0;
  rocksdb::Status s = slot->db()->SRem(slotKey, members, &count);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      LOG(INFO) << "Migrate slot: " << slot_num_ << " not found ";
      res_.AppendInteger(0);
    } else {
      LOG(WARNING) << "Migrate slot key: " << key_ << " error: " << s.ToString();
      res_.SetRes(CmdRes::kErrOther, "migrate slot error");
    }
    return -1;
  }
  return 0;
}

void SlotsMgrtTagOneCmd::Do(std::shared_ptr<Slot>slot) {
  if (KeyTypeCheck(slot) < 0){
    return;
  }
  if (SlotKeyRemCheck(slot) < 0){
    return;
  }

  net::NetCli *cli = net::NewRedisCli();
  cli->set_connect_timeout(timeout_ms_);
  if ((cli->Connect(dest_ip_, dest_port_, "")).ok()) {
    cli->set_send_timeout(timeout_ms_);
    cli->set_recv_timeout(timeout_ms_);
    if (doAuth(cli) < 0) {
      res_.SetRes(CmdRes::kErrOther, "Slot Migrate auth destination server error");
      cli->Close();
      delete cli;
      return;
    }

    if (MigrateOneKey(cli, key_, key_type_, false, slot) < 0){
      res_.SetRes(CmdRes::kErrOther, "migrate one error");
      cli->Close();
      delete cli;
      return;
    }
  } else  {
    LOG(WARNING) << "Slot Migrate Connect destination error: " <<strerror(errno);
    res_.SetRes(CmdRes::kErrOther, "Slot Migrate connect destination server error");
    cli->Close();
    delete cli;
    return;
  }

  cli->Close();
  delete cli;
  res_.AppendInteger(1);
  return;
}

void SlotsInfoCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsInfo);
  }
  return;
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
  PikaCmdArgsType::const_iterator it = argv_.begin() + 1; //Remember the first args is the opt name
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
  if (!pstd::string2int(str_slot_num.data(), str_slot_num.size(), &slot_num_) || slot_num_ < 0 || slot_num_ >= HASH_SLOTS_SIZE) {
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

void SlotsMgrtTagSlotAsyncCmd::Do(std::shared_ptr<Slot>slot) {
  // check whether open slotmigrate
  if (!g_pika_conf->slotmigrate()) {
    res_.SetRes(CmdRes::kErrOther, "please open slotmigrate and reload slot");
    return;
  }

  bool ret = g_pika_server -> SlotsMigrateBatch(dest_ip_, dest_port_, timeout_ms_, slot_num_, keys_num_, slot);
  if (!ret) {
    LOG(WARNING) << "Slot batch migrate keys error";
    res_.SetRes(CmdRes::kErrOther, "Slot batch migrating keys error, may be currently migrating");
    return;
  }

  int64_t moved = 0, remained = 0;
  ret = g_pika_server->GetSlotsMigrateResult(&moved, &remained);
  if (ret){
    res_.AppendArrayLen(2);
    res_.AppendInteger(moved);
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

void SlotsMgrtAsyncStatusCmd::Do(std::shared_ptr<Slot>slot) {
  std::string status;
  std::string ip;
  int64_t port, slots, moved, remained;
  bool migrating;
  g_pika_server -> GetSlotsMgrtSenderStatus(&ip, &port, &slots, &migrating, &moved, &remained);
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

SlotsMgrtSenderThread::SlotsMgrtSenderThread() :
                                                 net::Thread(),
                                                 dest_ip_("none"),
                                                 dest_port_(-1),
                                                 timeout_ms_(3000),
                                                 slot_num_(-1),
                                                 moved_keys_num_(-1),
                                                 moved_keys_all_(-1),
                                                 remained_keys_num_(-1),
                                                 error_(false),
                                                 slotsmgrt_cond_(),
                                                 is_migrating_(false) {
  cli_ = net::NewRedisCli();
  pthread_rwlockattr_t attr;
  pthread_rwlockattr_init(&attr);
  std::lock_guard ld(rwlock_db_);
  std::lock_guard lb(rwlock_batch_);
  std::lock_guard lo(rwlock_ones_);
}

SlotsMgrtSenderThread::~SlotsMgrtSenderThread() {
  set_should_stop();
  if (is_migrating_) {
    is_migrating_ = false;
  }
  if (is_running()) {
    StopThread();
  }

  /*pthread_rwlock_destroy(&rwlock_db_);
pthread_rwlock_destroy(&rwlock_batch_);
pthread_rwlock_destroy(&rwlock_ones_);*/

  delete cli_;
  LOG(INFO) << "SlotsMgrtSender thread " << thread_id() << " exit!";
}

int SlotsMgrtSenderThread::SlotsMigrateOne(const std::string &key, std::shared_ptr<Slot>slot){

  std::lock_guard ld(rwlock_db_);
  std::lock_guard lb(rwlock_batch_);
  std::lock_guard lo(rwlock_ones_);
  int slot_num = GetSlotNum(key);
  std::string type_str;
  char key_type;
  rocksdb::Status s = slot->db()->Type(key, &type_str);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      LOG(INFO) << "Migrate key: " << key << " not found";
      return 0;
    } else {
      LOG(WARNING) << "Migrate one key: " << key << " error: " << s.ToString();
      return -1;
    }
  }
  if (type_str == "string") {
    key_type = 'k';
  } else if (type_str == "hash") {
    key_type = 'h';
  } else if (type_str == "list") {
    key_type = 'l';
  } else if (type_str == "set") {
    key_type = 's';
  } else if (type_str == "zset") {
    key_type = 'z';
  } else if (type_str == "none") {
    return 0;
  } else {
    LOG(WARNING) << "Migrate  key: " << key << " type: " << type_str << " is  illegal";
    return -1;
  }

  // when this slot has been finished migrating, and the thread migrating status is reset, but the result
  // has not been returned to proxy, during this time, some more request on this slot are sent to the server,
  // so need to check if the key exists first, if not exists the proxy forwards the request to destination server
  // if the key exists, it is an error which should not happen.
  if (slot_num != slot_num_ ) {
    LOG(WARNING) << "Slot : " <<slot_num << " is not the migrating slot:" << slot_num_;
    return -1;
  } else if (!is_migrating_) {
    LOG(WARNING) << "Slot : " <<slot_num << " is not migrating";
    return -1;
  }

  std::pair<const char, std::string> kpair = std::make_pair(key_type, key);
  if (std::find(migrating_ones_.begin(),migrating_ones_.end(),kpair) != migrating_ones_.end()){
    return 1;
  }
  if (std::find(migrating_batch_.begin(),migrating_batch_.end(),kpair) != migrating_batch_.end()){
    return 1;
  }

  std::string slotKey = SlotKeyPrefix+std::to_string(slot_num);
  int32_t remained = 0;
  std::vector<std::string> members(1, key_type + key);
  s = slot->db()->SRem(slotKey, members, &remained);
  if (s.IsNotFound()) {
    // s.IsNotFound() is also error, key found in db but not in slotkey, run slotsreload cmd first
    LOG(WARNING) << "Migrate key: " << key << " SRem from slotkey error: " << s.ToString();
    return 0;
  }

  migrating_ones_.push_back(kpair);
  return 1;
}

bool SlotsMgrtSenderThread::SlotsMigrateBatch(const std::string &ip, int64_t port, int64_t time_out, int64_t slots,
                                              int64_t keys_num, std::shared_ptr<Slot>slot){
  std::lock_guard lb(slotsmgrt_cond_mutex_);
  // if is_migrating_, does some check and prepare the keys to be migrated
  if (is_migrating_){
    if (!(dest_ip_==ip && dest_port_==port && slot_num_==slots)) {
      LOG(WARNING) << "wrong dest_ip, dest_port or slot_num";
      return false;
    }
    timeout_ms_ = time_out;
    keys_num_ = keys_num;
    slot_ = slot;
    bool ret = ElectMigrateKeys(slot_);
    if (!ret) {
      LOG(WARNING) << "Slots migrating sender get batch keys error";
      is_migrating_ = false;
      set_should_stop();
      return false;
    }
    return true;
    // if not migrating, start thread migrating
  } else {
    // set state and start a new sender
    dest_ip_ = ip;
    dest_port_ = port;
    timeout_ms_ = time_out;
    slot_num_ = slots;
    keys_num_ = keys_num;
    is_migrating_ = true;
    slot_ = slot;
    if (error_) {
      StopThread();
    }
    error_ = false;
    // 启动迁移的线程
    StartThread();
    LOG(INFO) << "Migrate batch slot: " << slot;
  }
  return true;
}

bool SlotsMgrtSenderThread::GetSlotsMigrateResult(int64_t *moved, int64_t *remained){
  std::lock_guard lm(slotsmgrt_cond_mutex_);
  *moved = moved_keys_num_;
  *remained = remained_keys_num_;
  if (*remained <= 0){
    is_migrating_ == false;
    moved_keys_num_ = 0;
    set_should_stop();
    StopThread();
  }
  return true;
}

void SlotsMgrtSenderThread::GetSlotsMgrtSenderStatus(std::string *ip, int64_t *port, int64_t *slot, bool *migrating,
                                                     int64_t *moved, int64_t *remained){
  std::lock_guard ld(rwlock_db_);
  std::lock_guard lb(rwlock_batch_);
  std::lock_guard lo(rwlock_ones_);
  *ip = dest_ip_;
  *port = dest_port_;
  *slot = slot_num_;
  *migrating = is_migrating_;
  *moved = moved_keys_all_;
  *remained = remained_keys_num_;
}

bool SlotsMgrtSenderThread::SlotsMigrateAsyncCancel() {
  std::lock_guard ld(rwlock_db_);
  dest_ip_ = "none";
  dest_port_ = -1;
  timeout_ms_ = 3000;
  slot_num_ = -1;
  moved_keys_num_ = -1;
  moved_keys_all_ = -1;
  remained_keys_num_ = -1;
  is_migrating_ = false;
  if (is_running()) {
    set_should_stop();
    StopThread();
  }
  std::lock_guard lb(rwlock_batch_);
  std::lock_guard lo(rwlock_ones_);
  std::vector<std::pair<const char, std::string>>().swap(migrating_batch_);
  std::vector<std::pair<const char, std::string>>().swap(migrating_ones_);
  return true;
}

bool SlotsMgrtSenderThread::ElectMigrateKeys(std::shared_ptr<Slot>slot){
  std::lock_guard lb(rwlock_batch_);
  std::lock_guard lo(rwlock_ones_);
  std::string slotKey = SlotKeyPrefix+std::to_string(slot_num_);
  int32_t card = 0;
  rocksdb::Status s = slot->db()->SCard(slotKey, &card);
  remained_keys_num_ = card + migrating_batch_.size() + migrating_ones_.size();
  if (remained_keys_num_ == 0){
    LOG(WARNING) << "No keys in slot: " << slot_num_;
    is_migrating_ = false;
    return true;
  } else if (remained_keys_num_ < 0) {
    LOG(WARNING) << "Scard slotkey error in slot: " << slot_num_;
    return false;
  }
  std::string key;
  char key_type;

  std::string pattern("*");
  std::vector<std::string> members;
  int64_t cursor = 0, next_cursor = 0;

  s = slot->db()->SScan(slotKey, cursor, pattern, keys_num_, &members, &next_cursor);

  if (s.ok()) {
    for (const auto& member : members) {
      key_type = member.at(0);
      key = member;
      key.erase(key.begin());
      migrating_batch_.push_back(std::make_pair(key_type, key));
      int32_t count = 0;
      rocksdb::Status s = slot->db()->SRem(slotKey, std::vector<std::string>(1, member), &count);
      if (!s.ok()) {
        if (s.IsNotFound()) {
          LOG(INFO) << "SRem key " << key << " not found ";
        } else {
          LOG(WARNING) << "SRem key: " << key << " from slotKey, error: " << s.ToString();
          std::vector<std::pair<const char, std::string>>().swap(migrating_batch_);
          return false;
        }
      }
    }
  }

  return true;
}

void* SlotsMgrtSenderThread::ThreadMain() {
  LOG(INFO) << "SlotsMgrtSender thread " << thread_id() << " for slot:" << slot_num_ << " start!";
  pstd::Status result;
  cli_->set_connect_timeout(timeout_ms_);
  moved_keys_all_ = 0;
  while (!should_stop()) {
    result = cli_->Connect(dest_ip_, dest_port_, "");
    LOG(INFO) << "Slots Migrate Sender Connect server(" << dest_ip_ << ":" << dest_port_ << ") " << result.ToString();

    if (result.ok()) {
      cli_->set_send_timeout(timeout_ms_);
      cli_->set_recv_timeout(timeout_ms_);
      if (doAuth(cli_) < 0) {
        is_migrating_ = false;
        set_should_stop();
        break;
      }

      // 收集一批需要进行迁移的key，放到 migrating_batch_ 数组中
      bool ret = ElectMigrateKeys(slot_);
      if (!ret) {
        LOG(WARNING) << "Slots migrating sender get batch keys error";
        is_migrating_ = false;
        set_should_stop();
        break;
      } else if (remained_keys_num_ == 0) {
        set_should_stop();
        break;
      }

      std::string slotKey = SlotKeyPrefix+std::to_string(slot_num_);
      std::vector<std::pair<const char, std::string>>::const_iterator iter;

      // 循环进行迁移
      while (is_migrating_) {
        std::lock_guard lm(slotsmgrt_cond_mutex_);
        {
          std::lock_guard lb(rwlock_batch_);
          std::lock_guard lo(rwlock_ones_);
          int32_t card = 0;
          // 返回集合中的元素数量
          rocksdb::Status s = slot_->db()->SCard(slotKey, &card);
          remained_keys_num_ = card + migrating_batch_.size() + migrating_ones_.size();
          // add ones to batch end; empty ones
          std::copy (migrating_ones_.begin(), migrating_ones_.end(), std::back_inserter(migrating_batch_));
          if (migrating_batch_.size() != 0) {
            moved_keys_num_ = 0;
          }
          std::vector<std::pair<const char, std::string>>().swap(migrating_ones_);
        }

        iter = migrating_batch_.begin();
        while (iter != migrating_batch_.end()) {
          size_t j = 0;
          std::lock_guard lb(rwlock_batch_);
          for (int r; iter != migrating_batch_.end() && (j < asyncRecvsNum); iter++) {
            // 逐个 key 进行迁移
            if ((r = MigrateOneKey(cli_, iter->second, iter->first, true, slot_)) < 0){
              LOG(WARNING) << "Migrate batch key: " << iter->second << " error: ";
              is_migrating_ = false;
              set_should_stop();
              error_ = true;
              break;
            } else {
              j += r;
            }
            moved_keys_num_++;
            moved_keys_all_++;
            remained_keys_num_--;
          }
          if (error_) {
            break;
          }
          for (; j>0; j--) {
            pstd::Status s;
            s = cli_->Recv(NULL);
            if (!s.ok()) {
              LOG(WARNING) << "Migrate batch Recv error";
              is_migrating_ = false;
              set_should_stop();
              error_ = true;
              break;
            }
          }
        }

        if (error_) {
          break;
        }

        for (const auto& item : migrating_batch_) {
          DeleteKey(item.second, item.first, slot_);
        }

        {
          std::lock_guard lb(rwlock_batch_);
          std::vector<std::pair<const char, std::string>>().swap(migrating_batch_);
        }

        if (remained_keys_num_ == 0) {
          LOG(INFO) << "Migrate slot: " << slot_num_ << " finished";
          is_migrating_ = false;
          set_should_stop();
          break;
        }
      }
    } else {
      LOG(WARNING) << "Slots Migrate Sender Connect server(" << dest_ip_ << ":" << dest_port_ << ") error";
      moved_keys_num_ = -1;
      is_migrating_ = false;
      set_should_stop();
      error_ = true;
    }
  }

  cli_->Close();
  if (error_) {
    sleep(1);
  }

  LOG(INFO) << "SlotsMgrtSender thread " << thread_id() << " finished!";
  return NULL;
}

void SlotsMgrtAsyncCancelCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsMgrtAsyncCancel);
  }
  return;
}

void SlotsMgrtAsyncCancelCmd::Do(std::shared_ptr<Slot>slot) {
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

void SlotsDelCmd::Do(std::shared_ptr<Slot>slot) {
  std::vector<std::string> keys;
  std::vector<std::string>::const_iterator iter;
  for (iter = slots_.begin(); iter != slots_.end(); iter++){
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

  std::vector<std::string>::const_iterator iter = argv_.begin();
  keys_.assign(argv_.begin(), argv_.end());
  return;
}

void SlotsHashKeyCmd::Do(std::shared_ptr<Slot>slot) {
  std::vector<std::string>::const_iterator keys_it;

  res_.AppendArrayLen(keys_.size());
  for (keys_it = keys_.begin(); keys_it != keys_.end(); ++keys_it) {
    res_.AppendInteger(GetSlotsNum(*keys_it, NULL, NULL));;
  }

  return;
}