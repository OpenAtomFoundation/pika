// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <algorithm>
#include "slash/include/slash_string.h"
#include "slash/include/slash_status.h"
#include "include/pika_conf.h"
#include "include/pika_slot.h"
#include "include/pika_server.h"

#define min(a, b)  (((a) > (b)) ? (b) : (a))

extern PikaServer *g_pika_server;
extern PikaConf *g_pika_conf;

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

// get key slot number
int SlotNum(const std::string &str) {
    uint32_t crc = CRC32Update(0, str.data(), (int)str.size());
    return (int)(crc & HASH_SLOTS_MASK);
}

// add key to slotkey
void SlotKeyAdd(const std::string type, const std::string key) {
    if (g_pika_conf->slotmigrate() != true){
        return;
    }
    int32_t count = 0;
    std::vector<std::string> members(1, type + key);
    std::string slotKey = SlotKeyPrefix + std::to_string(SlotNum(key)); 
    rocksdb::Status s = g_pika_server->db()->SAdd(slotKey, members, &count);
    if (!s.ok()) {
        LOG(WARNING) << "SAdd key: " << key << " to slotKey, error: " << s.ToString();
    }
}

int KeyType(const std::string key, std::string &key_type) {
    std::string type_str;
    rocksdb::Status s = g_pika_server->db()->Type(key, &type_str);
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

//del key from slotkey
void SlotKeyRem(const std::string key) {
    if (g_pika_conf->slotmigrate() != true){
        return;
    }
    std::string type;
    if (KeyType(key, type) < 0){
        LOG(WARNING) << "SRem key: " << key << " from slotKey error";
        return;
    }
    std::string slotKey = SlotKeyPrefix + std::to_string(SlotNum(key)); 
    int32_t count = 0;
    std::vector<std::string> members(1, type + key);
    rocksdb::Status s = g_pika_server->db()->SRem(slotKey, members, &count);
    if (!s.ok()) {
        LOG(WARNING) << "SRem key: " << key << " from slotKey, error: " << s.ToString();
        return;
    }
}

//check key exists
void KeyNotExistsRem(const std::string type, const std::string key) {
    if (g_pika_conf->slotmigrate() != true){
        return;
    }
    std::vector<std::string> vkeys;
    vkeys.push_back(key);
    std::map<blackwidow::DataType, rocksdb::Status> type_status;
    int64_t res = g_pika_server->db()->Exists(vkeys, &type_status);
    if (res == 0) {
        std::string slotKey = SlotKeyPrefix + std::to_string(SlotNum(key)); 
        std::vector<std::string> members(1, type + key);
        int32_t count = 0;
        rocksdb::Status s = g_pika_server->db()->SRem(slotKey, members, &count);
        if (!s.ok()) {
            LOG(WARNING) << "Zrem key: " << key << " from slotKey, error: " << s.ToString();
            return;
        }
    }
    return;
}

//do migrate key to dest pika server
static int doMigrate(pink::PinkCli *cli, std::string send_str){
    slash::Status s;
    s = cli->Send(&send_str);
    if (!s.ok()) {
        LOG(WARNING) << "Slot Migrate Send error: " << s.ToString();
        return -1;
    }
    return 1;
}

//do migrate cli auth
static int doAuth(pink::PinkCli *cli){
    pink::RedisCmdArgsType argv;
    std::string wbuf_str;
    std::string requirepass = g_pika_conf->requirepass();
    if (requirepass != "") {
        argv.push_back("auth");
        argv.push_back(requirepass);
        pink::SerializeRedisCommand(argv, &wbuf_str);

        slash::Status s;
        s = cli->Send(&wbuf_str);
        if (!s.ok()) {
            LOG(WARNING) << "Slot Migrate auth error: " << s.ToString();
            return -1;
        }
        // Recv
        s = cli->Recv(NULL);
        if (!s.ok()) {
            LOG(WARNING) << "Slot Migrate auth Recv error: " << s.ToString();
            return -1;
        }
    }
    return 0;
}

// get kv key value
static int kvGet(const std::string key, std::string &value){
    rocksdb::Status s = g_pika_server->db()->Get(key, &value);
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


// delete key from db
int KeyDelete(const std::string key, const char key_type){
    std::map<const char, blackwidow::DataType> map = {
            {'k', blackwidow::kStrings},
            {'h', blackwidow::kHashes},
            {'l', blackwidow::kLists},
            {'s', blackwidow::kSets},
            {'z', blackwidow::kZSets},
    };
    std::vector<std::string> keys(1, key);
    int64_t count = g_pika_server->db()->DelByType(keys, map[key_type]);
    if (count == 0) {
        LOG(WARNING) << "Del key: " << key << " at slot " << SlotNum(key) << " not found ";
        return 0;
    } else if (count < 0) {
        LOG(WARNING) << "Del key: " << key << " at slot " << SlotNum(key) << " error";
        return -1;
    }
    return 1;
}

static int migrateKeyTTl(pink::PinkCli *cli, const std::string key){
    pink::RedisCmdArgsType argv;
    std::string send_str;
    std::map<blackwidow::DataType, rocksdb::Status> type_status;
    std::map<blackwidow::DataType, int64_t> type_timestamp;
    type_timestamp = g_pika_server->db()->TTL(key, &type_status);

    for (const auto& item : type_timestamp) {
        // mean operation exception errors happen in database
        if (item.second == -3) {
            return 0;
        }
    }

    int64_t ttl = 0;
    if (type_timestamp[blackwidow::kStrings] != -2) {
        ttl = type_timestamp[blackwidow::kStrings];
    } else if (type_timestamp[blackwidow::kHashes] != -2) {
        ttl = type_timestamp[blackwidow::kHashes];
    } else if (type_timestamp[blackwidow::kLists] != -2) {
        ttl = type_timestamp[blackwidow::kLists];
    } else if (type_timestamp[blackwidow::kSets] != -2) {
        ttl = type_timestamp[blackwidow::kSets];
    } else if (type_timestamp[blackwidow::kZSets] != -2) {
        ttl = type_timestamp[blackwidow::kZSets];
    } else {
        // mean this key not exist
        return 0;
    }

    if (ttl > 0){
        argv.push_back("expire");
        argv.push_back(key);
        argv.push_back(std::to_string(ttl));
        pink::SerializeRedisCommand(argv, &send_str);
        if (doMigrate(cli, send_str) < 0){
            return -1;
        }
        return 1;
    }
    return 0;
}

// migrate one kv key
static int migrateKv(pink::PinkCli *cli, const std::string key){
    int r, ret = 0;
    std::string value;
    if (kvGet(key, value) < 0){
        return -1;
    }
    if (value==""){
        return 0;
    }

    pink::RedisCmdArgsType argv;
    std::string send_str;
    argv.push_back("set");
    argv.push_back(key);
    argv.push_back(value);
    pink::SerializeRedisCommand(argv, &send_str);

    if (doMigrate(cli, send_str) < 0){
        return -1;
    } else {
        ret++;
    }

    if ((r = migrateKeyTTl(cli, key)) < 0){
        return -1;
    } else {
        ret += r;
    }

    KeyDelete(key, 'k'); //key already been migrated successfully, del error doesn't matter
    return ret;
}

//get all hash field and values
static int hashGetall(const std::string key, std::vector<blackwidow::FieldValue> *fvs){
    rocksdb::Status s = g_pika_server->db()->HGetall(key, fvs);
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
static int migrateHash(pink::PinkCli *cli, const std::string key){
    int r, ret = 0;
    std::vector<blackwidow::FieldValue> fvs;
    if (hashGetall(key, &fvs) < 0){
        return -1;
    }
    size_t keySize = fvs.size();
    if (keySize == 0){
        return 0;
    }

    pink::RedisCmdArgsType argv;
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
        pink::SerializeRedisCommand(argv, &send_str);
        if (doMigrate(cli, send_str) < 0){
            return -1;
        } else {
            ret++;
        }
    }

    if ((r = migrateKeyTTl(cli, key)) < 0){
        return -1;
    } else {
        ret += r;
    }

    KeyDelete(key, 'h'); //key already been migrated successfully, del error doesn't matter
    return ret;
}

// get list key all values
static int listGetall(const std::string key, std::vector<std::string> *values){
    rocksdb::Status s = g_pika_server->db()->LRange(key, 0, -1, values);
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

// migrate one list key
static int migrateList(pink::PinkCli *cli, const std::string key){
    int r, ret = 0;
    std::vector<std::string> values;
    if (listGetall(key, &values) < 0){
        return -1;
    }
    size_t keySize = values.size();
    if (keySize == 0){
        return 0;
    }

    pink::RedisCmdArgsType argv;
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
        pink::SerializeRedisCommand(argv, &send_str);
        if (doMigrate(cli, send_str) < 0){
            return -1;
        } else {
            ret++;
        }
    }

    if ((r = migrateKeyTTl(cli, key)) < 0){
        return -1;
    } else {
        ret += r;
    }

    KeyDelete(key, 'l'); //key already been migrated successfully, del error doesn't matter
    return ret;
}

// get set key all values
static int setGetall(const std::string key, std::vector<std::string> *members){
    rocksdb::Status s = g_pika_server->db()->SMembers(key, members);
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
static int migrateSet(pink::PinkCli *cli, const std::string key){
    int r, ret = 0;
    std::vector<std::string> members;
    if (setGetall(key, &members) < 0){
        return -1;
    }
    size_t keySize = members.size();
    if (keySize == 0){
        return 0;
    }

    pink::RedisCmdArgsType argv;
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
        pink::SerializeRedisCommand(argv, &send_str);
        if (doMigrate(cli, send_str) < 0){
            return -1;
        } else {
            ret++;
        }
    }

    if ((r = migrateKeyTTl(cli, key)) < 0){
        return -1;
    } else {
        ret += r;
    }

    KeyDelete(key, 's'); //key already been migrated successfully, del error doesn't matter
    return ret;
}

// get one zset key all values
static int zsetGetall(const std::string key, std::vector<blackwidow::ScoreMember> *score_members){
    rocksdb::Status s = g_pika_server->db()->ZRange(key, 0, -1, score_members);
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
static int migrateZset(pink::PinkCli *cli, const std::string key){
    int r, ret = 0;
    std::vector<blackwidow::ScoreMember> score_members;
    if (zsetGetall(key, &score_members) < 0){
        return -1;
    }
    size_t keySize = score_members.size();
    if (keySize == 0){
        return 0;
    }

    pink::RedisCmdArgsType argv;
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
        pink::SerializeRedisCommand(argv, &send_str);
        if (doMigrate(cli, send_str) < 0){
            return -1;
        } else {
            ret++;
        }
    }

    if ((r = migrateKeyTTl(cli, key)) < 0){
        return -1;
    } else {
        ret += r;
    }

    KeyDelete(key, 'z');
    return ret;
}

// migrate key
static int MigrateOneKey(pink::PinkCli *cli, const std::string key, const char key_type){
    int ret;
    switch (key_type) {
        case 'k':
            if ((ret = migrateKv(cli, key)) < 0){
                SlotKeyAdd("k", key);
                return -1;
            }
            break;
        case 'h':
            if ((ret = migrateHash(cli, key)) < 0){
                SlotKeyAdd("h", key);
                return -1;
            }
            break;
        case 'l':
            if ((ret = migrateList(cli, key)) < 0){
                SlotKeyAdd("l", key);
                return -1;
            }
            break;
        case 's':
            if ((ret = migrateSet(cli, key)) < 0){
                SlotKeyAdd("s", key);
                return -1;
            }
            break;
        case 'z':
            if ((ret = migrateZset(cli, key)) < 0){
                SlotKeyAdd("z", key);
                return -1;
            }
            break;
        default:
            return -1;
            break;
    }
    return ret;
}

// check slotkey remaind keys number
static void SlotKeyLenCheck(const std::string slotKey, CmdRes& res){
    int32_t card = 0;
    rocksdb::Status s = g_pika_server->db()->SCard(slotKey, &card);
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

void SlotsMgrtTagSlotCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsMgrtTagSlot);
        return;
    }
    PikaCmdArgsType::iterator it = argv.begin() + 1; //Remember the first args is the opt name
    dest_ip_ = slash::StringToLower(*it++);

    std::string str_dest_port = *it++;
    if (!slash::string2l(str_dest_port.data(), str_dest_port.size(), &dest_port_) || dest_port_ <= 0) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
    }

    if ((dest_ip_ == "127.0.0.1" || dest_ip_ == g_pika_server->host()) && dest_port_ == g_pika_server->port()) {
        res_.SetRes(CmdRes::kErrOther, "destination address error");
        return;
    }

    std::string str_timeout_ms = *it++;
    if (!slash::string2l(str_timeout_ms.data(), str_timeout_ms.size(), &timeout_ms_) || timeout_ms_ <= 0) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
    }

    std::string str_slot_num = *it++;
    if (!slash::string2l(str_slot_num.data(), str_slot_num.size(), &slot_num_) || slot_num_ < 0) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
    }
}

// pop one key from slotkey
int SlotsMgrtTagSlotCmd::SlotKeyPop(){
    std::string slotKey = SlotKeyPrefix+std::to_string(slot_num_);
    std::string member;
    rocksdb::Status s = g_pika_server->db()->SPop(slotKey, &member);
    if (!s.ok()) {
        LOG(WARNING) << "Migrate slot: " << slot_num_ << " error: " << s.ToString();
        res_.AppendArrayLen(2);
        res_.AppendInteger(0);
        res_.AppendInteger(0);
        return -1;
    }

    key_type_ = member.at(0);
    key_ = member;
    key_.erase(key_.begin());

    return 0;
}

void SlotsMgrtTagSlotCmd::Do() {
    if (SlotKeyPop() < 0){
      return;
    }

    pink::PinkCli *cli = pink::NewRedisCli();
    cli->set_connect_timeout(timeout_ms_);
    if ((cli->Connect(dest_ip_, dest_port_, g_pika_server->host())).ok()) {
        cli->set_send_timeout(timeout_ms_);
        cli->set_recv_timeout(timeout_ms_);
        if (doAuth(cli) < 0) {
            res_.SetRes(CmdRes::kErrOther, "Slot Migrate auth destination server error");
            cli->Close();
            delete cli;
            return;
        }

        if (MigrateOneKey(cli, key_, key_type_) < 0){
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
    SlotKeyLenCheck(slotKey, res_);
    return;
}

void SlotsMgrtTagOneCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsMgrtTagOne);
        return;
    }
    PikaCmdArgsType::iterator it = argv.begin() + 1; //Remember the first args is the opt name
    dest_ip_ = slash::StringToLower(*it++);

    std::string str_dest_port = *it++;
    if (!slash::string2l(str_dest_port.data(), str_dest_port.size(), &dest_port_) || dest_port_ <= 0) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
    }

    if ((dest_ip_ == "127.0.0.1" || dest_ip_ == g_pika_server->host()) && dest_port_ == g_pika_server->port()) {
        res_.SetRes(CmdRes::kErrOther, "destination address error");
        return;
    }

    std::string str_timeout_ms = *it++;
    if (!slash::string2l(str_timeout_ms.data(), str_timeout_ms.size(), &timeout_ms_) || timeout_ms_ <= 0) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
    }

    key_ = slash::StringToLower(*it++);

    slot_num_ = SlotNum(key_);
}

// check key type
int SlotsMgrtTagOneCmd::KeyTypeCheck() {
    std::string type_str;
    rocksdb::Status s = g_pika_server->db()->Type(key_, &type_str);
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
int SlotsMgrtTagOneCmd::SlotKeyRemCheck(){
    std::string slotKey = SlotKeyPrefix+std::to_string(slot_num_);
    std::string tkey = std::string(1,key_type_) + key_;
    std::vector<std::string> members(1, tkey);
    int32_t count = 0;
    rocksdb::Status s = g_pika_server->db()->SRem(slotKey, members, &count);
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

void SlotsMgrtTagOneCmd::Do() {
    if (KeyTypeCheck() < 0){
        return;
    }
    if (SlotKeyRemCheck() < 0){
        return;
    }

    pink::PinkCli *cli = pink::NewRedisCli();
    cli->set_connect_timeout(timeout_ms_);
    if ((cli->Connect(dest_ip_, dest_port_, g_pika_server->host())).ok()) {
        cli->set_send_timeout(timeout_ms_);
        cli->set_recv_timeout(timeout_ms_);
        if (doAuth(cli) < 0) {
            res_.SetRes(CmdRes::kErrOther, "Slot Migrate auth destination server error");
            cli->Close();
            delete cli;
            return;
        }

        if (MigrateOneKey(cli, key_, key_type_) < 0){
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

void SlotsInfoCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsInfo);
    }
    return;
}

void SlotsInfoCmd::Do() {
    std::map<int64_t,int64_t> slotsMap;
    for (int i = 0; i < HASH_SLOTS_SIZE; ++i){
        int32_t card = 0;
        rocksdb::Status s = g_pika_server->db()->SCard(SlotKeyPrefix+std::to_string(i), &card);
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

void SlotsHashKeyCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsHashKey);
    }
    std::vector<std::string>::iterator iter = argv.begin();
    keys_.assign(++iter, argv.end());
    return;
}

void SlotsHashKeyCmd::Do() {
    res_.AppendArrayLen(keys_.size());
    std::vector<std::string>::const_iterator iter;
    for (iter = keys_.begin(); iter != keys_.end(); iter++){
        res_.AppendInteger(SlotNum(*iter));
    }
    return;
}

void SlotsReloadCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsReload);
    }
    return;
}

void SlotsReloadCmd::Do() {
    g_pika_server->Bgslotsreload();
    const PikaServer::BGSlotsReload& info = g_pika_server->bgslots_reload();
    char buf[256];
    snprintf(buf, sizeof(buf), "+%s : %lu", 
        info.s_start_time.c_str(), g_pika_server->GetSlotsreloadingCursor());
    res_.AppendContent(buf);
    return;
}

void SlotsReloadOffCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsReloadOff);
    }
    return;
}

void SlotsReloadOffCmd::Do() {
    g_pika_server->StopBgslotsreload();
    res_.SetRes(CmdRes::kOk);
    return;
}

void SlotsCleanupCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsCleanup);
    }

    PikaCmdArgsType::iterator iter = argv.begin() + 1; //Remember the first args is the opt name
    std::string slot;
    long slotLong;
    std::vector<int> slots;
    for (; iter != argv.end(); iter++){
        slot = *iter;
        if (!slash::string2l(slot.data(), slot.size(), &slotLong) || slotLong < 0) {
            res_.SetRes(CmdRes::kInvalidInt);
            return;
        }
        slots.push_back(int(slotLong));
    }
    cleanup_slots_.swap(slots);
    return;
}

void SlotsCleanupCmd::Do() {
    g_pika_server->Bgslotscleanup(cleanup_slots_);
    std::vector<int> cleanup_slots(g_pika_server->GetCleanupSlots());
    res_.AppendArrayLen(cleanup_slots.size());
    std::vector<int>::const_iterator iter = cleanup_slots.begin();
    for (; iter != cleanup_slots.end(); iter++){
        res_.AppendInteger(*iter);
    }
    return;
}

void SlotsCleanupOffCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsCleanupOff);
    }
    return;
}

void SlotsCleanupOffCmd::Do() {
    g_pika_server->StopBgslotscleanup();
    res_.SetRes(CmdRes::kOk);
    return;
}

void SlotsDelCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsDel);
    }
    std::vector<std::string>::iterator iter = argv.begin();
    slots_.assign(++iter, argv.end());
    return;
}

void SlotsDelCmd::Do() {
    std::vector<std::string> keys;
    std::vector<std::string>::const_iterator iter;
    for (iter = slots_.begin(); iter != slots_.end(); iter++){
        keys.push_back(SlotKeyPrefix + *iter);
    }
    std::map<blackwidow::DataType, rocksdb::Status> type_status;
    int64_t count = g_pika_server->db()->Del(keys, &type_status);
    if (count >= 0) {
        res_.AppendInteger(count);
    } else {
        res_.SetRes(CmdRes::kErrOther, "SlotsDel error");
    }
    return;
}

void SlotsScanCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsScan);
        return;
    }
    key_ = SlotKeyPrefix + argv[1];
    if (!slash::string2l(argv[2].data(), argv[2].size(), &cursor_)) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsScan);
        return;
    }
    size_t argc = argv.size(), index = 3;
    while (index < argc) {
        std::string opt = slash::StringToLower(argv[index]); 
        if (opt == "match" || opt == "count") {
            index++;
            if (index >= argc) {
                res_.SetRes(CmdRes::kSyntaxErr);
                return;
            }
            if (opt == "match") {
                pattern_ = argv[index];
            } else if (!slash::string2l(argv[index].data(), argv[index].size(), &count_)) {
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
    int64_t next_cursor = 0;
    std::vector<std::string> members;
    rocksdb::Status s = g_pika_server->db()->SScan(key_, cursor_, pattern_, count_, &members, &next_cursor);

    if (s.ok() || s.IsNotFound()) {
        res_.AppendContent("*2");
        char buf[32];
        int64_t len = slash::ll2string(buf, sizeof(buf), next_cursor);
        res_.AppendStringLen(len);
        res_.AppendContent(buf);

        res_.AppendArrayLen(members.size());

        for (const auto& member : members) {
            res_.AppendString(member);
        }
    } else {
        res_.SetRes(CmdRes::kErrOther, s.ToString());
    }
    return;
}

void SlotsMgrtTagSlotAsyncCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsMgrtTagSlotAsync);
    }
    PikaCmdArgsType::iterator it = argv.begin() + 1; //Remember the first args is the opt name
    dest_ip_ = slash::StringToLower(*it++);

    std::string str_dest_port = *it++;
    if (!slash::string2l(str_dest_port.data(), str_dest_port.size(), &dest_port_) || dest_port_ <= 0) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
    }

    if ((dest_ip_ == "127.0.0.1" || dest_ip_ == g_pika_server->host()) && dest_port_ == g_pika_server->port()) {
        res_.SetRes(CmdRes::kErrOther, "destination address error");
        return;
    }

    std::string str_timeout_ms = *it++;
    if (!slash::string2l(str_timeout_ms.data(), str_timeout_ms.size(), &timeout_ms_) || timeout_ms_ <= 0) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
    }

    std::string str_max_bulks = *it++;
    if (!slash::string2l(str_max_bulks.data(), str_max_bulks.size(), &max_bulks_) || max_bulks_ <= 0) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
    }

    std::string str_max_bytes_ = *it++;
    if (!slash::string2l(str_max_bytes_.data(), str_max_bytes_.size(), &max_bytes_) || max_bytes_ <= 0) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
    }

    std::string str_slot_num = *it++;
    if (!slash::string2l(str_slot_num.data(), str_slot_num.size(), &slot_num_) || slot_num_ < 0 || slot_num_ >= HASH_SLOTS_SIZE) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
    }

    std::string str_keys_num = *it++;
    if (!slash::string2l(str_keys_num.data(), str_keys_num.size(), &keys_num_) || keys_num_ < 0) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
    }
    return;
}

void SlotsMgrtTagSlotAsyncCmd::Do() {
    bool ret = g_pika_server->SlotsMigrateBatch(dest_ip_, dest_port_, timeout_ms_, slot_num_, keys_num_);
    if (!ret) {
        LOG(WARNING) << "Slot batch migrate keys error";
        res_.SetRes(CmdRes::kErrOther, "Slot batch migrating keys error");
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

void SlotsMgrtExecWrapperCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsMgrtExecWrapper);
    }
    PikaCmdArgsType::iterator it = argv.begin() + 1;
    key_ = slash::StringToLower(*it++);
    return;
}

void SlotsMgrtExecWrapperCmd::Do() {
    res_.AppendArrayLen(2);
    int ret = g_pika_server->SlotsMigrateOne(key_);
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

void SlotsMgrtAsyncStatusCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsMgrtAsyncStatus);
    }
    return;
}

void SlotsMgrtAsyncStatusCmd::Do() {
    std::string status;
    std::string ip;
    int64_t port, slot, moved, remained;
    bool migrating;
    g_pika_server->GetSlotsMgrtSenderStatus(&ip, &port, &slot, &migrating, &moved, &remained);
    std::string mstatus = migrating ? "yes" : "no";
    res_.AppendArrayLen(5);
    status = "dest server: " + ip + ":" + std::to_string(port);
    res_.AppendStringLen(status.size());
    res_.AppendContent(status);
    status = "slot number: " + std::to_string(slot);
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

void SlotsMgrtAsyncCancelCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
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

SlotsMgrtSenderThread::SlotsMgrtSenderThread() :
    pink::Thread(),
    dest_ip_("none"),
    dest_port_(-1),
    timeout_ms_(3000),
    slot_num_(-1),
    moved_keys_num_(-1),
    moved_keys_all_(-1),
    remained_keys_num_(-1),
    slotsmgrt_cond_(&slotsmgrt_cond_mutex_),
    is_migrating_(false) {
        cli_ = pink::NewRedisCli();
        pthread_rwlockattr_t attr;
        pthread_rwlockattr_init(&attr);
        pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
        pthread_rwlock_init(&rwlock_db_, &attr);
        pthread_rwlock_init(&rwlock_batch_, &attr);
        pthread_rwlock_init(&rwlock_ones_, &attr);
}

SlotsMgrtSenderThread::~SlotsMgrtSenderThread() {
    if (is_migrating_) {
        slotsmgrt_cond_.SignalAll();
    }
    if (is_running()) {
        StopThread();
    }
    is_migrating_ = false;
    set_should_stop();
    pthread_rwlock_destroy(&rwlock_db_);
    pthread_rwlock_destroy(&rwlock_batch_);
    pthread_rwlock_destroy(&rwlock_ones_);

    delete cli_;
    LOG(INFO) << "SlotsMgrtSender thread " << thread_id() << " exit!";
}

int SlotsMgrtSenderThread::SlotsMigrateOne(const std::string &key){
    slash::RWLock l(&rwlock_db_, true);
    slash::RWLock lb(&rwlock_batch_, false);
    slash::RWLock lo(&rwlock_ones_, true);
    int slot_num = SlotNum(key);
    std::string type_str;
    char key_type;
    rocksdb::Status s = g_pika_server->db()->Type(key, &type_str);
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
    s = g_pika_server->db()->SRem(slotKey, members, &remained);
    if (s.IsNotFound()) {
        // s.IsNotFound() is also error, key found in db but not in slotkey, run slotsreload cmd first
        LOG(WARNING) << "Migrate key: " << key << " SRem from slotkey error: " << s.ToString();
        return 0;
    }

    migrating_ones_.push_back(kpair);
    return 1;
}

bool SlotsMgrtSenderThread::SlotsMigrateBatch(const std::string &ip, int64_t port, int64_t time_out, int64_t slot, int64_t keys_num){
    slash::MutexLock lm(&slotsmgrt_cond_mutex_);
    // if is_migrating_, does some check and prepare the keys to be migrated 
    if (is_migrating_){
        if (!(dest_ip_==ip && dest_port_==port && slot_num_==slot)) {
            LOG(WARNING) << "wrong dest_ip, dest_port or slot_num";
            return false;
        }
        timeout_ms_ = time_out;
        keys_num_ = keys_num;
        bool ret = ElectMigrateKeys();
        if (!ret) {
            LOG(WARNING) << "Slots migrating sender get batch keys error";
            is_migrating_ = false;
            set_should_stop();
            return false;
        }
        return true;
    // if not migrating, prepare the keys and start thread migrating
    } else {
        // set state and start a new sender
        dest_ip_ = ip;
        dest_port_ = port;
        timeout_ms_ = time_out;
        slot_num_ = slot;
        keys_num_ = keys_num;
        is_migrating_ = true;
        bool ret = ElectMigrateKeys();
        if (!ret) {
            LOG(WARNING) << "Slots migrating sender get batch keys error";
            is_migrating_ = false;
            return false;
        } else if (remained_keys_num_ != 0) {
            StartThread();
            LOG(INFO) << "Migrate batch slot: " << slot;
        }
    }
    return true;
}

bool SlotsMgrtSenderThread::GetSlotsMigrateResult(int64_t *moved, int64_t *remained){
    slash::MutexLock lm(&slotsmgrt_cond_mutex_);
    slotsmgrt_cond_.TimedWait(timeout_ms_);
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

void SlotsMgrtSenderThread::GetSlotsMgrtSenderStatus(std::string *ip, int64_t *port, int64_t *slot, bool *migrating, int64_t *moved, int64_t *remained){
    slash::RWLock l(&rwlock_db_, false);
    slash::RWLock lb(&rwlock_batch_, false);
    slash::RWLock lo(&rwlock_ones_, false);
    *ip = dest_ip_;
    *port = dest_port_;
    *slot = slot_num_;
    *migrating = is_migrating_;
    *moved = moved_keys_all_;
    *remained = remained_keys_num_;
}

bool SlotsMgrtSenderThread::SlotsMigrateAsyncCancel() {
    slash::RWLock l(&rwlock_db_, true);
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
    slash::RWLock lb(&rwlock_batch_, true);
    slash::RWLock lo(&rwlock_ones_, true);
    std::vector<std::pair<const char, std::string>>().swap(migrating_batch_);
    std::vector<std::pair<const char, std::string>>().swap(migrating_ones_);
    return true;
}

bool SlotsMgrtSenderThread::ElectMigrateKeys(){
    slash::RWLock l(&rwlock_db_, true);
    slash::RWLock lb(&rwlock_batch_, true);
    std::string slotKey = SlotKeyPrefix+std::to_string(slot_num_);
    int32_t card = 0;
    rocksdb::Status s = g_pika_server->db()->SCard(slotKey, &card);
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

    s = g_pika_server->db()->SScan(slotKey, cursor, pattern, keys_num_, &members, &next_cursor);

    if (s.ok()) {
        for (const auto& member : members) {
            key_type = member.at(0);
            key = member;
            key.erase(key.begin());
            migrating_batch_.push_back(std::make_pair(key_type, key));
            int32_t count = 0;
            rocksdb::Status s = g_pika_server->db()->SRem(slotKey, std::vector<std::string>(1, member), &count);
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
    slash::Status result;
    cli_->set_connect_timeout(timeout_ms_);
    moved_keys_all_ = 0;
    while (!should_stop()) {
        result = cli_->Connect(dest_ip_, dest_port_, g_pika_server->host());
        LOG(INFO) << "Slots Migrate Sender Connect server(" << dest_ip_ << ":" << dest_port_ << ") " << result.ToString();

        if (result.ok()) {
            cli_->set_send_timeout(timeout_ms_);
            cli_->set_recv_timeout(timeout_ms_);
            if (doAuth(cli_) < 0) {
                slotsmgrt_cond_.Signal();
                is_migrating_ = false;
                set_should_stop();
                break;
            }

            std::string slotKey = SlotKeyPrefix+std::to_string(slot_num_);
            std::vector<std::pair<const char, std::string>>::const_iterator iter;
            while (is_migrating_) {
                slash::MutexLock lm(&slotsmgrt_cond_mutex_);
                {
                    slash::RWLock lb(&rwlock_batch_, true);
                    slash::RWLock lo(&rwlock_ones_, true);
                    int32_t card = 0;
                    rocksdb::Status s = g_pika_server->db()->SCard(slotKey, &card);
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
                    slash::RWLock lb(&rwlock_batch_, false);
                    for (int r; iter != migrating_batch_.end() && (j < asyncRecvsNum); iter++) {
                        if ((r = MigrateOneKey(cli_, iter->second, iter->first)) < 0){
                            LOG(WARNING) << "Migrate batch key: " << iter->second << " error: ";
                            slotsmgrt_cond_.Signal();
                            is_migrating_ = false;
                            set_should_stop();
                            break;
                        } else {
                            j += r;
                        }
                        moved_keys_num_++;
                        moved_keys_all_++;
                        remained_keys_num_--;
                    }
                    for (; j>0; j--) {
                        slash::Status s;
                        s = cli_->Recv(NULL);
                        if (!s.ok()) {
                            break;
                        }
                    }
                }

                {
                    slash::RWLock lb(&rwlock_batch_, true);
                    std::vector<std::pair<const char, std::string>>().swap(migrating_batch_);
                }

                if (remained_keys_num_ == 0) {
                    LOG(INFO) << "Migrate slot: " << slot_num_ << " finished";
                    slotsmgrt_cond_.Signal();
                    is_migrating_ = false;
                    set_should_stop();
                    break;
                }
                slotsmgrt_cond_.Signal();
            }
        } else {
            LOG(WARNING) << "Slots Migrate Sender Connect server(" << dest_ip_ << ":" << dest_port_ << ") error";
            moved_keys_num_ = -1;
            is_migrating_ = false;
            set_should_stop();
            slotsmgrt_cond_.Signal();
        }
    }

    cli_->Close();
    sleep(1);

    LOG(INFO) << "SlotsMgrtSender thread " << thread_id() << " finished!";
    return NULL;
}
