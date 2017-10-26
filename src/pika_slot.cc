// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "slash/include/slash_string.h"
#include "slash/include/slash_status.h"
#include "nemo.h"
#include "pika_conf.h"
#include "pika_slot.h"
#include "pika_server.h"

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
    int64_t res = 0;
    std::string slotKey = SlotKeyPrefix + std::to_string(SlotNum(key)); 
    nemo::Status s = g_pika_server->db()->SAdd(slotKey, type+key, &res);
    if (!s.ok()) {
        LOG(WARNING) << "Zadd key: " << key <<" to slotKey, error: " <<strerror(errno);
    }
}

int KeyType(const std::string key, std::string &key_type) {
    std::string type_str;
    nemo::Status s = g_pika_server->db()->Type(key, &type_str);
    if (!s.ok()) {
        LOG(WARNING) << "Get key type error: "<< key <<" " <<strerror(errno);
        key_type = "";
        return -1; 
    }
    if (type_str=="string"){
        key_type = "k";
    }else if (type_str=="hash"){
        key_type = "h";
    }else if (type_str=="list"){
        key_type = "l";
    }else if (type_str=="set"){
        key_type = "s";
    }else if (type_str=="zset"){
        key_type = "z";
    }else{
        LOG(WARNING) << "Get key type error: "<< key <<" " <<strerror(errno);
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
        LOG(WARNING) << "Zrem key: " << key <<" from slotKey, error: " <<strerror(errno);
        return;
    }
    std::string slotKey = SlotKeyPrefix + std::to_string(SlotNum(key)); 
    int64_t count;
    nemo::Status s = g_pika_server->db()->SRem(slotKey, type+key, &count);
    if (!s.ok()) {
        LOG(WARNING) << "Zrem key: " << key <<" from slotKey, error: " <<strerror(errno);
        return;
    }
}

//check key exists
void KeyNotExistsRem(const std::string type, const std::string key) {
    if (g_pika_conf->slotmigrate() != true){
        return;
    }
    int64_t res = -1;
    std::vector<std::string> vkeys;
    vkeys.push_back(key);
    nemo::Status s = g_pika_server->db()->Exists(vkeys, &res);
    if (res == 0) {
        std::string slotKey = SlotKeyPrefix + std::to_string(SlotNum(key)); 
        nemo::Status s = g_pika_server->db()->SRem(slotKey, type+key, &res);
        if (!s.ok()) {
            LOG(WARNING) << "Zrem key: " << key <<" from slotKey, error: " <<strerror(errno);
            return;
        }
    }
    return;
}

//do migrate key to dest pika server
static int doMigrate(pink::PinkCli *cli, const std::string dest_ip, const int64_t dest_port, const std::string send_str){
    cli->set_connect_timeout(3000);
    if ((cli->Connect(dest_ip, dest_port, g_pika_server->host())).ok()) {
        cli->set_send_timeout(30000);
        cli->set_recv_timeout(30000);

        pink::RedisCmdArgsType argv;
        std::string wbuf_str;
        std::string requirepass = g_pika_conf->requirepass();
        if (requirepass != "") {
            argv.push_back("auth");
            argv.push_back(requirepass);
            pink::SerializeRedisCommand(argv, &wbuf_str);
        }

        wbuf_str.append(send_str);

        slash::Status s = cli->Send(&wbuf_str);
        if (!s.ok()) {
            LOG(WARNING) << "slotKvMigrate Send error: " <<strerror(errno);
            return -1;
        }

        cli->Close();
    } else  {
        LOG(WARNING) << "slotKvMigrate Connect destination error: " <<strerror(errno);
        return -1;
    }
    return 0;
}

// get kv key value
static int kvGet(const std::string key, std::string &value){
    nemo::Status s = g_pika_server->db()->Get(key, &value);
    if (!s.ok()) {
        if (s.IsNotFound()) {
            value = "";
            LOG(WARNING) << "Get kv key: "<< key <<" not found ";
            return 0;
        } else {
            value = "";
            LOG(WARNING) << "Get kv key: "<< key <<" error: " <<strerror(errno);
            return -1;
        }
    } 
    return 0;
}

// delete key from db
static int keyDel(const std::string key, const char key_type){
    int64_t count = 0;
    nemo::Status s = g_pika_server->db()->DelSingleType(key, &count, key_type);
    if (!s.ok()) {
        if (s.IsNotFound()) {
            LOG(WARNING) << "Del key: "<< key <<" not found ";
            return 0;
        } else {
            LOG(WARNING) << "Del key: "<< key <<" error: " <<strerror(errno);
            return -1;
        }
    }
    return 1;
}

static int migrateKeyTTl(pink::PinkCli *cli, std::string dest_ip, const int64_t dest_port, const std::string key){
    int64_t ttl = 0;
    pink::RedisCmdArgsType argv;
    std::string send_str;
    nemo::Status s = g_pika_server->db()->TTL(key, &ttl);
    if (s.ok() && (ttl > 0)){
        argv.push_back("expire");
        argv.push_back(key);
        argv.push_back(std::to_string(ttl));
        pink::SerializeRedisCommand(argv, &send_str);
        if (doMigrate(cli, dest_ip, dest_port, send_str) < 0){
            return -1;
        }

    }
    return 0;
}

// migrate one kv key
static int migrateKv(const std::string dest_ip, const int64_t dest_port, const std::string key){
    std::string value;
    if (kvGet(key, value) < 0){
      return -1;
    }
    if (value==""){
      return 0;
    }

    pink::PinkCli *cli = pink::NewRedisCli();
    pink::RedisCmdArgsType argv;
    std::string send_str;
    argv.push_back("set");
    argv.push_back(key);
    argv.push_back(value);
    pink::SerializeRedisCommand(argv, &send_str);

    if (doMigrate(cli, dest_ip, dest_port, send_str) < 0){
      return -1;
    }

    if (migrateKeyTTl(cli, dest_ip, dest_port, key) < 0){
        return -1;
    }

    delete cli;
    keyDel(key, 'k'); //key already been migrated successfully, del error doesn't matter
    return 1;
}

//get all hash field and values
static int hashGetall(const std::string key, std::vector<nemo::FV> &fvs){
    nemo::Status s = g_pika_server->db()->HGetall(key, fvs);
    if (!s.ok()) {
        if (s.IsNotFound()) {
            LOG(WARNING) << "HGetall key: "<< key <<" not found ";
            return 0;
        } else {
            LOG(WARNING) << "HGetall key: "<< key <<" error: " <<strerror(errno);
            return -1;
        }
    }
    return 1;
}

// migrate one hash key
static int migrateHash(const std::string dest_ip, const int64_t dest_port, const std::string key){
    std::vector<nemo::FV> fvs;
    if (hashGetall(key, fvs) < 0){
        return -1;
    }
    size_t keySize = fvs.size();
    if (keySize==0){
        return 0;
    }

    pink::PinkCli *cli = pink::NewRedisCli();
    pink::RedisCmdArgsType argv;
    std::string send_str;
    for (size_t i = 0; i <= keySize/MaxKeySendSize; ++i){
        if (0 < i){
            LOG(WARNING) << "Migrate big key: "<< key <<" size: " << keySize << " migrated value: " << min((i+1)*MaxKeySendSize, keySize);
        }
        argv.clear();
        send_str = "";
        argv.push_back("hmset");
        argv.push_back(key);
        for (size_t j = i*MaxKeySendSize; j < (i+1)*MaxKeySendSize && j < keySize; ++j){
            argv.push_back(fvs[j].field);
            argv.push_back(fvs[j].val);
        }
        pink::SerializeRedisCommand(argv, &send_str);
        if (doMigrate(cli, dest_ip, dest_port, send_str) < 0){
            return -1;
        }
    }

    if (migrateKeyTTl(cli, dest_ip, dest_port, key) < 0){
        return -1;
    }

    delete cli;
    keyDel(key, 'h'); //key already been migrated successfully, del error doesn't matter
    return 1;
}

// get list key all values
static int listGetall(const std::string key, std::vector<nemo::IV> &ivs){
    nemo::Status s = g_pika_server->db()->LRange(key, 0, -1, ivs);
    if (!s.ok()) {
        if (s.IsNotFound()) {
            LOG(WARNING) << "List get key: "<< key <<" value not found ";
            return 0;
        } else {
            LOG(WARNING) << "List get key: "<< key <<" value error: " <<strerror(errno);
            return -1;
        }
    }
    return 1;
}

// migrate one list key
static int migrateList(const std::string dest_ip, const int64_t dest_port, const std::string key){
    std::vector<nemo::IV> ivs;
    if (listGetall(key, ivs) < 0){
        return -1;
    }
    size_t keySize = ivs.size();
    if (keySize==0){
        return 0;
    }

    pink::PinkCli *cli = pink::NewRedisCli();
    pink::RedisCmdArgsType argv;
    std::string send_str;
    for (size_t i = 0; i <= keySize/MaxKeySendSize; ++i){
        if (0 < i){
            LOG(WARNING) << "Migrate big key: "<< key <<" size: " << keySize << " migrated value: " << min((i+1)*MaxKeySendSize, keySize);
        }
        argv.clear();
        send_str = "";
        argv.push_back("lpush");
        argv.push_back(key);
        for (size_t j = i*MaxKeySendSize; j < (i+1)*MaxKeySendSize && j < keySize; ++j){
            argv.push_back(ivs[j].val);
        }
        pink::SerializeRedisCommand(argv, &send_str);
        if (doMigrate(cli, dest_ip, dest_port, send_str) < 0){
            return -1;
        }
    }

    if (migrateKeyTTl(cli, dest_ip, dest_port, key) < 0){
        return -1;
    }

    delete cli;
    keyDel(key, 'l'); //key already been migrated successfully, del error doesn't matter
    return 1;
}

// get set key all values
static int setGetall(const std::string key, std::vector<std::string> &members){
    nemo::Status s = g_pika_server->db()->SMembers(key, members);
    if (!s.ok()) {
        if (s.IsNotFound()) {
            LOG(WARNING) << "Set get key: "<< key <<" value not found ";
            return 0;
        } else {
            LOG(WARNING) << "Set get key: "<< key <<" value error: " <<strerror(errno);
            return -1;
        }
    }
    return 1;
}

// migrate one set key
static int migrateSet(const std::string dest_ip, const int64_t dest_port, const std::string key){
    std::vector<std::string> members;
    if (setGetall(key, members) < 0){
        return -1;
    }
    size_t keySize = members.size();
    if (keySize==0){
        return 0;
    }

    pink::PinkCli *cli = pink::NewRedisCli();
    pink::RedisCmdArgsType argv;
    std::string send_str;
    for (size_t i = 0; i <= keySize/MaxKeySendSize; ++i){
        if (0 < i){
            LOG(WARNING) << "Migrate big key: "<< key <<" size: " << keySize << " migrated value: " << min((i+1)*MaxKeySendSize, keySize);
        }
        argv.clear();
        send_str = "";
        argv.push_back("sadd");
        argv.push_back(key);
        for (size_t j = i*MaxKeySendSize; j < (i+1)*MaxKeySendSize && j < keySize; ++j){
            argv.push_back(members[j]);
        }
        pink::SerializeRedisCommand(argv, &send_str);
        if (doMigrate(cli, dest_ip, dest_port, send_str) < 0){
            return -1;
        }
    }

    if (migrateKeyTTl(cli, dest_ip, dest_port, key) < 0){
        return -1;
    }

    delete cli;
    keyDel(key, 's'); //key already been migrated successfully, del error doesn't matter
    return 1;
}

// get one zset key all values
static int zsetGetall(const std::string key, std::vector<nemo::SM> &sms){
    nemo::Status s = g_pika_server->db()->ZRange(key, 0, -1, sms);
    if (!s.ok()) {
        if (s.IsNotFound()) {
            LOG(WARNING) << "zset get key: "<< key <<" not found ";
            return 0;
        } else {
            LOG(WARNING) << "zset get key: "<< key <<" value error: " <<strerror(errno);
            return -1;
        }
    }
    return 1;
}

// migrate zset key
static int migrateZset(const std::string dest_ip, const int64_t dest_port, const std::string key){
    std::vector<nemo::SM> sms;
    if (zsetGetall(key, sms) < 0){
        return -1;
    }
    size_t keySize = sms.size();
    if (keySize==0){
        return 0;
    }

    pink::PinkCli *cli = pink::NewRedisCli();
    pink::RedisCmdArgsType argv;
    std::string send_str;
    for (size_t i = 0; i <= keySize/MaxKeySendSize; ++i){
        if (0 < i){
            LOG(WARNING) << "Migrate big key: "<< key <<" size: " << keySize << " migrated value: " << min((i+1)*MaxKeySendSize, keySize);
        }
        argv.clear();
        send_str = "";
        argv.push_back("zadd");
        argv.push_back(key);
        for (size_t j = i*MaxKeySendSize; j < (i+1)*MaxKeySendSize && j < keySize; ++j){
            argv.push_back(std::to_string(sms[j].score));
            argv.push_back(sms[j].member);
        }
        pink::SerializeRedisCommand(argv, &send_str);
        if (doMigrate(cli, dest_ip, dest_port, send_str) < 0){
            return -1;
        }
    }

    if (migrateKeyTTl(cli, dest_ip, dest_port, key) < 0){
        return -1;
    }

    delete cli;
    keyDel(key, 'z');
    return 1;
}

// migrate key
static int MigrateOneKey(const std::string dest_ip, const int64_t dest_port, const std::string key, const char key_type, CmdRes& res){
    switch (key_type) {
        case 'k':
            if (migrateKv(dest_ip, dest_port, key) < 0){
                SlotKeyAdd("k", key);
                res.SetRes(CmdRes::kErrOther, "migrate slot error");
                return -1;
            }
            break;
        case 'h':
            if (migrateHash(dest_ip, dest_port, key) < 0){
                SlotKeyAdd("h", key);
                res.SetRes(CmdRes::kErrOther, "migrate slot error");
                return -1;
            }
            break;
        case 'l':
            if (migrateList(dest_ip, dest_port, key) < 0){
                SlotKeyAdd("l", key);
                res.SetRes(CmdRes::kErrOther, "migrate slot error");
                return -1;
            }
            break;
        case 's':
            if (migrateSet(dest_ip, dest_port, key) < 0){
                SlotKeyAdd("s", key);
                res.SetRes(CmdRes::kErrOther, "migrate slot error");
                return -1;
            }
            break;
        case 'z':
            if (migrateZset(dest_ip, dest_port, key) < 0){
                SlotKeyAdd("z", key);
                res.SetRes(CmdRes::kErrOther, "migrate slot error");
                return -1;
            }
            break;
        default:
            res.SetRes(CmdRes::kErrOther, "migrate slot error");
            return -1;
            break;
    }
    return 0;
}

// check slotkey remaind keys number
static void SlotKeyLenCheck(const std::string slotKey, CmdRes& res){
    int len = g_pika_server->db()->SCard(slotKey);
    if (len < 0){
        res.SetRes(CmdRes::kErrOther, "migrate slot kv error");
        res.AppendArrayLen(2);
        res.AppendInteger(1);
        res.AppendInteger(1);
        return;
    }
    res.AppendArrayLen(2);
    res.AppendInteger(1);
    res.AppendInteger(len);
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

// get and del one key from slotkey
int SlotsMgrtTagSlotCmd::SlotKeyPop(){
    std::string slotKey = SlotKeyPrefix+std::to_string(slot_num_);
    std::string tkey;
    nemo::SIterator *iter = g_pika_server->db()->SScan(slotKey, 1);
    if (!iter->Valid()) {
        delete iter;
        LOG(INFO) << "Migrate slot: "<< slot_num_ <<" not found ";
        res_.AppendArrayLen(2);
        res_.AppendInteger(0);
        res_.AppendInteger(0);
        return -1;
    }

    tkey = iter->member();
    key_type_ = tkey.at(0);
    key_ = tkey;
    key_.erase(key_.begin());

    int64_t count;
    nemo::Status s = g_pika_server->db()->SRem(slotKey, tkey, &count);
    if (!s.ok()) {
        LOG(WARNING) << "Zrem key: " << key_ <<" from slotKey, error: " <<strerror(errno);
    }

    delete iter;
    return 0;
}

void SlotsMgrtTagSlotCmd::Do() {
    if (g_pika_conf->slotmigrate() != true){
        LOG(WARNING) << "Not in slotmigrate mode";
        res_.SetRes(CmdRes::kErrOther, "not set slotmigrate");
        return;
    }
    if (SlotKeyPop() < 0){
      return;
    }
    if (MigrateOneKey(dest_ip_, dest_port_, key_, key_type_, res_) < 0){
      return;
    }
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
    nemo::Status s = g_pika_server->db()->Type(key_, &type_str);
    if (!s.ok()) {
        if (s.IsNotFound()) {
            LOG(INFO) << "Migrate slot key "<< key_ <<" not found";
            res_.AppendInteger(0);
        } else {
            LOG(WARNING) << "Migrate slot key: "<< key_ <<" error: " <<strerror(errno);
            res_.SetRes(CmdRes::kErrOther, "migrate slot error");
        }
        return -1; 
    }
    if (type_str=="string"){
        key_type_ = 'k';
    }else if (type_str=="hash"){
        key_type_ = 'h';
    }else if (type_str=="list"){
        key_type_ = 'l';
    }else if (type_str=="set"){
        key_type_ = 's';
    }else if (type_str=="zset"){
        key_type_ = 'z';
    }else{
        LOG(WARNING) << "Migrate slot key: "<<key_ <<" not found";
        res_.AppendInteger(0);
        return -1;
    }
    return 0;
}

// delete one key from slotkey
int SlotsMgrtTagOneCmd::SlotKeyRemCheck(){
    std::string slotKey = SlotKeyPrefix+std::to_string(slot_num_);
    std::string tkey = std::string(1,key_type_) + key_;
    int64_t count;
    nemo::Status s = g_pika_server->db()->SRem(slotKey, tkey, &count);
    if (!s.ok()) {
        if (s.IsNotFound()) {
            LOG(INFO) << "Migrate slot: "<< slot_num_ <<" not found ";
            res_.AppendInteger(0);
        } else {
            LOG(WARNING) << "Migrate slot key: "<< key_ <<" error: " <<strerror(errno);
            res_.SetRes(CmdRes::kErrOther, "migrate slot error");
        }
        return -1;
    }
    return 0;
}

void SlotsMgrtTagOneCmd::Do() {
    if (g_pika_conf->slotmigrate() != true){
        LOG(WARNING) << "Not in slotmigrate mode";
        res_.SetRes(CmdRes::kErrOther, "not set slotmigrate");
        return;
    }
    if (KeyTypeCheck() < 0){
        return;
    }
    if (SlotKeyRemCheck() < 0){
        return;
    }
    if (MigrateOneKey(dest_ip_, dest_port_, key_, key_type_, res_) < 0){
        return;
    }
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
    if (g_pika_conf->slotmigrate() != true){
        LOG(WARNING) << "Not in slotmigrate mode";
        res_.SetRes(CmdRes::kErrOther, "not set slotmigrate");
        return;
    }
    std::map<int64_t,int64_t> slotsMap;
    for (int i = 0; i < HASH_SLOTS_SIZE; ++i){
        int64_t card = 0;
        card = g_pika_server->db()->SCard(SlotKeyPrefix+std::to_string(i));
        if (card > 0) {
            slotsMap[i]=card;
        }else if (card == 0){
            continue;
        }else {
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
    if (g_pika_conf->slotmigrate() != true){
        LOG(WARNING) << "Not in slotmigrate mode";
        res_.SetRes(CmdRes::kErrOther, "not set slotmigrate");
        return;
    }
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
    if (g_pika_conf->slotmigrate() != true){
        LOG(WARNING) << "Not in slotmigrate mode";
        res_.SetRes(CmdRes::kErrOther, "not set slotmigrate");
        return;
    }
    g_pika_server->StopBgslotsreload();
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
    if (g_pika_conf->slotmigrate() != true){
        LOG(WARNING) << "Not in slotmigrate mode";
        res_.SetRes(CmdRes::kErrOther, "not set slotmigrate");
        return;
    }
    std::vector<std::string> keys;
    std::vector<std::string>::const_iterator iter;
    for (iter = slots_.begin(); iter != slots_.end(); iter++){
        keys.push_back(SlotKeyPrefix + *iter);
    }
    int64_t count = 0;
    nemo::Status s = g_pika_server->db()->MDel(keys, &count);
    if (s.ok()) {
        res_.AppendInteger(count);
    } else {
        res_.SetRes(CmdRes::kErrOther, s.ToString());
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
    if (g_pika_conf->slotmigrate() != true){
        LOG(WARNING) << "Not in slotmigrate mode";
        res_.SetRes(CmdRes::kErrOther, "not set slotmigrate");
        return;
    }
    int64_t card = g_pika_server->db()->SCard(key_);
    if (card >= 0 && cursor_ >= card) {
        cursor_ = 0;
    }
    std::vector<std::string> members;
    nemo::SIterator *iter = g_pika_server->db()->SScan(key_, -1);
    iter->Skip(cursor_);
    if (!iter->Valid()) {
        delete iter;
        iter = g_pika_server->db()->SScan(key_, -1);
        cursor_ = 0;
    }
    for (; iter->Valid() && count_; iter->Next()) {
        if (pattern_ != "*" && !slash::stringmatchlen(pattern_.data(), pattern_.size(), iter->member().data(), iter->member().size(), 0)) {
            continue;
        }
        members.push_back(iter->member());
        count_--;
        cursor_++;
    }
    if (members.size() <= 0 || !iter->Valid()) {
        cursor_ = 0;
    }
    res_.AppendContent("*2");
  
    char buf[32];
    int64_t len = slash::ll2string(buf, sizeof(buf), cursor_);
    res_.AppendStringLen(len);
    res_.AppendContent(buf);

    res_.AppendArrayLen(members.size());
    std::vector<std::string>::const_iterator iter_member = members.begin();
    for (; iter_member != members.end(); iter_member++) {
        res_.AppendStringLen(iter_member->size());
        res_.AppendContent(*iter_member);
    }
    delete iter;
    return;
}
