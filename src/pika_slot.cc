// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <algorithm>
#include "slash/include/slash_string.h"
#include "slash/include/slash_status.h"
#include "nemo.h"
#include "include/pika_conf.h"
#include "include/pika_slot.h"
#include "include/pika_server.h"

#define min(a, b)  (((a) > (b)) ? (b) : (a))

extern PikaServer *g_pika_server;
extern PikaConf *g_pika_conf;

const int asyncRecvsNum = 200;

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
static int doMigrate(pink::PinkCli *cli, std::string send_str){
    slash::Status s;
    s = cli->Send(&send_str);
    if (!s.ok()) {
        LOG(WARNING) << "slotKvMigrate Send error: " <<strerror(errno);
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
int KeyDelete(const std::string key, const char key_type){
    int64_t count = 0;
    nemo::Status s = g_pika_server->db()->DelSingleType(key, &count, key_type);
    if (!s.ok()) {
        if (s.IsNotFound()) {
            LOG(WARNING) << "Del key: "<< key <<"at slot "<< SlotNum(key) << " not found ";
            return 0;
        } else {
            LOG(WARNING) << "Del key: "<< key <<"at slot "<< SlotNum(key) <<" error: " <<strerror(errno);
            return -1;
        }
    }
    return 1;
}

static int migrateKeyTTl(pink::PinkCli *cli, const std::string key){
    int64_t ttl = 0;
    pink::RedisCmdArgsType argv;
    std::string send_str;
    nemo::Status s = g_pika_server->db()->TTL(key, &ttl);
    if (s.ok() && (ttl > 0)){
        argv.push_back("expire");
        argv.push_back(key);
        argv.push_back(std::to_string(ttl));
        pink::SerializeRedisCommand(argv, &send_str);
        if (doMigrate(cli, send_str) < 0){
            return -1;
        }

    }
    return 0;
}

// migrate one kv key
static int migrateKv(pink::PinkCli *cli, const std::string key){
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
    }

    if (migrateKeyTTl(cli, key) < 0){
        return -1;
    }

    KeyDelete(key, 'k'); //key already been migrated successfully, del error doesn't matter
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
static int migrateHash(pink::PinkCli *cli, const std::string key){
    std::vector<nemo::FV> fvs;
    if (hashGetall(key, fvs) < 0){
        return -1;
    }
    size_t keySize = fvs.size();
    if (keySize==0){
        return 0;
    }

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
        if (doMigrate(cli, send_str) < 0){
            return -1;
        }
    }

    if (migrateKeyTTl(cli, key) < 0){
        return -1;
    }

    KeyDelete(key, 'h'); //key already been migrated successfully, del error doesn't matter
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
static int migrateList(pink::PinkCli *cli, const std::string key){
    std::vector<nemo::IV> ivs;
    if (listGetall(key, ivs) < 0){
        return -1;
    }
    size_t keySize = ivs.size();
    if (keySize==0){
        return 0;
    }

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
        if (doMigrate(cli, send_str) < 0){
            return -1;
        }
    }

    if (migrateKeyTTl(cli, key) < 0){
        return -1;
    }

    KeyDelete(key, 'l'); //key already been migrated successfully, del error doesn't matter
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
static int migrateSet(pink::PinkCli *cli, const std::string key){
    std::vector<std::string> members;
    if (setGetall(key, members) < 0){
        return -1;
    }
    size_t keySize = members.size();
    if (keySize==0){
        return 0;
    }

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
        if (doMigrate(cli, send_str) < 0){
            return -1;
        }
    }

    if (migrateKeyTTl(cli, key) < 0){
        return -1;
    }

    KeyDelete(key, 's'); //key already been migrated successfully, del error doesn't matter
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
static int migrateZset(pink::PinkCli *cli, const std::string key){
    std::vector<nemo::SM> sms;
    if (zsetGetall(key, sms) < 0){
        return -1;
    }
    size_t keySize = sms.size();
    if (keySize==0){
        return 0;
    }

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
        if (doMigrate(cli, send_str) < 0){
            return -1;
        }
    }

    if (migrateKeyTTl(cli, key) < 0){
        return -1;
    }

    KeyDelete(key, 'z');
    return 1;
}

// migrate key
static int MigrateOneKey(pink::PinkCli *cli, const std::string key, const char key_type){
    switch (key_type) {
        case 'k':
            if (migrateKv(cli, key) < 0){
                SlotKeyAdd("k", key);
                return -1;
            }
            break;
        case 'h':
            if (migrateHash(cli, key) < 0){
                SlotKeyAdd("h", key);
                return -1;
            }
            break;
        case 'l':
            if (migrateList(cli, key) < 0){
                SlotKeyAdd("l", key);
                return -1;
            }
            break;
        case 's':
            if (migrateSet(cli, key) < 0){
                SlotKeyAdd("s", key);
                return -1;
            }
            break;
        case 'z':
            if (migrateZset(cli, key) < 0){
                SlotKeyAdd("z", key);
                return -1;
            }
            break;
        default:
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
    if (SlotKeyPop() < 0){
      return;
    }

    pink::PinkCli *cli = pink::NewRedisCli();
    cli->set_connect_timeout(timeout_ms_);
    if ((cli->Connect(dest_ip_, dest_port_, g_pika_server->host())).ok()) {
        cli->set_send_timeout(timeout_ms_);
        cli->set_recv_timeout(timeout_ms_);

        pink::RedisCmdArgsType argv;
        std::string wbuf_str;
        std::string requirepass = g_pika_conf->requirepass();
        if (requirepass != "") {
            argv.push_back("auth");
            argv.push_back(requirepass);
            pink::SerializeRedisCommand(argv, &wbuf_str);
        }

        slash::Status s;
        s = cli->Send(&wbuf_str);
        if (!s.ok()) {
            LOG(WARNING) << "Slot Migrate auth error: " <<strerror(errno);
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

        pink::RedisCmdArgsType argv;
        std::string wbuf_str;
        std::string requirepass = g_pika_conf->requirepass();
        if (requirepass != "") {
            argv.push_back("auth");
            argv.push_back(requirepass);
            pink::SerializeRedisCommand(argv, &wbuf_str);
        }

        slash::Status s;
        s = cli->Send(&wbuf_str);
        if (!s.ok()) {
            LOG(WARNING) << "Slot Migrate auth error: " <<strerror(errno);
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
    ret = g_pika_server->GetSlotsMigrateResul(&moved, &remained);
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
    std::stringstream tmp_stream;
    tmp_stream << "dest server: " << ip << ":" << std::to_string(port) << "\r\n";
    tmp_stream << "slot number: " << std::to_string(slot) << "\r\n";
    tmp_stream << "migrating  : " << mstatus << "\r\n";
    tmp_stream << "moved keys : " << std::to_string(moved) << "\r\n";
    tmp_stream << "remain keys: " << std::to_string(remained) << "\r\n";
    status.append(tmp_stream.str());
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
    if(!ret) {
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
    is_migrating_(false),
    should_exit_(false) {
        cli_ = pink::NewRedisCli();
        pthread_rwlockattr_t attr;
        pthread_rwlockattr_init(&attr);
        pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
        pthread_rwlock_init(&rwlock_db_, &attr);
        pthread_rwlock_init(&rwlock_batch_, &attr);
        pthread_rwlock_init(&rwlock_ones_, &attr);
}

SlotsMgrtSenderThread::~SlotsMgrtSenderThread() {
    if(is_migrating_) {
        slotsmgrt_cond_.SignalAll();
    }
    if (is_running()) {
        StopThread();
    }
    is_migrating_ = false;
    should_exit_ = true;
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
    nemo::Status s = g_pika_server->db()->Type(key, &type_str);
    if (!s.ok()) {
        if (s.IsNotFound()) {
            LOG(INFO) << "Migrate key: "<< key <<" not found";
            return 0;
        } else {
            LOG(WARNING) << "Migrate one key: "<< key <<" error: " <<strerror(errno);
            return -1;
        }
    }
    if (type_str=="string"){
        key_type = 'k';
    }else if (type_str=="hash"){
        key_type = 'h';
    }else if (type_str=="list"){
        key_type = 'l';
    }else if (type_str=="set"){
        key_type = 's';
    }else if (type_str=="zset"){
        key_type = 'z';
    }else if (type_str=="none"){
        return 0;
    }else{
        LOG(WARNING) << "Migrate  key: "<<key <<" type: " << type_str << " is  illegal";
        return -1;
    }

    // when this slot has been finished migrating, and the thread migrating status is reset, but the result 
    // has not been returned to proxy, during this time, some more request on this slot are sent to the server,
    // so need to check if the key exists first, if not exists the proxy forwards the request to destination server
    // if the key exists, it is an error which should not happen.
    if(slot_num != slot_num_ ) {
        LOG(WARNING) << "Slot : "<<slot_num <<" is not the migrating slot:" << slot_num_;
        return -1;
    } else if(!is_migrating_) {
        LOG(WARNING) << "Slot : "<<slot_num <<" is not migrating";
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
    int64_t remained;
    s = g_pika_server->db()->SRem(slotKey, key_type+key, &remained);
    if (s.IsNotFound()) {
        // s.IsNotFound() is also error, key found in db but not in slotkey, run slotsreload cmd first
        LOG(WARNING) << "Migrate key: "<< key <<" srem from slotkey error: " <<strerror(errno);
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
            LOG(WARNING) << "wrong des_ip, des_port or slot_num";
            return false;
        }
        timeout_ms_ = time_out;
        keys_num_ = keys_num;
        bool ret = ElectMigrateKeys();
        if (!ret) {
            LOG(WARNING) << "Slots migrating sender get batch keys error";
            is_migrating_ = false;
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
        }
        StartThread();
        LOG(INFO) << "Migrate batch slot: "<< slot;
    }
    return true;
}

bool SlotsMgrtSenderThread::GetSlotsMigrateResul(int64_t *moved, int64_t *remained){
    slash::MutexLock lm(&slotsmgrt_cond_mutex_);
    slotsmgrt_cond_.TimedWait(timeout_ms_);
    *moved = moved_keys_num_;
    *remained = remained_keys_num_;
    if(*remained <= 0){
        is_migrating_ == false;
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
    slash::RWLock lb(&rwlock_batch_, true);
    slash::RWLock lo(&rwlock_ones_, true);
    dest_ip_ = "none";
    dest_port_ = -1;
    timeout_ms_ = 3000;
    slot_num_ = -1;
    moved_keys_num_ = -1;
    moved_keys_all_ = -1;
    remained_keys_num_ = -1;
    is_migrating_ = false;
    should_exit_ = false;
    if (is_running()) {
        StopThread();
    }
    return true;
}

bool SlotsMgrtSenderThread::ElectMigrateKeys(){
    slash::RWLock l(&rwlock_db_, true);
    slash::RWLock lb(&rwlock_batch_, true);
    std::string slotKey = SlotKeyPrefix+std::to_string(slot_num_);
    remained_keys_num_ = g_pika_server->db()->SCard(slotKey) + migrating_batch_.size() + migrating_ones_.size();
    if(remained_keys_num_ == 0){
        LOG(WARNING) << "No keys in slot: " << slot_num_;
        should_exit_ = true;
        is_migrating_ = false;
        return true;
    } else if(remained_keys_num_ < 0) {
        LOG(WARNING) << "Scard slotkey error in slot: " << slot_num_;
        return false;
    }
    std::string tkey, key;
    char key_type;
    int64_t remained;
    nemo::SIterator *iter = g_pika_server->db()->SScan(slotKey, keys_num_);
    for(; iter->Valid(); iter->Next()){
        tkey = iter->member();
        key_type = tkey.at(0);
        key = tkey;
        key.erase(key.begin());
        migrating_batch_.push_back(std::make_pair(key_type, key));
        nemo::Status s = g_pika_server->db()->SRem(slotKey, tkey, &remained);
        if (!s.ok()) {
            if (s.IsNotFound()) {
                LOG(INFO) << "Srem key "<< key <<" not found";
                continue;
            } else {
                LOG(WARNING) << "Srem key: " << key <<" from slotKey, error: " <<strerror(errno);
                std::vector<std::pair<const char, std::string>>().swap(migrating_batch_);
                delete iter;
                return false;
            }
        }
    }
    delete iter;
    should_exit_ = false;
    return true;
}

void* SlotsMgrtSenderThread::ThreadMain() {
    LOG(INFO) << "SlotsMgrtSender thread " << thread_id() << " for slot:" << slot_num_ <<" start!";
    slash::Status result;
    cli_->set_connect_timeout(timeout_ms_);
    moved_keys_all_ = 0;
    while (!should_exit_) {
        result = cli_->Connect(dest_ip_, dest_port_, g_pika_server->host());
        LOG(INFO) << "Slots Migrate Sender Connect server(" << dest_ip_ << ":" << dest_port_ << ") " << result.ToString();

        if (result.ok()) {
            std::string slotKey = SlotKeyPrefix+std::to_string(slot_num_);
            std::vector<std::pair<const char, std::string>>::const_iterator iter;
            while (is_migrating_) {
                slash::MutexLock lm(&slotsmgrt_cond_mutex_);
                {
                    slash::RWLock lb(&rwlock_batch_, true);
                    slash::RWLock lo(&rwlock_ones_, true);
                    remained_keys_num_ = g_pika_server->db()->SCard(slotKey) + migrating_batch_.size() + migrating_ones_.size();
                    // add ones to batch end; empty ones
                    std::copy (migrating_ones_.begin(), migrating_ones_.end(), std::back_inserter(migrating_batch_));
                    if (migrating_batch_.size() != 0) {
                        moved_keys_num_ = 0;
                    }
                    std::vector<std::pair<const char, std::string>>().swap(migrating_ones_);
                }

                iter = migrating_batch_.begin();
                while(iter != migrating_batch_.end()) {
                    size_t j = 0;
                    slash::RWLock lb(&rwlock_batch_, false);
                    for (; (iter != migrating_batch_.end()) && (j<asyncRecvsNum); iter++,j++) {
                        if (MigrateOneKey(cli_, iter->second, iter->first) < 0){
                            LOG(WARNING) << "Migrate batch key: " << iter->second <<" error: ";
                            slotsmgrt_cond_.Signal();
                            is_migrating_ = false;
                            should_exit_ = true;
                            goto migrate_end;
                        }
                        moved_keys_num_++;
                        moved_keys_all_++;
                        remained_keys_num_--;
                    }
                    for(; j>0; j-- ) {
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
                    LOG(INFO) << "Migrate slot: " << slot_num_ <<" finished";
                    slotsmgrt_cond_.Signal();
                    moved_keys_num_ = 0;
                    is_migrating_ = false;
                    should_exit_ = true;
                    goto migrate_end;
                }
                slotsmgrt_cond_.Signal();
            }
        } else {
            LOG(WARNING)  << "Slots Migrate Sender Connect server(" << dest_ip_ << ":" << dest_port_ << ") error";
            moved_keys_num_ = -1;
            is_migrating_ = false;
            should_exit_ = true;
            slotsmgrt_cond_.Signal();
        }

migrate_end:
        cli_->Close();
        sleep(1);
    }
    return NULL;
}