// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "slash_string.h"
#include "nemo.h"
#include "pika_conf.h"
#include "pika_slot.h"
#include "pika_server.h"

//#include <sys/utsname.h>

extern PikaServer *g_pika_server;
extern PikaConf *g_pika_conf;

uint32_t crc32tab[256];
void CRC32_TableInit(uint32_t poly) {
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
    CRC32_TableInit(IEEE_POLY);
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

//del key from slotkey
void SlotKeyRem(const std::string type, const std::string key) {
    if (g_pika_conf->slotmigrate() != true){
        return;
    }
    int64_t res = 0;
    std::string slotKey = SlotKeyPrefix + std::to_string(SlotNum(key)); 
    nemo::Status s = g_pika_server->db()->SRem(slotKey, type+key, &res);
    if (!s.ok()) {
        LOG(WARNING) << "Zrem key: " << key <<" from slotKey, error: " <<strerror(errno);
    }
}

//check key exists
void KeyNotExistsRem(const std::string type, const std::string key) {
    int64_t res = -1;
    std::vector<std::string> vkeys;
    vkeys.push_back(key);
    nemo::Status s = g_pika_server->db()->Exists(vkeys, &res);
    if (res == 0) {
        SlotKeyRem(type, key);
    }
    return;
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

//migrate kv key to dest pika server
static int kvMigrate(const std::string dest_ip, const int64_t dest_port, const std::string key, const std::string value){
    pink::RedisCli *cli = new pink::RedisCli();
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
            pink::RedisCli::SerializeCommand(argv, &wbuf_str);
        }

        argv.clear();
        std::string tbuf_str;
        argv.push_back("set");
        argv.push_back(key);
        argv.push_back(value);
        pink::RedisCli::SerializeCommand(argv, &tbuf_str);

        wbuf_str.append(tbuf_str);

        pink::Status s;
        s = cli->Send(&wbuf_str);
        if (!s.ok()) {
            LOG(WARNING) << "slotKvMigrate Send: "<< key <<" error: " <<strerror(errno);
            return -1;
        }

        cli->Close();
    } else  {
        LOG(WARNING) << "slotKvMigrate Connect destination error: " <<strerror(errno);
        return -1;
    }
    delete cli;
    return 0;
}

// delete key from db
static int keyDel(const std::string key){
    int64_t count = 0;
    nemo::Status s = g_pika_server->db()->Del(key, &count);
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

// migrate one kv key
static int migrateKv(const std::string dest_ip, const int64_t dest_port, const std::string key){
    std::string value;
    if (kvGet(key, value) < 0){
      return -1;
    }
    if (value==""){
      return 0;
    }
    if (kvMigrate(dest_ip, dest_port, key, value) < 0){
      return -1;
    }
    keyDel(key); //key already been migrated successfully, del error doesn't matter
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

//migrate hash key to dest pika server
static int hashMigrate(const std::string dest_ip, const int64_t dest_port, const std::string key, const std::vector<nemo::FV> &fvs){
    pink::RedisCli *cli = new pink::RedisCli();
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
            pink::RedisCli::SerializeCommand(argv, &wbuf_str);
        }

        argv.clear();
        std::string tbuf_str;
        argv.push_back("hmset");
        argv.push_back(key);
        std::vector<nemo::FV>::const_iterator it;
        for (it = fvs.begin(); it != fvs.end(); it++) {
            argv.push_back(it->field);
            argv.push_back(it->val);
        }
        pink::RedisCli::SerializeCommand(argv, &tbuf_str);

        wbuf_str.append(tbuf_str);

        pink::Status s;
        s = cli->Send(&wbuf_str);
        if (!s.ok()) {
            LOG(WARNING) << "slotKvMigrate Send: "<< key <<" error: " <<strerror(errno);
            return -1;
        }

        cli->Close();
    } else  {
        LOG(WARNING) << "slotKvMigrate Connect destination error: " <<strerror(errno);
        return -1;
    }
    delete cli;
    return 1;
}

// migrate one hash key
static int migrateHash(const std::string dest_ip, const int64_t dest_port, const std::string key){
    std::vector<nemo::FV> fvs;
    if (hashGetall(key, fvs) < 0){
        return -1;
    }
    if (fvs.size()==0){
        return 0;
    }
    if (hashMigrate(dest_ip, dest_port, key, fvs) < 0){
        return -1;
    }
    keyDel(key); //key already been migrated successfully, del error doesn't matter
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

// migrate list key to dest pika server
static int listMigrate(const std::string dest_ip, const int64_t dest_port, const std::string key, const std::vector<nemo::IV> &ivs){
    pink::RedisCli *cli = new pink::RedisCli();
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
            pink::RedisCli::SerializeCommand(argv, &wbuf_str);
        }

        argv.clear();
        std::string tbuf_str;
        argv.push_back("lpush");
        argv.push_back(key);
        std::vector<nemo::IV>::const_iterator iter;
        for (iter = ivs.begin(); iter != ivs.end(); iter++) {
            argv.push_back(iter->val);
        }
        pink::RedisCli::SerializeCommand(argv, &tbuf_str);

        wbuf_str.append(tbuf_str);

        pink::Status s;
        s = cli->Send(&wbuf_str);
        if (!s.ok()) {
            LOG(WARNING) << "slotKvMigrate Send: "<< key <<" error: " <<strerror(errno);
            return -1;
        }

        cli->Close();
    } else  {
        LOG(WARNING) << "slotKvMigrate Connect destination error: " <<strerror(errno);
        return -1;
    }
    delete cli;
    return 1;
}

// migrate one list key
static int migrateList(const std::string dest_ip, const int64_t dest_port, const std::string key){
    std::vector<nemo::IV> ivs;
    if (listGetall(key, ivs) < 0){
        return -1;
    }
    if (ivs.size()==0){
        return 0;
    }
    if (listMigrate(dest_ip, dest_port, key, ivs) < 0){
        return -1;
    }
    keyDel(key); //key already been migrated successfully, del error doesn't matter
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

// migrate set key to dest pika server
static int setMigrate(const std::string dest_ip, const int64_t dest_port, const std::string key, const std::vector<std::string> &members){
    pink::RedisCli *cli = new pink::RedisCli();
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
            pink::RedisCli::SerializeCommand(argv, &wbuf_str);
        }

        argv.clear();
        std::string tbuf_str;
        argv.push_back("sadd");
        argv.push_back(key);
        std::vector<std::string>::const_iterator iter;
        for (iter = members.begin(); iter != members.end(); iter++) {
            argv.push_back(*iter);
        }
        pink::RedisCli::SerializeCommand(argv, &tbuf_str);

        wbuf_str.append(tbuf_str);

        pink::Status s;
        s = cli->Send(&wbuf_str);
        if (!s.ok()) {
            LOG(WARNING) << "slotKvMigrate Send: "<< key <<" error: " <<strerror(errno);
            return -1;
        }

        cli->Close();
    } else  {
        LOG(WARNING) << "slotKvMigrate Connect destination error: " <<strerror(errno);
        return -1;
    }
    delete cli;
    return 1;
}

// migrate one set key
static int migrateSet(const std::string dest_ip, const int64_t dest_port, const std::string key){
    std::vector<std::string> members;
    if (setGetall(key, members) < 0){
        return -1;
    }
    if (members.size()==0){
        return 0;
    }
    if (setMigrate(dest_ip, dest_port, key, members) < 0){
        return -1;
    }
    keyDel(key); //key already been migrated successfully, del error doesn't matter
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

// migrate zset key to dest pika server
static int zsetMigrate(const std::string dest_ip, const int64_t dest_port, const std::string key, const std::vector<nemo::SM> &sms){
    pink::RedisCli *cli = new pink::RedisCli();
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
            pink::RedisCli::SerializeCommand(argv, &wbuf_str);
        }

        argv.clear();
        std::string tbuf_str;
        argv.push_back("zadd");
        argv.push_back(key);
        std::vector<nemo::SM>::const_iterator iter;
        for (iter = sms.begin(); iter != sms.end(); iter++) {
            argv.push_back(std::to_string(iter->score));
            argv.push_back(iter->member);
        }
        pink::RedisCli::SerializeCommand(argv, &tbuf_str);

        wbuf_str.append(tbuf_str);

        pink::Status s;
        s = cli->Send(&wbuf_str);
        if (!s.ok()) {
            LOG(WARNING) << "slotKvMigrate Send: "<< key <<" error: " <<strerror(errno);
            return -1;
        }

        cli->Close();
    } else  {
        LOG(WARNING) << "slotKvMigrate Connect destination error: " <<strerror(errno);
        return -1;
    }
    delete cli;
    return 1;
}

// migrate zset key
static int migrateZset(const std::string dest_ip, const int64_t dest_port, const std::string key){
    std::vector<nemo::SM> sms;
    if (zsetGetall(key, sms) < 0){
        return -1;
    }
    if (sms.size()==0){
        return 0;
    }
    if (zsetMigrate(dest_ip, dest_port, key, sms) < 0){
        return -1;
    }
    keyDel(key);
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
    if (!slash::string2l(str_slot_num.data(), str_slot_num.size(), &slot_num_) || slot_num_ <= 0) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
    }
}

// spop one key from slotkey
int SlotsMgrtTagSlotCmd::SlotKeyPopCheck(){
    std::string slotKey = SlotKeyPrefix+std::to_string(slot_num_);
    std::string tkey;
    nemo::Status s = g_pika_server->db()->SPop(slotKey, tkey);
    if (!s.ok()) {
        if (s.IsNotFound()) {
            LOG(INFO) << "Migrate slot: "<< slot_num_ <<" not found ";
            res_.AppendArrayLen(2);
            res_.AppendInteger(0);
            res_.AppendInteger(0);
        } else {
            LOG(WARNING) << "Migrate slot key: "<< tkey <<" error: " <<strerror(errno);
            res_.SetRes(CmdRes::kErrOther, "migrate slot error");
        }
        return -1;
    }
    key_type_ = tkey.at(0);
    key_ = tkey;
    key_.erase(key_.begin());
    return 0;
}

void SlotsMgrtTagSlotCmd::Do() {
    if (SlotKeyPopCheck() < 0){
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
            res_.AppendArrayLen(2);
            res_.AppendInteger(0);
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
        LOG(WARNING) << "Migrate slot key: "<<key_ <<" error";
        res_.AppendArrayLen(2);
        res_.AppendInteger(0);
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
            res_.AppendArrayLen(2);
            res_.AppendInteger(0);
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
    if (MigrateOneKey(dest_ip_, dest_port_, key_, key_type_, res_) < 0){
      return;
    }
    std::string slotKey = SlotKeyPrefix+std::to_string(slot_num_);
    SlotKeyLenCheck(slotKey, res_);
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
        res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsInfo);
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