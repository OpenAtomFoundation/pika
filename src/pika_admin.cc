#include <algorithm>
#include <glog/logging.h>
#include <map>
#include <sys/utsname.h>
#include <sys/types.h>
#include <unistd.h>

#include "pika_command.h"
#include "pika_server.h"
#include "pika_conf.h"
#include "pika_define.h"
#include "util.h"
#include "mario.h"

extern PikaServer *g_pikaServer;
extern mario::Mario *g_pikaMario;
extern std::map<std::string, Cmd *> g_pikaCmd;
extern PikaConf* g_pikaConf;

void AuthCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string password = argv.front();
    argv.pop_front();
    if (password == std::string(g_pikaConf->requirepass()) || std::string(g_pikaConf->requirepass()) == "") {
        ret = "+OK\r\n";
    } else {
        ret = "-ERR invalid password\r\n";
    }
}

void PingCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    LOG(INFO)<<"Receive Ping";
    ret = "+PONG\r\n";
}

void ClientCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((argv.size() != 2 && argv.size() != 3) || (arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string opt = argv.front();
    argv.pop_front();
    transform(opt.begin(), opt.end(), opt.begin(), ::tolower);
    if (opt == "list") {
        g_pikaServer->ClientList(ret);
    } else if (opt == "kill") {
        std::string ip_port = argv.front();
        argv.pop_front();
        int res = g_pikaServer->ClientKill(ip_port);
        if (res == 1) {
            ret = "+OK\r\n";
        } else {
            ret = "-ERR No such client\r\n";
        }
        
    } else {
        ret = "-ERR Syntax error, try CLIENT (LIST | KILL ip:port)\r\n"; 
    }

}

//void BemasterCmd::Do(std::list<std::string> &argv, std::string &ret) {
//    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
//        ret = "-ERR wrong number of arguments for ";
//        ret.append(argv.front());
//        ret.append(" command\r\n");
//        return;
//    }
//    argv.pop_front();
//    std::string str_fd = argv.front();
//    argv.pop_front();
//    int64_t fd;
//    if (!string2l(str_fd.data(), str_fd.size(), &fd)) {
//        ret = "-ERR value is not an integer or out of range\r\n";
//        return;
//    }
//
//    int res = g_pikaServer->ClientRole(fd, CLIENT_MASTER);
//    if (res == 1) {
//        ret = "+OK\r\n";
//    } else {
//        ret = "-ERR No such client\r\n";
//    }
//}

void SlaveofCmd::Do(std::list<std::string> &argv, std::string &ret) {
    int as = argv.size();
    if ((as != 3 && as != 5) || (arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string p2 = argv.front();
    argv.pop_front();
    std::string p3 = argv.front();
    argv.pop_front();
    if (p2 == "no" && p3 == "one") {
        g_pikaServer->Slaveofnoone();
        ret = "+OK\r\n";
        return;
    }
    int64_t port = 0;
    if (!string2l(p3.data(), p3.size(), &port)) {
        ret = "-ERR value is not an integer or out of range\r\n";
        return;
    }
    
    std::string hostip = g_pikaServer->GetServerIp();
    if ((p2 == hostip || p2 == "127.0.0.1") && port == g_pikaServer->GetServerPort()) {
        ret = "-ERR you fucked up\r\n";
        return;
    }

    int64_t filenum = 0;
    int64_t pro_offset = 0;
    bool is_psync = false;
    if (argv.size() == 2) {
        std::string p4 = argv.front();
        argv.pop_front();
        if (!string2l(p4.data(), p4.size(), &filenum)) {
            ret = "-ERR value is not an integer or out of range\r\n";
            return;
        }
        
        std::string p5 = argv.front();
        argv.pop_front();
        if (!string2l(p5.data(), p5.size(), &pro_offset)) {
            ret = "-ERR value is not an integer or out of range\r\n";
            return;
        }
        is_psync = true;
    }

    {
    MutexLock l(g_pikaServer->Mutex());
    if (g_pikaServer->masterhost() == p2 && g_pikaServer->masterport() == port) {
        ret = "+OK Already connected to specified master\r\n";
        return;
    } else if (g_pikaServer->repl_state() != PIKA_SINGLE) { 
        ret = "-ERR State is not in PIKA_SINGLE\r\n";
        return;
    }

    if (g_pikaServer->ms_state_ == PIKA_REP_SINGLE) {
        if (is_psync) {
            g_pikaMario->SetProducerStatus(filenum, pro_offset);
        }
        g_pikaServer->ms_state_ = PIKA_REP_CONNECT;
        g_pikaServer->set_masterhost(p2);
        g_pikaServer->set_masterport(port);
        ret = "+OK\r\n";
    } else {
        ret = "-ERR State is not in PIKA_REP_SINGLE\r\n";
    }

    }
}

void PikasyncCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
//    std::string ip = argv.front();
//    argv.pop_front();
//    std::string str_port = argv.front();
//    argv.pop_front();
//    int64_t port = 0;
//    if (!string2l(str_port.data(), str_port.size(), &port)) {
//        ret = "-ERR value is not an integer or out of range\r\n";
//        return;
//    }
    std::string str_filenum = argv.front();
    argv.pop_front();
    int64_t filenum = 0;
    if (!string2l(str_filenum.data(), str_filenum.size(), &filenum)) {
        ret = "-ERR value is not an integer or out of range\r\n";
        return;
    }
    std::string str_offset = argv.front();
    argv.pop_front();
    int64_t offset = 0;
    if (!string2l(str_offset.data(), str_offset.size(), &offset)) {
        ret = "-ERR value is not an integer or out of range\r\n";
        return;
    }

    std::string str_fd = argv.front();
    argv.pop_front();
    int64_t fd = 0;
    if (!string2l(str_fd.data(), str_fd.size(), &fd)) {
        ret = "-ERR value is not an integer or out of range\r\n";
        return;
    }

//    LOG(INFO) << ip << " : " << str_port;
    int res = g_pikaServer->TrySync(/*ip, str_port, */fd, filenum, offset);
    if (res == PIKA_REP_STRATEGY_PSYNC) {
        ret = "*1\r\n$9\r\nucanpsync\r\n";
    } else {
        ret = "*1\r\n$9\r\nsyncerror\r\n";
    }
}

void UcanpsyncCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();

    {
    MutexLock l(g_pikaServer->Mutex());
    if (g_pikaServer->ms_state_ == PIKA_REP_CONNECTING) {
        g_pikaServer->ms_state_ = PIKA_REP_CONNECTED;
    }
    }

    pthread_rwlock_unlock(g_pikaServer->rwlock());
    {
    RWLock rwl(g_pikaServer->rwlock(), true);
    g_pikaServer->is_readonly_ = true;
    }
    LOG(INFO) << "Master told me that I can psync, open readonly mode now";
    ret = "";
}

void PutInt32(std::string *dst, int32_t v) {
    std::string vstr = std::to_string(v);
    dst->append(vstr);
}

void SyncerrorCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    g_pikaServer->DisconnectFromMaster();
    LOG(INFO) << "Master told me that I can not psync, rollback now";
    ret = ""; 
}

void LoaddbCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string path = argv.front();
    argv.pop_front();
    pthread_rwlock_unlock(g_pikaServer->rwlock());
    bool res = g_pikaServer->LoadDb(path);
    if (res == true) {
        ret = "+OK\r\n";
    } else {
        ret = "-ERR Load DB Error\r\n";
    }
}

void DumpCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
//    uint32_t filenum = 0;
//    uint64_t pro_offset = 0;
//    nemo::Snapshots snapshots;
    pthread_rwlock_unlock(g_pikaServer->rwlock());
//    {
//        RWLock l(g_pikaServer->rwlock(), true);
//        g_pikaMario->GetProducerStatus(&filenum, &pro_offset);
//        g_pikaServer->GetHandle()->BGSaveGetSnapshot(snapshots);
//    }
    g_pikaServer->Dump();
    ret = "+";
    ret.append(g_pikaServer->dump_time_);
    ret.append(" : ");
    char buf[32];
    snprintf(buf, sizeof(buf), "%d", g_pikaServer->dump_filenum_);
    ret.append(buf);
    ret.append(" : ");
    snprintf(buf, sizeof(buf), "%lu", g_pikaServer->dump_pro_offset_);
    ret.append(buf);
    ret.append("\r\n");
}

void ReadonlyCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string opt = argv.front();
    argv.pop_front();
    transform(opt.begin(), opt.end(), opt.begin(), ::tolower);
    ret = "+OK\r\n";
    if (opt == "on") {
        pthread_rwlock_unlock(g_pikaServer->rwlock());
        {
        RWLock l(g_pikaServer->rwlock(), true);
        g_pikaServer->is_readonly_ = true;
        }
    } else if (opt == "off") {
        pthread_rwlock_unlock(g_pikaServer->rwlock());
        {
        RWLock l(g_pikaServer->rwlock(), true);
        g_pikaServer->is_readonly_ = false;
        }
    } else {
        ret = "-ERR invalid option for readonly(on/off?)\r\n";
    }
}

void EncodeString(std::string *dst, const std::string &value) {
    dst->append("$");
    PutInt32(dst, value.size());
    dst->append("\r\n");
    dst->append(value.data(), value.size());
    dst->append("\r\n");
}

void EncodeInt32(std::string *dst, const int32_t v) {
    std::string vstr = std::to_string(v);
    dst->append("$");
    PutInt32(dst, vstr.length());
    dst->append("\r\n");
    dst->append(vstr);
    dst->append("\r\n");
}

void ConfigCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if (argv.size() != 3 && argv.size() != 4) {
        ret = "-ERR wrong number of arguments for 'config' command\r\n";
        return;
    }
    argv.pop_front();
    std::string opt = argv.front();
    argv.pop_front();
    transform(opt.begin(), opt.end(), opt.begin(), ::tolower);

    std::string conf_item = argv.front();
    argv.pop_front();
    transform(conf_item.begin(), conf_item.end(), conf_item.begin(), ::tolower);
  
    if (argv.size() == 0 && opt == "get") {
        if (conf_item == "port") {
            ret = "*2\r\n";
            EncodeString(&ret, "port");
            EncodeInt32(&ret, g_pikaConf->port());
        } else if (conf_item == "thread_num") {
            ret = "*2\r\n";
            EncodeString(&ret, "thread_num");
            EncodeInt32(&ret, g_pikaConf->thread_num());
        } else if (conf_item == "log_path") {
            ret = "*2\r\n";
            EncodeString(&ret, "log_path");
            EncodeString(&ret, g_pikaConf->log_path());
        } else if (conf_item == "log_level") {
            ret = "*2\r\n";
            EncodeString(&ret, "log_level");
            EncodeInt32(&ret, g_pikaConf->log_level());
        } else if (conf_item == "db_path") {
            ret = "*2\r\n";
            EncodeString(&ret, "db_path");
            EncodeString(&ret, g_pikaConf->db_path());
        } else if (conf_item == "write_buffer_size") {
            ret = "*2\r\n";
            EncodeString(&ret, "write_buffer_size");
            EncodeInt32(&ret, g_pikaConf->write_buffer_size());
        } else if (conf_item == "timeout") {
            ret = "*2\r\n";
            EncodeString(&ret, "timeout");
            EncodeInt32(&ret, g_pikaConf->timeout());
        } else if (conf_item == "requirepass") {
            ret = "*2\r\n";
            EncodeString(&ret, "requirepass");
            EncodeString(&ret, g_pikaConf->requirepass());
        } else if (conf_item == "daemonize") {
            ret = "*2\r\n";
            EncodeString(&ret, "daemonize");
            EncodeString(&ret, g_pikaConf->daemonize() ? "yes" : "no");
        } else {
            ret = "-ERR No such configure item\r\n";
        }
    } else if (argv.size() == 1 && opt == "set") {
        std::string value = argv.front();
        long int ival;
        if (conf_item == "timeout") {
            if (!string2l(value.data(), value.size(), &ival)) {
                ret = "-ERR Invalid argument " + value + " for CONFIG SET 'timeout'\r\n";
                return;
            }
            g_pikaConf->SetTimeout(ival);
            ret = "+OK\r\n";
        } else if (conf_item == "requirepass") {
            g_pikaConf->SetRequirePass(value);
            ret = "+OK\r\n";
        } else {
            ret = "-ERR No such configure item\r\n";
        }
    } else {
        ret = "-ERR wrong number of arguments for CONFIG ";
        ret.append(opt);
        ret.append(" command\r\n");
        return;
    }
}

void InfoCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if (argv.size() != 1 && argv.size() != 2) {
        ret = "-ERR wrong number of arguments for 'info' command\r\n";
        return;
    }
    argv.pop_front();
    bool allsections = true;
    std::string section = "";
    int sections = 0;

    if (!argv.empty()) {
        allsections = false;
        section = argv.front();
        transform(section.begin(), section.end(), section.begin(), ::tolower);
    }

    std::string info;
    // Server
    if (allsections || section == "server") {
        //TODO mode : standalone cluster sentinel

        if (sections++) info.append("\r\n");

        static bool call_uname = true;
        static struct utsname name;
        if (call_uname) {
            uname(&name);
            call_uname = false;
        }
        std::string s = g_pikaServer->is_bgsaving();
        char buf[512];
        snprintf (buf, sizeof(buf),
                  "# Server\r\n"
                  "pika_version:%s\r\n"
                  "os:%s %s %s\r\n"
                  "process_id:%ld\r\n"
                  "tcp_port:%d\r\n"
                  "thread_num:%d\r\n"
                  "config_file:%s\r\n"
                  "is_bgsaving:%s\r\n",
                  PIKA_VERSION,
                  name.sysname, name.release, name.machine,
                  (long) getpid(),
                  g_pikaConf->port(),
                  g_pikaConf->thread_num(),
                  g_pikaConf->conf_path(),
                  g_pikaServer->is_bgsaving().c_str()
                  );
        info.append(buf);
    }

    // Clients
    if (allsections || section == "clients") {
        if (sections++) info.append("\r\n");

        std::string clients;
        char buf[128];
        snprintf (buf, sizeof(buf),
                  "# Clients\r\n"
                  "connected_clients:%d\r\n",
                  g_pikaServer->ClientList(clients));
        info.append(buf);
    }

    // Key Space
    if (allsections || section == "keyspace") {
        if (sections++) info.append("\r\n");

        std::vector<uint64_t> nums;
        g_pikaServer->GetHandle()->GetKeyNum(nums);
        char buf[128];
        snprintf (buf, sizeof(buf),
                  "# Stats\r\n"
                  "kv keys:%lu\r\n"
                  "hash keys:%lu\r\n"
                  "list keys:%lu\r\n"
                  "zset keys:%lu\r\n"
                  "set keys:%lu\r\n",
                  nums[0], nums[1], nums[2], nums[3], nums[4]);

        info.append(buf);
    }

    ret.clear();
    char buf[32];
    snprintf (buf, sizeof(buf), "$%u\r\n", info.length());
    ret.append(buf);
    ret.append(info);
    ret.append("\r\n");
}
