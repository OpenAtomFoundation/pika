#include "pika_command.h"
#include "pika_server.h"
#include "pika_conf.h"
#include "util.h"
#include <algorithm>
#include <map>

extern PikaServer *g_pikaServer;
extern std::map<std::string, Cmd *> g_pikaCmd;
extern PikaConf* g_pikaConf;
extern pthread_rwlock_t *g_pikaRWlock;
extern std::map<std::string, client_info> *g_pikaClient;

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
    std::map<std::string, client_info>::iterator iter;
    if (opt == "list") {
        ret = "+";
        char buf[32];
        for (int i = 0; i < g_pikaConf->thread_num(); i++) {
            pthread_rwlock_rdlock(&g_pikaRWlock[i]);
            iter = g_pikaClient[i].begin();
            while (iter != g_pikaClient[i].end()) {
                ret.append("addr=");
                ret.append(iter->first);
                ret.append(" fd=");
                ll2string(buf,sizeof(buf), (iter->second).fd);
                ret.append(buf);
                ret.append("\n");
                iter++;
            }
            pthread_rwlock_unlock(&g_pikaRWlock[i]);
        }
        ret.append("\r\n");
    } else if (opt == "kill") {
        std::string ip_port = argv.front();
        argv.pop_front();
        int i = 0;
        for (i = 0; i < g_pikaConf->thread_num(); i++) {
            pthread_rwlock_wrlock(&g_pikaRWlock[i]);
            iter = g_pikaClient[i].find(ip_port);
            if (iter != g_pikaClient[i].end()) {
                (iter->second).is_killed = true;
                pthread_rwlock_unlock(&g_pikaRWlock[i]);
                break;
            }
            pthread_rwlock_unlock(&g_pikaRWlock[i]);
        }
        if (i < g_pikaConf->thread_num()) {
            ret = "+OK\r\n";
        } else {
            ret = "-ERR No such client\r\n";
        }
        
    } else {
        ret = "-ERR Syntax error, try CLIENT (LIST | KILL ip:port)\r\n"; 
    }

}

