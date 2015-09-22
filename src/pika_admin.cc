#include "pika_command.h"
#include "pika_server.h"
#include "pika_conf.h"
#include "util.h"
#include <algorithm>
#include <map>

extern PikaServer *g_pikaServer;
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

