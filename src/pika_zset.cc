#include "pika_command.h"
#include "pika_server.h"
#include "util.h"
#include <algorithm>
#include <map>

extern PikaServer *g_pikaServer;
extern std::map<std::string, Cmd *> g_pikaCmd;

void ZAddCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if (((int)argv.size() % 2 != 0) || (arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();

    double score;
    std::list<std::string>::iterator iter;
    for (iter = argv.begin(); iter !=  argv.end(); iter++) {
        if (!string2d(iter->data(), iter->size(), &score)) {
            ret = "-ERR value is not a valid float\r\n";
            return;
        }
        iter++;
    }
    std::string str_score;
    std::string member;
    int64_t res;
    int64_t count = 0;
    nemo::Status s;
    while (argv.size()) {
        str_score = argv.front();
        argv.pop_front();
        member = argv.front();
        argv.pop_front();
        string2d(str_score.data(), str_score.size(), &score);
        s = g_pikaServer->GetHandle()->ZAdd(key, score, member, &res);
        if (s.ok()) {
            count += res;
        } else {
            ret.append("-ERR ");
            ret.append(s.ToString().c_str());
            ret.append("\r\n");
            return;
        }
    }
    char buf[32];
    snprintf(buf, sizeof(buf), ":%ld\r\n", count);
    ret.append(buf);
}

void ZCardCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if (((int)argv.size() % 2 != 0) || (arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();

    int64_t res = g_pikaServer->GetHandle()->ZCard(key);
    if (res >= 0) {
        char buf[32];
        snprintf(buf, sizeof(buf), ":%ld\r\n", res);
        ret.append(buf);
    } else {
        ret = "-ERR zcard error\r\n";
    }
}
