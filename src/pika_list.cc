#include "pika_command.h"
#include "pika_server.h"
#include "util.h"
#include <algorithm>
#include <map>

extern PikaServer *g_pikaServer;
extern std::map<std::string, Cmd *> g_pikaCmd;

void LPushCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::string value;
    nemo::Status s;
    uint64_t llen = 0;
    while (argv.size() > 0) {
        value = argv.front();
        s = g_pikaServer->GetHandle()->LPush(key, value, &llen);
        if (s.IsCorruption()) {
            break;
        }
        argv.pop_front();
    }
    if (argv.size() > 0) {
        ret = "+ERR ";
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    } else {
        char buf[32];
        snprintf(buf, sizeof(buf), ":%lu\r\n", llen);
        ret.append(buf);
    }
}
