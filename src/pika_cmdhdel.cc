#include "pika_command.h"
#include "pika_server.h"
#include <algorithm>
#include <map>

extern PikaServer *g_pikaServer;
extern std::map<std::string, Cmd *> g_pikaCmd;

void HDelCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && argv.size() != arity) || (arity < 0 && argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    int num = 0;
    while (!argv.empty()) {
        nemo::Status s = g_pikaServer->GetHandle()->HDel(key, argv.front());
        if (s.ok()) {
            num++;
        }
        argv.pop_front();
    }
    char buf[32];
    snprintf(buf, sizeof(buf), "%d\r\n", num);
    ret.append(":");
    ret.append(buf);
}
