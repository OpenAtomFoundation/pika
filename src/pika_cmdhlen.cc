#include "pika_command.h"
#include "pika_server.h"
#include <algorithm>
#include <map>

extern PikaServer *g_pikaServer;
extern std::map<std::string, Cmd *> g_pikaCmd;

void HLenCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && argv.size() != arity) || (arity < 0 && argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    int res = g_pikaServer->GetHandle()->HLen(key);
    if (res) {
        char buf[32];
        snprintf(buf, sizeof(buf), ":%d\r\n", res);
        ret.append(buf);
    } else if (res == 0) {
        ret.append(":0\r\n");
    } else {
        ret.append("-ERR something wrong in hlen\r\n");
    }
}
