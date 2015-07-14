#include "pika_command.h"
#include "pika_server.h"
#include <algorithm>
#include <map>

extern PikaServer *g_pikaServer;
extern std::map<std::string, Cmd *> g_pikaCmd;

void HExistsCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && argv.size() != arity) || (arity < 0 && argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::string field = argv.front();
    argv.pop_front();
    bool res = g_pikaServer->GetHandle()->HExists(key, field);
    if (res) {
        ret = ":1\r\n";
    } else {
        ret = ":0\r\n";
    }
}
