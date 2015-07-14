#include "pika_command.h"
#include "pika_server.h"
#include <algorithm>
#include <map>

extern PikaServer *g_pikaServer;
extern std::map<std::string, Cmd *> g_pikaCmd;

void HSetnxCmd::Do(std::list<std::string> &argv, std::string &ret) {
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
    std::string value = argv.front();
    argv.pop_front();

    std::string res;
    nemo::Status s = g_pikaServer->GetHandle()->HSetnx(key, field, value);
    if (s.ok()) {
        ret = ":1\r\n";
    } else if (s.IsCorruption() && s.ToString() == "Corruption: Already Exist") {
        ret = ":0\r\n";
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}
