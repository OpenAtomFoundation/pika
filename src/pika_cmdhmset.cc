#include "pika_command.h"
#include "pika_server.h"
#include <algorithm>
#include <map>

extern PikaServer *g_pikaServer;
extern std::map<std::string, Cmd *> g_pikaCmd;

void HMSetCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && argv.size() != arity) || (arity < 0 && argv.size() < -arity) || argv.size() % 2 != 0) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::string field;
    std::string value;
    std::vector<nemo::FV> fvs;
    while (!argv.empty()) {
        field = argv.front(); 
        argv.pop_front();
        value = argv.front(); 
        argv.pop_front();
        fvs.push_back(nemo::FV{field, value});
    }
    nemo::Status s = g_pikaServer->GetHandle()->HMSet(key, fvs);
    if (s.ok()) {
        ret = "+OK\r\n";
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}
