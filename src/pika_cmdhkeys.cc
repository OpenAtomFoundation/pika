#include "pika_command.h"
#include "pika_server.h"
#include <algorithm>
#include <map>

extern PikaServer *g_pikaServer;
extern std::map<std::string, Cmd *> g_pikaCmd;

void HKeysCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && argv.size() != arity) || (arity < 0 && argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::vector<std::string> fields;
    nemo::Status s = g_pikaServer->GetHandle()->HKeys(key, fields);
    if (s.ok()) {
        char buf[32];
        snprintf(buf, sizeof(buf), "*%d\r\n", (int)fields.size());
        ret.append(buf);
        std::vector<std::string>::iterator iter;
        for (iter = fields.begin(); iter != fields.end(); iter++) {
            snprintf(buf, sizeof(buf), "$%d\r\n", iter->size());
            ret.append(buf);
            ret.append(iter->data(), iter->size());
            ret.append("\r\n");
        }
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}
