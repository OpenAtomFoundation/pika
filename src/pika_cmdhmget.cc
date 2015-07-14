#include "pika_command.h"
#include "pika_server.h"
#include <algorithm>
#include <map>

extern PikaServer *g_pikaServer;
extern std::map<std::string, Cmd *> g_pikaCmd;

void HMGetCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && argv.size() != arity) || (arity < 0 && argv.size() < -arity)) {
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
    std::vector<std::string> fields;
    std::vector<nemo::FVS> fvss;
    while (!argv.empty()) {
        fields.push_back(argv.front()); 
        argv.pop_front();
    }
    nemo::Status s = g_pikaServer->GetHandle()->HMGet(key, fields, fvss);
    if (s.ok()) {
        std::vector<nemo::FVS>::iterator iter;
        char buf[32];
        snprintf(buf, sizeof(buf), "*%d\r\n", (int)fvss.size());
        ret.append(buf);
        for (iter = fvss.begin(); iter != fvss.end(); iter++) {
            if (iter->status.ok()) {
                snprintf(buf, sizeof(buf), "$%d\r\n", iter->val.size());
                ret.append(buf);
                ret.append(iter->val.data(), iter->val.size());
                ret.append("\r\n");
            } else {
                ret.append("$-1\r\n");
            }
        }
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}
