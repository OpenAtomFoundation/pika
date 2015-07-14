#include "pika_command.h"
#include "pika_server.h"
#include <algorithm>
#include <map>

extern PikaServer *g_pikaServer;
extern std::map<std::string, Cmd *> g_pikaCmd;

void HGetallCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && argv.size() != arity) || (arity < 0 && argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::vector<nemo::FV> fvs;
    nemo::Status s = g_pikaServer->GetHandle()->HGetall(key, fvs);
    if (s.ok()) {
        char buf[32];
        snprintf(buf, sizeof(buf), "*%d\r\n", (int)fvs.size() * 2);
        ret.append(buf);
        std::vector<nemo::FV>::iterator iter;
        for (iter = fvs.begin(); iter != fvs.end(); iter++) {
            snprintf(buf, sizeof(buf), "$%d\r\n", iter->field.size());
            ret.append(buf);
            ret.append(iter->field.data(), iter->field.size());
            ret.append("\r\n");
            snprintf(buf, sizeof(buf), "$%d\r\n", iter->val.size());
            ret.append(buf);
            ret.append(iter->val.data(), iter->val.size());
            ret.append("\r\n");
        }
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}
