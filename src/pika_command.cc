#include <algorithm>
#include <map>
#include "pika_kv.h"

static std::map<std::string, CmdInfoPtr> cmd_infos;    /* Table for CmdInfo */

void InitCmdInfoTable() {
    CmdInfoPtr setptr(new CmdInfo(kCmdNameSet, 
                -2, kCmdFlagsWrite | kCmdFlagsKv));
    cmd_infos.insert(std::pair<std::string, CmdInfoPtr>(kCmdNameSet, setptr));
}

CmdInfoPtr GetCmdInfo(const std::string& opt) {
    std::map<std::string, CmdInfoPtr>::const_iterator it = cmd_infos.find(opt);
    if (it != cmd_infos.end()) {
        return it->second;
    }
    return CmdInfoPtr();
}

void InitCmdTable(std::map<std::string, CmdPtr> *cmd_table) {
   CmdPtr setptr(new SetCmd());
   cmd_table->insert(std::pair<std::string, CmdPtr>(kCmdNameSet, setptr));
}

CmdPtr GetCmdFromTable(const std::string& opt, 
        const std::map<std::string, CmdPtr> &cmd_table) {
    std::map<std::string, CmdPtr>::const_iterator it = cmd_table.find(opt);
    if (it != cmd_table.end()) {
        return it->second;
    }
    return CmdPtr();
}

std::string PopArg(PikaCmdArgsType& argv, bool keyword) {
    std::string next = argv.front();
    argv.pop_front();
    if (keyword) {
        transform(next.begin(), next.end(), next.begin(), ::tolower);
    }
    return next;
}

