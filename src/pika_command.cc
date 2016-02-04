#include <algorithm>
#include "pika_kv.h"

const CmdInfo SetCmd::info_(CmdNameSet, -2, CmdFlagsWrite | CmdFlagsKv);

Cmd* getCmd(const std::string& opt) {
    Cmd* cmd = NULL;
    if (opt == CmdNameSet) {
        cmd = new SetCmd();
    } else {
        //TODO error command
    }
    return cmd;
}

std::string PopArg(PikaCmdArgsType& argv, bool keyword) {
    std::string next = argv.front();
    argv.pop_front();
    if (keyword) {
        transform(next.begin(), next.end(), next.begin(), ::tolower);
    }
    return next;
}

