#include <algorithm>
#include "pika_command.h"

Cmd* CmdGenerator::getCmd(const std::string& opt) {
    Cmd* cmd = NULL;
    if (opt == CmdNameSet) {
        cmd = new SetCmd();
    } else {
        //TODO error command
    }
    return cmd;
}
std::string Cmd::PopArg(PikaCmdArgsType& argv, bool keyword) {
    std::string next = argv.front();
    argv.pop_front();
    if (keyword) {
        transform(next.begin(), next.end(), next.begin(), ::tolower);
    }
    return next;
}

bool Cmd::CheckArg(const PikaCmdArgsType& argv) {
    if ((arity_ > 0 && (int)argv.size() != arity_) ||
            (arity_ < 0 && (int)argv.size() < -arity_)) {
        return false;
    }
    return true;
}

