#ifndef __PIKA_KV_H__
#define __PIKA_KV_H__
#include "pika_command.h"


/*
 * kv
 */
class SetCmd : public Cmd {
public:
    enum SetCondition{ANY, NX, XX};
    SetCmd() : sec_(0), condition_(ANY) {};
    int16_t Do(PikaCmdArgsType &argvs, std::string &ret);
    const CmdInfo& cmd_info() {
        return info_;
    }
private:
    static const CmdInfo info_;
    std::string key_;
    std::string value_;
    int64_t sec_;
    SetCmd::SetCondition condition_;
    bool Initial(PikaCmdArgsType &argvs, std::string &ret);
};

#endif
