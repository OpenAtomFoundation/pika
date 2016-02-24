#ifndef __PIKA_KV_H__
#define __PIKA_KV_H__
#include "pika_command.h"

/*
 * kv
 */
class SetCmd : public Cmd {
public:
    enum SetCondition{kANY, kNX, kXX};
    SetCmd() : sec_(0), condition_(kANY) {};
    virtual void Do(PikaCmdArgsType &argvs, CmdRes &ret);
private:
    std::string key_;
    std::string value_;
    int64_t sec_;
    SetCmd::SetCondition condition_;
    virtual void Initial(PikaCmdArgsType &argvs, CmdRes &ret);
};

class GetCmd : public Cmd {
public:
    GetCmd() {};
    virtual void Do(PikaCmdArgsType &argvs, CmdRes &ret);
private:
    std::string key_;
    virtual void Initial(PikaCmdArgsType &argvs, CmdRes &ret);
};
#endif
