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
    virtual int Do(PikaCmdArgsType &argvs, std::string &ret);
private:
    std::string key_;
    std::string value_;
    long sec_;
    SetCmd::SetCondition condition_;
    virtual bool Initial(PikaCmdArgsType &argvs, std::string &ret);
};

#endif
