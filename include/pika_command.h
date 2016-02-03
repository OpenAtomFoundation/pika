#ifndef __PIKA_COMMAND_H__
#define __PIKA_COMMAND_H__

#include <deque>
#include <string>
//#include "pika_client_conn.h"

//typedef pink::RedisConnArgsType PikaCmdArgsType
typedef std::deque<std::string> PikaCmdArgsType;

enum CmdFlagsMask {
    CmdFlagsMaskRW         = 1,
    CmdFlagsMaskType       = 14,
    CmdFlagsMaskLocal      = 16,
    CmdFlagsMaskPrior      = 32
};

enum CmdFlags {
    CmdFlagsRead           = 0,
    CmdFlagsWrite          = 1,
    CmdFlagsAuth           = 0,
    CmdFlagsKv             = 2,
    CmdFlagsHash           = 4,
    CmdFlagsList           = 6,
    CmdFlagsSet            = 8,
    CmdFlagsZset           = 10,
    CmdFlagsNoLocal        = 0,
    CmdFlagsLocal          = 16,
    CmdFlagsNoPrior        = 0,
    CmdFlagsPrior          = 32
};

enum CmdRes {
    CmdResOk = 0,
    CmdResFail,
};

static const std::string CmdNameSet = "set";



class Cmd {
public:
    Cmd(const std::string _name, int _num, int16_t _flag) : name_(_name), arity_(_num), flag_(_flag) {};
    Cmd() {};
    virtual ~Cmd() = 0;
    virtual int16_t Do(PikaCmdArgsType &argvs, std::string &ret) = 0;
    int16_t flag()    { return flag_; }
    std::string name()      { return name_; }

protected:
    virtual bool Initial(PikaCmdArgsType &argvs, std::string &ret) = 0;
    std::string PopArg(PikaCmdArgsType& arg, bool keyword = false);
    bool CheckArg(const PikaCmdArgsType& argv);

private:
    std::string name_;
    int arity_;
    int16_t flag_;
};

class CmdGenerator {
public:
    static Cmd* getCmd(const std::string& opt);
};

class CmdHolder : public Cmd{
public:
    explicit CmdHolder(const std::string& opt) : valid_(true){
        cmd_ = CmdGenerator::getCmd(opt);
        if (cmd_ == NULL) { valid_ = false; }
    }
    ~CmdHolder() {
        if (valid_) delete cmd_;
    }
    int16_t Do(PikaCmdArgsType &argvs, std::string &ret) {
            return cmd_->Do(argvs, ret);
    }
    int16_t flag() { return cmd_->flag(); }
    std::string name() { return cmd_->name(); }
    bool valid() { return valid_; }
    
private:
    Cmd* cmd_;
    bool valid_;
};

/*
 * kv
 */
class SetCmd : public Cmd {
public:
    enum SetCondition{ANY, NX, XX};
    SetCmd() : Cmd(CmdNameSet, -2, CmdFlagsWrite | CmdFlagsKv), sec_(0), condition_(ANY) {};
    int16_t Do(PikaCmdArgsType &argvs, std::string &ret);
private:
    std::string key_;
    std::string value_;
    int64_t sec_;
    SetCmd::SetCondition condition_;
    bool Initial(PikaCmdArgsType &argvs, std::string &ret);
};

#endif
