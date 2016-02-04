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

class CmdInfo {
public:
    CmdInfo(const std::string _name, int _num, int16_t _flag) : name_(_name), arity_(_num), flag_(_flag) {}
    bool CheckArg(int num) const {
        if ((arity_ > 0 && num != arity_) || (arity_ < 0 && num < -arity_)) {
            return false;
        }
        return true;
    }
    int16_t flag_rw() const { 
        return flag_ & CmdFlagsMaskRW; 
    }
    int16_t flag_type() const {
        return flag_ & CmdFlagsMaskType;
    }
    int16_t flag_local() const {
        return flag_ & CmdFlagsMaskLocal;
    }
    int16_t flag_prior() const {
        return flag_ & CmdFlagsMaskPrior;
    }
    std::string name() const {
        return name_;
    }
private:
    std::string name_;
    int arity_;
    int flag_;
};

class Cmd {
public:
    virtual ~Cmd() {}
    virtual int16_t Do(PikaCmdArgsType &argvs, std::string &ret) = 0;
    virtual const CmdInfo& cmd_info() = 0;

protected:
    virtual bool Initial(PikaCmdArgsType &argvs, std::string &ret) = 0;
};

Cmd* getCmd(const std::string& opt);
std::string PopArg(PikaCmdArgsType& arg, bool keyword = false);

class CmdHolder : public Cmd{
public:
    explicit CmdHolder(const std::string& opt) : valid_(true){
        cmd_ = getCmd(opt);
        if (cmd_ == NULL) {
            valid_ = false;
        }
    }
    ~CmdHolder() {
        if (valid_) delete cmd_;
    }
    int16_t Do(PikaCmdArgsType &argvs, std::string &ret) {
        return cmd_->Do(argvs, ret);
    }
    const CmdInfo& cmd_info() {
        return cmd_->cmd_info();
    }
    bool valid() {
        return valid_;
    }
    
private:
    Cmd* cmd_;
    bool valid_;
};


#endif
