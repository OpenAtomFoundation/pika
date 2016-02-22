#ifndef __PIKA_COMMAND_H__
#define __PIKA_COMMAND_H__

#include <deque>
#include <string>
#include <memory>
#include <redis_conn.h>

//#include "pika_client_conn.h"

//Constant for command name
const std::string kCmdNameSet = "set";

typedef pink::RedisCmdArgsType PikaCmdArgsType;

enum CmdFlagsMask {
    kCmdFlagsMaskRW         = 1,
    kCmdFlagsMaskType       = 14,
    kCmdFlagsMaskLocal      = 16,
    kCmdFlagsMaskSuspend    = 32,
    kCmdFlagsMaskPrior      = 64
};

enum CmdFlags {
    kCmdFlagsRead           = 0,
    kCmdFlagsWrite          = 1,
    kCmdFlagsAuth           = 0,
    kCmdFlagsKv             = 2,
    kCmdFlagsHash           = 4,
    kCmdFlagsList           = 6,
    kCmdFlagsSet            = 8,
    kCmdFlagsZset           = 10,
    kCmdFlagsNoLocal        = 0,
    kCmdFlagsLocal          = 16,
    kCmdFlagsNoSuspend      = 0,
    kCmdFlagsSuspend        = 32,
    kCmdFlagsNoPrior        = 0,
    kCmdFlagsPrior          = 64
};

enum CmdRes {
    kCmdResOk = 0,
    kCmdResFail,
};

class CmdInfo {
public:
    CmdInfo(const std::string _name, int _num, uint16_t _flag)
        : name_(_name), arity_(_num), flag_(_flag) {}
    bool CheckArg(int num) const {
        if ((arity_ > 0 && num != arity_) || (arity_ < 0 && num < -arity_)) {
            return false;
        }
        return true;
    }
    bool is_write() const {
        return ((flag_ & kCmdFlagsMaskRW) == kCmdFlagsWrite);
    }
    uint16_t flag_type() const {
        return flag_ & kCmdFlagsMaskType;
    }
    bool is_local() const {
        return ((flag_ & kCmdFlagsMaskLocal) == kCmdFlagsLocal);
    }
    // Others need to be suspended when a suspend command run
    bool is_suspend() const {
        return ((flag_ & kCmdFlagsMaskSuspend) == kCmdFlagsSuspend);
    }
    bool is_prior() const {
        return ((flag_ & kCmdFlagsMaskPrior) == kCmdFlagsPrior);
    }
    std::string name() const {
        return name_;
    }
private:
    std::string name_;
    int arity_;
    uint16_t flag_;

    CmdInfo(const CmdInfo&);
    CmdInfo& operator=(const CmdInfo&);
};

typedef std::shared_ptr<const CmdInfo> CmdInfoPtr;

class Cmd {
public:
    Cmd() {}
    virtual ~Cmd() {}
    virtual int Do(PikaCmdArgsType &argvs, std::string &ret) = 0;

protected:
    virtual bool Initial(PikaCmdArgsType &argvs, std::string &ret) = 0;

private:
    Cmd(const Cmd&);
    Cmd& operator=(const Cmd&);
};

typedef std::shared_ptr<Cmd> CmdPtr;

// Method for CmdInfo Table
void InitCmdInfoTable();
CmdInfoPtr GetCmdInfo(const std::string& opt);
// Method for Cmd Table
void InitCmdTable(std::map<std::string, CmdPtr> *cmd_table);
CmdPtr GetCmdFromTable(const std::string& opt, 
        const std::map<std::string, CmdPtr> &cmd_table);

std::string PopArg(PikaCmdArgsType& arg, bool keyword = false);

#endif
