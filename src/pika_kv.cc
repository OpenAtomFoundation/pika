#include "nemo.h"
#include "pika_command.h"
//#include "pika_server.h"
#include <algorithm>

//class PikaServer {
//public:
//    nemo::Nemo* GetHandle() {return db_;}
//private:
//    nemo::Nemo *db_;
//};
//extern int string2l(const char *s, size_t slen, long *value);
extern PikaServer *g_pikaServer;

bool SetCmd::Initial(PikaCmdArgsType &argv, std::string &ret) {
    if (!CheckArg(argv)) {
        ret = "-ERR wrong number of arguments for " + name() + " command\r\n";
        return false;
    }
    key_ = PopArg(argv);
    value_ = PopArg(argv);
    while (argv.size() > 0) {
        std::string opt = PopArg(argv, true);
        if (opt == "xx") {
            condition_ = SetCmd::XX;
        } else if (opt == "nx") {
            condition_ = SetCmd::NX;
        } else if (opt == "ex") {
            if (argv.size() < 1) {
                ret = "-ERR syntax error\r\n";
                return false;
            }
            std::string str_sec = PopArg(argv);
            if (!string2l(str_sec.data(), str_sec.size(), &sec_)) {
                ret = "-ERR value is not an integer or out of range\r\n";
                return false;
            }
        } else {
            ret = "-ERR syntax error\r\n";
            return false;
        }
    }
    return true;
}

int16_t SetCmd::Do(PikaCmdArgsType &argv, std::string &ret) {
    bool ex_ret = Initial(argv, ret);
    if (!ex_ret) return CmdResFail;

    nemo::Status s;
    int64_t res = 1;
    switch (condition_){
        case SetCmd::XX:
            s = g_pikaServer->GetHandle()->Setxx(key_, value_, &res, sec_);
            break;
        case SetCmd::NX:
            s = g_pikaServer->GetHandle()->Setnx(key_, value_, &res, sec_);
            break;
        default:
            s = g_pikaServer->GetHandle()->Set(key_, value_, sec_);
    }

    if (s.ok() || s.IsNotFound()) {
        if (res == 1) {
            ret = "+OK\r\n";
        } else {
            ret = "*-1\r\n";
        }
    } else {
        ret.append("-ERR " + s.ToString() + "\r\n");
    }
    return CmdResOk;
}

