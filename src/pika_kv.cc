#include <algorithm>
#include "slash_string.h"
#include "nemo.h"
#include "pika_kv.h"
#include "pika_server.h"

extern PikaServer *g_pika_server;

bool SetCmd::Initial(PikaCmdArgsType &argv, std::string &ret) {
    if (!GetCmdInfo(kCmdNameSet)->CheckArg(argv.size())) {
        ret = "-ERR wrong number of arguments for " + GetCmdInfo(kCmdNameSet)->name() + " command\r\n";
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

int SetCmd::Do(PikaCmdArgsType &argv, std::string &ret) {
    bool ex_ret = Initial(argv, ret);
    if (!ex_ret) return kCmdResFail;

    nemo::Status s;
    int64_t res = 1;
    switch (condition_){
    case SetCmd::XX:
        s = g_pika_server->db()->Setxx(key_, value_, &res, sec_);
        break;
    case SetCmd::NX:
        s = g_pika_server->db()->Setnx(key_, value_, &res, sec_);
        break;
    default:
        s = g_pika_server->db()->Set(key_, value_, sec_);
        break;
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
    return kCmdResOk;
}

