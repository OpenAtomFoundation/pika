#include "slash_string.h"
#include "nemo.h"
#include "pika_kv.h"
#include "pika_server.h"

extern PikaServer *g_pika_server;

void SetCmd::Initial(PikaCmdArgsType &argv, CmdRes &ret) {
    if (!GetCmdInfo(kCmdNameSet)->CheckArg(argv.size())) {
        ret.SetErr("wrong number of arguments for " + 
            GetCmdInfo(kCmdNameSet)->name() + " command");
        return;
    }
    PikaCmdArgsType::iterator it = argv.begin() + 1; //Remember the first args is the opt name
    key_ = *it++;
    value_ = *it++;
    while (it != argv.end()) {
        std::string opt = slash::StringToLower(*it++);
        if (opt == "xx") {
            condition_ = SetCmd::kXX;
        } else if (opt == "nx") {
            condition_ = SetCmd::kNX;
        } else if (opt == "ex") {
            if (it == argv.end()) {
                ret.SetErr("syntax error");
                return;
            }
            if (!slash::string2l((*it).data(), (*it).size(), &sec_)) {
                ret.SetErr("value is not an integer or out of range");
                return;
            }
            ++it;
        } else {
            ret.SetErr("syntax error");
            return;
        }
    }
}

void SetCmd::Do(PikaCmdArgsType &argv, CmdRes &ret) {
    Initial(argv, ret);
    if (!ret.ok()) {
        return;
    }

    nemo::Status s;
    int64_t res = 1;
    switch (condition_) {
    case SetCmd::kXX:
        s = g_pika_server->db()->Setxx(key_, value_, &res, sec_);
        break;
    case SetCmd::kNX:
        s = g_pika_server->db()->Setnx(key_, value_, &res, sec_);
        break;
    default:
        s = g_pika_server->db()->Set(key_, value_, sec_);
        break;
    }

    if (s.ok() || s.IsNotFound()) {
        if (res == 1) {
            ret.SetContent("+OK");
        } else {
            ret.AppendArrayLen(-1);;
        }
    } else {
        ret.SetErr(s.ToString());
    }
}

void GetCmd::Initial(PikaCmdArgsType &argv, CmdRes &ret) {
    if (!GetCmdInfo(kCmdNameGet)->CheckArg(argv.size())) {
        ret.SetErr("wrong number of arguments for " + 
            GetCmdInfo(kCmdNameGet)->name() + " command");
        return;
    }
    key_ = argv[1];
    return;
}

void GetCmd::Do(PikaCmdArgsType &argv, CmdRes &ret) {
    Initial(argv, ret);
    if (!ret.ok()) {
        return;
    }
    std::string value;
    nemo::Status s = g_pika_server->db()->Get(key_, &value);
    if (s.ok()) {
        ret.AppendStringLen(value.size());
        ret.AppendContent(value);
    } else if (s.IsNotFound()) {
        ret.AppendStringLen(-1);
    } else {
        ret.SetErr(s.ToString());
    }
}
