#include "pika_command.h"
#include "pika_server.h"
#include "util.h"
#include <algorithm>
#include <map>

extern PikaServer *g_pikaServer;
extern std::map<std::string, Cmd *> g_pikaCmd;

void LIndexCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::string index = argv.front();
    argv.pop_front();
    std::string value;
    int64_t l_index = 0;
    if (!string2l(index.data(), index.size(), &l_index)) {
        ret = "-ERR value is not an integer or out of range\r\n";
        return;
    }
    nemo::Status s = g_pikaServer->GetHandle()->LIndex(key, l_index, &value);
    if (s.ok()) {
        char buf[32];
        snprintf(buf, sizeof(buf), "$%lu\r\n", value.size());
        ret.append(buf);
        ret.append(value.data(), value.size());
        ret.append("\r\n");
    } else if (s.IsNotFound() || (s.IsCorruption() && s.ToString() == "Corruption: index out of range")) {
        ret = "$-1\r\n";
    } else {
        ret = "+ERR ";
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void LInsertCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::string dir = argv.front();
    argv.pop_front();
    std::string pivot = argv.front();
    argv.pop_front();
    std::string value = argv.front();
    argv.pop_front();
    transform(dir.begin(), dir.end(), dir.begin(), ::tolower);
    if (dir != "before" && dir != "after") {
        ret = "-ERR syntax error\r\n";
    }
    nemo::Status s;
    int64_t llen;
    if (dir == "before") {
        s = g_pikaServer->GetHandle()->LInsert(key, nemo::BEFORE, pivot, value, &llen);
    } else if (dir == "after") {
        s = g_pikaServer->GetHandle()->LInsert(key, nemo::AFTER, pivot, value, &llen);
    } else {
        ret = "-ERR syntax error\r\n";
        return;
    }
    if (!s.IsCorruption()) {
        char buf[32];
        snprintf(buf, sizeof(buf), ":%ld\r\n", llen);
        ret.append(buf);
    } else {
        ret = "+ERR ";
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void LLenCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    int64_t llen;
    nemo::Status s = g_pikaServer->GetHandle()->LLen(key, &llen);
    if (s.IsCorruption()) {
        ret = "+ERR ";
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    } else {
        char buf[32];
        snprintf(buf, sizeof(buf), ":%ld\r\n", llen);
        ret.append(buf);
    }
}

void LPopCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::string value;
    nemo::Status s = g_pikaServer->GetHandle()->LPop(key, &value);
    if (s.ok()) {
        char buf[32];
        snprintf(buf, sizeof(buf), "$%lu\r\n", value.size());
        ret.append(buf);
        ret.append(value.data(), value.size());
        ret.append("\r\n");
    } else if (s.IsNotFound()) {
        ret.append("$-1\r\n");
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void LPushCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::string value;
    nemo::Status s;
    int64_t llen = 0;
    while (argv.size() > 0) {
        value = argv.front();
        s = g_pikaServer->GetHandle()->LPush(key, value, &llen);
        if (s.IsCorruption()) {
            break;
        }
        argv.pop_front();
    }
    if (argv.size() > 0) {
        ret = "+ERR ";
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    } else {
        char buf[32];
        snprintf(buf, sizeof(buf), ":%ld\r\n", llen);
        ret.append(buf);
    }
}

void LPushxCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::string value = argv.front();
    argv.pop_front();
    int64_t llen = 0;
    nemo::Status s = g_pikaServer->GetHandle()->LPushx(key, value, &llen);
    if (s.IsCorruption()) {
        ret = "+ERR ";
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    } else {
        char buf[32];
        snprintf(buf, sizeof(buf), ":%ld\r\n", llen);
        ret.append(buf);
    }
}

void LRangeCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::string left = argv.front();
    argv.pop_front();
    std::string right = argv.front();
    argv.pop_front();
    int64_t l_left, l_right;
    if (!string2l(left.data(), left.size(), &l_left)) {
        ret = "-ERR value is not an integer or out of range\r\n";
        return;
    }
    if (!string2l(right.data(), right.size(), &l_right)) {
        ret = "-ERR value is not an integer or out of range\r\n";
        return;
    }
    std::vector<nemo::IV> ivs;
    nemo::Status s = g_pikaServer->GetHandle()->LRange(key, l_left, l_right, ivs);
    if (s.ok()) {
        char buf[32];
        snprintf(buf, sizeof(buf), "*%lu\r\n", ivs.size());
        ret.append(buf);
        std::vector<nemo::IV>::iterator iter;
        for (iter = ivs.begin(); iter != ivs.end(); iter++) {
            snprintf(buf, sizeof(buf), "$%lu\r\n", iter->val.size());
            ret.append(buf);
            ret.append(iter->val.data(), iter->val.size());
            ret.append("\r\n");
        }    
    } else if (s.IsNotFound()) {
        ret = "*0\r\n";
    } else {
        ret = "+ERR ";
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void LRemCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::string str_count = argv.front();
    argv.pop_front();
    std::string value = argv.front();
    argv.pop_front();
    int64_t count;
    if (!string2l(str_count.data(), str_count.size(), &count)) {
        ret = "-ERR value is not an integer or out of range\r\n";
        return;
    }
    int64_t res;
    nemo::Status s = g_pikaServer->GetHandle()->LRem(key, count, value, &res);
    if (!s.IsCorruption()) {
        char buf[32];
        snprintf(buf, sizeof(buf), ":%ld\r\n", res);
        ret.append(buf);
    } else {
        ret = "+ERR ";
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void LSetCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::string index = argv.front();
    argv.pop_front();
    int64_t l_index = 0;
    if (!string2l(index.data(), index.size(), &l_index)) {
        ret = "-ERR value is not an integer or out of range\r\n";
        return;
    }
    std::string new_val = argv.front();
    argv.pop_front();

    nemo::Status s = g_pikaServer->GetHandle()->LSet(key, l_index, new_val);
    if (s.ok()) {
        ret = "+OK\r\n";
    } else if (s.IsNotFound()) {
        ret = "+ERR no such key\r\n";
    } else if (s.IsCorruption() && s.ToString() == "Corruption: index out of range") {
        ret = "+ERR index out of range\r\n";
    } else {
        ret = "+ERR ";
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void LTrimCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::string left = argv.front();
    argv.pop_front();
    std::string right = argv.front();
    argv.pop_front();
    int64_t l_left, l_right;
    if (!string2l(left.data(), left.size(), &l_left)) {
        ret = "-ERR value is not an integer or out of range\r\n";
        return;
    }
    if (!string2l(right.data(), right.size(), &l_right)) {
        ret = "-ERR value is not an integer or out of range\r\n";
        return;
    }
    
    nemo::Status s = g_pikaServer->GetHandle()->LTrim(key, l_left, l_right);
    if (s.ok() || s.IsNotFound()) {
        ret = "+OK\r\n";
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void RPopCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::string value;
    nemo::Status s = g_pikaServer->GetHandle()->RPop(key, &value);
    if (s.ok()) {
        char buf[32];
        snprintf(buf, sizeof(buf), "$%lu\r\n", value.size());
        ret.append(buf);
        ret.append(value.data(), value.size());
        ret.append("\r\n");
    } else if (s.IsNotFound()) {
        ret.append("$-1\r\n");
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void RPopLPushCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string src = argv.front();
    argv.pop_front();
    std::string dest = argv.front();
    argv.pop_front();
    std::string value;
    nemo::Status s = g_pikaServer->GetHandle()->RPopLPush(src, dest, value);
    if (s.ok()) {
        char buf[32];
        snprintf(buf, sizeof(buf), "$%lu\r\n", value.size());
        ret.append(buf);
        ret.append(value.data(), value.size());
        ret.append("\r\n");
    } else if (s.IsNotFound() && s.ToString() == "NotFound: not found the source key") {
        ret.append("$-1\r\n");
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void RPushCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::string value;
    nemo::Status s;
    int64_t llen = 0;
    while (argv.size() > 0) {
        value = argv.front();
        s = g_pikaServer->GetHandle()->RPush(key, value, &llen);
        if (s.IsCorruption()) {
            break;
        }
        argv.pop_front();
    }
    if (argv.size() > 0) {
        ret = "+ERR ";
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    } else {
        char buf[32];
        snprintf(buf, sizeof(buf), ":%ld\r\n", llen);
        ret.append(buf);
    }
}

void RPushxCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::string value = argv.front();
    argv.pop_front();
    int64_t llen = 0;
    nemo::Status s = g_pikaServer->GetHandle()->RPushx(key, value, &llen);
    if (s.IsCorruption()) {
        ret = "+ERR ";
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    } else {
        char buf[32];
        snprintf(buf, sizeof(buf), ":%ld\r\n", llen);
        ret.append(buf);
    }
}
