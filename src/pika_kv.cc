#include "pika_command.h"
#include "pika_server.h"
#include "util.h"
#include <algorithm>
#include <map>

extern PikaServer *g_pikaServer;
extern std::map<std::string, Cmd *> g_pikaCmd;

void SetCmd::Do(std::list<std::string> &argv, std::string &ret) {
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
    if (argv.size() > 0) {
        std::string opt = argv.front();
        argv.pop_front();
        transform(opt.begin(), opt.end(), opt.begin(), ::tolower);
        if (opt == "xx") {
            ret = "+OK\r\n";
            return;
        } else if (opt == "nx" && argv.empty()) {
            ret = "+OK\r\n";
        } else if (opt == "ex") {
            if (argv.size() != 1) {
                ret = "-ERR syntax error\r\n";
                return;
            }
            ret = "+OK\r\n";
        } else if (opt == "px") {
            if (argv.size() != 1) {
                ret = "-ERR syntax error\r\n";
                return;
            }
            ret = "+OK\r\n";
        } else {
            ret = "-ERR syntax error\r\n";
            return;
        }
    }
    nemo::Status s = g_pikaServer->GetHandle()->Set(key, value);
    if (s.ok()) {
        ret = "+OK\r\n";
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void GetCmd::Do(std::list<std::string> &argv, std::string &ret) {
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
    nemo::Status s = g_pikaServer->GetHandle()->Get(key, &value);
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

void DelCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::vector<std::string> keys;
    while (argv.size()) {
        keys.push_back(argv.front());
        argv.pop_front();
    }
    int64_t count = 0;
    nemo::Status s = g_pikaServer->GetHandle()->MDel(keys, &count);
    if (s.ok()) {
        char buf[32];
        snprintf(buf, sizeof(buf), ":%ld\r\n", count);
        ret.append(buf);
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void IncrCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::string new_val;
    nemo::Status s = g_pikaServer->GetHandle()->Incrby(key, 1, new_val);
    if (s.ok() || s.IsNotFound()) {
        ret = ":";
        ret.append(new_val);
        ret.append("\r\n");
    } else if (s.IsCorruption() && s.ToString() == "Corruption: value is not a integer") {
        ret = "-ERR value is not an integer or out of range\r\n";
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void IncrbyCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::string str_by = argv.front();
    argv.pop_front();
    int64_t by; 
    if (!string2l(str_by.data(), str_by.size(), &by)) {
        ret = "-ERR value is not an integer or out of range\r\n";
        return;
    }
    std::string new_val;
    nemo::Status s = g_pikaServer->GetHandle()->Incrby(key, by, new_val);
    if (s.ok() || s.IsNotFound()) {
        ret = ":";
        ret.append(new_val);
        ret.append("\r\n");
    } else if (s.IsCorruption() && s.ToString() == "Corruption: value is not a integer") {
        ret = "-ERR value is not an integer or out of range\r\n";
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void IncrbyfloatCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::string str_by = argv.front();
    argv.pop_front();
    double by; 
    if (!string2d(str_by.data(), str_by.size(), &by)) {
        ret = "-ERR value is not an float\r\n";
        return;
    }
    std::string new_val;
    nemo::Status s = g_pikaServer->GetHandle()->Incrbyfloat(key, by, new_val);
    if (s.ok() || s.IsNotFound()) {
        char buf[32];
        snprintf(buf, sizeof(buf), "$%lu\r\n", new_val.size());
        ret.append(buf);
        ret.append(new_val.data(), new_val.size());
        ret.append("\r\n");
    } else if (s.IsCorruption() && s.ToString() == "Corruption: value is not a float") {
        ret = "-ERR value is not an float\r\n";
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void DecrCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::string new_val;
    nemo::Status s = g_pikaServer->GetHandle()->Decrby(key, 1, new_val);
    if (s.ok() || s.IsNotFound()) {
        ret = ":";
        ret.append(new_val);
        ret.append("\r\n");
    } else if (s.IsCorruption() && s.ToString() == "Corruption: value is not a integer") {
        ret = "-ERR value is not an integer or out of range\r\n";
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void DecrbyCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::string str_by = argv.front();
    argv.pop_front();
    int64_t by; 
    if (!string2l(str_by.data(), str_by.size(), &by)) {
        ret = "-ERR value is not an integer or out of range\r\n";
        return;
    }
    std::string new_val;
    nemo::Status s = g_pikaServer->GetHandle()->Decrby(key, by, new_val);
    if (s.ok() || s.IsNotFound()) {
        ret = ":";
        ret.append(new_val);
        ret.append("\r\n");
    } else if (s.IsCorruption() && s.ToString() == "Corruption: value is not a integer") {
        ret = "-ERR value is not an integer or out of range\r\n";
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void ScanCmd::Do(std::list<std::string> &argv, std::string &ret) {
    int size = argv.size();    
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity) || (size != 2 && size != 4 && size != 6)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string cursor = argv.front();
    argv.pop_front();

    long count = 10;
    std::string str_count;
    std::string ar;
    std::string pattern;
    bool use_pat = false;
    while (argv.size()) {
        ar = argv.front();
        argv.pop_front();
        transform(ar.begin(), ar.end(), ar.begin(), ::tolower);
        if (ar == "match") {
            use_pat = true;
            pattern = argv.front();
            argv.pop_front();
        } else if (ar == "count") {
            str_count = argv.front();
            argv.pop_front();
            if (!string2l(str_count.data(), str_count.size(), &count)) {
                ret = "-ERR value is not an integer or out of range\r\n";
                return;
            }
            if (count <= 0) {
                ret = "-ERR syntax error\r\n";
                return;
            }
        } else {
            ret = "-ERR syntax error\r\n";
            return;
        }
    }
    long index = 0;
    if (!string2l(cursor.data(), cursor.size(), &index)) {
        ret = "-ERR invalid cursor\r\n";
        return;
    };


    std::vector<std::string> keys;
    nemo::KIterator *iter = g_pikaServer->GetHandle()->Scan("", "", -1);
    iter->Skip(index);
    bool iter_ret = false;
    while ((iter_ret=iter->Next()) && count) {
        count--;
        index++;
        if (use_pat == true && !stringmatchlen(pattern.data(), pattern.size(), iter->Key().data(), iter->Key().size(), 0)) {
            continue;
        }
        keys.push_back(iter->Key());
    }
    if (!iter_ret) {
        index = 0;
    }
    delete iter;

    ret = "*2\r\n";
    char buf[32];
    char buf_len[32];
    std::string str_index;
    int len = ll2string(buf, sizeof(buf), index);
    snprintf(buf_len, sizeof(buf_len), "$%d\r\n", len);
    ret.append(buf_len);
    ret.append(buf);
    ret.append("\r\n");

    snprintf(buf, sizeof(buf), "*%lu\r\n", keys.size());
    ret.append(buf);
    std::vector<std::string>::iterator it;
    for (it = keys.begin(); it != keys.end(); it++) {
        snprintf(buf, sizeof(buf), "$%lu\r\n", it->size());
        ret.append(buf);
        ret.append(it->data(), it->size());
        ret.append("\r\n");
    }
}
