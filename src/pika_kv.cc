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
    bool is_xx = false;
    bool is_nx = false;
    int64_t sec = 0; 
    while (argv.size() > 0) {
        std::string opt = argv.front();
        argv.pop_front();
        transform(opt.begin(), opt.end(), opt.begin(), ::tolower);
        if (opt == "xx") {
            is_xx = true;
        } else if (opt == "nx") {
            is_nx = true;
        } else if (opt == "ex") {
            if (argv.size() < 1) {
                ret = "-ERR syntax error\r\n";
                return;
            }
            std::string str_sec = argv.front();
            argv.pop_front();
            if (!string2l(str_sec.data(), str_sec.size(), &sec)) {
                ret = "-ERR value is not an integer or out of range\r\n";
                return;
            }
        } else {
            ret = "-ERR syntax error\r\n";
            return;
        }
    }
    nemo::Status s;
    int64_t res = 1;
    if (is_nx) {
        s = g_pikaServer->GetHandle()->Setnx(key, value, &res, (int32_t)sec);
    } else {
        if (is_xx) {
            s = g_pikaServer->GetHandle()->Setxx(key, value, &res, sec);
        } else {
            s = g_pikaServer->GetHandle()->Set(key, value, sec);
        }
    }

    if (s.ok() || s.IsNotFound()) {
        if (res == 1) {
            ret = "+OK\r\n";
        } else {
            ret = "*-1\r\n";
        }
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

void GetsetCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::string new_value = argv.front();
    argv.pop_front();
    std::string old_value;
    nemo::Status s = g_pikaServer->GetHandle()->GetSet(key, new_value, &old_value);
    if (s.ok()) {
        if (old_value.size() == 0) {
            ret = "$-1\r\n";
        } else {
            char buf[32];
            snprintf(buf, sizeof(buf), "$%lu\r\n", old_value.size());
            ret.append(buf);
            ret.append(old_value.data(), old_value.size());
            ret.append("\r\n");
        }
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void AppendCmd::Do(std::list<std::string> &argv, std::string &ret) {
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
    int64_t new_len = 0;
    nemo::Status s = g_pikaServer->GetHandle()->Append(key, value, &new_len);
    if (s.ok() || s.IsNotFound()) {
        char buf[32];
        snprintf(buf, sizeof(buf), ":%ld\r\n", new_len);
        ret.append(buf);
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void MgetCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::vector<std::string> keys;
    std::vector<nemo::KVS> kvs;
    while (argv.size()) {
        keys.push_back(argv.front());
        argv.pop_front();
    }
    nemo::Status s = g_pikaServer->GetHandle()->MGet(keys, kvs);
    char buf[32];
    snprintf(buf, sizeof(buf), "*%lu\r\n", kvs.size());
    ret.append(buf);
    std::vector<nemo::KVS>::iterator iter;
    for (iter = kvs.begin(); iter != kvs.end(); iter++ ) {
        if ((iter->status).ok()) {
            snprintf(buf, sizeof(buf), "$%lu\r\n", (iter->val).size());
            ret.append(buf);
            ret.append((iter->val).data(), (iter->val).size());
            ret.append("\r\n");
        } else {
            ret.append("$-1\r\n");
        }
    }
}

void SetnxCmd::Do(std::list<std::string> &argv, std::string &ret) {
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
    int64_t res = 0;
    nemo::Status s = g_pikaServer->GetHandle()->Setnx(key, value, &res);
    if (s.ok()) {
        char buf[32];
        snprintf(buf, sizeof(buf), ":%ld\r\n", res);
        ret.append(buf);
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void SetexCmd::Do(std::list<std::string> &argv, std::string &ret) {
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
    std::string str_sec = argv.front();
    argv.pop_front();
    int64_t sec = 0;
    if (!string2l(str_sec.data(), str_sec.size(), &sec)) {
        ret = "-ERR value is not an integer or out of range\r\n";
        return;
    }
    nemo::Status s = g_pikaServer->GetHandle()->Set(key, value, sec);
    if (s.ok()) {
        ret = "+OK\r\n";
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void MsetCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((((int)argv.size()) % 2 != 1) || (arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::vector<nemo::KV> kvs;
    std::string key;
    std::string val;
    while (argv.size()) {
        key = argv.front();
        argv.pop_front();
        val = argv.front();
        argv.pop_front();
        kvs.push_back({key, val});
    }
    nemo::Status s = g_pikaServer->GetHandle()->MSet(kvs);
    if (s.ok()) {
        ret = "+OK\r\n";
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void MsetnxCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((((int)argv.size()) % 2 != 1) || (arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::vector<nemo::KV> kvs;
    std::string key;
    std::string val;
    while (argv.size()) {
        key = argv.front();
        argv.pop_front();
        val = argv.front();
        argv.pop_front();
        kvs.push_back({key, val});
    }
    int64_t res;
    nemo::Status s = g_pikaServer->GetHandle()->MSetnx(kvs, &res);
    if (s.ok()) {
        char buf[32];
        snprintf(buf, sizeof(buf), ":%ld\r\n", res);
        ret.append(buf);
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void GetrangeCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::string str_start = argv.front();
    argv.pop_front();
    int64_t start;
    if (!string2l(str_start.data(), str_start.size(), &start)) {
        ret = "-ERR value is not an integer or out of range\r\n";
        return;
    }
    std::string str_end = argv.front();
    argv.pop_front();
    int64_t end;
    if (!string2l(str_end.data(), str_end.size(), &end)) {
        ret = "-ERR value is not an integer or out of range\r\n";
        return;
    }
    std::string substr;
    nemo::Status s = g_pikaServer->GetHandle()->Getrange(key, start, end, substr);
    if (s.ok() || s.IsNotFound()) {
        char buf[32];
        snprintf(buf, sizeof(buf), "$%lu\r\n", substr.size());
        ret.append(buf);
        ret.append(substr.data(), substr.size());
        ret.append("\r\n");
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void SetrangeCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::string str_offset = argv.front();
    argv.pop_front();
    int64_t offset;
    if (!string2l(str_offset.data(), str_offset.size(), &offset)) {
        ret = "-ERR value is not an integer or out of range\r\n";
        return;
    }
    if (offset < 0) {
        ret = "-ERR offset is out of range\r\n";
        return;
    }
    std::string value = argv.front();
    argv.pop_front();
    int64_t new_len;
    nemo::Status s = g_pikaServer->GetHandle()->Setrange(key, offset, value, &new_len);
    if (s.ok()) {
        char buf[32];
        snprintf(buf, sizeof(buf), ":%ld\r\n", new_len);
        ret.append(buf);
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void StrlenCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    int64_t len;
    nemo::Status s = g_pikaServer->GetHandle()->Strlen(key, &len);
    if (s.ok() || s.IsNotFound()) {
        char buf[32];
        snprintf(buf, sizeof(buf), ":%lu\r\n", len);
        ret.append(buf);
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void ExistsCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::string val;
    nemo::Status s = g_pikaServer->GetHandle()->Get(key, &val);
    if (s.ok()) {
        ret = ":1\r\n";
    } else if (s.IsNotFound()) {
        ret = ":0\r\n";
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void ExpireCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::string str_sec = argv.front();
    argv.pop_front();
    int64_t sec;
    if (!string2l(str_sec.data(), str_sec.size(), &sec)) {
        ret = "-ERR value is not an integer or out of range\r\n";
        return;
    }
    int64_t res;
    nemo::Status s = g_pikaServer->GetHandle()->Expire(key, sec, &res);
    if (s.ok() || s.IsNotFound()) {
        char buf[32];
        snprintf(buf, sizeof(buf), ":%ld\r\n", res);
        ret.append(buf);
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void ExpireatCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::string str_timestamp = argv.front();
    argv.pop_front();
    int64_t timestamp;
    if (!string2l(str_timestamp.data(), str_timestamp.size(), &timestamp)) {
        ret = "-ERR value is not an integer or out of range\r\n";
        return;
    }
    int64_t res;
    nemo::Status s = g_pikaServer->GetHandle()->Expireat(key, timestamp, &res);
    if (s.ok() || s.IsNotFound()) {
        char buf[32];
        snprintf(buf, sizeof(buf), ":%ld\r\n", res);
        ret.append(buf);
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void TtlCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    int64_t ttl;
    nemo::Status s = g_pikaServer->GetHandle()->TTL(key, &ttl);
    if (s.ok() || s.IsNotFound()) {
        char buf[32];
        snprintf(buf, sizeof(buf), ":%ld\r\n", ttl);
        ret.append(buf);
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void PersistCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    int64_t res;
    nemo::Status s = g_pikaServer->GetHandle()->Persist(key, &res);
    if (s.ok() || s.IsNotFound()) {
        char buf[32];
        snprintf(buf, sizeof(buf), ":%ld\r\n", res);
        ret.append(buf);
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
