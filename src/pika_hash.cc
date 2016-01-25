#include "pika_command.h"
#include "pika_server.h"
#include "util.h"
#include <algorithm>
#include <map>

extern PikaServer *g_pikaServer;
extern std::map<std::string, Cmd *> g_pikaCmd;

void HDelCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    int num = 0;
    while (!argv.empty()) {
        nemo::Status s = g_pikaServer->GetHandle()->HDel(key, argv.front());
        if (s.ok()) {
            num++;
        }
        argv.pop_front();
    }
    char buf[32];
    snprintf(buf, sizeof(buf), "%d\r\n", num);
    ret.append(":");
    ret.append(buf);
}

void HExistsCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::string field = argv.front();
    argv.pop_front();
    bool res = g_pikaServer->GetHandle()->HExists(key, field);
    if (res) {
        ret = ":1\r\n";
    } else {
        ret = ":0\r\n";
    }
}

void HGetCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::string field = argv.front();
    argv.pop_front();
    std::string value;
    nemo::Status s = g_pikaServer->GetHandle()->HGet(key, field, &value);
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

void HGetallCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::vector<nemo::FV> fvs;
    nemo::Status s = g_pikaServer->GetHandle()->HGetall(key, fvs);
    if (s.ok()) {
        char buf[32];
        snprintf(buf, sizeof(buf), "*%lu\r\n", fvs.size() * 2);
        ret.append(buf);
        std::vector<nemo::FV>::iterator iter;
        for (iter = fvs.begin(); iter != fvs.end(); iter++) {
            snprintf(buf, sizeof(buf), "$%lu\r\n", iter->field.size());
            ret.append(buf);
            ret.append(iter->field.data(), iter->field.size());
            ret.append("\r\n");
            snprintf(buf, sizeof(buf), "$%lu\r\n", iter->val.size());
            ret.append(buf);
            ret.append(iter->val.data(), iter->val.size());
            ret.append("\r\n");
        }
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void HIncrbyCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::string field = argv.front();
    argv.pop_front();
    std::string str_by = argv.front();
    argv.pop_front();
    int64_t by;
    std::string new_val;
    if ((str_by.find(" ") != std::string::npos) || !string2l(str_by.data(), str_by.size(), &by)) {
        ret = "-ERR value is not an integer or out of range\r\n";
        return;
    }
    nemo::Status s = g_pikaServer->GetHandle()->HIncrby(key, field, by, new_val);
    if (s.ok() || s.IsNotFound()) {
        ret = ":";
        ret.append(new_val.data(), new_val.size());
        ret.append("\r\n");
    } else if (s.IsCorruption() && s.ToString() == "Corruption: value is not integer"){
        ret = "-ERR hash value is not an integer\r\n"; 
    } else if (s.IsInvalidArgument()) {
        ret = "-ERR increment or decrement would overflow\r\n";
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void HIncrbyfloatCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::string field = argv.front();
    argv.pop_front();
    std::string str_by = argv.front();
    argv.pop_front();
    double by;
    std::string new_val;
    if ((str_by.find(" ") != std::string::npos) || !string2d(str_by.data(), str_by.size(), &by)) {
        ret = "-ERR value is not a valid float\r\n";
        return;
    }
    nemo::Status s = g_pikaServer->GetHandle()->HIncrbyfloat(key, field, by, new_val);
    if (s.ok() || s.IsNotFound()) {
        char buf[32];
        snprintf(buf, sizeof(buf), "$%lu\r\n", new_val.size());
        ret.append(buf);
        ret.append(new_val.data(), new_val.size());
        ret.append("\r\n");
    } else if (s.IsCorruption() && s.ToString() == "Corruption: value is not float"){
        ret = "-ERR hash value is not an float\r\n"; 
    } else if (s.IsInvalidArgument()) {
        ret = "-ERR increment or decrement would overflow\r\n";
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void HKeysCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::vector<std::string> fields;
    nemo::Status s = g_pikaServer->GetHandle()->HKeys(key, fields);
    if (s.ok()) {
        char buf[32];
        snprintf(buf, sizeof(buf), "*%lu\r\n", fields.size());
        ret.append(buf);
        std::vector<std::string>::iterator iter;
        for (iter = fields.begin(); iter != fields.end(); iter++) {
            snprintf(buf, sizeof(buf), "$%lu\r\n", iter->size());
            ret.append(buf);
            ret.append(iter->data(), iter->size());
            ret.append("\r\n");
        }
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void HLenCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    int res = g_pikaServer->GetHandle()->HLen(key);
    if (res) {
        char buf[32];
        snprintf(buf, sizeof(buf), ":%d\r\n", res);
        ret.append(buf);
    } else if (res == 0) {
        ret.append(":0\r\n");
    } else {
        ret.append("-ERR something wrong in hlen\r\n");
    }
}

void HMGetCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::string field;
    std::string value;
    std::vector<std::string> fields;
    std::vector<nemo::FVS> fvss;
    while (!argv.empty()) {
        fields.push_back(argv.front()); 
        argv.pop_front();
    }
    nemo::Status s = g_pikaServer->GetHandle()->HMGet(key, fields, fvss);
    if (s.ok()) {
        std::vector<nemo::FVS>::iterator iter;
        char buf[32];
        snprintf(buf, sizeof(buf), "*%lu\r\n", fvss.size());
        ret.append(buf);
        for (iter = fvss.begin(); iter != fvss.end(); iter++) {
            if (iter->status.ok()) {
                snprintf(buf, sizeof(buf), "$%lu\r\n", iter->val.size());
                ret.append(buf);
                ret.append(iter->val.data(), iter->val.size());
                ret.append("\r\n");
            } else {
                ret.append("$-1\r\n");
            }
        }
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void HMSetCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if (((int)argv.size() % 2 != 0 ) || (arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::string field;
    std::string value;
    std::vector<nemo::FV> fvs;
    while (!argv.empty()) {
        field = argv.front(); 
        argv.pop_front();
        value = argv.front(); 
        argv.pop_front();
        fvs.push_back(nemo::FV{field, value});
    }
    nemo::Status s = g_pikaServer->GetHandle()->HMSet(key, fvs);
    if (s.ok()) {
        ret = "+OK\r\n";
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void DoHSet(const std::string &key, const std::string &field, const std::string &value, std::string &ret, const std::string &exp_ret) {
    nemo::Status s = g_pikaServer->GetHandle()->HSet(key, field, value);
    if (s.ok()) {
        ret = exp_ret;
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void HSetCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::string field = argv.front();
    argv.pop_front();
    std::string value = argv.front();
    argv.pop_front();

    std::string res;
    nemo::Status s = g_pikaServer->GetHandle()->HGet(key, field, &res);
    if (s.ok()) {
        if (res != value) {
            DoHSet(key, field, value, ret, ":0\r\n");
        } else {
            ret = ":0\r\n";
        }
    } else if (s.IsNotFound()) {
        DoHSet(key, field, value, ret, ":1\r\n");
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void HSetnxCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::string field = argv.front();
    argv.pop_front();
    std::string value = argv.front();
    argv.pop_front();

    std::string res;
    nemo::Status s = g_pikaServer->GetHandle()->HSetnx(key, field, value);
    if (s.ok()) {
        ret = ":1\r\n";
    } else if (s.IsCorruption() && s.ToString() == "Corruption: Already Exist") {
        ret = ":0\r\n";
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void HStrlenCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::string field = argv.front();
    argv.pop_front();
    int res = g_pikaServer->GetHandle()->HStrlen(key, field);
    if (res) {
        char buf[32];
        snprintf(buf, sizeof(buf), ":%d\r\n", res);
        ret.append(buf);
    } else if (res == 0) {
        ret.append(":0\r\n");
    } else {
        ret.append("-ERR something wrong in hstrlen\r\n");
    }
}

void HValsCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::vector<std::string> vals;
    nemo::Status s = g_pikaServer->GetHandle()->HVals(key, vals);
    if (s.ok()) {
        char buf[32];
        snprintf(buf, sizeof(buf), "*%lu\r\n", vals.size());
        ret.append(buf);
        std::vector<std::string>::iterator iter;
        for (iter = vals.begin(); iter != vals.end(); iter++) {
            snprintf(buf, sizeof(buf), "$%lu\r\n", iter->size());
            ret.append(buf);
            ret.append(iter->data(), iter->size());
            ret.append("\r\n");
        }
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void HScanCmd::Do(std::list<std::string> &argv, std::string &ret) {
    int size = argv.size();    
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity) || (size != 3 && size != 5 && size != 7)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
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

    int hlen = g_pikaServer->GetHandle()->HLen(key);
    if (hlen >= 0 && index >= hlen) {
        index = 0;
    }

    std::vector<nemo::FV> fvs;
    nemo::HIterator *iter = g_pikaServer->GetHandle()->HScan(key, "", "", -1);
    iter->Skip(index);
    if (!iter->Valid()) {
      delete iter;
      iter = g_pikaServer->GetHandle()->HScan(key, "", "", -1);
    }

    //bool skip_ret = iter->Skip(index);
   // if (skip_ret && !iter->Valid()) {
   //     count--;
   //     if (!(use_pat == true && !stringmatchlen(pattern.data(), pattern.size(), iter->field().data(), iter->field().size(), 0))) {
   //         fvs.push_back(nemo::FV{iter->field(), iter->value()});
   //     }
   //     index = 0;
   // } else {
        for (; iter->Valid() && count; iter->Next()) {
   //     while ((iter_ret=iter->Next()) && count) {
            count--;
            index++;
            if (use_pat == true && !stringmatchlen(pattern.data(), pattern.size(), iter->field().data(), iter->field().size(), 0)) {
                continue;
            }
            fvs.push_back(nemo::FV{iter->field(), iter->value()});
        }

        // no valid element or iterator the last one
        if (fvs.size() <= 0 || count > 0) {
            index = 0;
        }
    //}
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

    snprintf(buf, sizeof(buf), "*%lu\r\n", fvs.size() * 2);
    ret.append(buf);
    std::vector<nemo::FV>::iterator it;
    for (it = fvs.begin(); it != fvs.end(); it++) {
        snprintf(buf, sizeof(buf), "$%lu\r\n", it->field.size());
        ret.append(buf);
        ret.append(it->field.data(), it->field.size());
        ret.append("\r\n");
        snprintf(buf, sizeof(buf), "$%lu\r\n", it->val.size());
        ret.append(buf);
        ret.append(it->val.data(), it->val.size());
        ret.append("\r\n");
    }
}
