#include "pika_command.h"
#include "pika_server.h"
#include "util.h"
#include <algorithm>
#include <map>

extern PikaServer *g_pikaServer;
extern std::map<std::string, Cmd *> g_pikaCmd;

void ZAddCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if (((int)argv.size() % 2 != 0) || (arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();

    double score;
    std::list<std::string>::iterator iter;
    for (iter = argv.begin(); iter !=  argv.end(); iter++) {
        if (!string2d(iter->data(), iter->size(), &score)) {
            ret = "-ERR value is not a valid float\r\n";
            return;
        }
        iter++;
    }
    std::string str_score;
    std::string member;
    int64_t res;
    int64_t count = 0;
    nemo::Status s;
    while (argv.size()) {
        str_score = argv.front();
        argv.pop_front();
        member = argv.front();
        argv.pop_front();
        string2d(str_score.data(), str_score.size(), &score);
        s = g_pikaServer->GetHandle()->ZAdd(key, score, member, &res);
        if (s.ok()) {
            count += res;
        } else {
            ret.append("-ERR ");
            ret.append(s.ToString().c_str());
            ret.append("\r\n");
            return;
        }
    }
    char buf[32];
    snprintf(buf, sizeof(buf), ":%ld\r\n", count);
    ret.append(buf);
}

void ZCardCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if (((int)argv.size() % 2 != 0) || (arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();

    int64_t res = g_pikaServer->GetHandle()->ZCard(key);
    if (res >= 0) {
        char buf[32];
        snprintf(buf, sizeof(buf), ":%ld\r\n", res);
        ret.append(buf);
    } else {
        ret = "-ERR zcard error\r\n";
    }
}

void ZScanCmd::Do(std::list<std::string> &argv, std::string &ret) {
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


    std::vector<nemo::SM> sms;
    nemo::ZIterator *iter = g_pikaServer->GetHandle()->ZScan(key, nemo::ZSET_SCORE_MIN, nemo::ZSET_SCORE_MAX, -1);
    iter->Skip(index);
    bool iter_ret = false;
    while ((iter_ret=iter->Next()) && count) {
        count--;
        index++;
        if (use_pat == true && !stringmatchlen(pattern.data(), pattern.size(), iter->Member().data(), iter->Member().size(), 0)) {
            continue;
        }
        sms.push_back(nemo::SM{iter->Score(), iter->Member()});
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

    snprintf(buf, sizeof(buf), "*%lu\r\n", sms.size() * 2);
    ret.append(buf);
    std::string str_score;
    std::vector<nemo::SM>::iterator it;
    for (it = sms.begin(); it != sms.end(); it++) {
        snprintf(buf, sizeof(buf), "$%lu\r\n", it->member.size());
        ret.append(buf);
        ret.append(it->member.data(), it->member.size());
        ret.append("\r\n");
        len = 0;
        len = d2string(buf, sizeof(buf), it->score);
        snprintf(buf_len, sizeof(buf_len), "$%d\r\n", len);
        ret.append(buf_len);
        ret.append(buf);
        ret.append("\r\n");
    }
}

void ZIncrbyCmd::Do(std::list<std::string> &argv, std::string &ret) {
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
    std::string member = argv.front();
    argv.pop_front();
    std::string new_val;
    nemo::Status s = g_pikaServer->GetHandle()->ZIncrby(key, member, by, new_val);
    if (s.ok() || s.IsNotFound()) {
        char buf[32];
        snprintf(buf, sizeof(buf), "$%lu\r\n", new_val.size());
        ret.append(buf);
        ret.append(new_val.data(), new_val.size());
        ret.append("\r\n");
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void ZRangeCmd::Do(std::list<std::string> &argv, std::string &ret) {
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
    std::string str_stop = argv.front();
    argv.pop_front();
    int64_t stop;
    if (!string2l(str_stop.data(), str_stop.size(), &stop)) {
        ret = "-ERR value is not an integer or out of range\r\n";
        return;
    }
    bool is_ws = false;
    if (argv.size() == 1) {
        std::string ws = argv.front(); 
        transform(ws.begin(), ws.end(), ws.begin(), ::tolower);
        if (ws == "withscores") {
            is_ws = true;
        } else {
            ret = "-ERR syntax error\r\n";
            return;
        }
    } else if (argv.size() > 1) {
        ret = "-ERR syntax error\r\n";
        return;
    }

    std::vector<nemo::SM> sms;
    nemo::Status s = g_pikaServer->GetHandle()->ZRange(key, start, stop, sms);
    if (s.ok()) {
        char buf[32];
        char buf_len[32];
        if (is_ws) {
            snprintf(buf, sizeof(buf), "*%lu\r\n", sms.size() * 2);
            ret.append(buf);
            std::vector<nemo::SM>::iterator iter;
            for (iter = sms.begin(); iter != sms.end(); iter++) {
                snprintf(buf, sizeof(buf), "$%lu\r\n", iter->member.size());
                ret.append(buf);
                ret.append(iter->member.data(), iter->member.size());
                ret.append("\r\n");
                int32_t len = 0;
                len = d2string(buf, sizeof(buf), iter->score);
                snprintf(buf_len, sizeof(buf_len), "$%d\r\n", len);
                ret.append(buf_len);
                ret.append(buf);
                ret.append("\r\n");
            }
        } else {
            snprintf(buf, sizeof(buf), "*%lu\r\n", sms.size());
            ret.append(buf);
            std::vector<nemo::SM>::iterator iter;
            for (iter = sms.begin(); iter != sms.end(); iter++) {
                snprintf(buf, sizeof(buf), "$%lu\r\n", iter->member.size());
                ret.append(buf);
                ret.append(iter->member.data(), iter->member.size());
                ret.append("\r\n");
            }
        }
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void ZRangebyscoreCmd::Do(std::list<std::string> &argv, std::string &ret) {
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
    bool is_lo = false;
    bool is_ro = false;
    double start;
    if (str_start[0] == '(') {
        is_lo = true;
        str_start = str_start.substr(1, str_start.size());
    }
    if (str_start == "-inf") {
        start = nemo::ZSET_SCORE_MIN;
    } else if (str_start == "+inf" || str_start == "inf") {
        start = nemo::ZSET_SCORE_MAX;
    } else {
        if (!string2d(str_start.data(), str_start.size(), &start)) {
            ret = "-ERR value is not an integer or out of range\r\n";
            return;
        }
    }
    std::string str_stop = argv.front();
    argv.pop_front();
    double stop;
    if (str_stop[0] == '(') {
        is_ro = true;
        str_stop = str_stop.substr(1, str_stop.size());
    }
    if (str_stop == "+inf" || str_stop == "inf") {
        stop = nemo::ZSET_SCORE_MAX;
    } else if (str_stop == "-inf") {
        stop = nemo::ZSET_SCORE_MIN;
    } else {
        if (!string2d(str_stop.data(), str_stop.size(), &stop)) {
            ret = "-ERR value is not an integer or out of range\r\n";
            return;
        }
    }

    bool is_ws = false;
    size_t size = argv.size();
    int64_t offset = 0;
    int64_t count = -1;
    std::string ar;
    if (size != 0 && size != 1 && size != 3 && size != 4) {
        ret = "-ERR syntax error\r\n";
        return;
    } else {
        while (argv.size() > 0) {
            ar = argv.front();
            argv.pop_front();
            transform(ar.begin(), ar.end(), ar.begin(), ::tolower);
            if (ar ==  "withscores") {
                is_ws = true;
            } else if (ar == "limit" && argv.size() >= 2) {
                std::string str_offset;
                std::string str_count;
                str_offset = argv.front();
                argv.pop_front();
                if (!string2l(str_offset.data(), str_offset.size(), &offset)) {
                    ret = "-ERR syntax error\r\n";
                    return;
                }
                if (offset < 0) {
                    offset = 0;
                }
                str_count = argv.front();
                argv.pop_front();
                if (!string2l(str_count.data(), str_count.size(), &count)) {
                    ret = "-ERR syntax error\r\n";
                    return;
                }
            } else {
                ret = "-ERR syntax error\r\n";
                return;
            }
        }
        std::vector<nemo::SM> sms;
        nemo::Status s = g_pikaServer->GetHandle()->ZRangebyscore(key, start, stop, sms, offset, is_lo, is_ro);
        if (s.ok()) {
            std::vector<nemo::SM>::iterator iter;
            count = count >= 0 ? count : INT_MAX;  
            char buf[32];
            char buf_len[32];
            uint64_t len = sms.size() > (uint64_t)count ? count : sms.size();
            if (is_ws) {
                snprintf(buf, sizeof(buf), "*%lu\r\n", len * 2);
                ret.append(buf);
                for (iter = sms.begin(); count != 0 && iter != sms.end(); iter++, count--) {
                    snprintf(buf, sizeof(buf), "$%lu\r\n", iter->member.size());
                    ret.append(buf);
                    ret.append(iter->member.data(), iter->member.size());
                    ret.append("\r\n");
                    int32_t len = 0;
                    len = d2string(buf, sizeof(buf), iter->score);
                    snprintf(buf_len, sizeof(buf_len), "$%d\r\n", len);
                    ret.append(buf_len);
                    ret.append(buf);
                    ret.append("\r\n");
                }
            } else {
                snprintf(buf, sizeof(buf), "*%lu\r\n", len);
                ret.append(buf);
                for (iter = sms.begin(); count != 0 && iter != sms.end(); iter++, count--) {
                    snprintf(buf, sizeof(buf), "$%lu\r\n", iter->member.size());
                    ret.append(buf);
                    ret.append(iter->member.data(), iter->member.size());
                    ret.append("\r\n");
                }
            }
        } else {
            ret.append("-ERR ");
            ret.append(s.ToString().c_str());
            ret.append("\r\n");
        }

    }
}

void ZCountCmd::Do(std::list<std::string> &argv, std::string &ret) {
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
    bool is_lo = false;
    bool is_ro = false;
    double start;
    if (str_start[0] == '(') {
        is_lo = true;
        str_start = str_start.substr(1, str_start.size());
    }
    if (str_start == "-inf") {
        start = nemo::ZSET_SCORE_MIN;
    } else if (str_start == "+inf" || str_start == "inf") {
        start = nemo::ZSET_SCORE_MAX;
    } else {
        if (!string2d(str_start.data(), str_start.size(), &start)) {
            ret = "-ERR value is not an integer or out of range\r\n";
            return;
        }
    }

    std::string str_stop = argv.front();
    argv.pop_front();
    double stop;
    if (str_stop[0] == '(') {
        is_ro = true;
        str_stop = str_stop.substr(1, str_stop.size());
    }
    if (str_stop == "+inf" || str_stop == "inf") {
        stop = nemo::ZSET_SCORE_MAX;
    } else if (str_stop == "-inf") {
        stop = nemo::ZSET_SCORE_MIN;
    } else {
        if (!string2d(str_stop.data(), str_stop.size(), &stop)) {
            ret = "-ERR value is not an integer or out of range\r\n";
            return;
        }
    }

    uint64_t count = g_pikaServer->GetHandle()->ZCount(key, start, stop, is_lo, is_ro);
    char buf[32];
    snprintf(buf, sizeof(buf), ":%lu\r\n", count);
    ret.append(buf);
}

