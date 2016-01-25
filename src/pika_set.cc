#include "pika_command.h"
#include "pika_server.h"
#include "util.h"
#include <algorithm>
#include <map>

extern PikaServer *g_pikaServer;
extern std::map<std::string, Cmd *> g_pikaCmd;

void SAddCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();

    std::string member;
    int64_t res;
    int64_t count = 0;
    nemo::Status s;
    while (argv.size()) {
        member = argv.front();
        argv.pop_front();
        s = g_pikaServer->GetHandle()->SAdd(key, member, &res);
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

void SCardCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();

    int64_t res = g_pikaServer->GetHandle()->SCard(key);
    if (res >= 0) {
        char buf[32];
        snprintf(buf, sizeof(buf), ":%ld\r\n", res);
        ret.append(buf);
    } else {
        ret = "-ERR scard error\r\n";
    }
}

void SMembersCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();

    nemo::Status s;
    std::vector<std::string> members;
    s = g_pikaServer->GetHandle()->SMembers(key, members);

    std::vector<std::string>::iterator it;
    char buf[32];
    snprintf(buf, sizeof(buf), "*%lu\r\n", members.size());
    ret.append(buf);
    for (it = members.begin(); it != members.end(); it++) {
        snprintf(buf, sizeof(buf), "$%lu\r\n", (*it).size());
        ret.append(buf);
        ret.append((*it).data(), (*it).size());
        ret.append("\r\n");
    }
}

void SScanCmd::Do(std::list<std::string> &argv, std::string &ret) {
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

    int64_t card = g_pikaServer->GetHandle()->SCard(key);
    if (card >= 0 && index >= card) {
        index = 0;
    }

    std::vector<std::string> members;
    nemo::SIterator *iter = g_pikaServer->GetHandle()->SScan(key, -1);
    iter->Skip(index);
    if (!iter->Valid()) {
      delete iter;
      iter = g_pikaServer->GetHandle()->SScan(key, -1);
    }

   // bool skip_ret = iter->Skip(index);
   // if (skip_ret && !iter->Valid()) {
   //     count--;
   //     if (!(use_pat == true && !stringmatchlen(pattern.data(), pattern.size(), iter->member().data(), iter->member().size(), 0))) {
   //         members.push_back(iter->member());
   //     }
   //     index = 0;
   // } else {
        for (; iter->Valid() && count; iter->Next()) {
   //     while ((iter_ret=iter->Next()) && count) {
            count--;
            index++;
            if (use_pat == true && !stringmatchlen(pattern.data(), pattern.size(), iter->member().data(), iter->member().size(), 0)) {
                continue;
            }
            members.push_back(iter->member());
        }

        // no valid element or iterator the last one
        if (members.size() <= 0 || count > 0) {
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

    snprintf(buf, sizeof(buf), "*%lu\r\n", members.size());
    ret.append(buf);
    std::vector<std::string>::iterator it;
    for (it = members.begin(); it != members.end(); it++) {
        snprintf(buf, sizeof(buf), "$%lu\r\n", it->size());
        ret.append(buf);
        ret.append(*it);
        ret.append("\r\n");
    }
}

void SRemCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    int64_t count = 0;
    int64_t res = 0;
    nemo::Status s;
    while (argv.size()) {
        s = g_pikaServer->GetHandle()->SRem(key, argv.front(), &res);
        if (s.ok()) {
            count++;
        }
        argv.pop_front();
    }
    char buf[32];
    snprintf(buf, sizeof(buf), ":%ld\r\n", count);
    ret.append(buf);
}

void SUnionCmd::Do(std::list<std::string> &argv, std::string &ret) {
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

    std::vector<std::string> members;
    nemo::Status s = g_pikaServer->GetHandle()->SUnion(keys, members);
    char buf[32];

    snprintf(buf, sizeof(buf), "*%lu\r\n", members.size());
    ret.append(buf);
    std::vector<std::string>::iterator iter;
    for (iter = members.begin(); iter != members.end(); iter++) {
        snprintf(buf, sizeof(buf), "$%lu\r\n", iter->size());
        ret.append(buf);
        ret.append(iter->data(), iter->size());
        ret.append("\r\n");
    }

}

void SUnionstoreCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string dest = argv.front();
    argv.pop_front();
    std::vector<std::string> keys;
    while (argv.size()) {
       keys.push_back(argv.front());
       argv.pop_front();
    }

    int64_t res = 0;
    nemo::Status s = g_pikaServer->GetHandle()->SUnionStore(dest, keys, &res);
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

void SInterCmd::Do(std::list<std::string> &argv, std::string &ret) {
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

    std::vector<std::string> members;
    nemo::Status s = g_pikaServer->GetHandle()->SInter(keys, members);
    char buf[32];

    snprintf(buf, sizeof(buf), "*%lu\r\n", members.size());
    ret.append(buf);
    std::vector<std::string>::iterator iter;
    for (iter = members.begin(); iter != members.end(); iter++) {
        snprintf(buf, sizeof(buf), "$%lu\r\n", iter->size());
        ret.append(buf);
        ret.append(iter->data(), iter->size());
        ret.append("\r\n");
    }
}

void SInterstoreCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string dest = argv.front();
    argv.pop_front();
    std::vector<std::string> keys;
    while (argv.size()) {
       keys.push_back(argv.front());
       argv.pop_front();
    }

    int64_t res = 0;
    nemo::Status s = g_pikaServer->GetHandle()->SInterStore(dest, keys, &res);
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

void SIsmemberCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    std::string member = argv.front();
    argv.pop_front();
    bool is_member = g_pikaServer->GetHandle()->SIsMember(key, member);
    if (is_member) {
        ret = ":1\r\n";
    } else {
        ret = ":0\r\n";
    }
}

void SDiffCmd::Do(std::list<std::string> &argv, std::string &ret) {
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

    std::vector<std::string> members;
    nemo::Status s = g_pikaServer->GetHandle()->SDiff(keys, members);
    char buf[32];

    snprintf(buf, sizeof(buf), "*%lu\r\n", members.size());
    ret.append(buf);
    std::vector<std::string>::iterator iter;
    for (iter = members.begin(); iter != members.end(); iter++) {
        snprintf(buf, sizeof(buf), "$%lu\r\n", iter->size());
        ret.append(buf);
        ret.append(iter->data(), iter->size());
        ret.append("\r\n");
    }
}

void SDiffstoreCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string dest = argv.front();
    argv.pop_front();
    std::vector<std::string> keys;
    while (argv.size()) {
       keys.push_back(argv.front());
       argv.pop_front();
    }

    int64_t res = 0;
    nemo::Status s = g_pikaServer->GetHandle()->SDiffStore(dest, keys, &res);
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

void SMoveCmd::Do(std::list<std::string> &argv, std::string &ret) {
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
    std::string member = argv.front();
    argv.pop_front();

    int64_t res = 0;
    nemo::Status s = g_pikaServer->GetHandle()->SMove(src, dest, member, &res);
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

void SPopCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();

    std::string member;
    nemo::Status s = g_pikaServer->GetHandle()->SPop(key, member);
    if (s.ok()) {
        char buf[32];
        snprintf(buf, sizeof(buf), "$%lu\r\n", member.size());
        ret.append(buf);
        ret.append(member);
        ret.append("\r\n");
    } else if (s.IsNotFound()) {
        ret = "$-1\r\n";
    } else {
        ret.append("-ERR ");
        ret.append(s.ToString().c_str());
        ret.append("\r\n");
    }
}

void SRandmemberCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((argv.size() != 2 && argv.size() != 3) || (arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }

    argv.pop_front();
    std::string key = argv.front();
    argv.pop_front();
    int64_t count = 1;
    if (argv.size()) {
        std::string str_count = argv.front();
        argv.pop_front();
        if (!string2l(str_count.data(), str_count.size(), &count)) {
            ret = "-ERR value is not an integer or out of range\r\n";
            return;
        }
    }

    std::vector<std::string> members;
    nemo::Status s = g_pikaServer->GetHandle()->SRandMember(key, members, count);
    if (s.ok() || s.IsNotFound()) {
        char buf[32];
        snprintf(buf, sizeof(buf), "*%lu\r\n", members.size());
        ret.append(buf);
        std::vector<std::string>::iterator iter;
        for (iter = members.begin(); iter != members.end(); iter++) {
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
