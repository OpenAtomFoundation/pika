#ifndef AOF_SENDER_H
#define AOF_SENDER_H

#include <string>
#include <deque>
#include "aof_lock.h"
#include "aof_info.h"

#define RM_NONE     0
#define RM_READBLE  1
#define RM_WRITABLE 2
#define RM_RECONN   4

#define READ_BUF_MAX 100
#define MSG_BLOCK_MAX 128 * 1024

typedef struct ConnInfo{
    ConnInfo(){}
    ConnInfo(const std::string &h, const std::string &p, const std::string &a):host_(h),port_(p),auth_(a){}
    std::string host_;
    std::string port_;
    std::string auth_;
} ConnInfo;


class AOFSender
{
public:
    AOFSender():buf_wcond_(&buf_mutex_), buf_rcond_(&buf_mutex_){ sockfd_ = -1; nsucc_ = nfail_ = 0; }
    ~AOFSender();
    bool rconnect(const std::string&, const std::string&, const std::string&);
    bool message_add(const std::string&);
    bool process();
    void print_result();

private:
    int sockfd_;
    int nsucc_, nfail_;
    ConnInfo *conn_info_;
    Mutex buf_mutex_;
    CondVar buf_wcond_;
    CondVar buf_rcond_;
    std::deque<std::string> read_buffer_;
    std::string to_send_;
    std::string last_sent_;
    bool message_get_();
    bool check_succ_(const std::string&, long&, long&);
    int mask_wait_(int fd, int mask, long long milliseconds);
    bool set_nonblock_(int fd);
};

#endif
