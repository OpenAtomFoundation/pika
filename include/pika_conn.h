#ifndef __PIKA_CONN_H__
#define __PIKA_CONN_H__

#include "status.h"
#include "csapp.h"
#include "pika_thread.h"
#include "pika_define.h"
#include "mutexlock.h"
#include "sds.h"
#include <list>
#include <map>

class PikaConn
{
public:
    PikaConn(int fd, std::string ip_port, int role);
    ~PikaConn();
    /*
     * Set the fd to nonblock && set the flag_ the the fd flag
     */
    bool SetNonblock();
    Status PikaReadBuf();
    int PikaGetRequest();
    int PikaSendReply();

    int GetArgc() {return argv_.size(); };
    void AddArgv(std::string a) { argv_.push_back(a); };
    void Reset();
    void CloseAfterReply() {should_close_after_reply = true; };
    bool ShouldCloseAfterReply() { return should_close_after_reply; };
    int ProcessInputBuffer();
    int ProcessInlineBuffer(std::string &err_msg);
    int ProcessMultibulkBuffer(std::string &err_msg);
    int DoCmd();
    void append_wbuf(const std::string &item);
    struct timeval tv() { return tv_; };
    void UpdateTv(struct timeval now) { tv_ = now; };
    int fd() { return fd_; };
    int role() {return role_; };
    void set_role(int role) { role_ = role; }
    std::string ip_port() { return ip_port_; };
//    struct timeval  lastinteraction() { return lastinteraction_; };
//    void UpdateLastInteraction() { gettimeofday(&lastinteraction_, NULL); };
    

private:

    int fd_;
    std::list<std::string> argv_;
    int flags_;
    sds rbuf_;
    int32_t cur_pos_;
    int32_t rbuf_len_;
    int req_type_;
    int multibulklen_;
    long bulklen_;
    bool should_close_after_reply;
    struct timeval tv_;
    std::string ip_port_;
    bool is_authed_;
    int role_;
//    struct timeval lastinteraction_;
    sds wbuf_;
    sds msbuf_;
    int32_t wbuf_len_;
    int32_t wbuf_pos_;
    PikaThread *thread_;
    port::Mutex mutex_;
};

#endif
