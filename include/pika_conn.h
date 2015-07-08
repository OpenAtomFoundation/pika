#ifndef __PIKA_CONN_H__
#define __PIKA_CONN_H__

#include "status.h"
#include "csapp.h"
#include "pika_thread.h"
#include "pika_define.h"
#include "sds.h"
#include <list>

class PikaConn
{
public:
    PikaConn(int fd);
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
    int ProcessInputBuffer();
    int ProcessInlineBuffer();
    int ProcessMultibulkBuffer();
    int DoCmd();

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

    sds wbuf_;
    int32_t wbuf_len_;
    int32_t wbuf_pos_;
    PikaThread *thread_;
};

#endif
