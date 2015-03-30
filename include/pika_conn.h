#ifndef __PIKA_CONN_H__
#define __PIKA_CONN_H__

#include "status.h"
#include "csapp.h"
#include "pika_thread.h"
#include "pika_define.h"

class PikaConn
{
public:
    PikaConn(int fd);
    PikaConn();
    ~PikaConn();
    /*
     * Set the fd to nonblock && set the flag_ the the fd flag
     */
    bool SetNonblock();
    void InitPara();
    Status PikaReadBuf();
    void DriveMachine();
    int PikaGetRequest();
    int PikaSendReply();
    void set_fd(int fd) { fd_ = fd; };
    int flags() { return flags_; };

private:


    int fd_;
    int flags_;


    /*
     * These functions parse the message from client
     */
    Status PikaReadHeader(rio_t *rio);
    Status PikaReadCode(rio_t *rio);
    Status PikaReadPacket(rio_t *rio);


    Status BuildObuf(int32_t opcode, const int packet_len);
    /*
     * The Variable need by read the buf,
     * We allocate the memory when we start the server
     */
    int header_len_;
    int32_t r_opcode_;
    char* rbuf_;
    int32_t cur_pos_;
    int32_t rbuf_len_;

    ConnStatus connStatus_;

    char* wbuf_;
    int32_t wbuf_len_;
    int32_t wbuf_pos_;
    PikaThread *thread_;
};

#endif
