#ifndef __TICK_CONN_H__
#define __TICK_CONN_H__

#include "status.h"
#include "csapp.h"
#include "tick_thread.h"
#include "tick_define.h"

class TickConn
{
public:
    TickConn(int fd);
    ~TickConn();
    /*
     * Set the fd to nonblock && set the flag_ the the fd flag
     */
    bool SetNonblock();
    Status TickReadBuf();
    Status TickWriteBuf();
    void DriveMachine();
    Status TickAReadHeader();

private:

    int fd_;
    int flags_;
    /*
     * These functions parse the message from client
     */
    Status TickReadHeader(rio_t *rio);
    Status TickReadCode(rio_t *rio);
    Status TickReadPacket(rio_t *rio);


    Status BuildObuf();
    /*
     * The Variable need by read the buf,
     * We allocate the memory when we start the server
     */
    int header_len_;
    int32_t r_opcode_;
    char* rbuf_;
    int32_t cur_pos_;
    int32_t rbuf_len_;

    ConnStatus connStatus;

    char* wbuf_;
    int32_t wbuf_len_;
    TickThread *thread_;
};

#endif
