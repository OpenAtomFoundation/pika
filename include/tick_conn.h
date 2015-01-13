#ifndef __TICK_CONN_H__
#define __TICK_CONN_H__

class TickConn
{
public:
    TickConn(int fd);
    /*
     * Set the fd to nonblock && set the flag_ the the fd flag
     */
    bool SetNonblock();

private:

    int fd_;
    int flag_;
    /*
     * These functions parse the message from client
     */
    Status TickReadHeader(rio_t *rio);
    Status TickReadCode(rio_t *rio);
    Status TickReadPacket(rio_t *rio);

    /*
     * The Variable need by read the buf,
     * We allocate the memory when we start the server
     */
    int header_len_;
    int32_t r_opcode_;
    char* rbuf_;
    int32_t rbuf_len_;

    char* wbuf_;
    int32_t wbuf_len_;
    TickThread *thread;
};

#endif
