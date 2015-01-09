#ifndef __TICK_H__
#define __TICK_H__

#include <stdio.h>
#include <sys/epoll.h>
#include <stdlib.h>
#include <fcntl.h>
#include <event.h>
#include <unistd.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <sys/epoll.h>

#include "csapp.h"
#include "xdebug.h"
#include "tick_define.h"
#include "status.h"


class TickThread;
class TickEpoll;

class TickServer
{

public:
    TickServer();
    ~TickServer();

    void RunProcess();

    static void* StartThread(void* arg);

private:

    Status SetBlockType(BlockType type);
    /*
     * These functions parse the message from client
     */
    Status TickReadHeader(rio_t *rio);
    Status TickReadCode(rio_t *rio);
    Status TickReadPacket(rio_t *rio);

    Status BuildObuf();
    /*
     * The udp server port and address
     */
    int sockfd_;
    int flags_;
    int port_;
    struct sockaddr_in servaddr_;

    /*
     * The Epoll event handler
     */
    TickEpoll *tickEpoll_;

    int header_len_;
    int32_t r_opcode_;
    char* rbuf_;
    int32_t rbuf_len_;


    /*
     * Here we used auto poll to find the next work thread, 
     * last_thread_ is the last work thread
     */
    int last_thread_;
    /*
     * This is the work threads
     */
    TickThread *tickThread_[TICK_THREAD_NUM];

    // No copying allowed
    TickServer(const TickServer&);
    void operator=(const TickServer&);

};


#endif
