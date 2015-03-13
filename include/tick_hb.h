#ifndef __TICK_HB_H__
#define __TICK_HB_H__

#include "status.h"
#include "csapp.h"
#include "tick_thread.h"
#include "tick_define.h"
#include "tick_epoll.h"

class TickHb
{
public:
    TickHb(int hb_port);


private:
    TickEpoll *tickEpoll_;
    pthread_t thread_id_; 

    /*
     * The heartbeat servaddr and port information
     * get the servaddr_ from the tick_server
     * get the port from config file
     */
    int sockfd_;
    int flags_;
    int hb_port_;
    struct sockaddr_in servaddr_;

};

#endif
