#include "tick_hb.h"


TickHb::TickHb(int hb_port) : 
    hb_port_(hb_port)
{
    thread_id_ = pthread_self();
    // init sock
    sockfd_ = socket(AF_INET, SOCK_STREAM, 0);
    memset(&servaddr_, 0, sizeof(servaddr_));

    hb_port_ = g_tickConf->hb_port();
    servaddr_.sin_family = AF_INET;
    servaddr_.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr_.sin_port = htons(port_);

    bind(sockfd_, (struct sockaddr *) &servaddr_, sizeof(servaddr_));
    listen(sockfd_, 10);

    SetBlockType(kNonBlock);
    /*
     * inital the tickepoll object, add the notify_receive_fd to epoll
     */
    tickEpoll_ = new TickEpoll();
}
