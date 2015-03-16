#ifndef __TICK_HB_H__
#define __TICK_HB_H__

#include "status.h"
#include "csapp.h"
#include "tick_thread.h"
#include "tick_define.h"
#include "tick_epoll.h"

#include <vector>


class TickHb
{
public:
    TickHb();

    /*
     * run the main heartbeat process
     */
    void RunHb();


    void CreatePulse();

    /*
     * Send the pulse to other every 3 second
     */
    void RunPulse();
    Status DoConnect(const char* adj_hostname, int adj_port, TickConn* tickConn);

private:

    struct Node
    {
        Node(std::string host, int port) :
            host_(host),
            port_(port)
        {};

        std::string host_;
        int port_;
    };

    std::vector<Node> srv_;
    std::vector<Node> getHosts_;

    TickEpoll *tickEpoll_;


    /*
     * Here we used auto poll to find the next hb thread,
     * last_thread_ is the last hb thread
     * even at the first time we only have one hb thread
     */
    int last_thread_;
    /*
     * This is the thread that deal with heartbeat
     */
    TickThread *hbThread_[TICK_HEARTBEAT_THREAD];

    pthread_t thread_id_;

    /*
     * The connect to other tick node
     */
    std::vector<TickConn *> hbConns_;

    /*
     * The heartbeat servaddr and port information
     * get the servaddr_ from the tick_server
     * get the port from config file
     */
    int sockfd_;
    int flags_;
    int hb_port_;
    struct sockaddr_in servaddr_;
    bool isSeed_;


    struct timeval timeout_;

};

#endif
