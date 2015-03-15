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
    TickHb();

    /*
     * run the main heartbeat process
     */
    void RunHb();


    void CreatePulse();

    void RunPulse();
    Status DoConnect();

private:

    struct Node
    {
        Node(std::string host, std::string port) :
            host_(host),
            port_(port)
        {};
        std::string host_;
        std::string port_;
    };

    vector<Node> srv_;
    vector<Node> getHosts_;

    TickEpoll *tickEpoll_;
    pthread_t thread_id_; 


    vector<TickConn *> hbConns_;
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

};

#endif
