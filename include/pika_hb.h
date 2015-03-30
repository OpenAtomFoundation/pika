#ifndef __PIKA_HB_H__
#define __PIKA_HB_H__

#include "status.h"
#include "csapp.h"
#include "pika_thread.h"
#include "pika_define.h"
#include "pika_epoll.h"

#include <vector>

class HbContext;
class PikaConn;

class PikaHb
{
public:
    PikaHb();
    ~PikaHb();

    /*
     * run the main heartbeat process
     */
    void Start();


    void CreatePulse();

    /*
     * Send the pulse to other every 3 second
     */
    void RunPulse();

    /*
     * Connect to the adj node
     */
    Status DoConnect(const char* adj_hostname, int adj_port, HbContext* hbContext);



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

    Status Pulse(HbContext*, const std::string &host, const int port);

    std::vector<Node> srv_;
    std::vector<Node> getHosts_;

    /*
     * This debug function used to printf the node is srv or gethosts
     */
    void DebugSrv();

    PikaEpoll *pikaEpoll_;


    /*
     * Here we used auto poll to find the next hb thread,
     * last_thread_ is the last hb thread
     * even at the first time we only have one hb thread
     */
    int last_thread_;
    /*
     * This is the thread that deal with heartbeat
     */
    PikaThread *hbThread_[PIKA_HEARTBEAT_THREAD];

    pthread_t thread_id_;

    /*
     * The server side of connect to other pika node
     */
    std::vector<PikaConn *> hbConns_;

    /*
     * The client side of context to other pika node
     */
    std::vector<HbContext *> hbContexts_;

    /*
     * The heartbeat servaddr and port information
     * get the servaddr_ from the pika_worker
     * get the port from config file
     */
    int sockfd_;
    int flags_;
    int hb_port_;
    struct sockaddr_in servaddr_;
    bool isSeed_;


    struct timeval timeout_;

    /*
     * Don't allow copy construct and copy assign construct
     */
    PikaHb(const PikaHb&);
    void operator=(const PikaHb&);
};

#endif
