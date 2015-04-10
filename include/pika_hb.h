#ifndef __PIKA_HB_H__
#define __PIKA_HB_H__

/*
 * 
 *                                    Pika heartbeat of module
 *                                    ==========================
 * 
 *  Overview:
 *  ---------
 *  The Pika heartbeat module just build connection between nodes and sent
 *  message between nodes. The Pika module will save the heartbeat result in srv
 *  The there will be pika_event thread to check whether the srv has change, if
 *  we find that srv has change, in the pika_event will make a change message to
 *  other node
 *
 */

#include "status.h"
#include "csapp.h"
#include "pika_thread.h"
#include "pika_define.h"
#include "pika_epoll.h"
#include "pika_node.h"
#include "pthread.h"

#include <vector>

class HbContext;
class PikaConn;
class PikaNode;

class PikaHb
{
public:
    PikaHb(std::vector<PikaNode>* cur);
    ~PikaHb();

    /*
     * run the main heartbeat process
     */
    void RunProcess();


    void CreatePulse();

    /*
     * Send the pulse to other every 3 second
     */
    void StartPulse();

    static void CreateHb(pthread_t* pid, PikaHb* pikaHb);
    static void* StartHb(void* arg);


    /*
     * Connect to the adj node
     */
    Status DoConnect(const char* adj_hostname, int adj_port, HbContext* hbContext);

    pthread_t* thread_id() { return &thread_id_; }


private:

    pthread_t thread_id_;
    Status Pulse(HbContext*, const std::string &host, const int port);

    std::vector<PikaNode>* cur_;
    std::vector<PikaNode> getHosts_;

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
