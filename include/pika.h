#ifndef __PIKA_H__
#define __PIKA_H__

#include <stdio.h>
#include <pthread.h>
#include <vector>

class PikaMeta;
class PikaWorker;
class PikaHb;
class PikaHbMonitor;
class PikaNode;

class Pika
{
public:
    Pika();
    ~Pika();

    /*
     * Start the heartbeat thread
     * Start heartbeat will listen to the hb_port and start receive
     * connection
     */
    int RunHb();

    int RunHbMonitor();


    /*
     * Start the main worker thread
     * The worker must be the last thread to start
     */
    int RunWorker();
    static void* StartWorker(void* args);

    /*
     * Start the pulse thread, pulse thread will connect all the node that
     * has connected.
     */
    int RunPulse();
    static void* StartPulse(void* args);

    PikaWorker* pikaWorker() { return pikaWorker_; }
    PikaHb* pikaHb() { return pikaHb_; }

private:

    std::vector<PikaNode> preNode_;
    /*
     * The thread contain the meta infomation
     */
    PikaMeta* pikaMeta_;

    pthread_t workerId_, hbId_;
    /*
     * The accept thread, accept user request, and redirect conn to worker
     * thread 
     */
    PikaWorker* pikaWorker_;
    /*
     * The heartbeat thread, accept other node heartbeat, and redirect to hbworker thred
     */
    PikaHb* pikaHb_;

    PikaHbMonitor* pikaHbMonitor_;


    // No copying allowed
    Pika(const Pika&);
    void operator=(const Pika&);

};

#endif
