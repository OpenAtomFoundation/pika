#ifndef PIKA_HB_MONITOR_H_
#define PIKA_HB_MONITOR_H_

/*
 *
 *  Overview:
 *  ---------
 *
 *  The event class used to deal with event that will change metadata
 *  There is many kinds of event
 *  1. After heartbeat we find a connection is down
 *  2. 
 *  
 *
 */

#include "pika_node.h"
#include <vector>
#include "pthread.h"

class PikaNode;

class PikaHbMonitor
{
public:
    PikaHbMonitor(std::vector<PikaNode>* cur);

    static void CreateHbMonitor(pthread_t* pid, PikaHbMonitor* pikaHbMonitor);
    static void* StartHbMonitor(void* arg);

    void RunProcess();

    pthread_t* thread_id() { return &thread_id_;}

private:

    pthread_t thread_id_;
    std::vector<PikaNode>* cur_;
    std::vector<PikaNode> pre;

};

#endif
