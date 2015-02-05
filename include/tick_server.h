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
#include "leveldb/db.h"
#include "leveldb/write_batch.h"

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
    

    friend class TickConn;
    Status SetBlockType(BlockType type);

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

    /*
     * The leveldb handler
     */
    leveldb::DB *db_;

    leveldb::Options options_;

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
