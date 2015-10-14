#ifndef __PIKA_SERVER_H__
#define __PIKA_SERVER_H__

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

#include "status.h"
#include "csapp.h"
#include "xdebug.h"
#include "pika_define.h"
#include "nemo.h"
//#include "leveldb/db.h"
//#include "leveldb/write_batch.h"

class PikaThread;
class PikaEpoll;
class PikaConn;

class PikaServer
{
public:
    PikaServer();
    ~PikaServer();

    void RunProcess();

    static void* StartThread(void* arg);
    nemo::Nemo* GetHandle() {return db_;};
    int ClientList(std::string &res);
    int ClientKill(std::string &ip_port);
    

private:
    friend class PikaConn;
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
    PikaEpoll *pikaEpoll_;

    /*
     * The leveldb handler
     */
    nemo::Nemo *db_;
 //   leveldb::DB *db_;

 //   leveldb::Options options_;

    /*
     * Here we used auto poll to find the next work thread,
     * last_thread_ is the last work thread
     */
    int last_thread_;
    /*
     * This is the work threads
     */
    PikaThread *pikaThread_[PIKA_THREAD_NUM];


//    int64_t stat_numcommands;
//    int64_t stat_numconnections;
//
//    int64_t stat_keyspace_hits;
//    int64_t stat_keyspace_misses;

    // No copying allowed
    PikaServer(const PikaServer&);
    void operator=(const PikaServer&);

};

#endif
