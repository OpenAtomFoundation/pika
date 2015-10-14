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
#include "mutexlock.h"
//#include "leveldb/db.h"
//#include "leveldb/write_batch.h"

class PikaThread;
class PikaEpoll;
class PikaConn;

struct SlaveItem {
    std::string ip;
    int64_t port;
    int64_t state;
};

class PikaServer
{
public:
    PikaServer();
    ~PikaServer();

    void RunProcess();

    static void* StartThread(void* arg);
    nemo::Nemo* GetHandle() {return db_;};
    void ClientList(std::string &res);
    int ClientKill(std::string &ip_port);
    int ClientRole(int fd, int role);
    
    void set_masterhost(std::string &masterhost) { masterhost_ = masterhost; }
    std::string masterhost() { return masterhost_; }
    void set_masterport(int masterport) { masterport_ = masterport; }
    int masterport() { return masterport_; }
    void set_repl_state(int repl_state) { repl_state_ = repl_state; }
    int repl_state() { return repl_state_; }
    port::Mutex* Mutex() { return &mutex_; }
    std::string GetServerIp();
    void Offline(std::string ip_port);
    int GetServerPort();
    int TrySync(std::string &ip, std::string &port);
    int Slavenum() { return slaves_.size(); }
    

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

    std::string masterhost_;
    int masterport_;
    int repl_state_;

    port::Mutex mutex_;
    std::map<std::string, SlaveItem> slaves_;
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



    // No copying allowed
    PikaServer(const PikaServer&);
    void operator=(const PikaServer&);

};

#endif
