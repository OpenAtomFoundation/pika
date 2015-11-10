#ifndef __PIKA_THREAD_H__
#define __PIKA_THREAD_H__

#include "xdebug.h"

#include <pthread.h>
#include <sys/epoll.h>
#include <queue>
#include <map>
#include <time.h>

#include "port.h"
#include "pika_item.h"
#include "csapp.h"


class PikaItem;
class PikaEpoll;
class PikaConn;

class PikaThread
{
public:
    PikaThread(int thread_index);
    ~PikaThread();

    void RunProcess();
    int ProcessTimeEvent(struct timeval* target);


    int notify_receive_fd() { return notify_receive_fd_; }
    int notify_send_fd() { return notify_send_fd_; }
    int thread_index() { return thread_index_; }
    pthread_rwlock_t* rwlock() { return &rwlock_; }
    std::map<std::string, client_info>* clients() { return &clients_; }
    std::map<int, PikaConn *>* conns() { return &conns_; }

    pthread_t thread_id_;
    bool is_master_thread_;
    bool is_first_;
    struct timeval last_ping_time_;
    int querynums_;
    int last_sec_querynums_;

    // port::Mutex mutex() { return mutex_; }

private:

    friend class PikaServer;

    /*
     * These two fd receive the notify from master thread
     */
    int notify_receive_fd_;
    int notify_send_fd_;
    int thread_index_;

    /*
     * The PikaItem queue is the fd queue, receive from master thread
     */
    std::queue<PikaItem> conn_queue_;

    /*
     * The epoll handler
     */
    PikaEpoll *pikaEpoll_;

    std::map<int, PikaConn *> conns_;
    std::map<std::string, client_info> clients_;
    pthread_rwlock_t rwlock_;


    port::Mutex mutex_;

    // No copy || assigned operator allowed
    PikaThread(const PikaThread&);
    void operator=(const PikaThread &);
};

#endif
