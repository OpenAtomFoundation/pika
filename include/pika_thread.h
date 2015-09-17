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

    pthread_t thread_id_;

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


    port::Mutex mutex_;

    // No copy || assigned operator allowed
    PikaThread(const PikaThread&);
    void operator=(const PikaThread &);
};

#endif
